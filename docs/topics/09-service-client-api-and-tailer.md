# 09 — Service Client API & Tailer

**Goal:** Producer and subscriber APIs in the service runtime and the C ABI, with subscribers reading
the local queue via an embedded ringloom-queue tailer.
**Modules:** `src/service/topics/topic_publisher.zig`, `src/service/topics/topic_subscription.zig`,
`src/service/service_client.zig`, `src/service/c_abi.zig`, `include/ringloom_service.h`.
**Depends on:** 03.

---

## 1. Producer API (native Zig)

```zig
pub const AckMode = enum(u8) { fire_and_forget = 0, replicate_once = 1 };

pub const TopicPublisher = struct {
    topic_id: u64,
    leader_node_id: u16,              // the TOPIC leader (spec 08), not the cluster master
    leader_epoch: u64,
    leader_pub: *aeron.Publication,   // IPC if topic leader co-located, else UDP to it
    replicated_hwm: u64,              // updated from TopicAckFeedbackMsg (spec 03 §6)
    // updated on TopicLeaderChanged / TopicEndpoint control messages

    /// Non-blocking. Builds RingLoomDataHeader(flag_topic)+TopicPublishHeader(ack_mode)+payload,
    /// offers to the topic-leader publication. Returns the assigned local sequence in `out_seq`
    /// for replicate_once (so the caller can await its ack), or ignores it for fire_and_forget.
    pub fn publish(self: *@This(), payload: []const u8, correlation_id: i64,
                   ack_mode: AckMode) PublishResult;

    /// replicate_once: true once replicated_hwm >= the index assigned to `correlation_id`.
    /// Driven by TopicAckFeedbackMsg; never blocks the publish hot path.
    pub fn isAcked(self: *const @This(), publish_index: u64) bool {
        return self.replicated_hwm >= publish_index;
    }
};
```

Registration: `client.registerTopicPublication(name, TopicConfig) !TopicPublisher` — sends
`RegisterTopicPublicationMsg`, awaits `TopicPublicationResponseMsg`, resolves the topic-leader
endpoint from `TopicEndpointMsg`, returns the handle. On `TopicLeaderChanged` the control agent
updates the handle's `leader_node_id`/`leader_epoch`/publication; on `TopicAckFeedbackMsg` it advances
`replicated_hwm` so pending `replicate_once` publishes complete.

Ack mode is **per publish**: `fire_and_forget` returns as soon as the frame is offered;
`replicate_once` lets the caller poll `isAcked(publish_index)` (or await a future wrapping it). The
broker delivers acks as **throttled high-water-mark feedback**, so a single feedback frame completes
many pending publishes at once. On a single-node topics-enabled broker the HWM advances on leader
append (no replica to wait for).

Publishing uses the **same service-side direct-publication mechanism** already used for remote
service sends (Aeron IPC to a co-located broker, Aeron UDP to a remote topic leader). No new transport
on the service side — just a new target stream (`pub_stream_base`) and header.

## 2. Subscriber API (native Zig)

```zig
pub const TopicSubscription = struct {
    topic_id: u64,
    tailer: rq.Tailer(RawBytes),   // opened on the queue_dir from the broker response
    pub fn poll(self: *@This()) !?[]const u8;          // borrowed payload, valid until next poll
    pub fn collect(self: *@This(), backoff) ![]const u8;
    pub fn index(self: *const @This()) u64;
    pub fn deinit(self: *@This()) void;
};
```

Subscription: `client.subscribeTopic(name, .{ .start = .earliest|.latest }) !TopicSubscription` —
sends `SubscribeTopicMsg`, awaits `TopicSubscriptionResponseMsg`, opens a ringloom-queue tailer on
`queue_dir` at `start_index`. The service then polls the tailer in its own loop. **No broker round
trip per message.**

The service runtime links `ringloom_queue` (the broker already does). The tailer reads the local
replica (or master, if co-located with the leader) directory via mmap.

## 3. C ABI (`include/ringloom_service.h`, ABI bump)

Follow existing conventions: leading `size` field on option structs, opaque handles, borrowed views,
stable return codes (extend the existing service ABI version — see repo memory "Service ABI vN").

```c
// Producer
int  ringloom_register_topic_publication(ringloom_client_t*,
        const ringloom_topic_config_t* cfg, const char* name,
        ringloom_topic_publisher_t** out);
// ack_mode: 0 = fire_and_forget, 1 = replicate_once. out_index receives the assigned
// publish sequence (use with ringloom_topic_is_acked) for replicate_once; ignored otherwise.
int  ringloom_publish_to_topic(ringloom_topic_publisher_t*,
        const uint8_t* payload, size_t len, int64_t correlation_id,
        uint8_t ack_mode, uint64_t* out_index);  // -> result enum
// replicate_once: non-blocking ack check; true once the publish_index is replicated to >=1 replica
// (or appended, on a single-node broker). Driven by throttled HWM feedback.
int  ringloom_topic_is_acked(ringloom_topic_publisher_t*, uint64_t publish_index); // 1=acked,0=pending
void ringloom_unregister_topic_publication(ringloom_topic_publisher_t*);

// Subscriber
int  ringloom_subscribe_topic(ringloom_client_t*, const char* name,
        ringloom_topic_start_t start, ringloom_topic_subscription_t** out);
int  ringloom_topic_poll(ringloom_topic_subscription_t*,
        const uint8_t** out_payload, size_t* out_len, int64_t* out_index); // borrowed
void ringloom_unsubscribe_topic(ringloom_topic_subscription_t*);
```

`ringloom_topic_config_t`: `{ uint32_t size; char roll_scheme[16]; uint32_t retention_cycles;
uint32_t flags; }` — **ack mode is per publish, not part of topic config**.
`ringloom_topic_start_t`: `EARLIEST=0, LATEST=1`. `ringloom_topic_poll` returns a borrowed payload
valid until the next poll (mirrors ringloom-queue's tailer borrow semantics) — copy if retaining.

## 4. Bindings

Mirror the C ABI additions into the existing language bindings (Java/Node/C++ test bindings exist —
repo memory: `zig build test-java|test-node|test-cpp`). Each gets `registerTopicPublication`,
`publishToTopic` (with an `ackMode` argument + an `isAcked(index)`/await helper for `replicate_once`),
`subscribeTopic`, `poll`/iterator, `unsubscribe`. The subscriber binding opens the
ringloom-queue tailer through that language's existing ringloom-queue binding **or** through a thin
service-runtime passthrough; prefer the service-runtime passthrough so bindings depend only on
`ringloom_service`.

## 5. Test service binaries

Add to `src/bin/` (per the repo's test-service convention: echo/ping/forwarder/...):

- `test_topic_publisher_service.zig` — registers + publishes N messages at a rate; supports
  `fire_and_forget` and `replicate_once` (awaiting acks).
- `test_topic_subscriber_service.zig` — subscribes (earliest/latest), reads, asserts
  order/count/contiguity, reports via the harness.

## 6. Tests

- Register→publish→(leader appends)→subscriber tailer reads exact payloads in order.
- `replicate_once`: publish returns an index; `isAcked` flips true only after a replica applies (or
  immediately on a single-node broker); `fire_and_forget` never waits.
- `latest` subscriber misses pre-existing messages, sees only new ones; `earliest` sees all.
- Borrowed-view lifetime: payload valid until next poll; copying works.
- Leader-change mid-stream: producer re-targets the new topic leader, no duplicate/dropped messages
  beyond the documented failover window (spec 08); acked messages survive.
