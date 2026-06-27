# 03 — Control-Plane Protocol (Service ↔ Broker)

**Goal:** New control ring-buffer messages for registering publications, subscribing, and the
responses that let a service publish to the leader and open a tailer.
**Modules:** `src/broker/topics/topic_messages.zig`, wired into
`src/broker/control/control_loop.zig` and `src/service/control_agent.zig`.
**Depends on:** 01, 02.

These extend the existing control message family in
`src/broker/control/control_messages.zig` (4-byte `ControlMessageHeader{template_id, body_length}`,
extern structs, flyweight over ring-buffer memory). Template IDs 1–6 are taken; topics use **7–15**.

---

## 1. Message table

| Template | Name | Dir | Purpose |
|---|---|---|---|
| 7 | `RegisterTopicPublicationMsg` | S→B | Producer declares intent to publish to a topic. |
| 8 | `TopicPublicationResponseMsg` | B→S | `{topic_id, leader_node, leader_epoch, effective_config, status}`. |
| 9 | `SubscribeTopicMsg` | S→B | Subscriber requests a topic; carries start position. |
| 10 | `TopicSubscriptionResponseMsg` | B→S | `{topic_id, queue_dir, geometry, start_index, status}`. |
| 11 | `UnregisterTopicPublicationMsg` | S→B | Producer stops publishing. |
| 12 | `UnsubscribeTopicMsg` | S→B | Subscriber stops; may close the replica if last subscriber. |
| 13 | `TopicLeaderChangedMsg` | B→S | Producer must re-target leader/epoch (failover, spec 08). |
| 14 | `TopicEndpointMsg` | B→S | Topic-leader node → Aeron endpoint/stream mapping for publishing. |
| 15 | `TopicAckFeedbackMsg` | B→S | Throttled `{topic_id, leader_epoch, replicated_hwm}` for `replicate_once` (§6). |

## 2. Key encodings (extern struct sketches)

```zig
pub const RegisterTopicPublicationMsg = extern struct {
    header: ControlMessageHeader, // template_id = 7
    local_service_id: i32,
    config: TopicConfig,          // requested geometry (first_wins; spec 01)
    name_length: u16,
    _pad: u16 = 0,
    // followed by name_length bytes of UTF-8 topic name
};

pub const TopicPublicationResponseMsg = extern struct {
    header: ControlMessageHeader, // template_id = 8
    topic_id: u64,
    leader_node_id: i16,
    status: u8,                   // 0=ok, 1=config_mismatch, 2=collision, 3=disabled
    _pad: u8 = 0,
    leader_epoch: u64,
    effective_config: TopicConfig,
};

pub const SubscribeTopicMsg = extern struct {
    header: ControlMessageHeader, // template_id = 9
    local_service_id: i32,
    start_position: u8,           // 0 = earliest, 1 = latest
    _pad: u8 = 0,
    name_length: u16,
    // followed by name_length bytes of topic name
};

pub const TopicSubscriptionResponseMsg = extern struct {
    header: ControlMessageHeader, // template_id = 10
    topic_id: u64,
    status: u8,                   // 0=ok, 1=unknown_topic, 2=disabled
    _pad: u8 = 0,
    start_index: u64,             // ringloom index to open the tailer at
    geometry: TopicConfig,        // so the service can validate / open correctly
    queue_dir_length: u16,
    _pad2: u16 = 0,
    // followed by queue_dir_length bytes of absolute queue directory path
};
```

`align(1)` any field that follows a variable-length region, mirroring `ServiceInstanceEntry`. Add
`comptime` size asserts and encode/decode helpers + tests as in `control_messages.zig`.

## 3. Broker handling — register publication (control loop)

```
on RegisterTopicPublicationMsg(name, config, svc):
    id = topicIdOf(name)
    if not topics.enabled: reply status=disabled
    if self is the TOPIC leader:
        result = registry.createOrValidate(id, name, config)   // spec 01/02
        if ok and newly created:
            cmd → receiver engine: OpenMaster{id, config}
            broadcast TopicCreated (admin)  // every topics-enabled peer eagerly opens a replica
        reply TopicPublicationResponse{ok|mismatch|collision, id, leader_node, epoch, eff_config}
    else:
        leader-proxied create (see §5): forward create to the topic leader, await TopicCreated,
        then reply pointing the producer at leader_node.
    also send TopicEndpointMsg so the producer knows how to reach the topic leader.
```

## 4. Broker handling — subscribe (control loop)

```
on SubscribeTopicMsg(name, start_position, svc):
    id = topicIdOf(name)
    rec = registry.get(id) orelse { lookup from topic leader; if still unknown → status=unknown_topic }
    # full mesh: this topics-enabled broker already opened a local replica (or master) when it
    # learned the topic via TopicCreated/TopicInfo. No OpenReplica on subscribe.
    if not rec.local_queue_open:           # only if the eager-open hasn't landed yet
        cmd → receiver engine: OpenReplica{id, rec.config} ; StartSink{id, rec.leader_node}
    registry.local_subscriber_count++      # informational only
    queue_dir = topics.path/<group>/node-<id>/<name>/
    start_index = (start_position==earliest) ? rec.first_available : current_tip
                  (for a not-yet-populated replica, earliest = 0/first; latest = "open at tip,
                   which may be 0 until catch-up — acceptable, tailer blocks)
    reply TopicSubscriptionResponse{ok, id, queue_dir, geometry, start_index}
```

`start_index` semantics for `latest`: the broker returns the master's current HWM (queried from the
registry/topic-engine status). The replica may not have caught up to it yet; the tailer simply waits.
For `earliest`, return the leader's first retained index (or 0).

## 5. Leader-proxied creation (recommended)

To keep **first_wins** deterministic, route creation through the topic leader even for remote
producers: the local broker, on `RegisterTopicPublication` when it is not the topic leader, sends an
admin `TopicLookup`; if absent it forwards a create request to the topic leader (new admin template
`TEMPLATE_TOPIC_CREATE_REQUEST{name, config, origin_node}`), the leader validates + creates +
broadcasts `TopicCreated`, and the originating broker replies to the producer once it observes the
record. This avoids two brokers creating the same topic with different configs racing the broadcast.

## 6. Publish acknowledgement (`replicate_once`)

Ack mode is chosen **per publish** (carried in `TopicPublishHeader`, spec 04). For `replicate_once`,
the topic leader emits **throttled** `TEMPLATE_TOPIC_ACK_FEEDBACK{topic_id, leader_epoch,
replicated_hwm}` admin frames toward brokers that have local producers for the topic (interval =
`broker.topics.ack_feedback_interval_us`). Each such broker forwards the HWM to its local producer
services as `TopicAckFeedbackMsg` (template 15) over the control RB.

`replicated_hwm` is the highest master index that **≥1 replica** has applied (multi-node), or the
master append index on a single-node broker (no replica to wait for). The producer client completes
every pending publish future whose assigned index ≤ `replicated_hwm` (spec 09). This is HWM-based and
**not** per message; it never sits on the publish hot path. `fire_and_forget` publishes are never
tracked and never wait.

## 7. Tests

- Encode/decode round-trips + size asserts for templates 7–15.
- Register (topic leader) creates record + emits `OpenMaster` + `TopicCreated`; duplicate is
  idempotent; mismatch → status.
- Subscribe returns the eagerly-opened replica's queue_dir; a race before eager-open still opens once;
  second local subscriber does **not** re-open; unsubscribe decrements (does not close — full mesh).
- Subscribe for unknown topic triggers lookup; unknown after lookup → status=unknown_topic.
- `replicate_once`: `TopicAckFeedbackMsg` advances; producer future completes when index ≤ hwm.
