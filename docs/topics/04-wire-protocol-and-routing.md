# 04 — Wire Protocol & Routing (Aeron)

**Goal:** Byte-exact wire formats for topic publish frames and replication frames, plus how the
receiver loop demultiplexes them.
**Modules:** `src/common/message/topic_data_header.zig`; routing hooks in
`src/broker/receiver/receiver_event_loop.zig`.
**Depends on:** 01.

---

## 1. Background: existing transport

The broker already carries application data as `RingLoomDataHeader` (32 bytes, `"RLM2"`, see
`src/common/message/data_header.zig`) over Aeron, routed by `(target_node_id, target_service_id)`.
The receiver loop (`receiver_event_loop.zig`) polls two subscriptions: **data UDP** and **admin
UDP**. Topics add **two more logical streams** and **reuse the data header's `flags`/reserved**
fields to discriminate, so we do not break existing routing.

Stream base IDs (config, spec: `topics-architecture.md` §10): `pub_stream_base` (default 50000),
`repl_stream_base` (default 40000). Per-topic stream id = `base + (topic_id % stream_span)` or a
single multiplexed stream demuxed by the in-payload `topic_id`; **recommended: a single publish
stream and a single replication stream per node, demuxed by `topic_id`** (Aeron sessions are scarce;
the envelope carries the id). Document the multiplexed choice as default.

## 2. Topic publish frame (producer → topic leader)

Carried on the **publish stream**, targeted at the topic-leader node. Outer header is the existing
`RingLoomDataHeader` with a **new flag**:

```zig
// add to common/platform/constants.zig
pub const flag_topic: u8 = 0x10;   // currently-unused bit; data header legal_flags must include it
```

Layout: `RingLoomDataHeader (flags |= flag_topic; target_node_id = topic_leader; target_service_id = 0)`
followed by:

```zig
pub const TopicPublishHeader = extern struct {
    topic_id: u64,
    leader_epoch: u64,     // epoch the producer believes is current (fenced at leader, spec 08)
    correlation_id: i64,   // app-supplied; used by the leader for replicate_once ack accounting
    ack_mode: u8,          // AckMode (spec 01): 0=fire_and_forget, 1=replicate_once
    flags: u8 = 0,
    _reserved: [6]u8 = .{0}**6,
    // followed by the application payload bytes (length = data header payload_length - 24)
    comptime { std.debug.assert(@sizeOf(TopicPublishHeader) == 32); }
};
```

The receiver decodes the `RingLoomDataHeader`, sees `flag_topic`, then reads the
`TopicPublishHeader` from the payload, and hands `{topic_id, leader_epoch, correlation_id, ack_mode,
payload}` to the topic engine's `onPublish` append path (spec 05), which appends directly to the
master queue. For `ack_mode == replicate_once`, the leader records the assigned index so the ack HWM
(spec 03 §6) can advance once a replica applies it. `source_node_id`/`source_service_id` identify the
producer for diagnostics and for addressing throttled ack feedback.

> The legal-flags validation in `data_header.zig` (`legal_flags`) MUST be extended to permit
> `flag_topic` or topic frames will be dropped as invalid.

## 3. Topic replication envelope (leader source ⇄ replica sink)

Carried on the **replication stream**. ringloom-queue repl frames are already self-framed (16-byte
header, `session_id`, types HELLO/HELLO_ACK/EXCERPT/.../ACK/RESET — see ringloom-queue
`docs/12-replication.md` §4). We wrap each whole repl frame in a thin routing envelope so a single
Aeron stream can carry many topics in **both** directions:

```zig
pub const topic_repl_magic = [4]u8{ 'R','T','P','1' };
pub const ReplDirection = enum(u8) { source_to_sink = 0, sink_to_source = 1 };

pub const TopicReplEnvelope = extern struct {
    magic: [4]u8 = topic_repl_magic,
    version: u8 = 1,
    direction: u8,         // ReplDirection
    _pad: u16 = 0,
    target_node_id: u16,   // demux target (drop if != local)
    source_node_id: u16,
    topic_id: u64,
    leader_epoch: u64,     // fencing (spec 08); stale epochs dropped
    frame_length: u32,     // length of the wrapped ringloom-queue repl frame
    _reserved: u32 = 0,
    // followed by `frame_length` bytes: one whole ringloom-queue repl frame
    comptime { std.debug.assert(@sizeOf(TopicReplEnvelope) == 32); }
};
```

- **source → sink:** EXCERPT/EXCERPT_BATCH/CYCLE_ROLL/HEARTBEAT/HELLO_ACK/HELLO_NACK/CLOSE.
- **sink → source:** HELLO/ACK/RESET/CLOSE. The **ACK** frame carries the sink's `applied_index`,
  which the leader aggregates across replicas to advance the `replicate_once` ack HWM (spec 03 §6)
  and which the new leader uses to pick the most-advanced replica during the failover catch-up
  barrier (spec 08).

The envelope's `topic_id` + `direction` + `(source_node_id, session_id-inside-frame)` uniquely
identify which per-(topic, peer) **session** the frame belongs to (spec 06). Whole-frame delivery is
guaranteed by Aeron fragment reassembly (`FragmentAssembler`, already used). `frame_length` lets the
sink/source hand the exact borrowed slice to ringloom-queue's `nextFrame`.

## 4. Receiver-loop demux

`receiver_event_loop.zig` currently routes data frames via `routeDataFrameToService`. Add:

```
processBrokerUdpFragment(bytes):
    header = decode RingLoomDataHeader
    if header.target_node_id != local: drop (misdirected)   # unchanged
    if header.flags & flag_topic:
        decode TopicPublishHeader from payload
        engine.onPublish({topic_id, leader_epoch, correlation_id, ack_mode, payload})  # APPEND DIRECTLY to master
        return
    else: routeDataFrameToService(...)   # unchanged service path
```

Replication frames arrive on the dedicated **replication subscription** (new), polled by a third
`FragmentAssembler` in the receiver loop (alongside `broker_udp_assembler` and
`broker_admin_udp_assembler`):

```
processTopicReplFragment(bytes):
    env = decode TopicReplEnvelope ; if env.target_node_id != local: drop
    engine.deliverReplFrame(env.topic_id, env.direction, env.leader_epoch, frame_bytes)
    # routed to the matching source/sink session inbound FrameQueue; the engine steps it in-loop,
    # applying excerpts via writeAtIndex (replica) or shipping (leader) — all on the receiver thread
```

The receiver loop **appends topic data directly** (master append on `onPublish`, replica apply when
stepping sinks). It does not stall on disk because the separate prefetcher thread (spec 05) keeps the
write/read pages resident ahead of the tip; a page fault only occurs as a reported fallback if the
prefetcher falls behind.

## 5. Why not reuse `target_service_id`?

`target_service_id` is `u16` and indexes the service ring-buffer registry. Topics need a `u64` id
and append to queues, not ring buffers. Overloading the field would collide with real services and
break `message_router.lookup`. A distinct `flag_topic` + `TopicPublishHeader.topic_id` keeps the two
planes cleanly separated while reusing the same Aeron transport and `RingLoomDataHeader` framing.

## 6. Tests

- `TopicPublishHeader` / `TopicReplEnvelope` are 32 bytes; encode/decode round-trip; bad magic /
  wrong target_node rejected.
- `flag_topic` accepted by `data_header` validation; a topic frame is *not* routed to a service.
- A wrapped repl frame survives envelope encode→decode with `frame_length` exact.
- Misdirected envelope (wrong target_node) dropped with a counter increment.
