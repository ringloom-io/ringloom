# 06 — Replication over Aeron

**Goal:** Implement ringloom-queue's pluggable transport contract over the broker's Aeron UDP so a
leader's `ReplicationSource` ships excerpts to each replica's `ReplicationSink`, with the bidirectional
control path and reliable-or-disconnect semantics ringloom-queue requires.
**Modules:** `src/broker/topics/repl_aeron_transport.zig`, `repl_session.zig`; driven by the receiver-loop topic engine (spec 05).
**Depends on:** 04, 05.

---

## 1. Contract recap (from ringloom-queue `docs/12-replication.md`)

- `ReplicationSource(Outbound, Inbound)` / `ReplicationSink(Outbound, Inbound)` are **comptime
  generic** over channel types. We supply concrete `Outbound`/`Inbound` structs.
- **Outbound:** `offer(frame) i64` (copy/consume before return; `>=0` accepted, negative
  `OfferResult`), `isConnected()`, `isBackPressured()`.
- **Inbound (pull):** `poll(fragment_limit) u32` (reassemble whole frames into our queue),
  `nextFrame() ?[]const u8` (borrowed until next poll), `isConnected()`.
- **Hard requirement:** transport is **reliable, ordered, in-session**; any loss must surface as a
  **disconnect**, never a silent gap. The sink detects gaps→RESET, but relies on us not silently
  dropping mid-stream.

## 2. Mapping onto Aeron

Aeron gives reliable, ordered delivery **within an image** (NAK + retransmit). Image loss /
reconnection is surfaced by Aeron as unavailable-image events. We translate:

| ringloom-queue need | Aeron mechanism |
|---|---|
| Ordered, reliable in-session bytes | One Aeron publication→subscription image per direction per session. |
| "Loss ⇒ disconnect, not gap" | On unavailable-image / new-session-id, our channel returns `isConnected()=false`. The sink/source tears down; the sink re-HELLOs and the source replays. |
| Whole-frame delivery | `FragmentAssembler` reassembles fragmented repl frames before `nextFrame` returns them. |
| Backpressure | Aeron `offer` returning `BACK_PRESSURED`/`ADMIN_ACTION` → return `OfferResult.back_pressured`; the source holds and retries the exact frame. |

### 2.1 Channels

```zig
pub const OutboundChannel = struct {
    pub_: *aeron.Publication,   // topic replication publication toward `peer`
    envelope: TopicReplEnvelope,// pre-filled {topic_id, leader_epoch, direction, src, target}
    scratch: []u8,              // envelope + frame staging (>= max_frame_bytes + 32)
    pub fn offer(self: *@This(), frame: []const u8) i64 {
        // build envelope header into scratch, copy frame, then aeron offer the whole thing.
        // map aeron result: >=0 → position; BACK_PRESSURED/ADMIN_ACTION → back_pressured;
        // NOT_CONNECTED/CLOSED/MAX_POSITION → not_connected/closed.
    }
    pub fn isConnected(self: *@This()) bool { return self.pub_.isConnected(); }
    pub fn isBackPressured(self: *@This()) bool { ... }
};

pub const InboundChannel = struct {
    frames: FrameQueue,         // filled by the receiver loop's repl demux (spec 04 §4)
    connected: AtomicBool,      // cleared on unavailable-image for this session
    pub fn poll(self: *@This(), limit: u32) u32 { return self.frames.availableUpTo(limit); }
    pub fn nextFrame(self: *@This()) ?[]const u8 { return self.frames.pop(); } // borrowed
    pub fn isConnected(self: *@This()) bool { return self.connected.load(.acquire); }
};
```

> The repl frames themselves are **unwrapped** before reaching `InboundChannel` (the receiver-loop
> demux strips `TopicReplEnvelope` and routes the inner frame to the matching session's
> `FrameQueue`). So `nextFrame` hands ringloom-queue exactly one whole repl frame.

## 3. Sessions: who connects to whom (full mesh)

Because replication is **full mesh**, every topics-enabled broker maintains a sink for **every** topic
at all times (opened eagerly on `TopicCreated`, spec 02/07), and the topic leader maintains a source
toward **every other topics-enabled broker** — not only those with subscribers.

- A **replica** (every non-leader topics-enabled broker) opens a sink for `topic_id`: it creates an
  `OutboundChannel` (direction `sink_to_source`, target = topic-leader node) and an `InboundChannel`,
  then constructs `repl.Sink(Transport)` and starts `step()`-ing it from the receiver engine. The
  sink emits **HELLO** with its `last_applied_index` (derived from its local replica queue) + roll
  config, and thereafter **ACK**s carry the sink's advancing `applied_index` (used by the leader for
  `replicate_once` ack accounting, spec 05 §3.1, and by failover, spec 08).
- The **topic leader** lazily creates a **source session** for `(topic_id, peer)` upon the first
  inbound HELLO from that peer: it binds a `repl.Source(Transport)` to the master queue with an
  `OutboundChannel` (direction `source_to_sink`, target = that peer). It replies HELLO_ACK and begins
  shipping.
- **`ReplHub`** (`repl_session.zig`) owns the maps:
  `sinks: AutoHashMap(TopicId, SinkSession)` (this node as replica) and
  `sources: AutoHashMap(struct{TopicId, peer:u8}, SourceSession)` (this node as leader). `stepAll`
  iterates both with a per-session work budget and surfaces each sink's `applied_index` to the engine.

Aeron publications/subscriptions for the repl stream are set up once per peer (or one multiplexed
stream demuxed by envelope `topic_id` + inner `session_id`). Recommended: **one repl publication per
peer node**, multiplexing all topics; the envelope demuxes. This bounds Aeron resource usage to
O(peers), not O(topics×peers).

## 4. Geometry / compatibility

The replica queue MUST be created (spec 07) with the **same roll scheme/geometry** as the master, or
the source replies `HELLO_NACK(config_mismatch)`. The geometry comes from the replicated
`TopicRecord.config` (spec 02). `queue_id` for the sink = `topic_id` bytes (16-byte field: pack
`topic_id` + zero pad, or a deterministic UUID from `topic_id`); the source validates it matches.

## 5. Epoch interaction (fencing)

- Outbound frames carry `leader_epoch` in the envelope.
- A sink drops inbound frames whose envelope `leader_epoch` is **older** than the highest epoch it
  has accepted, and treats an epoch **increase** as a reason to tear down and re-HELLO against the
  new leader (spec 08).
- A source ignores HELLO/ACK envelopes stamped with an epoch other than its current term.

## 6. Failure handling

- **Backpressure:** `offer → back_pressured`; source holds the frame, retries next `step`. Watchdog
  via ringloom-queue `backpressure_watchdog_ns`.
- **Disconnect:** Aeron unavailable-image → `isConnected()=false`; source drops the session, sink
  reconnects with backoff and re-HELLOs; ringloom-queue resumes from the sink's true
  `last_applied_index` (re-derived from the local queue).
- **Gap/corrupt:** ringloom-queue sink sends RESET; both re-handshake; never writes past a gap.

## 7. Tests

- Loopback-style: drive a source+sink in one process over in-memory channels (mirror ringloom-queue
  `loopback.zig`) wrapped in `TopicReplEnvelope`; assert byte-for-byte replica parity.
- Aeron e2e: leader + replica brokers, publish N messages, assert the replica queue tailer reads all
  N in order (spec 11).
- Full mesh: a topics-enabled broker with **no** local subscriber still opens a sink and stays
  byte-parity with the leader (replica exists without a reader).
- ACK applied_index: the sink's ACK advances and the leader's `replicated_hwm` reflects the slowest
  ≥1-replica progress (drives `replicate_once`).
- Backpressure: throttle the outbound channel; assert no gaps, source holds frames.
- Disconnect/reconnect: drop the image mid-stream; assert sink re-HELLOs and converges with no gap.
- Stale epoch: deliver an older-epoch envelope; assert it is dropped/counted.
