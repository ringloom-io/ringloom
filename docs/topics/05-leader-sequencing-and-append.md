# 05 — Leader Sequencing & Append

**Goal:** Turn the stream of incoming publish frames into a single totally-ordered, durable log per
topic on the leader, appending **directly on the receiver loop**, with mmap jitter eliminated by a
separate prefetcher thread.
**Modules:** `src/broker/topics/topic_engine.zig`, `topic_store.zig`, `topic_prefetcher.zig`.
**Depends on:** 02, 04.

---

## 1. The receiver-loop topic engine

There is **no dedicated writer thread**. The **receiver loop** is the topic engine and the **sole
writer of every topic queue on this process** (master queues when leader, replica queues when
follower). The engine is a sub-component the receiver loop steps each iteration:

```zig
pub const TopicEngine = struct {
    store: TopicStore,                 // owns open Queue/Appender/Tailer per topic
    repl: ReplHub,                     // sources + sinks (spec 06)
    ctrl_in: *CmdQueue,                // commands from control loop (spec 02 §4)
    ctrl_out: *CmdQueue,               // status to control loop

    /// Called from ReceiverEventLoop.doWork(), in addition to the existing
    /// service-data / admin polling.
    pub fn step(self: *TopicEngine) u32 {
        var w: u32 = 0;
        w += self.applyControlCommands();   // open/close queues, set epoch, promote (spec 02/08)
        w += self.repl.stepAll(BUDGET);     // leader sources (ship) + replica sinks (apply)
        return w;                            // publish appends happen in onPublish() (below)
    }

    /// Invoked directly from the receiver demux for each topic-publish frame (spec 04 §4).
    pub fn onPublish(self: *TopicEngine, item: PublishView) void { ... } // §3
};
```

`maintenancePoll` is **not** called here — it runs on the separate prefetcher thread (§4). The
receiver loop keeps its existing service-message routing path; the topic engine adds bounded work
(append + repl stepping) on the same thread.

## 2. TopicStore

Wraps ringloom-queue per topic. Queues are opened with **`spawn_helper_threads = false`** so the
broker (not ringloom-queue) owns the single maintenance thread (§4).

```zig
const rq = @import("ringloom_queue");

pub const TopicQueue = struct {
    topic_id: TopicId,
    role: enum { leader, replica },
    queue: rq.Queue(RawBytes),     // raw-bytes codec: payload stored verbatim
    appender: ?rq.Appender,        // leader: normal append; replica: writeAtIndex via sink
    epoch: u64,                    // current leader_epoch for this topic
    hwm_index: u64,                // master append HWM
    replicated_hwm: u64,           // leader: highest index applied by ≥1 replica (ack accounting)
    accepting_writes: bool,        // leader: false until the failover catch-up barrier clears (spec 08)
};
```

- **Leader master queue:** opened `create=true` with the topic's `RollScheme`
  (`config.roll_scheme_name`) + `retention_cycles`, `enable_prefetcher=true`,
  `spawn_helper_threads=false`. Appended via the normal append path **on the receiver thread**. The
  ringloom-queue **ReplicationSource** is bound to it (it only *tails*, never writes) — spec 06.
- **Replica queue:** opened with the **same** roll scheme/geometry (mandatory for index-exact
  replication) and the same prefetch settings. Written **only** by the **ReplicationSink** through
  `writeAtIndex` (also on the receiver thread, when the engine steps the sink). The engine never
  calls `append` on a replica.

The raw-bytes codec stores the application payload exactly. Optionally prepend a small fixed
**stored record header** `{correlation_id, source_node, source_service, leader_epoch}` so subscribers
get provenance; if so, that header is part of the replicated bytes and identical on all replicas.
Decide once and keep it stable (it affects the on-disk format / replica parity).

## 3. Direct append path (leader), invoked from the receiver demux

`onPublish` is called inline by the receiver demux (spec 04 §4) — no SPSC handoff. The Aeron fragment
buffer is borrowed; the payload is consumed/copied into the queue by `append` before returning.

```
onPublish(view = {topic_id, leader_epoch, correlation_id, source_node, source_service, ack_mode, payload}):
    tq = store.get(view.topic_id) orelse { count topic_publish_dropped_unknown; return }
    if tq.role != leader:        { count topic_publish_dropped_not_leader; return }  # not the sequencer
    if not tq.accepting_writes:  { count topic_publish_dropped_barrier; return }     # catch-up barrier (spec 08)
    if view.leader_epoch != tq.epoch: { count topic_publish_dropped_stale_epoch; return }  # fencing (spec 08)
    idx = tq.append(payload)     # ringloom assigns (cycle<<32|seq) — TOTAL ORDER, durable on leader now
    tq.hwm_index = idx
    count topic_publish_accepted
    if view.ack_mode == replicate_once:
        # remember (source_node) interest so ack feedback is addressed; the actual ack fires later
        # when replicated_hwm advances (multi-node) or immediately for a single-node broker.
        ackTracker.note(view.topic_id, view.source_node, idx)
        if store.replicaPeerCount(view.topic_id) == 0:   # single-node broker carve-out
            tq.replicated_hwm = idx                      # nothing to replicate to → acked on append
```

### 3.1 Ack high-water-mark (`replicate_once`)

The leader advances `tq.replicated_hwm` from replica ACK frames (spec 06 carries each sink's
`applied_index`): `replicated_hwm = max index that ≥1 replica has applied`. A throttled task
(interval `broker.topics.ack_feedback_interval_us`) emits `TopicAckFeedback{topic_id, epoch,
replicated_hwm}` to brokers noted in `ackTracker` (spec 03 §6). On a single-node broker there is no
replica, so `replicated_hwm` tracks `hwm_index` directly (acked on append).

The total order is exactly the order the single-threaded **receiver loop** appends — i.e. Aeron's
per-image arrival order across producers. Multiple producers interleave by arrival; within one
producer order is preserved (single Aeron publication, FIFO). Documented guarantee: **a single
global order defined by leader arrival; no cross-producer causal ordering**.

Because the append runs on the receiver thread, the **prefetcher thread (§4) keeps the next pages
resident** so `append` does not page-fault; the per-message cost is a `memcpy` into a hot page plus
the atomic publish.

## 4. Prefetcher / maintenance thread (separate)

A single broker-owned thread (`topic_prefetcher.zig`) is the *only* extra thread topics introduce. It
round-robins ringloom-queue `maintenancePoll(budget)` over every open topic queue:

```zig
pub fn run(self: *TopicPrefetcher) void {
    while (self.running.load(.acquire)) {
        var did: u32 = 0;
        for (self.store.openQueues()) |q| did += try q.maintenancePoll(BUDGET);
        if (did == 0) self.idle.idle();   // backoff
    }
}
```

`maintenancePoll` performs (per ringloom-queue `docs/01` and `docs/05/06`):

- **Write pre-touch / pre-map:** prepares the appender's write runway
  (`prefetch_runway_bytes = broker.topics.write_runway_bytes`) — pre-creates the next cycle file,
  `fallocate`s, maps, and write-touches future pages so the receiver's `append`/`writeAtIndex` hits
  resident pages and the cycle-roll is a pointer swap (no mmap syscall on the hot path).
- **Read prefetch:** prepares each leader source-tailer's read runway
  (`read_prefetch_runway_bytes = broker.topics.read_runway_bytes`), bounded by published positions,
  so shipping excerpts does not read-fault.
- **Cleaner / retention:** unmaps stale windows, drops old page-cache, deletes retained cycles.

The thread touches mmap pages and metadata only; it **never mutates queue contents** (no append /
writeAtIndex), so it does not violate the single-writer invariant. Optional CPU pin via
`broker.topics.prefetcher_cpu_affinity`. If the prefetcher ever lags, ringloom-queue's appender does
a synchronous fallback (a reported latency-profile miss), not a stall.

## 5. Backpressure & retention

- A slow/disconnected replica only backs up that replica's **source session** (spec 06); the master
  append path is unaffected.
- The master queue grows per `retention_cycles`. If a replica falls so far behind that its
  `last_applied_index` is no longer retained, its sink gets `HELLO_NACK(index_unavailable)` and must
  `full_replay` from the new first-available index — meaning that replica (and its subscribers) skip
  the reclaimed range. Operators size retention vs. expected replica downtime.

## 6. Tests

- Single-writer order: many producers → strictly increasing index sequence; replaying the queue
  reproduces append (arrival) order.
- Epoch fence: a publish with `leader_epoch != tq.epoch` is dropped and counted.
- Not-leader: a topic whose local role is `replica` rejects `onPublish`.
- Catch-up barrier: a leader topic with `accepting_writes == false` rejects `onPublish` until the
  barrier clears (spec 08).
- `replicate_once` ack HWM: single-node broker acks on append; multi-node advances `replicated_hwm`
  only after a replica ACK reports the index applied; `fire_and_forget` is never tracked.
- Prefetcher: with the prefetcher running, sustained append takes no page faults on the hot path
  (assert via the latency-miss counter staying ~0); with it paused, fallbacks are reported (not
  errors). Cycle-roll under load does not stall.
- `maintenancePoll` never writes queue contents (replica byte-parity unaffected by running it).
