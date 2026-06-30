# 10 — Observability & Metrics

**Goal:** Expose topic health (roles, replication lag, publish accept/drop, session state) through
the existing counters + `ringloom-stat` / `ringloom-observability` tooling.
**Modules:** counters in `topic_engine.zig`/`repl_session.zig`; views in `tools/ringloom_stat.zig`,
`tools/ringloom_observability.zig`.
**Depends on:** 05, 06.

---

## 1. Broker counters (via `CountersManager`, like `ReceiverCounters`)

Process-wide (allocate in the receiver engine init, mirror `ReceiverCounters.allocate`):

| Counter | Meaning |
|---|---|
| `topic_publish_accepted` | publish frames appended to a master |
| `topic_publish_dropped_unknown` | publish for unknown/closed topic |
| `topic_publish_dropped_not_leader` | publish received where local role ≠ topic leader |
| `topic_publish_dropped_stale_epoch` | fenced by epoch (spec 08) |
| `topic_publish_dropped_barrier` | dropped because the failover catch-up barrier is not yet clear (spec 08) |
| `topic_append_page_faults` | append hit a non-resident page (prefetcher fell behind → jitter) |
| `topic_acks_completed` | `replicate_once` publishes confirmed replicated (HWM advanced) |
| `topic_ack_feedback_frames` | throttled `TopicAckFeedback` frames emitted |
| `topic_leader_changes` | topic-leadership transitions observed locally |
| `topic_barrier_catchup_nanos` | cumulative time spent in the failover catch-up barrier |
| `topic_repl_frames_sent` / `topic_repl_frames_applied` | source / sink throughput |
| `topic_repl_bytes_sent` / `applied` | byte throughput |
| `topic_repl_gaps_detected` / `hello_nacks` / `resets` | replication health |
| `topic_repl_backpressure_nanos` | cumulative source backpressure |
| `topic_sessions_source` / `topic_sessions_sink` | active session counts |
| `topic_masters_open` / `topic_replicas_open` | local queue counts |
| `topic_is_leader` | 1 if this broker is the current topic leader, else 0 |

## 2. Per-topic metrics

The receiver engine maintains a per-topic snapshot (lock-free double-buffer or atomics, read by
out-of-process tools), one row per local topic:

```zig
pub const TopicStat = extern struct {
    topic_id: u64,
    leader_node_id: u16,        // current TOPIC leader (spec 08)
    role: u8,            // 0 none,1 leader,2 replica
    accepting_writes: u8,       // leader: 0 while catch-up barrier pending (spec 08)
    leader_epoch: u64,
    master_hwm_index: u64,      // leader: append HWM
    replicated_hwm_index: u64,  // leader: highest index applied by ≥1 replica (replicate_once ack)
    replica_applied_index: u64, // replica: sink last_applied_index
    source_hwm_index: u64,      // replica: leader HWM as reported by HEARTBEAT
    lag_index: u64,             // replica: source_hwm - replica_applied (replication lag)
    subscriber_count: u32,
    name: [64]u8,
};
```

Published to a shared-memory stats region (reuse the existing monitoring layout pattern) so
`ringloom-stat` reads it without touching the broker's hot path.

## 3. Tooling

- **`ringloom-stat`**: add a `--topics` view listing each local topic's role, epoch, HWM,
  replica lag, subscriber count, and session counts. Color/threshold on rising lag, gaps, nacks
  (high lag ⇒ slow replica/transport; rising resets ⇒ unreliable transport per ringloom-queue §9).
- **`ringloom-observability`**: add `--topics-path <dir>` to inspect on-disk topic queues directly
  (queue geometry, first/last index, cycle files, retention) using ringloom-queue's own diagnostics
  — independent of a running broker, for post-mortem.

## 4. Logging

Scoped loggers per component: `std.log.scoped(.topic_engine)`, `.topic_prefetcher`, `.topic_repl`, `.topic_registry`.
Log (info) topic create/open/close, leader changes, sink/source session up/down, HELLO_NACK reasons;
(warn) append page-faults, fencing drops, sink-ahead resets; (debug, compiled out in release) per-step
detail.

## 5. Tests

- Counters increment on accept/drop/fence paths (assert exact deltas, as in receiver tests).
- `TopicStat` is fixed-size; snapshot read reflects appends/applies; `lag_index` computed correctly.
- `ringloom-stat --topics` renders a known fixture region.
