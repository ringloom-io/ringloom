# 11 — Testing Plan

**Goal:** Validate correctness (ordering, durability, replication parity, failover) and performance,
using the existing test infrastructure.
**Depends on:** all prior specs.

> Build/test commands (repo memory): `zig build test` (unit+integration, musl Aeron targets),
> `zig build e2e` (multi-process), `zig build perf` (ReleaseFast benches), `zig build test-bins`.
> Bindings: `zig build test-java|test-node|test-cpp`.

---

## 1. Unit tests (inline `test` blocks)

Per spec, co-located with the module:

- **01** topic id stability/distinctness; `TopicConfig` size (no ack_mode); eql geometry.
- **02** registry insert/lookup/remove + name ownership; admin template round-trips (incl.
  ack-feedback + applied-query/reply); eager-replica-on-TopicCreated; command POD.
- **03** control message encode/decode + size asserts (templates 7–15); register/subscribe handler
  logic; `replicate_once` ack-feedback fan-out (single-thread, mocked registry + command queue).
- **04** `TopicPublishHeader`(incl. ack_mode)/`TopicReplEnvelope` round-trips; `flag_topic`
  validation; demux routes topic vs service frames correctly; misdirected drops.
- **05** single-writer order; epoch fence; not-leader reject; catch-up-barrier reject; direct-append
  correctness; `replicate_once` HWM (single-node acks on append); prefetcher keeps pages resident.
- **06** **loopback replication parity** (source+sink in-process over in-memory channels wrapped in
  envelopes) — full_replay, catchup, live, backpressure, disconnect/reconnect, stale-epoch; full-mesh
  replica without a subscriber; ACK `applied_index` advances leader `replicated_hwm`.
- **08** topic election restricted to topics-enabled set (topics-disabled lower-nodeId not elected);
  term monotonicity; **catch-up barrier** (sync to max replica before accepting writes); barrier
  preserves acked data; promote-in-place index continuity; sink-ahead reset.

## 2. End-to-end tests (`src/e2e/`, `TestHarness`)

Multi-process, spawning real brokers + test services. Each: `init` → `errdefer markFailed()` →
`deinit`, `startBroker` / `waitForBrokerReady`, `startService` / `waitForServiceReady`.

| Scenario | Asserts |
|---|---|
| `topic_disabled_broker` | a broker with `topics.enabled=false` rejects register/subscribe (status=disabled) and stores no queues. |
| `topic_single_node_pubsub` | 1 broker, publisher + subscriber: subscriber reads all N in order; `replicate_once` acks on append. |
| `topic_multi_producer_order` | M producers → single global order; subscriber sees a total order; per-producer FIFO preserved. |
| `topic_full_mesh_replication` | 3 topics-enabled brokers; producer + subscriber on different nodes; every node's replica receives all N via Aeron repl, byte-exact, even nodes with **no** subscriber. |
| `topic_replicate_once_ack` | `replicate_once` publish future completes only after ≥1 replica applies; HWM feedback throttled. |
| `topic_topic_leader_not_cluster_master` | cluster master has `topics.enabled=false`; topic leader is a different (lowest-nodeId topics-enabled) broker; pub/sub works. |
| `topic_late_subscriber_earliest` | subscribe `earliest` after K messages → reads all K via full_replay/catchup. |
| `topic_late_subscriber_latest` | subscribe `latest` after K → reads only subsequent messages. |
| `topic_replica_reconnect` | kill/restart a replica broker mid-stream → catchup resumes, no gap, final parity. |
| `topic_leader_failover_catchup_barrier` | kill topic leader; new leader syncs to the most-advanced replica BEFORE accepting writes; all `replicate_once`-acked messages survive; subscribers continue. |
| `topic_first_wins_config` | second registration with mismatched config rejected; identical idempotent. |
| `topic_unsubscribe_keeps_replica` | unsubscribe does **not** tear down the replica (full mesh); it keeps following the leader. |

`markFailed()` preserves `/tmp/ringloom-e2e-{scenario}-{seq}/` with configs, logs, and topic queue
dirs for debugging.

## 3. Fault-injection / soak

- Backpressured replica (slow-consumer style service) → leader source holds frames, no gaps; lag
  counter rises then recovers.
- Aeron image drop (kill/restart driver or partition) → sink RESET/reconnect; converge to parity.
- Retention reclaim while a replica is behind → `HELLO_NACK(index_unavailable)` → full_replay from
  new first-available (documented skip).

## 4. Performance (`src/perf/`, ReleaseFast)

- `topic_publish_bench`: producer→leader direct-append throughput/latency (IPC and UDP leader),
  measuring append-jitter with the prefetcher on vs off.
- `topic_replication_bench`: leader→replica end-to-end (publish→subscriber-visible) latency and
  sustained throughput; report replication lag distribution. Reuse the `scripts/bench-topic.sh`
  style harness if/when present; otherwise add one. Write JSON under
  `/tmp/ringloom-perf-results/topics/` (matches existing perf-output convention).
- Compare topic broadcast fan-out (1→K subscribers on one node) cost vs single subscriber.

## 5. Acceptance criteria

- All unit + e2e scenarios green under `zig build test` and `zig build e2e`.
- Replica byte-for-byte parity with leader for every replicated scenario (tailer-read equality).
- No silent gaps in any fault-injection run (gaps surface as RESET+reconvergence only).
- Documented loss windows reproduced exactly (no *more* loss than specified) in failover scenarios.
- Bindings smoke tests pass for the new topic APIs.
