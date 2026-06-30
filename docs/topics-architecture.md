# RingLoom Persistent Topics — Architecture

**Status:** Design (pre-implementation).
**Audience:** RingLoom broker/service engineers and AI agents implementing the feature.
**Scope:** MPMC, persistent, broadcast publish/subscribe topics layered on RingLoom's
embedded Aeron transport and on [ringloom-queue](https://github.com/ringloom-io/ringloom-queue)
for durable storage and replication.

> Detailed, task-level implementation specs live in [`docs/topics/`](topics/00-index.md).
> This document is the cohesive narrative; the impl specs are the contracts.

---

## 1. Goals & non-goals

### 1.1 Goals

- **Persistent broadcast pub/sub.** Many producers publish to a named topic; every subscriber
  receives every message, in a single global order, and can replay history.
- **Durable.** Messages are stored on disk in a per-topic ringloom-queue and survive broker
  restarts.
- **Lowest latency / highest throughput on the read path.** Subscribers read **directly** from a
  local ringloom-queue replica via a memory-mapped tailer — zero broker involvement, zero copy.
- **Reuse existing transport.** Cross-host movement (publish to leader, leader→replica
  replication) reuses the embedded Aeron UDP transport already in the broker. Storage and
  replication reuse ringloom-queue's index-exact replication.
- **No new hot-path allocation, extern-struct wire formats**, consistent with the rest of RingLoom.

### 1.2 Non-goals (this iteration)

- No consumer groups / competing-consumers / partitioned consumption. Consumption is **broadcast**:
  each subscriber owns an independent tailer cursor over the full topic.
- No multi-master / conflict resolution. Exactly one writer (the topic leader) per topic queue,
  enforced by ringloom-queue's single-master model and by epoch fencing.
- No strong consensus (Raft/Paxos). Topic leadership reuses the broker's existing lowest-nodeId-alive
  election (restricted to topics-enabled brokers); we add **epoch fencing** plus a **failover
  catch-up barrier** (§9) for safety, but still accept bounded loss of *un-acked* data on failover.
- No quorum-durable publishes in this iteration. Two ack modes only: `fire_and_forget` (default) and
  `replicate_once` (ack after ≥1 replica applies; §8).
- No built-in encryption/auth beyond what the Aeron transport provides.

---

## 2. Design decisions (locked)

| Area | Decision | Rationale / consequence |
|---|---|---|
| Subsystem enablement | **Per-broker opt-in via `broker.topics.enabled` (default `false`).** Only topics-enabled brokers participate in topic leadership, storage, and replication. | A broker with topics disabled never stores topic queues nor stands for topic leadership. |
| Sequencer / leadership | **A dedicated TOPIC leader sequences ALL topics**, elected **among topics-enabled brokers** (lowest-nodeId-alive of that set) — **decoupled from the cluster master** (which may have topics disabled). | The cluster master need not host topics. Single topic-leader keeps a simple single-writer story. Trade-off: write bottleneck + one failover moves all topics (per-topic distribution is future work). |
| Partition behaviour | **AP + epoch fencing.** The topic leader keeps accepting; stale-leader writes are fenced by a monotonic `leader_epoch`. | Availability over consistency. Bounded loss of the *un-acked* tail on failover. No quorum. |
| Replication scope | **Full mesh.** Every topics-enabled broker holds a replica queue for **every** topic, regardless of local subscribers. | No zero-replica loss window; robust failover; any topics-enabled broker can take over. Trade-off: every topics-enabled broker stores/receives every topic. |
| Publish durability / ack | **`fire_and_forget` (default)** — no ack. **`replicate_once` (opt-in per send)** — acked once the message is applied by **≥1 replica** (on a single-node broker, acked once the leader appends). | App chooses per send. `replicate_once` survives single-node loss because acked data is on ≥1 replica and the new leader applies the catch-up barrier (§9). |
| Ack delivery | **High-water-mark feedback.** The leader sends throttled "replicated-up-to-index R (epoch E)" frames; the producer client completes all per-message ack futures with index ≤ R. | No per-message round trip; preserves throughput while giving per-message completion semantics. |
| Failover safety | **Catch-up barrier.** On leadership change, the new topic leader first syncs to the **most-advanced surviving replica** (highest applied sequence) before accepting any new writes. | Guarantees the new leader's index space is a superset of every acked message; prevents truncating acked data. |
| Consumption | **Broadcast.** Every subscriber sees every message. | Independent tailer cursors per subscriber. |
| Topic ID | **Deterministic 64-bit hash of the topic name.** | No coordination needed; every broker computes the same `topic_id`. Collisions detected via the name stored in the registry. |
| New subscriber start | **Subscriber chooses** `earliest` (full replay) or `latest` (tail from now). | Maps onto ringloom-queue tailer start index. |
| Conflicting config | **First-creation wins; config is immutable.** A later registration with mismatched config is rejected. | Stable queue geometry is mandatory for index-exact replication. |

---

## 3. System model

### 3.1 Roles per topic

For a given topic `T` with `topic_id = hash(name)`:

- **Topic leader broker** — the current **topic leader**: the lowest-nodeId alive broker **among the
  topics-enabled brokers** (a separate election from the cluster master; see
  `src/broker/topics/topic_leader_election.zig` and §9). It owns the single **master queue** for `T`
  and is the only writer to it. It sequences all publishes into a total order and runs a
  ringloom-queue **ReplicationSource** toward every other topics-enabled broker.
- **Replica broker** — **every other topics-enabled broker** (full mesh; subscribers not required).
  It owns a **replica queue** (a byte-exact follower) written **only** by a ringloom-queue
  **ReplicationSink**. Local subscribers, if any, read it via tailers.
- **Producer service** — any service that registered a publication to `T`. It sends each message to
  the **topic leader broker** (IPC if the leader is co-located, Aeron UDP otherwise).
- **Subscriber service** — any service subscribed to `T`. It reads its **local replica (or master)
  queue** directly via a ringloom-queue tailer. It never talks to the broker on the read hot path.

A broker with topics disabled does not appear in any of these roles. A single topics-enabled broker
can simultaneously be the **leader** for all topics (it is the lowest-nodeId topics-enabled broker)
or a **replica** for all topics (it is not). Both write roles are owned by one thread per process
(§6), so there is exactly one writer per queue.

### 3.2 Storage tiers

- **Service IPC** (control + message ring buffers): `broker.storage.path` (default `/dev/shm`),
  unchanged.
- **Topic queues**: new `broker.topics.path` config (persistent disk, e.g. `/var/lib/ringloom/topics`).
  Layout: `<topics.path>/<group>/node-<id>/<topic_name-or-id>/` containing a standard ringloom-queue
  directory (`metadata.ringloom` + cycle files). Both the broker (writer) and co-located subscriber
  services (tailers) mmap this directory across processes.

---

## 4. End-to-end flows

### 4.1 Register publication

```
producer service ──(control RB)──▶ local broker
  local broker ──(admin UDP, if not topic leader)──▶ TOPIC leader
    topic leader: hash(name) → topic_id
            if topic exists  → validate config matches (first_wins); else reject
            else             → create master queue with requested geometry,
                               insert registry record {topic_id,name,config,leader_epoch}
            propagate TopicCreated to all topics-enabled peers (eventually consistent registry);
            each peer eagerly opens a replica queue + sink (full mesh)
  topic leader ──▶ local broker ──(control RB)──▶ producer
            TopicPublicationResponse{topic_id, leader_node, leader_epoch, effective_config}
```

The producer caches `{topic_id, leader_node, leader_epoch}` and the topic leader's Aeron endpoint. It
refreshes these on `TopicLeaderChanged` notifications (§9).

### 4.2 Subscribe

```
subscriber service ──(control RB)──▶ local broker
  local broker:
    hash(name) → topic_id; look up topic in registry (may need to ask topic leader if unknown)
    # full mesh: a local replica (or master, if this broker is the topic leader) already exists
    # because every topics-enabled broker replicates every topic. No lazy creation needed.
    compute start_index from {earliest | latest}
  local broker ──(control RB)──▶ subscriber
        TopicSubscriptionResponse{topic_id, queue_dir, geometry, start_index}
subscriber: open ringloom-queue Tailer(queue_dir, start_index); poll in its own loop
```

The local replica may still be catching up when the response is returned; the tailer simply blocks
(`awaiting_entry`) until data arrives. `earliest` replays everything the leader retains; `latest`
starts at the current tip.

### 4.3 Publish a message

```
producer: stamp TopicPublishHeader{topic_id, leader_epoch, correlation_id, ack_mode}
          send (RingLoomDataHeader flag_topic | TopicPublishHeader | payload) to topic leader
            - leader co-located → Aeron IPC
            - leader remote     → Aeron UDP (service-side direct publication, as today)
topic-leader receiver loop: decode frame, see flag_topic
          fence: if header.leader_epoch != current_epoch(topic) → drop (stale producer)
          append payload DIRECTLY to master queue (receiver is sole writer) → assigns ringloom index
          (prefetcher thread keeps pages resident → no page-fault jitter on append)
          if ack_mode == replicate_once: record (correlation_id → index) for ack tracking
```

### 4.3a Acknowledgement (ack_mode = replicate_once)

```
multi-node: leader tracks each replica sink's applied index (from repl ACK frames, §6).
            replicated_hwm = max applied index across replicas that have applied ≥1 (≥1-replica rule)
single-node (no other topics-enabled broker): replicated_hwm = master append index (no replica to wait for)
leader → producer's local broker: throttled TopicAckFeedback{topic_id, leader_epoch, replicated_hwm}
producer client: completes every pending publish future whose assigned index ≤ replicated_hwm
```

`fire_and_forget` publishes carry no `correlation_id` ack tracking and never wait. The ack feedback
is **high-water-mark based and throttled** (not one frame per message); see
[`03-control-plane-protocol.md`](topics/03-control-plane-protocol.md) §6 and
[`04-wire-protocol-and-routing.md`](topics/04-wire-protocol-and-routing.md).

### 4.4 Replicate to subscribers

```
receiver loop (leader): for each connected replica session, ReplicationSource.step()
          tails the master queue, ships EXCERPT/EXCERPT_BATCH/CYCLE_ROLL/HEARTBEAT
          over the topic replication Aeron stream (wrapped in a TopicReplEnvelope)
receiver loop (replica): demux envelope by topic_id → ReplicationSink.step()
          applies each excerpt via writeAtIndex DIRECTLY at the master's exact index
          (replica is byte-identical to the master)
subscriber tailer: sees the new entry in the local replica queue, returns it to the app
```

### 4.5 Join / reconnect / catch-up

A topics-enabled broker that rejoins the cluster, or whose sink reconnects, simply (re)starts its
**ReplicationSink** for the topic. The sink's **HELLO** carries its `last_applied_index`; the topic
leader's source replies `HELLO_ACK` and chooses `full_replay`, `catchup`, or `live`. This is
ringloom-queue's built-in mechanism — **no bespoke replay protocol is needed** (see
[`06-replication-over-aeron.md`](topics/06-replication-over-aeron.md)). Because replication is
**full mesh**, every topics-enabled broker maintains a live sink for every topic at all times, not
only when it has a subscriber.

---

## 5. Why this maps cleanly onto ringloom-queue

ringloom-queue replication (`docs/12-replication.md` in that repo) provides exactly the primitives
this design needs:

- **Single-master, multi-follower, index-exact.** One leader writer; followers reproduce the
  master's `index` space byte-for-byte. Followers are ordinary queues readable by tailers — that is
  literally our subscriber read path.
- **Pluggable comptime transport.** `ReplicationSource`/`ReplicationSink` are generic over
  `Outbound`/`Inbound` channel types with `offer` / `poll` / `nextFrame`. We implement these over
  Aeron (§6, [`06-...`](topics/06-replication-over-aeron.md)).
- **Built-in replay modes & recovery.** `full_replay` / `catchup` / `live`, gap detection → RESET →
  re-handshake, reconnect with backoff, and "the follower never silently diverges."
- **Pollable state machines, no mandatory threads.** `step(max_work_units)` integrates into the
  receiver loop's topic engine (§6).

The one property ringloom-queue **requires from us**: the transport must be *reliable, ordered, and
in-session*; any loss must surface as a **disconnect**, never a silent gap. §6 explains how the
Aeron binding satisfies this (map image-unavailable → sink RESET/reconnect).

---

## 6. Threading & data-flow placement

The broker keeps its three loops (control, sender, receiver) and adds **one background
prefetcher/maintenance thread**. There is **no separate writer loop**: the **receiver loop appends
topic data directly** to the ringloom-queue, and jitter is eliminated by the prefetcher rather than
by offloading the append.

- **Receiver loop = the topic engine.** It already polls Aeron and is where topic frames arrive, so
  it performs all topic queue mutation:
  - **Publish frames** (leader role): decode → **append directly** to the master queue (this assigns
    the total-order index).
  - **Replication frames** (replica role): drive the `ReplicationSink`, which applies excerpts via
    `writeAtIndex` **directly** to the follower queue.
  - It also steps each leader `ReplicationSource` (tail master → `offer` excerpts to **every other
    topics-enabled broker**) and the sink/source control directions, and tracks per-replica applied
    indexes to compute the `replicate_once` ack high-water-mark (§8).

  Queue *open/close/promote/epoch* decisions come from the control loop (which owns the registry)
  via a small POD command queue. Because **only the receiver loop mutates topic queues**,
  ringloom-queue's single-writer invariant holds by construction — whether a broker is the topic
  leader (one master queue per topic, written from the publish path) or a replica (follower queues
  written from sinks).

- **Prefetcher/maintenance thread (separate).** A single broker-owned thread drives ringloom-queue
  `maintenancePoll` round-robin across all open topic queues. Per ringloom-queue's design
  (`docs/01-architecture-overview.md`, `docs/05/06`) it **pre-touches future appender pages** (write
  runway) and **read-prefetches** pages ahead of each source's tailer, and runs the cleaner/retention
  — all **off the receiver hot path**. Queues are opened with `spawn_helper_threads = false` so the
  broker owns exactly **one** maintenance thread instead of N per-queue helper threads.

**Rationale (revised):** the latency spike that would otherwise motivate a separate writer is the
**mmap page fault on write** (and the mmap syscall at cycle-roll). ringloom-queue's prefetcher
removes exactly that by faulting and pre-mapping pages ahead of the write tip, so the receiver's
per-message cost is a `memcpy` into an already-resident page plus an atomic publish — bounded and
low. If the prefetcher ever falls behind, ringloom-queue performs a synchronous fallback (reported
as a latency-profile miss), never a stall.

Aeron streams (data, admin, plus the new topic streams) are still serviced by the existing
sender/receiver agents; the receiver calls `offer`/`poll` on the Aeron-backed repl channels.

---

## 7. Identity, routing & the wire

- **`topic_id: u64 = hash(topic_name)`** (see
  [`01-topic-identity-and-config.md`](topics/01-topic-identity-and-config.md)). It is a distinct
  ID space from `service_id` (`u16`). Topic frames are **not** routed by `target_service_id`.
- **Message identity:** `(topic_id, leader_epoch, sequence)`, where `sequence` is the ringloom-queue
  index assigned by the topic leader. `leader_epoch` fences stale leaders (§9).
- **Three new logical Aeron streams** (base stream IDs added to `BrokerConfig`):
  1. **Topic publish** (producer → topic leader): `RingLoomDataHeader` with `flag_topic`, then a
     `TopicPublishHeader` (incl. `ack_mode`), then payload. Routed by `target_node_id == topic leader`.
  2. **Topic replication** (leader source ⇄ replica sink): a `TopicReplEnvelope`
     (`{target_node, source_node, topic_id, leader_epoch, direction}`) wrapping a whole
     ringloom-queue repl frame. The envelope lets the receiver demux frames to the right
     per-topic source/sink session and carries the bidirectional control direction (HELLO/ACK/RESET).
     The sink→source ACK direction also carries each replica's `applied_index`, which the leader uses
     for `replicate_once` ack accounting (§8).
  3. Topic **metadata + ack feedback** reuse the existing **admin** stream with new admin templates
     (`TopicCreated`, `TopicConfigChanged` (rejected under first_wins, reserved), `TopicLeaderChanged`,
     and throttled `TopicAckFeedback{topic_id, leader_epoch, replicated_hwm}`).

Full byte layouts: [`04-wire-protocol-and-routing.md`](topics/04-wire-protocol-and-routing.md).

---

## 8. Durability & acknowledgement

Two ack modes, chosen **per publish** by the producer:

- **`fire_and_forget` (default):** the producer does not wait. The send is "accepted by the leader's
  transport." No `correlation_id` ack tracking; highest throughput.
- **`replicate_once` (opt-in):** the publish completes once the message is **applied by ≥1 replica**.
  - On a **multi-node** topics-enabled cluster: the leader watches each replica sink's applied index
    (carried on ringloom-queue repl ACK frames, §6) and treats the message as acked once **any one**
    replica's applied index reaches it. Acked data therefore exists on **≥2 nodes** (leader + ≥1
    replica) and survives a single-node failure.
  - On a **single-node** topics-enabled broker (no other topics-enabled peer to replicate to): there
    is no replica to wait for, so the message is acked once the **leader appends** it. This is the
    documented single-node carve-out.

**Ack delivery is high-water-mark feedback, throttled** — *not* one frame per message. The leader
periodically emits `TopicAckFeedback{topic_id, leader_epoch, replicated_hwm}` toward brokers with
local producers; each producer client completes every pending publish future whose assigned index is
`≤ replicated_hwm`. This preserves throughput while giving the application per-message completion
semantics. See [`03-...`](topics/03-control-plane-protocol.md) §6 and
[`04-...`](topics/04-wire-protocol-and-routing.md).

- **Loss windows:**
  - `fire_and_forget` (or the un-acked tip of any topic) appended on a leader that dies before any
    replica applied it is **lost**. Fenced by epoch so it can never resurface and diverge.
  - `replicate_once`-**acked** data is **not** lost on a single-node failure: it is on ≥1 surviving
    replica, and the new leader's **catch-up barrier** (§9) syncs to the most-advanced replica before
    accepting writes, so the acked prefix is preserved.
  - Because replication is **full mesh**, there is **no zero-replica total-loss window** in a
    multi-node topics-enabled cluster (every topics-enabled broker replicates every topic).

If stronger durability is later required, the path is `quorum_durable` acks (ack after a majority of
replicas apply) and/or a configurable min-ack-replica count — both noted as future work in
[`08-failover-and-epoch-fencing.md`](topics/08-failover-and-epoch-fencing.md).

---

## 9. Leadership, epochs & failover

- **Topic leadership is a separate election from the cluster master.** It runs **only among
  topics-enabled brokers**, using the same lowest-nodeId-alive rule (`topic_leader_election.zig`,
  built on the existing membership/heartbeat signal; see §11). The cluster master may have topics
  disabled and never stands for topic leadership.
- **`leader_epoch`** is a monotonically increasing term. It advances on every topic-leadership
  transition and is piggybacked on broker heartbeats / topic admin frames so all topics-enabled nodes
  converge on the current term. Each topic tracks the epoch under which its current tip was written.
- **Fencing.** Every topic-publish frame and every replication envelope carries `leader_epoch`:
  - A replica sink ignores frames whose epoch is **older** than the highest epoch it has seen.
  - The new leader stamps its (higher) epoch; producers learn it via `TopicLeaderChanged` /
    refreshed publication responses and re-stamp subsequent sends. Stale-leader and stale-producer
    frames are dropped.
- **Failover sequence** (topics-enabled broker N becomes the new topic leader):
  1. N detects it is the new topic leader (topic election among topics-enabled brokers).
  2. **Catch-up barrier — before accepting ANY new writes:** N queries every surviving topics-enabled
     peer for its `last_applied_index` per topic, identifies the **most-advanced replica** (highest
     applied sequence), and runs a sink to pull from it until N's queue reaches that tip. N already
     holds a full-mesh replica of every topic, so this is normally a short `catchup`, not a
     `full_replay`. N does this for **every** topic before opening any of them for append.
  3. Only after a topic is fully caught up does N bump its `leader_epoch`, open the queue for append,
     and begin accepting publishes + serving sources for that topic.
  4. Producers receive `TopicLeaderChanged`, re-target N, and stamp subsequent sends with the new
     epoch. Other replicas tear down their old sink session and re-HELLO against N.

This guarantees the new leader's accepted index space is a **superset of every acked message**: no
acked `replicate_once` publish is ever truncated. Only the un-acked tip beyond the most-advanced
replica may be lost (bounded, fenced).

Detailed state machine, edge cases (split-brain heal, sink-ahead, partial rebuild):
[`08-failover-and-epoch-fencing.md`](topics/08-failover-and-epoch-fencing.md).

---

## 10. Configuration (additions to `BrokerConfig`)

| Key | Default | Meaning |
|---|---|---|
| `broker.topics.enabled` | `false` | Per-broker master switch. Only enabled brokers store topic queues, replicate, and stand for topic leadership. |
| `broker.topics.path` | `<storage_path>/topics` | Root dir for persistent topic queues. |
| `broker.topics.default_roll_scheme` | `FAST_DAILY` | Default ringloom-queue roll scheme for new topics. |
| `broker.topics.default_retention_cycles` | unset (keep all) | Default `retention_cycles`. |
| `broker.topics.max_topics` | `1024` | Registry capacity. |
| `broker.topics.repl_stream_base` | `40000` | Aeron stream base for topic replication. |
| `broker.topics.pub_stream_base` | `50000` | Aeron stream base for topic publish. |
| `broker.topics.ack_feedback_interval_us` | `200` | Throttle interval for `TopicAckFeedback` HWM frames (`replicate_once`). |
| `broker.topics.prefetcher_cpu_affinity` | unset | Optional pin for the prefetcher/maintenance thread. |
| `broker.topics.write_runway_bytes` | `8 MiB` | ringloom-queue appender prefetch runway (write pre-touch). |
| `broker.topics.read_runway_bytes` | `4 MiB` | ringloom-queue source-tailer read-prefetch runway. |

> Replication is **full mesh** and is not configurable per-topic in this iteration: every
> topics-enabled broker replicates every topic. There is no `sub_only`/replication-factor knob.

Per-topic config (carried in the registration request, immutable after first creation): roll scheme,
`retention_cycles`, optional huge pages. (Ack mode is **per publish**, not per topic.)

---

## 11. Component / module layout (new code)

```
src/broker/topics/
  topic_id.zig            ── hash(name) → topic_id, collision check
  topic_registry.zig      ── cluster-replicated topic metadata (topic_id → record)
  topic_config.zig        ── per-topic queue geometry / retention
  topic_leader_election.zig ── topic-leader election among TOPICS-ENABLED brokers (lowest-nodeId-alive)
  topic_engine.zig        ── receiver-loop topic engine: direct append/apply + source/sink stepping + epoch fencing + ack HWM tracking
  topic_prefetcher.zig    ── single background maintenance thread: maintenancePoll (page pre-touch, cleaner, retention) for all queues
  topic_store.zig         ── opens/owns ringloom-queue master & replica queues (spawn_helper_threads=false)
  topic_commands.zig      ── POD command queue: control loop → receiver engine (open/close/promote/set-epoch/catch-up-barrier)
  repl_aeron_transport.zig── Aeron Outbound/Inbound channels for source/sink
  repl_session.zig        ── per-(topic,peer) source/sink session bookkeeping + envelope demux
  topic_messages.zig      ── control-plane topic message codecs (service<->broker), incl. ack feedback
  topic_admin.zig         ── admin-plane topic message codecs (broker<->broker)
src/common/message/
  topic_data_header.zig   ── TopicPublishHeader + TopicReplEnvelope wire formats
src/service/topics/
  topic_publisher.zig     ── producer-side: register + publish to leader + per-message ack futures
  topic_subscription.zig  ── subscriber-side: wraps a ringloom-queue Tailer
```

C ABI additions (`include/ringloom_service.h`, `src/service/c_abi.zig`):
`ringloom_register_topic_publication`, `ringloom_publish_to_topic`, `ringloom_subscribe_topic`,
`ringloom_topic_poll` (+ unsubscribe / unregister). See
[`09-service-client-api-and-tailer.md`](topics/09-service-client-api-and-tailer.md).

---

## 12. Observability

New counters and a `ringloom-stat` view: per-topic leader/replica role, repl lag (sink
`lag_from_source_hwm`), `frames_sent/applied`, `gaps_detected`, `hello_nacks`, publish
accept/fence drops, master HWM index, replica `last_applied_index`, active sessions. See
[`10-observability-and-metrics.md`](topics/10-observability-and-metrics.md).

---

## 13. Risks & open questions

| Risk | Mitigation / status |
|---|---|
| Split-brain double-writer corrupts a master queue | Epoch fencing + single-writer receiver loop + failover catch-up barrier; replicas reject older-epoch frames (§9). |
| Single topic-leader is a write bottleneck for all topics | Accepted (locked decision). Per-topic leadership distribution is the documented future scaling path. |
| Full-mesh replication cost (every topics-enabled broker stores/receives every topic) | Accepted trade-off for robust failover + no zero-replica loss window (§2, §8). Bound participation via `broker.topics.enabled`. |
| Un-acked tail lost on failover | Bounded & fenced; `replicate_once` acked data is preserved by the catch-up barrier (§8, §9). Future: `quorum_durable`. |
| Aeron image loss appears as a silent gap to the sink | Map image-unavailable → RESET/reconnect; the sink re-HELLOs and the source replays (§6). |
| Hash collision on `topic_id` | Registry stores the name; a colliding distinct name is rejected at registration (§7, `01-...`). |
| Topic disk I/O jitter on the receiver hot path | ringloom-queue prefetcher pre-touches write/read pages off-thread; the receiver append is a `memcpy` into resident pages + atomic publish (§6). |

---

## 14. Reading order for implementers

1. [`00-index.md`](topics/00-index.md) — task map & dependencies
2. `01` identity/config → `02` registry/propagation → `03` control protocol → `04` wire/routing
3. `05` leader append → `06` replication-over-Aeron → `07` replica/subscription → `08` failover
4. `09` service API → `10` observability → `11` testing
