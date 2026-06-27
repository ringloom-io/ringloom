# 02 — Topic Registry & Metadata Propagation

**Goal:** A cluster-replicated map of topic metadata so any broker can answer subscription requests
and a new leader knows which topics exist.
**Modules:** `src/broker/topics/topic_registry.zig`, `src/broker/topics/topic_admin.zig`.
**Depends on:** 01.

---

## 1. Registry record

```zig
pub const TopicRecord = struct {
    topic_id: TopicId,
    name: []const u8,          // owned (duped), like ServiceRegistry names
    config: TopicConfig,       // immutable (spec 01)
    leader_node_id: u8,        // current TOPIC leader (sequencer); see spec 08
    leader_epoch: u64,         // term under which the current tip was written (spec 08)
    created_ns: i64,
    // local-only fields (not propagated):
    local_role: enum { none, leader, replica },
    local_queue_open: bool,    // full mesh: open on every topics-enabled broker
    local_subscriber_count: u32, // informational only (does NOT gate replica creation)
};

pub const TopicRegistry = struct {
    by_id: std.AutoHashMap(TopicId, TopicRecord),
    by_name: std.StringHashMap(TopicId),
    allocator: std.mem.Allocator,
    // Owned by the control thread (like ServiceRegistry) — no locking.
};
```

Ownership and lifetime mirror `src/broker/control/service_registry.zig` (own the name slice;
free on remove). The registry lives on the **control loop** (the metadata authority); the receiver
engine reads an immutable snapshot of the fields it needs (topic_id, config, role, epoch) via a
small lock-free handoff or by being told through a command queue (see §4).

## 2. Authority & consistency model (AP)

- Topic **creation** is performed by the **topic leader** only (the lowest-nodeId topics-enabled
  broker, spec 08 — **not** necessarily the cluster master). It assigns
  `leader_node_id = self`, `leader_epoch = current term`, then **propagates** `TopicCreated` to all
  topics-enabled peers over the **admin** stream.
- On receiving `TopicCreated`, **every topics-enabled peer eagerly opens a local replica queue + sink**
  for the topic (full mesh) — it does **not** wait for a local subscriber. Brokers with topics
  disabled ignore topic admin frames.
- Non-leader brokers also learn topics on-demand via **`TopicLookup`/`TopicInfo`** to the topic leader
  when a local subscribe arrives for a still-unknown topic (avoids waiting for the broadcast).
- This is **eventually consistent** (no quorum). Under a partition both sides may believe different
  things; epoch fencing + the failover catch-up barrier (spec 08) prevent *data* divergence even if
  *metadata* races. First-wins + deterministic `topic_id` make concurrent creations of the same name
  converge (same id, same config or one is rejected).

## 3. Admin messages (broker↔broker, over the existing admin UDP stream)

Add templates to `src/broker/cluster/admin_messages.zig` (follow existing `TEMPLATE_*` + extern body
pattern, fixed-size, `padServiceName`-style name padding):

| Template | Body | Direction | Purpose |
|---|---|---|---|
| `TEMPLATE_TOPIC_CREATED` | `topic_id, leader_node, leader_epoch, config, name[64]` | leader → all | Register/refresh a topic everywhere (each topics-enabled peer eagerly opens a replica). |
| `TEMPLATE_TOPIC_LOOKUP` | `topic_id` | any → topic leader | "Tell me about this topic." |
| `TEMPLATE_TOPIC_INFO` | same as CREATED + `exists:u8` | topic leader → requester | Reply to lookup. |
| `TEMPLATE_TOPIC_LEADER_CHANGED` | `topic_id(0=all), new_leader, leader_epoch` | new topic leader → all | Failover announcement (spec 08). |
| `TEMPLATE_TOPIC_APPLIED_QUERY` | `topic_id(0=all)` | new topic leader → topics-enabled peers | Catch-up barrier: ask each peer's `last_applied_index` (spec 08). |
| `TEMPLATE_TOPIC_APPLIED_REPLY` | `topic_id, node, last_applied_index, leader_epoch` | peer → new topic leader | Reply used to pick the most-advanced replica (spec 08). |
| `TEMPLATE_TOPIC_ACK_FEEDBACK` | `topic_id, leader_epoch, replicated_hwm` | topic leader → brokers with local producers | Throttled HWM ack for `replicate_once` (spec 04 §, spec 03 §6). |

`topic_id = 0` in `TOPIC_LEADER_CHANGED` / `TOPIC_APPLIED_QUERY` means "applies to all topics" (used
on topic-leader failover, since the single topic leader owns all topics).

Routing/decoding reuses `admin_messages.decodeAeronAdminFrame` + `admin_dispatch.dispatchAdminMessage`
in the receiver loop; new templates enqueue control commands on the existing `AdminCommandQueue`.

## 4. Control-loop → receiver-engine command channel

The control loop owns the registry; the **receiver loop (topic engine, spec 05)** owns the queues
and is their sole writer. They communicate via a small bounded SPSC command queue (one per
direction):

- control → engine: `OpenMaster{topic_id, config}`, `OpenReplica{topic_id, config}` (eager, on
  `TopicCreated`), `CloseQueue{topic_id}`, `SetEpoch{topic_id, epoch}`,
  `StartSink{topic_id, leader_node}`, `CatchUpBarrier{topic_id, from_node, target_index}` then
  `PromoteToLeader{topic_id}` (spec 08).
- engine → control: `MasterHwm{topic_id, index}`, `ReplicaApplied{topic_id, index}`,
  `ReplicatedHwm{topic_id, index}` (min-1-replica HWM for ack feedback, spec 04),
  `SessionState{topic_id, peer, state}` (for observability + barrier completion).

Commands carry only POD (ids, config copies) — no borrowed slices — so no cross-thread lifetime
hazards. The separate prefetcher thread (spec 05) only ever calls `maintenancePoll` on queues the
engine has opened; it never mutates queue contents.

## 5. Persistence

The registry is rebuilt on restart from: (a) admin broadcasts after rejoin, and (b) scanning
`broker.topics.path` for existing queue directories (each dir name encodes the topic; its
`metadata.ringloom` gives geometry). The leader re-announces `TopicCreated` for every topic it
re-opens. No separate registry file is mandated this iteration (queue dirs are the durable source of
truth for *existence*; epoch is re-derived per spec 08).

## 6. Tests

- Create → `TopicCreated` encode/decode round-trip; registry insert by id and name.
- Lookup miss → `TopicLookup` emitted; `TopicInfo(exists=0)` handled.
- First-wins: second create with mismatched config rejected; identical config idempotent.
- Collision: same id, different name rejected.
- Command-channel POD round-trips; capacity/backpressure behaviour.
