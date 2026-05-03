# Flow Control Improvements: Client-Side Remaining-Bytes Backpressure

**Status:** Design Proposal
**Depends on:** [architecture.md](architecture.md)

---

## Table of Contents

1. [Motivation](#1-motivation)
2. [Design Principles](#2-design-principles)
3. [Five Flow-Controlled Paths](#3-five-flow-controlled-paths)
4. [Remaining-Bytes Visibility by Path](#4-remaining-bytes-visibility-by-path)
5. [Flow Control Counters Region (Shared Memory)](#5-flow-control-counters-region-shared-memory)
6. [Per-Peer Send Counters Region (Shared Memory)](#6-per-peer-send-counters-region-shared-memory)
7. [Service Registry & Discovery Changes](#7-service-registry--discovery-changes)
8. [ServiceClient Flow Control](#8-serviceclient-flow-control)
9. [Inter-Broker Counter Propagation](#9-inter-broker-counter-propagation)
10. [Broker Implementation Changes](#10-broker-implementation-changes)
11. [Configuration](#11-configuration)
12. [New Counters & Monitoring](#12-new-counters--monitoring)
13. [Edge Cases & Staleness Analysis](#13-edge-cases--staleness-analysis)
14. [Migration & Backward Compatibility](#14-migration--backward-compatibility)

---

## 1. Motivation

The current backpressure model is **reactive**: messages are dropped when a target service's ring buffer is full, and a counter is incremented. The sending service has no visibility into the target's remaining capacity until the write fails. This leads to:

- **Wasted work:** Messages are serialized, routed through the send ring buffer, transmitted over TCP, and parsed by the remote broker — only to be dropped at the final step.
- **No sender-side adaptation:** Service clients cannot throttle or shed load proactively.
- **Silent data loss:** The sender is not notified of drops on the cross-host path.

This design introduces **client-side flow control** where service clients know the remaining bytes in each target service's buffer (and the broker's send ring buffer) and can perform **proactive backpressure** before writes are attempted.

---

## 2. Design Principles

1. **Advisory, not guaranteed.** Remaining-bytes counters are best-effort estimates. The system still handles `BufferFull` at actual write points. Counters reduce drops but cannot eliminate them entirely (due to races and propagation delays).

2. **Unified client API, path-specific data sources.** The `ServiceClient` exposes a single `remainingBytes()` API regardless of whether the target is local or remote. Under the hood, local paths read ring buffer positions directly from shared memory (exact, no staleness). Remote paths read propagated counters (advisory, with bounded staleness).

3. **Minimal hot-path overhead.** Local flow control adds one subtraction (capacity − used) per send. No extra atomic operations, no extra shared-memory writes, no extra syscalls.

4. **Backward compatible.** Flow control is opt-in. When disabled (default for initial rollout), the system behaves identically to today. The broker metadata file layout is extended, not changed.

---

## 3. Five Flow-Controlled Paths

Every message send from a `ServiceClient` traverses one of five flow-control checkpoints. Paths 2–5 apply only to cross-host sends.

```
Path 1: Local Target (same-host IPC)
  ServiceClient → IpcProducer → target service's messages ring buffer
  Backpressure point: target ring buffer remaining capacity

Path 2: Remote Target — Send Buffer (cross-host, outbound)
  ServiceClient → broker's send ring buffer
  Backpressure point: send ring buffer remaining capacity

Path 3: Remote Target — Destination Buffer (cross-host, inbound on remote)
  [send RB] → TCP → remote broker receiver → target service's messages ring buffer
  Backpressure point: remote service's ring buffer remaining capacity (propagated)

Path 4: Remote Target — Per-Peer Send Congestion (cross-host, outbound)
  [send RB] → sender thread → per-peer write queue → TCP
  Backpressure point: per-peer bytes pending (ring + write queue), published by sender

Path 5: Remote Target — Peer Connectivity (cross-host, outbound)
  ServiceClient → peer connection state check
  Backpressure point: peer connection state (connected/disconnected), published by sender
```

For a cross-host send, the client checks **all four** applicable paths (2–5) plus the target
buffer (Path 3) before writing. The checks are ordered from cheapest to most expensive:
Path 5 (single byte read) → Path 4 (two u64 reads) → Path 2 (ring buffer arithmetic) →
Path 3 (propagated counter read).

### Design Rationale: Single Send Ring Buffer with Per-Peer Visibility

The architecture retains a **single MPSC send ring buffer** shared by all peers, rather than
splitting into per-peer ring buffers. This is a deliberate design choice:

**Why not per-peer send ring buffers?**

1. **Burst elasticity.** A single 1 MB ring lets any hot peer borrow all available capacity.
   With N per-peer rings (e.g. 8 × 1 MB), one hot peer is capped at 1 MB while 7 MB sits
   idle. Cross-host traffic is inherently bursty; pooled capacity handles bursts better.

2. **The sender already isolates peers downstream.** After draining the shared ring, the
   sender dispatches to per-peer `WriteQueue`s. Overflow there drops oldest per-peer. A slow
   peer's TCP backpressure does not stall the sender thread or block other peers' messages
   from being drained. Per-peer rings would only improve **pre-dispatch admission** isolation.

3. **Sender scheduling simplicity.** One `ring_buffer.read()` call per duty cycle is the
   simplest possible consumer loop. With N rings, the sender must define budget allocation
   (64 total? 64 per ring?), implement fairness, and accept more cache-line misses per cycle.

4. **Disconnected peer semantics.** With per-peer rings, a disconnected peer's ring fills
   and stalls producers (stall-fast). With a shared ring, disconnected-peer messages are
   drained and dropped by the sender (drop-fast). Drop-fast is the better default — it matches
   the existing best-effort delivery semantics.

5. **Memory layout simplicity.** No per-peer ring offset mapping, no sparse-peer handling,
   no config-mismatch risk across processes.

**What per-peer visibility solves (Paths 4 & 5):** The gap in the single-buffer design is
that `sendBufferRemaining()` conflates all peers' usage into one number. If Peer 2's messages
are accumulating (slow TCP, write-queue overflow), `sendBufferRemaining()` may look low even
though Peer 1's path is healthy. Per-peer send counters — published by the sender thread into
shared memory — provide **per-peer admission signals** without restructuring the buffer layout.

**When to reconsider per-peer rings:** If measurements show high CAS retry rates on the send
ring buffer tail_position, frequent `send_ring_buffer_full` events dominated by one peer's
traffic, or latency inflation during multi-peer fan-out bursts, per-peer rings become worth
the trade-offs. See §13.7 for the measurement checklist.

---

## 4. Remaining-Bytes Visibility by Path

### 4.1 Local Target Service (Path 1)

**Data source:** Direct ring buffer positions in shared memory.

The `IpcProducer` already wraps the target service's messages ring buffer, which resides in the target's mmap'd metadata file. Remaining capacity is computed directly:

```
remaining = capacity - (@atomicLoad(tail_position, .acquire) - @atomicLoad(head_position, .acquire))
```

This is **exact at the instant of the read** (modulo concurrent writes by other producers, which is inherent to MPSC).

**No separate counter is needed for local services.** The ring buffer's own trailer positions in shared memory ARE the counter.

### 4.2 Broker Send Ring Buffer (Path 2)

**Data source:** Direct ring buffer positions in shared memory.

The send ring buffer is in the broker's metadata file, which every service already mmaps. Remaining capacity:

```
send_rb_remaining = send_rb_capacity - (send_tail - send_head)
```

Same approach as Path 1 — direct shared-memory reads, exact, no extra mechanism.

### 4.3 Remote Target Service (Path 3)

**Data source:** Propagated counter in shared-memory flow control region.

The remote service's ring buffer is on another host — the service client cannot read it directly. Instead:

1. The remote broker monitors its local services' ring buffer utilization.
2. When remaining capacity crosses a watermark, the remote broker sends a `RemainingBytesUpdate` admin message to peer brokers.
3. The local broker writes the received value into a **flow control counters region** in its own shared memory (extension of the broker metadata file).
4. The service client reads this counter before sending.

This counter is **advisory** — it reflects the remote service's state at the time the update was sent, minus network propagation delay.

### 4.4 Accounting: Actual Ring Buffer Cost

When comparing remaining bytes to a message, the client must account for the **actual ring buffer cost**, not just the application payload size:

```
Path 1 (local IPC):
  ring_cost = ALIGN(RING_RECORD_HEADER (8) + payload.len, 8)

Path 2 (send ring buffer):
  ring_cost = ALIGN(RING_RECORD_HEADER (8) + MESSAGE_HEADER (24) + payload.len, 8)

Path 3 (remote — advisory):
  advisory_cost = ALIGN(RING_RECORD_HEADER (8) + payload.len, 8)
  (The remote broker writes the payload into the service ring buffer with a record header.)
```

Wrap padding may add up to `ring_cost` additional bytes in the worst case. For conservative admission, a simple approximation is `2 × ring_cost`.

### 4.5 Per-Peer Send Congestion (Path 4)

**Data source:** Per-peer send counters region in shared memory (published by sender thread).

The send ring buffer remaining (Path 2) is a single number for all peers. It cannot
distinguish whether remaining capacity is low because Peer 1 is slow or because all
peers are under heavy load. Path 4 provides **per-peer visibility** into the outbound
pipeline:

```
peer_bytes_pending = @atomicLoad(u64, &peer_entry.ring_bytes_pending, .acquire)
                   + @atomicLoad(u64, &peer_entry.queue_bytes_pending, .acquire)
```

- `ring_bytes_pending`: bytes in the send ring buffer destined for this peer (tracked by
  sender thread as it drains the ring and accumulates per-peer tallies).
- `queue_bytes_pending`: bytes currently in this peer's write queue (updated by sender
  after enqueue/dequeue operations).

This counter is **advisory** — it reflects the sender thread's last published state. Between
sender duty cycles, new messages may enter the ring buffer for this peer. The staleness is
bounded by the sender's duty-cycle interval (typically < 10µs under load).

**When this triggers backpressure:** If `peer_bytes_pending` exceeds a configurable threshold
(`per_peer_pending_threshold`), the ServiceClient applies the configured backpressure strategy
for this specific peer only. Other peers remain unaffected.

### 4.6 Peer Connectivity (Path 5)

**Data source:** Per-peer send counters region in shared memory (published by sender thread).

```
is_connected = @atomicLoad(u8, &peer_entry.connection_state, .acquire) == 1
```

This is the cheapest check — a single byte read. If the peer is disconnected, the
ServiceClient can fail fast with `error.PeerDisconnected` rather than writing to the send
ring buffer only for the message to be drained and dropped by the sender.

**Staleness:** Connection state changes are published by the sender thread on its duty cycle
after connection establishment or loss detection. Staleness is bounded by one sender cycle.
During the brief window where stale state causes a write for a disconnected peer, the sender
drains and drops the message (existing behavior, no regression).

---

## 5. Flow Control Counters Region (Shared Memory)

### 5.1 Broker Metadata File Extension

The flow control counters region is appended to the broker metadata file. The layout
must account for the **ring buffer trailer** (768 bytes) appended to each ring buffer's
data capacity, and the total file size is **page-aligned** (4096 bytes).

**Important:** The header's `controlBufferLength` and `messagesBufferLength` store the
data capacity only (power of two). The actual mapped region for each ring buffer is
`data_capacity + 768` (trailer). This matches the existing `BrokerMetadataFile.create()`
implementation.

```
Broker Metadata File (extended):

┌──────────────────────────────────────────────┐  ← offset 0
│  Metadata Header (512 bytes)                 │
│                                              │
│  +0:    controlBufferLength    (i32)         │  ← data capacity (no trailer)
│  +4:    messagesBufferLength   (i32)         │  ← data capacity (no trailer)
│  +8:    serviceId              (i32) = 0     │
│  +12:   nodeId                 (i16)         │
│  +14:   padding                (i16)         │
│  +16:   pid                    (i64)         │
│  +24:   startTimestampMs       (i64)         │
│  ... (raw offsets within 512-byte header):   │
│  +256:  heartbeatTimeMs        (volatile i64)│
│  +288:  nextServiceId          (volatile i32)│
│  +292:  fcBufferLength         (i32)         │  ← NEW: 0 = flow control disabled
│  +296:  peerSendCountersLength (i32)         │  ← NEW: 0 = per-peer counters disabled
│  +300:  padding to 512 bytes                 │
├──────────────────────────────────────────────┤  ← offset 512
│  Control Ring Buffer                         │
│  (controlBufferLength + 768 bytes)           │  ← data capacity + trailer
├──────────────────────────────────────────────┤  ← offset 512 + ctrl_region
│  Send Ring Buffer                            │
│  (messagesBufferLength + 768 bytes)          │  ← data capacity + trailer
├──────────────────────────────────────────────┤  ← offset 512 + ctrl_region + msgs_region
│  Flow Control Counters Region                │  ← NEW (§5)
│  (fcBufferLength bytes)                      │
│  Only present when fcBufferLength > 0        │
├──────────────────────────────────────────────┤  ← offset 512 + ctrl_region + msgs_region + fcBufferLength
│  Per-Peer Send Counters Region               │  ← NEW (§6)
│  (peerSendCountersLength bytes)              │
│  Only present when peerSendCountersLength > 0│
└──────────────────────────────────────────────┘

Where:
  ctrl_region = controlBufferLength + 768 (ring buffer trailer)
  msgs_region = messagesBufferLength + 768 (ring buffer trailer)

Total size = alignUp(512 + ctrl_region + msgs_region + fcBufferLength + peerSendCountersLength, 4096)
```

The `fcBufferLength` field is at raw offset 292 within the header (immediately after
`nextServiceId` at offset 288). The `peerSendCountersLength` field is at raw offset 296.
Both fall within existing padding. Old brokers/services write zeros to this area, which
correctly means "disabled."

When `fcBufferLength` is 0, the flow control region is absent and the system behaves as before.

**Service-side access:** Services already mmap the broker metadata file. To locate the
flow control region, a service reads `fcBufferLength` from offset 292. If non-zero, the
region starts at `512 + (controlBufferLength + 768) + (messagesBufferLength + 768)`.
To locate the per-peer send counters region, read `peerSendCountersLength` from offset 296.
If non-zero, it starts immediately after the flow control counters region.

### 5.2 Flow Control Counters Region Layout

```
Flow Control Counters Region:

┌──────────────────────────────────────────────┐  ← fc_offset
│  Header (64 bytes)                           │
│                                              │
│  +0:    version (u32)           = 1          │
│  +4:    maxEntries (u32)                     │
│  +8:    padding to 64 bytes                  │
├──────────────────────────────────────────────┤  ← fc_offset + 64
│  Entry 0 (64 bytes)                          │
│  Entry 1 (64 bytes)                          │
│  ...                                         │
│  Entry N-1 (64 bytes)                        │
└──────────────────────────────────────────────┘

Total: 64 + maxEntries × 64 bytes
```

### 5.3 Flow Control Entry (64 bytes, cache-line aligned)

```
Offset  Size  Type              Field
──────────────────────────────────────────────────────
0       1     volatile u8       state              0=FREE, 1=ALLOCATED, 2=RECLAIMED
1       1     volatile u8       pressure_state     0=UNKNOWN, 1=NORMAL, 2=PRESSURED
2       2     u16               reserved
4       4     i32               service_id         target service's ID
8       2     i16               node_id            target service's host
10      2     u16               generation         slot reuse generation
12      4     u32               capacity           target ring buffer capacity (bytes)
16      4     volatile u32      remaining_bytes    last known remaining bytes
20      4     u32               reserved
24      8     volatile i64      last_update_ns     monotonic timestamp of last update
32      32    [32]u8            padding            (pad to 64 bytes / 1 cache line)
```

**Field semantics:**

| Field | Writer | Reader | Notes |
|---|---|---|---|
| `state` | Control loop | ServiceClient | Slot lifecycle management |
| `pressure_state` | Control loop | ServiceClient | Coarse-grained signal |
| `service_id`, `node_id` | Control loop (on allocation) | ServiceClient | Identifies the remote service |
| `generation` | Control loop | ServiceClient | Prevents stale slot reads after reuse |
| `capacity` | Control loop | ServiceClient | For computing utilization percentage |
| `remaining_bytes` | Control loop (via command from receiver) | ServiceClient | Advisory remaining capacity |
| `last_update_ns` | Control loop | ServiceClient | Freshness indicator |

**Single-writer invariant:** All fields are written by the **control loop** only.
When the receiver event loop decodes a `RemainingBytesUpdate` from TCP, it forwards
the decoded entries to the control loop via the internal command queue (existing
pattern used for all admin message handling). The control loop then writes the updated
values to the shared-memory FC entries. This preserves the architecture's single-writer
principle — the receiver never writes to shared memory directly.

### 5.4 Slot Allocation & Lifecycle

Slots are managed by the **control loop** (single writer for lifecycle operations):

**Allocation (on learning about a remote service):**
1. Scan entries for first `state == FREE`.
2. Set `service_id`, `node_id`, `generation` (increment from previous tenant).
3. Set `capacity` (from `ServiceAdded` message or cluster snapshot).
4. Set `remaining_bytes` to `capacity` (optimistic default — no backpressure until first update).
5. Set `pressure_state` to `UNKNOWN`.
6. Set `state` to `ALLOCATED` (release store — makes all fields visible).

**Reclamation (on remote service removal):**
1. Set `state` to `RECLAIMED` (release store).
2. Do **not** immediately reuse. The slot remains in `RECLAIMED` state for a grace period (e.g., 10 seconds) to allow service clients to observe the state change via the next `ServiceInstances` discovery update.
3. After the grace period, set `state` to `FREE`.

**Generation counter** prevents stale reads: if a service client holds a stale `fc_slot_id` from a previous discovery update, it can detect reuse by comparing `generation` against the expected value.

---

## 6. Per-Peer Send Counters Region (Shared Memory)

The per-peer send counters region is appended after the flow control counters region.
It provides per-peer visibility into the outbound pipeline, enabling Paths 4 and 5.

### 6.1 Region Layout

The region consists of a header followed by a fixed-size array of peer entries:

```
Per-Peer Send Counters Region:

┌────────────────────────────────────────────────┐  ← region start
│  Region Header (128 bytes, 2 cache lines)      │
│                                                │
│  +0:   version          (u32) = 1              │
│  +4:   entry_count      (u32)                  │  ← max peers (fixed at create)
│  +8:   entry_size       (u32) = 128            │  ← bytes per entry
│  +12:  reserved         (116 bytes)            │
├────────────────────────────────────────────────┤  ← offset 128
│  Peer Entry [0] (128 bytes, 2 cache lines)     │
│                                                │
│  +0:   node_id              (i16)              │  ← 0 = slot unused
│  +2:   state                (u8)               │  ← 0=free, 1=active
│  +3:   connection_state     (volatile u8)      │  ← 0=disconnected, 1=connected
│  +4:   reserved_1           (u32)              │
│  +8:   ring_bytes_pending   (volatile u64)     │  ← bytes in send ring for this peer
│  +16:  queue_bytes_pending  (volatile u64)     │  ← bytes in write queue for this peer
│  +24:  queue_capacity       (u64)              │  ← write queue capacity
│  +32:  total_bytes_sent     (volatile u64)     │  ← lifetime bytes sent (monotonic)
│  +40:  total_bytes_dropped  (volatile u64)     │  ← lifetime bytes dropped (monotonic)
│  +48:  last_update_ns       (volatile u64)     │  ← timestamp of last update
│  +56:  reserved_2           (72 bytes)         │  ← future use, zero-filled
├────────────────────────────────────────────────┤  ← offset 128 + 128
│  Peer Entry [1] (128 bytes)                    │
│  ...                                           │
├────────────────────────────────────────────────┤
│  Peer Entry [N-1] (128 bytes)                  │
└────────────────────────────────────────────────┘

Total region size = 128 + (entry_count × 128)
```

**Cache line alignment:** Each entry is 128 bytes = 2 cache lines. The first cache line
contains all fields a ServiceClient needs for fast-path checks (`connection_state`,
`ring_bytes_pending`, `queue_bytes_pending`). The second cache line holds diagnostic
counters and padding.

### 6.2 Writer/Reader Invariants

- **Single writer:** Only the sender thread writes to peer entries. This eliminates the
  need for CAS operations or locks. The sender uses `@atomicStore(.release)` for volatile
  fields and ServiceClients use `@atomicLoad(.acquire)` to read them.
- **Slot lifecycle:** When a peer connects, the sender allocates a free slot (state=free),
  sets `node_id` and `state=active`, then sets `connection_state=connected`. On disconnect,
  `connection_state` is set to 0. The slot is freed when the peer is fully removed.
- **ServiceClient lookup:** ServiceClients scan entries linearly for a matching `node_id`.
  With typical peer counts (< 16), this is a single cache-line scan. For performance, the
  ServiceClient can cache the slot index after first lookup and validate with `node_id`.

### 6.3 Size Calculation

```
peerSendCountersLength = 128 + (max_peers × 128)

Example: 8 peers → 128 + (8 × 128) = 1,152 bytes (< 1 page)
Example: 32 peers → 128 + (32 × 128) = 4,224 bytes (~1 page)
```

This is negligible compared to the ring buffers (typically 1MB+).

---

## 7. Service Registry & Discovery Changes

### 7.1 ServiceInstance Extension

```
const ServiceInstance = struct {
    service_id: i32,
    service_name: []const u8,
    node_id: i16,
    is_leader: bool = false,
    ipc_producer: ?*IpcProducer = null,

    // ── Flow Control (NEW) ──────────────────────────
    /// For remote instances: index into the flow control counters region.
    /// For local instances: -1 (use IpcProducer.remainingCapacity() instead).
    fc_slot_id: i32 = -1,

    /// Expected generation of the flow control slot.
    /// Used to detect stale slot references after reuse.
    fc_slot_generation: u16 = 0,

    /// Buffer capacity of this instance's messages ring buffer.
    /// For local instances: read from the ring buffer directly.
    /// For remote instances: received via ServiceAdded / cluster snapshot.
    messages_buffer_capacity: u32 = 0,
};
```

### 7.2 ServiceInstances Discovery Message Extension

When the broker sends `ServiceInstances` (template_id=4) to subscribing local services, it now includes flow control metadata per instance:

```
Current per-instance payload:
  service_id (i32), node_id (i16)

Extended per-instance payload:
  service_id (i32), node_id (i16), fc_slot_id (i32), fc_slot_generation (u16),
  messages_buffer_capacity (u32)
```

The `fc_slot_id` tells the service client where to read the remaining-bytes counter in the flow control counters region. For local instances, `fc_slot_id = -1` (sentinel — use direct ring buffer access).

**Backward compatibility:** The decoder (`decodeServiceInstance`) must check
`payload.len >= offset + 10` after reading the base fields before attempting to read
the FC extension fields (`fc_slot_id`, `fc_slot_generation`, `messages_buffer_capacity`).
If insufficient bytes remain (old-format broker sending short payload), default to
`fc_slot_id = -1`, `fc_slot_generation = 0`, `messages_buffer_capacity = 0`. This
means services connected to old brokers simply see "no FC data" and skip backpressure.

### 7.3 ServiceCapacityUpdate Admin Message

The existing `ServiceAddedBody` is a `comptime`-size-asserted `extern struct` (36 bytes)
and cannot be extended without breaking the build-time assert and old-format decoding.

**Instead, capacity is communicated via a new admin message:**

When a broker registers a new local service, it broadcasts `ServiceAdded` (template_id=6)
as before, then immediately follows it with `ServiceCapacityUpdate` (template_id=11):

```
AdminMessageHeader:
  template_id = 11

Payload:
  Offset  Size  Type    Field
  ──────────────────────────────────────
  0       1     u8      source_node_id
  1       1     u8      reserved
  2       2     u16     service_id
  4       4     u32     messages_buffer_capacity
  8       4     u32     current_remaining_bytes
```

Old brokers ignore the unknown template_id=11. New brokers use it to initialize the
FC entry's capacity field. If a `ServiceCapacityUpdate` is not received (old sender),
the FC entry stays at capacity=0, meaning "unknown" — the service client skips FC
for that target (graceful degradation).

### 7.4 Flow Control State Sync (New Admin Message)

The existing `ClusterStateSnapshot` (template_id=5) uses a fixed-size `SnapshotEntry`
struct with a `block_length` field in its SBE group header. Extending `SnapshotEntry`
with new fields would break backward compatibility: old brokers use a hard-coded max
buffer size based on the old entry size and would truncate or misparse extended snapshots.

**Instead, a separate admin message is used for flow control state synchronization:**

**FlowControlSnapshot (template_id = 10):**

Sent by each broker to new peers after TCP handshake completes (alongside, but separate
from, the existing `ClusterStateSnapshot`). Also sent after reconnection.

```
Payload:
  Offset  Size  Type    Field
  ──────────────────────────────────────
  0       1     u8      source_node_id
  1       1     u8      reserved
  2       2     u16     entry_count
  4       ...           entries[entry_count]:

Per Entry (12 bytes):
  Offset  Size  Type    Field
  ──────────────────────────────────────
  0       2     u16     service_id
  2       2     u16     reserved
  4       4     u32     messages_buffer_capacity
  8       4     u32     current_remaining_bytes
```

This is a complete snapshot of flow control state for all services on the sending broker.
Receiving brokers use it to initialize (or reset) flow control counter entries.

**Why a separate message:** This keeps the existing cluster protocol unchanged. Brokers
that don't support flow control will log and ignore the unrecognized template_id=10,
with no disruption to cluster state synchronization.

---

## 8. ServiceClient Flow Control

### 8.0 Prerequisite: Fix ServiceClient Concurrency

**This is a pre-existing bug that must be fixed before flow control can be implemented.**

The current `ServiceClient` stores discovered service instances in an `ArrayList` that
is mutated by the `control_agent` thread (via `addInstance`, `removeInstance`,
`updateLeader`) while `send()`, `sendTo()`, and `sendToLeader()` read/iterate the same
list on the caller's thread. This is a **data race**: if the `ArrayList` reallocates
during an `addInstance` while `send()` is iterating, the caller reads freed memory.

Flow control makes this worse because `send()` would now also read `fc_slot_id` and
`fc_slot_generation` from `ServiceInstance` entries — fields written by `control_agent`.

**Required fix (choose one):**

1. **COW immutable snapshots (recommended):** `control_agent` builds a new immutable
   slice of `ServiceInstance` on each update and swaps it in via an atomic pointer.
   `send()` loads the current snapshot pointer atomically — no locking, no contention
   on the hot path. Old snapshots are freed after a grace period or via reference
   counting.

2. **Read-write lock:** Wrap the instances list in an `std.Thread.RwLock`. `send()` takes
   a shared read lock; `control_agent` takes an exclusive write lock. Simpler to
   implement but adds lock contention on every `send()` call.

3. **Atomic pointer swap with arena:** Similar to (1) but uses two arena-allocated
   snapshots, ping-ponging between them. `control_agent` writes to the inactive arena,
   then atomically swaps the pointer.

Option 1 is preferred for zero-contention on the send hot path.

### 8.1 Unified API

```
pub const ServiceClient = struct {
    // ... existing fields ...

    /// Flow control configuration.
    fc_config: FlowControlConfig,

    /// Cached pointer to the flow control counters region base.
    /// Set during RingLoomEngine initialization.
    fc_region: ?*FlowControlRegion,

    // ── Flow Control API ─────────────────────────────

    /// Returns the estimated remaining bytes in the target's buffer.
    /// For local instances: exact (from ring buffer).
    /// For remote instances: advisory (from propagated counter).
    pub fn remainingBytes(self: *Self, instance: *const ServiceInstance) usize {
        if (instance.fc_slot_id < 0) {
            // Local instance — read ring buffer directly.
            const producer = instance.ipc_producer orelse return 0;
            return producer.remainingCapacity();
        } else {
            // Remote instance — read from flow control counters region.
            return self.readFcCounter(instance);
        }
    }

    /// Returns the estimated remaining bytes in the broker's send ring buffer.
    pub fn sendBufferRemaining(self: *Self) usize {
        const broker = self.broker_meta orelse return 0;
        const send_buf = broker.getSendBuffer();
        var send_rb = RingBuffer.init(@alignCast(send_buf), false, null, null)
            catch return 0;
        return send_rb.getCapacity() - send_rb.size();
    }

    /// Returns the total bytes pending in the outbound pipeline for a specific peer.
    /// This is the sum of bytes in the send ring buffer destined for this peer plus
    /// bytes in the peer's write queue. Returns null if per-peer counters are disabled
    /// or the peer is not found.
    pub fn peerSendPending(self: *Self, node_id: i16) ?u64 {
        const region = self.peer_send_counters orelse return null;
        const entry = region.findPeer(node_id) orelse return null;
        const ring_pending = @atomicLoad(u64, &entry.ring_bytes_pending, .acquire);
        const queue_pending = @atomicLoad(u64, &entry.queue_bytes_pending, .acquire);
        return ring_pending + queue_pending;
    }

    /// Returns true if the peer broker is currently connected.
    /// Returns null if per-peer counters are disabled or the peer is not found.
    pub fn isPeerConnected(self: *Self, node_id: i16) ?bool {
        const region = self.peer_send_counters orelse return null;
        const entry = region.findPeer(node_id) orelse return null;
        return @atomicLoad(u8, &entry.connection_state, .acquire) == 1;
    }
};
```

### 8.2 Pre-Send Flow Control Check

The `send()`, `sendTo()`, and `sendToLeader()` methods are all augmented with flow
control. The `applyFlowControl()` helper is called before the actual write in each path:

```
pub fn send(self: *Self, payload: []const u8) SendError!void {
    const instance = self.balancer.next(self.instances.items) orelse
        return error.NoAvailableInstance;

    if (self.fc_config.enabled) {
        try self.applyFlowControl(instance, payload.len);
    }

    // Existing send logic (unchanged)
    if (instance.node_id == self.local_node_id) {
        const producer = instance.ipc_producer orelse
            return error.ProducerNotInitialized;
        try producer.write(constants.application_msg_type_id, payload);
    } else {
        try self.sendToRemoteService(instance.node_id, instance.service_id, payload);
    }
}

pub fn sendTo(self: *Self, service_id: i32, payload: []const u8) SendError!void {
    const instance = self.findInstance(service_id) orelse
        return error.NoAvailableInstance;

    if (self.fc_config.enabled) {
        try self.applyFlowControl(instance, payload.len);
    }

    // Existing sendTo logic...
}

pub fn sendToLeader(self: *Self, payload: []const u8) SendError!void {
    const instance = self.findLeader() orelse
        return error.NoLeaderAvailable;

    if (self.fc_config.enabled) {
        try self.applyFlowControl(instance, payload.len);
    }

    // Existing sendToLeader logic...
}

fn applyFlowControl(self: *Self, instance: *const ServiceInstance, payload_len: usize) SendError!void {
    // ringCost accounts for ring buffer overhead (message header + 8-byte alignment),
    // not just payload_len. This is the actual number of bytes consumed in the ring.
    const required = self.ringCost(instance, payload_len);

    // Check 1: Target service buffer remaining.
    try self.checkRemainingOrBackpressure(instance, required);

    // Check 2: Send ring buffer remaining (only for remote targets).
    if (instance.node_id != self.local_node_id) {
        const send_cost = alignUp(8 + MessageHeader.encoded_length + payload_len, 8);

        // Check 2a: Peer connectivity (cheapest — single byte read).
        if (self.isPeerConnected(instance.node_id)) |connected| {
            if (!connected) return error.PeerDisconnected;
        }

        // Check 2b: Per-peer send congestion (two u64 reads).
        if (self.peerSendPending(instance.node_id)) |pending| {
            if (pending + send_cost > self.fc_config.per_peer_pending_threshold) {
                try self.applyStrategy(0, send_cost, .peer_congested);
            }
        }

        // Check 2c: Global send ring buffer remaining.
        const send_remaining = self.sendBufferRemaining();
        if (send_remaining < send_cost) {
            try self.applyStrategy(send_remaining, send_cost, .send_buffer);
        }
    }
}
```

### 8.3 Backpressure Strategies

```
pub const BackpressureStrategy = enum {
    /// Return error.BackPressure immediately.
    /// The application decides whether to retry, drop, or buffer.
    drop,

    /// Spin-wait until remaining capacity is sufficient, or timeout.
    /// For local targets: spins on ring buffer positions (responsive).
    /// For remote targets: spins on advisory counter (less responsive — bounded by
    /// inter-broker propagation latency; timeout recommended ≤ 10ms).
    spin,
};

pub const FlowControlConfig = struct {
    enabled: bool = false,
    strategy: BackpressureStrategy = .drop,

    /// Maximum time to spin-wait before returning error.BackPressureTimeout.
    /// Only used when strategy = .spin.
    spin_timeout_ns: u64 = 1_000_000,  // 1ms default

    /// Optional: minimum remaining bytes threshold.
    /// Backpressure triggers when remaining < max(required, min_threshold).
    /// Useful for reserving headroom. 0 = disabled.
    min_remaining_threshold: u32 = 0,

    /// Per-peer congestion threshold. Backpressure triggers when bytes pending
    /// in the outbound pipeline for a specific peer exceed this value.
    /// 0 = disabled (per-peer congestion check skipped).
    per_peer_pending_threshold: u64 = 0,
};
```

**Strategy behavior by path:**

| Path | `drop` strategy | `spin` strategy |
|---|---|---|
| Local target | Return `error.BackPressure` | Spin on ring buffer head/tail positions (sub-µs responsiveness) |
| Peer connectivity | Return `error.PeerDisconnected` | N/A — always fail fast (no point waiting for reconnection in send path) |
| Per-peer congestion | Return `error.PeerCongested` | Spin on per-peer counter (responsiveness bounded by sender duty cycle; ~10µs) |
| Send buffer | Return `error.BackPressure` | Spin on ring buffer head/tail positions (sub-µs responsiveness) |
| Remote target | Return `error.BackPressure` | Spin on flow control counter (responsiveness bounded by broker update frequency; recommended timeout ≤ 10ms) |

**Spin implementation:**

```
fn spinUntilCapacity(self: *Self, instance: *const ServiceInstance, required: usize) SendError!void {
    const deadline_ns = monotonic_clock() + self.fc_config.spin_timeout_ns;

    while (true) {
        const remaining = self.remainingBytes(instance);
        if (remaining >= required) return;

        if (monotonic_clock() >= deadline_ns) {
            return error.BackPressureTimeout;
        }

        // CPU pause (reduces contention on shared cache lines)
        @as(void, asm volatile ("pause" ::: "memory"));
    }
}
```

### 8.4 Flow Control Error Types

```
pub const SendError = error{
    NoAvailableInstance,
    ProducerNotInitialized,
    SendBufferFull,
    NoLeaderAvailable,
    BackPressure,          // NEW: flow control triggered (drop strategy)
    BackPressureTimeout,   // NEW: spin timed out
    PeerCongested,         // NEW: per-peer send pipeline over threshold
    PeerDisconnected,      // NEW: target peer is not connected
} || RingBuffer.WriteError;
```

---

## 9. Inter-Broker Counter Propagation

### 9.1 Pressure State Machine (Per Local Service)

Each local service tracked by the broker has a **pressure state** for flow control:

```
                     remaining >= high_watermark
           ┌────────────────────────────────────────┐
           │                                        │
           ▼                                        │
      ┌─────────┐                            ┌───────────┐
      │ NORMAL  │                            │ PRESSURED │
      └────┬────┘                            └─────┬─────┘
           │                                       │
           │    remaining < low_watermark           │
           └───────────────────────────────────────►│
                                                    │
            ┌──────────────┐                  periodic refresh
            │ both states  │                  (every fc_refresh_interval_ms)
            │ have periodic│
            │ refresh      │
            └──────────────┘
```

**State transitions:**

| Transition | Condition | Action |
|---|---|---|
| NORMAL → PRESSURED | `remaining < low_watermark` | Send `RemainingBytesUpdate` to all peers |
| PRESSURED → NORMAL | `remaining >= high_watermark` | Send `RemainingBytesUpdate` to all peers |
| PRESSURED → PRESSURED | Periodic refresh timer fires | Send `RemainingBytesUpdate` to all peers |
| NORMAL → NORMAL | Low-frequency refresh timer fires | Send `RemainingBytesUpdate` to all peers |

**Hysteresis** (separate low and high watermarks) prevents oscillation when remaining hovers near a threshold.

**Periodic refresh while pressured** combats staleness: even without watermark crossings, peers receive updated remaining-bytes values at a bounded interval. This ensures that if a remote service recovers to (say) 40% remaining — between low (25%) and high (50%) watermarks — peers eventually learn this.

**Low-frequency refresh while normal:** Without this, remote counters would become
arbitrarily stale when the low watermark is never crossed. For example, a service could
go from 100% to 30% free with no update sent (low watermark is 25%). This slow drift
would make `remainingBytes()` unreliable for remote services even when buffers are
moderately full.

To address this, a lower-frequency periodic refresh runs in NORMAL state
(`fc_normal_refresh_interval_ms`, default: 2000ms). This is 10× less frequent than the
PRESSURED refresh (200ms) to minimize overhead, but ensures remote counters are never
more than ~2 seconds stale during normal operation.

**Advisory nature of remote counters:** Even with periodic refresh, remote
`remainingBytes()` values are inherently advisory — they reflect the state at the time
the update was sent, plus network propagation delay. Client-side flow control logic
should treat these as approximate signals, not exact byte counts. The actual admission
decision is always made by the receiving broker or the ring buffer CAS.

### 9.2 Watermark Defaults

Watermarks are expressed as a percentage of the service's messages ring buffer capacity:

| Parameter | Default | Meaning |
|---|---|---|
| `low_watermark_pct` | 25% | Trigger PRESSURED when remaining drops below 25% of capacity |
| `high_watermark_pct` | 50% | Trigger NORMAL when remaining rises above 50% of capacity |
| `fc_refresh_interval_ms` | 200 | While PRESSURED, refresh counter every 200ms |
| `fc_normal_refresh_interval_ms` | 2000 | While NORMAL, refresh counter every 2000ms |

With a 1 MB buffer:
- Low watermark = 256 KB remaining
- High watermark = 512 KB remaining

### 9.3 RemainingBytesUpdate Admin Message

**New admin message (template_id = 9):**

The message uses the standard admin framing: TCP frame header has `flags = 0x20`
(ADMIN flag) and `template_id = 0`. The real type is encoded in the
`AdminMessageHeader` at the start of the payload, which is how all admin messages
(elections, heartbeats, snapshots, etc.) are dispatched. The admin dispatcher in
`admin_dispatch.zig` switches on `AdminMessageHeader.template_id` — not the TCP
frame header's template_id.

```
TCP Frame Header (24 bytes):
  frame_length     = 24 + admin_header_length + payload_length
  flags            = 0x20 (ADMIN)
  template_id      = 0 (standard for all admin messages)
  source_node_id   = this broker's node ID
  target_node_id   = peer's node ID

AdminMessageHeader (8 bytes, payload prefix):
  template_id      = 9 (REMAINING_BYTES_UPDATE)
  schema_id        = <TBD, next available schema ID>
  block_length     = 0
  version          = 0

Payload (after AdminMessageHeader):
  Offset  Size  Type    Field
  ──────────────────────────────────────
  0       2     u16     entry_count
  2       2     u16     reserved
  4       ...           entries[entry_count]:

Per Entry (12 bytes):
  Offset  Size  Type    Field
  ──────────────────────────────────────
  0       4     i32     service_id
  4       4     u32     remaining_bytes
  8       4     u32     capacity
```

Total frame size: `24 + 8 + 4 + entry_count × 12` bytes.

The message batches updates for multiple services into a single frame. Multiple services
may cross watermarks in the same broker cycle.

**Dispatch integration:** Add a `remaining_bytes_update` variant to the `AdminCommand`
union in `admin_dispatch.zig`. The `dispatchAdminMessage()` switch should decode the
payload and forward it to the control loop for FC entry updates.

### 9.4 Initial Sync & Reconnect

To avoid incorrect optimistic assumptions after events that invalidate existing state:

**On ServiceAdded:** The existing `ServiceAddedBody` is a `comptime`-size-asserted
`extern struct` (36 bytes). Appending fields would break both the build-time assert and
old-format decoding.

**Instead, capacity is communicated via a separate new admin message:**

**ServiceCapacityUpdate (template_id = 11):**

```
AdminMessageHeader:
  template_id = 11

Payload:
  Offset  Size  Type    Field
  ──────────────────────────────────────
  0       1     u8      source_node_id
  1       1     u8      reserved
  2       2     u16     service_id
  4       4     u32     messages_buffer_capacity
  8       4     u32     current_remaining_bytes
```

The broker sends `ServiceCapacityUpdate` immediately after `ServiceAdded` for each
new local service. This approach:
- Leaves `ServiceAddedBody` and its comptime assert untouched
- Old brokers ignore the unknown template_id=11 harmlessly
- Provides capacity info needed to initialize FC entries

**On FlowControlSnapshot (template_id = 10, see §7.4):**
- Sent to peers on new TCP connection and after reconnection.
- Contains `messages_buffer_capacity` and `current_remaining_bytes` for all local services.
- Receiving brokers initialize or reset all flow control entries to snapshot values.

**On peer reconnection (new TCP connection established):**
- The reconnecting broker sends a full `RemainingBytesUpdate` for all its local services.
- This ensures the peer has current values, not stale pre-disconnection data.

### 9.5 Broadcast vs. Targeted

Updates are broadcast to **all connected peers**. Rationale:

- Payload is small: 256 services × 12 bytes = 3 KB maximum per update frame.
- Tracking "interested peers" per service adds complexity with minimal bandwidth savings.
- Any peer could have services that want to send to any other service.

---

## 10. Broker Implementation Changes

### 10.1 Control Loop Changes

The control loop gains new responsibilities:

```
fn control_loop_do_work(control_loop: *ControlLoop) u32 {
    var work_count: u32 = 0;
    const now_ns = monotonic_clock();

    // ... existing duties (commands, control RB, heartbeats, cluster) ...

    // NEW: Flow control monitoring (every fc_check_interval)
    if (control_loop.fc_enabled and now_ns > control_loop.next_fc_check_ns) {
        work_count += control_loop.updateFlowControlState(now_ns);
        control_loop.next_fc_check_ns = now_ns + FC_CHECK_INTERVAL_NS;
    }

    return work_count;
}

fn updateFlowControlState(control_loop: *ControlLoop, now_ns: i64) u32 {
    var updates_needed = false;

    for (control_loop.local_services) |*svc| {
        // Read current remaining from the service's ring buffer.
        const remaining = svc.computeRingBufferRemaining();
        const prev_state = svc.fc_pressure_state;

        // UNKNOWN → initial transition: newly registered services start in UNKNOWN.
        // Immediately classify based on current utilization and send first update.
        if (prev_state == .UNKNOWN) {
            svc.fc_pressure_state = if (remaining < svc.low_watermark)
                .PRESSURED
            else
                .NORMAL;
            updates_needed = true;
        }
        // Check watermark crossings.
        else if (prev_state == .NORMAL and remaining < svc.low_watermark) {
            svc.fc_pressure_state = .PRESSURED;
            updates_needed = true;
        } else if (prev_state == .PRESSURED and remaining >= svc.high_watermark) {
            svc.fc_pressure_state = .NORMAL;
            updates_needed = true;
        } else if (prev_state == .PRESSURED and
                   now_ns > svc.last_fc_broadcast_ns + FC_REFRESH_INTERVAL_NS) {
            // Periodic refresh while pressured.
            updates_needed = true;
        } else if (prev_state == .NORMAL and
                   now_ns > svc.last_fc_broadcast_ns + FC_NORMAL_REFRESH_INTERVAL_NS) {
            // Low-frequency periodic refresh while normal.
            // Prevents remote counters from becoming arbitrarily stale.
            updates_needed = true;
        }

        svc.last_remaining = remaining;
    }

    if (updates_needed) {
        // Enqueue RemainingBytesUpdate to sender for broadcast.
        control_loop.enqueueFlowControlBroadcast();
        return 1;
    }
    return 0;
}
```

**FC_CHECK_INTERVAL_NS:** Controls how often the control loop reads ring buffer positions for flow control. Default: 1ms (1,000,000 ns). This is a trade-off between counter freshness and cache-line traffic from reading remote ring buffer trailers.

**Processing incoming RemainingBytesUpdate:**
When the receiver event loop receives a `RemainingBytesUpdate` admin message from a peer, it forwards the payload to the control loop via command queue. The control loop writes the values into the flow control counters region:

```
fn handleRemainingBytesUpdate(control_loop: *ControlLoop, peer_node_id: u8, entries: []const FcEntry) void {
    for (entries) |entry| {
        const slot = control_loop.lookupFcSlot(entry.service_id, peer_node_id) orelse continue;
        @atomicStore(u32, &slot.remaining_bytes, entry.remaining_bytes, .release);
        @atomicStore(u8, &slot.pressure_state,
            if (entry.remaining_bytes < slot.computeLowWatermark()) @intFromEnum(PressureState.PRESSURED)
            else @intFromEnum(PressureState.NORMAL),
            .release);
        @atomicStore(i64, &slot.last_update_ns, monotonic_clock(), .release);
    }
}
```

### 10.2 Receiver Event Loop Changes

The receiver event loop processes `RemainingBytesUpdate` messages and forwards them to the control loop:

```
fn handle_admin_message(header: FrameHeader, payload: []const u8) void {
    switch (header.template_id) {
        // ... existing admin handlers ...
        9 => handle_remaining_bytes_update(header.source_node_id, payload),
        else => log_unknown_admin_template(header.template_id),
    }
}

fn handle_remaining_bytes_update(source_node_id: u8, payload: []const u8) void {
    // Parse entries and forward to control loop via command queue.
    const update = parseRemainingBytesUpdate(payload);
    control_loop_cmd_queue.enqueue(.{
        .handler = onFcUpdateCommand,
        .data = .{ .node_id = source_node_id, .entries = update.entries },
    });
}
```

### 10.3 Sender Event Loop Changes

The sender event loop sends `RemainingBytesUpdate` frames when commanded by the control loop,
and publishes per-peer send counters to the shared memory region:

```
fn handleFlowControlBroadcastCommand(sender: *SenderEventLoop, cmd: *FcBroadcastCmd) void {
    const frame = buildRemainingBytesUpdateFrame(cmd.entries);
    for (sender.peers) |peer| {
        if (peer.connection_state == .CONNECTED) {
            peer.write_queue.enqueue(frame) catch {
                // Flow control updates are best-effort. If write queue is full,
                // the next cycle will retry.
            };
        }
    }
}

/// Called after each duty cycle iteration. Publishes per-peer send counters
/// to the shared-memory region so that ServiceClients can read them.
fn publishPeerSendCounters(sender: *SenderEventLoop) void {
    const region = sender.peer_send_counters orelse return;

    for (sender.peers) |peer| {
        const entry = region.findOrAllocPeer(peer.node_id) orelse continue;

        // ring_bytes_pending: tracked by accumulating bytes destined for this peer
        // during send ring drain, and decrementing when written to write queue.
        @atomicStore(u64, &entry.ring_bytes_pending, peer.ring_bytes_pending, .release);

        // queue_bytes_pending: current write queue fill level.
        @atomicStore(u64, &entry.queue_bytes_pending, peer.write_queue.size(), .release);

        // connection_state: 1=connected, 0=disconnected.
        @atomicStore(u8, &entry.connection_state,
            if (peer.connection_state == .CONNECTED) @as(u8, 1) else @as(u8, 0),
            .release);

        // Diagnostic counters (monotonic).
        @atomicStore(u64, &entry.total_bytes_sent, peer.total_bytes_sent, .release);
        @atomicStore(u64, &entry.total_bytes_dropped, peer.total_bytes_dropped, .release);
        @atomicStore(u64, &entry.last_update_ns, monotonic_clock(), .release);
    }
}
```

The `publishPeerSendCounters()` call is added to the sender's `doWork()` method, executed
at the end of each duty cycle after draining the send ring buffer and processing write queues.
This ensures counters reflect the latest state. The sender is the **sole writer** — no
atomics contention, only `release` stores for ServiceClient `acquire` loads.

### 10.4 Flow Control Counter Update Paths (Summary)

```
Who updates what:

Local service counters (for remote propagation):
  Control loop reads ring buffer positions → detects watermark crossings → commands sender

Remote service counters (in shared-memory flow control region):
  Receiver reads RemainingBytesUpdate → commands control loop → control loop writes to shared memory

Per-peer send counters (in shared-memory per-peer region):
  Sender thread publishes ring_bytes_pending, queue_bytes_pending, connection_state
  after each duty cycle → ServiceClients read atomically

Send ring buffer remaining:
  Service client reads directly from ring buffer trailer in broker metadata file (no broker involvement)

Local service remaining (for local ServiceClient):
  Service client reads directly from ring buffer trailer via IpcProducer (no broker involvement)
```

---

## 11. Configuration

### 11.1 Broker Configuration

| Property | Default | Description |
|---|---|---|
| `broker.flow.control.enabled` | `false` | Enable flow control counters region and inter-broker propagation |
| `broker.flow.control.max.entries` | `256` | Max flow control counter entries (determines region size) |
| `broker.flow.control.low.watermark.pct` | `25` | Remaining % below which PRESSURED state triggers |
| `broker.flow.control.high.watermark.pct` | `50` | Remaining % above which NORMAL state triggers |
| `broker.flow.control.refresh.interval.ms` | `200` | Periodic refresh interval while PRESSURED |
| `broker.flow.control.normal.refresh.interval.ms` | `2000` | Periodic refresh interval while NORMAL |
| `broker.flow.control.check.interval.ms` | `1` | How often control loop checks ring buffer positions |
| `broker.flow.control.slot.reuse.delay.ms` | `10000` | Grace period before reclaiming a RECLAIMED slot |
| `broker.flow.control.peer.send.counters.enabled` | `false` | Enable per-peer send counters region |
| `broker.flow.control.peer.send.counters.max.peers` | `32` | Max peer entries (determines region size) |

When `broker.flow.control.enabled` is `true`, the broker allocates the flow control counters region in the metadata file (`fcBufferLength = 64 + max_entries × 64`).

When `broker.flow.control.peer.send.counters.enabled` is `true`, the broker additionally allocates the per-peer send counters region (`peerSendCountersLength = 128 + max_peers × 128`).

### 11.2 ServiceClient Configuration

| Property | Default | Description |
|---|---|---|
| `flow.control.strategy` | `drop` | `drop` or `spin` |
| `flow.control.spin.timeout.ms` | `1` | Max spin-wait time before returning timeout error |
| `flow.control.min.remaining` | `0` | Min remaining bytes threshold (0 = disabled) |
| `flow.control.per.peer.pending.threshold` | `0` | Per-peer congestion threshold in bytes (0 = disabled) |

These are set per `ServiceClient` instance by the application:

```
var client = ServiceClient.init(allocator, "pricing", broker_meta, node_id, service_id);
client.fc_config = .{
    .enabled = true,
    .strategy = .spin,
    .spin_timeout_ns = 1_000_000,  // 1ms
    .min_remaining_threshold = 4096,
    .per_peer_pending_threshold = 512 * 1024,  // 512KB — adjust based on write queue capacity
};
```

---

## 12. New Counters & Monitoring

### 12.1 System Counters

| ID | Name | Description |
|---|---|---|
| 21 | `fc_updates_sent` | RemainingBytesUpdate messages sent to peers |
| 22 | `fc_updates_received` | RemainingBytesUpdate messages received from peers |
| 23 | `fc_pressure_events` | Times a local service entered PRESSURED state |
| 24 | `fc_recovery_events` | Times a local service returned to NORMAL state |
| 25 | `fc_client_backpressure` | ServiceClient sends blocked by flow control (drop strategy) |
| 26 | `fc_client_spin_timeouts` | ServiceClient spin-waits that timed out |
| 27 | `fc_slot_allocations` | Flow control slots allocated |
| 28 | `fc_slot_reclamations` | Flow control slots reclaimed |
| 29 | `fc_peer_congestion_events` | ServiceClient sends blocked by per-peer congestion |
| 30 | `fc_peer_disconnected_sends_avoided` | ServiceClient sends avoided due to disconnected peer |

### 12.2 Per-Service Observable State

Each flow control entry in shared memory provides per-service observability:

- `remaining_bytes`: current remaining capacity estimate
- `capacity`: total buffer capacity
- `pressure_state`: UNKNOWN / NORMAL / PRESSURED
- `last_update_ns`: freshness indicator

Each per-peer send counter entry provides per-peer observability:

- `ring_bytes_pending`: bytes in send ring buffer destined for this peer
- `queue_bytes_pending`: bytes in this peer's write queue
- `queue_capacity`: write queue capacity (static)
- `connection_state`: connected / disconnected
- `total_bytes_sent`: lifetime bytes sent (monotonic)
- `total_bytes_dropped`: lifetime bytes dropped (monotonic)
- `last_update_ns`: freshness indicator

External monitoring tools can mmap the broker metadata file and read both regions directly.

---

## 13. Edge Cases & Staleness Analysis

### 13.1 Staleness by Path

| Path | Data Source | Staleness | Impact |
|---|---|---|---|
| Local target | Ring buffer trailer (shared memory) | Exact at read time; other producers may write between check and send | Minimal — ring buffer's `BufferFull` is the ultimate guard |
| Peer connectivity | Per-peer send counters (shared memory) | Bounded by sender duty cycle (~10µs under load) | Minimal — stale "connected" causes write + drop (existing behavior); stale "disconnected" delays send briefly |
| Per-peer congestion | Per-peer send counters (shared memory) | Bounded by sender duty cycle (~10µs under load) | Low — advisory; send ring `BufferFull` is the ultimate guard |
| Send buffer | Ring buffer trailer (shared memory) | Same as local target | Minimal |
| Remote target | Propagated counter | Bounded by: check interval (1ms) + command queue latency (~µs) + TCP write latency (~µs) + network RTT (~10-50µs) + receiver processing (~µs) | ~1-50ms. Reduced by periodic refresh while pressured |

### 13.2 Counter Stale After Reconnect

**Scenario:** Peer disconnects while a service is pressured. After reconnection, the local counter still shows the old (low) remaining value.

**Mitigation:** On peer reconnection, the reconnecting broker immediately sends a full `RemainingBytesUpdate` for all its local services (§9.4). Additionally, if no update has been received within `peer_liveness_timeout / 2`, the counter's `pressure_state` reverts to `UNKNOWN` and the service client treats it as "no data" (no backpressure).

### 13.3 Counter Stale After Service Restart

**Scenario:** Remote service restarts. Its ring buffer is now empty, but the local counter still shows "pressured."

**Mitigation:** Service restart triggers deregistration + re-registration → `ServiceRemoved` + `ServiceAdded` admin messages → local broker reclaims old flow control slot and allocates a new one with fresh values from the `ServiceAdded` message.

### 13.4 Multiple Producers Race

**Scenario:** Multiple local services check the remaining-bytes counter for the same target, all see sufficient capacity, and all write simultaneously, potentially overflowing the buffer.

**Impact:** The ring buffer's own `BufferFull` error is the ultimate guard. The flow control counter is a heuristic that reduces the frequency of this scenario but cannot eliminate it.

**Acceptable:** This matches the behavior of any advisory admission control system (cf. TCP window, semaphore-based flow control).

### 13.5 Flow Control Disabled Peer

**Scenario:** Broker A has flow control enabled, Broker B does not.

**Impact:** Broker B will never send `RemainingBytesUpdate` messages. Broker A's counters for services on Broker B will remain in `UNKNOWN` pressure state with the initial remaining-bytes value (capacity).

**Behavior:** ServiceClient sees `UNKNOWN` pressure state → treats as "no flow control data" → sends without backpressure (same as current behavior). No functional regression.

### 13.6 Per-Peer Counter Staleness

**Scenario:** The sender thread is busy with a long write queue flush for one peer, delaying
counter publication for other peers.

**Impact:** Per-peer counters can be stale by up to one full sender duty cycle. Under heavy
load, the duty cycle is bounded by the sender's I/O budget. For peers with large write queues,
`queue_bytes_pending` may lag behind actual TCP send completion.

**Mitigation:** The `last_update_ns` field allows ServiceClients (or monitoring tools) to
detect staleness. If `monotonic_clock() - last_update_ns > stale_threshold`, the ServiceClient
can treat the counter as unknown and fall back to global send ring buffer checks only.

### 13.7 Measurement Checklist: When to Reconsider Per-Peer Rings

The hybrid approach (single ring + per-peer counters) is optimal under expected workloads.
If future profiling reveals any of the following, per-peer send ring buffers should be
reconsidered:

1. **CAS contention on `tail_position`**: If `@atomicRmw` retries on the send ring buffer
   exceed 5% of total send attempts, per-peer rings would eliminate this contention.
   Measure: counter for CAS retries vs. successful claims.

2. **Head-of-line blocking**: If one slow peer causes the send ring to fill up, blocking
   messages to healthy peers for > 1ms on average. Measure: histogram of send ring buffer
   wait times, correlated with per-peer queue depths.

3. **Sender fairness issues**: If the sender's drain loop starves some peers' messages.
   Measure: per-peer drain latency distribution (time from ring write to write queue enqueue).

4. **Cross-host traffic dominance**: If cross-host messages constitute > 60% of total
   message volume (currently expected to be < 20%), the single send ring becomes a hotter
   contention point. Measure: ratio of local vs. remote sends.

---

## 14. Migration & Backward Compatibility

### 14.1 Metadata File Layout

- The `fcBufferLength` field at offset +292 in the metadata header is in existing padding space (between `nextServiceId` at +288 and the end of the 512-byte header). Old brokers/services write 0 to this area, which correctly means "flow control disabled."
- The `peerSendCountersLength` field at offset +296 is similarly in existing padding. Old brokers/services write 0, meaning "per-peer counters disabled."
- The flow control region is appended after the send ring buffer. The per-peer send counters region is appended after the flow control region. Old code that only reads up to `512 + (controlBufferLength + 768) + (messagesBufferLength + 768)` is unaffected.

### 14.2 Wire Protocol

- `RemainingBytesUpdate` (template_id=9), `FlowControlSnapshot` (template_id=10), and `ServiceCapacityUpdate` (template_id=11) are framed as standard admin messages. Old brokers encounter unrecognized template_ids in `dispatchAdminMessage()`, which are logged and ignored via the `else => {}` arm. No connection disruption.
- `ServiceAdded` (template_id=6) is **not modified**. Its `ServiceAddedBody` extern struct and comptime size assert remain unchanged. Capacity information is sent separately via `ServiceCapacityUpdate`.
- Extended `ServiceInstances` (template_id=4) payload: the decoder checks remaining payload length before reading FC extension fields. Short payloads (old broker) result in default values (`fc_slot_id = -1`), meaning services gracefully skip flow control for that target.

### 14.3 Rolling Upgrade

1. Deploy new broker code with `broker.flow.control.enabled = false` (default).
2. Verify cluster stability.
3. Enable flow control on each broker: `broker.flow.control.enabled = true`.
4. Optionally enable per-peer counters: `broker.flow.control.peer.send.counters.enabled = true`.
5. Update service client code to set `fc_config.enabled = true` and desired strategy.
6. Optionally set `fc_config.per_peer_pending_threshold` for per-peer congestion detection.

No flag-day required. Mixed clusters (some flow-control-enabled, some not) are safe.

---

## Appendix A: End-to-End Flow Control Sequence

### Cross-Host Send with Flow Control

```
Service A (Host 1)                    Broker 1                              Broker 2                    Service B (Host 2)
     │                                   │                                     │                             │
     │  1. send("pricing", payload)      │                                     │                             │
     │                                   │                                     │                             │
     │  2. Read fc_counter for           │                                     │                             │
     │     Service B (remote)            │                                     │                             │
     │     → remaining = 800KB ✓         │                                     │                             │
     │                                   │                                     │                             │
     │  3. Check peer connectivity       │                                     │                             │
     │     → connection_state = 1 ✓      │                                     │                             │
     │                                   │                                     │                             │
     │  4. Check per-peer congestion     │                                     │                             │
     │     → pending = 50KB < 512KB ✓    │                                     │                             │
     │                                   │                                     │                             │
     │  5. Read send_rb remaining        │                                     │                             │
     │     → remaining = 600KB ✓         │                                     │                             │
     │                                   │                                     │                             │
     │  6. Write to send ring buffer ───►│                                     │                             │
     │                                   │  7. Drain send RB                   │                             │
     │                                   │  8. Update peer counters            │                             │
     │                                   │     (ring_bytes_pending,            │                             │
     │                                   │      queue_bytes_pending)           │                             │
     │                                   │  9. TCP write ─────────────────────►│                             │
     │                                   │                                     │  10. Read TCP frame         │
     │                                   │                                     │  11. Route to Service B ───►│
     │                                   │                                     │                             │
     │                                   │                                     │  12. remaining drops below  │
     │                                   │                                     │      low watermark (25%)    │
     │                                   │                                     │                             │
     │                                   │  13. RemainingBytesUpdate ◄─────────│                             │
     │                                   │      (svc_b: remaining=200KB)       │                             │
     │                                   │                                     │                             │
     │                                   │  14. Write 200KB to fc_counter      │                             │
     │                                   │      for Service B                  │                             │
     │                                   │                                     │                             │
     │  15. Next send():                 │                                     │                             │
     │      Read fc_counter → 200KB      │                                     │                             │
     │      Message needs 300KB          │                                     │                             │
     │      → BackPressure! (drop/spin)  │                                     │                             │
     │                                   │                                     │                             │
```

### Same-Host Send with Flow Control

```
Service A (Host 1)                                                 Service B (Host 1)
     │                                                                  │
     │  1. send("analytics", payload)                                   │
     │                                                                  │
     │  2. Read ring buffer remaining via IpcProducer                   │
     │     → remaining = capacity - (tail - head) = 50KB                │
     │     Message needs 2KB ✓                                         │
     │                                                                  │
     │  3. IpcProducer.write(payload) ─────────────────────────────────►│
     │     (direct shared-memory write)                                 │
     │                                                                  │
```
