# Flow Control Improvements: Client-Side Remaining-Bytes Backpressure

**Status:** Design Proposal
**Depends on:** [architecture.md](architecture.md)

---

## Table of Contents

1. [Motivation](#1-motivation)
2. [Design Principles](#2-design-principles)
3. [Three Flow-Controlled Paths](#3-three-flow-controlled-paths)
4. [Remaining-Bytes Visibility by Path](#4-remaining-bytes-visibility-by-path)
5. [Flow Control Counters Region (Shared Memory)](#5-flow-control-counters-region-shared-memory)
6. [Service Registry & Discovery Changes](#6-service-registry--discovery-changes)
7. [ServiceClient Flow Control](#7-serviceclient-flow-control)
8. [Inter-Broker Counter Propagation](#8-inter-broker-counter-propagation)
9. [Broker Implementation Changes](#9-broker-implementation-changes)
10. [Configuration](#10-configuration)
11. [New Counters & Monitoring](#11-new-counters--monitoring)
12. [Edge Cases & Staleness Analysis](#12-edge-cases--staleness-analysis)
13. [Migration & Backward Compatibility](#13-migration--backward-compatibility)

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

## 3. Three Flow-Controlled Paths

Every message send from a `ServiceClient` traverses one of three paths, each with its own backpressure point:

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
```

For a cross-host send, the client checks **both** Path 2 (send buffer) and Path 3 (remote target buffer) before writing.

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
│  +296:  padding to 512 bytes                 │
├──────────────────────────────────────────────┤  ← offset 512
│  Control Ring Buffer                         │
│  (controlBufferLength + 768 bytes)           │  ← data capacity + trailer
├──────────────────────────────────────────────┤  ← offset 512 + ctrl_region
│  Send Ring Buffer                            │
│  (messagesBufferLength + 768 bytes)          │  ← data capacity + trailer
├──────────────────────────────────────────────┤  ← offset 512 + ctrl_region + msgs_region
│  Flow Control Counters Region                │  ← NEW
│  (fcBufferLength bytes)                      │
│  Only present when fcBufferLength > 0        │
└──────────────────────────────────────────────┘

Where:
  ctrl_region = controlBufferLength + 768 (ring buffer trailer)
  msgs_region = messagesBufferLength + 768 (ring buffer trailer)

Total size = alignUp(512 + ctrl_region + msgs_region + fcBufferLength, 4096)
```

The `fcBufferLength` field is at raw offset 292 within the header (immediately after
`nextServiceId` at offset 288). This falls within existing padding. Old brokers/services
write zeros to this area, which correctly means "flow control disabled."

When `fcBufferLength` is 0, the flow control region is absent and the system behaves as before.

**Service-side access:** Services already mmap the broker metadata file. To locate the
flow control region, a service reads `fcBufferLength` from offset 292. If non-zero, the
region starts at `512 + (controlBufferLength + 768) + (messagesBufferLength + 768)`.

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

## 6. Service Registry & Discovery Changes

### 6.1 ServiceInstance Extension

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

### 6.2 ServiceInstances Discovery Message Extension

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

### 6.3 ServiceCapacityUpdate Admin Message

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

### 6.4 Flow Control State Sync (New Admin Message)

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

## 7. ServiceClient Flow Control

### 7.0 Prerequisite: Fix ServiceClient Concurrency

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

### 7.1 Unified API

```
pub const ServiceClient = struct {
    // ... existing fields ...

    /// Flow control configuration.
    fc_config: FlowControlConfig,

    /// Cached pointer to the flow control counters region base.
    /// Set during BrzEngine initialization.
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
};
```

### 7.2 Pre-Send Flow Control Check

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
        const send_remaining = self.sendBufferRemaining();
        if (send_remaining < send_cost) {
            try self.applyStrategy(send_remaining, send_cost, .send_buffer);
        }
    }
}
```

### 7.3 Backpressure Strategies

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
};
```

**Strategy behavior by path:**

| Path | `drop` strategy | `spin` strategy |
|---|---|---|
| Local target | Return `error.BackPressure` | Spin on ring buffer head/tail positions (sub-µs responsiveness) |
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

### 7.4 Flow Control Error Types

```
pub const SendError = error{
    NoAvailableInstance,
    ProducerNotInitialized,
    SendBufferFull,
    NoLeaderAvailable,
    BackPressure,          // NEW: flow control triggered (drop strategy)
    BackPressureTimeout,   // NEW: spin timed out
} || RingBuffer.WriteError;
```

---

## 8. Inter-Broker Counter Propagation

### 8.1 Pressure State Machine (Per Local Service)

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

### 8.2 Watermark Defaults

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

### 8.3 RemainingBytesUpdate Admin Message

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

### 8.4 Initial Sync & Reconnect

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

**On FlowControlSnapshot (template_id = 10, see §6.4):**
- Sent to peers on new TCP connection and after reconnection.
- Contains `messages_buffer_capacity` and `current_remaining_bytes` for all local services.
- Receiving brokers initialize or reset all flow control entries to snapshot values.

**On peer reconnection (new TCP connection established):**
- The reconnecting broker sends a full `RemainingBytesUpdate` for all its local services.
- This ensures the peer has current values, not stale pre-disconnection data.

### 8.5 Broadcast vs. Targeted

Updates are broadcast to **all connected peers**. Rationale:

- Payload is small: 256 services × 12 bytes = 3 KB maximum per update frame.
- Tracking "interested peers" per service adds complexity with minimal bandwidth savings.
- Any peer could have services that want to send to any other service.

---

## 9. Broker Implementation Changes

### 9.1 Control Loop Changes

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

### 9.2 Receiver Event Loop Changes

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

### 9.3 Sender Event Loop Changes

The sender event loop sends `RemainingBytesUpdate` frames when commanded by the control loop:

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
```

### 9.4 Flow Control Counter Update Paths (Summary)

```
Who updates what:

Local service counters (for remote propagation):
  Control loop reads ring buffer positions → detects watermark crossings → commands sender

Remote service counters (in shared-memory flow control region):
  Receiver reads RemainingBytesUpdate → commands control loop → control loop writes to shared memory

Send ring buffer remaining:
  Service client reads directly from ring buffer trailer in broker metadata file (no broker involvement)

Local service remaining (for local ServiceClient):
  Service client reads directly from ring buffer trailer via IpcProducer (no broker involvement)
```

---

## 10. Configuration

### 10.1 Broker Configuration

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

When `broker.flow.control.enabled` is `true`, the broker allocates the flow control counters region in the metadata file (`fcBufferLength = 64 + max_entries × 64`).

### 10.2 ServiceClient Configuration

| Property | Default | Description |
|---|---|---|
| `flow.control.strategy` | `drop` | `drop` or `spin` |
| `flow.control.spin.timeout.ms` | `1` | Max spin-wait time before returning timeout error |
| `flow.control.min.remaining` | `0` | Min remaining bytes threshold (0 = disabled) |

These are set per `ServiceClient` instance by the application:

```
var client = ServiceClient.init(allocator, "pricing", broker_meta, node_id, service_id);
client.fc_config = .{
    .enabled = true,
    .strategy = .spin,
    .spin_timeout_ns = 1_000_000,  // 1ms
    .min_remaining_threshold = 4096,
};
```

---

## 11. New Counters & Monitoring

### 11.1 System Counters

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

### 11.2 Per-Service Observable State

Each flow control entry in shared memory provides per-service observability:

- `remaining_bytes`: current remaining capacity estimate
- `capacity`: total buffer capacity
- `pressure_state`: UNKNOWN / NORMAL / PRESSURED
- `last_update_ns`: freshness indicator

External monitoring tools can mmap the broker metadata file and read the flow control region directly.

---

## 12. Edge Cases & Staleness Analysis

### 12.1 Staleness by Path

| Path | Data Source | Staleness | Impact |
|---|---|---|---|
| Local target | Ring buffer trailer (shared memory) | Exact at read time; other producers may write between check and send | Minimal — ring buffer's `BufferFull` is the ultimate guard |
| Send buffer | Ring buffer trailer (shared memory) | Same as above | Minimal |
| Remote target | Propagated counter | Bounded by: check interval (1ms) + command queue latency (~µs) + TCP write latency (~µs) + network RTT (~10-50µs) + receiver processing (~µs) | ~1-50ms. Reduced by periodic refresh while pressured |

### 12.2 Counter Stale After Reconnect

**Scenario:** Peer disconnects while a service is pressured. After reconnection, the local counter still shows the old (low) remaining value.

**Mitigation:** On peer reconnection, the reconnecting broker immediately sends a full `RemainingBytesUpdate` for all its local services (§8.4). Additionally, if no update has been received within `peer_liveness_timeout / 2`, the counter's `pressure_state` reverts to `UNKNOWN` and the service client treats it as "no data" (no backpressure).

### 12.3 Counter Stale After Service Restart

**Scenario:** Remote service restarts. Its ring buffer is now empty, but the local counter still shows "pressured."

**Mitigation:** Service restart triggers deregistration + re-registration → `ServiceRemoved` + `ServiceAdded` admin messages → local broker reclaims old flow control slot and allocates a new one with fresh values from the `ServiceAdded` message.

### 12.4 Multiple Producers Race

**Scenario:** Multiple local services check the remaining-bytes counter for the same target, all see sufficient capacity, and all write simultaneously, potentially overflowing the buffer.

**Impact:** The ring buffer's own `BufferFull` error is the ultimate guard. The flow control counter is a heuristic that reduces the frequency of this scenario but cannot eliminate it.

**Acceptable:** This matches the behavior of any advisory admission control system (cf. TCP window, semaphore-based flow control).

### 12.5 Flow Control Disabled Peer

**Scenario:** Broker A has flow control enabled, Broker B does not.

**Impact:** Broker B will never send `RemainingBytesUpdate` messages. Broker A's counters for services on Broker B will remain in `UNKNOWN` pressure state with the initial remaining-bytes value (capacity).

**Behavior:** ServiceClient sees `UNKNOWN` pressure state → treats as "no flow control data" → sends without backpressure (same as current behavior). No functional regression.

---

## 13. Migration & Backward Compatibility

### 13.1 Metadata File Layout

- The `fcBufferLength` field at offset +292 in the metadata header is in existing padding space (between `nextServiceId` at +288 and the end of the 512-byte header). Old brokers/services write 0 to this area, which correctly means "flow control disabled."
- The flow control region is appended after the send ring buffer. Old code that only reads up to `512 + (controlBufferLength + 768) + (messagesBufferLength + 768)` is unaffected.

### 13.2 Wire Protocol

- `RemainingBytesUpdate` (template_id=9), `FlowControlSnapshot` (template_id=10), and `ServiceCapacityUpdate` (template_id=11) are framed as standard admin messages. Old brokers encounter unrecognized template_ids in `dispatchAdminMessage()`, which are logged and ignored via the `else => {}` arm. No connection disruption.
- `ServiceAdded` (template_id=6) is **not modified**. Its `ServiceAddedBody` extern struct and comptime size assert remain unchanged. Capacity information is sent separately via `ServiceCapacityUpdate`.
- Extended `ServiceInstances` (template_id=4) payload: the decoder checks remaining payload length before reading FC extension fields. Short payloads (old broker) result in default values (`fc_slot_id = -1`), meaning services gracefully skip flow control for that target.

### 13.3 Rolling Upgrade

1. Deploy new broker code with `broker.flow.control.enabled = false` (default).
2. Verify cluster stability.
3. Enable flow control on each broker: `broker.flow.control.enabled = true`.
4. Update service client code to set `fc_config.enabled = true` and desired strategy.

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
     │  3. Read send_rb remaining        │                                     │                             │
     │     → remaining = 600KB ✓         │                                     │                             │
     │                                   │                                     │                             │
     │  4. Write to send ring buffer ───►│                                     │                             │
     │                                   │  5. Drain send RB                   │                             │
     │                                   │  6. TCP write ─────────────────────►│                             │
     │                                   │                                     │  7. Read TCP frame          │
     │                                   │                                     │  8. Route to Service B ────►│
     │                                   │                                     │                             │
     │                                   │                                     │  9. remaining drops below   │
     │                                   │                                     │     low watermark (25%)     │
     │                                   │                                     │                             │
     │                                   │  10. RemainingBytesUpdate ◄─────────│                             │
     │                                   │      (svc_b: remaining=200KB)       │                             │
     │                                   │                                     │                             │
     │                                   │  11. Write 200KB to fc_counter      │                             │
     │                                   │      for Service B                  │                             │
     │                                   │                                     │                             │
     │  12. Next send():                 │                                     │                             │
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
