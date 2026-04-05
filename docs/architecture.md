# BRZ Broker Architecture

**For Implementation in Zig**

This document describes the complete architecture of the BRZ broker, a high-performance IPC and cross-host message routing system. The design incorporates and simplifies concepts from Aeron's architecture, tailored specifically for the BRZ broker's needs: fixed topology, single-stream-per-link semantics, `nodeId`/`serviceId` routing, and minimal complexity for lowest latency.

---

## Table of Contents

1. [Design Philosophy](#1-design-philosophy)
2. [System Overview](#2-system-overview)
3. [Simplifications from Aeron](#3-simplifications-from-aeron)
4. [Memory Layout & Shared Memory IPC](#4-memory-layout--shared-memory-ipc)
5. [Concurrent Data Structures](#5-concurrent-data-structures)
6. [Service ↔ Broker IPC](#6-service--broker-ipc)
7. [UDP Wire Protocol](#7-udp-wire-protocol)
8. [Send Path Architecture](#8-send-path-architecture)
9. [Receive Path Architecture](#9-receive-path-architecture)
10. [Flow Control](#10-flow-control)
11. [Loss Detection & Retransmission](#11-loss-detection--retransmission)
12. [Threading Model](#12-threading-model)
13. [Broker Control Loop](#13-broker-control-loop)
14. [Message Routing Engine](#14-message-routing-engine)
15. [Service Registration & Discovery](#15-service-registration--discovery)
16. [Heartbeat & Health Checking](#16-heartbeat--health-checking)
17. [Cluster Management](#17-cluster-management)
18. [Configuration](#18-configuration)
19. [Counters & Monitoring](#19-counters--monitoring)
20. [Error Handling](#20-error-handling)
21. [Platform Abstraction](#21-platform-abstraction)
22. [Constants Reference](#22-constants-reference)

---

## 1. Design Philosophy

### Core Principles

1. **Zero-copy on the hot path.** All local data transfer uses memory-mapped shared memory. No `memcpy` between service and broker for same-host IPC. Cross-host routing performs exactly one copy: from the receive log buffer to the target service's ring buffer.
2. **Lock-free everywhere.** All shared data structures use atomic fetch-and-add or CAS. No mutexes on any data path.
3. **Single-writer principle.** Every mutable shared location has exactly one writer. Readers never block writers.
4. **Allocation-free hot path.** All buffers are pre-allocated. Message routing uses flyweight patterns over mapped memory.
5. **Simplicity over generality.** Unlike Aeron, which is a general-purpose transport, this system has exactly one use case: BRZ broker message routing. Every abstraction that doesn't serve this use case is eliminated.
6. **Position-based progress tracking.** All progress is tracked via monotonically increasing 64-bit positions, not sequence numbers.
7. **Duty-cycle event loops.** All processing is done in tight loops returning work counts, driving idle strategies.

### Key Differences from Aeron

| Aeron Concept | BRZ Broker Equivalent | Rationale |
|---|---|---|
| `streamId` / `sessionId` multiplexing | `nodeId` / `serviceId` in message header | BRZ has one logical stream per peer link; routing is by service identity, not stream identity |
| 3-partition rotating log buffers | Single receive log buffer per peer node | One stream per link means no partition rotation needed for the receive path |
| Per-publication log buffer files | Single MPSC send ring buffer per broker | All local services write to one outbound buffer; broker drains and routes |
| CnC file + command/response protocol | Direct metadata file with embedded ring buffers | Services map the broker's file directly; no driver process needed |
| Media Driver as separate process | Transport integrated into broker process | The broker IS the driver; no client↔driver split needed |
| Broadcast buffer for responses | Direct writes to service control ring buffers | Broker writes responses directly to each service's mapped memory |
| Generic URI-based channel configuration | Fixed per-peer UDP endpoints from config | No URI parsing needed; topology is known at startup |

---

## 2. System Overview

```
                          Host A                                          Host B
┌─────────────────────────────────────────────────┐     ┌─────────────────────────────────────────────────┐
│                                                 │     │                                                 │
│  ┌───────────┐  ┌───────────┐  ┌───────────┐   │     │   ┌───────────┐  ┌───────────┐  ┌───────────┐  │
│  │ Service A │  │ Service B │  │ Service C │   │     │   │ Service D │  │ Service E │  │ Service F │  │
│  │ (id=1)    │  │ (id=2)    │  │ (id=3)    │   │     │   │ (id=4)    │  │ (id=5)    │  │ (id=6)    │  │
│  └─────┬─────┘  └─────┬─────┘  └─────┬─────┘   │     │   └─────┬─────┘  └─────┬─────┘  └─────┬─────┘  │
│        │               │               │         │     │         │               │               │        │
│   IPC  │          IPC  │          IPC  │         │     │    IPC  │          IPC  │          IPC  │        │
│  (shm) │         (shm) │         (shm) │         │     │   (shm) │         (shm) │         (shm) │        │
│        │               │               │         │     │         │               │               │        │
│  ┌─────┴───────────────┴───────────────┴─────┐   │     │   ┌─────┴───────────────┴───────────────┴─────┐  │
│  │              Broker (nodeId=1)             │   │     │   │              Broker (nodeId=2)             │  │
│  │                                           │   │     │   │                                           │  │
│  │  ┌─────────┐ ┌──────────┐ ┌───────────┐  │   │     │   │  ┌─────────┐ ┌──────────┐ ┌───────────┐  │  │
│  │  │Control  │ │ Sender   │ │ Receiver  │  │   │     │   │  │Control  │ │ Sender   │ │ Receiver  │  │  │
│  │  │ Loop    │ │Evt. Loop │ │Evt. Loop  │  │   │     │   │  │ Loop    │ │Evt. Loop │ │Evt. Loop  │  │  │
│  │  └────┬────┘ └────┬─────┘ └─────┬─────┘  │   │     │   │  └────┬────┘ └────┬─────┘ └─────┬─────┘  │  │
│  │       │           │             │         │   │     │   │       │           │             │         │  │
│  │       │      ┌────┴────┐   ┌────┴────┐    │   │     │   │       │      ┌────┴────┐   ┌────┴────┐    │  │
│  │       │      │ Send    │   │ Receive │    │   │     │   │       │      │ Send    │   │ Receive │    │  │
│  │       │      │ Ring    │   │ Log     │    │   │     │   │       │      │ Ring    │   │ Log     │    │  │
│  │       │      │ Buffer  │   │ Buffer  │    │   │     │   │       │      │ Ring    │   │ Log     │    │  │
│  │       │      │ (MPSC)  │   │ (per    │    │   │     │   │       │      │ Buffer  │   │ Buffer  │    │  │
│  │       │      │         │   │  peer)  │    │   │     │   │       │      │ (MPSC)  │   │  (per   │    │  │
│  │       │      └────┬────┘   └────┬────┘    │   │     │   │       │      └────┬────┘   │  peer)  │    │  │
│  │       │           │             │         │   │     │   │       │           │        └────┬────┘    │  │
│  └───────┼───────────┼─────────────┼─────────┘   │     │   └───────┼───────────┼────────────┼─────────┘  │
│          │           │             │              │     │           │           │            │             │
└──────────┼───────────┼─────────────┼──────────────┘     └───────────┼───────────┼────────────┼─────────────┘
                       │             │                                │            │
                       │             │         UDP (unicast)          │            │
                       └─────────────┼────────────────────────────────┘            │
                                     └────────────────────────────────────────────┘
```

### Component Summary

| Component | Role |
|---|---|
| **Metadata File** | Per-service and per-broker shared memory file containing ring buffers and metadata |
| **Control Ring Buffer** | MPSC ring buffer for service↔broker control messages (registration, discovery, leader election) |
| **Message Ring Buffer** | MPSC ring buffer for service→broker cross-host message submission |
| **Send Ring Buffer** | Single MPSC ring buffer per broker; services write outbound cross-host messages here |
| **Receive Log Buffer** | One per peer node; incoming UDP data is assembled here for routing to local services |
| **Control Loop** | Control plane: service registration, heartbeat checking, cluster management |
| **Sender Event Loop** | Data plane: drains send ring buffer, transmits UDP to peer brokers |
| **Receiver Event Loop** | Data plane: receives UDP from peers, writes to receive log buffers, routes to local services |

---

## 3. Simplifications from Aeron

### 3.1 No Stream/Session Multiplexing

Aeron supports arbitrary numbers of streams and sessions per UDP endpoint, requiring `streamId`/`sessionId` lookup on every packet. BRZ has exactly **one logical link per peer broker**, using a fixed UDP endpoint. The `nodeId` field in the BRZ message header replaces stream/session routing:

```
Aeron:   endpoint:port → streamId → sessionId → log buffer → poll
BRZ:     endpoint:port → single receive log buffer → route by (targetNodeId, targetServiceId)
```

This eliminates:
- Stream interest maps
- Session-to-image maps
- Data packet dispatcher
- Session ID generation and management

### 3.2 No CnC File or Command Protocol

Aeron uses a Command-and-Control shared memory file with an MPSC ring buffer (client→driver) and a broadcast buffer (driver→clients) because the media driver is a separate process that manages resources for multiple clients.

In BRZ, the broker IS the transport. Services communicate with the broker through **direct shared memory ring buffers** embedded in metadata files:

```
Aeron:   Client → CnC ring buffer → Driver Conductor → broadcast → Client
BRZ:     Service → Broker control ring buffer (direct write)
         Broker → Service control ring buffer (direct write)
```

This eliminates:
- CnC file layout and handshake
- Broadcast buffer (one-to-many)
- Client conductor and keepalive protocol
- Registration state machine
- Correlation ID tracking for async responses

### 3.3 No Log Buffer Rotation

Aeron uses 3 term partitions per log buffer, rotating when one fills up. This enables continuous streaming without blocking while one term is being read and another is being cleaned.

For BRZ's receive path, a **single contiguous log buffer** suffices because:
- There is exactly one stream per peer link (no concurrent streams sharing a log buffer)
- The broker processes received data immediately (routes to service ring buffers), so the log buffer is consumed quickly
- Back-pressure is applied at the UDP level if the receiver falls behind

For the send path, an MPSC ring buffer is used instead of a log buffer entirely, since multiple services write outbound messages and a single sender thread drains them.

### 3.4 No Publication/Subscription Abstraction

Aeron has a rich Publication/Subscription API with log buffer files per publication, subscriber position counters, and image lifecycle management. BRZ replaces all of this with:

- **Outbound:** Services write to the broker's MPSC send ring buffer (or directly to another service's message ring buffer for same-host IPC)
- **Inbound:** The broker writes received messages directly to the target service's message ring buffer

### 3.5 Simplified Flow Control

Aeron's flow control tracks per-subscriber positions and uses Status Messages to communicate receiver window back to senders. BRZ simplifies this to:

- **Single receiver per link:** Each peer connection has exactly one receiver (the peer broker). No min/max/tagged flow control strategies needed.
- **Window-based:** A fixed or RTT-adaptive receiver window is communicated via Status Messages.
- **Back-pressure propagation:** When a service's ring buffer is full, the broker cannot deliver and signals back-pressure to the sender via Status Messages.

---

## 4. Memory Layout & Shared Memory IPC

### 4.1 Broker Metadata File

Located at `<storage_path>/<group>/services/broker_0.dat` (or `0.dat` with encryption).

```
┌──────────────────────────────────────────────┐  ← offset 0
│  Metadata Header (512 bytes)                 │
│                                              │
│  +0:    controlBufferLength    (i32)         │
│  +4:    messagesBufferLength   (i32)         │
│  +8:    serviceId              (i32)  = 0    │
│  +12:   nodeId                 (i16)         │
│  +14:   padding                (i16)         │
│  +16:   pid                    (i64)         │
│  +24:   startTimestampMs       (i64)         │
│  +32:   nextServiceId          (volatile i32)│  ← atomic counter for service ID assignment
│  +36:   padding to 256 bytes                 │
│  +256:  heartbeatTimeMs        (volatile i64)│  ← broker heartbeat timestamp
│  +264:  padding to 512 bytes                 │
├──────────────────────────────────────────────┤  ← offset 512
│  Control Ring Buffer                         │
│  (controlBufferLength bytes)                 │
│  MPSC ring buffer for service→broker         │
│  control messages (register, subscribe,      │
│  heartbeat)                                  │
├──────────────────────────────────────────────┤  ← offset 512 + controlBufferLength
│  Send Ring Buffer                            │
│  (messagesBufferLength bytes)                │
│  MPSC ring buffer for service→broker         │
│  cross-host messages to route                │
└──────────────────────────────────────────────┘

Total size = 512 + controlBufferLength + messagesBufferLength
```

The **send ring buffer** serves as the single outbound buffer for all cross-host messages. Any local service that needs to send a message to a remote node writes to this buffer with the target `nodeId` and `serviceId` set in the BRZ message header.

### 4.2 Service Metadata File

Located at `<storage_path>/<group>/services/<name>_<id>.dat`.

```
┌──────────────────────────────────────────────┐  ← offset 0
│  Metadata Header (512 bytes)                 │
│                                              │
│  +0:    controlBufferLength    (i32)         │
│  +4:    messagesBufferLength   (i32)         │
│  +8:    serviceId              (i32)         │
│  +12:   nodeId                 (i16)         │
│  +14:   blockingMode           (i16)         │
│  +16:   pid                    (i64)         │
│  +24:   startTimestampMs       (i64)         │
│  +32:   heartbeatTimeoutMs     (i32)         │
│  +36:   padding to 256 bytes                 │
│  +256:  heartbeatTimeMs        (volatile i64)│
│  +264:  padding to 512 bytes                 │
├──────────────────────────────────────────────┤  ← offset 512
│  [Blocking Trailer (if blocking mode)]       │
│  (3 × 128 bytes = 384 bytes)                │
│  writer_wait_state, reader_wait_state,       │
│  wait_timeout                                │
├──────────────────────────────────────────────┤
│  Control Ring Buffer                         │
│  (controlBufferLength bytes)                 │
│  MPSC: broker→service control messages       │
│  (registration response, service instances,  │
│   leader changed)                            │
├──────────────────────────────────────────────┤
│  Messages Ring Buffer                        │
│  (messagesBufferLength bytes)                │
│  MPSC: producers→service application         │
│  messages (from local services via IPC,      │
│   or from broker for cross-host messages)    │
└──────────────────────────────────────────────┘
```

### 4.3 Receive Log Buffer (Per-Peer)

One receive log buffer is allocated per connected peer broker. Unlike Aeron's 3-partition log buffer, BRZ uses a **single-partition log buffer** because there is exactly one stream per peer.

```
┌──────────────────────────────────────────────┐  ← offset 0
│  Log Data                                    │
│  (log_buffer_length bytes, power of 2)       │
│                                              │
│  Contains received UDP data frames from      │
│  this peer, assembled by the receiver.       │
│  Frames are 32-byte aligned.                 │
│                                              │
├──────────────────────────────────────────────┤  ← offset = log_buffer_length
│  Log Metadata (256 bytes)                    │
│                                              │
│  +0:   tail_position     (volatile i64)      │  ← receiver writes (single writer)
│  +8:   padding[120]                          │
│  +128: rebuild_position  (volatile i64)      │  ← control loop updates (rebuild progress)
│  +136: padding to 256 bytes                  │
└──────────────────────────────────────────────┘

Total size = log_buffer_length + 256
```

**Key differences from Aeron log buffers:**

| Aeron Log Buffer | BRZ Receive Log Buffer |
|---|---|
| 3 term partitions + 4096 byte metadata | 1 data region + 256 byte metadata |
| `term_tail_counters[3]` packed with `term_id` | Single `tail_position` (monotonic) |
| `active_term_count` for rotation | No rotation — single partition wraps |
| Per-publication or per-image file | Per-peer-node; one per connected broker |
| `initial_term_id` for position math | Positions are absolute from buffer start |

The receive log buffer uses a **circular overwrite model**: the receiver writes at `tail_position % log_buffer_length`, and the control loop/router reads at its tracked position. If the router falls behind by more than `log_buffer_length`, data is lost (back-pressure via Status Messages should prevent this).

---

## 5. Concurrent Data Structures

### 5.1 Foundational Primitives

```
CACHE_LINE_LENGTH = 64 bytes
CACHE_LINE_PAD   = 128 bytes  (2 × cache line, prevents false sharing)

ALIGN(value, alignment) = (value + (alignment - 1)) & ~(alignment - 1)
IS_POWER_OF_TWO(value)  = (value > 0) and ((value & (value - 1)) == 0)
```

#### Atomic Operations (Zig)

| Operation | Zig Builtin | x86-64 | ARM |
|---|---|---|---|
| Acquire load | `@atomicLoad(.acquire)` | Plain load + compiler fence | `ldapr` |
| Release store | `@atomicStore(.release)` | Compiler fence + plain store | `stlr` |
| Fetch-and-add | `@atomicRmw(.Add, .monotonic)` | `lock xadd` | `ldadd` |
| Compare-and-swap | `@cmpxchgWeak(.acquire, .monotonic)` | `lock cmpxchg` | `cas` |
| Full fence | `@fence(.seq_cst)` | `mfence` | `dmb ish` |

Zig's `@atomicLoad` and `@atomicStore` with explicit ordering map directly to the required semantics. On x86-64, acquire/release are effectively free (compiler fences only).

### 5.2 MPSC Ring Buffer

Used for: service→broker control messages, service→broker outbound messages, broker→service responses (single writer in this case, but MPSC structure is reused for uniformity).

#### Memory Layout

```
┌──────────────────────────────────────┐  ← offset 0
│  Data Buffer                         │
│  (capacity bytes, power of 2)        │
├──────────────────────────────────────┤  ← offset = capacity
│  Trailer (768 bytes)                 │
│                                      │
│  +0:     begin_pad[128]              │
│  +128:   tail_position (i64)         │  ← producers CAS here
│          tail_pad[120]               │
│  +256:   head_cache (i64)            │  ← producer-cached head (reduces contention)
│          head_cache_pad[120]         │
│  +384:   head_position (i64)         │  ← consumer writes here
│          head_pad[120]               │
│  +512:   correlation_counter (i64)   │  ← atomic counter for correlation IDs
│          corr_pad[120]               │
│  +640:   consumer_heartbeat (i64)    │  ← consumer liveness timestamp
│          hb_pad[120]                 │
└──────────────────────────────────────┘

Total allocation: capacity + 768 bytes
```

#### Record Header (8 bytes)

```
Offset  Size  Type          Field
───────────────────────────────────
0       4     volatile i32  length       (negative = uncommitted, positive = committed)
4       4     i32           msg_type_id  (≥1 for valid, PADDING = -1)
```

Records are aligned to 8 bytes. Max message length = `capacity / 8`.

#### Write Algorithm (Multi-Producer)

```
fn write(rb: *RingBuffer, msg_type_id: i32, payload: []const u8) !void {
    const record_length = payload.len + HEADER_LENGTH;
    const aligned_length = ALIGN(record_length, 8);

    // Claim space via CAS loop on tail_position
    while (true) {
        const head = @atomicLoad(i64, &rb.trailer.head_cache, .acquire);
        const tail = @atomicLoad(i64, &rb.trailer.tail_position, .acquire);
        const available = rb.capacity - (tail - head);

        const tail_index = @intCast(usize, tail & (rb.capacity - 1));
        const to_end = rb.capacity - tail_index;
        var required = aligned_length;
        var padding: usize = 0;

        if (required > to_end) {
            // Wrap: need padding to end + space at start
            padding = to_end;
            required = aligned_length + padding;
            // Re-read actual head if stale cache
            if (available < required) {
                const real_head = @atomicLoad(i64, &rb.trailer.head_position, .acquire);
                @atomicStore(i64, &rb.trailer.head_cache, real_head, .release);
                if (rb.capacity - (tail - real_head) < required) return error.BufferFull;
            }
        } else if (available < required) {
            const real_head = @atomicLoad(i64, &rb.trailer.head_position, .acquire);
            @atomicStore(i64, &rb.trailer.head_cache, real_head, .release);
            if (rb.capacity - (tail - real_head) < required) return error.BufferFull;
        }

        if (@cmpxchgWeak(i64, &rb.trailer.tail_position, tail, tail + required, .acquire, .monotonic) == null) {
            // Claimed! Write padding if wrapping
            if (padding > 0) {
                write_padding_record(rb.buffer, tail_index, padding);
            }
            // Write message record
            const record_index = (tail + padding) & (rb.capacity - 1);
            write_record(rb.buffer, record_index, msg_type_id, payload, record_length);
            return;
        }
        // CAS failed, retry
    }
}
```

#### Read Algorithm (Single Consumer)

```
fn read(rb: *RingBuffer, handler: HandlerFn, limit: u32) u32 {
    const head = rb.trailer.head_position;  // plain read (single consumer)
    const head_index = head & (rb.capacity - 1);
    var bytes_consumed: usize = 0;
    var messages_read: u32 = 0;

    while (bytes_consumed < rb.capacity and messages_read < limit) {
        const idx = head_index + bytes_consumed;
        const length = @atomicLoad(i32, &rb.buffer[idx].length, .acquire);
        if (length <= 0) break;  // uncommitted or empty

        const aligned = ALIGN(length, 8);
        bytes_consumed += aligned;

        if (rb.buffer[idx].msg_type_id != PADDING_MSG_TYPE_ID) {
            handler(rb.buffer[idx].msg_type_id, rb.buffer[idx + 8 ..][0 .. length - 8]);
            messages_read += 1;
        }
    }

    if (bytes_consumed > 0) {
        @memset(rb.buffer[head_index..][0..bytes_consumed], 0);
        @atomicStore(i64, &rb.trailer.head_position, head + bytes_consumed, .release);
    }

    return messages_read;
}
```

#### Dead Producer Recovery

If a producer dies mid-write (negative length at head):
1. Read the length value. If negative, convert to padding: set `msg_type_id = PADDING`, flip length positive.
2. If zero length at head but non-zero further ahead, insert padding spanning the gap.

### 5.3 Counters

Shared atomic `i64` values for monitoring, aligned to 128 bytes (2 cache lines) to prevent false sharing.

```
┌────────────────────────────┐
│  Counter Value (128 bytes) │
│  +0:  value (volatile i64) │
│  +8:  padding[120]         │
├────────────────────────────┤
│  Counter Meta (256 bytes)  │
│  +0:  state (volatile i32) │  (0=UNUSED, 1=ALLOCATED, -1=RECLAIMED)
│  +4:  type_id (i32)        │
│  +8:  label_len (i32)      │
│  +12: label[244]           │
└────────────────────────────┘
```

Counters are allocated from a flat buffer. Max counter ID = `values_buffer_length / 128 - 1`.

---

## 6. Service ↔ Broker IPC

### 6.1 Overview

```
┌──────────────┐                                    ┌──────────────┐
│   Service    │                                    │   Broker     │
│              │                                    │              │
│  ┌────────┐  │     (1) mmap broker_0.dat          │  ┌────────┐  │
│  │ Control │◄├────────────────────────────────────┤► │ Control │  │
│  │ Writer  │  │     writes to broker's             │  │ Reader  │  │
│  └────────┘  │     control ring buffer             │  └────────┘  │
│              │                                    │              │
│  ┌────────┐  │     (2) mmap broker_0.dat          │  ┌────────┐  │
│  │ Send   │◄├────────────────────────────────────┤► │ Send   │  │
│  │ Writer  │  │     writes cross-host msgs to      │  │ Reader  │  │
│  └────────┘  │     broker's send ring buffer       │  └────────┘  │
│              │                                    │              │
│  ┌────────┐  │     (3) broker mmaps service.dat    │  ┌────────┐  │
│  │ Control │◄├────────────────────────────────────┤► │ Control │  │
│  │ Reader  │  │     broker writes to service's     │  │ Writer  │  │
│  └────────┘  │     control ring buffer             │  └────────┘  │
│              │                                    │              │
│  ┌────────┐  │     (4) broker mmaps service.dat    │  ┌────────┐  │
│  │ Msg    │◄├────────────────────────────────────┤► │ Msg    │  │
│  │ Reader  │  │     broker writes received msgs    │  │ Writer  │  │
│  └────────┘  │     to service's message buffer     │  └────────┘  │
└──────────────┘                                    └──────────────┘
```

### 6.2 Same-Host Message Path (Zero Broker Involvement)

When `targetNodeId == sourceNodeId`:

```
Service A                           Service B
┌──────────┐                        ┌──────────┐
│          │   mmap service_B.dat   │          │
│  Writer ─┼───────────────────────►│ Messages │
│          │   write to B's         │ Ring Buf │
│          │   messages ring buffer │          │
└──────────┘                        └──────────┘
```

1. Service A resolves Service B as local (same `nodeId`).
2. Service A maps Service B's metadata file.
3. Service A writes directly to Service B's messages ring buffer via `IpcProducer`.
4. **The broker is not involved.** This is the lowest-latency path.

### 6.3 Cross-Host Message Path

When `targetNodeId != sourceNodeId`:

```
Service A          Broker (Node 1)                    Broker (Node 2)         Service B
┌────────┐        ┌──────────────┐                   ┌──────────────┐        ┌────────┐
│        │  IPC   │ Send Ring    │       UDP          │ Recv Log     │  IPC   │        │
│ Writer ┼───────►│ Buffer       │──────────────────►│ Buffer       │───────►│ Msgs   │
│        │        │ (MPSC)       │   Data Frames      │ (node 1)     │        │ Ring   │
│        │        │              │                   │              │        │ Buffer │
└────────┘        └──────────────┘                   └──────────────┘        └────────┘
```

1. Service A writes to Broker 1's **send ring buffer** with `targetNodeId=2, targetServiceId=B`.
2. Broker 1's **Sender Event Loop** drains the send ring buffer.
3. Sender looks up the UDP endpoint for `nodeId=2`.
4. Sender transmits the message as a UDP data frame.
5. Broker 2's **Receiver Event Loop** receives the UDP frame.
6. Receiver inserts the frame into the **receive log buffer** for node 1.
7. Broker 2's **Receiver Event Loop** (or a routing step) reads `targetServiceId` from the BRZ header.
8. Broker 2 writes the message payload to Service B's messages ring buffer.

### 6.4 Control Message Flow

All control messages use a hardcoded wire protocol with a `templateId` for dispatch:

| templateId | Message | Direction |
|---|---|---|
| 1 | `RegisterService` | Service → Broker (broker's control RB) |
| 2 | `RegistrationResponse` | Broker → Service (service's control RB) |
| 3 | `SubscribeToServiceUpdates` | Service → Broker (broker's control RB) |
| 4 | `ServiceInstances` | Broker → Service (service's control RB) |
| 5 | `UnregisterService` | Service → Broker (broker's control RB) |
| 6 | `LeaderChanged` | Broker → Service (service's control RB) |

---

## 7. UDP Wire Protocol

All on-wire frames use **little-endian** byte order and are aligned to 4 bytes.

### 7.1 Common Frame Header (8 bytes)

```
Offset  Size  Type          Field
──────────────────────────────────
0       4     volatile i32  frame_length   (total frame size including header)
4       1     u8            version        (always 0)
5       1     u8            flags
6       2     u16           frame_type
```

### 7.2 Frame Types

| Value | Name | Description |
|---|---|---|
| `0x00` | `PAD` | Padding frame (skip) |
| `0x01` | `DATA` | Data frame carrying a BRZ message |
| `0x02` | `NAK` | Negative acknowledgement (retransmit request) |
| `0x03` | `SM` | Status message (flow control + receiver window) |
| `0x04` | `SETUP` | Connection establishment |
| `0x05` | `HEARTBEAT` | Keepalive (zero-length data frame) |

**Compared to Aeron:** BRZ eliminates RTT measurement frames (RTTM), error frames, resolution frames, and ATS frames. RTT measurement can be added later if adaptive congestion control is needed.

### 7.3 Data Frame Header (40 bytes)

The data frame header combines Aeron's transport header with BRZ routing fields, eliminating the need for a separate BRZ message header inside the payload:

```
Offset  Size  Type    Field
──────────────────────────────────
0       4     i32     frame_length
4       1     u8      version (0)
5       1     u8      flags
6       2     u16     frame_type (DATA = 0x01)
── transport fields ──
8       4     i32     term_offset          ← position within send buffer
12      1     u8      source_node_id
13      1     u8      target_node_id
14      2     u16     source_service_id
16      2     u16     target_service_id
── routing fields ──
18      2     u16     template_id          ← message type (0 = raw user message)
20      4     i32     correlation_id       ← for request-response matching
24      1     u8      msg_flags            ← BRZ-specific flags (chunked, etc.)
25      7     u8[7]   reserved
32      8     i64     sequence_number      ← monotonic per source→target link
40      ...   bytes   payload
```

**Total header: 40 bytes**, aligned to 8 bytes.

**Flags field (byte 5):**

| Flag | Bit | Meaning |
|---|---|---|
| `BEGIN` | `0x80` | First fragment of a fragmented message |
| `END` | `0x40` | Last fragment of a fragmented message |
| `UNFRAGMENTED` | `0xC0` | `BEGIN \| END` — complete message in one frame |
| `ADMIN` | `0x20` | Admin/cluster message (not routed to services) |

**Key design choice:** By integrating `nodeId`/`serviceId` into the transport header (instead of keeping them in a separate BRZ header inside the payload), we avoid double-parsing on the receive path. The receiver can route the message with a single read of the 40-byte header.

### 7.4 Setup Frame (24 bytes)

Sent by a broker to establish a connection with a peer:

```
Offset  Size  Type    Field
──────────────────────────────────
0       8     ...     frame_header (frame_type = SETUP)
8       1     u8      source_node_id
9       1     u8      reserved
10      2     u16     reserved
12      4     i32     log_buffer_length     ← receiver's log buffer size
16      4     i32     mtu_length            ← maximum transmission unit
20      4     i32     initial_sequence      ← starting sequence number
```

### 7.5 Status Message (28 bytes)

Sent by receivers back to senders for flow control:

```
Offset  Size  Type    Field
──────────────────────────────────
0       8     ...     frame_header (frame_type = SM)
8       1     u8      node_id               ← receiver's node ID
9       1     u8      reserved
10      2     u16     reserved
12      8     i64     consumption_position   ← how far the receiver has consumed
20      4     i32     receiver_window        ← how far ahead the sender may write
24      4     i32     reserved
```

### 7.6 NAK Frame (24 bytes)

Sent by receivers requesting retransmission:

```
Offset  Size  Type    Field
──────────────────────────────────
0       8     ...     frame_header (frame_type = NAK)
8       1     u8      node_id
9       1     u8      reserved
10      2     u16     reserved
12      8     i64     position               ← start of missing data
20      4     i32     length                 ← length of missing data
```

### 7.7 Heartbeat

When no data is available, the sender sends **zero-length data frames** every 100ms to maintain the connection:

```
frame_length = 40  (header only, no payload)
flags        = UNFRAGMENTED
frame_type   = DATA
sequence_number = current_sequence  (no increment)
```

The receiver uses heartbeats to:
1. Confirm the sender is alive
2. Track the sender's current position (for gap detection)

---

## 8. Send Path Architecture

### 8.1 Overview

```
┌────────────┐  ┌────────────┐  ┌────────────┐
│ Service A  │  │ Service B  │  │ Service C  │
│  (writer)  │  │  (writer)  │  │  (writer)  │
└─────┬──────┘  └─────┬──────┘  └─────┬──────┘
      │               │               │
      └───────────────┼───────────────┘
                      │
                      ▼
         ┌────────────────────────┐
         │  Send Ring Buffer      │
         │  (MPSC, in broker's    │
         │   metadata file)       │
         └────────────┬───────────┘
                      │
                      ▼
         ┌────────────────────────┐
         │  Sender Event Loop     │
         │  (single consumer)     │
         │                        │
         │  1. Read from ring buf │
         │  2. Parse targetNodeId │
         │  3. Lookup peer socket │
         │  4. sendmsg() / batch  │
         └────────────┬───────────┘
                      │
                      ▼
                 UDP Socket(s)
                 (one per peer)
```

### 8.2 Send Ring Buffer vs. Aeron Log Buffer

**Aeron approach:** Each publication has its own log buffer file (3 term partitions). The sender event loop scans the active term for new data using the `term_tail_counter`. Multiple concurrent publications each have their own buffer.

**BRZ approach:** A single MPSC ring buffer serves as the outbound queue for all local services. This is simpler and sufficient because:

1. **Single sender thread** drains the buffer. No concurrent readers.
2. **Messages are small** relative to buffer size (typically < 16KB, often < 1KB).
3. **No zero-copy needed** on the send path — the message must be copied into a UDP packet anyway.
4. **Routing multiplexing** is trivial: each message already contains `targetNodeId` in its header.

**Trade-off:** The MPSC ring buffer introduces CAS contention when many services write concurrently. This is acceptable because:
- Cross-host messages are the minority case (same-host IPC bypasses the broker entirely)
- The CAS is on the tail position only; producers rarely contend in practice
- The alternative (per-service send buffers) would require the sender to poll N buffers, adding latency

### 8.3 Sender Event Loop Duty Cycle

```
fn sender_do_work(sender: *SenderEventLoop) u32 {
    var work_count: u32 = 0;
    const now_ns = monotonic_clock();

    // 1. Drain send ring buffer (batch of messages)
    work_count += send_ring_buffer.read(on_outbound_message, SEND_BATCH_LIMIT);

    // 2. Flush any pending UDP sends (sendmmsg batching)
    work_count += flush_pending_sends();

    // 3. Process incoming control messages (Status Messages, NAKs)
    for (peer_sockets) |socket| {
        work_count += poll_control_messages(socket, now_ns);
    }

    // 4. Send heartbeats to peers (every 100ms)
    if (now_ns > next_heartbeat_ns) {
        send_heartbeats_to_all_peers();
        next_heartbeat_ns = now_ns + HEARTBEAT_INTERVAL_NS;
    }

    // 5. Process retransmit timeouts
    retransmit_handler.process_timeouts(now_ns);

    return work_count;
}
```

### 8.4 Message Fragmentation

Messages larger than `mtu_length - DATA_HEADER_LENGTH` are fragmented:

```
max_payload = mtu_length - 40  (40-byte header)

fn fragment_and_send(payload: []const u8, header: DataFrameHeader) void {
    var offset: usize = 0;
    var is_first = true;

    while (offset < payload.len) {
        const chunk_len = @min(payload.len - offset, max_payload);
        const is_last = (offset + chunk_len == payload.len);

        var flags: u8 = 0;
        if (is_first) flags |= BEGIN;
        if (is_last) flags |= END;

        send_frame(header, flags, payload[offset..][0..chunk_len]);
        offset += chunk_len;
        is_first = false;
    }
}
```

### 8.5 Retransmit Buffer

The sender must retain recently sent data for retransmission. Rather than using Aeron's log buffer (which is the publication's term buffer), BRZ uses a **dedicated retransmit ring buffer** per peer:

```
┌─────────────────────────────────┐
│  Retransmit Buffer              │
│  (per peer, 4MB default)        │
│                                 │
│  Circular buffer of recently    │
│  sent frames, indexed by        │
│  sequence_number.               │
│                                 │
│  Single writer (sender event loop). │
│  Frames are overwritten when    │
│  the buffer wraps.              │
└─────────────────────────────────┘
```

On receiving a NAK for `position` + `length`, the sender looks up the corresponding frames in the retransmit buffer and resends them.

---

## 9. Receive Path Architecture

### 9.1 Overview

```
                 UDP Socket
                 (bound to broker's host:port)
                      │
                      ▼
         ┌────────────────────────┐
         │  Receiver Event Loop   │
         │  (single thread)       │
         │                        │
         │  1. recvmmsg()         │
         │  2. Parse frame header │
         │  3. Insert into recv   │
         │     log buffer         │
         │  4. Route message to   │
         │     target service     │
         └────────────┬───────────┘
                      │
                      ▼
         ┌────────────────────────┐
         │  Target Service's      │
         │  Messages Ring Buffer  │
         │  (direct write)        │
         └────────────────────────┘
```

### 9.2 Receive Log Buffer Design

Unlike Aeron, where images write data into a log buffer and the application polls it later, BRZ's receive path has the broker **immediately route** each received message to its target service. The receive log buffer serves primarily as:

1. **Gap tracking:** Detect missing frames for NAK generation
2. **Reassembly:** Assemble fragmented messages before routing
3. **Ordering:** Ensure messages are delivered in sequence order

```
Receive Log Buffer (per peer):

Position:  0        1024      2048      3072      4096
           ┌─────────┬─────────┬─────────┬─────────┬───
           │ Frame 1 │ Frame 2 │  (gap)  │ Frame 4 │ ...
           │ seq=0   │ seq=1   │ seq=2?  │ seq=3   │
           └─────────┴─────────┴─────────┴─────────┴───
                                    ▲
                                    │
                              Loss detected:
                              NAK sent for seq=2
```

### 9.3 Packet Insertion

```
fn insert_packet(log: *ReceiveLogBuffer, frame: []const u8) void {
    const header = parse_data_header(frame);
    const position = header.sequence_number * max_frame_size;
    const offset = position & (log.capacity - 1);

    // Only write if slot is empty (idempotent for retransmits)
    if (@atomicLoad(i32, &log.data[offset].frame_length, .acquire) == 0) {
        // Copy payload first, then header fields, then frame_length LAST
        @memcpy(log.data[offset + 40 ..], frame[40..]);
        // Write header fields (backwards order for cache friendliness)
        write_header_fields(log.data[offset..], header);
        // Commit: write frame_length with release semantics
        @atomicStore(i32, &log.data[offset].frame_length, header.frame_length, .release);
    }

    // Update high-water mark
    propose_max(&log.metadata.tail_position, position + ALIGN(frame.len, 32));
}
```

### 9.4 Receiver Event Loop Duty Cycle

```
fn receiver_do_work(receiver: *ReceiverEventLoop) u32 {
    var work_count: u32 = 0;
    const now_ns = monotonic_clock();

    // 1. Receive UDP packets (batch via recvmmsg)
    const received = recvmmsg(receiver.socket, &recv_buffers, RECV_BATCH_LIMIT);
    for (recv_buffers[0..received]) |buf| {
        const frame_type = parse_frame_type(buf);
        switch (frame_type) {
            .DATA => {
                const header = parse_data_header(buf);
                const peer = lookup_peer(header.source_node_id);

                // Insert into receive log buffer
                peer.recv_log.insert_packet(buf);
                work_count += 1;

                // Immediate routing: write to target service
                if (header.flags & ADMIN != 0) {
                    handle_admin_message(header, buf[40..]);
                } else {
                    route_to_service(header.target_service_id, buf);
                }
            },
            .SETUP => handle_setup(buf),
            .SM    => handle_status_message(buf),
            .NAK   => handle_nak(buf),
            else   => {},  // ignore unknown
        }
    }

    // 2. Scan for losses and send NAKs
    for (receiver.peers) |peer| {
        work_count += peer.loss_detector.scan(peer.recv_log, now_ns);
    }

    // 3. Send Status Messages (rate-limited)
    if (now_ns > next_sm_ns) {
        send_status_messages_to_all_peers();
        next_sm_ns = now_ns + SM_INTERVAL_NS;
    }

    return work_count;
}
```

### 9.5 Message Routing (Receive → Service)

When the receiver has a complete, unfragmented message (or has reassembled all fragments):

```
fn route_to_service(target_service_id: u16, frame: []const u8) void {
    const service = service_registry.lookup(target_service_id);
    if (service == null) {
        increment_counter(.unknown_service_drops);
        return;
    }

    const payload = frame[40..];  // skip data frame header

    // Write to service's messages ring buffer (MPSC write, broker is one of N writers)
    const result = service.messages_ring_buffer.write(MSG_TYPE_APPLICATION, payload);
    if (result == error.BufferFull) {
        increment_counter(.service_back_pressure);
        // Message is lost — the service is too slow
        // Future: configurable back-pressure strategy (drop vs. block vs. retry)
    }
}
```

### 9.6 Fragment Reassembly

For fragmented messages, the receiver accumulates fragments before routing:

```
struct FragmentAssembler {
    buffer: GrowableBuffer,
    expected_sequence: i64,
    source_header: DataFrameHeader,  // captured from BEGIN fragment

    fn on_fragment(self: *FragmentAssembler, header: DataFrameHeader, payload: []const u8) ?[]const u8 {
        if (header.flags & BEGIN != 0) {
            self.buffer.reset();
            self.source_header = header;
            self.buffer.append(payload);
            self.expected_sequence = header.sequence_number + 1;
            if (header.flags & END != 0) {
                return self.buffer.slice();  // unfragmented
            }
            return null;
        }

        if (header.sequence_number != self.expected_sequence) {
            self.buffer.reset();  // out-of-order, discard
            return null;
        }

        self.buffer.append(payload);
        self.expected_sequence += 1;

        if (header.flags & END != 0) {
            return self.buffer.slice();  // complete message
        }
        return null;  // more fragments expected
    }
};
```

---

## 10. Flow Control

### 10.1 Simplified Model

BRZ uses a **single-receiver, window-based** flow control model. Each peer link has exactly one sender and one receiver.

```
Sender (Broker A)                          Receiver (Broker B)
┌──────────────────┐                      ┌──────────────────┐
│                  │   DATA frames        │                  │
│  send_position ──┼─────────────────────►│  recv_position   │
│                  │                      │                  │
│  send_limit    ◄─┼──────────────────────┼─ Status Message  │
│                  │  (consumption_pos    │  (window)        │
│                  │   + receiver_window) │                  │
└──────────────────┘                      └──────────────────┘

Invariant: send_position <= send_limit
           send_limit = consumption_position + receiver_window
```

### 10.2 Receiver Window Calculation

The receiver window reflects how much buffer space is available:

```
fn calculate_receiver_window(peer: *PeerState) i32 {
    const log_capacity = peer.recv_log.capacity;
    const consumed = peer.consumption_position;
    const hwm = peer.recv_log.metadata.tail_position;
    const buffered = hwm - consumed;

    // Window = log capacity minus buffered data, clamped
    const available = @intCast(i32, @max(0, log_capacity - buffered));

    // Never advertise more than half the buffer
    return @min(available, @intCast(i32, log_capacity / 2));
}
```

### 10.3 Back-Pressure Propagation

```
Service A → Broker 1 send RB → UDP → Broker 2 recv log → Service B msg RB

Back-pressure flows backwards:

1. Service B's message ring buffer is full
   → Broker 2 cannot route messages to Service B
   → Broker 2's recv log fills up
   → Broker 2 advertises smaller receiver_window in SM

2. Broker 1 sees reduced send_limit
   → Broker 1's sender event loop stops draining send ring buffer
   → Send ring buffer fills up

3. Service A's write to send ring buffer returns BufferFull
   → Service A's producer returns back-pressure error to application
```

### 10.4 Status Message Timing

| Trigger | Action |
|---|---|
| Consumer position advanced by ≥ `window / 4` | Send SM immediately |
| No SM sent for `sm_timeout` (200ms) | Send SM |
| Loss detected | Send SM with `SEND_SETUP` flag |
| Connection established | Send SM with initial window |

---

## 11. Loss Detection & Retransmission

### 11.1 Loss Detection (Receiver Side)

The receiver scans the receive log buffer for gaps (frames with `frame_length == 0` between non-zero frames):

```
struct LossDetector {
    active_gap_start: ?i64,
    active_gap_length: ?i32,
    nak_expiry_ns: i64,

    fn scan(self: *LossDetector, log: *ReceiveLogBuffer, now_ns: i64) u32 {
        var work_count: u32 = 0;
        const rebuild_pos = log.metadata.rebuild_position;
        const hwm = log.metadata.tail_position;

        if (rebuild_pos >= hwm) return 0;

        // Walk frames from rebuild_position
        var offset = rebuild_pos;
        while (offset < hwm) {
            const idx = offset & (log.capacity - 1);
            const frame_length = @atomicLoad(i32, &log.data[idx].frame_length, .acquire);

            if (frame_length == 0) {
                // Gap found
                const gap_start = offset;
                // Scan forward to find gap end
                const gap_end = find_gap_end(log, offset, hwm);
                const gap_length = gap_end - gap_start;

                if (self.active_gap_start != gap_start) {
                    // New gap — set initial NAK delay
                    self.active_gap_start = gap_start;
                    self.active_gap_length = gap_length;
                    self.nak_expiry_ns = now_ns + NAK_INITIAL_DELAY_NS;
                }

                if (now_ns >= self.nak_expiry_ns) {
                    send_nak(gap_start, gap_length);
                    self.nak_expiry_ns = now_ns + NAK_RETRY_DELAY_NS;
                    work_count += 1;
                }
                break;  // only track one gap at a time
            }

            offset += ALIGN(frame_length, 32);
        }

        return work_count;
    }
};
```

### 11.2 Retransmission (Sender Side)

```
struct RetransmitHandler {
    const State = enum { INACTIVE, DELAYED, LINGERING };

    state: State,
    position: i64,
    length: i32,
    expiry_ns: i64,

    fn on_nak(self: *RetransmitHandler, position: i64, length: i32, now_ns: i64) void {
        if (self.state != .INACTIVE and overlaps(self, position, length)) {
            return;  // suppress duplicate NAK
        }

        // Immediate retransmit for unicast (no delay)
        resend_from_retransmit_buffer(position, length);
        self.state = .LINGERING;
        self.position = position;
        self.length = length;
        self.expiry_ns = now_ns + RETRANSMIT_LINGER_NS;
    }

    fn process_timeouts(self: *RetransmitHandler, now_ns: i64) void {
        if (self.state == .LINGERING and now_ns >= self.expiry_ns) {
            self.state = .INACTIVE;
        }
    }
};
```

### 11.3 NAK Timing Constants

| Parameter | Value | Rationale |
|---|---|---|
| `NAK_INITIAL_DELAY_NS` | 60ms | Allow reordering before NAK |
| `NAK_RETRY_DELAY_NS` | 60ms | Retry if retransmit not received |
| `RETRANSMIT_LINGER_NS` | 10µs | Suppress duplicate NAKs briefly |

These are unicast-optimized values. Multicast NAK suppression is not needed since BRZ uses unicast only.

---

## 12. Threading Model

### 12.1 Thread Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Broker Process                        │
│                                                         │
│  Thread 1: Control Loop                                 │
│  ┌───────────────────────────────────────────────────┐  │
│  │  • Poll broker's control ring buffer              │  │
│  │  • Service registration / deregistration          │  │
│  │  • Heartbeat checking (every 3s)                  │  │
│  │  • Service discovery (notify subscribers)         │  │
│  │  • Cluster state management                       │  │
│  │  • Leader election                                │  │
│  │  • Counter updates                                │  │
│  │  • Scheduled tasks                                │  │
│  └───────────────────────────────────────────────────┘  │
│                                                         │
│  Thread 2: Sender Event Loop                            │
│  ┌───────────────────────────────────────────────────┐  │
│  │  • Drain send ring buffer (outbound messages)     │  │
│  │  • UDP sendmsg/sendmmsg to peer brokers           │  │
│  │  • Process incoming SMs and NAKs                  │  │
│  │  • Send heartbeats (zero-length DATA every 100ms) │  │
│  │  • Retransmission handling                        │  │
│  └───────────────────────────────────────────────────┘  │
│                                                         │
│  Thread 3: Receiver Event Loop                          │
│  ┌───────────────────────────────────────────────────┐  │
│  │  • UDP recvmmsg from all peer brokers             │  │
│  │  • Insert packets into receive log buffers        │  │
│  │  • Route messages to target service ring buffers  │  │
│  │  • Loss detection and NAK sending                 │  │
│  │  • Send Status Messages                           │  │
│  │  • Handle admin messages (cluster protocol)       │  │
│  └───────────────────────────────────────────────────┘  │
│                                                         │
└─────────────────────────────────────────────────────────┘
```

### 12.2 Inter-Event-Loop Communication

Event loops never share mutable state directly. They communicate through **MPSC command queues**:

```
┌──────────────┐                     ┌───────────┐
│ Control Loop │ ── command queue ──►│  Sender   │
│              │ (add/remove peer,   │           │
│              │  update endpoints)  │           │
└──────────────┘                     └───────────┘
      │                                    │
      │                                    │
      │          ┌───────────┐             │
      └─────────►│ Receiver  │◄────────────┘
                 │           │   (via shared command queue
                 └───────────┘    from control loop)
```

| From | To | Queue | Commands |
|---|---|---|---|
| Control Loop → Sender | `sender_cmd_queue` | Add peer endpoint, remove peer endpoint, update flow control |
| Control Loop → Receiver | `receiver_cmd_queue` | Add peer, remove peer, update service registry |
| Receiver → Control Loop | `control_loop_cmd_queue` | Peer connected (setup received), peer disconnected, admin messages |
| Sender → Control Loop | `control_loop_cmd_queue` | Send errors, peer unreachable |

Command queues use self-dispatching function pointers (as in Aeron):

```
const Command = struct {
    handler: *const fn (event_loop_state: *anyopaque, cmd: *Command) void,
    data: *anyopaque,
};
```

### 12.3 Threading Modes

| Mode | Threads | Use Case |
|---|---|---|
| **DEDICATED** (default) | 3 | Production — lowest latency |
| **SHARED_NETWORK** | 2 | Control Loop + combined Sender/Receiver |
| **SHARED** | 1 | All event loops on one thread — simplest, for testing |

In `SHARED` mode, command queues are bypassed; event loops call handler functions directly.

### 12.4 Event Loop Runner

```
const EventLoopRunner = struct {
    role_name: []const u8,
    event_loop: *EventLoop,
    idle_strategy: IdleStrategy,
    running: std.atomic.Value(bool),

    fn run(self: *EventLoopRunner) void {
        set_thread_name(self.role_name);
        while (self.running.load(.acquire)) {
            const work_count = self.event_loop.do_work();
            self.idle_strategy.idle(work_count);
        }
        self.event_loop.on_close();
    }
};
```

### 12.5 Idle Strategies

| Strategy | Behavior | Use Case |
|---|---|---|
| `busy_spin` | CPU pause instruction when idle | Lowest latency, highest CPU |
| `yielding` | `sched_yield()` when idle | Low latency, some CPU sharing |
| `sleeping` | `nanosleep(1ns)` when idle | Balanced |
| `backoff` | Spin → yield → sleep (exponential) | Production default |
| `blocking` | Kernel futex/ulock wait | Lowest CPU, higher latency |

---

## 13. Broker Control Loop

### 13.1 Control Loop Duty Cycle

```
fn control_loop_do_work(control_loop: *ControlLoop) u32 {
    var work_count: u32 = 0;
    const now_ns = monotonic_clock();

    // 1. Process inter-event-loop commands (1 per cycle)
    work_count += control_loop.cmd_queue.drain(dispatch_command, 1);

    // 2. Poll broker's control ring buffer (service registrations, subscriptions)
    work_count += control_loop.control_rb.read(on_control_message, CONTROL_READ_LIMIT);

    // 3. Periodic tasks (rate-limited to every 1s)
    if (now_ns > control_loop.next_timeout_check_ns) {

        // Heartbeat checking (every 3s)
        if (now_ns > control_loop.next_heartbeat_check_ns) {
            control_loop.check_service_heartbeats(now_ns);
            control_loop.next_heartbeat_check_ns = now_ns + HEARTBEAT_CHECK_INTERVAL_NS;
        }

        // Cluster protocol tasks
        control_loop.cluster_manager.do_work(now_ns);

        control_loop.next_timeout_check_ns = now_ns + TIMEOUT_CHECK_INTERVAL_NS;
    }

    // 4. Update counters
    control_loop.update_counters();

    return work_count;
}
```

### 13.2 Control Message Dispatch

```
fn on_control_message(msg_type_id: i32, payload: []const u8) void {
    const template_id = parse_template_id(payload);
    switch (template_id) {
        1 => handle_register_service(payload),
        3 => handle_subscribe_to_service_updates(payload),
        5 => handle_unregister_service(payload),
        else => log_unknown_template(template_id),
    }
}
```

---

## 14. Message Routing Engine

### 14.1 Routing Decision

```
fn route_message(header: DataFrameHeader, payload: []const u8) void {
    if (header.target_node_id == local_node_id) {
        // Local delivery
        route_to_local_service(header.target_service_id, payload);
    } else {
        // Should not normally happen (services should write to send RB)
        // But handle gracefully: forward to send ring buffer
        forward_to_send_buffer(header, payload);
    }
}

fn route_to_local_service(service_id: u16, payload: []const u8) void {
    const service = service_registry.get(service_id) orelse {
        counters.increment(.unknown_service_drops);
        return;
    };
    _ = service.messages_rb.write(MSG_TYPE_APPLICATION, payload) catch {
        counters.increment(.service_back_pressure_events);
    };
}
```

### 14.2 Admin Message Routing

Admin messages (cluster protocol) are identified by the `ADMIN` flag in the frame header:

```
fn handle_admin_message(header: DataFrameHeader, payload: []const u8) void {
    const template_id = header.template_id;
    switch (template_id) {
        // Broker cluster protocol
        1 => handle_initiate_election(payload),
        2 => handle_node_acknowledgment(payload),
        3 => handle_leader_announcement(payload),
        4 => handle_broker_heartbeat(payload),
        5 => handle_cluster_state_snapshot(payload),
        6 => handle_service_added(payload),
        7 => handle_service_removed(payload),
        8 => handle_service_leader_designated(payload),
        else => log_unknown_admin_template(template_id),
    }
}
```

---

## 15. Service Registration & Discovery

### 15.1 Registration Flow

```
   Service                      Broker Control Loop
      │                              │
      │  1. Create metadata file     │
      │  2. Map broker's metadata    │
      │                              │
      │  RegisterService             │
      ├─────────────────────────────►│
      │  (templateId=1,              │  3. Register in ServiceRegistry
      │   serviceId, serviceName,    │  4. Map service's metadata file
      │   leaderElectionEnabled)     │  5. Evaluate service leader
      │                              │  6. Broadcast ServiceAdded to peers
      │         RegistrationResponse │  7. Notify local subscribers
      │◄─────────────────────────────┤
      │  (templateId=2,              │
      │   serviceId, nodeId,         │
      │   isLeader)                  │
      │                              │
      │  Start heartbeat writes      │
      │                              │
```

### 15.2 Service Registry

```
const ServiceRegistry = struct {
    /// (serviceId, nodeId) → ServiceInstance
    instances: BiIntMap(ServiceInstance),

    /// serviceName → Set<subscriberServiceId>
    subscriptions: StringHashMap(IntHashSet),

    /// serviceId → BuffersProvider (mapped service metadata)
    local_buffers: IntHashMap(*BuffersProvider),
};
```

### 15.3 Discovery Protocol

```
   Service A                    Broker
      │                           │
      │  SubscribeToServiceUpdates│
      ├──────────────────────────►│
      │  (localServiceId=1,       │  Register subscription
      │   remoteServiceName=      │  Check existing instances
      │   "service-b")            │
      │                           │
      │       ServiceInstances    │
      │◄──────────────────────────┤  (all known instances of "service-b")
      │  [{serviceId=4, nodeId=1},│
      │   {serviceId=7, nodeId=2}]│
      │                           │
      │  ... later, new instance  │
      │  registers ...            │
      │                           │
      │       ServiceInstances    │
      │◄──────────────────────────┤  (updated complete list)
      │  [{serviceId=4, nodeId=1},│
      │   {serviceId=7, nodeId=2},│
      │   {serviceId=9, nodeId=2}]│
      │                           │
```

---

## 16. Heartbeat & Health Checking

### 16.1 Service Heartbeat

Services write current epoch milliseconds to their metadata file every ~1 second:

```
// Service side (volatile store, no ring buffer)
@atomicStore(i64, &metadata.heartbeat_time_ms, epoch_ms(), .release);
```

### 16.2 Broker Health Checking

```
fn check_service_heartbeats(control_loop: *ControlLoop, now_ns: i64) void {
    const now_ms = now_ns / 1_000_000;

    for (control_loop.service_registry.local_services()) |service| {
        const last_heartbeat = @atomicLoad(
            i64,
            &service.buffers.metadata.heartbeat_time_ms,
            .acquire,
        );
        const elapsed = now_ms - last_heartbeat;

        if (elapsed > HEARTBEAT_TIMEOUT_MS) {
            handle_service_removed(service);
        }
    }
}

fn handle_service_removed(service: *ServiceInstance) void {
    // 1. Unregister from ServiceRegistry
    service_registry.remove(service.id, local_node_id);

    // 2. Close BuffersProvider
    service.buffers.close();

    // 3. Broadcast ServiceRemoved to peer brokers
    broadcast_admin(service_removed_message(service));

    // 4. Notify local subscribers
    notify_subscribers(service.name);

    // 5. Re-evaluate service leader election
    if (service.leader_election_enabled) {
        evaluate_service_leader(service.name);
    }
}
```

### 16.3 Broker-to-Broker Heartbeat

Sent via admin stream (DATA frames with `ADMIN` flag) every ~1 second. Used for liveness tracking alongside connection lifecycle events.

### 16.4 Timeout Constants

| Parameter | Value |
|---|---|
| Service heartbeat write interval | ~1 second |
| Broker health check interval | 3 seconds |
| Service heartbeat timeout | 10 seconds |
| Broker-to-broker heartbeat interval | ~1 second |
| UDP heartbeat (zero-length DATA) | 100ms |

---

## 17. Cluster Management

### 17.1 Broker Leader Election (Bully Algorithm)

```
Election trigger: peer connection established or lost

        Broker A (nodeId=1)      Broker B (nodeId=2)      Broker C (nodeId=3)
              │                        │                        │
              │  InitiateElection(1)   │                        │
              ├───────────────────────►├───────────────────────►│
              │                        │                        │
              │                        │  InitiateElection(2)   │
              │◄───────────────────────┼───────────────────────►│
              │                        │                        │
              │  NodeAcknowledgment(1) │                        │
              ├───────────────────────►├───────────────────────►│
              │  (I have lower ID,     │                        │
              │   I have priority)     │                        │
              │                        │                        │
              │  LeaderAnnouncement(1) │                        │
              ├───────────────────────►├───────────────────────►│
              │                        │                        │
              │  ClusterStateSnapshot  │                        │
              ├───────────────────────►├───────────────────────►│
              │                        │                        │
```

**Rule:** Lowest `nodeId` wins.

### 17.2 Service Leader Election

Managed exclusively by the **broker cluster leader**:

1. Lowest `serviceId` wins (first-registered policy).
2. Re-evaluated on: service added/removed, broker leader change, cluster state update.
3. Leader broadcasts `ServiceLeaderDesignated` to peers; peers forward `LeaderChanged` to local services.

### 17.3 Cluster State Synchronization

```
┌─────────────────────────────────────────────────┐
│              State Flow                         │
│                                                 │
│  Election settles                               │
│    └──► Leader sends ClusterStateSnapshot       │
│           └──► All peers merge state            │
│                  └──► Subscribers notified       │
│                                                 │
│  Service registers on Broker A                  │
│    └──► Broker A sends ServiceAdded to all      │
│           └──► Peers add to registry            │
│                  └──► Peers notify subscribers   │
│                                                 │
│  Service removed on Broker B                    │
│    └──► Broker B sends ServiceRemoved to all    │
│           └──► Peers remove from registry       │
│                  └──► Peers notify subscribers   │
│                                                 │
│  Leader designates service leader               │
│    └──► ServiceLeaderDesignated to all peers    │
│           └──► Peers send LeaderChanged locally  │
│                                                 │
└─────────────────────────────────────────────────┘
```

**Consistency model:** Eventual consistency. Updates are idempotent (keyed by `serviceId + nodeId`). After any disruption, election + state snapshot restores consistency.

---

## 18. Configuration

### 18.1 Broker Configuration

| Property | Default | Description |
|---|---|---|
| `broker.node.id` | (required) | Unique node ID (0–255) |
| `broker.local.host.port` | (required) | This broker's `host:port` for UDP |
| `broker.member.host.ports` | (empty) | Comma-separated peer `host:port` list |
| `broker.group.name` | `"brz"` | Broker group name (directory prefix) |
| `broker.storage.path` | `/dev/shm` | Base path for metadata files |
| `broker.control.buffer.size` | `65536` | Control ring buffer capacity (power of 2) |
| `broker.messages.buffer.size` | `1048576` | Send ring buffer capacity (power of 2) |
| `broker.recv.log.buffer.size` | `4194304` | Receive log buffer per peer (power of 2) |
| `broker.mtu.length` | `1408` | Maximum UDP payload size |
| `broker.threading.mode` | `DEDICATED` | `DEDICATED`, `SHARED_NETWORK`, `SHARED` |
| `broker.idle.strategy` | `backoff` | `busy_spin`, `yielding`, `sleeping`, `backoff` |
| `broker.media.driver.enabled` | `true` | (N/A — transport is built in) |

### 18.2 Buffer Sizing Guidelines

| Buffer | Sizing Rule | Default | Notes |
|---|---|---|---|
| Control ring buffer | Small; only control messages | 64 KB | wire-protocol-encoded, small messages |
| Send ring buffer | Services × avg cross-host msg rate × latency | 1 MB | MPSC, all services write here |
| Receive log buffer | Per peer; mtu × inflight_window | 4 MB | Retransmit window bounds this |
| Retransmit buffer | Per peer; must cover retransmit window | 4 MB | Circular, overwritten |
| Service messages ring buffer | Configurable per service | 1 MB | Application-specific |

All buffer sizes must be a power of 2.

---

## 19. Counters & Monitoring

### 19.1 System Counters

| ID | Name | Description |
|---|---|---|
| 0 | `bytes_sent` | Total bytes sent via UDP |
| 1 | `bytes_received` | Total bytes received via UDP |
| 2 | `messages_routed_local` | Messages routed to local services |
| 3 | `messages_routed_remote` | Messages forwarded to remote brokers |
| 4 | `naks_sent` | NAKs sent (loss detected) |
| 5 | `naks_received` | NAKs received (retransmit requested) |
| 6 | `retransmits_sent` | Retransmitted frames |
| 7 | `status_messages_sent` | Status Messages sent |
| 8 | `status_messages_received` | Status Messages received |
| 9 | `heartbeats_sent` | UDP heartbeats sent |
| 10 | `heartbeats_received` | UDP heartbeats received |
| 11 | `services_registered` | Currently registered services |
| 12 | `services_removed` | Total services removed |
| 13 | `send_rb_back_pressure` | Send ring buffer full events |
| 14 | `service_back_pressure` | Service ring buffer full events |
| 15 | `unknown_service_drops` | Messages for unknown service IDs |
| 16 | `invalid_packets` | Malformed UDP packets received |
| 17 | `control_loop_cycle_time_max` | Max control loop cycle time (ns) |
| 18 | `sender_cycle_time_max` | Max sender cycle time (ns) |
| 19 | `receiver_cycle_time_max` | Max receiver cycle time (ns) |
| 20 | `flow_control_under_runs` | Received data below consumption point |
| 21 | `flow_control_over_runs` | Received data beyond window |

### 19.2 Counter Layout

```
Counter Values Buffer (shared memory):
  ┌──────────────────────────┐
  │ Counter 0 value (128B)   │
  │ Counter 1 value (128B)   │
  │ ...                      │
  │ Counter N value (128B)   │
  └──────────────────────────┘

Counter Metadata Buffer:
  ┌──────────────────────────┐
  │ Counter 0 meta (256B)    │
  │ Counter 1 meta (256B)    │
  │ ...                      │
  │ Counter N meta (256B)    │
  └──────────────────────────┘
```

---

## 20. Error Handling

### 20.1 Error Log

A flat buffer for recording unique error observations (same design as Aeron):

```
Error Log Entry:
  Offset  Size  Type          Field
  ────────────────────────────────────
  0       4     volatile i32  length (0 = empty, >0 = entry size)
  4       4     volatile i32  observation_count
  8       8     volatile i64  last_observation_timestamp
  16      8     i64           first_observation_timestamp
  24      var   bytes         description
```

New errors append at `next_offset`. Repeated errors atomically increment `observation_count` and update `last_observation_timestamp`.

### 20.2 Thread-Local Error State

```
threadlocal var err_state: ErrorState = .{};

const ErrorState = struct {
    errcode: i32 = 0,
    errmsg: [8192]u8 = undefined,
    msg_len: usize = 0,
};
```

### 20.3 Hot-Path Error Handling

No exceptions or allocations on the hot path. Errors are communicated via:
- Return values (e.g., `error.BufferFull`, negative offsets)
- Counter increments (for monitoring)
- Error log entries (for diagnostics)

---

## 21. Platform Abstraction

### 21.1 Required Platform Abstractions

| Abstraction | Linux | macOS | Windows |
|---|---|---|---|
| Memory-mapped files | `mmap`/`munmap`/`msync` | `mmap`/`munmap`/`msync` | `CreateFileMapping`/`MapViewOfFile` |
| Atomic operations | Zig builtins (compiler intrinsics) | Same | Same |
| Threads | `pthread_create`/`pthread_join` | Same | `CreateThread` |
| UDP sockets | `socket`/`bind`/`sendmsg`/`recvmmsg` | `socket`/`bind`/`sendmsg`/`recvmsg` | `WSASendMsg`/`WSARecvMsg` |
| Non-blocking I/O | `fcntl(O_NONBLOCK)` | Same | `ioctlsocket(FIONBIO)` |
| I/O polling | `epoll` | `kqueue` | `WSAPoll` |
| Monotonic clock | `clock_gettime(MONOTONIC)` | `mach_absolute_time` | `QueryPerformanceCounter` |
| Wall clock | `clock_gettime(REALTIME_COARSE)` | `clock_gettime(REALTIME)` | `GetSystemTimeAsFileTime` |
| Thread naming | `pthread_setname_np` | `pthread_setname_np` | `SetThreadDescription` |
| CPU pause | `_mm_pause` (x86), `yield` (ARM) | Same | Same |
| Process synchronization | `futex(2)` | `__ulock_wait`/`wake` | `WaitOnAddress` |

### 21.2 Zig-Specific Advantages

1. **Comptime configuration:** Buffer sizes, alignment constants, and hash map types can be configured at compile time with zero runtime cost.
2. **No hidden allocations:** Zig does not allocate implicitly. All memory management is explicit and visible.
3. **Cross-compilation:** Single build system targets all platforms.
4. **C ABI compatibility:** Direct calls to platform APIs without FFI overhead.
5. **Packed structs:** `packed struct` maps directly to wire formats without padding concerns.
6. **Atomic builtins:** `@atomicLoad`, `@atomicStore`, `@atomicRmw`, `@cmpxchgWeak` map directly to hardware instructions.

---

## 22. Constants Reference

### 22.1 Buffer Sizes

| Constant | Value | Notes |
|---|---|---|
| `CACHE_LINE_LENGTH` | 64 | Hardware cache line |
| `CACHE_LINE_PAD` | 128 | 2 × cache line, prevents false sharing |
| `RING_BUFFER_TRAILER_LENGTH` | 768 | 6 × 128-byte padded slots |
| `RING_BUFFER_RECORD_HEADER_LENGTH` | 8 | `i32 length` + `i32 msg_type_id` |
| `RING_BUFFER_ALIGNMENT` | 8 | Record alignment |
| `RECV_LOG_METADATA_LENGTH` | 256 | Receive log buffer metadata |
| `DATA_FRAME_HEADER_LENGTH` | 40 | On-wire data frame header |
| `METADATA_HEADER_LENGTH` | 512 | Broker/service metadata header |
| `PAGE_SIZE` | 4096 | Memory page alignment |

### 22.2 Protocol Constants

| Constant | Value |
|---|---|
| `FRAME_HEADER_VERSION` | 0 |
| `PADDING_MSG_TYPE_ID` | -1 |
| `FRAME_TYPE_PAD` | 0x00 |
| `FRAME_TYPE_DATA` | 0x01 |
| `FRAME_TYPE_NAK` | 0x02 |
| `FRAME_TYPE_SM` | 0x03 |
| `FRAME_TYPE_SETUP` | 0x04 |
| `FLAG_BEGIN` | 0x80 |
| `FLAG_END` | 0x40 |
| `FLAG_UNFRAGMENTED` | 0xC0 |
| `FLAG_ADMIN` | 0x20 |
| `BROKER_SERVICE_ID` | 0 |
| `BROKER_SERVICE_NAME` | `"broker"` |

### 22.3 Timing Constants

| Constant | Value |
|---|---|
| `UDP_HEARTBEAT_INTERVAL_NS` | 100ms |
| `SM_TIMEOUT_NS` | 200ms |
| `NAK_INITIAL_DELAY_NS` | 60ms |
| `NAK_RETRY_DELAY_NS` | 60ms |
| `RETRANSMIT_LINGER_NS` | 10µs |
| `SERVICE_HEARTBEAT_WRITE_INTERVAL_MS` | 1000 |
| `SERVICE_HEARTBEAT_CHECK_INTERVAL_MS` | 3000 |
| `SERVICE_HEARTBEAT_TIMEOUT_MS` | 10000 |
| `CONTROL_LOOP_TIMEOUT_CHECK_INTERVAL_NS` | 1s |
| `COMMAND_DRAIN_LIMIT` | 1 |
| `CONTROL_READ_LIMIT` | 10 |
| `SEND_BATCH_LIMIT` | 10 |
| `RECV_BATCH_LIMIT` | 4 |

### 22.4 Memory Ordering Summary

| Operation | Ordering | Rationale |
|---|---|---|
| Write record length (negative) | Release | Sentinel visible before data |
| Write record length (positive) | Release | Commits record — all data visible |
| Read record length | Acquire | Ensures reading committed data |
| Write head_position | Release | Frees space for producers |
| Read head_position | Acquire | Producers see consumer progress |
| Write tail_position (MPSC) | CAS | Multi-producer coordination |
| Write frame_length in log | Release (last) | Entire frame visible atomically |
| Read frame_length in log | Acquire | Subsequent fields valid |
| Write heartbeat timestamp | Release | Readers see fresh value |
| Read heartbeat timestamp | Acquire | Stale reads acceptable but bounded |

### 22.5 Default Configuration Values

| Parameter | Default |
|---|---|
| `mtu_length` | 1408 bytes |
| `control_buffer_length` | 64 KB |
| `send_buffer_length` | 1 MB |
| `recv_log_buffer_length` | 4 MB |
| `retransmit_buffer_length` | 4 MB |
| `counter_values_buffer_length` | 64 KB |
| `error_log_buffer_length` | 256 KB |
| `max_services` | 256 |
| `max_peers` | 16 |

---

## Appendix A: Connection Lifecycle

### A.1 Peer Connection Establishment

```
Broker A (sender)                      Broker B (receiver)
     │                                      │
     │  SETUP {nodeId=1, logBufLen,          │
     │         mtu, initialSeq}             │
     ├─────────────────────────────────────►│
     │                                      │  Allocate receive log buffer
     │                                      │  for node 1
     │                                      │
     │         SM {nodeId=2,                 │
     │             consumption_pos=0,        │
     │             window=recv_log_len/2}    │
     │◄─────────────────────────────────────┤
     │                                      │
     │  Connection established              │
     │  send_limit = 0 + window             │
     │                                      │
     │  DATA frames...                       │
     ├─────────────────────────────────────►│
     │                                      │
```

### A.2 Peer Disconnection

Detected via:
1. No heartbeat received for `image_liveness_timeout` (10s)
2. Socket error on send/receive

On disconnection:
1. Notify control loop (trigger election)
2. Close receive log buffer for that peer
3. Remove peer from sender's endpoint list

---

## Appendix B: Comparison with Full Aeron

| Feature | Aeron | BRZ Broker | Impact |
|---|---|---|---|
| Streams per endpoint | Unlimited | 1 | No stream dispatch needed |
| Sessions per stream | Unlimited | N/A | No session tracking |
| Log buffer partitions | 3 (rotating) | 1 (circular) | Simpler, less memory |
| Publications | Concurrent + Exclusive | MPSC ring buffer | Simpler send path |
| Subscriptions | Multiple per stream | Direct routing by serviceId | No subscription bookkeeping |
| CnC file | Required | Not needed | No driver↔client protocol |
| Media Driver | Separate process | Integrated | Fewer context switches |
| Broadcast buffer | Driver→clients | Direct ring buffer writes | Lower latency |
| Flow control strategies | Max, Min, Tagged | Single-receiver window | Much simpler |
| Congestion control | Static, Cubic | Static (initially) | Can add Cubic later |
| Multicast | Full support | Not supported | Unicast only |
| Name resolution | Pluggable | Static config | Simple |
| URI parsing | Full grammar | Not needed | Config-driven |
| IPC publications | Log buffer per pub | Direct shared memory ring buffers | Already part of BRZ |
| Total code complexity | ~100K lines (C) | ~10-15K lines (Zig, estimated) | 10× reduction |

---

## Appendix C: End-to-End Latency Analysis

### Same-Host Path (IPC)

```
Service A write → ring buffer CAS → memory fence → Service B poll
                     ~50ns              ~5ns          ~50ns (idle poll)

Total: ~100ns (sub-microsecond)
```

### Cross-Host Path

```
Service A → send RB → sender drain → UDP send → network → UDP recv → route → service RB → Service B
  ~50ns      ~100ns     ~200ns        ~1µs      ~10-50µs    ~200ns   ~100ns    ~100ns       ~50ns

Total: ~12-52µs (dominated by network RTT)
```

### Latency Budget

| Step | Time | Notes |
|---|---|---|
| Service → send ring buffer write | 50-200ns | CAS + memcpy |
| Sender drain + syscall | 200-500ns | Ring buffer read + sendmsg |
| Network transit | 10-50µs | LAN dependent |
| Receiver syscall + insert | 200-500ns | recvmmsg + log buffer write |
| Route to service ring buffer | 50-200ns | Lookup + memcpy |
| Service poll | 0-50ns | Depends on idle strategy |
| **Total** | **~11-52µs** | **End-to-end cross-host** |