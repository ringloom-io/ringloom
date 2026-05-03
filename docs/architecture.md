# RingLoom Broker Architecture

**For Implementation in Zig**

This document describes the complete architecture of the RingLoom broker, a high-performance IPC and cross-host message routing system. The design uses shared-memory ring buffers for same-host IPC and TCP connections for cross-host communication, with `nodeId`/`serviceId` routing and minimal complexity for lowest latency. The TCP transport lives in a separate library (`ringloom_tcp`) that uses `io_uring` on Linux and `kqueue` on macOS, with planned future support for kernel-bypass technologies (TCPDirect, F-Stack).

---

## Table of Contents

1. [Design Philosophy](#1-design-philosophy)
2. [System Overview](#2-system-overview)
3. [Simplifications from Aeron](#3-simplifications-from-aeron)
4. [Memory Layout & Shared Memory IPC](#4-memory-layout--shared-memory-ipc)
5. [Concurrent Data Structures](#5-concurrent-data-structures)
6. [Service ↔ Broker IPC](#6-service--broker-ipc)
7. [TCP Wire Protocol](#7-tcp-wire-protocol)
8. [Send Path Architecture](#8-send-path-architecture)
9. [Receive Path Architecture](#9-receive-path-architecture)
10. [Back-Pressure](#10-back-pressure)
11. [TCP Transport Library](#11-tcp-transport-library)
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

1. **Zero-copy on the hot path.** All local data transfer uses memory-mapped shared memory. No `memcpy` between service and broker for same-host IPC. Cross-host routing performs exactly one copy: from the TCP receive buffer to the target service's ring buffer.
2. **Lock-free everywhere.** All shared data structures use atomic fetch-and-add or CAS. No mutexes on any data path.
3. **Single-writer principle.** Every mutable shared location has exactly one writer. Readers never block writers.
4. **Allocation-free hot path.** All buffers are pre-allocated. Message routing uses flyweight patterns over mapped memory.
5. **Simplicity over generality.** Unlike Aeron, which is a general-purpose transport, this system has exactly one use case: RingLoom broker message routing. Every abstraction that doesn't serve this use case is eliminated.
6. **Position-based progress tracking.** All progress is tracked via monotonically increasing 64-bit positions, not sequence numbers.
7. **Duty-cycle event loops.** All processing is done in tight loops returning work counts, driving idle strategies.
8. **TCP for cross-host transport.** Reliability, ordering, and flow control are delegated to TCP. The broker does not implement a custom reliable transport protocol. A separate TCP library (`ringloom_tcp`) provides the I/O engine with platform-optimized backends.

### Key Differences from Aeron

| Aeron Concept | RingLoom Broker Equivalent | Rationale |
|---|---|---|
| Custom reliable UDP protocol | TCP connections between brokers | TCP provides reliability, ordering, and flow control natively; eliminates NAK/retransmit/loss detection/flow control protocol |
| `streamId` / `sessionId` multiplexing | `nodeId` / `serviceId` in message header | RingLoom has one logical link per peer broker; routing is by service identity, not stream identity |
| 3-partition rotating log buffers | No receive log buffer (TCP guarantees ordering) | TCP delivers bytes in order; no gap tracking or reassembly needed |
| Per-publication log buffer files | Single MPSC send ring buffer per broker | All local services write to one outbound buffer; broker drains and routes |
| CnC file + command/response protocol | Direct metadata file with embedded ring buffers | Services map the broker's file directly; no driver process needed |
| Media Driver as separate process | Transport integrated into broker process | The broker IS the driver; no client↔driver split needed |
| Broadcast buffer for responses | Direct writes to service control ring buffers | Broker writes responses directly to each service's mapped memory |
| Generic URI-based channel configuration | Fixed per-peer TCP endpoints from config | No URI parsing needed; topology is known at startup |
| `io_uring` / `epoll` for UDP | `io_uring` (Linux) / `kqueue` (macOS) for TCP | Async I/O engine in separate `ringloom_tcp` library with pluggable backends |

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
│  │       │      │ Send    │   │ TCP     │    │   │     │   │       │      │ Send    │   │ TCP     │    │  │
│  │       │      │ Ring    │   │ Recv    │    │   │     │   │       │      │ Ring    │   │ Recv    │    │  │
│  │       │      │ Buffer  │   │ Conns   │    │   │     │   │       │      │ Buffer  │   │ Conns   │    │  │
│  │       │      │ (MPSC)  │   │ (per    │    │   │     │   │       │      │ (MPSC)  │   │ (per    │    │  │
│  │       │      │         │   │  peer)  │    │   │     │   │       │      │         │   │  peer)  │    │  │
│  │       │      └────┬────┘   └────┬────┘    │   │     │   │       │      └────┬────┘   └────┬────┘    │  │
│  │       │           │             │         │   │     │   │       │           │             │         │  │
│  └───────┼───────────┼─────────────┼─────────┘   │     │   └───────┼───────────┼─────────────┼─────────┘  │
│          │           │             │              │     │           │           │             │             │
└──────────┼───────────┼─────────────┼──────────────┘     └───────────┼───────────┼─────────────┼─────────────┘
                       │             │                                │            │
                       │             │     TCP (dual unidirectional)  │            │
                       └─────────────┼───────────────────────────────┘            │
                                     └───────────────────────────────────────────┘
```

### Component Summary

| Component | Role |
|---|---|
| **Metadata File** | Per-service and per-broker shared memory file containing ring buffers and metadata |
| **Control Ring Buffer** | MPSC ring buffer for service↔broker control messages (registration, discovery, leader election) |
| **Message Ring Buffer** | MPSC ring buffer for service→broker cross-host message submission |
| **Send Ring Buffer** | Single MPSC ring buffer per broker; services write outbound cross-host messages here |
| **TCP Send Connections** | One outgoing TCP connection per peer; sender event loop writes to these |
| **TCP Recv Connections** | One incoming TCP connection per peer; receiver event loop reads from these |
| **Control Loop** | Control plane: service registration, heartbeat checking, cluster management |
| **Sender Event Loop** | Data plane: drains send ring buffer, writes to outgoing TCP connections |
| **Receiver Event Loop** | Data plane: reads from incoming TCP connections, routes to local service ring buffers |

---

## 3. Simplifications from Aeron

### 3.1 No Custom Reliable Transport Protocol

Aeron implements a complete reliable transport over UDP: sequence numbers, NAKs, retransmit buffers, loss detection, Status Messages for flow control, receiver window tracking, and connection setup/teardown frames. RingLoom replaces all of this with **TCP**:

```
Aeron:   UDP + custom reliability (NAK, retransmit, flow control, setup)
RingLoom:     TCP connections (reliability, ordering, flow control handled by kernel)
```

This eliminates:
- NAK frames and retransmit buffers
- Loss detection and gap scanning
- Status Message protocol for receiver window
- Receive log buffers for packet reassembly
- Setup/teardown frame types
- Message fragmentation/reassembly at the transport level
- Flow control state machines

The broker-to-broker wire protocol is reduced to **length-prefixed message framing** over TCP.

### 3.2 No Stream/Session Multiplexing

Aeron supports arbitrary numbers of streams and sessions per endpoint, requiring `streamId`/`sessionId` lookup on every packet. RingLoom has exactly **one logical link per peer broker**, using a pair of TCP connections (one outgoing, one incoming). The `nodeId` field in the RingLoom message header replaces stream/session routing:

```
Aeron:   endpoint:port → streamId → sessionId → log buffer → poll
RingLoom:     TCP connection → read framed message → route by (targetNodeId, targetServiceId)
```

This eliminates:
- Stream interest maps
- Session-to-image maps
- Data packet dispatcher
- Session ID generation and management

### 3.3 No CnC File or Command Protocol

Aeron uses a Command-and-Control shared memory file with an MPSC ring buffer (client→driver) and a broadcast buffer (driver→clients) because the media driver is a separate process that manages resources for multiple clients.

In RingLoom, the broker IS the transport. Services communicate with the broker through **direct shared memory ring buffers** embedded in metadata files:

```
Aeron:   Client → CnC ring buffer → Driver Conductor → broadcast → Client
RingLoom:     Service → Broker control ring buffer (direct write)
         Broker → Service control ring buffer (direct write)
```

This eliminates:
- CnC file layout and handshake
- Broadcast buffer (one-to-many)
- Client conductor and keepalive protocol
- Registration state machine
- Correlation ID tracking for async responses

### 3.4 No Log Buffer Rotation

Aeron uses 3 term partitions per log buffer, rotating when one fills up. RingLoom does not use log buffers at all for the receive path — TCP delivers data in order, and the broker routes each complete message immediately upon receipt. For the send path, an MPSC ring buffer is used, since multiple services write outbound messages and a single sender thread drains them.

### 3.5 No Publication/Subscription Abstraction

Aeron has a rich Publication/Subscription API with log buffer files per publication, subscriber position counters, and image lifecycle management. RingLoom replaces all of this with:

- **Outbound:** Services write to the broker's MPSC send ring buffer (or directly to another service's message ring buffer for same-host IPC)
- **Inbound:** The broker writes received messages directly to the target service's message ring buffer

### 3.6 Simplified Back-Pressure

Aeron's flow control tracks per-subscriber positions and uses Status Messages to communicate receiver window back to senders. RingLoom delegates transport-level flow control entirely to TCP and handles application-level back-pressure through message dropping:

- **Receiver always reads:** The receiver event loop always drains incoming TCP connections. It never pauses reads.
- **Drop on full:** If a target service's ring buffer is full, the message is dropped and a counter is incremented. This matches the current behavior and prevents head-of-line blocking across services.
- **Natural TCP propagation:** If the sender's TCP write buffer is full (peer unreachable or slow to read), the sender skips that peer and continues serving other peers. The send ring buffer fills only when all outbound paths are blocked.

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

The **send ring buffer** serves as the single outbound buffer for all cross-host messages. Any local service that needs to send a message to a remote node writes to this buffer with the target `nodeId` and `serviceId` set in the RingLoom message header.

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

### 4.3 TCP Receive Buffers

Unlike the previous UDP-based design which required per-peer receive log buffers for packet reassembly and gap tracking, the TCP-based design requires no application-level receive buffers. TCP guarantees in-order, reliable delivery of bytes.

The receiver event loop reads complete framed messages from each peer's incoming TCP connection using pre-allocated read buffers managed by the `ringloom_tcp` library. Once a complete message frame is read (determined by the length prefix), it is immediately routed to the target service's ring buffer.

Per-peer state is minimal:

```
PeerRecvState:
  connection: *TcpConnection       ← managed by ringloom_tcp
  read_buffer: [max_frame_length]u8  ← pre-allocated, reused per message
  read_position: usize              ← bytes read into current frame so far
  expected_length: u32              ← frame_length from header (0 if reading header)
  session_epoch: u64                ← from connection handshake
```

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
│        │  IPC   │ Send Ring    │       TCP          │              │  IPC   │        │
│ Writer ┼───────►│ Buffer       │──────────────────►│  Receiver    │───────►│ Msgs   │
│        │        │ (MPSC)       │   Framed Messages  │  Event Loop  │        │ Ring   │
│        │        │              │                   │              │        │ Buffer │
└────────┘        └──────────────┘                   └──────────────┘        └────────┘
```

1. Service A writes to Broker 1's **send ring buffer** with `targetNodeId=2, targetServiceId=B`.
2. Broker 1's **Sender Event Loop** drains the send ring buffer.
3. Sender looks up the TCP connection for `nodeId=2`.
4. Sender writes the length-prefixed message frame to the TCP connection.
5. Broker 2's **Receiver Event Loop** reads the complete frame from TCP.
6. Receiver parses `targetServiceId` from the message header.
7. Broker 2 writes the message payload to Service B's messages ring buffer.

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

## 7. TCP Wire Protocol

All broker-to-broker communication uses **TCP** with length-prefixed message framing. The protocol is minimal: a fixed-size message header carries routing fields, and TCP provides reliability, ordering, and flow control.

### 7.1 Connection Model

Each broker pair uses **two unidirectional TCP connections**:

```
Broker A                                    Broker B
┌──────────────┐                           ┌──────────────┐
│              │  outgoing TCP (A→B)       │              │
│ Sender  ─────┼──────────────────────────►│ Receiver     │
│ Evt Loop     │                           │ Evt Loop     │
│              │  incoming TCP (B→A)       │              │
│ Receiver ◄───┼───────────────────────────┤ Sender       │
│ Evt Loop     │                           │ Evt Loop     │
└──────────────┘                           └──────────────┘
```

- Each broker **listens** on a TCP port.
- Each broker **connects** to each configured peer (outgoing connection for sending).
- Each broker **accepts** connections from peers (incoming connection for receiving).
- The sender event loop owns outgoing connections (write-only).
- The receiver event loop owns incoming connections (read-only).

### 7.2 Connection Handshake

After a TCP connection is established, the connecting broker sends a **handshake frame** to identify itself:

```
Connection Handshake (24 bytes):

Offset  Size  Type      Field
──────────────────────────────────────
0       4     u32       magic             (0x474E4952 = "RING")
4       1     u8        protocol_version  (1)
5       1     u8        source_node_id    (connecting broker's node ID)
6       1     u8        target_node_id    (expected peer's node ID)
7       1     u8        direction         (0 = SEND, 1 = RECV — from connector's perspective)
8       8     u64       session_epoch     (monotonic counter, incremented on restart)
16      4     u32       group_name_hash   (FNV-1a hash of broker.group.name)
20      4     u32       reserved
```

The accepting broker validates the handshake:

1. Verify `magic` and `protocol_version`.
2. Verify `target_node_id` matches this broker's node ID.
3. Verify `group_name_hash` matches this broker's group.
4. If a connection already exists for this `(source_node_id, direction)`:
   - If `session_epoch` > existing connection's epoch: replace (close old, accept new).
   - If `session_epoch` <= existing: reject (close new connection).
5. Associate the connection with the peer's state.

**Session epoch** enables clean reconnection after restart. Each broker persists or derives a monotonically increasing epoch (e.g., startup timestamp in microseconds).

### 7.3 Message Frame Header (24 bytes)

All messages (data and admin) use a single frame format with a **length prefix**:

```
Offset  Size  Type    Field
──────────────────────────────────
0       4     u32     frame_length        total frame size including this header
4       1     u8      flags               message flags
5       1     u8      source_node_id
6       1     u8      target_node_id
7       1     u8      reserved
8       2     u16     source_service_id
10      2     u16     target_service_id
12      2     u16     template_id         message type (0 = raw application message)
14      2     u16     reserved
16      8     i64     correlation_id      for request-response matching
24      ...   bytes   payload
```

**Total header: 24 bytes**, aligned to 8 bytes.

**Flags field (byte 4):**

| Flag | Bit | Meaning |
|---|---|---|
| `ADMIN` | `0x01` | Admin/cluster message (not routed to services) |

Unused flag bits are reserved for future use.

**Constraints:**

- `frame_length` must be >= 24 (header only, no payload) and <= `max_frame_length` (configurable, default 1 MB).
- `frame_length` is encoded as **little-endian** on the wire.
- All multi-byte fields are **little-endian**.

**Key design choices:**

- **No fragmentation flags.** TCP handles segmentation; the broker reads complete framed messages.
- **No sequence numbers.** TCP guarantees in-order delivery within a connection.
- **No frame_type field.** All frames use the same header. The `ADMIN` flag and `template_id` distinguish message types.
- **i64 correlation_id.** Consistent with the shared-memory message model to avoid translation on the cross-host path.

### 7.4 Frame Validation

On receiving a frame, the receiver validates:

1. `frame_length >= 24` (minimum header size).
2. `frame_length <= max_frame_length` (prevent memory exhaustion).
3. `source_node_id` matches the peer associated with this connection.
4. If validation fails, the connection is closed (TCP stream is corrupted).

### 7.5 Message Size Limits

| Limit | Value | Rationale |
|---|---|---|
| Minimum frame | 24 bytes | Header only, no payload |
| Maximum frame (default) | 1 MB | Must fit in a single ring buffer write; configurable |
| Maximum payload | `max_frame_length - 24` | Frame length minus header |

The maximum frame length must be less than or equal to `send_ring_buffer_capacity / 8` (the ring buffer's max message size). Messages exceeding this limit cannot be sent cross-host.

### 7.6 Delivery Semantics

- **Within a connection:** Messages are delivered in order, exactly once (TCP guarantees).
- **Across connection loss:** Delivery is **best-effort**. Messages in flight during a TCP disconnect may be lost. The broker does not implement application-level acknowledgment or replay. After reconnection, normal message flow resumes from that point forward.
- **Service ring buffer full:** Messages are dropped at the receiver. A counter (`service_back_pressure`) is incremented. The sending service is not notified of the drop.

### 7.7 Heartbeat Messages

Broker-to-broker heartbeat messages are sent as regular framed messages with `ADMIN` flag set and a specific `template_id`. They flow through the same TCP connection as data, ensuring that the connection is exercised regularly.

```
Heartbeat message:
  frame_length   = 24 (header only, no payload)
  flags          = ADMIN
  template_id    = HEARTBEAT_TEMPLATE_ID
  source_node_id = this broker's node ID
  target_node_id = peer's node ID
```

The receiver uses heartbeat arrival to confirm peer liveness. If no message (heartbeat or data) is received from a peer within `peer_liveness_timeout`, the peer is considered dead.

Since the receiver always reads from TCP (never pauses), heartbeat messages are never blocked by application data back-pressure.

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
         │  3. Lookup peer conn   │
         │  4. TCP write (via     │
         │     io_uring/kqueue)   │
         └────────────┬───────────┘
                      │
                      ▼
              TCP Connections
              (one per peer)
```

### 8.2 Send Ring Buffer vs. Aeron Log Buffer

**Aeron approach:** Each publication has its own log buffer file (3 term partitions). The sender event loop scans the active term for new data using the `term_tail_counter`. Multiple concurrent publications each have their own buffer.

**RingLoom approach:** A single MPSC ring buffer serves as the outbound queue for all local services. This is simpler and sufficient because:

1. **Single sender thread** drains the buffer. No concurrent readers.
2. **Messages are small** relative to buffer size (typically < 16KB, often < 1KB).
3. **No fragmentation needed** — TCP handles segmentation. The only constraint is that messages fit in the send ring buffer.
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

    // 1. Process inter-event-loop commands (peer add/remove)
    work_count += sender.cmd_queue.drain(dispatch_command, 1);

    // 2. Drain send ring buffer (batch of messages)
    work_count += send_ring_buffer.read(on_outbound_message, SEND_BATCH_LIMIT);

    // 3. Flush pending TCP writes per peer (fair round-robin)
    //    Each peer gets a write budget per cycle to prevent one
    //    blocked peer from starving others.
    for (sender.peers) |peer| {
        work_count += peer.flush_pending_writes(WRITE_BUDGET_PER_PEER);
    }

    // 4. Submit and reap io_uring/kqueue completions
    work_count += sender.io_engine.poll_completions();

    // 5. Send heartbeats to peers (every 100ms)
    if (now_ns > next_heartbeat_ns) {
        send_heartbeats_to_all_peers();
        next_heartbeat_ns = now_ns + HEARTBEAT_INTERVAL_NS;
    }

    // 6. Check connection health
    for (sender.peers) |peer| {
        if (peer.connection_state == .CONNECTED and
            now_ns - peer.last_write_completion_ns > WRITE_TIMEOUT_NS)
        {
            peer.handle_write_timeout();
        }
    }

    return work_count;
}
```

### 8.4 Per-Peer Write Path

When a message is read from the send ring buffer:

```
fn on_outbound_message(msg_type_id: i32, payload: []const u8) void {
    const header = parse_message_header(payload);
    const peer = peer_registry.lookup(header.target_node_id) orelse {
        increment_counter(.unknown_peer_drops);
        return;
    };

    if (peer.connection_state != .CONNECTED) {
        increment_counter(.peer_disconnected_drops);
        return;
    };

    // Enqueue framed message for TCP write
    // The frame header is already part of the payload from the send ring buffer
    peer.write_queue.enqueue(payload) catch {
        increment_counter(.peer_write_queue_full);
        return;
    };
}
```

### 8.5 TCP Write Mechanics

Writes use the platform I/O engine (`ringloom_tcp`):

- **Linux (io_uring):** Writes are submitted as `IORING_OP_SEND` or `IORING_OP_WRITEV` operations. Multiple messages to the same peer can be coalesced into a single vectored write.
- **macOS (kqueue):** Writes use `writev()` when the socket is ready (signaled by `EVFILT_WRITE`).

If a peer's TCP connection becomes write-blocked (kernel send buffer full), the sender:
1. Stops writing to that peer's connection.
2. Continues serving other peers normally.
3. Retries on the next duty cycle when the socket becomes writable.
4. If the write queue for that peer exceeds a high-water mark, drops oldest messages and increments a counter.

**No retransmit buffer.** TCP handles retransmission at the kernel level.

**No fragmentation.** TCP handles segmentation. Messages that exceed `max_frame_length` are rejected at the sending service API level.

---

## 9. Receive Path Architecture

### 9.1 Overview

```
              TCP Connections
              (one incoming per peer)
                      │
                      ▼
         ┌────────────────────────┐
         │  Receiver Event Loop   │
         │  (single thread)       │
         │                        │
         │  1. Read from TCP      │
         │     (io_uring/kqueue)  │
         │  2. Frame: read length │
         │     prefix, then body  │
         │  3. Validate header    │
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

### 9.2 TCP Framing (Length-Prefix Protocol)

Each TCP connection carries a continuous byte stream of length-prefixed message frames. The receiver reads in two phases:

1. **Read header (24 bytes):** Determine `frame_length` from the first 4 bytes.
2. **Read body:** Read remaining `frame_length - 24` bytes of payload.

```
Per-connection read state:

struct PeerReadState {
    // Frame parsing
    header_buf: [24]u8,        // accumulates partial header reads
    header_bytes_read: usize,  // 0..24
    payload_buf: []u8,         // points into read buffer
    payload_bytes_read: usize, // 0..(frame_length - 24)
    expected_payload_len: u32, // from parsed header

    // Connection
    connection: *TcpConnection,
    session_epoch: u64,
    peer_node_id: u8,
}
```

The receiver **always reads** from every peer connection on every duty cycle. It never pauses reads due to back-pressure. This is critical to avoid head-of-line blocking — one slow service must not prevent heartbeats and other service traffic from the same peer from being processed.

### 9.3 Receiver Event Loop Duty Cycle

```
fn receiver_do_work(receiver: *ReceiverEventLoop) u32 {
    var work_count: u32 = 0;
    const now_ns = monotonic_clock();

    // 1. Process inter-event-loop commands (peer add/remove)
    work_count += receiver.cmd_queue.drain(dispatch_command, 1);

    // 2. Poll I/O engine for read completions
    work_count += receiver.io_engine.poll_completions();

    // 3. Process completed reads — frame and route messages
    //    Each peer gets a read budget per cycle for fairness.
    for (receiver.peers) |peer| {
        work_count += peer.process_completed_reads(READ_BUDGET_PER_PEER);
    }

    // 4. Accept new incoming connections (from the listener)
    work_count += receiver.accept_pending_connections();

    // 5. Check peer liveness
    if (now_ns > receiver.next_liveness_check_ns) {
        for (receiver.peers) |peer| {
            if (peer.connection_state == .CONNECTED and
                now_ns - peer.last_message_ns > PEER_LIVENESS_TIMEOUT_NS)
            {
                peer.handle_liveness_timeout();
            }
        }
        receiver.next_liveness_check_ns = now_ns + LIVENESS_CHECK_INTERVAL_NS;
    }

    return work_count;
}
```

### 9.4 Message Routing (Receive → Service)

When a complete framed message is read:

```
fn route_received_message(peer: *PeerReadState, header: FrameHeader, payload: []const u8) void {
    // Validate
    if (header.frame_length < 24 or header.frame_length > max_frame_length) {
        peer.handle_protocol_error("invalid frame length");
        return;
    }
    if (header.source_node_id != peer.peer_node_id) {
        peer.handle_protocol_error("source_node_id mismatch");
        return;
    }

    // Route
    if (header.flags & ADMIN != 0) {
        handle_admin_message(header, payload);
    } else {
        route_to_service(header.target_service_id, header, payload);
    }
}

fn route_to_service(target_service_id: u16, header: FrameHeader, payload: []const u8) void {
    const service = service_registry.lookup(target_service_id) orelse {
        increment_counter(.unknown_service_drops);
        return;
    };

    // Write to service's messages ring buffer
    const result = service.messages_ring_buffer.write(MSG_TYPE_APPLICATION, payload);
    if (result == error.BufferFull) {
        increment_counter(.service_back_pressure);
        // Message is dropped — receiver continues reading from TCP
    }
}
```

**No receive log buffer.** TCP delivers bytes in order. Messages are routed immediately.

**No loss detection.** TCP handles retransmission.

**No fragment reassembly.** TCP handles segmentation.

---

## 10. Back-Pressure

### 10.1 Model

RingLoom delegates transport-level flow control to TCP and handles application-level back-pressure through **message dropping**:

```
Service A → Send Ring Buffer → TCP (Broker 1 → Broker 2) → Route → Service B Ring Buffer

Back-pressure points:

1. Service B's ring buffer full → message dropped, counter incremented
   (receiver keeps reading TCP — no HOL blocking)

2. TCP send buffer full (peer slow to read) → sender skips peer this cycle
   (other peers unaffected)

3. Send ring buffer full → service write returns BufferFull
   (service decides: retry, drop, or block)
```

### 10.2 Why the Receiver Never Pauses

In the previous UDP-based design, back-pressure propagated through Status Messages: if the receiver's log buffer filled, it stopped advertising window to the sender, causing the sender to stop draining the send ring buffer.

With TCP, a single connection carries traffic for **all services** between two peers plus **admin/heartbeat** messages. Pausing reads on the TCP connection when one service is slow would block:
- Heartbeat messages (peer declared dead)
- Admin/cluster protocol (election failures)
- Other services' traffic

Therefore, the receiver **always reads** and drops messages for full service ring buffers. This matches the original design's behavior (messages lost when service is slow) but avoids cross-service interference.

### 10.3 Send-Side Back-Pressure

| Condition | Behavior |
|---|---|
| Peer TCP write buffer full | Skip peer, try next cycle. Peer's write queue buffers messages up to a limit. |
| Peer write queue full | Drop oldest messages in queue. Increment `peer_write_queue_overflow` counter. |
| Send ring buffer full | Return `BufferFull` to the writing service. Service handles via retry or drop. |
| Peer disconnected | Messages for that peer are dropped until reconnection. |

### 10.4 Monitoring Back-Pressure

Back-pressure events are visible through counters:

| Counter | Meaning |
|---|---|
| `service_back_pressure` | Messages dropped because target service ring buffer was full |
| `peer_write_queue_overflow` | Messages dropped because peer's outbound write queue was full |
| `peer_disconnected_drops` | Messages dropped because peer TCP connection was down |
| `send_ring_buffer_full` | Write attempts that failed because the send ring buffer was full |

---

## 11. TCP Transport Library (`ringloom_tcp`)

The TCP transport is implemented as a **separate library** (`ringloom_tcp`) that the broker links against. This library provides a platform-abstracted, high-performance TCP I/O layer.

### 11.1 Architecture

```
┌─────────────────────────────────────────────────────────┐
│  ringloom_tcp Library                                         │
│                                                         │
│  ┌───────────────────────────────────────────────────┐  │
│  │  Layer 3: Message Framing                         │  │
│  │  • Length-prefixed frame reading/writing           │  │
│  │  • Partial read/write state machines               │  │
│  │  • Zero-copy where possible (direct buffer refs)   │  │
│  │  • Max frame length enforcement                    │  │
│  └───────────────────────────┬───────────────────────┘  │
│                              │                           │
│  ┌───────────────────────────┴───────────────────────┐  │
│  │  Layer 2: Connection Manager                      │  │
│  │  • Listener (accept incoming connections)          │  │
│  │  • Connector (outgoing connections with backoff)    │  │
│  │  • Connection state machine                        │  │
│  │  • Handshake protocol (send/validate)              │  │
│  │  • Connection health monitoring                    │  │
│  └───────────────────────────┬───────────────────────┘  │
│                              │                           │
│  ┌───────────────────────────┴───────────────────────┐  │
│  │  Layer 1: I/O Engine (platform-specific)          │  │
│  │                                                    │  │
│  │  ┌──────────────┐  ┌──────────────┐  ┌─────────┐ │  │
│  │  │  io_uring    │  │  kqueue      │  │ (future)│ │  │
│  │  │  (Linux)     │  │  (macOS)     │  │ TCPDirect│ │  │
│  │  │              │  │              │  │ F-Stack  │ │  │
│  │  └──────────────┘  └──────────────┘  └─────────┘ │  │
│  └───────────────────────────────────────────────────┘  │
└─────────────────────────────────────────────────────────┘
```

### 11.2 I/O Engine Interface

All I/O backends implement a common interface:

```
const IoEngine = struct {
    // Lifecycle
    fn init(allocator: Allocator, config: IoConfig) !IoEngine;
    fn deinit(self: *IoEngine) void;

    // Connection management
    fn listen(self: *IoEngine, addr: Address) !ListenHandle;
    fn connect(self: *IoEngine, addr: Address) !ConnectionHandle;
    fn close(self: *IoEngine, handle: ConnectionHandle) void;

    // I/O operations (non-blocking, completion-based)
    fn submit_read(self: *IoEngine, handle: ConnectionHandle, buffer: []u8) !void;
    fn submit_write(self: *IoEngine, handle: ConnectionHandle, data: []const u8) !void;
    fn submit_writev(self: *IoEngine, handle: ConnectionHandle, iovecs: []const iovec) !void;

    // Event polling
    fn poll_completions(self: *IoEngine, completions: []Completion) u32;

    // Configuration
    fn set_tcp_nodelay(self: *IoEngine, handle: ConnectionHandle, enabled: bool) !void;
    fn set_send_buffer_size(self: *IoEngine, handle: ConnectionHandle, size: u32) !void;
    fn set_recv_buffer_size(self: *IoEngine, handle: ConnectionHandle, size: u32) !void;
};

const Completion = struct {
    handle: ConnectionHandle,
    op: enum { READ, WRITE, ACCEPT, CONNECT },
    result: union { bytes_transferred: usize, err: anyerror },
    user_data: usize,
};
```

### 11.3 io_uring Backend (Linux)

Uses `io_uring` for all TCP I/O:

- **Ring setup:** One `io_uring` instance per event loop thread. Ring size configurable (default 256 entries).
- **Read submissions:** `IORING_OP_RECV` with pre-registered buffers.
- **Write submissions:** `IORING_OP_SEND` for single buffers, `IORING_OP_WRITEV` for vectored writes.
- **Accept:** `IORING_OP_ACCEPT` with `IORING_ACCEPT_MULTISHOT` for the listener.
- **Connect:** `IORING_OP_CONNECT` for outgoing connections.
- **Buffer registration:** `io_uring_register_buffers()` for zero-copy I/O where supported.
- **Completion batching:** `io_uring_peek_batch_cqe()` to harvest multiple completions per poll.

### 11.4 kqueue Backend (macOS)

Uses `kqueue` with non-blocking sockets:

- **Read events:** `EVFILT_READ` triggers `recv()` / `readv()`.
- **Write events:** `EVFILT_WRITE` triggers `send()` / `writev()`. Only registered when there is pending data.
- **Accept:** `EVFILT_READ` on the listener socket.
- **Connect:** `EVFILT_WRITE` on connecting socket (fires on connection complete).
- **Edge-triggered:** Uses `EV_CLEAR` for edge-triggered behavior to avoid redundant wakeups.

### 11.5 Future Backends

The I/O engine interface is designed to support kernel-bypass networking:

- **TCPDirect (Xilinx/Solarflare):** User-space TCP stack over Onload-capable NICs. Compiled in optionally via build flag.
- **F-Stack:** FreeBSD TCP stack running in user space on DPDK. Compiled in optionally via build flag.

These backends would implement the same `IoEngine` interface, selected at compile time via `build.zig` options:

```
// build.zig
const tcp_backend = b.option(TcpBackend, "tcp-backend", "TCP I/O backend") orelse .default;
// .default = io_uring on Linux, kqueue on macOS
// .tcpdirect = TCPDirect kernel bypass
// .fstack = F-Stack kernel bypass
```

### 11.6 Connection State Machine

```
                    ┌──────────┐
                    │ CLOSED   │
                    └────┬─────┘
                         │ connect() / accept()
                         ▼
                    ┌──────────┐
                    │CONNECTING│ (outgoing only)
                    └────┬─────┘
                         │ TCP established
                         ▼
                    ┌──────────┐
                    │HANDSHAKE │ send/receive handshake frame
                    └────┬─────┘
                         │ handshake validated
                         ▼
                    ┌──────────┐
           ┌───────│CONNECTED  │◄──── normal operation
           │       └────┬─────┘
           │            │ error / timeout / peer close
           │            ▼
           │       ┌──────────┐
           └──────►│DRAINING  │ flush pending writes, then close
                   └────┬─────┘
                        │
                        ▼
                   ┌──────────┐
                   │ CLOSED   │──── reconnect with backoff (outgoing)
                   └──────────┘
```

### 11.7 Reconnection

Outgoing connections use exponential backoff on failure:

| Attempt | Delay |
|---|---|
| 1 | 100ms |
| 2 | 200ms |
| 3 | 400ms |
| 4 | 800ms |
| 5+ | 1000ms (capped) |

On reconnection, the connecting broker sends a new handshake with an incremented `session_epoch`. The accepting broker replaces the old connection state. Messages in flight on the old connection are lost (best-effort delivery semantics).

---

## 12. Threading Model

### 12.1 Thread Architecture

```
┌─────────────────────────────────────────────────────────┐
│                    Broker Process                         │
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
│  │  • TCP writes to peer brokers (io_uring/kqueue)   │  │
│  │  • Per-peer write queue management                │  │
│  │  • Send heartbeats (ADMIN frame every 100ms)      │  │
│  │  • Connection health monitoring                   │  │
│  │  • Outgoing connection establishment              │  │
│  └───────────────────────────────────────────────────┘  │
│                                                         │
│  Thread 3: Receiver Event Loop                          │
│  ┌───────────────────────────────────────────────────┐  │
│  │  • TCP reads from all peer brokers (io_uring/kq)  │  │
│  │  • Length-prefix framing and message parsing       │  │
│  │  • Route messages to target service ring buffers  │  │
│  │  • Accept incoming connections + handshake         │  │
│  │  • Peer liveness detection                        │  │
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
| Control Loop → Sender | `sender_cmd_queue` | Add peer endpoint, remove peer endpoint, initiate connection |
| Control Loop → Receiver | `receiver_cmd_queue` | Add peer, remove peer, update service registry |
| Receiver → Control Loop | `control_loop_cmd_queue` | Peer connected (handshake received), peer disconnected, admin messages |
| Sender → Control Loop | `control_loop_cmd_queue` | Connection established, connection failed, peer unreachable |

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
fn route_message(header: FrameHeader, payload: []const u8) void {
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
fn handle_admin_message(header: FrameHeader, payload: []const u8) void {
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

Sent via TCP as ADMIN-flagged message frames (with `HEARTBEAT` template ID) every ~100ms by the sender event loop. Used for peer liveness tracking. Since the receiver always reads from TCP, heartbeat delivery is not blocked by application back-pressure.

### 16.4 Timeout Constants

| Parameter | Value |
|---|---|
| Service heartbeat write interval | ~1 second |
| Broker health check interval | 3 seconds |
| Service heartbeat timeout | 10 seconds |
| Broker-to-broker heartbeat interval | 100ms |
| Peer liveness timeout | 10 seconds |

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
| `broker.local.host.port` | (required) | This broker's `host:port` for TCP listener |
| `broker.member.host.ports` | (empty) | Comma-separated peer `host:port` list |
| `broker.group.name` | `"ringloom"` | Broker group name (directory prefix) |
| `broker.storage.path` | `/dev/shm` | Base path for metadata files |
| `broker.control.buffer.size` | `65536` | Control ring buffer capacity (power of 2) |
| `broker.messages.buffer.size` | `1048576` | Send ring buffer capacity (power of 2) |
| `broker.max.frame.length` | `1048576` | Maximum TCP message frame size |
| `broker.tcp.send.buffer.size` | `262144` | TCP kernel send buffer size per connection |
| `broker.tcp.recv.buffer.size` | `262144` | TCP kernel receive buffer size per connection |
| `broker.tcp.nodelay` | `true` | Enable TCP_NODELAY (disable Nagle) |
| `broker.peer.write.queue.size` | `1024` | Max pending messages per peer write queue |
| `broker.threading.mode` | `DEDICATED` | `DEDICATED`, `SHARED_NETWORK`, `SHARED` |
| `broker.idle.strategy` | `backoff` | `busy_spin`, `yielding`, `sleeping`, `backoff` |

### 18.2 Buffer Sizing Guidelines

| Buffer | Sizing Rule | Default | Notes |
|---|---|---|---|
| Control ring buffer | Small; only control messages | 64 KB | Wire-protocol-encoded, small messages |
| Send ring buffer | Services × avg cross-host msg rate × latency | 1 MB | MPSC, all services write here |
| Max frame length | Largest application message + 24 byte header | 1 MB | Must be ≤ send_ring_buffer / 8 |
| TCP send buffer | Per connection; kernel-managed | 256 KB | Tuned via `setsockopt(SO_SNDBUF)` |
| TCP recv buffer | Per connection; kernel-managed | 256 KB | Tuned via `setsockopt(SO_RCVBUF)` |
| Peer write queue | Messages buffered while TCP is write-blocked | 1024 msgs | Overflow → message drop |
| Service messages ring buffer | Configurable per service | 1 MB | Application-specific |

All buffer sizes must be a power of 2.

---

## 19. Counters & Monitoring

### 19.1 System Counters

| ID | Name | Description |
|---|---|---|
| 0 | `bytes_sent` | Total bytes sent via TCP |
| 1 | `bytes_received` | Total bytes received via TCP |
| 2 | `messages_routed_local` | Messages routed to local services |
| 3 | `messages_routed_remote` | Messages forwarded to remote brokers |
| 4 | `tcp_connections_established` | TCP connections successfully established |
| 5 | `tcp_connections_closed` | TCP connections closed (any reason) |
| 6 | `tcp_connection_errors` | TCP connection errors (connect fail, write fail, etc.) |
| 7 | `heartbeats_sent` | Broker-to-broker heartbeats sent |
| 8 | `heartbeats_received` | Broker-to-broker heartbeats received |
| 9 | `services_registered` | Currently registered services |
| 10 | `services_removed` | Total services removed |
| 11 | `send_rb_back_pressure` | Send ring buffer full events |
| 12 | `service_back_pressure` | Service ring buffer full events (messages dropped) |
| 13 | `unknown_service_drops` | Messages for unknown service IDs |
| 14 | `peer_write_queue_overflow` | Messages dropped due to peer write queue full |
| 15 | `peer_disconnected_drops` | Messages dropped because peer TCP was down |
| 16 | `invalid_frames` | Malformed TCP frames received (connection closed) |
| 17 | `handshake_failures` | TCP handshake validation failures |
| 18 | `control_loop_cycle_time_max` | Max control loop cycle time (ns) |
| 19 | `sender_cycle_time_max` | Max sender cycle time (ns) |
| 20 | `receiver_cycle_time_max` | Max receiver cycle time (ns) |

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
| TCP sockets | `socket`/`bind`/`listen`/`accept`/`connect` | Same | `WSASocket`/`WSAConnect` |
| Non-blocking I/O | `fcntl(O_NONBLOCK)` | Same | `ioctlsocket(FIONBIO)` |
| I/O engine | `io_uring` | `kqueue` | IOCP (future) |
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
| `TCP_FRAME_HEADER_LENGTH` | 24 | On-wire TCP message frame header |
| `TCP_HANDSHAKE_LENGTH` | 24 | Connection handshake frame |
| `METADATA_HEADER_LENGTH` | 512 | Broker/service metadata header |
| `PAGE_SIZE` | 4096 | Memory page alignment |

### 22.2 Protocol Constants

| Constant | Value |
|---|---|
| `TCP_HANDSHAKE_MAGIC` | `0x474E4952` ("RING") |
| `TCP_PROTOCOL_VERSION` | 1 |
| `PADDING_MSG_TYPE_ID` | -1 |
| `FLAG_ADMIN` | 0x01 |
| `DIRECTION_SEND` | 0 |
| `DIRECTION_RECV` | 1 |
| `BROKER_SERVICE_ID` | 0 |
| `BROKER_SERVICE_NAME` | `"broker"` |
| `DEFAULT_MAX_FRAME_LENGTH` | 1 MB |

### 22.3 Timing Constants

| Constant | Value |
|---|---|
| `TCP_HEARTBEAT_INTERVAL_NS` | 100ms |
| `PEER_LIVENESS_TIMEOUT_NS` | 10s |
| `RECONNECT_INITIAL_DELAY_MS` | 100 |
| `RECONNECT_MAX_DELAY_MS` | 1000 |
| `SERVICE_HEARTBEAT_WRITE_INTERVAL_MS` | 1000 |
| `SERVICE_HEARTBEAT_CHECK_INTERVAL_MS` | 3000 |
| `SERVICE_HEARTBEAT_TIMEOUT_MS` | 10000 |
| `CONTROL_LOOP_TIMEOUT_CHECK_INTERVAL_NS` | 1s |
| `COMMAND_DRAIN_LIMIT` | 1 |
| `CONTROL_READ_LIMIT` | 10 |
| `SEND_BATCH_LIMIT` | 10 |
| `READ_BUDGET_PER_PEER` | 16 |
| `WRITE_BUDGET_PER_PEER` | 16 |

### 22.4 Memory Ordering Summary

| Operation | Ordering | Rationale |
|---|---|---|
| Write record length (negative) | Release | Sentinel visible before data |
| Write record length (positive) | Release | Commits record — all data visible |
| Read record length | Acquire | Ensures reading committed data |
| Write head_position | Release | Frees space for producers |
| Read head_position | Acquire | Producers see consumer progress |
| Write tail_position (MPSC) | CAS | Multi-producer coordination |
| Write heartbeat timestamp | Release | Readers see fresh value |
| Read heartbeat timestamp | Acquire | Stale reads acceptable but bounded |

### 22.5 Default Configuration Values

| Parameter | Default |
|---|---|
| `max_frame_length` | 1 MB |
| `control_buffer_length` | 64 KB |
| `send_buffer_length` | 1 MB |
| `tcp_send_buffer_size` | 256 KB |
| `tcp_recv_buffer_size` | 256 KB |
| `peer_write_queue_size` | 1024 messages |
| `counter_values_buffer_length` | 64 KB |
| `error_log_buffer_length` | 256 KB |
| `max_services` | 256 |
| `max_peers` | 16 |
| `io_uring_ring_size` | 256 |

---

## Appendix A: Connection Lifecycle

### A.1 Peer Connection Establishment

```
Broker A (connector)                    Broker B (listener)
     │                                      │
     │  TCP connect()                       │
     ├─────────────────────────────────────►│  TCP accept()
     │                                      │
     │  Handshake {magic, version=1,        │
     │    source=1, target=2, dir=SEND,     │
     │    epoch=42, group_hash=0xABCD}      │
     ├─────────────────────────────────────►│  Validate handshake:
     │                                      │  - magic/version OK
     │                                      │  - target_node_id matches
     │                                      │  - group_hash matches
     │                                      │  - epoch > existing → replace
     │                                      │
     │  Connection established              │  Associate with peer state
     │                                      │
     │  Message frames...                   │
     ├─────────────────────────────────────►│
     │                                      │
```

### A.2 Peer Disconnection

Detected via:
1. TCP read returns 0 (peer closed) or error
2. TCP write returns error (broken pipe, connection reset)
3. No message (including heartbeats) received for `peer_liveness_timeout` (10s)

On disconnection:
1. Close both TCP connections for that peer (send and recv)
2. Notify control loop (trigger election if needed)
3. Remove peer from sender's connection map
4. Sender begins reconnect with exponential backoff
5. Messages for that peer are dropped until reconnection

---

## Appendix B: Comparison with Full Aeron

| Feature | Aeron | RingLoom Broker | Impact |
|---|---|---|---|
| Transport | Custom reliable UDP | TCP (kernel-managed) | No retransmit/NAK/flow-control code |
| Streams per endpoint | Unlimited | 1 per peer (TCP conn) | No stream dispatch needed |
| Sessions per stream | Unlimited | N/A | No session tracking |
| Log buffer partitions | 3 (rotating) | None (TCP streams) | No receive log buffers |
| Publications | Concurrent + Exclusive | MPSC ring buffer | Simpler send path |
| Subscriptions | Multiple per stream | Direct routing by serviceId | No subscription bookkeeping |
| Flow control | Status Messages + window | TCP kernel + message drop | Much simpler |
| Loss detection | NAK + retransmit buffers | TCP handles it | No NAK/retransmit code |
| Fragmentation | BEGIN/END flags | TCP handles segmentation | No fragment reassembly |
| CnC file | Required | Not needed | No driver↔client protocol |
| Media Driver | Separate process | Integrated | Fewer context switches |
| Broadcast buffer | Driver→clients | Direct ring buffer writes | Lower latency |
| Congestion control | Static, Cubic | TCP kernel (Cubic/BBR) | Kernel-managed |
| Multicast | Full support | Not supported | Unicast only |
| Name resolution | Pluggable | Static config | Simple |
| IPC publications | Log buffer per pub | Direct shared memory ring buffers | Already part of RingLoom |
| I/O model | `sendmsg`/`recvmmsg` | io_uring/kqueue | Completion-based, async |
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
Service A → send RB → sender drain → TCP write → network → TCP read → route → service RB → Service B
  ~50ns      ~100ns     ~200ns       ~0.5µs     ~10-50µs   ~0.5µs   ~100ns    ~100ns       ~50ns

Total: ~11-52µs (dominated by network RTT)
```

### Latency Budget

| Step | Time | Notes |
|---|---|---|
| Service → send ring buffer write | 50-200ns | CAS + memcpy |
| Sender drain + io_uring submit | 200-500ns | Ring buffer read + SQE submission |
| TCP kernel processing | 200-500ns | Segmentation, checksumming |
| Network transit | 10-50µs | LAN dependent |
| TCP kernel receive | 200-500ns | Checksum, reassembly |
| Receiver io_uring completion | 100-300ns | CQE harvest + frame parse |
| Route to service ring buffer | 50-200ns | Lookup + memcpy |
| Service poll | 0-50ns | Depends on idle strategy |
| **Total** | **~11-52µs** | **End-to-end cross-host** |

**Note:** TCP_NODELAY is enabled by default, so Nagle's algorithm does not add buffering delay. The io_uring/kqueue completion model avoids per-message syscall overhead when batching.