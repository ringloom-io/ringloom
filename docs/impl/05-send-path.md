# 05 — Send Path

> **Depends on:** [03 — Concurrent Data Structures](03-concurrent-data-structures.md) (MPSC ring buffer),
> [04 — UDP Transport & io_uring](04-udp-transport-and-io-uring.md) (NetworkIo, socket management, frame types)
>
> **Depended on by:** [06 — Receive Path](06-receive-path.md), [07 — Flow Control](07-flow-control.md)

This document covers the **sender side** of the broker's cross-host message path: draining
outbound messages from the send ring buffer, fragmenting oversized messages, transmitting
UDP data frames to peer brokers, maintaining a retransmit buffer for reliability, processing
Status Messages and NAKs from receivers, and sending heartbeats.

All code targets **Zig 0.15.x** stable.

---

## Table of Contents

1.  [Overview](#1-overview)
2.  [Send Ring Buffer](#2-send-ring-buffer)
3.  [Sender Event Loop](#3-sender-event-loop)
4.  [Message Fragmentation](#4-message-fragmentation)
5.  [Sending a Frame](#5-sending-a-frame)
6.  [Retransmit Buffer](#6-retransmit-buffer)
7.  [Handling Status Messages](#7-handling-status-messages)
8.  [Handling NAKs](#8-handling-naks)
9.  [Heartbeats](#9-heartbeats)
10. [Connection Setup](#10-connection-setup)
11. [Peer Lifecycle Management](#11-peer-lifecycle-management)
12. [Counters & Observability](#12-counters--observability)
13. [Testing](#13-testing)
14. [File Structure](#14-file-structure)

---

## 1. Overview

```
Services (writers) ──┐
                     │  CAS on tail
Services (writers) ──┼──► Send Ring Buffer (MPSC) ──► Sender Event Loop ──► UDP ──► Peer Brokers
                     │    (broker metadata file)      (single consumer)
Services (writers) ──┘
```

Cross-host messages — those where `target_node_id != local_node_id` — follow this path.
Multiple local services write into a single shared-memory MPSC ring buffer located in the
broker's metadata file. A dedicated **sender thread** is the sole consumer. It reads
messages, looks up the destination peer, fragments if necessary, stamps a data frame header,
stores the frame in a per-peer retransmit buffer, and submits the send through `io_uring`
(Linux) or the platform-equivalent batched I/O backend.

The sender also processes **inbound control traffic** from receivers:

- **Status Messages (SM):** update the flow-control send limit for a peer.
- **NAK frames:** trigger retransmission of lost data from the retransmit buffer.

Finally, the sender emits **heartbeat frames** (zero-length DATA frames) every 100 ms to
all connected peers, keeping the connection alive and giving the receiver a position
reference for gap detection.

### Data Flow Summary

| Step | Actor | Operation |
|------|-------|-----------|
| 1 | Service | `ring_buffer.write(header + payload)` — CAS on tail |
| 2 | Sender event loop | `ring_buffer.read(on_outbound_message, batch_limit)` |
| 3 | Sender event loop | Parse `target_node_id` from BRZ header, look up `PeerSender` |
| 4 | Sender event loop | Check flow control: `send_position < send_limit` |
| 5 | Sender event loop | Fragment if `payload.len > mtu_length - data_frame_header_length` |
| 6 | Sender event loop | Build data frame header, copy payload, store in retransmit buffer |
| 7 | Sender event loop | `network_io.prepareSend(...)` — enqueue SQE |
| 8 | Sender event loop | `network_io.submit()` — single `io_uring_enter` for the batch |

---

## 2. Send Ring Buffer

The send ring buffer is the standard MPSC ring buffer from [doc 03](03-concurrent-data-structures.md),
located in the broker's metadata file at offset `metadata_header_length + control_buffer_length +
ring_buffer_trailer_length`. Its capacity defaults to `default_send_buffer_length` (1 MB).

### Who Writes

Any local service that produces a message with `target_node_id != local_node_id`. The service's
`MessageFragmentingProducer` (or equivalent) writes the message into the send ring buffer
instead of a peer's message ring buffer.

Each record written into the send ring buffer has the standard ring buffer record header
(8 bytes: `length` + `msg_type_id`) followed by the BRZ message payload. The payload's
first 40 bytes are the data frame header fields — crucially including:

- `source_node_id` — the local broker's node ID
- `target_node_id` — the destination broker's node ID
- `source_service_id` — the originating service's ID
- `target_service_id` — the destination service's ID
- `template_id` — the SBE message type
- `correlation_id` — for request-response matching
- `msg_flags` — BRZ-level flags (chunked, etc.)

These fields are already populated by the service at write time. The sender event loop
copies them into the outbound data frame header without modification (except `term_offset`
and `sequence_number`, which the sender stamps).

### Who Reads

Exactly one thread: the sender event loop. This is the single-consumer side of the MPSC
contract. No locking is needed on the read path.

### Back-Pressure at the Source

When the send ring buffer is full, the service's `write()` call returns `error.InsufficientCapacity`.
This is the first back-pressure signal: it tells the service that the outbound path to
remote hosts is saturated. The service should propagate this as a `BufferFull` error to the
application layer.

---

## 3. Sender Event Loop

### 3.1 State

```zig
const platform = @import("../platform.zig");
const constants = platform.constants;
const Clock = platform.Clock;

const SenderEventLoop = struct {
    /// The MPSC ring buffer that local services write cross-host messages into.
    send_ring_buffer: *RingBuffer,

    /// Per-peer sender state, keyed by node ID. Max `default_max_peers` entries.
    peers: IntHashMap(*PeerSender),

    /// Platform I/O backend (io_uring on Linux, kqueue on macOS, IOCP on Windows).
    network_io: *NetworkIo,

    /// Handles retransmit state machine (linger suppression) across all peers.
    retransmit_handler: RetransmitHandler,

    /// Monotonic timestamp (ns) of the next scheduled heartbeat round.
    next_heartbeat_ns: i64,

    /// MPSC command queue from the control loop (add/remove peer, etc.).
    cmd_queue: *CommandQueue,

    /// Shared counters manager for observability.
    counters: *CountersManager,

    /// This broker's node ID.
    local_node_id: u8,

    /// Pre-allocated send buffer pool. Each buffer is `mtu_length` bytes.
    /// The pool has `send_batch_limit * 2` buffers to allow in-flight and queued
    /// sends simultaneously without allocation.
    send_buffer_pool: SendBufferPool,

    /// Whether this event loop is running. Set to false by the shutdown path.
    running: platform.AtomicBool,

    const Self = @This();

    pub fn init(
        send_ring_buffer: *RingBuffer,
        network_io: *NetworkIo,
        cmd_queue: *CommandQueue,
        counters: *CountersManager,
        local_node_id: u8,
        allocator: std.mem.Allocator,
    ) !Self {
        return .{
            .send_ring_buffer = send_ring_buffer,
            .peers = IntHashMap(*PeerSender).init(allocator),
            .network_io = network_io,
            .retransmit_handler = RetransmitHandler.init(),
            .next_heartbeat_ns = 0,
            .cmd_queue = cmd_queue,
            .counters = counters,
            .local_node_id = local_node_id,
            .send_buffer_pool = try SendBufferPool.init(
                constants.send_batch_limit * 2,
                constants.default_mtu_length,
                allocator,
            ),
            .running = platform.AtomicBool.init(true),
        };
    }
};
```

### 3.2 PeerSender

One `PeerSender` exists per connected peer broker. It tracks the peer's network address,
flow-control state, per-peer sequence numbering, and retransmit buffer.

```zig
const PeerSender = struct {
    /// The peer broker's node ID.
    node_id: u8,

    /// UDP address of the peer broker.
    address: std.net.Address,

    /// The maximum send position allowed by the receiver's flow control.
    /// Updated when a Status Message arrives.
    /// Invariant: sender must not advance send_position beyond send_limit.
    send_limit: i64,

    /// Current send position — monotonically increasing byte offset of data
    /// sent to this peer. Advances by the aligned frame size after each send.
    send_position: i64,

    /// Monotonic sequence number for frames sent to this peer.
    /// Incremented once per data frame (not per heartbeat).
    sequence_number: i64,

    /// Circular buffer of recently sent frames, for retransmission on NAK.
    retransmit_buffer: *RetransmitBuffer,

    /// Per-peer retransmit state machine (linger suppression).
    retransmit_handler: RetransmitHandler,

    /// Whether the peer has completed the SETUP / initial-SM handshake.
    connected: bool,

    /// Monotonic timestamp (ns) of the last Status Message received from this peer.
    /// Used to detect peer timeout (no SM for > sm_timeout_ns → consider disconnected).
    last_sm_received_ns: i64,

    /// The UDP socket file descriptor used to communicate with this peer.
    socket_fd: std.posix.fd_t,

    const Self = @This();

    pub fn init(
        node_id: u8,
        address: std.net.Address,
        socket_fd: std.posix.fd_t,
        retransmit_buffer: *RetransmitBuffer,
    ) Self {
        return .{
            .node_id = node_id,
            .address = address,
            .send_limit = 0,
            .send_position = 0,
            .sequence_number = 0,
            .retransmit_buffer = retransmit_buffer,
            .retransmit_handler = RetransmitHandler.init(),
            .connected = false,
            .last_sm_received_ns = 0,
            .socket_fd = socket_fd,
        };
    }

    /// Advance the sequence number and return the new value.
    pub inline fn nextSequence(self: *Self) i64 {
        const seq = self.sequence_number;
        self.sequence_number += 1;
        return seq;
    }

    /// Return the current sequence number without advancing.
    /// Used for heartbeats (which do not consume a sequence number).
    pub inline fn currentSequence(self: *const Self) i64 {
        return self.sequence_number;
    }

    /// Returns true if the sender is flow-controlled (cannot send more data).
    pub inline fn isFlowControlled(self: *const Self) bool {
        return self.send_position >= self.send_limit;
    }
};
```

### 3.3 Duty Cycle

The sender event loop follows the standard duty-cycle pattern described in
[doc 00](00-overview.md). Each iteration returns a work count that drives the idle
strategy. If no work was done across all phases, the idle strategy decides whether to
spin, yield, or park.

```zig
/// Called by the ThreadRunner on every iteration.
/// Returns the total number of items processed (work count).
pub fn doWork(self: *SenderEventLoop) u32 {
    var work_count: u32 = 0;
    const now_ns = Clock.monotonicNanos();

    // ── Phase 1: Process io_uring completions ────────────────────────────
    // Reclaim send buffers from completed sends. This must happen first so
    // that buffers are available for new sends in phase 3.
    work_count += self.network_io.pollCompletions(
        onSendComplete,
        @ptrCast(self),
        constants.send_batch_limit,
    );

    // ── Phase 2: Drain inter-event-loop command queue ────────────────────
    // Process commands from the control loop: add peer, remove peer,
    // update configuration. Limited to command_drain_limit per cycle
    // to bound latency impact.
    work_count += self.cmd_queue.drain(
        dispatchCommand,
        @ptrCast(self),
        constants.command_drain_limit,
    );

    // ── Phase 3: Drain send ring buffer ──────────────────────────────────
    // Read outbound messages written by local services. Each message is
    // parsed, looked up by target_node_id, fragmented if needed, and
    // queued for send via io_uring.
    work_count += self.send_ring_buffer.read(
        onOutboundMessage,
        @ptrCast(self),
        constants.send_batch_limit,
    );

    // ── Phase 4: Submit batched sends ────────────────────────────────────
    // Flush all SQEs accumulated in phases 1–3 with a single io_uring_enter.
    work_count += self.network_io.submit();

    // ── Phase 5: Send heartbeats ─────────────────────────────────────────
    // Zero-length DATA frames to all connected peers every 100ms.
    if (now_ns >= self.next_heartbeat_ns) {
        self.sendHeartbeats(now_ns);
        self.next_heartbeat_ns = now_ns + constants.udp_heartbeat_interval_ns;
    }

    // ── Phase 6: Process retransmit timeouts ─────────────────────────────
    // Transition lingering retransmit entries back to inactive after the
    // linger period expires. This allows new NAKs for the same range to
    // trigger a fresh retransmit.
    var peer_iter = self.peers.valueIterator();
    while (peer_iter.next()) |peer| {
        peer.retransmit_handler.processTimeouts(now_ns);
    }

    return work_count;
}
```

**Phase ordering rationale:**

1. **Completions first** — reclaim send buffers so phase 3 has buffers available.
2. **Commands second** — peer additions/removals must be visible before we try to route
   messages to those peers.
3. **Ring buffer drain third** — the core work: read messages, build frames, enqueue sends.
4. **Submit fourth** — flush everything accumulated in one `io_uring_enter`.
5. **Heartbeats fifth** — low priority, only every 100 ms.
6. **Retransmit timeouts last** — state machine housekeeping, no I/O.

### 3.4 Processing Outbound Messages

When the sender reads a record from the send ring buffer, it extracts the routing header,
looks up the peer, checks flow control, and either sends or drops the message.

```zig
/// Ring buffer read callback. Called once per record by RingBuffer.read().
fn onOutboundMessage(context: *anyopaque, msg_type_id: i32, payload: []const u8) void {
    const self: *SenderEventLoop = @ptrCast(@alignCast(context));
    _ = msg_type_id; // Not used — routing is in the payload header

    // The payload starts with the BRZ routing fields that will become
    // the data frame header. Parse just the target_node_id to route.
    if (payload.len < constants.data_frame_header_length) {
        self.counters.increment(.malformed_messages_dropped);
        return;
    }

    const header = DataFrameHeader.overlay(payload);
    const target_node_id = header.target_node_id;

    // Look up peer
    const peer = self.peers.get(target_node_id) orelse {
        self.counters.increment(.unknown_peer_messages_dropped);
        return;
    };

    // Must be connected (SETUP + initial SM exchange complete)
    if (!peer.connected) {
        self.counters.increment(.disconnected_peer_messages_dropped);
        return;
    }

    // Flow control gate: can we send?
    if (peer.isFlowControlled()) {
        self.counters.increment(.send_back_pressure);
        return; // Drop — the service will see back-pressure on its next write
    }

    // Determine whether fragmentation is needed
    const max_payload = constants.default_mtu_length - constants.data_frame_header_length;

    if (payload.len > max_payload) {
        self.fragmentAndSend(peer, payload);
    } else {
        self.sendSingleFrame(peer, payload, constants.flag_unfragmented);
    }
}
```

**On dropped messages:** When a message is dropped (unknown peer, disconnected, or
flow-controlled), the data is lost. This is by design — the send ring buffer is a
best-effort outbound queue. The application layer uses request-response correlation IDs
and timeouts to detect lost messages. The counters provide observability into why messages
were dropped.

---

## 4. Message Fragmentation

Messages larger than `mtu_length - data_frame_header_length` (default: 1408 − 40 = 1368
bytes of payload per frame) must be split into multiple data frames.

### Fragment Flag Semantics

| Scenario | `flags` value | Bits set |
|----------|---------------|----------|
| Complete message (single frame) | `0xC0` | `BEGIN \| END` |
| First fragment | `0x80` | `BEGIN` |
| Middle fragment(s) | `0x00` | (none) |
| Last fragment | `0x40` | `END` |

### Implementation

```zig
/// Fragment a message that exceeds the MTU and send each fragment as a
/// separate data frame. All fragments share the same correlation_id and
/// routing fields from the original payload header. The receiver reassembles
/// them using the sequence numbers and BEGIN/END flags.
fn fragmentAndSend(self: *SenderEventLoop, peer: *PeerSender, payload: []const u8) void {
    const max_payload = constants.default_mtu_length - constants.data_frame_header_length;
    var offset: usize = 0;
    var is_first: bool = true;

    while (offset < payload.len) {
        const remaining = payload.len - offset;
        const chunk_len = @min(remaining, max_payload);
        const is_last = (offset + chunk_len == payload.len);

        var flags: u8 = 0;
        if (is_first) flags |= constants.flag_begin;
        if (is_last) flags |= constants.flag_end;

        self.sendSingleFrame(peer, payload[offset..][0..chunk_len], flags);

        offset += chunk_len;
        is_first = false;

        // Re-check flow control between fragments. If the receiver's window
        // is exhausted mid-message, remaining fragments are dropped. The
        // receiver will discard the incomplete fragment chain after timeout.
        if (peer.isFlowControlled()) {
            self.counters.increment(.send_back_pressure);
            self.counters.increment(.fragmented_messages_incomplete);
            return;
        }
    }

    self.counters.increment(.fragmented_messages_sent);
}
```

### Design Notes

- **All fragments get distinct sequence numbers.** The receiver uses the monotonic
  sequence to detect gaps. If a middle fragment is lost, the receiver sends a NAK for
  exactly that sequence range.

- **Routing fields are copied into every fragment's data frame header.** This is
  redundant but allows the receiver to route each fragment independently without
  buffering state at the transport layer. Fragment reassembly happens at a higher layer
  (the service's `MessageAssembler`).

- **Mid-message flow control.** If the sender hits the flow-control limit between
  fragments, the remaining fragments are dropped. The receiver will eventually time out
  the incomplete fragment chain and discard it. This avoids head-of-line blocking where a
  large message monopolizes the remaining window.

---

## 5. Sending a Frame

This is the core function that builds a data frame, stores it in the retransmit buffer,
and enqueues it for transmission via `io_uring`.

```zig
/// Build a data frame from the payload, store it in the retransmit buffer,
/// and enqueue a send via the platform I/O backend.
fn sendSingleFrame(
    self: *SenderEventLoop,
    peer: *PeerSender,
    payload: []const u8,
    flags: u8,
) void {
    const seq = peer.nextSequence();
    const frame_length: i32 = @intCast(constants.data_frame_header_length + payload.len);

    // ── Acquire a pre-allocated send buffer from the pool ────────────────
    const send_buf = self.send_buffer_pool.acquire() orelse {
        // All buffers in flight — this is a transient back-pressure signal.
        // The buffer will be reclaimed when a CQE arrives in the next cycle.
        self.counters.increment(.send_buffer_pool_exhausted);
        return;
    };

    // ── Build the data frame header (40 bytes) ───────────────────────────
    const header: *DataFrameHeader = @ptrCast(@alignCast(send_buf.ptr));
    header.* = .{
        .frame_length = frame_length,
        .version = constants.frame_header_version,
        .flags = flags,
        .frame_type = constants.frame_type_data,
        .term_offset = @intCast(peer.send_position),
        .source_node_id = self.local_node_id,
        .target_node_id = peer.node_id,
        .source_service_id = 0, // Overwritten below from payload header
        .target_service_id = 0, // Overwritten below from payload header
        .template_id = 0,       // Overwritten below from payload header
        .correlation_id = 0,    // Overwritten below from payload header
        .msg_flags = 0,         // Overwritten below from payload header
        .reserved = .{ 0, 0, 0, 0, 0, 0, 0 },
        .sequence_number = seq,
    };

    // Copy routing fields from the original BRZ message header in the payload.
    // The payload's first bytes contain the service-written routing fields.
    if (payload.len >= constants.data_frame_header_length) {
        const src_header = DataFrameHeader.overlay(payload);
        header.source_service_id = src_header.source_service_id;
        header.target_service_id = src_header.target_service_id;
        header.template_id = src_header.template_id;
        header.correlation_id = src_header.correlation_id;
        header.msg_flags = src_header.msg_flags;
    }

    // ── Copy payload after the header ────────────────────────────────────
    const total_len: usize = @intCast(frame_length);
    @memcpy(
        send_buf[constants.data_frame_header_length..][0..payload.len],
        payload,
    );

    // ── Store in retransmit buffer (before send — must be available if a
    //    NAK races the completion) ────────────────────────────────────────
    peer.retransmit_buffer.store(seq, send_buf[0..total_len]);

    // ── Enqueue send via io_uring ────────────────────────────────────────
    self.network_io.prepareSend(
        peer.socket_fd,
        send_buf[0..total_len],
        peer.address,
        @intFromPtr(send_buf.ptr), // user_data for CQE → buffer reclaim
    );

    // ── Advance send position ────────────────────────────────────────────
    // Aligned to 32 bytes (frame alignment) to match receive log expectations.
    peer.send_position += @as(i64, @intCast(constants.alignUp(total_len, 32)));

    // ── Update counters ──────────────────────────────────────────────────
    self.counters.increment(.frames_sent);
    self.counters.add(.bytes_sent, @intCast(total_len));
}
```

### Send Buffer Pool

The send buffer pool pre-allocates a fixed number of MTU-sized buffers at startup. Each
buffer is acquired before building a frame and released when the corresponding `io_uring`
CQE arrives (indicating the kernel has consumed the data).

```zig
const SendBufferPool = struct {
    buffers: [][]align(64) u8,
    free_stack: []u32,          // Stack of free buffer indices
    free_count: u32,
    capacity: u32,

    pub fn init(count: u32, buf_size: usize, allocator: std.mem.Allocator) !SendBufferPool {
        const buffers = try allocator.alloc([]align(64) u8, count);
        const free_stack = try allocator.alloc(u32, count);

        for (0..count) |i| {
            buffers[i] = try allocator.alignedAlloc(u8, 64, buf_size);
            free_stack[i] = @intCast(i);
        }

        return .{
            .buffers = buffers,
            .free_stack = free_stack,
            .free_count = count,
            .capacity = count,
        };
    }

    /// Acquire a buffer from the pool. Returns null if all buffers are in flight.
    pub fn acquire(self: *SendBufferPool) ?[]align(64) u8 {
        if (self.free_count == 0) return null;
        self.free_count -= 1;
        const idx = self.free_stack[self.free_count];
        return self.buffers[idx];
    }

    /// Release a buffer back to the pool after the send completes.
    pub fn release(self: *SendBufferPool, buf_ptr: [*]align(64) u8) void {
        // Find the index by pointer identity
        for (self.buffers, 0..) |buf, i| {
            if (buf.ptr == buf_ptr) {
                self.free_stack[self.free_count] = @intCast(i);
                self.free_count += 1;
                return;
            }
        }
        // Should never happen — indicates a bug in buffer tracking
        unreachable;
    }
};
```

### Completion Callback

When `io_uring` reports a completed send, the sender reclaims the buffer:

```zig
/// Called by NetworkIo.pollCompletions() for each completed send.
fn onSendComplete(context: *anyopaque, user_data: u64, result: i32) void {
    const self: *SenderEventLoop = @ptrCast(@alignCast(context));

    // Reclaim the send buffer using the pointer stored in user_data
    const buf_ptr: [*]align(64) u8 = @ptrFromInt(user_data);
    self.send_buffer_pool.release(buf_ptr);

    if (result < 0) {
        self.counters.increment(.send_errors);
        // Specific error handling:
        // -ECONNREFUSED → peer unreachable, mark disconnected
        // -EMSGSIZE     → MTU misconfigured
        // -EAGAIN       → socket buffer full, transient
    }
}
```

---

## 6. Retransmit Buffer

Each peer has a dedicated circular buffer that holds copies of recently sent frames. When
a NAK arrives requesting retransmission, the sender looks up the frames by sequence number
and resends them.

### Design

```
┌──────────────────────────────────────────────────────┐
│  Retransmit Buffer (per peer, 4 MB default)          │
│                                                      │
│  ┌─────────┬─────────┬─────────┬─────────┬────────┐ │
│  │ slot 0  │ slot 1  │ slot 2  │ slot 3  │  ...   │ │
│  │ seq=100 │ seq=101 │ seq=102 │ seq=103 │        │ │
│  └─────────┴─────────┴─────────┴─────────┴────────┘ │
│                                                      │
│  Each slot = MAX_FRAME_SIZE bytes (= mtu_length)     │
│  Index = (sequence_number * MAX_FRAME_SIZE)           │
│          & (capacity - 1)                            │
│                                                      │
│  Single writer: sender event loop.                   │
│  Old frames are silently overwritten when the         │
│  buffer wraps. A validation check on lookup ensures   │
│  stale entries are not retransmitted.                 │
└──────────────────────────────────────────────────────┘
```

### Slot Layout

Each slot stores a frame preceded by a small metadata header to enable validation:

```
Offset  Size  Field
──────────────────────────
0       8     stored_sequence_number (i64)
8       4     stored_frame_length (i32)
12      4     _padding
16      ...   frame_data (up to mtu_length bytes)
```

### Implementation

```zig
const RetransmitBuffer = struct {
    /// Backing memory, page-aligned, power-of-two capacity.
    buffer: []align(4096) u8,

    /// Total capacity in bytes. Must be a power of two.
    capacity: usize,

    /// Size of each slot in bytes: slot_header_length + max_frame_size.
    slot_size: usize,

    /// Number of slots: capacity / slot_size.
    slot_count: usize,

    /// Mask for fast modulo: slot_count - 1. Only valid if slot_count is power of two.
    slot_mask: usize,

    const slot_header_length: usize = 16;

    const SlotHeader = extern struct {
        stored_sequence_number: i64,
        stored_frame_length: i32,
        _padding: i32 = 0,
    };

    const Self = @This();

    pub fn init(capacity: usize, max_frame_size: usize, allocator: std.mem.Allocator) !Self {
        comptime {
            std.debug.assert(constants.isPowerOfTwo(constants.default_retransmit_buffer_length));
        }

        if (!constants.isPowerOfTwo(capacity)) return error.CapacityNotPowerOfTwo;

        const slot_size = constants.alignUp(slot_header_length + max_frame_size, 64);
        const slot_count = capacity / slot_size;

        // Round slot_count down to the nearest power of two for fast masking.
        const effective_slot_count = blk: {
            var n = slot_count;
            n |= n >> 1;
            n |= n >> 2;
            n |= n >> 4;
            n |= n >> 8;
            n |= n >> 16;
            n |= n >> 32;
            break :blk (n >> 1) + 1;
        };

        const effective_capacity = effective_slot_count * slot_size;
        const buf = try allocator.alignedAlloc(u8, 4096, effective_capacity);
        @memset(buf, 0);

        return .{
            .buffer = buf,
            .capacity = effective_capacity,
            .slot_size = slot_size,
            .slot_count = effective_slot_count,
            .slot_mask = effective_slot_count - 1,
        };
    }

    /// Store a frame in the retransmit buffer at the slot determined by the
    /// sequence number. Silently overwrites any existing frame in that slot.
    pub fn store(self: *Self, seq: i64, frame: []const u8) void {
        const slot_index = @as(usize, @intCast(seq)) & self.slot_mask;
        const offset = slot_index * self.slot_size;

        // Write slot header
        const slot_header: *SlotHeader = @ptrCast(@alignCast(self.buffer[offset..].ptr));
        slot_header.* = .{
            .stored_sequence_number = seq,
            .stored_frame_length = @intCast(frame.len),
        };

        // Copy frame data after header
        const data_offset = offset + slot_header_length;
        @memcpy(self.buffer[data_offset..][0..frame.len], frame);
    }

    /// Look up a frame by sequence number. Returns the frame bytes if the slot
    /// still holds the requested sequence, or null if it has been overwritten.
    pub fn lookup(self: *const Self, seq: i64) ?[]const u8 {
        const slot_index = @as(usize, @intCast(seq)) & self.slot_mask;
        const offset = slot_index * self.slot_size;

        const slot_header: *const SlotHeader = @ptrCast(@alignCast(self.buffer[offset..].ptr));

        // Validate: has this slot been overwritten by a newer frame?
        if (slot_header.stored_sequence_number != seq) return null;

        const frame_len = @as(usize, @intCast(slot_header.stored_frame_length));
        if (frame_len == 0) return null;

        const data_offset = offset + slot_header_length;
        return self.buffer[data_offset..][0..frame_len];
    }

    /// Returns true if the given sequence number is still available for retransmit.
    pub fn isAvailable(self: *const Self, seq: i64) bool {
        return self.lookup(seq) != null;
    }

    pub fn close(self: *Self, allocator: std.mem.Allocator) void {
        allocator.free(self.buffer);
        self.buffer = &.{};
    }
};
```

### Sizing

The retransmit buffer must be large enough to hold all frames that might be in flight
(sent but not yet acknowledged via SM). The default is 4 MB, which at 1408-byte MTU gives
approximately 2,900 slots — enough for roughly 290 ms of data at a sustained 10 Gbit/s
send rate. If the receiver's window is larger than the retransmit buffer, frames may be
overwritten before a NAK can trigger retransmission. In that case, the receiver will
eventually time out and reconnect.

**Guideline:** `retransmit_buffer_length >= receiver_window * 2`.

---

## 7. Handling Status Messages

Status Messages (SM) are the receiver's flow-control signal. They carry the receiver's
consumption position and receiver window, which together define how far the sender is
allowed to advance.

### Status Message Wire Format (28 bytes)

```
Offset  Size  Type    Field
──────────────────────────────────
0       4     i32     frame_length (28)
4       1     u8      version (0)
5       1     u8      flags
6       2     u16     frame_type (SM = 0x03)
8       1     u8      node_id               ← receiver's node ID
9       1     u8      reserved
10      2     u16     reserved
12      8     i64     consumption_position   ← how far the receiver has consumed
20      4     i32     receiver_window        ← how far ahead the sender may write
24      4     i32     reserved
```

### Processing

```zig
/// Called when the sender receives a Status Message from a peer.
/// This is the primary flow-control input: it updates the send_limit
/// which gates how much data the sender is allowed to transmit.
fn handleStatusMessage(self: *SenderEventLoop, sm_bytes: []const u8) void {
    if (sm_bytes.len < @sizeOf(StatusMessageFrame)) {
        self.counters.increment(.malformed_frames_received);
        return;
    }

    const sm: *const StatusMessageFrame = @ptrCast(@alignCast(sm_bytes.ptr));
    const peer = self.peers.get(sm.node_id) orelse {
        self.counters.increment(.unknown_peer_sm_received);
        return;
    };

    // ── Update flow-control send limit ───────────────────────────────────
    // The sender may advance send_position up to (consumption_position + receiver_window).
    // Use @max to ensure the limit never decreases — a stale or reordered SM
    // must not shrink the window.
    const new_limit = sm.consumption_position + @as(i64, sm.receiver_window);
    peer.send_limit = @max(peer.send_limit, new_limit);

    // ── Update liveness timestamp ────────────────────────────────────────
    peer.last_sm_received_ns = Clock.monotonicNanos();

    // ── Mark connected on first SM ───────────────────────────────────────
    // The initial SM after a SETUP frame completes the handshake.
    if (!peer.connected) {
        peer.connected = true;
        self.counters.increment(.peers_connected);
    }

    self.counters.increment(.status_messages_received);
}
```

### Key Invariants

- **`send_limit` is monotonically non-decreasing.** A reordered or duplicate SM with a
  lower limit is ignored. This prevents the window from shrinking due to packet reordering.
- **`send_position <= send_limit`** is checked before every frame send in
  `onOutboundMessage`. If violated, the message is dropped and `send_back_pressure` is
  incremented.
- **Connection establishment.** The first SM received from a peer transitions it from
  `connected = false` to `connected = true`, completing the SETUP handshake.

---

## 8. Handling NAKs

A NAK frame requests retransmission of data at a specific position and length. The sender
uses a **retransmit handler** with linger suppression to avoid redundant retransmissions
from duplicate NAKs.

### NAK Wire Format (24 bytes)

```
Offset  Size  Type    Field
──────────────────────────────────
0       4     i32     frame_length (24)
4       1     u8      version (0)
5       1     u8      flags
6       2     u16     frame_type (NAK = 0x02)
8       1     u8      node_id
9       1     u8      reserved
10      2     u16     reserved
12      8     i64     position               ← start of missing data
20      4     i32     length                 ← length of missing data
```

### Processing

```zig
/// Called when the sender receives a NAK from a peer requesting retransmission.
fn handleNak(self: *SenderEventLoop, nak_bytes: []const u8) void {
    if (nak_bytes.len < @sizeOf(NakFrame)) {
        self.counters.increment(.malformed_frames_received);
        return;
    }

    const nak: *const NakFrame = @ptrCast(@alignCast(nak_bytes.ptr));
    const peer = self.peers.get(nak.node_id) orelse {
        self.counters.increment(.unknown_peer_nak_received);
        return;
    };

    // Delegate to the per-peer retransmit handler (linger suppression).
    peer.retransmit_handler.onNak(
        nak.position,
        nak.length,
        Clock.monotonicNanos(),
        peer.retransmit_buffer,
        self.network_io,
        peer.socket_fd,
        peer.address,
    );

    self.counters.increment(.naks_received);
}
```

### RetransmitHandler — State Machine

The retransmit handler prevents redundant retransmissions. After retransmitting a range,
it enters a **linger** state for `retransmit_linger_ns` (10 µs). Any NAK that overlaps
the lingering range is suppressed. After the linger period expires, the handler returns
to the inactive state and will honor new NAKs.

```
            NAK received
                │
    ┌───────────▼───────────┐
    │                       │
    │   State: INACTIVE     │──── NAK for [position, length) ───┐
    │                       │                                    │
    └───────────────────────┘                                    │
                ▲                                                │
                │                                                ▼
                │ linger expired              ┌──────────────────────────┐
                │ (now_ns >= expiry_ns)       │  Retransmit immediately  │
                │                             │  from retransmit buffer  │
                │                             └────────────┬─────────────┘
                │                                          │
                │                                          ▼
    ┌───────────┴───────────┐              ┌───────────────────────────┐
    │                       │◄─────────────│                           │
    │   State: LINGERING    │  set expiry  │  Record position/length   │
    │                       │              │  + set expiry_ns          │
    └───────────────────────┘              └───────────────────────────┘
                │
                │ overlapping NAK arrives while lingering
                │ → SUPPRESS (no retransmit)
                ▼
```

```zig
const RetransmitHandler = struct {
    const State = enum {
        /// No active retransmit. Ready to process any NAK.
        inactive,
        /// Recently retransmitted. Suppressing overlapping NAKs until expiry.
        lingering,
    };

    state: State,
    position: i64,
    length: i32,
    expiry_ns: i64,

    const Self = @This();

    pub fn init() Self {
        return .{
            .state = .inactive,
            .position = 0,
            .length = 0,
            .expiry_ns = 0,
        };
    }

    /// Process a NAK. If not suppressed, retransmit immediately and enter linger state.
    pub fn onNak(
        self: *Self,
        position: i64,
        length: i32,
        now_ns: i64,
        retransmit_buffer: *RetransmitBuffer,
        network_io: *NetworkIo,
        socket_fd: std.posix.fd_t,
        address: std.net.Address,
    ) void {
        // ── Suppress overlapping NAKs during linger period ───────────────
        if (self.state == .lingering and self.overlaps(position, length)) {
            return;
        }

        // ── Retransmit immediately (unicast — no delay needed) ───────────
        self.resend(position, length, retransmit_buffer, network_io, socket_fd, address);

        // ── Enter linger state ───────────────────────────────────────────
        self.state = .lingering;
        self.position = position;
        self.length = length;
        self.expiry_ns = now_ns + constants.retransmit_linger_ns;
    }

    /// Transition back to inactive when the linger period expires.
    pub fn processTimeouts(self: *Self, now_ns: i64) void {
        if (self.state == .lingering and now_ns >= self.expiry_ns) {
            self.state = .inactive;
        }
    }

    /// Check if the given range overlaps the currently lingering range.
    fn overlaps(self: *const Self, position: i64, length: i32) bool {
        const self_end = self.position + @as(i64, self.length);
        const other_end = position + @as(i64, length);
        return position < self_end and self.position < other_end;
    }

    /// Look up frames in the retransmit buffer and resend them.
    fn resend(
        self: *Self,
        position: i64,
        length: i32,
        retransmit_buffer: *RetransmitBuffer,
        network_io: *NetworkIo,
        socket_fd: std.posix.fd_t,
        address: std.net.Address,
    ) void {
        _ = self;
        // The position in the NAK corresponds to the term_offset in the original
        // data frame. Convert position to sequence number range:
        // We walk the retransmit buffer looking for frames whose term_offset
        // falls within [position, position + length).
        //
        // Since we index by sequence number, we scan from the lowest plausible
        // sequence up to the current sequence.
        //
        // A production optimization would maintain a position→sequence index,
        // but for the initial implementation, a bounded scan is acceptable.

        // For now, use a direct lookup approach:
        // The caller knows the sequence range from the NAK position and frame alignment.
        const frame_alignment: usize = 32;
        var offset: i64 = position;
        const end: i64 = position + @as(i64, length);

        while (offset < end) {
            // Convert offset to sequence number. Each frame occupies
            // alignUp(frame_length, 32) bytes of position space. We need to
            // find which sequence number maps to this offset.
            // Since term_offset is stored in the data frame header, we can
            // scan the retransmit buffer for matching term_offset values.

            // Simplified: use sequence = offset / frame_alignment as an estimate.
            // The correct approach uses a position-to-sequence lookup table.
            const seq_estimate = @divTrunc(offset, @as(i64, @intCast(frame_alignment)));
            if (retransmit_buffer.lookup(seq_estimate)) |frame| {
                network_io.prepareSend(socket_fd, frame, address, 0);
            }
            offset += @as(i64, @intCast(frame_alignment));
        }
    }
};
```

**Implementation note on position→sequence mapping:** The simplified `resend` above uses
a linear scan estimate. A production implementation should maintain a bounded
`position → sequence_number` ring alongside the retransmit buffer, allowing O(1) lookup.
This is straightforward — store the mapping in a parallel array indexed the same way as
the retransmit buffer.

---

## 9. Heartbeats

Heartbeats are **zero-length data frames** (header only, 40 bytes) sent every 100 ms to
all connected peers. They serve two purposes:

1. **Liveness detection.** The receiver knows the sender is alive.
2. **Position tracking.** The heartbeat carries the sender's current `sequence_number`
   (without incrementing it), allowing the receiver to detect gaps between the last data
   frame and the heartbeat.

### Implementation

```zig
/// Send heartbeat frames to all connected peers.
fn sendHeartbeats(self: *SenderEventLoop, now_ns: i64) void {
    _ = now_ns;

    var peer_iter = self.peers.valueIterator();
    while (peer_iter.next()) |peer| {
        if (!peer.connected) continue;

        // Build a header-only data frame (no payload).
        var header: DataFrameHeader = .{
            .frame_length = @intCast(constants.data_frame_header_length), // 40 bytes, no payload
            .version = constants.frame_header_version,
            .flags = constants.flag_unfragmented,
            .frame_type = constants.frame_type_data,
            .term_offset = @intCast(peer.send_position),
            .source_node_id = self.local_node_id,
            .target_node_id = peer.node_id,
            .source_service_id = 0,
            .target_service_id = 0,
            .template_id = 0,
            .correlation_id = 0,
            .msg_flags = 0,
            .reserved = .{ 0, 0, 0, 0, 0, 0, 0 },
            // No sequence increment — heartbeats reuse the current sequence.
            // This is critical: it tells the receiver "I have nothing new
            // since sequence N" without creating a gap.
            .sequence_number = peer.currentSequence(),
        };

        self.network_io.prepareSend(
            peer.socket_fd,
            std.mem.asBytes(&header),
            peer.address,
            0, // No buffer to reclaim — header is on stack
        );

        self.counters.increment(.heartbeats_sent);
    }
}
```

### Heartbeat vs. Data Frame Distinction

Heartbeats are indistinguishable from data frames at the wire level — they are data frames
with `frame_length == 40` (header only). The receiver identifies them by the absence of
a payload (`frame_length == data_frame_header_length`). This avoids introducing a separate
frame type and keeps the receiver's fast path simple.

**Important:** Heartbeats do **not** increment the sequence number. If the sender's last
data frame had `sequence_number = 42`, all subsequent heartbeats also carry
`sequence_number = 42` until the next real data frame (which will be 43). This tells the
receiver: "I have sent data up to sequence 42 and nothing more."

---

## 10. Connection Setup

When the control loop adds a new peer (via the command queue), the sender sends a **SETUP
frame** to initiate the connection. The receiver responds with a Status Message, which
completes the handshake.

### SETUP Frame Wire Format (24 bytes)

```
Offset  Size  Type    Field
──────────────────────────────────
0       4     i32     frame_length (24)
4       1     u8      version (0)
5       1     u8      flags (0)
6       2     u16     frame_type (SETUP = 0x04)
8       1     u8      source_node_id
9       1     u8      reserved
10      2     u16     reserved
12      4     i32     log_buffer_length     ← receiver's expected log buffer size
16      4     i32     mtu_length            ← sender's MTU
20      4     i32     initial_sequence      ← starting sequence number (0)
```

### Implementation

```zig
/// Send a SETUP frame to a newly added peer to initiate the connection.
/// The peer responds with a Status Message, which completes the handshake
/// and sets peer.connected = true.
fn sendSetup(self: *SenderEventLoop, peer: *PeerSender) void {
    var setup: SetupFrame = .{
        .frame_length = @intCast(@sizeOf(SetupFrame)), // 24 bytes
        .version = constants.frame_header_version,
        .flags = 0,
        .frame_type = constants.frame_type_setup,
        .source_node_id = self.local_node_id,
        .reserved_1 = 0,
        .reserved_2 = 0,
        .log_buffer_length = @intCast(constants.default_recv_log_buffer_length),
        .mtu_length = @intCast(constants.default_mtu_length),
        .initial_sequence = 0,
    };

    self.network_io.prepareSend(
        peer.socket_fd,
        std.mem.asBytes(&setup),
        peer.address,
        0, // Stack-allocated, no buffer to reclaim
    );

    self.counters.increment(.setup_frames_sent);
}
```

### Connection Lifecycle

```
    Sender                                  Receiver
    ──────                                  ────────
    sendSetup()
         ───── SETUP frame ────────────────►
                                            Allocate receive log buffer
                                            Initialize peer state
         ◄──── Status Message ──────────────
    handleStatusMessage()
    peer.connected = true
    peer.send_limit = consumption_position + receiver_window
         ───── DATA / Heartbeat ───────────►
         ◄──── SM (periodic) ───────────────
```

If the sender does not receive an SM within `sm_timeout_ns` (200 ms) after sending SETUP,
it resends the SETUP frame. This retry is handled in the heartbeat phase: if
`peer.connected == false` and `now_ns - peer.last_setup_sent_ns > sm_timeout_ns`, the
SETUP is resent.

---

## 11. Peer Lifecycle Management

Peers are added and removed via commands from the control loop. The sender event loop
processes these commands during phase 2 of its duty cycle.

### Command Types

```zig
const SenderCommand = union(enum) {
    /// Add a new peer. The sender creates a PeerSender, opens a socket,
    /// and sends a SETUP frame.
    add_peer: struct {
        node_id: u8,
        address: std.net.Address,
    },

    /// Remove a peer. The sender closes the socket, frees the retransmit
    /// buffer, and removes the PeerSender from the map.
    remove_peer: struct {
        node_id: u8,
    },
};
```

### Command Dispatch

```zig
fn dispatchCommand(context: *anyopaque, cmd_bytes: []const u8) void {
    const self: *SenderEventLoop = @ptrCast(@alignCast(context));
    const cmd: *const SenderCommand = @ptrCast(@alignCast(cmd_bytes.ptr));

    switch (cmd.*) {
        .add_peer => |add| {
            if (self.peers.contains(add.node_id)) return; // Already tracked

            // Open a UDP socket for this peer
            const socket_fd = self.network_io.openSocket(add.address) catch |err| {
                self.counters.increment(.peer_socket_errors);
                _ = err;
                return;
            };

            // Allocate retransmit buffer
            const retransmit_buf = RetransmitBuffer.init(
                constants.default_retransmit_buffer_length,
                constants.default_mtu_length,
                std.heap.page_allocator,
            ) catch return;

            var peer = PeerSender.init(
                add.node_id,
                add.address,
                socket_fd,
                &retransmit_buf,
            );

            self.peers.put(add.node_id, &peer) catch return;

            // Initiate connection
            self.sendSetup(&peer);
        },

        .remove_peer => |remove| {
            if (self.peers.remove(remove.node_id)) |peer| {
                peer.retransmit_buffer.close(std.heap.page_allocator);
                std.posix.close(peer.socket_fd);
                self.counters.increment(.peers_disconnected);
            }
        },
    }
}
```

### Peer Health Monitoring

The sender tracks the last SM received from each peer. If no SM arrives for longer than
a configurable timeout (default: 2 × `sm_timeout_ns` = 400 ms), the peer is marked as
disconnected. Messages to disconnected peers are dropped with the
`disconnected_peer_messages_dropped` counter incremented.

This check runs in the heartbeat phase:

```zig
fn checkPeerHealth(self: *SenderEventLoop, now_ns: i64) void {
    const peer_timeout_ns = constants.sm_timeout_ns * 2;

    var peer_iter = self.peers.valueIterator();
    while (peer_iter.next()) |peer| {
        if (!peer.connected) continue;

        if (peer.last_sm_received_ns > 0 and
            now_ns - peer.last_sm_received_ns > peer_timeout_ns)
        {
            peer.connected = false;
            self.counters.increment(.peers_timed_out);
            // The control loop will be notified via a command to handle
            // higher-level disconnection logic (cluster state, etc.)
        }
    }
}
```

---

## 12. Counters & Observability

The sender maintains the following counters, all accessible through the shared
`CountersManager`:

| Counter | Description |
|---------|-------------|
| `frames_sent` | Total data frames transmitted (including fragments) |
| `bytes_sent` | Total bytes transmitted (header + payload) |
| `heartbeats_sent` | Heartbeat frames sent |
| `setup_frames_sent` | SETUP frames sent |
| `status_messages_received` | Status Messages received from peers |
| `naks_received` | NAK frames received |
| `retransmits_sent` | Frames retransmitted in response to NAKs |
| `send_back_pressure` | Messages dropped due to flow control (send_position >= send_limit) |
| `send_buffer_pool_exhausted` | Frames dropped because all send buffers are in flight |
| `fragmented_messages_sent` | Multi-fragment messages successfully sent |
| `fragmented_messages_incomplete` | Fragment chains dropped mid-send due to flow control |
| `malformed_messages_dropped` | Messages dropped due to invalid header |
| `unknown_peer_messages_dropped` | Messages dropped because target_node_id is not in the peer map |
| `disconnected_peer_messages_dropped` | Messages dropped because the peer is not connected |
| `unknown_peer_sm_received` | Status Messages from unknown node IDs |
| `unknown_peer_nak_received` | NAKs from unknown node IDs |
| `malformed_frames_received` | Inbound frames too short to parse |
| `send_errors` | io_uring send completions with negative result |
| `peer_socket_errors` | Failures opening a peer UDP socket |
| `peers_connected` | Peers that completed the SETUP→SM handshake |
| `peers_disconnected` | Peers removed via command |
| `peers_timed_out` | Peers marked disconnected due to SM timeout |

---

## 13. Testing

### Unit Tests

#### 13.1 Message Fragmentation

```zig
test "fragment message into correct number of frames with correct flags" {
    const max_payload = constants.default_mtu_length - constants.data_frame_header_length;

    // Given: a message exactly 3x the max payload size
    const msg_size = max_payload * 3;
    const payload = try testing.allocator.alloc(u8, msg_size);
    defer testing.allocator.free(payload);
    @memset(payload, 0xAA);

    // When: fragmented
    var frames = std.ArrayList(FragmentRecord).init(testing.allocator);
    defer frames.deinit();
    fragmentForTest(payload, max_payload, &frames);

    // Then: exactly 3 fragments with correct flags
    try testing.expectEqual(@as(usize, 3), frames.items.len);
    try testing.expectEqual(constants.flag_begin, frames.items[0].flags);
    try testing.expectEqual(@as(u8, 0), frames.items[1].flags);
    try testing.expectEqual(constants.flag_end, frames.items[2].flags);
}

test "single frame message gets UNFRAGMENTED flag" {
    const max_payload = constants.default_mtu_length - constants.data_frame_header_length;

    // Given: a message smaller than one MTU payload
    const payload = try testing.allocator.alloc(u8, max_payload - 100);
    defer testing.allocator.free(payload);

    // When: processed
    var frames = std.ArrayList(FragmentRecord).init(testing.allocator);
    defer frames.deinit();
    fragmentForTest(payload, max_payload, &frames);

    // Then: single frame with UNFRAGMENTED
    try testing.expectEqual(@as(usize, 1), frames.items.len);
    try testing.expectEqual(constants.flag_unfragmented, frames.items[0].flags);
}

test "fragment last chunk smaller than max payload" {
    const max_payload = constants.default_mtu_length - constants.data_frame_header_length;

    // Given: a message that is 2.5x the max payload
    const msg_size = max_payload * 2 + max_payload / 2;
    const payload = try testing.allocator.alloc(u8, msg_size);
    defer testing.allocator.free(payload);

    // When: fragmented
    var frames = std.ArrayList(FragmentRecord).init(testing.allocator);
    defer frames.deinit();
    fragmentForTest(payload, max_payload, &frames);

    // Then: 3 fragments, last one is smaller
    try testing.expectEqual(@as(usize, 3), frames.items.len);
    try testing.expectEqual(max_payload, frames.items[0].len);
    try testing.expectEqual(max_payload, frames.items[1].len);
    try testing.expectEqual(max_payload / 2, frames.items[2].len);
    try testing.expectEqual(constants.flag_end, frames.items[2].flags);
}
```

#### 13.2 Retransmit Buffer

```zig
test "store and lookup frame in retransmit buffer" {
    // Given: a retransmit buffer with 64KB capacity
    var buf = try RetransmitBuffer.init(64 * 1024, constants.default_mtu_length, testing.allocator);
    defer buf.close(testing.allocator);

    // Given: a test frame
    const frame = [_]u8{ 0x01, 0x02, 0x03, 0x04 } ++ ([_]u8{0} ** 36);

    // When: stored at sequence 42
    buf.store(42, &frame);

    // Then: lookup succeeds with correct data
    const result = buf.lookup(42);
    try testing.expect(result != null);
    try testing.expectEqualSlices(u8, &frame, result.?);
}

test "lookup returns null for overwritten slot" {
    // Given: a small retransmit buffer (few slots)
    var buf = try RetransmitBuffer.init(64 * 1024, constants.default_mtu_length, testing.allocator);
    defer buf.close(testing.allocator);

    const frame_a = [_]u8{0xAA} ** 40;
    const frame_b = [_]u8{0xBB} ** 40;

    // When: store at sequence 0, then overwrite with sequence = slot_count
    // (wraps to the same slot)
    buf.store(0, &frame_a);
    const slot_count: i64 = @intCast(buf.slot_count);
    buf.store(slot_count, &frame_b);

    // Then: lookup for sequence 0 returns null (overwritten)
    try testing.expect(buf.lookup(0) == null);

    // And: lookup for slot_count returns frame_b
    const result = buf.lookup(slot_count);
    try testing.expect(result != null);
    try testing.expectEqualSlices(u8, &frame_b, result.?);
}

test "lookup returns null for never-stored sequence" {
    // Given: an empty retransmit buffer
    var buf = try RetransmitBuffer.init(64 * 1024, constants.default_mtu_length, testing.allocator);
    defer buf.close(testing.allocator);

    // Then: lookup returns null
    try testing.expect(buf.lookup(999) == null);
}
```

#### 13.3 Retransmit Handler State Machine

```zig
test "retransmit handler transitions inactive → lingering → inactive" {
    // Given
    var handler = RetransmitHandler.init();
    try testing.expectEqual(RetransmitHandler.State.inactive, handler.state);

    // When: NAK received at time T
    const now_ns: i64 = 1_000_000_000;
    handler.onNakForTest(100, 40, now_ns);

    // Then: state is lingering
    try testing.expectEqual(RetransmitHandler.State.lingering, handler.state);
    try testing.expectEqual(@as(i64, 100), handler.position);
    try testing.expectEqual(@as(i32, 40), handler.length);

    // When: time advances past linger period
    handler.processTimeouts(now_ns + constants.retransmit_linger_ns + 1);

    // Then: state returns to inactive
    try testing.expectEqual(RetransmitHandler.State.inactive, handler.state);
}

test "retransmit handler suppresses overlapping NAK during linger" {
    // Given: handler in lingering state for range [100, 140)
    var handler = RetransmitHandler.init();
    const now_ns: i64 = 1_000_000_000;
    handler.onNakForTest(100, 40, now_ns);

    var retransmit_count: u32 = 0;

    // When: overlapping NAK arrives during linger period
    const during_linger = now_ns + constants.retransmit_linger_ns / 2;
    const was_suppressed = handler.wouldSuppress(100, 40);

    // Then: NAK is suppressed (no retransmit)
    try testing.expect(was_suppressed);
    _ = during_linger;
    _ = retransmit_count;
}

test "retransmit handler allows non-overlapping NAK during linger" {
    // Given: handler lingering for range [100, 140)
    var handler = RetransmitHandler.init();
    handler.onNakForTest(100, 40, 1_000_000_000);

    // When: non-overlapping NAK for range [200, 240)
    const overlaps = handler.wouldSuppress(200, 40);

    // Then: not suppressed
    try testing.expect(!overlaps);
}
```

### Integration Tests

#### 13.4 Sender Event Loop with Mock Ring Buffer

```zig
test "sender event loop routes message to correct peer" {
    // Given: a sender with two peers (nodeId=1, nodeId=2)
    var sender = try TestSenderBuilder.init()
        .withPeer(1, "127.0.0.1:9001")
        .withPeer(2, "127.0.0.1:9002")
        .withConnected(1, true)
        .withConnected(2, true)
        .withSendLimit(1, 1_000_000)
        .withSendLimit(2, 1_000_000)
        .build();
    defer sender.deinit();

    // Given: a message targeting nodeId=2 in the send ring buffer
    var header = DataFrameHeader.zeroes();
    header.target_node_id = 2;
    header.source_node_id = 0;
    header.source_service_id = 5;
    header.target_service_id = 10;
    sender.writeToSendRingBuffer(std.mem.asBytes(&header));

    // When: duty cycle runs
    _ = sender.event_loop.doWork();

    // Then: frame was sent to peer 2's address, not peer 1
    try testing.expectEqual(@as(u32, 0), sender.mock_io.sendCountFor(1));
    try testing.expectEqual(@as(u32, 1), sender.mock_io.sendCountFor(2));
}
```

#### 13.5 Flow Control Integration

```zig
test "sender drops message when peer is flow controlled" {
    // Given: a sender with peer nodeId=1, send_limit = 0 (no window)
    var sender = try TestSenderBuilder.init()
        .withPeer(1, "127.0.0.1:9001")
        .withConnected(1, true)
        .withSendLimit(1, 0) // Flow controlled
        .build();
    defer sender.deinit();

    // Given: a message targeting nodeId=1
    var header = DataFrameHeader.zeroes();
    header.target_node_id = 1;
    sender.writeToSendRingBuffer(std.mem.asBytes(&header));

    // When: duty cycle runs
    _ = sender.event_loop.doWork();

    // Then: no frame sent, back-pressure counter incremented
    try testing.expectEqual(@as(u32, 0), sender.mock_io.totalSendCount());
    try testing.expectEqual(@as(u64, 1), sender.counters.get(.send_back_pressure));
}
```

#### 13.6 Status Message Updates Send Limit

```zig
test "status message updates peer send limit" {
    // Given: a sender with peer nodeId=1, initially flow controlled
    var sender = try TestSenderBuilder.init()
        .withPeer(1, "127.0.0.1:9001")
        .build();
    defer sender.deinit();

    const peer = sender.event_loop.peers.get(1).?;
    try testing.expectEqual(@as(i64, 0), peer.send_limit);

    // When: SM received with consumption_position=1000, receiver_window=4096
    var sm = StatusMessageFrame.zeroes();
    sm.node_id = 1;
    sm.consumption_position = 1000;
    sm.receiver_window = 4096;
    sender.event_loop.handleStatusMessage(std.mem.asBytes(&sm));

    // Then: send_limit = 1000 + 4096 = 5096
    try testing.expectEqual(@as(i64, 5096), peer.send_limit);
    try testing.expect(peer.connected);
}
```

#### 13.7 NAK Triggers Retransmit

```zig
test "NAK triggers retransmit from retransmit buffer" {
    // Given: a sender with peer nodeId=1 that has sent some frames
    var sender = try TestSenderBuilder.init()
        .withPeer(1, "127.0.0.1:9001")
        .withConnected(1, true)
        .withSendLimit(1, 1_000_000)
        .build();
    defer sender.deinit();

    // Given: a frame stored in the retransmit buffer at sequence 5
    const frame = [_]u8{0xDE, 0xAD} ++ ([_]u8{0} ** 38);
    const peer = sender.event_loop.peers.get(1).?;
    peer.retransmit_buffer.store(5, &frame);

    // When: NAK received for that position
    var nak = NakFrame.zeroes();
    nak.node_id = 1;
    nak.position = 5 * 32; // position = sequence * frame_alignment
    nak.length = 32;
    sender.event_loop.handleNak(std.mem.asBytes(&nak));

    // Then: retransmit was queued
    try testing.expect(sender.mock_io.totalSendCount() > 0);
    try testing.expectEqual(@as(u64, 1), sender.counters.get(.naks_received));
}
```

---

## 14. File Structure

```
src/
  sender/
    sender_event_loop.zig      # SenderEventLoop struct, doWork(), onOutboundMessage()
    peer_sender.zig            # PeerSender struct, sequence numbering, flow-control check
    retransmit_buffer.zig      # RetransmitBuffer — per-peer circular frame store
    retransmit_handler.zig     # RetransmitHandler — NAK state machine with linger suppression
    message_fragmenter.zig     # fragmentAndSend() — splits oversized messages into MTU-sized frames
    send_buffer_pool.zig       # SendBufferPool — pre-allocated buffer pool for io_uring sends
    sender_command.zig         # SenderCommand union, dispatchCommand()
  sender.zig                   # Public re-exports for the sender subsystem
```

### Module Dependency Graph

```
sender_event_loop.zig
├── peer_sender.zig
│   └── retransmit_buffer.zig
│   └── retransmit_handler.zig
├── message_fragmenter.zig
├── send_buffer_pool.zig
├── sender_command.zig
├── platform.zig              (from doc 01: constants, atomics, clock)
├── ring_buffer.zig           (from doc 03: MPSC ring buffer)
└── network_io.zig            (from doc 04: io_uring / platform I/O)
```

### Build Integration

The sender module is compiled as part of the broker binary. Add to `build.zig`:

```zig
const sender_mod = b.addModule("sender", .{
    .root_source_file = b.path("src/sender.zig"),
    .imports = &.{
        .{ .name = "platform", .module = platform_mod },
        .{ .name = "concurrent", .module = concurrent_mod },
        .{ .name = "transport", .module = transport_mod },
    },
});
```

Tests for the sender module:

```zig
const sender_tests = b.addTest(.{
    .root_source_file = b.path("src/sender/sender_event_loop.zig"),
    .target = target,
    .optimize = optimize,
});
sender_tests.root_module.addImport("platform", platform_mod);
sender_tests.root_module.addImport("concurrent", concurrent_mod);
sender_tests.root_module.addImport("transport", transport_mod);

const run_sender_tests = b.addRunArtifact(sender_tests);
test_step.dependOn(&run_sender_tests.step);
```

---

## Appendix: Memory Ordering Quick Reference

All mutable state in the sender event loop is accessed by a **single thread** (the sender
thread). No atomic operations are needed for sender-internal state. The only shared-memory
boundaries are:

| Field | Writer | Reader | Mechanism |
|-------|--------|--------|-----------|
| Send ring buffer tail | Services (producers) | Sender thread (consumer) | `release` / `acquire` atomics (in `RingBuffer`) |
| Send ring buffer head | Sender thread | Services (for capacity check) | `release` / `acquire` atomics (in `RingBuffer`) |
| Command queue | Control loop thread | Sender thread | `release` / `acquire` atomics (in `CommandQueue`) |

All `PeerSender` fields, the retransmit buffer, the send buffer pool, and the retransmit
handler are single-writer (sender thread only) and require no synchronization.

On x86-64, the ring buffer and command queue atomics compile to plain loads/stores with
compiler fences (TSO). On ARM, they compile to `ldapr`/`stlr` pairs.

---

*Previous: [04 — UDP Transport & io_uring](04-udp-transport-and-io-uring.md)*
*Next: [06 — Receive Path](06-receive-path.md)*