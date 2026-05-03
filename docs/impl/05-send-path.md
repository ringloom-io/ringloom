# 05 — Send Path (TCP)

> **Depends on:** [03 — Concurrent Data Structures](03-concurrent-data-structures.md) (MPSC ring buffer),
> [04 — TCP Transport Library](04-tcp-transport-library.md) (`FrameHeader`, handshake frame, `ringloom_tcp` library)
>
> **Depended on by:** [06 — Receive Path](06-receive-path.md) (coordinated connection management),
> [08 — Service IPC](08-service-ipc.md) (send ring buffer integration)

This document covers the **TCP sender path** — the subsystem that drains outbound
messages from the send ring buffer, prepends a 24-byte length-prefixed frame header,
enqueues them in per-peer write queues, and writes them to outgoing TCP connections
via `ringloom_tcp` (io_uring on Linux, kqueue on macOS).

The TCP send path is significantly simpler than the previous UDP design. TCP provides
reliable, ordered byte-stream delivery, so there is no retransmit buffer, no message
fragmentation, no NAK handling, and no Status Messages. The sender writes complete
frames to the TCP stream and relies on the kernel for segmentation, retransmission,
and flow control.

**Delivery semantics:** best-effort across disconnects. If a TCP connection drops,
in-flight messages in the kernel buffer and the per-peer write queue are lost. There
is no application-level replay. Services use correlation IDs and timeouts to detect
missing responses.

All code targets **Zig 0.14.x** stable.

---

## Table of Contents

1.  [Overview](#1-overview)
2.  [Send Ring Buffer](#2-send-ring-buffer)
3.  [Sender Event Loop Duty Cycle](#3-sender-event-loop-duty-cycle)
4.  [Message Routing](#4-message-routing)
5.  [Per-Peer Write Queue](#5-per-peer-write-queue)
6.  [TCP Write Mechanics](#6-tcp-write-mechanics)
7.  [Frame Construction](#7-frame-construction)
8.  [Heartbeat Sending](#8-heartbeat-sending)
9.  [Connection Management](#9-connection-management)
10. [Fairness and Budgets](#10-fairness-and-budgets)
11. [Counters & Observability](#11-counters--observability)
12. [Configuration Parameters](#12-configuration-parameters)
13. [Error Handling](#13-error-handling)
14. [Testing](#14-testing)

---

## 1. Overview

```
Services (writers) ──┐
                     │  CAS on tail
Services (writers) ──┼──► Send Ring Buffer ──► Sender Event Loop ──► Per-Peer ──► TCP ──► Peer
                     │    (MPSC, shared mem)    (single consumer)     Write Queue   Write   Brokers
Services (writers) ──┘
```

Cross-host messages — those where `target_node_id != local_node_id` — follow this path.
Multiple local services write into a single shared-memory MPSC ring buffer located in the
broker's metadata file. A dedicated **sender thread** is the sole consumer. It reads
messages, looks up the destination peer by `target_node_id`, prepends a 24-byte frame
header, and enqueues the frame into the peer's write queue. The write queue is drained
to the TCP connection via io_uring (Linux) or kqueue (macOS) through the `ringloom_tcp` library.

### What Changed from UDP

| Concern | UDP (old) | TCP (new) |
|---------|-----------|-----------|
| Reliability | Retransmit buffer + NAK handling | TCP handles retransmission |
| Ordering | Sequence numbers + receive log | TCP provides ordered delivery |
| Fragmentation | Sender splits at MTU boundary | TCP handles segmentation |
| Flow control | Status Messages + send_limit | Per-peer write queue + TCP backpressure |
| Connection | Stateless UDP + SETUP frame | Persistent TCP connection + handshake |
| Heartbeat | Zero-length DATA frame at 100 ms | Minimal frame at 500 ms |
| Frame header | 40 bytes (data frame header) | 24 bytes (length-prefixed) |

### Data Flow Summary

| Step | Actor | Operation |
|------|-------|-----------|
| 1 | Service | `ring_buffer.write(record_header + frame_header + payload)` — CAS on tail |
| 2 | Sender event loop | `ring_buffer.read(on_outbound_message, SEND_BATCH_SIZE)` |
| 3 | Sender event loop | Read `target_node_id` from frame header, look up `PeerConnection` |
| 4 | Sender event loop | If peer CONNECTED → enqueue frame into peer's write queue |
| 5 | Sender event loop | If peer not CONNECTED → drop message, increment counter |
| 6 | Sender event loop | For each peer with queued data, submit TCP writes (up to `WRITE_BUDGET_PER_PEER`) |
| 7 | `ringloom_tcp` | `submit_write` / `submit_writev` → io_uring SQE or kqueue kevent |
| 8 | Kernel | TCP sends data to remote peer |

---

## 2. Send Ring Buffer

The send ring buffer is the standard MPSC ring buffer from [doc 03](03-concurrent-data-structures.md),
located in the broker's metadata file at offset `metadata_header_length + control_buffer_length +
ring_buffer_trailer_length`. Its capacity defaults to `send_ring_buffer_size` (2 MB).

### 2.1 Who Writes

Any local service that produces a message with `target_node_id != local_node_id`. The service
writes the message into the send ring buffer via CAS on the tail position. Multiple services
may write concurrently (the MPSC ring buffer handles contention).

Each record in the send ring buffer has the following layout:

```
┌────────────────────────────────────────────────────────┐
│  Record Header (8 bytes)                               │
│  +0: record_length  (i32)  ← total payload length      │
│  +4: msg_type_id    (i32)  ← message type tag           │
├────────────────────────────────────────────────────────┤
│  Frame Header (24 bytes) — written by the service       │
│  +0:  frame_length       (u32)                          │
│  +4:  flags              (u8)                           │
│  +5:  source_node_id     (u8)                           │
│  +6:  target_node_id     (u8)                           │
│  +7:  reserved           (u8)                           │
│  +8:  source_service_id  (u16)                          │
│  +10: target_service_id  (u16)                          │
│  +12: template_id        (u16)                          │
│  +14: reserved           (u16)                          │
│  +16: correlation_id     (i64)                          │
├────────────────────────────────────────────────────────┤
│  Application Payload (variable length)                  │
├────────────────────────────────────────────────────────┤
│  Padding (0–7 bytes to align to 8-byte boundary)        │
└────────────────────────────────────────────────────────┘
```

The frame header fields are populated by the service at write time. The sender event loop
reads them without modification — it does not need to construct the frame header from
scratch, only verify and forward it.

### 2.2 Who Reads

Exactly one thread: the sender event loop. This is the single-consumer side of the MPSC
contract. No locking is needed on the read path.

### 2.3 Committed vs. Uncommitted Records

A record becomes visible to the consumer only after the producer commits it. The commit
protocol uses release/acquire semantics on the record header's length field:

1. **Producer** writes payload and frame header into the ring buffer.
2. **Producer** writes `record_length` with `release` ordering (commit).
3. **Consumer** reads `record_length` with `acquire` ordering.
4. If `record_length > 0`, the record is committed and readable.
5. If `record_length == 0`, the record is not yet committed (producer still writing).

### 2.4 Back-Pressure at the Source

When the send ring buffer is full, the service's `write()` call returns
`error.InsufficientCapacity`. This is the first back-pressure signal: it tells the service
that the outbound path to remote hosts is saturated. The service should propagate this as
a `BufferFull` error to the application layer.

---

## 3. Sender Event Loop Duty Cycle

The sender event loop follows the standard duty-cycle pattern described in
[doc 00](00-overview.md). Each iteration returns a work count that drives the idle
strategy. If no work was done across all phases, the idle strategy decides whether to
spin, yield, or park.

### 3.1 State

**File: `src/sender/sender_event_loop.zig`**

```zig
// src/sender/sender_event_loop.zig

const std = @import("std");
const platform = @import("../platform.zig");
const constants = @import("../platform/constants.zig");
const Clock = platform.Clock;
const RingBuffer = @import("../concurrent/ring_buffer.zig").RingBuffer;
const CommandQueue = @import("../concurrent/command_queue.zig").CommandQueue;
const CountersManager = @import("../concurrent/counters.zig").CountersManager;
const TcpIo = @import("../transport/ringloom_tcp.zig").TcpIo;
const FrameHeader = @import("../protocol/frames.zig").FrameHeader;

pub const SenderEventLoop = struct {
    /// The MPSC ring buffer that local services write cross-host messages into.
    send_ring_buffer: *RingBuffer,

    /// Per-peer connection state, indexed by node_id (0..max_peers).
    /// Direct array index — not a hash map — for O(1) lookup on the hot path.
    peers: [constants.max_peers]?*PeerConnection,

    /// Count of currently connected peers. Used for iteration bounds.
    connected_peer_count: u8,

    /// TCP I/O backend: io_uring on Linux, kqueue on macOS.
    tcp_io: *TcpIo,

    /// MPSC command queue from the control loop (add/remove peer, config changes).
    cmd_queue: *CommandQueue,

    /// Shared counters for observability.
    counters: *CountersManager,

    /// This broker's node ID — stamped into outgoing frame headers.
    local_node_id: u8,

    /// Monotonic timestamp (ns) of the last heartbeat round.
    last_heartbeat_ns: i64,

    /// Whether this event loop is running. Set to false by the shutdown path.
    running: platform.AtomicBool,

    const Self = @This();

    pub fn init(
        send_ring_buffer: *RingBuffer,
        tcp_io: *TcpIo,
        cmd_queue: *CommandQueue,
        counters: *CountersManager,
        local_node_id: u8,
    ) Self {
        return .{
            .send_ring_buffer = send_ring_buffer,
            .peers = [_]?*PeerConnection{null} ** constants.max_peers,
            .connected_peer_count = 0,
            .tcp_io = tcp_io,
            .cmd_queue = cmd_queue,
            .counters = counters,
            .local_node_id = local_node_id,
            .last_heartbeat_ns = 0,
            .running = platform.AtomicBool.init(true),
        };
    }
};
```

### 3.2 Duty Cycle

One iteration of the sender loop:

```zig
/// Called by the ThreadRunner on every iteration.
/// Returns the total number of items processed (work count).
pub fn doWork(self: *SenderEventLoop) u32 {
    var work_count: u32 = 0;
    const now_ns = Clock.monotonicNanos();

    // ── Phase 1: Drain command queue ─────────────────────────────────────
    // Process commands from the control loop: add/remove peers,
    // configuration changes. Must happen first so that peer state is
    // up-to-date before we try to route messages.
    work_count += self.cmd_queue.drain(
        dispatchCommand,
        @ptrCast(self),
        constants.command_drain_limit,
    );

    // ── Phase 2: Process write completions ───────────────────────────────
    // Drain io_uring CQEs / kqueue kevents for completed TCP writes.
    // Advances the write position for each connection, freeing queue
    // slots for new frames.
    work_count += self.tcp_io.pollCompletions(
        onWriteComplete,
        @ptrCast(self),
        constants.completion_batch_limit,
    );

    // ── Phase 3: Read messages from send ring buffer ─────────────────────
    // Read up to SEND_BATCH_SIZE records from the ring buffer.
    // Each message is routed to the correct peer's write queue.
    work_count += self.send_ring_buffer.read(
        onOutboundMessage,
        @ptrCast(self),
        constants.send_batch_size,
    );

    // ── Phase 4: Submit TCP writes for each peer ─────────────────────────
    // For each connected peer with queued data, submit TCP writes
    // up to WRITE_BUDGET_PER_PEER frames.
    work_count += self.drainWriteQueues();

    // ── Phase 5: Send heartbeats ─────────────────────────────────────────
    // Minimal frames to peers that haven't received data recently.
    if (now_ns - self.last_heartbeat_ns >= constants.heartbeat_interval_ns) {
        self.sendHeartbeats(now_ns);
        self.last_heartbeat_ns = now_ns;
    }

    // ── Phase 6: Process reconnection timers ─────────────────────────────
    // For disconnected peers, check if the backoff timer has expired
    // and attempt reconnection.
    self.processReconnections(now_ns);

    return work_count;
}
```

### 3.3 Phase Ordering Rationale

| Phase | Why this order |
|-------|---------------|
| 1. Commands | Peer additions/removals must be visible before routing messages |
| 2. Write completions | Free write queue slots so phase 4 has capacity |
| 3. Ring buffer drain | Core work: read messages, route to per-peer queues |
| 4. Submit TCP writes | Flush queued frames to the network |
| 5. Heartbeats | Low priority, only every 500 ms |
| 6. Reconnections | State machine housekeeping, may initiate I/O |

---

## 4. Message Routing

When the sender reads a record from the send ring buffer, it extracts the routing header,
looks up the peer, and either enqueues or drops the message.

### 4.1 Routing Logic

```zig
// src/sender/sender_event_loop.zig

/// Ring buffer read callback. Called once per record by RingBuffer.read().
fn onOutboundMessage(context: *anyopaque, msg_type_id: i32, payload: []const u8) void {
    const self: *SenderEventLoop = @ptrCast(@alignCast(context));
    _ = msg_type_id;

    // Payload must contain at least a complete frame header.
    if (payload.len < @sizeOf(FrameHeader)) {
        self.counters.increment(.send_ring_reads);
        self.counters.increment(.malformed_messages_dropped);
        return;
    }

    const header: *const FrameHeader = @ptrCast(@alignCast(payload.ptr));
    const target_node_id = header.target_node_id;

    self.counters.increment(.send_ring_reads);

    // ── Self-routing for admin messages ──────────────────────────────────
    // Admin messages (flags & ADMIN != 0) may target the local node for
    // cluster operations like configuration distribution.
    if (target_node_id == self.local_node_id) {
        if (header.flags & constants.flag_admin != 0) {
            self.handleAdminMessage(payload);
            return;
        }
        // Non-admin self-targeted message — programming error, drop it.
        self.counters.increment(.malformed_messages_dropped);
        return;
    }

    // ── Look up peer by node_id ──────────────────────────────────────────
    // Direct array index for O(1) lookup. node_id is a u8 and the array
    // is sized to max_peers, so bounds are guaranteed at init time.
    if (target_node_id >= constants.max_peers) {
        self.counters.increment(.peer_not_connected_drops);
        return;
    }

    const peer = self.peers[target_node_id] orelse {
        self.counters.increment(.peer_not_connected_drops);
        return;
    };

    // ── Check connection state ───────────────────────────────────────────
    if (peer.state != .connected) {
        self.counters.increment(.peer_not_connected_drops);
        return;
    }

    // ── Enqueue into peer's write queue ──────────────────────────────────
    peer.write_queue.enqueue(payload) catch {
        // Queue overflow — drop the oldest message to make room.
        _ = peer.write_queue.dropOldest();
        self.counters.incrementFor(peer.node_id, .peer_queue_overflow_drops);
        // Retry enqueue after dropping — guaranteed to succeed since we
        // just freed a slot.
        peer.write_queue.enqueue(payload) catch unreachable;
    };
}
```

### 4.2 Why Array Index, Not Hash Map

The peer table is a fixed-size array indexed by `node_id` (a `u8`, max 256 entries).
This gives O(1) lookup with no hashing overhead on the hot path. The trade-off is
wasted memory for sparse node ID spaces, but with `max_peers` defaulting to 16, the
array is only 16 × `@sizeOf(?*PeerConnection)` = 128 bytes.

### 4.3 Drop Semantics

When a message is dropped (peer unknown, peer disconnected, or queue overflow), the
data is lost. This is by design — the send ring buffer is a best-effort outbound queue.
The application layer uses request-response correlation IDs and timeouts to detect lost
messages. Counters provide observability into drop reasons.

---

## 5. Per-Peer Write Queue

Each peer has a bounded outbound queue that decouples message routing from TCP writes.
This is the key design element that prevents one slow or congested peer from blocking
writes to other peers.

### 5.1 Design

```
Peer A (fast)       ┌──────────┐     TCP write
  write_queue ─────►│ ████░░░░ │────────────────► Peer A socket
                    └──────────┘     (drains quickly)

Peer B (slow)       ┌──────────┐     TCP write
  write_queue ─────►│ ████████ │───── blocked ──► Peer B socket
                    └──────────┘     (EAGAIN / partial write)

Peer C (fast)       ┌──────────┐     TCP write
  write_queue ─────►│ ██░░░░░░ │────────────────► Peer C socket
                    └──────────┘     (drains quickly)

Each peer has its own queue. Peer B being slow does NOT block Peers A and C.
```

### 5.2 Queue Implementation

The write queue is a bounded ring buffer of serialized frame data (header + payload bytes).
Each entry stores a complete frame ready for TCP transmission.

```zig
// src/sender/write_queue.zig

const std = @import("std");
const constants = @import("../platform/constants.zig");
const FrameHeader = @import("../protocol/frames.zig").FrameHeader;

pub const WriteQueue = struct {
    /// Backing storage for frame data. Each slot holds a serialized frame
    /// (24-byte header + payload). Slot size is fixed at max_frame_size.
    slots: []align(8) u8,

    /// Array of frame lengths for each slot. 0 = empty.
    lengths: []u32,

    /// Ring buffer indices.
    head: u32,        // Next slot to dequeue (oldest frame)
    tail: u32,        // Next slot to enqueue (newest frame)
    count: u32,       // Number of frames currently in the queue
    capacity: u32,    // Total number of slots
    mask: u32,        // capacity - 1 (capacity must be power of two)

    /// Maximum frame size (header + payload) per slot.
    max_frame_size: u32,

    const Self = @This();

    pub fn init(capacity: u32, max_frame_size: u32, allocator: std.mem.Allocator) !Self {
        std.debug.assert(std.math.isPowerOfTwo(capacity));

        const slots = try allocator.alignedAlloc(
            u8,
            8,
            @as(usize, capacity) * @as(usize, max_frame_size),
        );
        @memset(slots, 0);

        const lengths = try allocator.alloc(u32, capacity);
        @memset(lengths, 0);

        return .{
            .slots = slots,
            .lengths = lengths,
            .head = 0,
            .tail = 0,
            .count = 0,
            .capacity = capacity,
            .mask = capacity - 1,
            .max_frame_size = max_frame_size,
        };
    }

    /// Enqueue a complete frame (header + payload) into the write queue.
    /// Returns error.Overflow if the queue is full.
    pub fn enqueue(self: *Self, frame_data: []const u8) !void {
        if (self.count >= self.capacity) return error.Overflow;
        if (frame_data.len > self.max_frame_size) return error.FrameTooLarge;

        const slot_index = self.tail & self.mask;
        const offset = @as(usize, slot_index) * @as(usize, self.max_frame_size);

        @memcpy(self.slots[offset..][0..frame_data.len], frame_data);
        self.lengths[slot_index] = @intCast(frame_data.len);

        self.tail +%= 1;
        self.count += 1;
    }

    /// Drop the oldest enqueued message to make room.
    /// Returns the length of the dropped frame.
    pub fn dropOldest(self: *Self) u32 {
        if (self.count == 0) return 0;

        const slot_index = self.head & self.mask;
        const dropped_len = self.lengths[slot_index];
        self.lengths[slot_index] = 0;

        self.head +%= 1;
        self.count -= 1;

        return dropped_len;
    }

    /// Peek at the next frame to send without removing it.
    /// Returns null if the queue is empty.
    pub fn peek(self: *const Self) ?[]const u8 {
        if (self.count == 0) return null;

        const slot_index = self.head & self.mask;
        const len = self.lengths[slot_index];
        const offset = @as(usize, slot_index) * @as(usize, self.max_frame_size);

        return self.slots[offset..][0..len];
    }

    /// Dequeue the oldest frame after it has been successfully written.
    pub fn dequeue(self: *Self) void {
        if (self.count == 0) return;

        const slot_index = self.head & self.mask;
        self.lengths[slot_index] = 0;

        self.head +%= 1;
        self.count -= 1;
    }

    /// Return the number of contiguous frames available for vectored write.
    /// Up to `limit` frames, stopping at the ring buffer wrap boundary.
    pub fn contiguousCount(self: *const Self, limit: u32) u32 {
        if (self.count == 0) return 0;

        const start = self.head & self.mask;
        const until_wrap = self.capacity - start;
        return @min(@min(self.count, until_wrap), limit);
    }

    pub fn isEmpty(self: *const Self) bool {
        return self.count == 0;
    }

    pub fn isFull(self: *const Self) bool {
        return self.count >= self.capacity;
    }

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        allocator.free(self.slots);
        allocator.free(self.lengths);
    }
};
```

### 5.3 Overflow Strategy

When the write queue is full:

1. **Drop the oldest enqueued message** (`dropOldest()`).
2. **Increment `peer_queue_overflow_drops`** counter for the peer.
3. **Enqueue the new message** into the freed slot.

This strategy prioritizes recency — newer messages are more likely to be relevant than
older ones stuck behind a slow connection. The sender loop never blocks on a full queue;
it always makes progress.

**Alternative considered:** Dropping the *newest* message (i.e., rejecting the enqueue).
Rejected because it would cause the sender loop to discard messages in FIFO order from
the ring buffer, effectively starving a peer that recovers from congestion.

---

## 6. TCP Write Mechanics

The sender submits TCP writes through the `ringloom_tcp` library, which abstracts io_uring
(Linux) and kqueue (macOS).

### 6.1 Draining Write Queues

For each peer with queued data, the sender submits writes up to `WRITE_BUDGET_PER_PEER`:

```zig
// src/sender/sender_event_loop.zig

/// Drain write queues for all connected peers, submitting TCP writes
/// up to the per-peer budget.
fn drainWriteQueues(self: *SenderEventLoop) u32 {
    var total_writes: u32 = 0;

    for (&self.peers) |maybe_peer| {
        const peer = maybe_peer orelse continue;
        if (peer.state != .connected) continue;
        if (peer.write_queue.isEmpty()) continue;

        // Check if peer has a pending partial write from a previous cycle.
        if (peer.write_blocked) continue;

        var budget: u32 = constants.write_budget_per_peer;

        // ── Vectored write (coalesce multiple frames) ────────────────────
        // If multiple frames are queued contiguously, use writev to send
        // them in a single syscall.
        const contiguous = peer.write_queue.contiguousCount(budget);
        if (contiguous > 1) {
            total_writes += self.submitWritev(peer, contiguous);
            budget -|= contiguous;
        }

        // ── Single-frame writes for remaining budget ─────────────────────
        while (budget > 0) {
            const frame = peer.write_queue.peek() orelse break;
            self.submitWrite(peer, frame);
            total_writes += 1;
            budget -= 1;
        }
    }

    return total_writes;
}
```

### 6.2 Single Frame Write

```zig
/// Submit a single TCP write for one frame.
fn submitWrite(self: *SenderEventLoop, peer: *PeerConnection, frame: []const u8) void {
    self.tcp_io.submitWrite(
        peer.socket_fd,
        frame,
        @intFromPtr(peer),  // user_data for completion callback
    ) catch |err| {
        self.handleWriteError(peer, err);
    };

    peer.last_send_ns = Clock.monotonicNanos();
}
```

### 6.3 Vectored Write (writev)

When multiple frames are contiguous in the write queue, the sender coalesces them into
a single `writev` syscall to reduce system call overhead:

```zig
/// Submit a vectored TCP write coalescing multiple contiguous frames.
fn submitWritev(self: *SenderEventLoop, peer: *PeerConnection, count: u32) u32 {
    // Build iovec array from contiguous queue slots.
    var iovecs: [constants.write_budget_per_peer]std.posix.iovec_const = undefined;
    var total_bytes: usize = 0;

    var slot = peer.write_queue.head;
    for (0..count) |i| {
        const slot_index = slot & peer.write_queue.mask;
        const len = peer.write_queue.lengths[slot_index];
        const offset = @as(usize, slot_index) * @as(usize, peer.write_queue.max_frame_size);

        iovecs[i] = .{
            .base = peer.write_queue.slots[offset..].ptr,
            .len = len,
        };
        total_bytes += len;
        slot +%= 1;
    }

    self.tcp_io.submitWritev(
        peer.socket_fd,
        iovecs[0..count],
        @intFromPtr(peer),
    ) catch |err| {
        self.handleWriteError(peer, err);
        return 0;
    };

    peer.last_send_ns = Clock.monotonicNanos();
    return count;
}
```

### 6.4 Handling Partial Writes

TCP may accept fewer bytes than submitted. When this happens:

```zig
/// Called by TcpIo.pollCompletions() for each completed write.
fn onWriteComplete(context: *anyopaque, user_data: u64, result: i32) void {
    const self: *SenderEventLoop = @ptrCast(@alignCast(context));
    const peer: *PeerConnection = @ptrFromInt(user_data);

    if (result < 0) {
        // Write error — connection broken.
        self.handleWriteError(peer, error.BrokenPipe);
        return;
    }

    const bytes_written: usize = @intCast(result);
    self.counters.incrementFor(peer.node_id, .bytes_sent, bytes_written);
    self.counters.increment(.write_completions_processed);

    // ── Advance through completed frames ─────────────────────────────────
    // Dequeue frames whose bytes have been fully written.
    var remaining = bytes_written;
    while (remaining > 0) {
        const frame = peer.write_queue.peek() orelse break;
        if (remaining >= frame.len) {
            remaining -= frame.len;
            peer.write_queue.dequeue();
            self.counters.incrementFor(peer.node_id, .frames_sent);
        } else {
            // Partial write — track the write position within this frame
            // and retry the remainder on the next cycle.
            peer.partial_write_offset = frame.len - remaining;
            peer.write_blocked = true;
            remaining = 0;
        }
    }

    // If we finished all pending data, clear the write-blocked flag.
    if (peer.write_queue.isEmpty()) {
        peer.write_blocked = false;
        peer.partial_write_offset = 0;
    }
}
```

### 6.5 Platform-Specific I/O

| Platform | io_uring | kqueue |
|----------|----------|--------|
| Single write | `IORING_OP_SEND` with buffer pointer | `write()` triggered by `EVFILT_WRITE` |
| Vectored write | `IORING_OP_WRITEV` with iovec array | `writev()` triggered by `EVFILT_WRITE` |
| SQE linking | Chain related writes with `IOSQE_IO_LINK` | N/A (sequential writes) |
| Write readiness | Always submit; CQE reports result | Register `EVFILT_WRITE` only when data pending (edge-triggered) |
| Batching | Multiple SQEs → single `io_uring_enter` | Multiple kevents → single `kevent()` call |

---

## 7. Frame Construction

### 7.1 Wire Format

The RingLoom wire protocol uses a 24-byte length-prefixed frame header. All fields are
**little-endian**.

```
Offset  Size  Type   Field
──────────────────────────────────────────────────────────
0       4     u32    frame_length          ← total frame size (header + payload)
4       1     u8     flags                 ← ADMIN, HEARTBEAT, etc.
5       1     u8     source_node_id        ← originating broker
6       1     u8     target_node_id        ← destination broker
7       1     u8     reserved              ← must be 0
8       2     u16    source_service_id     ← originating service
10      2     u16    target_service_id     ← destination service
12      2     u16    template_id           ← message type (SBE template)
14      2     u16    reserved              ← must be 0
16      8     i64    correlation_id        ← request-response matching
```

### 7.2 Packed Struct

```zig
// src/protocol/frames.zig

const std = @import("std");

/// 24-byte frame header for the RingLoom TCP wire protocol.
/// All fields little-endian. Used for both data frames and heartbeats.
///
/// The packed struct allows zero-copy overlay on wire bytes:
///   const header: *const FrameHeader = @ptrCast(@alignCast(bytes.ptr));
pub const FrameHeader = packed struct {
    frame_length: u32,
    flags: u8,
    source_node_id: u8,
    target_node_id: u8,
    reserved_0: u8 = 0,
    source_service_id: u16,
    target_service_id: u16,
    template_id: u16,
    reserved_1: u16 = 0,
    correlation_id: i64,

    comptime {
        std.debug.assert(@sizeOf(FrameHeader) == 24);
    }

    pub const HEADER_SIZE: u32 = 24;

    /// Overlay a FrameHeader on a byte slice for zero-copy reads.
    pub fn overlay(bytes: []const u8) *const FrameHeader {
        std.debug.assert(bytes.len >= HEADER_SIZE);
        return @ptrCast(@alignCast(bytes.ptr));
    }

    /// Overlay a mutable FrameHeader on a byte slice for zero-copy writes.
    pub fn overlayMut(bytes: []u8) *FrameHeader {
        std.debug.assert(bytes.len >= HEADER_SIZE);
        return @ptrCast(@alignCast(bytes.ptr));
    }

    /// Serialize this header into the first 24 bytes of a buffer.
    pub fn writeTo(self: FrameHeader, buf: []u8) void {
        std.debug.assert(buf.len >= HEADER_SIZE);
        const dest: *FrameHeader = @ptrCast(@alignCast(buf.ptr));
        dest.* = self;
    }

    /// Create a heartbeat frame header.
    pub fn heartbeat(source_node_id: u8, target_node_id: u8) FrameHeader {
        return .{
            .frame_length = HEADER_SIZE,
            .flags = constants.flag_heartbeat,
            .source_node_id = source_node_id,
            .target_node_id = target_node_id,
            .reserved_0 = 0,
            .source_service_id = 0,
            .target_service_id = 0,
            .template_id = constants.heartbeat_template_id,
            .reserved_1 = 0,
            .correlation_id = 0,
        };
    }
};
```

### 7.3 Frame Construction in the Send Path

When the sender reads a message from the ring buffer, the frame header is already
populated by the service. The sender validates and forwards it:

```zig
/// Build a complete frame for TCP transmission.
/// The ring buffer payload already contains the 24-byte frame header
/// followed by the application payload. The sender verifies the header
/// and copies the full frame into the peer's write queue.
fn buildFrame(self: *SenderEventLoop, payload: []const u8) ?[]const u8 {
    if (payload.len < FrameHeader.HEADER_SIZE) return null;

    const header = FrameHeader.overlay(payload);

    // Validate frame_length matches the payload.
    // frame_length includes the header, so it should equal payload.len.
    if (header.frame_length != @as(u32, @intCast(payload.len))) {
        self.counters.increment(.malformed_messages_dropped);
        return null;
    }

    // Stamp the source_node_id from broker config. The service may have
    // written it already, but the sender is authoritative.
    const mutable_header = FrameHeader.overlayMut(@constCast(payload));
    mutable_header.source_node_id = self.local_node_id;

    return payload;
}
```

**Design choice: services write the frame header.** The alternative — having the sender
construct the header from metadata fields — would require copying metadata separately
and assembling it in the sender. By having services write the header directly into the
ring buffer, the sender only needs to verify and forward it. This reduces per-message
work on the sender thread, which is a single-threaded bottleneck.

---

## 8. Heartbeat Sending

If no data has been sent to a peer within `HEARTBEAT_INTERVAL` (500 ms), the sender
sends a heartbeat frame. Heartbeats serve as application-layer TCP keepalives, allowing
the remote receiver to detect liveness without relying on TCP keepalive timers (which
typically have much longer intervals).

### 8.1 Heartbeat Frame

A heartbeat is a minimal frame: 24 bytes (header only, zero payload).

```
┌──────────────────────────────────────────────────────────────┐
│  FrameHeader (24 bytes)                                      │
│                                                              │
│  frame_length     = 24 (header only, no payload)             │
│  flags            = HEARTBEAT                                │
│  source_node_id   = local broker's node ID                   │
│  target_node_id   = peer's node ID                           │
│  template_id      = HEARTBEAT_TEMPLATE (0xFFFF)              │
│  correlation_id   = 0                                        │
│  all other fields = 0                                        │
└──────────────────────────────────────────────────────────────┘
```

### 8.2 Implementation

```zig
// src/sender/sender_event_loop.zig

/// Send heartbeat frames to peers that haven't received data recently.
fn sendHeartbeats(self: *SenderEventLoop, now_ns: i64) void {
    for (&self.peers) |maybe_peer| {
        const peer = maybe_peer orelse continue;
        if (peer.state != .connected) continue;

        // Only send heartbeat if no data was sent within the interval.
        if (now_ns - peer.last_send_ns < constants.heartbeat_interval_ns) continue;

        // Build a heartbeat frame (header only, no payload).
        var header_bytes: [FrameHeader.HEADER_SIZE]u8 = undefined;
        const hb = FrameHeader.heartbeat(self.local_node_id, peer.node_id);
        hb.writeTo(&header_bytes);

        // Enqueue directly — heartbeats bypass the write queue and go
        // straight to the TCP layer. They are small (24 bytes) and
        // must not be dropped due to queue overflow.
        self.tcp_io.submitWrite(
            peer.socket_fd,
            &header_bytes,
            @intFromPtr(peer),
        ) catch |err| {
            self.handleWriteError(peer, err);
            continue;
        };

        peer.last_send_ns = now_ns;
        self.counters.incrementFor(peer.node_id, .heartbeats_sent);
    }
}
```

### 8.3 Design Notes

- **Heartbeats bypass the write queue.** They are 24 bytes — small enough to submit
  directly without risking queue overflow or head-of-line blocking behind data frames.
  If the TCP socket buffer is full, the heartbeat write will fail, which triggers the
  same error handling path as a data write failure.

- **`last_send_ns` is updated on every write.** Both data frames and heartbeats update
  this timestamp. This ensures that a peer receiving a steady stream of data does not
  receive redundant heartbeats.

- **No sequence numbers.** Unlike the old UDP heartbeats, TCP heartbeats do not carry
  sequence numbers. The receiver identifies heartbeats by `frame_length == 24` and
  `template_id == HEARTBEAT_TEMPLATE`.

---

## 9. Connection Management

The sender owns all outgoing TCP connections — one per peer broker. The receiver loop
handles incoming connections separately (see [doc 06](06-receive-path.md)).

### 9.1 PeerConnection State

```zig
// src/sender/peer_connection.zig

const std = @import("std");
const constants = @import("../platform/constants.zig");
const WriteQueue = @import("write_queue.zig").WriteQueue;

pub const ConnectionState = enum {
    /// Not yet connected. Waiting for reconnect timer.
    disconnected,
    /// TCP connect in progress (non-blocking).
    connecting,
    /// Connected. Handshake sent, ready to send data.
    connected,
};

pub const PeerConnection = struct {
    /// The peer broker's node ID.
    node_id: u8,

    /// TCP socket file descriptor for the outgoing connection.
    socket_fd: std.posix.fd_t,

    /// IP address and port of the peer broker.
    address: std.net.Address,

    /// Current connection state.
    state: ConnectionState,

    /// Per-peer outbound write queue.
    write_queue: WriteQueue,

    /// Whether the write path is blocked (partial write pending).
    write_blocked: bool,

    /// Byte offset into the current frame for a partial write.
    partial_write_offset: usize,

    /// Monotonic timestamp (ns) of the last data or heartbeat sent.
    last_send_ns: i64,

    /// Reconnection backoff state.
    reconnect_delay_ms: u32,
    next_reconnect_ns: i64,

    const Self = @This();

    pub fn init(
        node_id: u8,
        address: std.net.Address,
        allocator: std.mem.Allocator,
    ) !Self {
        return .{
            .node_id = node_id,
            .socket_fd = -1,
            .address = address,
            .state = .disconnected,
            .write_queue = try WriteQueue.init(
                constants.peer_write_queue_capacity,
                constants.max_frame_size,
                allocator,
            ),
            .write_blocked = false,
            .partial_write_offset = 0,
            .last_send_ns = 0,
            .reconnect_delay_ms = constants.reconnect_base_delay_ms,
            .next_reconnect_ns = 0,
        };
    }

    /// Reset connection state for a new connection attempt.
    pub fn resetForReconnect(self: *Self) void {
        if (self.socket_fd >= 0) {
            std.posix.close(self.socket_fd);
            self.socket_fd = -1;
        }
        self.state = .disconnected;
        self.write_blocked = false;
        self.partial_write_offset = 0;
    }

    /// Advance the reconnect backoff timer (exponential with cap).
    pub fn advanceBackoff(self: *Self, now_ns: i64) void {
        self.next_reconnect_ns = now_ns +
            @as(i64, self.reconnect_delay_ms) * std.time.ns_per_ms;
        self.reconnect_delay_ms = @min(
            self.reconnect_delay_ms * 2,
            constants.reconnect_max_delay_ms,
        );
    }

    /// Reset backoff after successful connection.
    pub fn resetBackoff(self: *Self) void {
        self.reconnect_delay_ms = constants.reconnect_base_delay_ms;
    }

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        if (self.socket_fd >= 0) std.posix.close(self.socket_fd);
        self.write_queue.deinit(allocator);
    }
};
```

### 9.2 Connection Establishment

When a new peer is added (via command queue) or a disconnected peer's backoff timer
expires, the sender initiates a TCP connection:

```zig
// src/sender/sender_event_loop.zig

/// Initiate a TCP connection to a peer.
fn connectToPeer(self: *SenderEventLoop, peer: *PeerConnection) void {
    // Open a non-blocking TCP socket.
    const socket_fd = self.tcp_io.createSocket(peer.address) catch |err| {
        _ = err;
        self.counters.incrementFor(peer.node_id, .reconnect_attempts);
        peer.advanceBackoff(Clock.monotonicNanos());
        return;
    };

    peer.socket_fd = socket_fd;
    peer.state = .connecting;

    // Submit non-blocking connect via io_uring / kqueue.
    self.tcp_io.submitConnect(
        socket_fd,
        peer.address,
        @intFromPtr(peer),
    ) catch |err| {
        _ = err;
        peer.resetForReconnect();
        peer.advanceBackoff(Clock.monotonicNanos());
        self.counters.incrementFor(peer.node_id, .reconnect_attempts);
    };
}
```

### 9.3 Handshake

After the TCP connection is established, the sender sends a handshake frame to identify
itself and indicate the connection direction:

```zig
/// Send handshake frame on a newly established connection.
/// The handshake identifies this broker and the connection direction.
fn sendHandshake(self: *SenderEventLoop, peer: *PeerConnection) void {
    var handshake: [FrameHeader.HEADER_SIZE]u8 = undefined;
    const header = FrameHeader.overlayMut(&handshake);
    header.* = .{
        .frame_length = FrameHeader.HEADER_SIZE,
        .flags = constants.flag_handshake | constants.flag_direction_send,
        .source_node_id = self.local_node_id,
        .target_node_id = peer.node_id,
        .reserved_0 = 0,
        .source_service_id = 0,
        .target_service_id = 0,
        .template_id = constants.handshake_template_id,
        .reserved_1 = 0,
        .correlation_id = 0,
    };

    self.tcp_io.submitWrite(
        peer.socket_fd,
        &handshake,
        @intFromPtr(peer),
    ) catch |err| {
        self.handleWriteError(peer, err);
        return;
    };

    peer.state = .connected;
    peer.resetBackoff();
    peer.last_send_ns = Clock.monotonicNanos();
    self.connected_peer_count += 1;

    self.counters.incrementFor(peer.node_id, .reconnect_attempts);
}
```

### 9.4 Connection Lifecycle

```
    Sender                                              Receiver
    ──────                                              ────────
    connectToPeer()
         ───── TCP SYN ────────────────────────────────►
         ◄──── TCP SYN-ACK ────────────────────────────
         ───── TCP ACK ────────────────────────────────►
    sendHandshake()
         ───── Handshake frame (direction=SEND) ───────►
                                                        Validate handshake
                                                        Register sender peer
         ───── DATA frames ────────────────────────────►
         ───── Heartbeats (every 500ms if idle) ───────►

    ... connection healthy ...

    Write error (EPIPE / ECONNRESET):
    handleWriteError()
         peer.state = disconnected
         peer.advanceBackoff()
         Notify receiver loop to close incoming half

    Backoff timer expires:
    connectToPeer()
         ───── TCP SYN ────────────────────────────────►
         (cycle repeats)
```

### 9.5 Reconnection with Exponential Backoff

```zig
/// Process reconnection timers for disconnected peers.
fn processReconnections(self: *SenderEventLoop, now_ns: i64) void {
    for (&self.peers) |maybe_peer| {
        const peer = maybe_peer orelse continue;

        if (peer.state != .disconnected) continue;
        if (now_ns < peer.next_reconnect_ns) continue;

        self.connectToPeer(peer);
    }
}
```

Backoff schedule: 100 ms → 200 ms → 400 ms → 800 ms → 1000 ms (cap). Resets to 100 ms
on successful connection.

### 9.6 Coordinated Connection Recycling

If the receiver loop detects that an incoming connection from a peer has failed (read
error, heartbeat timeout), it sends a command to the sender loop to reconnect the
outgoing connection. This ensures both halves of the connection are reset together:

```
Receiver loop detects incoming failure
    │
    ├── Close incoming TCP socket
    │
    └── Send RECONNECT command to sender loop via cmd_queue
            │
            Sender loop processes command
            │
            ├── Close outgoing TCP socket
            ├── Reset PeerConnection state
            └── Start reconnect backoff timer
```

---

## 10. Fairness and Budgets

### 10.1 Per-Peer Write Budget

Each peer is limited to `WRITE_BUDGET_PER_PEER` (default: 16) frames per duty cycle
iteration. This prevents one peer with a large backlog from monopolizing the sender
thread's time and starving other peers.

```
Iteration N:
    Peer A: 50 queued frames → write 16, 34 remain
    Peer B:  3 queued frames → write  3,  0 remain
    Peer C: 20 queued frames → write 16,  4 remain

Total writes this iteration: 35
Next iteration: Peer A gets another 16, Peer C gets 4.
```

### 10.2 Round-Robin Across Peers

The sender iterates over all peers in order. Each peer gets up to its budget before
the sender moves on. This is simple round-robin — not weighted or priority-based.

```zig
/// Total write budget per iteration.
/// With 8 connected peers and budget=16, up to 128 writes per iteration.
const total_budget = constants.write_budget_per_peer * self.connected_peer_count;
```

### 10.3 Why Per-Peer Queues + Budgets

Without per-peer queues, a single slow peer could cause the entire write path to block
in the kernel's TCP send buffer. With per-peer queues and budgets:

| Problem | Solution |
|---------|----------|
| Slow peer blocks fast peers | Per-peer queues isolate backlog |
| One peer monopolizes sender | Write budget caps per-peer writes per iteration |
| Queue overflow drops wrong messages | Drop oldest (least relevant) in overflowing queue |
| Total throughput limited | Budget × peers scales with cluster size |

---

## 11. Counters & Observability

The sender maintains the following counters, all accessible through the shared
`CountersManager`. Per-peer counters are indexed by `node_id`.

### 11.1 Per-Peer Counters

| Counter | Description |
|---------|-------------|
| `bytes_sent` | Total bytes written to TCP (header + payload) |
| `frames_sent` | Total frames written to TCP |
| `heartbeats_sent` | Heartbeat frames sent |
| `peer_queue_overflow_drops` | Messages dropped because the peer's write queue overflowed |
| `peer_not_connected_drops` | Messages dropped because the peer is not connected |
| `write_errors` | TCP write errors (broken pipe, connection reset, etc.) |
| `reconnect_attempts` | Number of TCP reconnection attempts |

### 11.2 Global Counters

| Counter | Description |
|---------|-------------|
| `send_ring_reads` | Total records read from the send ring buffer |
| `write_completions_processed` | Total io_uring/kqueue write completions processed |
| `malformed_messages_dropped` | Messages dropped due to invalid or short frame header |

### 11.3 Counter Access Pattern

```zig
// Per-peer counter increment (hot path).
self.counters.incrementFor(peer.node_id, .frames_sent);

// Per-peer counter with value (hot path).
self.counters.incrementFor(peer.node_id, .bytes_sent, bytes_written);

// Global counter increment.
self.counters.increment(.send_ring_reads);
```

All counters are monotonically increasing. External monitoring reads them periodically
and computes rates by differencing consecutive readings.

---

## 12. Configuration Parameters

All sender configuration is defined in `src/platform/constants.zig` and can be overridden
at broker startup.

| Parameter | Default | Description |
|-----------|---------|-------------|
| `send_ring_buffer_size` | 2 MB | Capacity of the shared-memory MPSC ring buffer |
| `peer_write_queue_capacity` | 8192 | Maximum frames per peer's outbound write queue (must be power of 2) |
| `write_budget_per_peer` | 16 | Maximum frames written to one peer per duty cycle iteration |
| `send_batch_size` | 64 | Maximum records read from send ring buffer per iteration |
| `heartbeat_interval_ms` | 500 | Interval between heartbeat frames to idle peers |
| `reconnect_base_delay_ms` | 100 | Initial reconnect backoff delay |
| `reconnect_max_delay_ms` | 1000 | Maximum reconnect backoff delay (cap) |
| `max_frame_size` | 65536 | Maximum frame size (header + payload) per write queue slot |
| `max_peers` | 16 | Maximum number of peer brokers (array size) |
| `completion_batch_limit` | 64 | Maximum io_uring/kqueue completions processed per iteration |
| `command_drain_limit` | 8 | Maximum commands processed from the control loop per iteration |

### Configuration Interactions

```
send_ring_buffer_size
    └── Must be large enough to absorb burst writes from all local services.
        If services produce faster than the sender drains, they see
        error.InsufficientCapacity.

peer_write_queue_capacity × max_frame_size
    └── Maximum memory per peer for outbound buffering.
        8192 × 65536 = 512 MB worst case — but typical frames are much smaller.
        Actual memory = capacity × max_frame_size (pre-allocated).

write_budget_per_peer × max_peers
    └── Maximum total writes per duty cycle iteration.
        16 × 16 = 256 writes. At ~24µs per io_uring submit,
        this keeps the iteration under 6 ms.
```

---

## 13. Error Handling

All errors in the sender event loop are **non-fatal** to the event loop itself. The sender
never panics or exits due to a transient error. Each error class is handled locally.

### 13.1 TCP Write Error

```zig
/// Handle a write error on a peer connection.
/// Closes the connection and enters reconnect state.
fn handleWriteError(self: *SenderEventLoop, peer: *PeerConnection, err: anyerror) void {
    _ = err;

    // Close the socket.
    peer.resetForReconnect();
    self.connected_peer_count -|= 1;

    // Enter reconnect state with backoff.
    peer.advanceBackoff(Clock.monotonicNanos());

    self.counters.incrementFor(peer.node_id, .write_errors);

    // Notify receiver loop to close its incoming connection from this peer.
    // This is done via a command on the receiver's command queue.
    self.notifyReceiverDisconnect(peer.node_id);
}
```

Write errors include:
- `EPIPE` — peer closed the connection.
- `ECONNRESET` — connection reset by peer.
- `ETIMEDOUT` — TCP retransmit timeout exceeded.
- `ENOTCONN` — socket not connected (race with disconnect).

All trigger the same path: close → backoff → reconnect.

### 13.2 Send Ring Buffer Corruption

If a record in the send ring buffer has an invalid length or the frame header is
malformed, the sender skips the record and increments `malformed_messages_dropped`.
The ring buffer's read position advances past the corrupted record, so subsequent
records are unaffected.

### 13.3 Queue Overflow

When a peer's write queue overflows, the oldest message is dropped (see §5.3). This
is a deliberate design choice: the sender loop must never block. Blocking would stall
the entire event loop, affecting all peers and all phases of the duty cycle.

### 13.4 Error Summary

| Error Class | Action | Impact |
|-------------|--------|--------|
| TCP write error | Close connection, reconnect with backoff | Peer temporarily unreachable |
| Ring buffer corruption | Skip record, increment counter | One message lost |
| Write queue overflow | Drop oldest, increment counter | One message lost |
| Connect failure | Increment backoff, retry later | Peer temporarily unreachable |
| Socket creation failure | Log, retry on next backoff tick | Peer temporarily unreachable |

---

## 14. Testing

### 14.1 Unit Tests — Frame Construction

```zig
// src/protocol/frames_test.zig

const std = @import("std");
const testing = std.testing;
const FrameHeader = @import("frames.zig").FrameHeader;
const constants = @import("../platform/constants.zig");

test "FrameHeader has correct size" {
    try testing.expectEqual(@as(usize, 24), @sizeOf(FrameHeader));
}

test "FrameHeader overlay reads fields correctly" {
    // Given: a 24-byte buffer with known values (little-endian).
    var buf: [24]u8 = .{
        // frame_length = 100 (0x64000000 LE)
        0x64, 0x00, 0x00, 0x00,
        // flags = 0x01, source_node_id = 2, target_node_id = 3, reserved = 0
        0x01, 0x02, 0x03, 0x00,
        // source_service_id = 10 (0x0A00 LE), target_service_id = 20 (0x1400 LE)
        0x0A, 0x00, 0x14, 0x00,
        // template_id = 42 (0x2A00 LE), reserved = 0
        0x2A, 0x00, 0x00, 0x00,
        // correlation_id = 12345 (0x3930000000000000 LE)
        0x39, 0x30, 0x00, 0x00, 0x00, 0x00, 0x00, 0x00,
    };

    // When: overlaid.
    const header = FrameHeader.overlay(&buf);

    // Then: fields match.
    try testing.expectEqual(@as(u32, 100), header.frame_length);
    try testing.expectEqual(@as(u8, 0x01), header.flags);
    try testing.expectEqual(@as(u8, 2), header.source_node_id);
    try testing.expectEqual(@as(u8, 3), header.target_node_id);
    try testing.expectEqual(@as(u16, 10), header.source_service_id);
    try testing.expectEqual(@as(u16, 20), header.target_service_id);
    try testing.expectEqual(@as(u16, 42), header.template_id);
    try testing.expectEqual(@as(i64, 0x3039), header.correlation_id);
}

test "FrameHeader writeTo produces correct bytes" {
    // Given: a header with known values.
    const header = FrameHeader{
        .frame_length = 48,
        .flags = 0,
        .source_node_id = 1,
        .target_node_id = 5,
        .reserved_0 = 0,
        .source_service_id = 100,
        .target_service_id = 200,
        .template_id = 7,
        .reserved_1 = 0,
        .correlation_id = -1,
    };

    // When: serialized.
    var buf: [24]u8 = undefined;
    header.writeTo(&buf);

    // Then: reading it back produces the same values.
    const read_back = FrameHeader.overlay(&buf);
    try testing.expectEqual(@as(u32, 48), read_back.frame_length);
    try testing.expectEqual(@as(u8, 1), read_back.source_node_id);
    try testing.expectEqual(@as(u8, 5), read_back.target_node_id);
    try testing.expectEqual(@as(u16, 100), read_back.source_service_id);
    try testing.expectEqual(@as(u16, 200), read_back.target_service_id);
    try testing.expectEqual(@as(u16, 7), read_back.template_id);
    try testing.expectEqual(@as(i64, -1), read_back.correlation_id);
}

test "heartbeat frame has correct template_id and size" {
    const hb = FrameHeader.heartbeat(1, 2);

    try testing.expectEqual(FrameHeader.HEADER_SIZE, hb.frame_length);
    try testing.expectEqual(constants.heartbeat_template_id, hb.template_id);
    try testing.expectEqual(@as(u8, 1), hb.source_node_id);
    try testing.expectEqual(@as(u8, 2), hb.target_node_id);
    try testing.expectEqual(@as(i64, 0), hb.correlation_id);
}
```

### 14.2 Unit Tests — Write Queue Overflow

```zig
// src/sender/write_queue_test.zig

const std = @import("std");
const testing = std.testing;
const WriteQueue = @import("write_queue.zig").WriteQueue;

test "enqueue and dequeue single frame" {
    var queue = try WriteQueue.init(4, 256, testing.allocator);
    defer queue.deinit(testing.allocator);

    // Given: a test frame.
    const frame = [_]u8{ 0x18, 0x00, 0x00, 0x00 } ++ ([_]u8{0xAA} ** 20);

    // When: enqueued.
    try queue.enqueue(&frame);

    // Then: peek returns the frame.
    const peeked = queue.peek().?;
    try testing.expectEqualSlices(u8, &frame, peeked);

    // When: dequeued.
    queue.dequeue();

    // Then: queue is empty.
    try testing.expect(queue.isEmpty());
    try testing.expect(queue.peek() == null);
}

test "queue overflow returns error" {
    // Given: a queue with capacity 4.
    var queue = try WriteQueue.init(4, 64, testing.allocator);
    defer queue.deinit(testing.allocator);

    const frame = [_]u8{0xBB} ** 24;

    // When: filled to capacity.
    try queue.enqueue(&frame);
    try queue.enqueue(&frame);
    try queue.enqueue(&frame);
    try queue.enqueue(&frame);

    // Then: next enqueue returns Overflow.
    try testing.expectError(error.Overflow, queue.enqueue(&frame));
    try testing.expect(queue.isFull());
}

test "dropOldest frees slot for new enqueue" {
    // Given: a full queue with capacity 4.
    var queue = try WriteQueue.init(4, 64, testing.allocator);
    defer queue.deinit(testing.allocator);

    const frame_a = [_]u8{0xAA} ** 24;
    const frame_b = [_]u8{0xBB} ** 24;

    try queue.enqueue(&frame_a);
    try queue.enqueue(&frame_a);
    try queue.enqueue(&frame_a);
    try queue.enqueue(&frame_a);
    try testing.expect(queue.isFull());

    // When: drop oldest and enqueue new frame.
    const dropped = queue.dropOldest();
    try testing.expect(dropped > 0);

    try queue.enqueue(&frame_b);

    // Then: queue has 4 frames, oldest is frame_a (second one).
    try testing.expectEqual(@as(u32, 4), queue.count);

    // Dequeue 3 frame_a's.
    queue.dequeue();
    queue.dequeue();
    queue.dequeue();

    // Then: last frame is frame_b.
    const last = queue.peek().?;
    try testing.expectEqualSlices(u8, &frame_b, last);
}

test "contiguousCount stops at wrap boundary" {
    // Given: a queue with capacity 4, head near the end.
    var queue = try WriteQueue.init(4, 64, testing.allocator);
    defer queue.deinit(testing.allocator);

    const frame = [_]u8{0xCC} ** 24;

    // Advance head to slot 3 by enqueuing and dequeuing.
    try queue.enqueue(&frame);
    try queue.enqueue(&frame);
    try queue.enqueue(&frame);
    queue.dequeue();
    queue.dequeue();
    queue.dequeue();

    // Now head=3, tail=3, count=0. Enqueue 3 frames.
    try queue.enqueue(&frame); // slot 3
    try queue.enqueue(&frame); // slot 0 (wraps)
    try queue.enqueue(&frame); // slot 1

    // Then: contiguous from head=3 is only 1 (slot 3, then wraps).
    try testing.expectEqual(@as(u32, 1), queue.contiguousCount(16));
}
```

### 14.3 Unit Tests — Routing Logic

```zig
// src/sender/routing_test.zig

const std = @import("std");
const testing = std.testing;
const FrameHeader = @import("../protocol/frames.zig").FrameHeader;

test "route message to correct peer by target_node_id" {
    // Given: a mock sender with peers at node_id 1 and 3.
    var sender = try TestSenderBuilder.init(testing.allocator)
        .withPeer(1, "10.0.0.1:9001")
        .withPeer(3, "10.0.0.3:9001")
        .withConnected(1)
        .withConnected(3)
        .build();
    defer sender.deinit();

    // Given: a message targeting node_id=3.
    var frame_buf: [100]u8 = undefined;
    const header = FrameHeader.overlayMut(&frame_buf);
    header.* = .{
        .frame_length = 100,
        .flags = 0,
        .source_node_id = 0,
        .target_node_id = 3,
        .reserved_0 = 0,
        .source_service_id = 5,
        .target_service_id = 10,
        .template_id = 42,
        .reserved_1 = 0,
        .correlation_id = 12345,
    };

    // When: routed.
    sender.routeMessage(&frame_buf);

    // Then: message is in peer 3's write queue, not peer 1's.
    try testing.expect(!sender.peerQueue(3).isEmpty());
    try testing.expect(sender.peerQueue(1).isEmpty());
}

test "drop message when peer is disconnected" {
    // Given: a sender with peer at node_id 2, not connected.
    var sender = try TestSenderBuilder.init(testing.allocator)
        .withPeer(2, "10.0.0.2:9001")
        .build();
    defer sender.deinit();

    // Given: a message targeting node_id=2.
    var frame_buf: [100]u8 = undefined;
    const header = FrameHeader.overlayMut(&frame_buf);
    header.* = .{
        .frame_length = 100,
        .flags = 0,
        .source_node_id = 0,
        .target_node_id = 2,
        .reserved_0 = 0,
        .source_service_id = 1,
        .target_service_id = 2,
        .template_id = 1,
        .reserved_1 = 0,
        .correlation_id = 0,
    };

    // When: routed.
    sender.routeMessage(&frame_buf);

    // Then: message dropped, counter incremented.
    try testing.expect(sender.peerQueue(2).isEmpty());
    try testing.expectEqual(
        @as(u64, 1),
        sender.counters.get(.peer_not_connected_drops),
    );
}

test "drop message when target_node_id has no peer entry" {
    // Given: a sender with no peers.
    var sender = try TestSenderBuilder.init(testing.allocator).build();
    defer sender.deinit();

    // Given: a message targeting node_id=15 (no peer).
    var frame_buf: [48]u8 = undefined;
    const header = FrameHeader.overlayMut(&frame_buf);
    header.* = .{
        .frame_length = 48,
        .flags = 0,
        .source_node_id = 0,
        .target_node_id = 15,
        .reserved_0 = 0,
        .source_service_id = 1,
        .target_service_id = 2,
        .template_id = 1,
        .reserved_1 = 0,
        .correlation_id = 0,
    };

    // When: routed.
    sender.routeMessage(&frame_buf);

    // Then: message dropped, counter incremented.
    try testing.expectEqual(
        @as(u64, 1),
        sender.counters.get(.peer_not_connected_drops),
    );
}
```

### 14.4 Integration Tests — Sender Loop with Mock TCP Transport

```zig
// src/sender/sender_event_loop_test.zig

const std = @import("std");
const testing = std.testing;
const SenderEventLoop = @import("sender_event_loop.zig").SenderEventLoop;
const FrameHeader = @import("../protocol/frames.zig").FrameHeader;

test "sender event loop routes message and submits TCP write" {
    // Given: a sender with a mock TCP transport and one connected peer.
    var env = try TestSenderEnv.init(testing.allocator);
    defer env.deinit();

    env.addPeer(1, "10.0.0.1:9001");
    env.connectPeer(1);

    // Given: a message in the send ring buffer targeting node_id=1.
    var frame_buf: [80]u8 = undefined;
    @memset(&frame_buf, 0);
    const header = FrameHeader.overlayMut(&frame_buf);
    header.frame_length = 80;
    header.target_node_id = 1;
    header.source_service_id = 5;
    header.target_service_id = 10;
    header.template_id = 42;
    header.correlation_id = 99;

    env.writeToSendRingBuffer(&frame_buf);

    // When: one duty cycle.
    _ = env.sender.doWork();

    // Then: TCP write was submitted to the mock transport.
    try testing.expectEqual(@as(u32, 1), env.mock_tcp.writeCountFor(1));

    // Then: the written bytes contain the correct frame header.
    const written = env.mock_tcp.lastWrittenFrame(1);
    const written_header = FrameHeader.overlay(written);
    try testing.expectEqual(@as(u32, 80), written_header.frame_length);
    try testing.expectEqual(@as(u8, 1), written_header.target_node_id);
    try testing.expectEqual(@as(u16, 42), written_header.template_id);
    try testing.expectEqual(@as(i64, 99), written_header.correlation_id);
}

test "sender sends heartbeat when peer is idle" {
    // Given: a sender with one connected peer that hasn't sent data.
    var env = try TestSenderEnv.init(testing.allocator);
    defer env.deinit();

    env.addPeer(1, "10.0.0.1:9001");
    env.connectPeer(1);

    // Simulate time passing beyond the heartbeat interval.
    env.sender.last_heartbeat_ns = 0;
    env.setPeerLastSend(1, 0);
    env.mock_clock.setNanos(constants.heartbeat_interval_ns + 1);

    // When: duty cycle runs.
    _ = env.sender.doWork();

    // Then: heartbeat was sent.
    const writes = env.mock_tcp.writeCountFor(1);
    try testing.expect(writes > 0);

    const last_frame = env.mock_tcp.lastWrittenFrame(1);
    const header = FrameHeader.overlay(last_frame);
    try testing.expectEqual(FrameHeader.HEADER_SIZE, header.frame_length);
    try testing.expectEqual(constants.heartbeat_template_id, header.template_id);
}

test "sender reconnects after write error with exponential backoff" {
    // Given: a sender with one connected peer.
    var env = try TestSenderEnv.init(testing.allocator);
    defer env.deinit();

    env.addPeer(1, "10.0.0.1:9001");
    env.connectPeer(1);

    // When: write error occurs.
    env.mock_tcp.injectWriteError(1, error.BrokenPipe);
    env.writeToSendRingBuffer(&env.makeFrame(1, 48));
    _ = env.sender.doWork();

    // Then: peer is disconnected.
    const peer = env.sender.peers[1].?;
    try testing.expectEqual(.disconnected, peer.state);

    // Then: backoff is set.
    try testing.expectEqual(
        @as(u32, constants.reconnect_base_delay_ms * 2),
        peer.reconnect_delay_ms,
    );

    // When: time passes beyond backoff.
    env.mock_clock.advance(peer.next_reconnect_ns);
    _ = env.sender.doWork();

    // Then: reconnect was attempted.
    try testing.expect(env.mock_tcp.connectAttempts(1) > 0);
}

test "frame ordering preserved within a peer's write queue" {
    // Given: a sender with one connected peer.
    var env = try TestSenderEnv.init(testing.allocator);
    defer env.deinit();

    env.addPeer(1, "10.0.0.1:9001");
    env.connectPeer(1);

    // Given: 3 messages with distinct correlation IDs.
    for ([_]i64{ 100, 200, 300 }) |corr_id| {
        var frame_buf: [48]u8 = undefined;
        @memset(&frame_buf, 0);
        const header = FrameHeader.overlayMut(&frame_buf);
        header.frame_length = 48;
        header.target_node_id = 1;
        header.correlation_id = corr_id;
        env.writeToSendRingBuffer(&frame_buf);
    }

    // When: duty cycle runs.
    _ = env.sender.doWork();

    // Then: frames arrive in order.
    const frames = env.mock_tcp.allWrittenFrames(1);
    try testing.expectEqual(@as(usize, 3), frames.len);
    try testing.expectEqual(@as(i64, 100), FrameHeader.overlay(frames[0]).correlation_id);
    try testing.expectEqual(@as(i64, 200), FrameHeader.overlay(frames[1]).correlation_id);
    try testing.expectEqual(@as(i64, 300), FrameHeader.overlay(frames[2]).correlation_id);
}
```

### 14.5 Performance Tests

```zig
// src/sender/sender_bench.zig

const std = @import("std");
const FrameHeader = @import("../protocol/frames.zig").FrameHeader;
const WriteQueue = @import("write_queue.zig").WriteQueue;

test "write queue throughput benchmark" {
    // Measure enqueue/dequeue throughput with 64-byte frames.
    var queue = try WriteQueue.init(8192, 256, std.testing.allocator);
    defer queue.deinit(std.testing.allocator);

    const frame = [_]u8{0xAA} ** 64;
    const iterations: u32 = 1_000_000;

    var timer = std.time.Timer.start() catch unreachable;

    var i: u32 = 0;
    while (i < iterations) : (i += 1) {
        try queue.enqueue(&frame);
        if (queue.count >= 4096) {
            // Drain half the queue to keep it from filling.
            var j: u32 = 0;
            while (j < 2048) : (j += 1) {
                queue.dequeue();
            }
        }
    }

    const elapsed_ns = timer.read();
    const ns_per_op = elapsed_ns / iterations;

    // Expect < 100ns per enqueue on modern hardware.
    std.debug.print(
        "\nWriteQueue: {d} ops in {d}ms ({d}ns/op)\n",
        .{ iterations, elapsed_ns / std.time.ns_per_ms, ns_per_op },
    );
}

test "frame header construction throughput benchmark" {
    // Measure zero-copy header overlay throughput.
    const iterations: u32 = 10_000_000;
    var buf: [24]u8 = undefined;

    var timer = std.time.Timer.start() catch unreachable;

    var i: u32 = 0;
    while (i < iterations) : (i += 1) {
        const header = FrameHeader.overlayMut(&buf);
        header.* = FrameHeader.heartbeat(1, @as(u8, @truncate(i)));
        std.mem.doNotOptimizeAway(&buf);
    }

    const elapsed_ns = timer.read();
    const ns_per_op = elapsed_ns / iterations;

    // Expect < 10ns per construction on modern hardware.
    std.debug.print(
        "\nFrameHeader construction: {d} ops in {d}ms ({d}ns/op)\n",
        .{ iterations, elapsed_ns / std.time.ns_per_ms, ns_per_op },
    );
}
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

All `PeerConnection` fields, write queues, and connection state are single-writer (sender
thread only) and require no synchronization.

On x86-64, the ring buffer and command queue atomics compile to plain loads/stores with
compiler fences (TSO). On ARM, they compile to `ldapr`/`stlr` pairs.

---

*Previous: [04 — TCP Transport Library](04-tcp-transport-library.md)*
*Next: [06 — Receive Path](06-receive-path.md)*
