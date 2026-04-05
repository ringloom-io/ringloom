# 06 — Receive Path

> **Depends on:** [02 — Memory Layout & Shared Memory](02-memory-layout-and-shared-memory.md) (receive log buffer layout, service metadata files, `BuffersProvider`),
> [03 — Concurrent Data Structures](03-concurrent-data-structures.md) (MPSC ring buffer for routing to services),
> [04 — UDP Transport & io_uring](04-udp-transport-and-io-uring.md) (`DataFrameHeader`, `SetupFrame`, `StatusMessage`, `NakFrame`, `NetworkIo`, `parseFrame`)
>
> **Depended on by:** [07 — Flow Control](07-flow-control.md) (receiver window calculation, Status Message timing, back-pressure propagation),
> [08 — Service IPC](08-service-ipc.md) (cross-host messages land in service ring buffers via the routing described here)

This document describes the **receive path** — the subsystem that accepts inbound UDP
packets from peer brokers, inserts them into per-peer receive log buffers, detects
packet loss and requests retransmission, reassembles fragmented messages, routes
complete messages to target service ring buffers, and sends Status Messages back to
senders for flow control.

All code targets **Zig 0.14.x** stable.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Receive Log Buffer](#2-receive-log-buffer)
3. [Receiver Event Loop](#3-receiver-event-loop)
4. [Packet Processing](#4-packet-processing)
5. [Message Routing](#5-message-routing)
6. [Fragment Reassembly](#6-fragment-reassembly)
7. [Loss Detection & NAK Generation](#7-loss-detection--nak-generation)
8. [Status Messages](#8-status-messages)
9. [Connection Handling (SETUP)](#9-connection-handling-setup)
10. [Admin Message Handling](#10-admin-message-handling)
11. [Counters & Observability](#11-counters--observability)
12. [Testing](#12-testing)
13. [File Structure](#13-file-structure)

---

## 1. Overview

```
UDP Socket → io_uring CQE → Receiver Event Loop → Receive Log Buffer (per peer)
                                                        │
                                                        ├── Unfragmented → Route to Service Ring Buffer
                                                        ├── Fragment → FragmentAssembler → Route
                                                        ├── Admin → handleAdminMessage()
                                                        └── Heartbeat → update liveness, no route
```

The receiver event loop is a **single thread** that handles all incoming UDP traffic
from all peer brokers. It runs as a duty-cycle event loop (spin on work count, idle
strategy when no work), following the same pattern as every other event loop in BRZ
(see doc 01, §5).

Each peer broker gets its own **receive log buffer** — an anonymous-mmap'd circular
buffer (defined in doc 02, §5) where inbound data frames are stored for gap tracking,
ordering verification, and reassembly. The receive log decouples packet reception from
message delivery: frames land in the log immediately, and the routing step reads them
out in order.

The duty cycle has five phases per iteration:

1. **Poll io_uring completions** — drain received packets, parse frame type, dispatch.
2. **Process inter-loop commands** — peer add/remove, shutdown signals.
3. **Scan for losses** — walk each peer's receive log for gaps, queue NAK frames.
4. **Send Status Messages** — rate-limited, periodic + eager on consumption advance.
5. **Submit io_uring SQEs** — batch-submit all queued sends (NAKs, SMs) in one syscall.

All five phases return work counts that feed the idle strategy. The single-writer
principle holds: only the receiver thread writes to receive log buffers, only the
receiver thread sends NAKs and SMs for its peers.

---

## 2. Receive Log Buffer

The receive log buffer's memory layout and struct definition are in doc 02, §5. This
section covers the **insertion algorithm** and **consumption protocol** that the
receiver event loop uses at runtime.

### 2.1 Recap: Layout

```
┌──────────────────────────────────────────────────────────────┐
│  Log Data (capacity bytes, power of 2)                       │
│                                                              │
│  [len₀|frame₀|pad][len₁|frame₁|pad][0000 = gap][len₃|...]  │
│                                                              │
├──────────────────────────────────────────────────────────────┤  ← offset = capacity
│  Log Metadata (256 bytes)                                    │
│  +0:   tail_position      (volatile i64)  ← receiver thread │
│  +128: rebuild_position   (volatile i64)  ← control/router  │
└──────────────────────────────────────────────────────────────┘
```

- **`tail_position`** — the highest byte position written. Advances monotonically as
  frames are inserted. Written only by the receiver thread.
- **`rebuild_position`** — the highest contiguous position verified by the loss
  detector. Written by the receiver thread during loss detection scans.
- Frames are aligned to 32-byte boundaries (`frame_alignment = 32`).
- Each frame slot starts with a 4-byte `i32` length prefix. A length of `0` means the
  slot is empty (gap). A length of `-1` means the frame has been consumed and routed
  (see §5.2).

### 2.2 Packet Insertion

When a DATA frame arrives from the network, the receiver inserts it into the peer's
receive log. The insertion is **position-based**: the frame's `sequence_number` field
determines where it lands in the log.

**File: `src/receiver/receive_log_buffer.zig`** (extends the struct from doc 02)

```zig
const std = @import("std");
const constants = @import("../platform/constants.zig");
const frames = @import("../protocol/frames.zig");
const ReceiveLogBuffer = @import("../memory/receive_log.zig").ReceiveLogBuffer;

pub const frame_alignment: usize = 32;

/// Insert a received data frame into the log buffer at the position
/// determined by its sequence_number.
///
/// The frame is stored as: [i32 length prefix][frame bytes][padding to 32B].
///
/// The length prefix is written LAST with release semantics — this is the
/// commit pattern. Readers (loss detector, router) spin on the length field
/// and only see the frame once the length becomes positive.
///
/// Insertion is idempotent: if the slot already contains a frame (length > 0),
/// the write is skipped. This handles retransmitted packets gracefully.
pub fn insertPacket(log: *ReceiveLogBuffer, frame: []const u8) void {
    if (frame.len < @sizeOf(frames.DataFrameHeader)) return;

    const header: *const frames.DataFrameHeader = @ptrCast(@alignCast(frame.ptr));
    const sequence = header.sequence_number;
    if (sequence < 0) return; // invalid sequence

    // Calculate the buffer position for this sequence number.
    // Each sequence number maps to a fixed slot: seq * max_aligned_frame_size.
    // But in BRZ's simplified model, sequence_number IS the byte position
    // in the logical stream — the sender assigns it as a monotonic byte offset.
    const position: i64 = sequence;
    const index: usize = @intCast(@as(u64, @bitCast(position)) & log.mask);

    // ── Idempotency check ─────────────────────────────────────────────
    // If the slot already has a committed frame (length > 0) or has been
    // consumed (length == -1), skip the write. This handles retransmits
    // and duplicate packets.
    const length_ptr: *align(1) volatile i32 = @ptrCast(&log.data[index]);
    const existing = @atomicLoad(i32, length_ptr, .acquire);
    if (existing != 0) return;

    // ── Write frame data FIRST ────────────────────────────────────────
    // Copy the entire frame (header + payload) into the slot, starting
    // after the 4-byte length prefix.
    const frame_length: i32 = @intCast(frame.len);
    const data_start = index + @sizeOf(i32);
    @memcpy(log.data[data_start..][0..frame.len], frame);

    // ── Write aligned-length field ────────────────────────────────────
    // Store the 32-byte-aligned total slot size in a secondary field
    // at a fixed offset within the slot. This is needed by the
    // consumption position scanner (doc 07, §3.3) to advance past
    // consumed frames even after the primary length is overwritten
    // with the consumed marker (-1).
    const total_slot_size: i32 = @intCast(constants.alignUp(
        frame.len + @sizeOf(i32),
        frame_alignment,
    ));
    const aligned_len_offset = index + @sizeOf(i32) + @sizeOf(i32); // after length prefix + frame_length field of DataFrameHeader
    const aligned_len_ptr: *align(1) volatile i32 = @ptrCast(&log.data[aligned_len_offset]);
    @atomicStore(i32, aligned_len_ptr, total_slot_size, .monotonic);

    // ── Commit: write length prefix LAST with release ─────────────────
    // This is the publication barrier. Once the length becomes positive,
    // readers can see the complete frame data.
    @atomicStore(i32, length_ptr, frame_length, .release);

    // ── Advance high-water mark (tail_position) ───────────────────────
    // Single writer (receiver thread), so a simple max is sufficient.
    const new_tail: i64 = position + @as(i64, @intCast(total_slot_size));
    const current_tail = log.loadTailPosition();
    if (new_tail > current_tail) {
        log.storeTailPosition(new_tail);
    }
}
```

**Key invariants:**

| Property | Guarantee |
|----------|-----------|
| Commit ordering | Frame data is fully written before length prefix becomes non-zero |
| Idempotency | Retransmitted packets are silently skipped (slot already occupied) |
| Single writer | Only the receiver thread calls `insertPacket` for a given peer's log |
| Alignment | Every slot is 32-byte aligned; index is derived from `position & mask` |
| No allocation | Zero allocations — writes go directly into pre-allocated mmap'd memory |

**Memory ordering rationale:**

| Operation | Ordering | Why |
|-----------|----------|-----|
| Load existing length | Acquire | Must see prior writes to this slot |
| Store frame data | (plain memcpy) | Data is not visible until the commit store |
| Store aligned-length | Monotonic | Only read after the length prefix is seen (acquire on reader) |
| Store length prefix | Release | Publishes all preceding writes to readers |
| Load/store tail_position | Acquire/Release | Cross-thread visibility with loss detector |

### 2.3 Reading Frames from the Log

The routing step and loss detector read frames from the log by position. This uses
the `readFrame` function defined in doc 02:

```zig
/// Read a frame at the given absolute position.
/// Returns the frame data slice (excluding the 4-byte length prefix),
/// or null if the frame is not yet committed (length <= 0).
pub fn readFrame(log: *const ReceiveLogBuffer, position: i64) ?[]const u8 {
    const index: usize = @intCast(@as(u64, @bitCast(position)) & log.mask);
    const length_ptr: *const align(1) volatile i32 = @ptrCast(&log.data[index]);
    const length = @atomicLoad(i32, length_ptr, .acquire);

    if (length <= 0) return null;

    const data_start = index + @sizeOf(i32);
    return log.data[data_start..][0..@as(usize, @intCast(length))];
}
```

A return of `null` means either:
- The slot is empty (gap — length == 0), or
- The frame has been consumed (length == -1).

The caller distinguishes these cases by checking whether `position < consumption_position`
(consumed) or `position >= consumption_position` (gap).

### 2.4 Circular Overwrite Model

The receive log buffer uses a **circular overwrite model** (doc 02, §5.4). If the
router falls behind by more than `capacity` bytes, stale data is silently overwritten.
Flow control (doc 07) prevents this under normal operation: the receiver advertises a
window that is at most `capacity / 2`, ensuring the sender cannot outrun the consumer
by more than half the buffer.

If overwrite does occur (e.g., during a burst or flow control misconfiguration), the
loss detector will see corrupted frames and the receiver will send a Status Message
with the `SEND_SETUP` flag, triggering connection re-establishment.

---

## 3. Receiver Event Loop

**File: `src/receiver/receiver_event_loop.zig`**

### 3.1 State

```zig
const std = @import("std");
const constants = @import("../platform/constants.zig");
const platform = @import("../platform.zig");
const frames = @import("../protocol/frames.zig");
const frame_parser = @import("../protocol/frame_parser.zig");
const NetworkIo = @import("../transport/network_io.zig").NetworkIo;
const ReceiveLogBuffer = @import("../memory/receive_log.zig").ReceiveLogBuffer;
const RingBuffer = @import("../concurrent/ring_buffer.zig").RingBuffer;
const CountersManager = @import("../concurrent/counters.zig").CountersManager;
const ReceiverFlowControl = @import("../flow_control/receiver_flow_control.zig").ReceiverFlowControl;
const StatusMessageScheduler = @import("../flow_control/status_message.zig").StatusMessageScheduler;

pub const ReceiverEventLoop = struct {
    // ── I/O ───────────────────────────────────────────────────────────
    /// io_uring (Linux) / kqueue (macOS) abstraction for UDP recv/send.
    io_ring: *NetworkIo,

    /// The UDP socket file descriptor bound to this broker's receive port.
    socket_fd: std.posix.fd_t,

    // ── Peer state ────────────────────────────────────────────────────
    /// nodeId → PeerReceiver. One entry per connected peer broker.
    /// Keyed by u8 node ID, stored in a fixed-size array (max 16 peers).
    peers: [constants.default_max_peers]?*PeerReceiver,

    /// Count of active peers. Used for iteration bounds.
    peer_count: u8,

    // ── Routing ───────────────────────────────────────────────────────
    /// Service registry: serviceId → service ring buffer + metadata.
    service_registry: *ServiceRegistry,

    /// Fragment assemblers, keyed by (source_node_id << 16 | source_service_id).
    /// One assembler per unique source.
    fragment_assemblers: [MAX_FRAGMENT_ASSEMBLERS]FragmentAssembler,
    fragment_assembler_count: u16,

    // ── Inter-loop communication ──────────────────────────────────────
    /// Command queue for commands from the control loop (add peer, remove peer).
    cmd_queue: *RingBuffer,

    /// Command queue for sending commands TO the control loop.
    control_cmd_queue: *RingBuffer,

    // ── Observability ─────────────────────────────────────────────────
    counters: *CountersManager,

    // ── Timing ────────────────────────────────────────────────────────
    next_sm_ns: i64,

    // ── Identity ──────────────────────────────────────────────────────
    local_node_id: u8,

    // ── Constants ─────────────────────────────────────────────────────
    const MAX_FRAGMENT_ASSEMBLERS: u16 = 256;

    const Self = @This();
    // ...
};
```

### 3.2 PeerReceiver

Each connected peer broker is represented by a `PeerReceiver`. This struct holds the
per-peer receive log buffer, loss detection state, flow control state, and the peer's
network address (for sending NAKs and SMs back).

```zig
/// Per-peer receiver state. One instance per connected peer broker.
pub const PeerReceiver = struct {
    /// The peer's node ID. Matches the source_node_id in data frame headers.
    node_id: u8,

    /// Per-peer receive log buffer. Frames from this peer are inserted here.
    recv_log: *ReceiveLogBuffer,

    /// Loss detector state — scans recv_log for gaps and generates NAKs.
    loss_detector: LossDetector,

    /// Receiver-side flow control — tracks consumption_position, calculates
    /// receiver window for Status Messages. Defined in doc 07, §3.1.
    flow_control: ReceiverFlowControl,

    /// Status message scheduler — determines when to send SMs (periodic
    /// + eager). Defined in doc 07, §4.4.
    sm_scheduler: StatusMessageScheduler,

    /// The peer's source address. Captured from the SETUP frame or the
    /// first data frame received. Used as the destination for NAKs and SMs.
    address: std.net.Address,

    /// Timestamp (monotonic ns) of the last packet received from this peer.
    /// Used for liveness detection — if no packets arrive for a configurable
    /// timeout, the peer is considered dead.
    last_packet_received_ns: i64,

    /// The initial sequence number from the peer's SETUP frame. Used to
    /// initialize the loss detector's scan start position.
    initial_sequence: i64,

    /// Pre-allocated buffer for encoding outbound Status Messages to this peer.
    sm_buffer: [28]u8,

    /// Pre-allocated buffer for encoding outbound NAK frames to this peer.
    nak_buffer: [24]u8,

    const Self = @This();

    pub fn init(
        node_id: u8,
        recv_log: *ReceiveLogBuffer,
        address: std.net.Address,
        initial_sequence: i64,
    ) Self {
        return .{
            .node_id = node_id,
            .recv_log = recv_log,
            .loss_detector = LossDetector.init(initial_sequence),
            .flow_control = ReceiverFlowControl.init(recv_log),
            .sm_scheduler = StatusMessageScheduler{},
            .address = address,
            .last_packet_received_ns = platform.Clock.monotonicNanos(),
            .initial_sequence = initial_sequence,
            .sm_buffer = [_]u8{0} ** 28,
            .nak_buffer = [_]u8{0} ** 24,
        };
    }
};
```

### 3.3 Duty Cycle

The receiver event loop follows the standard BRZ duty-cycle pattern: each call to
`doWork` performs a bounded amount of work and returns a count. The `ThreadRunner`
(doc 01, §5.3) drives the loop and applies the idle strategy when the work count is
zero.

```zig
/// One iteration of the receiver event loop.
/// Returns the total number of work items processed. Zero means the
/// idle strategy should engage.
pub fn doWork(self: *ReceiverEventLoop) u32 {
    var work_count: u32 = 0;
    const now_ns = platform.Clock.monotonicNanos();

    // ── Phase 1: Poll io_uring completions (received packets) ─────────
    // Process up to RECV_BATCH_LIMIT completions. Each completion
    // represents one received UDP packet. The callback parses the frame
    // type and dispatches to the appropriate handler.
    work_count += self.io_ring.pollCompletions(
        onPacketReceived,
        @ptrCast(self),
        constants.recv_batch_limit,
    );

    // ── Phase 2: Process inter-loop commands ──────────────────────────
    // The control loop may enqueue commands like "add peer", "remove peer",
    // or "shutdown". Process up to COMMAND_DRAIN_LIMIT per iteration.
    work_count += self.cmd_queue.read(
        dispatchCommand,
        constants.command_drain_limit,
    );

    // ── Phase 3: Scan for losses and queue NAK SQEs ───────────────────
    // Walk each peer's receive log buffer from rebuild_position to
    // tail_position. If a gap is found and the NAK timer has expired,
    // encode a NAK frame and queue a send SQE.
    var peer_idx: u8 = 0;
    while (peer_idx < self.peer_count) : (peer_idx += 1) {
        if (self.peers[peer_idx]) |peer| {
            const nak_work = peer.loss_detector.scan(peer.recv_log, now_ns);
            if (nak_work > 0) {
                self.queueNakFrame(peer);
                work_count += nak_work;
            }
        }
    }

    // ── Phase 4: Send Status Messages (rate-limited) ──────────────────
    // SMs are sent periodically (every 200ms) and eagerly when
    // consumption_position advances by >= window/4. See doc 07, §4.
    peer_idx = 0;
    while (peer_idx < self.peer_count) : (peer_idx += 1) {
        if (self.peers[peer_idx]) |peer| {
            const sm_sent = peer.sm_scheduler.maybeSendStatusMessage(
                &peer.flow_control,
                &peer.sm_buffer,
                now_ns,
                sendSmCallback,
                @ptrCast(self),
                peer,
            );
            if (sm_sent) work_count += 1;
        }
    }

    // ── Phase 5: Submit all queued io_uring SQEs ──────────────────────
    // All NAKs and SMs queued in phases 3-4 are submitted to the kernel
    // in a single io_uring_enter() call. This batching is the key
    // advantage of io_uring over individual sendmsg() calls.
    if (self.io_ring.pendingSubmissions() > 0) {
        const submitted = self.io_ring.submit() catch 0;
        work_count += submitted;
    }

    return work_count;
}
```

### 3.4 Event Loop Integration with ThreadRunner

The receiver event loop is wrapped in a `platform.EventLoop` and driven by a
`platform.ThreadRunner`:

```zig
const platform = @import("../platform.zig");

pub fn createReceiverRunner(
    receiver: *ReceiverEventLoop,
    idle_strategy: platform.IdleStrategy,
) platform.ThreadRunner {
    const event_loop = platform.EventLoop{
        .context = @ptrCast(receiver),
        .doWorkFn = @ptrCast(&ReceiverEventLoop.doWork),
        .onCloseFn = @ptrCast(&ReceiverEventLoop.onClose),
    };

    return platform.ThreadRunner.init(
        "receiver-event-loop",
        event_loop,
        idle_strategy,
    );
}
```

The thread is named `"receiver-event-loop"` via `setThreadName` (doc 01, §5.4) for
debuggability. In production, the idle strategy is typically `backoff` (spin → yield →
sleep) rather than `busy_spin`, since the receiver is I/O-bound and benefits from
yielding to the kernel's io_uring polling thread.

---

## 4. Packet Processing

### 4.1 Completion Callback

When `io_ring.pollCompletions()` fires the callback, the receiver decodes the
completion, extracts the received buffer, and dispatches by frame type:

```zig
/// Callback invoked for each io_uring CQE representing a received UDP packet.
fn onPacketReceived(ctx: *anyopaque, completion: NetworkIo.Completion) void {
    const self: *ReceiverEventLoop = @ptrCast(@alignCast(ctx));

    // Extract the receive buffer from the completion.
    const buf = completion.getRecvBuffer() orelse {
        // Zero-length or error — re-submit the recv SQE.
        self.io_ring.resubmitRecv(completion.buffer_index);
        self.counters.increment(.recv_errors);
        return;
    };

    if (buf.len < @sizeOf(frames.FrameHeader)) {
        // Runt packet — too small to contain even a frame header.
        self.io_ring.resubmitRecv(completion.buffer_index);
        self.counters.increment(.invalid_packets);
        return;
    }

    // Parse the frame header to determine the frame type.
    const parsed = frame_parser.parseFrame(buf) orelse {
        self.io_ring.resubmitRecv(completion.buffer_index);
        self.counters.increment(.invalid_packets);
        return;
    };

    switch (parsed) {
        .data => |header| self.handleDataFrame(header, buf),
        .setup => |setup| self.handleSetup(setup, completion.source_address),
        .sm => |sm| self.handleStatusMessage(sm),
        .nak => |nak| self.handleNak(nak),
        .pad => {},     // silently drop padding frames
        .unknown => {
            self.counters.increment(.invalid_packets);
        },
    }

    // Re-submit the recv buffer for the next packet.
    self.io_ring.resubmitRecv(completion.buffer_index);
}
```

**Critical detail — recv SQE resubmission:** After processing each received packet,
we immediately re-submit the buffer slot as a new recv SQE. This keeps the io_uring
receive pipeline full. The number of outstanding recv SQEs equals the number of
pre-allocated receive buffers (typically 64), ensuring the kernel always has buffers
available for incoming packets.

### 4.2 Data Frame Handling

Data frames are the primary frame type — they carry application messages between
brokers. Processing a data frame involves inserting it into the peer's receive log
and then either routing it immediately (unfragmented) or passing it to the fragment
assembler.

```zig
/// Handle a received DATA frame.
fn handleDataFrame(
    self: *ReceiverEventLoop,
    header: *const frames.DataFrameHeader,
    frame: []const u8,
) void {
    // ── Look up the peer ──────────────────────────────────────────────
    const peer = self.lookupPeer(header.source_node_id) orelse {
        // Unknown peer. This can happen if we receive data before the
        // SETUP handshake completes. Drop the frame.
        self.counters.increment(.unknown_peer_drops);
        return;
    };

    // ── Insert into receive log buffer ────────────────────────────────
    // This is always done, even for admin messages and fragments.
    // The receive log is the single source of truth for loss detection
    // and flow control — every frame must be recorded.
    insertPacket(peer.recv_log, frame);
    peer.last_packet_received_ns = platform.Clock.monotonicNanos();
    self.counters.add(.bytes_received, frame.len);

    // ── Check for heartbeat (zero-length data frame) ──────────────────
    // Heartbeats have frame_length == 40 (header only, no payload).
    // They update tail_position in the recv log but carry no message
    // to route. Their purpose is liveness detection and gap discovery
    // (the sequence_number helps the loss detector find gaps).
    if (header.frame_length == @sizeOf(frames.DataFrameHeader)) {
        self.counters.increment(.heartbeats_received);
        return;
    }

    // ── Check for admin/cluster message ───────────────────────────────
    // Admin messages (FLAG_ADMIN set) are handled by the broker's cluster
    // management subsystem, not routed to services.
    if (header.isAdmin()) {
        self.handleAdminMessage(header, frames.DataFrameHeader.payloadSlice(frame));
        return;
    }

    // ── Route or reassemble ───────────────────────────────────────────
    if (header.isUnfragmented()) {
        // Complete message — route directly to the target service.
        self.routeToService(
            header.target_service_id,
            header.source_node_id,
            header.source_service_id,
            frame,
            header.frame_length,
            header.sequence_number,
        );
    } else {
        // Fragmented message — pass to the assembler.
        self.handleFragment(header, frame);
    }
}
```

### 4.3 Frame Type Dispatch Summary

| Frame Type | Handler | Action |
|------------|---------|--------|
| `DATA` (unfragmented) | `handleDataFrame` → `routeToService` | Insert in recv log, route payload to target service ring buffer |
| `DATA` (fragment) | `handleDataFrame` → `handleFragment` | Insert in recv log, accumulate in `FragmentAssembler`, route on completion |
| `DATA` (heartbeat) | `handleDataFrame` | Insert in recv log (advances tail), update liveness timestamp, no routing |
| `DATA` (admin) | `handleDataFrame` → `handleAdminMessage` | Insert in recv log, dispatch to cluster management |
| `SETUP` | `handleSetup` | Allocate peer state, send initial Status Message |
| `SM` | `handleStatusMessage` | Forward to sender event loop via command queue |
| `NAK` | `handleNak` | Forward to sender event loop via command queue |
| `PAD` | (dropped) | Silently ignored |
| Unknown | (dropped) | Increment `invalid_packets` counter |

### 4.4 Status Message and NAK Forwarding

Status Messages and NAK frames are **not processed by the receiver** — they are
destined for the **sender event loop** (doc 05). The receiver forwards them via the
inter-loop command queue:

```zig
/// Forward a received Status Message to the sender event loop.
fn handleStatusMessage(self: *ReceiverEventLoop, sm: *const frames.StatusMessage) void {
    // Encode as a command: [cmd_type(i32)][node_id(u8)][pad(3)][consumption_pos(i64)][window(i32)]
    var cmd_buf: [20]u8 = undefined;
    std.mem.writeInt(i32, cmd_buf[0..4], CMD_TYPE_STATUS_MESSAGE, .little);
    cmd_buf[4] = sm.node_id;
    cmd_buf[5] = 0;
    cmd_buf[6] = 0;
    cmd_buf[7] = 0;
    std.mem.writeInt(i64, cmd_buf[8..16], sm.consumption_position, .little);
    std.mem.writeInt(i32, cmd_buf[16..20], sm.receiver_window, .little);

    // Best-effort enqueue — if the command queue is full, the SM is dropped.
    // The sender will eventually receive the next periodic SM.
    self.sender_cmd_queue.write(CMD_MSG_TYPE_CONTROL, &cmd_buf) catch {
        self.counters.increment(.command_queue_overflow);
    };
    self.counters.increment(.status_messages_received);
}

/// Forward a received NAK to the sender event loop.
fn handleNak(self: *ReceiverEventLoop, nak: *const frames.NakFrame) void {
    var cmd_buf: [16]u8 = undefined;
    std.mem.writeInt(i32, cmd_buf[0..4], CMD_TYPE_NAK, .little);
    cmd_buf[4] = nak.node_id;
    cmd_buf[5] = 0;
    cmd_buf[6] = 0;
    cmd_buf[7] = 0;
    std.mem.writeInt(i64, cmd_buf[8..16], nak.position, .little);

    self.sender_cmd_queue.write(CMD_MSG_TYPE_CONTROL, &cmd_buf) catch {
        self.counters.increment(.command_queue_overflow);
    };
    self.counters.increment(.naks_received);
}
```

The sender event loop reads these commands on its own duty cycle and processes them
(SM → update flow control state, NAK → retransmit from retransmit buffer). This
separation ensures neither event loop blocks the other.

---

## 5. Message Routing

When a complete message (unfragmented, or fully reassembled from fragments) is ready,
the receiver routes it to the target service's messages ring buffer.

### 5.1 Route to Service

**File: `src/receiver/message_router.zig`**

```zig
const std = @import("std");
const constants = @import("../platform/constants.zig");
const frames = @import("../protocol/frames.zig");
const RingBuffer = @import("../concurrent/ring_buffer.zig").RingBuffer;

/// Message type ID for application messages written to service ring buffers.
/// Must be >= 1 (ring buffer requires positive msg_type_id).
const msg_type_application: i32 = 1;

/// Route a complete message to the target service's messages ring buffer.
///
/// The frame includes the 40-byte DataFrameHeader. The header is preserved
/// in the ring buffer write so that the service can read routing fields
/// (source_node_id, source_service_id, correlation_id, template_id, etc.)
/// without a separate metadata channel.
///
/// If the target service is unknown or its ring buffer is full, the message
/// is NOT consumed from the receive log. This allows the consumption_position
/// to stall, shrinking the receiver window and applying back-pressure upstream
/// (see doc 07, §5).
pub fn routeToService(
    self: *ReceiverEventLoop,
    target_service_id: u16,
    source_node_id: u8,
    source_service_id: u16,
    frame: []const u8,
    frame_length: i32,
    position: i64,
) void {
    // ── Look up the target service ────────────────────────────────────
    const service = self.service_registry.lookup(target_service_id) orelse {
        // Unknown service — the service may have deregistered between
        // when the sender dispatched the message and now. Mark as
        // consumed to avoid stalling the entire recv log for one
        // unknown service.
        self.counters.increment(.unknown_service_drops);
        markFrameConsumed(self.currentPeerLog(), position, frame_length);
        return;
    };

    // ── Extract payload ───────────────────────────────────────────────
    // The payload starts after the 40-byte DataFrameHeader. For service
    // consumption, we write the FULL frame (header + payload) so the
    // service handler has access to routing metadata.
    const result = service.messages_ring_buffer.write(
        msg_type_application,
        frame[0..@as(usize, @intCast(frame_length))],
    );

    switch (result) {
        // Frame successfully written to the service's ring buffer.
        // Mark consumed in the recv log so consumption_position can advance.
        .success => {
            markFrameConsumed(self.currentPeerLog(), position, frame_length);
            self.counters.increment(.messages_routed);
        },

        // Service ring buffer is full — service is consuming too slowly.
        // DO NOT mark consumed. The frame stays in the recv log. The
        // consumption_position will NOT advance past this point, which
        // shrinks the receiver window on the next Status Message, which
        // slows down the sender. This is deliberate back-pressure.
        .buffer_full => {
            self.counters.increment(.service_back_pressure);
        },
    }
}
```

### 5.2 Frame Consumption Marking

When a frame has been successfully routed, we mark its slot in the receive log as
consumed by overwriting the length prefix with a sentinel value (`-1`). This allows
the consumption position scanner to advance past it.

```zig
/// Sentinel value written to the length prefix of a consumed frame.
/// The consumption position scanner (doc 07, §3.3) checks for this value
/// to know that a slot has been processed and can be counted as consumed.
const frame_consumed_marker: i32 = -1;

/// Mark a frame at the given position as consumed.
/// This overwrites the length prefix with the consumed marker (-1).
/// The aligned-length field (written during insertion) is preserved
/// so that the consumption position scanner can advance past it.
fn markFrameConsumed(recv_log: *ReceiveLogBuffer, position: i64, frame_length: i32) void {
    _ = frame_length;
    const index: usize = @intCast(@as(u64, @bitCast(position)) & recv_log.mask);
    const length_ptr: *align(1) volatile i32 = @ptrCast(&recv_log.data[index]);
    @atomicStore(i32, length_ptr, frame_consumed_marker, .release);
}
```

### 5.3 Service Registry

The `ServiceRegistry` is shared between the receiver event loop and the control plane
(doc 09). The control plane adds and removes entries as services register and
deregister. The receiver event loop only reads from it (never writes), so no
synchronization is needed beyond atomic pointer loads for the service entries
themselves.

```zig
/// Service registry — maps serviceId to service state.
/// Updated by the control loop via commands. Read by the receiver for routing.
pub const ServiceRegistry = struct {
    /// Fixed-size array indexed by service ID. Max 256 services per broker.
    entries: [constants.default_max_services]?ServiceEntry,

    pub const ServiceEntry = struct {
        service_id: u16,
        service_name: []const u8,
        node_id: u8,
        messages_ring_buffer: *RingBuffer,
    };

    /// Look up a service by ID. Returns null if the service is not registered.
    pub fn lookup(self: *const ServiceRegistry, service_id: u16) ?*const ServiceEntry {
        if (service_id >= constants.default_max_services) return null;
        const entry = &self.entries[service_id];
        return if (entry.* != null) &entry.*.? else null;
    }
};
```

### 5.4 Routing Flow Summary

```
                ┌─────────────────────────────────────────┐
                │         routeToService()                 │
                └────────────────────┬────────────────────┘
                                     │
                          ┌──────────▼──────────┐
                          │  service_registry    │
                          │  .lookup(svc_id)     │
                          └──────────┬──────────┘
                                     │
                       ┌─────────────┼─────────────┐
                       │             │             │
                 ┌─────▼────┐  ┌────▼─────┐  ┌────▼──────┐
                 │  Found   │  │  Not     │  │  Found    │
                 │  + write │  │  found   │  │  + write  │
                 │  succeeds│  │          │  │  fails    │
                 └─────┬────┘  └────┬─────┘  └────┬──────┘
                       │            │              │
              mark consumed   mark consumed   DO NOT mark
              counter: routed counter: drops  counter: backpressure
                       │            │              │
                       └────────────┴──────────────┘
                                     │
                          ┌──────────▼──────────┐
                          │  consumption_position│
                          │  advances only if    │
                          │  frame is consumed   │
                          └─────────────────────┘
```

---

## 6. Fragment Reassembly

When the sender fragments a message that exceeds the MTU, the receiver must
reassemble the fragments before routing the complete message to the target service.

### 6.1 Fragmentation Flags Recap

From doc 04, §2.3, the `flags` byte in the `DataFrameHeader` encodes fragmentation:

| flags & 0xC0 | Meaning |
|---------------|---------|
| `0xC0` (`BEGIN \| END`) | Unfragmented — complete message in one frame |
| `0x80` (`BEGIN`) | First fragment of a multi-frame message |
| `0x00` (neither) | Middle fragment |
| `0x40` (`END`) | Last fragment |

Fragments from the same message share the same `correlation_id` and arrive with
monotonically increasing `sequence_number` values. The receiver uses the correlation
ID to associate fragments and the sequence number to detect gaps.

### 6.2 FragmentAssembler

**File: `src/receiver/fragment_assembler.zig`**

One `FragmentAssembler` exists per unique source (keyed by the combination of
`source_node_id` and `source_service_id`). This means a single source can only have
one fragmented message in flight at a time — a simplification that avoids the
complexity of tracking multiple concurrent message assemblies from the same source.

```zig
const std = @import("std");
const constants = @import("../platform/constants.zig");
const frames = @import("../protocol/frames.zig");

/// Maximum reassembled message size. Messages larger than this are dropped.
/// This is a safety limit — in practice, messages rarely exceed a few hundred KB.
const MAX_REASSEMBLED_MESSAGE_SIZE: usize = 16 * 1024 * 1024; // 16 MiB

/// Assembles a fragmented message from multiple DATA frames.
///
/// The assembler is stateful: it accumulates fragment payloads in a pre-allocated
/// buffer and emits the complete message when the END fragment arrives. If
/// fragments arrive out of order or a gap is detected, the in-progress message
/// is discarded and reassembly restarts on the next BEGIN fragment.
pub const FragmentAssembler = struct {
    /// Growable assembly buffer. Allocated once at startup, grows as needed.
    /// This is the one place where the hot path MAY allocate (if a message
    /// exceeds the initial buffer capacity). In practice, the buffer is sized
    /// to handle the expected maximum message size, so allocations are rare.
    buffer: []u8,

    /// Current write position within the buffer.
    buffer_len: usize,

    /// Total buffer capacity.
    buffer_capacity: usize,

    /// The expected sequence_number of the next fragment.
    expected_sequence: i64,

    /// The correlation_id of the message currently being assembled.
    /// Used to detect interleaved messages from the same source (which
    /// would indicate a protocol error).
    active_correlation_id: i32,

    /// True if we are actively assembling a message (received BEGIN,
    /// waiting for more fragments or END).
    assembling: bool,

    /// Captured routing fields from the BEGIN fragment's header.
    /// These are used when routing the reassembled message.
    source_node_id: u8,
    source_service_id: u16,
    target_service_id: u16,
    template_id: u16,

    /// Key for this assembler: (source_node_id << 16) | source_service_id.
    key: u32,

    const Self = @This();

    /// Initialize a fragment assembler with pre-allocated buffer.
    pub fn init(initial_capacity: usize, key: u32) Self {
        // Pre-allocate using page_allocator — this is startup, not hot path.
        const buf = std.heap.page_allocator.alloc(u8, initial_capacity) catch
            @panic("failed to allocate fragment assembler buffer");

        return .{
            .buffer = buf,
            .buffer_len = 0,
            .buffer_capacity = initial_capacity,
            .expected_sequence = 0,
            .active_correlation_id = 0,
            .assembling = false,
            .source_node_id = 0,
            .source_service_id = 0,
            .target_service_id = 0,
            .template_id = 0,
            .key = key,
        };
    }

    /// Process a fragment. Returns the complete reassembled message payload
    /// if this was the final fragment, or null if more fragments are expected.
    ///
    /// If the fragment is out of order or belongs to a different message than
    /// the one being assembled, the in-progress assembly is discarded.
    pub fn onFragment(
        self: *Self,
        header: *const frames.DataFrameHeader,
        frame: []const u8,
    ) ?[]const u8 {
        const payload = frames.DataFrameHeader.payloadSlice(frame);

        // ── BEGIN fragment ────────────────────────────────────────────
        if (header.isBegin()) {
            // Start a new assembly, discarding any in-progress message.
            self.buffer_len = 0;
            self.assembling = true;
            self.active_correlation_id = header.correlation_id;
            self.expected_sequence = header.sequence_number;
            self.source_node_id = header.source_node_id;
            self.source_service_id = header.source_service_id;
            self.target_service_id = header.target_service_id;
            self.template_id = header.template_id;

            if (!self.appendPayload(payload)) {
                self.reset();
                return null;
            }

            // Advance expected sequence by the aligned frame size.
            self.advanceExpectedSequence(header.frame_length);

            // BEGIN+END = unfragmented message that shouldn't reach here,
            // but handle it gracefully.
            if (header.isEnd()) {
                self.assembling = false;
                return self.buffer[0..self.buffer_len];
            }

            return null;
        }

        // ── Not a BEGIN fragment — must be in active assembly ─────────
        if (!self.assembling) {
            // Received a middle or end fragment without a preceding BEGIN.
            // This can happen after packet loss. Discard.
            return null;
        }

        // ── Sequence check ────────────────────────────────────────────
        if (header.sequence_number != self.expected_sequence) {
            // Out-of-order or gap. The loss detector will send a NAK and
            // the sender will retransmit. Meanwhile, discard the partial
            // assembly — we'll start fresh from the next BEGIN.
            self.reset();
            return null;
        }

        // ── Correlation ID check ──────────────────────────────────────
        if (header.correlation_id != self.active_correlation_id) {
            // Interleaved message from the same source. Protocol violation.
            self.reset();
            return null;
        }

        // ── Append payload ────────────────────────────────────────────
        if (!self.appendPayload(payload)) {
            self.reset();
            return null;
        }

        self.advanceExpectedSequence(header.frame_length);

        // ── END fragment ──────────────────────────────────────────────
        if (header.isEnd()) {
            self.assembling = false;
            return self.buffer[0..self.buffer_len];
        }

        // More fragments expected.
        return null;
    }

    /// Append payload bytes to the assembly buffer. Returns false if the
    /// message exceeds the maximum size limit.
    fn appendPayload(self: *Self, payload: []const u8) bool {
        const new_len = self.buffer_len + payload.len;

        if (new_len > MAX_REASSEMBLED_MESSAGE_SIZE) return false;

        // Grow buffer if necessary (rare — only on first large message).
        if (new_len > self.buffer_capacity) {
            const new_capacity = std.math.ceilPowerOfTwo(usize, new_len) catch
                return false;
            const new_buf = std.heap.page_allocator.realloc(
                self.buffer,
                new_capacity,
            ) catch return false;
            self.buffer = new_buf;
            self.buffer_capacity = new_capacity;
        }

        @memcpy(self.buffer[self.buffer_len..][0..payload.len], payload);
        self.buffer_len = new_len;
        return true;
    }

    /// Calculate the next expected sequence number by advancing past the
    /// current frame's aligned slot in the send buffer.
    fn advanceExpectedSequence(self: *Self, frame_length: i32) void {
        const aligned = constants.alignUp(
            @as(usize, @intCast(frame_length)),
            32, // frame alignment in the send buffer
        );
        self.expected_sequence += @as(i64, @intCast(aligned));
    }

    /// Discard in-progress assembly.
    fn reset(self: *Self) void {
        self.buffer_len = 0;
        self.assembling = false;
        self.active_correlation_id = 0;
        self.expected_sequence = 0;
    }

    /// Release the assembly buffer.
    pub fn deinit(self: *Self) void {
        std.heap.page_allocator.free(self.buffer);
        self.* = undefined;
    }
};
```

### 6.3 Fragment Handling in the Event Loop

```zig
/// Handle a fragmented data frame.
fn handleFragment(
    self: *ReceiverEventLoop,
    header: *const frames.DataFrameHeader,
    frame: []const u8,
) void {
    // Look up or create the assembler for this source.
    const assembler_key: u32 =
        (@as(u32, header.source_node_id) << 16) | @as(u32, header.source_service_id);

    const assembler = self.getOrCreateAssembler(assembler_key);

    // Process the fragment.
    if (assembler.onFragment(header, frame)) |reassembled_payload| {
        // Reassembly complete — route the full message.
        // For reassembled messages, we construct a synthetic frame with the
        // routing metadata from the BEGIN fragment header.
        self.routeReassembledMessage(
            assembler.target_service_id,
            assembler.source_node_id,
            assembler.source_service_id,
            assembler.template_id,
            header.correlation_id,
            reassembled_payload,
        );
        self.counters.increment(.fragments_reassembled);
    }
}

/// Route a reassembled message to the target service.
/// Since the original DataFrameHeader is spread across multiple fragments,
/// we write a synthetic header + the reassembled payload into the service's
/// ring buffer.
fn routeReassembledMessage(
    self: *ReceiverEventLoop,
    target_service_id: u16,
    source_node_id: u8,
    source_service_id: u16,
    template_id: u16,
    correlation_id: i32,
    payload: []const u8,
) void {
    const service = self.service_registry.lookup(target_service_id) orelse {
        self.counters.increment(.unknown_service_drops);
        return;
    };

    // Build a synthetic frame: DataFrameHeader + reassembled payload.
    // Use the try-claim API for zero-copy: claim space in the ring buffer,
    // write the header and payload directly, then commit.
    const total_length = @sizeOf(frames.DataFrameHeader) + payload.len;

    const claim = service.messages_ring_buffer.tryClaim(@intCast(total_length)) orelse {
        self.counters.increment(.service_back_pressure);
        return;
    };

    // Write synthetic header.
    const header_buf = claim.buffer[0..@sizeOf(frames.DataFrameHeader)];
    const synth_header: *frames.DataFrameHeader = @ptrCast(@alignCast(header_buf.ptr));
    synth_header.* = .{
        .frame_length = @intCast(total_length),
        .flags = constants.flag_unfragmented, // reassembled = unfragmented from service's perspective
        .source_node_id = source_node_id,
        .source_service_id = source_service_id,
        .target_service_id = target_service_id,
        .template_id = template_id,
        .correlation_id = correlation_id,
    };

    // Write reassembled payload.
    @memcpy(claim.buffer[@sizeOf(frames.DataFrameHeader)..][0..payload.len], payload);

    // Commit — makes the message visible to the service consumer.
    claim.commit();
    self.counters.increment(.messages_routed);
}
```

### 6.4 Assembler Lifecycle

Fragment assemblers are pre-allocated at startup in a fixed-size array. When a new
source is encountered, the next available slot is used. Assemblers are reused across
messages — after a message is fully reassembled, the same assembler handles the next
fragmented message from that source.

Assemblers are evicted when a peer disconnects (all assemblers keyed to that peer's
`source_node_id` are reset) or when the array is full and a new source appears (LRU
eviction based on `last_packet_received_ns`).

---

## 7. Loss Detection & NAK Generation

The loss detector scans each peer's receive log buffer for gaps — positions where the
frame length is zero (slot empty) between the `rebuild_position` and `tail_position`.
When a gap is found and persists beyond the NAK delay, a NAK frame is sent to the
peer requesting retransmission.

### 7.1 LossDetector

**File: `src/receiver/loss_detector.zig`**

```zig
const std = @import("std");
const constants = @import("../platform/constants.zig");
const ReceiveLogBuffer = @import("../memory/receive_log.zig").ReceiveLogBuffer;

/// Detects gaps in the receive log buffer and generates NAK requests.
///
/// The detector tracks one active gap at a time (the earliest gap from
/// rebuild_position). This is a deliberate simplification: NAKing the
/// earliest gap first ensures that retransmitted frames fill in from the
/// bottom, allowing consumption_position to advance. Tracking multiple
/// gaps adds complexity with marginal benefit — once the first gap is
/// filled, the next scan will find the next gap.
pub const LossDetector = struct {
    /// Position of the currently tracked gap (if any).
    active_gap_position: i64,

    /// Length of the currently tracked gap in bytes.
    active_gap_length: i32,

    /// Timestamp (monotonic ns) at which we will send the NAK for the
    /// active gap. Set to `now + NAK_INITIAL_DELAY_NS` when a new gap
    /// is first detected. Reset to `now + NAK_RETRY_DELAY_NS` after
    /// each NAK is sent.
    nak_expiry_ns: i64,

    /// True if there is an active gap being tracked.
    has_active_gap: bool,

    /// The highest position that has been verified as contiguous.
    /// This is the loss detector's own cursor — it only advances when
    /// all frames up to this point are present.
    rebuild_position: i64,

    const Self = @This();

    pub fn init(initial_position: i64) Self {
        return .{
            .active_gap_position = 0,
            .active_gap_length = 0,
            .nak_expiry_ns = 0,
            .has_active_gap = false,
            .rebuild_position = initial_position,
        };
    }

    /// Scan the receive log buffer for gaps between rebuild_position and
    /// tail_position. If a gap is found and the NAK timer has expired,
    /// returns 1 (indicating a NAK should be sent). Otherwise returns 0.
    ///
    /// This function also advances rebuild_position past contiguous
    /// committed frames, which is an important secondary effect: the
    /// rebuild_position is used by the receiver window calculation to
    /// determine how much data is "in flight" vs. "fully received".
    pub fn scan(self: *Self, log: *ReceiveLogBuffer, now_ns: i64) u32 {
        const hwm = log.loadTailPosition();

        // Nothing to scan — tail hasn't advanced beyond our cursor.
        if (self.rebuild_position >= hwm) return 0;

        const mask = log.mask;
        var pos = self.rebuild_position;

        // ── Advance through contiguous committed frames ───────────────
        while (pos < hwm) {
            const index: usize = @intCast(@as(u64, @bitCast(pos)) & mask);
            const length_ptr: *const align(1) volatile i32 = @ptrCast(&log.data[index]);
            const frame_length = @atomicLoad(i32, length_ptr, .acquire);

            if (frame_length <= 0 and frame_length != frame_consumed_marker) {
                // ── Gap found (length == 0) ───────────────────────────
                // The frame at this position has not been received.
                break;
            }

            // Frame is present (length > 0) or consumed (length == -1).
            // Read the aligned slot size to advance past it.
            const aligned_len = readAlignedFrameLength(log, index);
            if (aligned_len <= 0) break; // safety: corrupted slot

            pos += @as(i64, aligned_len);
        }

        // Update rebuild_position — everything up to `pos` is contiguous.
        if (pos > self.rebuild_position) {
            self.rebuild_position = pos;
            log.storeRebuildPosition(pos);

            // If we advanced past the active gap, clear it.
            if (self.has_active_gap and pos > self.active_gap_position) {
                self.has_active_gap = false;
            }
        }

        // If we've caught up to the high-water mark, no gap.
        if (pos >= hwm) return 0;

        // ── A gap exists at `pos` ─────────────────────────────────────
        // Find the end of the gap (scan forward until a committed frame).
        const gap_start = pos;
        var gap_end = pos;
        while (gap_end < hwm) {
            const idx: usize = @intCast(@as(u64, @bitCast(gap_end)) & mask);
            const lp: *const align(1) volatile i32 = @ptrCast(&log.data[idx]);
            const fl = @atomicLoad(i32, lp, .acquire);

            if (fl > 0 or fl == frame_consumed_marker) break; // end of gap

            // Advance by one frame alignment — we don't know the actual
            // frame size for the missing slot, so we advance by the
            // minimum alignment.
            gap_end += @as(i64, @intCast(frame_alignment));
        }

        const gap_length: i32 = @intCast(gap_end - gap_start);

        // ── Track the gap ─────────────────────────────────────────────
        if (!self.has_active_gap or self.active_gap_position != gap_start) {
            // New gap (or the gap moved). Set the initial NAK delay.
            // The delay allows for packet reordering on the network —
            // if the missing packet is just slightly delayed, it may
            // arrive before the NAK timer fires, avoiding an unnecessary
            // retransmit.
            self.active_gap_position = gap_start;
            self.active_gap_length = gap_length;
            self.has_active_gap = true;
            self.nak_expiry_ns = now_ns + constants.nak_initial_delay_ns;
            return 0; // don't NAK yet — wait for the delay
        }

        // ── Check NAK timer ───────────────────────────────────────────
        if (now_ns < self.nak_expiry_ns) return 0; // not yet

        // Timer expired — update the gap length (it may have grown or
        // partially filled since we started tracking it) and signal
        // that a NAK should be sent.
        self.active_gap_length = gap_length;
        self.nak_expiry_ns = now_ns + constants.nak_retry_delay_ns;

        return 1; // caller should send a NAK
    }

    /// Returns the position and length of the current active gap.
    /// Only valid when scan() returned > 0.
    pub fn activeGap(self: *const Self) struct { position: i64, length: i32 } {
        return .{
            .position = self.active_gap_position,
            .length = self.active_gap_length,
        };
    }

    /// Sentinel value for consumed frames (must match the router's marker).
    const frame_consumed_marker: i32 = -1;

    /// Minimum frame alignment in the receive log.
    const frame_alignment: usize = 32;
};

/// Read the aligned frame length from the secondary field in the receive log.
/// This field is written during insertion (§2.2) at a fixed offset within each
/// slot and is NOT overwritten when the frame is marked as consumed.
fn readAlignedFrameLength(log: *const ReceiveLogBuffer, slot_index: usize) i32 {
    // The aligned-length field is at offset +8 within the slot
    // (after the 4-byte length prefix + 4-byte frame_length field
    // of the DataFrameHeader).
    const offset = slot_index + @sizeOf(i32) + @sizeOf(i32);
    const ptr: *const align(1) volatile i32 = @ptrCast(&log.data[offset]);
    return @atomicLoad(i32, ptr, .acquire);
}
```

### 7.2 NAK Timing

| Parameter | Value | Purpose |
|-----------|-------|---------|
| `nak_initial_delay_ns` | 60 ms | Time to wait after first detecting a gap before sending a NAK. Allows for packet reordering on the network. |
| `nak_retry_delay_ns` | 60 ms | Time between subsequent NAK retries for the same gap. If the retransmit doesn't arrive within this window, we NAK again. |

The 60ms initial delay is a trade-off:
- **Too short** → false NAKs for packets that were merely reordered, causing unnecessary
  retransmits that waste bandwidth.
- **Too long** → real losses take longer to recover, increasing latency for messages
  that depend on the missing packet.

60ms is chosen as a reasonable default for same-datacenter links (where reordering
windows are typically < 10ms). For cross-datacenter deployments, this should be tuned
upward via configuration.

### 7.3 NAK Encoding and Queuing

When the loss detector signals that a NAK should be sent, the receiver event loop
encodes a `NakFrame` and queues it as a send SQE:

```zig
/// Encode and queue a NAK frame for the given peer.
fn queueNakFrame(self: *ReceiverEventLoop, peer: *PeerReceiver) void {
    const gap = peer.loss_detector.activeGap();

    // Encode the NAK directly into the peer's pre-allocated buffer.
    const nak: *frames.NakFrame = @ptrCast(@alignCast(&peer.nak_buffer));
    nak.* = .{
        .frame_length = @sizeOf(frames.NakFrame),
        .frame_type = @intFromEnum(frames.FrameType.nak),
        .node_id = self.local_node_id,
        .position = gap.position,
        .length = gap.length,
    };

    // Queue a send SQE. The actual send happens in phase 5 (submit).
    self.io_ring.prepareSend(
        self.socket_fd,
        &peer.nak_buffer,
        peer.address,
    );

    self.counters.increment(.naks_sent);
}
```

### 7.4 Single-Gap Tracking Rationale

The loss detector only tracks **one gap at a time** (the earliest). This is a
deliberate simplification:

1. **Ordering:** By always NAKing the earliest gap first, retransmitted frames fill in
   from the bottom, allowing `consumption_position` to advance as soon as possible.
   Advancing `consumption_position` opens the receiver window and sends back-pressure
   relief to the sender.

2. **Simplicity:** Tracking multiple gaps requires a gap list, gap merging when adjacent
   gaps are filled, and more complex NAK scheduling. For the typical case (one or two
   lost packets per RTT), single-gap tracking performs equivalently.

3. **Cascading recovery:** After the first gap is filled, the next `scan()` call
   immediately finds the next gap (if any) and starts the NAK timer. The total recovery
   time for N gaps is N × `nak_initial_delay_ns` in the worst case, but in practice
   the retransmit for gap 1 often arrives within a few milliseconds, leaving plenty of
   time to NAK gap 2 before the sender's retransmit buffer expires.

---

## 8. Status Messages

Status Messages are sent from the receiver to the sender for flow control. They carry
the receiver's `consumption_position` and `receiver_window`, which the sender uses to
update its `send_limit`.

The full Status Message encoding, timing, and semantics are defined in
[doc 07 — Flow Control](07-flow-control.md), §3–4. This section covers the receiver
event loop's integration with the SM subsystem.

### 8.1 SM Triggers

Status Messages are sent under four conditions:

| Trigger | Timing | Purpose |
|---------|--------|---------|
| **Periodic** | Every 200 ms (`sm_timeout_ns`) | Heartbeat — ensures the sender always has a recent view of the receiver window, even if no data is flowing |
| **Eager (consumption advance)** | When `consumption_position` advances by ≥ `last_sm_receiver_window / 4` | Opens the sender's window quickly when the receiver is keeping up |
| **Connection establishment** | Immediately after receiving a SETUP frame | Grants the sender its initial window so it can start sending |
| **Loss detection** | When a gap is detected (with `SEND_SETUP` flag) | Hints to the sender that the receiver may need re-synchronization |

### 8.2 Periodic and Eager SM Integration

The `StatusMessageScheduler` (doc 07, §4.4) encapsulates the timing logic. The
receiver event loop calls it once per peer per duty cycle iteration:

```zig
/// Callback invoked by StatusMessageScheduler when an SM should be sent.
fn sendSmCallback(ctx: *anyopaque, sm_buf: []const u8, peer: *PeerReceiver) void {
    const self: *ReceiverEventLoop = @ptrCast(@alignCast(ctx));

    // Queue a send SQE for the Status Message.
    self.io_ring.prepareSend(
        self.socket_fd,
        sm_buf,
        peer.address,
    );

    self.counters.increment(.status_messages_sent);
}
```

### 8.3 Receiver Window Calculation

The receiver window is calculated by `ReceiverFlowControl.calculateReceiverWindow()`
(doc 07, §3.1). The key formula:

```
available = log_capacity - (tail_position - consumption_position)
window    = min(available, log_capacity / 2)
```

The `log_capacity / 2` cap is a safety margin: even if the sender fills its entire
grant, the receiver still has half the buffer for concurrent consumption. Without the
cap, a burst of sends could fill the entire log buffer before the receiver processes
any data, causing overwrites.

### 8.4 Initial Status Message

When a SETUP frame is received (§9), the receiver sends an immediate SM with the
full initial window:

```zig
fn sendInitialStatusMessage(self: *ReceiverEventLoop, peer: *PeerReceiver) void {
    const initial_window = peer.flow_control.initialWindow();

    const sm: *frames.StatusMessage = @ptrCast(@alignCast(&peer.sm_buffer));
    sm.* = .{
        .frame_length = @sizeOf(frames.StatusMessage),
        .frame_type = @intFromEnum(frames.FrameType.sm),
        .node_id = self.local_node_id,
        .consumption_position = 0,
        .receiver_window = initial_window,
    };

    self.io_ring.prepareSend(
        self.socket_fd,
        &peer.sm_buffer,
        peer.address,
    );

    peer.flow_control.last_sm_consumption_position = 0;
    peer.flow_control.last_sm_receiver_window = initial_window;
    peer.flow_control.last_sm_sent_ns = platform.Clock.monotonicNanos();

    self.counters.increment(.status_messages_sent);
}
```

---

## 9. Connection Handling (SETUP)

When a SETUP frame is received from a new peer, the receiver allocates per-peer state
and initiates the flow control handshake.

### 9.1 SETUP Frame Recap

From doc 04, §2.4:

```
SetupFrame (24 bytes):
  frame_header     (8 bytes)
  source_node_id   (u8)
  reserved         (3 bytes)
  log_buffer_length (i32)   — sender's send buffer capacity
  mtu_length        (i32)   — sender's MTU
  initial_sequence  (i32)   — starting sequence number
```

### 9.2 Handler

```zig
/// Handle a SETUP frame from a new or reconnecting peer.
fn handleSetup(
    self: *ReceiverEventLoop,
    setup: *const frames.SetupFrame,
    src_addr: std.net.Address,
) void {
    const source_node_id = setup.source_node_id;

    // ── Check for existing peer ───────────────────────────────────────
    if (self.lookupPeer(source_node_id)) |existing_peer| {
        // Peer reconnect — reset state for the new connection.
        // This handles the case where a peer broker restarts: the old
        // receive log may contain stale data from the previous session.
        existing_peer.recv_log.reset();
        existing_peer.loss_detector = LossDetector.init(setup.initial_sequence);
        existing_peer.flow_control.reset();
        existing_peer.address = src_addr;
        existing_peer.last_packet_received_ns = platform.Clock.monotonicNanos();
        existing_peer.initial_sequence = setup.initial_sequence;

        // Reset all fragment assemblers keyed to this peer.
        self.resetAssemblersForPeer(source_node_id);

        // Send initial Status Message to grant the sender a window.
        self.sendInitialStatusMessage(existing_peer);

        self.counters.increment(.peer_reconnects);
        return;
    }

    // ── New peer — allocate state ─────────────────────────────────────
    const recv_log = ReceiveLogBuffer.allocate(
        constants.default_recv_log_buffer_length,
    ) catch {
        self.counters.increment(.peer_allocation_failures);
        return;
    };

    const peer = self.allocatePeer() orelse {
        // Max peers reached. Drop the SETUP.
        recv_log.close();
        self.counters.increment(.peer_allocation_failures);
        return;
    };

    peer.* = PeerReceiver.init(
        source_node_id,
        recv_log,
        src_addr,
        setup.initial_sequence,
    );

    // ── Send initial Status Message ───────────────────────────────────
    self.sendInitialStatusMessage(peer);

    // ── Notify control loop of new peer ───────────────────────────────
    var cmd_buf: [8]u8 = undefined;
    std.mem.writeInt(i32, cmd_buf[0..4], CMD_TYPE_PEER_CONNECTED, .little);
    cmd_buf[4] = source_node_id;
    cmd_buf[5] = 0;
    cmd_buf[6] = 0;
    cmd_buf[7] = 0;

    self.control_cmd_queue.write(CMD_MSG_TYPE_CONTROL, &cmd_buf) catch {
        self.counters.increment(.command_queue_overflow);
    };

    self.counters.increment(.peer_connections);
}
```

### 9.3 Connection Establishment Sequence

```
Peer Sender                                          Local Receiver
    │                                                      │
    │  ─── SETUP (source_node_id, mtu, initial_seq) ────► │
    │                                                      │
    │                                  allocate PeerReceiver│
    │                                  allocate ReceiveLogBuffer
    │                                  init LossDetector    │
    │                                  init ReceiverFlowControl
    │                                                      │
    │  ◄── Status Message (consumption=0, window=cap/2) ── │
    │                                                      │
    │  sender updates send_limit                           │
    │  sender begins sending DATA frames                   │
    │                                                      │
    │  ─── DATA (seq=0, payload=...) ────────────────────► │
    │  ─── DATA (seq=N, payload=...) ────────────────────► │
    │                                                      │
    │                                  insert into recv log │
    │                                  route to service RB  │
    │                                  consumption advances │
    │                                                      │
    │  ◄── Status Message (consumption=M, window=W) ────── │
    │                                                      │
    │  ... steady state ...                                │
```

### 9.4 Reconnection

When a peer restarts, it sends a new SETUP with a potentially different
`initial_sequence`. The receiver detects this because a `PeerReceiver` already exists
for that `source_node_id`. The handler resets all per-peer state:

- The receive log buffer is zeroed (`recv_log.reset()`).
- The loss detector is re-initialized with the new `initial_sequence`.
- Flow control is reset to its initial state.
- Fragment assemblers for that peer are discarded.
- A fresh initial Status Message is sent.

This ensures that stale data from the previous session doesn't corrupt the new
connection.

---

## 10. Admin Message Handling

Admin messages are data frames with the `FLAG_ADMIN` bit set. They carry cluster
management payloads (leader election, state synchronization, member join/leave)
between brokers. The receiver extracts the payload and forwards it to the cluster
management subsystem (doc 11).

```zig
/// Handle an admin (cluster management) message.
fn handleAdminMessage(
    self: *ReceiverEventLoop,
    header: *const frames.DataFrameHeader,
    payload: []const u8,
) void {
    // Admin messages are routed to the broker's own control ring buffer
    // (service_id = 0, the broker's well-known ID) for processing by
    // the control loop.
    const broker_service = self.service_registry.lookup(constants.broker_service_id) orelse {
        // Broker service not registered — this shouldn't happen but
        // handle it gracefully.
        self.counters.increment(.admin_message_errors);
        return;
    };

    // Write the full frame (header + payload) to the broker's control
    // ring buffer. The control loop will parse the template_id to
    // dispatch to the appropriate handler.
    const frame_data = @as([*]const u8, @ptrCast(header))[0..@as(usize, @intCast(header.frame_length))];
    broker_service.messages_ring_buffer.write(
        constants.msg_type_admin,
        frame_data,
    ) catch {
        self.counters.increment(.admin_message_errors);
    };

    self.counters.increment(.admin_messages_received);
}
```

---

## 11. Counters & Observability

The receiver event loop maintains counters for monitoring and debugging. All counters
are managed through the `CountersManager` (doc 03, §2) and can be read by external
monitoring tools.

### 11.1 Counter Definitions

```zig
/// Counter IDs for the receiver event loop.
pub const ReceiverCounterId = enum(u16) {
    /// Total bytes received from all peers (UDP payload, including headers).
    bytes_received = 100,

    /// Number of complete messages successfully routed to service ring buffers.
    messages_routed = 101,

    /// Number of packets dropped because the target service ID is unknown.
    unknown_service_drops = 102,

    /// Number of messages dropped because the target service's ring buffer is full.
    service_back_pressure = 103,

    /// Number of NAK frames sent to peers requesting retransmission.
    naks_sent = 104,

    /// Number of NAK frames received from peers (forwarded to sender event loop).
    naks_received = 105,

    /// Number of Status Messages sent to peers.
    status_messages_sent = 106,

    /// Number of Status Messages received from peers (forwarded to sender).
    status_messages_received = 107,

    /// Number of heartbeat frames received (zero-length data frames).
    heartbeats_received = 108,

    /// Number of packets dropped because the source peer is unknown.
    unknown_peer_drops = 109,

    /// Number of packets that failed to parse (too small, invalid frame type).
    invalid_packets = 110,

    /// Number of recv SQE errors (io_uring completion with negative result).
    recv_errors = 111,

    /// Number of new peer connections established via SETUP.
    peer_connections = 112,

    /// Number of peer reconnections (SETUP from an already-known peer).
    peer_reconnects = 113,

    /// Number of failed peer allocations (max peers reached or OOM).
    peer_allocation_failures = 114,

    /// Number of inter-loop command queue overflows (SM/NAK forwarding dropped).
    command_queue_overflow = 115,

    /// Number of fragmented messages successfully reassembled.
    fragments_reassembled = 116,

    /// Number of admin messages received and forwarded to the control loop.
    admin_messages_received = 117,

    /// Number of admin message forwarding failures.
    admin_message_errors = 118,
};
```

### 11.2 Key Diagnostic Relationships

| Symptom | Counter to check | Likely cause |
|---------|-----------------|--------------|
| Messages not reaching services | `unknown_service_drops` high | Service deregistered or wrong target_service_id |
| High latency | `service_back_pressure` high | Target service consuming too slowly |
| High latency | `naks_sent` high | Packet loss on the network link |
| No data flowing | `peer_connections` = 0 | SETUP handshake failed or never initiated |
| Peer appears dead | `heartbeats_received` not advancing | Network partition or peer process crashed |
| Memory growth | `fragments_reassembled` low but messages large | Fragment assembler buffer growing due to very large messages |

---

## 12. Testing

### 12.1 Unit Test: Packet Insertion

```zig
const std = @import("std");
const testing = std.testing;
const ReceiveLogBuffer = @import("../memory/receive_log.zig").ReceiveLogBuffer;
const receiver = @import("receive_log_buffer.zig");
const frames = @import("../protocol/frames.zig");
const constants = @import("../platform/constants.zig");

test "insertPacket writes frame and advances tail" {
    // Given
    var log = try ReceiveLogBuffer.allocate(4096);
    defer log.close();

    var frame_buf: [128]u8 = [_]u8{0} ** 128;
    const header: *frames.DataFrameHeader = @ptrCast(@alignCast(&frame_buf));
    header.* = .{
        .frame_length = 80, // 40-byte header + 40-byte payload
        .flags = constants.flag_unfragmented,
        .sequence_number = 0,
        .source_node_id = 1,
        .target_service_id = 5,
    };

    // When
    receiver.insertPacket(&log, frame_buf[0..80]);

    // Then
    const tail = log.loadTailPosition();
    try testing.expect(tail > 0);
    // Tail should be aligned to 32 bytes: 4 (length prefix) + 80 (frame) = 84 → aligned to 96
    try testing.expectEqual(@as(i64, 96), tail);

    // Verify the frame is readable
    const read_frame = log.readFrame(0);
    try testing.expect(read_frame != null);
    try testing.expectEqual(@as(usize, 80), read_frame.?.len);
}

test "insertPacket is idempotent — retransmit is skipped" {
    // Given
    var log = try ReceiveLogBuffer.allocate(4096);
    defer log.close();

    var frame_buf: [128]u8 = [_]u8{0} ** 128;
    const header: *frames.DataFrameHeader = @ptrCast(@alignCast(&frame_buf));
    header.* = .{
        .frame_length = 80,
        .flags = constants.flag_unfragmented,
        .sequence_number = 0,
    };

    // When — insert twice at the same position
    receiver.insertPacket(&log, frame_buf[0..80]);
    const tail_after_first = log.loadTailPosition();

    receiver.insertPacket(&log, frame_buf[0..80]);
    const tail_after_second = log.loadTailPosition();

    // Then — tail should not advance on retransmit
    try testing.expectEqual(tail_after_first, tail_after_second);
}

test "insertPacket handles out-of-order packets" {
    // Given
    var log = try ReceiveLogBuffer.allocate(4096);
    defer log.close();

    // Insert packet at sequence 96 first (out of order)
    var frame2_buf: [128]u8 = [_]u8{0} ** 128;
    const header2: *frames.DataFrameHeader = @ptrCast(@alignCast(&frame2_buf));
    header2.* = .{
        .frame_length = 80,
        .flags = constants.flag_unfragmented,
        .sequence_number = 96,
    };
    receiver.insertPacket(&log, frame2_buf[0..80]);

    // Then — tail should jump to position after packet 2
    const tail_after_second = log.loadTailPosition();
    try testing.expectEqual(@as(i64, 192), tail_after_second);

    // Insert packet at sequence 0 (filling the gap)
    var frame1_buf: [128]u8 = [_]u8{0} ** 128;
    const header1: *frames.DataFrameHeader = @ptrCast(@alignCast(&frame1_buf));
    header1.* = .{
        .frame_length = 80,
        .flags = constants.flag_unfragmented,
        .sequence_number = 0,
    };
    receiver.insertPacket(&log, frame1_buf[0..80]);

    // Then — tail should NOT decrease
    const tail_after_first = log.loadTailPosition();
    try testing.expectEqual(@as(i64, 192), tail_after_first);

    // Both frames should be readable
    try testing.expect(log.readFrame(0) != null);
    try testing.expect(log.readFrame(96) != null);
}
```

### 12.2 Unit Test: Loss Detection

```zig
const LossDetector = @import("loss_detector.zig").LossDetector;

test "scan detects gap and returns 1 after initial delay" {
    // Given
    var log = try ReceiveLogBuffer.allocate(4096);
    defer log.close();

    var detector = LossDetector.init(0);

    // Insert a packet at position 96 (skipping position 0 — gap!)
    var frame_buf: [128]u8 = [_]u8{0} ** 128;
    const header: *frames.DataFrameHeader = @ptrCast(@alignCast(&frame_buf));
    header.* = .{
        .frame_length = 80,
        .flags = constants.flag_unfragmented,
        .sequence_number = 96,
    };
    receiver.insertPacket(&log, frame_buf[0..80]);

    // When — first scan at t=0
    const now_ns: i64 = 1_000_000_000; // 1 second
    const result1 = detector.scan(&log, now_ns);

    // Then — gap detected, but NAK not yet due (initial delay)
    try testing.expectEqual(@as(u32, 0), result1);
    try testing.expect(detector.has_active_gap);
    try testing.expectEqual(@as(i64, 0), detector.active_gap_position);

    // When — scan again after initial delay (60ms)
    const result2 = detector.scan(&log, now_ns + 61 * std.time.ns_per_ms);

    // Then — NAK should fire
    try testing.expectEqual(@as(u32, 1), result2);
}

test "scan advances rebuild_position through contiguous frames" {
    // Given
    var log = try ReceiveLogBuffer.allocate(4096);
    defer log.close();

    var detector = LossDetector.init(0);

    // Insert contiguous packets at positions 0 and 96
    var frame_buf: [128]u8 = [_]u8{0} ** 128;
    const header: *frames.DataFrameHeader = @ptrCast(@alignCast(&frame_buf));

    header.* = .{ .frame_length = 80, .flags = constants.flag_unfragmented, .sequence_number = 0 };
    receiver.insertPacket(&log, frame_buf[0..80]);

    header.* = .{ .frame_length = 80, .flags = constants.flag_unfragmented, .sequence_number = 96 };
    receiver.insertPacket(&log, frame_buf[0..80]);

    // When
    _ = detector.scan(&log, 0);

    // Then — rebuild_position should advance past both frames
    try testing.expectEqual(@as(i64, 192), detector.rebuild_position);
}

test "gap is cleared when missing packet arrives" {
    // Given
    var log = try ReceiveLogBuffer.allocate(4096);
    defer log.close();

    var detector = LossDetector.init(0);

    // Create a gap: insert at position 96, skip position 0
    var frame_buf: [128]u8 = [_]u8{0} ** 128;
    const header: *frames.DataFrameHeader = @ptrCast(@alignCast(&frame_buf));
    header.* = .{ .frame_length = 80, .flags = constants.flag_unfragmented, .sequence_number = 96 };
    receiver.insertPacket(&log, frame_buf[0..80]);

    _ = detector.scan(&log, 0); // detect gap
    try testing.expect(detector.has_active_gap);

    // Fill the gap
    header.* = .{ .frame_length = 80, .flags = constants.flag_unfragmented, .sequence_number = 0 };
    receiver.insertPacket(&log, frame_buf[0..80]);

    // When — scan again
    _ = detector.scan(&log, 0);

    // Then — gap should be cleared, rebuild_position advanced
    try testing.expect(!detector.has_active_gap);
    try testing.expectEqual(@as(i64, 192), detector.rebuild_position);
}
```

### 12.3 Unit Test: Fragment Reassembly

```zig
const FragmentAssembler = @import("fragment_assembler.zig").FragmentAssembler;

test "reassemble 3-fragment message" {
    // Given
    var assembler = FragmentAssembler.init(4096, 0x00010002);
    defer assembler.deinit();

    // Fragment 1: BEGIN
    var frag1_buf: [80]u8 = [_]u8{0} ** 80;
    const h1: *frames.DataFrameHeader = @ptrCast(@alignCast(&frag1_buf));
    h1.* = .{
        .frame_length = 80,
        .flags = constants.flag_begin,
        .sequence_number = 0,
        .correlation_id = 42,
        .source_node_id = 1,
        .source_service_id = 2,
        .target_service_id = 5,
    };
    // Fill payload with 'A'
    @memset(frag1_buf[40..80], 'A');

    const result1 = assembler.onFragment(h1, &frag1_buf);
    try testing.expect(result1 == null); // not complete yet

    // Fragment 2: middle
    var frag2_buf: [80]u8 = [_]u8{0} ** 80;
    const h2: *frames.DataFrameHeader = @ptrCast(@alignCast(&frag2_buf));
    h2.* = .{
        .frame_length = 80,
        .flags = 0, // neither BEGIN nor END
        .sequence_number = 96, // aligned: 80 → 96
        .correlation_id = 42,
        .source_node_id = 1,
        .source_service_id = 2,
        .target_service_id = 5,
    };
    @memset(frag2_buf[40..80], 'B');

    const result2 = assembler.onFragment(h2, &frag2_buf);
    try testing.expect(result2 == null); // not complete yet

    // Fragment 3: END
    var frag3_buf: [80]u8 = [_]u8{0} ** 80;
    const h3: *frames.DataFrameHeader = @ptrCast(@alignCast(&frag3_buf));
    h3.* = .{
        .frame_length = 80,
        .flags = constants.flag_end,
        .sequence_number = 192, // aligned: 96 + 96
        .correlation_id = 42,
        .source_node_id = 1,
        .source_service_id = 2,
        .target_service_id = 5,
    };
    @memset(frag3_buf[40..80], 'C');

    // When
    const result3 = assembler.onFragment(h3, &frag3_buf);

    // Then — should return the reassembled message (120 bytes: 3 × 40-byte payloads)
    try testing.expect(result3 != null);
    const reassembled = result3.?;
    try testing.expectEqual(@as(usize, 120), reassembled.len);

    // Verify payload contents
    for (reassembled[0..40]) |b| try testing.expectEqual(@as(u8, 'A'), b);
    for (reassembled[40..80]) |b| try testing.expectEqual(@as(u8, 'B'), b);
    for (reassembled[80..120]) |b| try testing.expectEqual(@as(u8, 'C'), b);
}

test "out-of-order fragment discards in-progress assembly" {
    // Given
    var assembler = FragmentAssembler.init(4096, 0x00010002);
    defer assembler.deinit();

    // BEGIN fragment
    var frag1_buf: [80]u8 = [_]u8{0} ** 80;
    const h1: *frames.DataFrameHeader = @ptrCast(@alignCast(&frag1_buf));
    h1.* = .{
        .frame_length = 80,
        .flags = constants.flag_begin,
        .sequence_number = 0,
        .correlation_id = 42,
    };
    _ = assembler.onFragment(h1, &frag1_buf);
    try testing.expect(assembler.assembling);

    // Out-of-order fragment (wrong sequence — expected 96, got 192)
    var frag_bad_buf: [80]u8 = [_]u8{0} ** 80;
    const h_bad: *frames.DataFrameHeader = @ptrCast(@alignCast(&frag_bad_buf));
    h_bad.* = .{
        .frame_length = 80,
        .flags = 0,
        .sequence_number = 192, // wrong — should be 96
        .correlation_id = 42,
    };

    // When
    const result = assembler.onFragment(h_bad, &frag_bad_buf);

    // Then
    try testing.expect(result == null);
    try testing.expect(!assembler.assembling); // assembly discarded
}

test "fragment with wrong correlation_id discards assembly" {
    // Given
    var assembler = FragmentAssembler.init(4096, 0x00010002);
    defer assembler.deinit();

    var frag1_buf: [80]u8 = [_]u8{0} ** 80;
    const h1: *frames.DataFrameHeader = @ptrCast(@alignCast(&frag1_buf));
    h1.* = .{
        .frame_length = 80,
        .flags = constants.flag_begin,
        .sequence_number = 0,
        .correlation_id = 42,
    };
    _ = assembler.onFragment(h1, &frag1_buf);

    // Middle fragment with different correlation_id
    var frag2_buf: [80]u8 = [_]u8{0} ** 80;
    const h2: *frames.DataFrameHeader = @ptrCast(@alignCast(&frag2_buf));
    h2.* = .{
        .frame_length = 80,
        .flags = 0,
        .sequence_number = 96,
        .correlation_id = 99, // wrong!
    };

    // When
    const result = assembler.onFragment(h2, &frag2_buf);

    // Then
    try testing.expect(result == null);
    try testing.expect(!assembler.assembling);
}
```

### 12.4 Unit Test: Receiver Window Calculation

See doc 07, §10.2 for the full receiver window test suite. The tests verify:

- Empty buffer → window = `capacity / 2`
- Partially filled buffer → window shrinks proportionally
- Full buffer → window = 0
- After consumption → window grows

### 12.5 Unit Test: Message Routing

```zig
test "routeToService writes to service ring buffer" {
    // Given — set up a mock service registry with a ring buffer
    var ring_buf_mem: [4096 + 768]u8 align(8) = undefined;
    @memset(&ring_buf_mem, 0);
    var ring_buffer = RingBuffer.init(&ring_buf_mem, false, null) catch unreachable;

    var registry: ServiceRegistry = .{ .entries = [_]?ServiceRegistry.ServiceEntry{null} ** 256 };
    registry.entries[5] = .{
        .service_id = 5,
        .service_name = "test-service",
        .node_id = 0,
        .messages_ring_buffer = &ring_buffer,
    };

    // Build a frame
    var frame_buf: [128]u8 = [_]u8{0} ** 128;
    const header: *frames.DataFrameHeader = @ptrCast(@alignCast(&frame_buf));
    header.* = .{
        .frame_length = 80,
        .flags = constants.flag_unfragmented,
        .target_service_id = 5,
        .source_node_id = 2,
        .source_service_id = 1,
    };

    // When — route the message
    var recv_loop = createTestReceiverEventLoop(&registry);
    recv_loop.routeToService(5, 2, 1, frame_buf[0..80], 80, 0);

    // Then — verify the ring buffer received the message
    var received = false;
    _ = ring_buffer.read(struct {
        fn handler(_: i32, _: []const u8) void {
            received = true;
        }
    }.handler, 10);
    try testing.expect(received);
}

test "routeToService increments service_back_pressure on full ring buffer" {
    // Given — a service with a tiny ring buffer that's already full
    var ring_buf_mem: [128 + 768]u8 align(8) = undefined; // tiny buffer
    @memset(&ring_buf_mem, 0);
    var ring_buffer = RingBuffer.init(&ring_buf_mem, false, null) catch unreachable;

    // Fill the ring buffer
    const filler = [_]u8{0xFF} ** 64;
    _ = ring_buffer.write(1, &filler); // fill it

    var registry: ServiceRegistry = .{ .entries = [_]?ServiceRegistry.ServiceEntry{null} ** 256 };
    registry.entries[5] = .{
        .service_id = 5,
        .service_name = "test-service",
        .node_id = 0,
        .messages_ring_buffer = &ring_buffer,
    };

    // When — attempt to route
    var recv_loop = createTestReceiverEventLoop(&registry);
    const initial_bp = recv_loop.counters.get(.service_back_pressure);

    var frame_buf: [128]u8 = [_]u8{0} ** 128;
    const header: *frames.DataFrameHeader = @ptrCast(@alignCast(&frame_buf));
    header.* = .{ .frame_length = 80, .target_service_id = 5 };
    recv_loop.routeToService(5, 2, 1, frame_buf[0..80], 80, 0);

    // Then — back-pressure counter should increment
    try testing.expect(recv_loop.counters.get(.service_back_pressure) > initial_bp);
}
```

### 12.6 Integration Test: Full Receive Path

```zig
test "full receive path: UDP packet → recv log → service ring buffer" {
    // Given
    var log = try ReceiveLogBuffer.allocate(constants.default_recv_log_buffer_length);
    defer log.close();

    var service_rb_mem: [65536 + 768]u8 align(8) = undefined;
    @memset(&service_rb_mem, 0);
    var service_rb = RingBuffer.init(&service_rb_mem, false, null) catch unreachable;

    var registry: ServiceRegistry = .{ .entries = [_]?ServiceRegistry.ServiceEntry{null} ** 256 };
    registry.entries[5] = .{
        .service_id = 5,
        .service_name = "test-service",
        .node_id = 0,
        .messages_ring_buffer = &service_rb,
    };

    // Build a complete (unfragmented) data frame
    var frame_buf: [128]u8 = [_]u8{0} ** 128;
    const header: *frames.DataFrameHeader = @ptrCast(@alignCast(&frame_buf));
    header.* = .{
        .frame_length = 80,
        .flags = constants.flag_unfragmented,
        .source_node_id = 2,
        .source_service_id = 1,
        .target_service_id = 5,
        .template_id = 100,
        .correlation_id = 42,
        .sequence_number = 0,
    };
    @memset(frame_buf[40..80], 0xAB); // payload

    // When — simulate the receive path
    // Step 1: Insert into receive log
    receiver.insertPacket(&log, frame_buf[0..80]);

    // Step 2: Read from receive log
    const read_frame = log.readFrame(0);
    try testing.expect(read_frame != null);

    // Step 3: Route to service
    var recv_loop = createTestReceiverEventLoop(&registry);
    recv_loop.routeToService(5, 2, 1, read_frame.?, 80, 0);

    // Then — verify the service ring buffer received the message
    var routed_payload: ?[]const u8 = null;
    _ = service_rb.read(struct {
        fn handler(_: i32, payload: []const u8) void {
            routed_payload = payload;
        }
    }.handler, 10);

    try testing.expect(routed_payload != null);
    // The routed payload should contain the full frame (header + payload)
    try testing.expectEqual(@as(usize, 80), routed_payload.?.len);
}

test "loss scenario: gap triggers NAK after delay" {
    // Given
    var log = try ReceiveLogBuffer.allocate(4096);
    defer log.close();

    var detector = LossDetector.init(0);

    // Insert packet at position 96, creating a gap at position 0
    var frame_buf: [128]u8 = [_]u8{0} ** 128;
    const header: *frames.DataFrameHeader = @ptrCast(@alignCast(&frame_buf));
    header.* = .{
        .frame_length = 80,
        .flags = constants.flag_unfragmented,
        .sequence_number = 96,
    };
    receiver.insertPacket(&log, frame_buf[0..80]);

    // When — simulate time progression
    var now_ns: i64 = 0;

    // Scan 1: detect gap, start NAK timer
    const r1 = detector.scan(&log, now_ns);
    try testing.expectEqual(@as(u32, 0), r1); // no NAK yet

    // Scan 2: 30ms later — still waiting
    now_ns += 30 * std.time.ns_per_ms;
    const r2 = detector.scan(&log, now_ns);
    try testing.expectEqual(@as(u32, 0), r2);

    // Scan 3: 61ms total — NAK fires
    now_ns += 31 * std.time.ns_per_ms;
    const r3 = detector.scan(&log, now_ns);
    try testing.expectEqual(@as(u32, 1), r3); // NAK!

    // Then — verify gap details
    const gap = detector.activeGap();
    try testing.expectEqual(@as(i64, 0), gap.position);
    try testing.expect(gap.length > 0);

    // Scan 4: 30ms after NAK — retry not yet due
    now_ns += 30 * std.time.ns_per_ms;
    const r4 = detector.scan(&log, now_ns);
    try testing.expectEqual(@as(u32, 0), r4);

    // Scan 5: 61ms after NAK — retry fires
    now_ns += 31 * std.time.ns_per_ms;
    const r5 = detector.scan(&log, now_ns);
    try testing.expectEqual(@as(u32, 1), r5); // retry NAK
}
```

### 12.7 Testing Tips

- **Pre-allocate log buffers** with small capacities (4096 bytes) for unit tests to
  keep tests fast and make wrapping behavior observable.
- **Use deterministic timestamps.** Pass explicit `now_ns` values to `scan()` and
  scheduler functions rather than reading the real clock. This makes NAK timing tests
  reliable.
- **Verify counter increments** after every operation to catch cases where error paths
  silently swallow failures.
- **Test idempotency** by inserting the same frame twice and verifying that tail doesn't
  advance and the frame content is unchanged.
- **Test wrap-around** by inserting enough frames to wrap the log buffer and verifying
  that the circular indexing works correctly.
- **Fragment tests should cover:** normal 3-fragment reassembly, out-of-order fragments,
  wrong correlation ID, missing BEGIN, duplicate END, and exceeding
  `MAX_REASSEMBLED_MESSAGE_SIZE`.

---

## 13. File Structure

```
src/
  receiver/
    receiver_event_loop.zig      # ReceiverEventLoop struct, doWork duty cycle,
                                 # packet dispatch, SM/NAK forwarding
    receive_log_buffer.zig       # insertPacket, readFrame extensions on ReceiveLogBuffer
    peer_receiver.zig            # PeerReceiver struct, per-peer state
    loss_detector.zig            # LossDetector — gap scanning, NAK timing
    fragment_assembler.zig       # FragmentAssembler — multi-frame message reassembly
    message_router.zig           # routeToService, routeReassembledMessage,
                                 # markFrameConsumed, ServiceRegistry
    status_message_sender.zig    # sendInitialStatusMessage, sendSmCallback,
                                 # SM encoding helpers (delegates to doc 07 types)
  receiver.zig                   # Public re-exports
```

**Re-exports (`src/receiver.zig`):**

```zig
pub const ReceiverEventLoop = @import("receiver/receiver_event_loop.zig").ReceiverEventLoop;
pub const PeerReceiver = @import("receiver/peer_receiver.zig").PeerReceiver;
pub const LossDetector = @import("receiver/loss_detector.zig").LossDetector;
pub const FragmentAssembler = @import("receiver/fragment_assembler.zig").FragmentAssembler;
pub const ServiceRegistry = @import("receiver/message_router.zig").ServiceRegistry;

pub const ReceiverCounterId = @import("receiver/receiver_event_loop.zig").ReceiverCounterId;
pub const insertPacket = @import("receiver/receive_log_buffer.zig").insertPacket;
```

---

## Summary of Dependencies

```
                    ┌───────────────────────────────┐
                    │  06 — Receive Path             │
                    │  (this document)               │
                    └───────┬───────────────────────┘
                            │
              depends on    │    depended on by
           ┌────────────────┼────────────────────┐
           │                │                    │
     ┌─────▼─────┐   ┌─────▼─────┐        ┌─────▼──────┐
     │ 02 Memory │   │ 03 Conc.  │        │ 07 Flow    │
     │ Layout    │   │ Data Str. │        │ Control    │
     └───────────┘   └───────────┘        └────────────┘
     ReceiveLog-      RingBuffer           ReceiverFlow-
     Buffer,          .write(),            Control,
     ServiceMeta-     .tryClaim(),         StatusMessage-
     dataFile         CountersManager      Scheduler

     ┌───────────┐                        ┌────────────┐
     │ 04 UDP    │                        │ 08 Service │
     │ Transport │                        │ IPC        │
     └───────────┘                        └────────────┘
     DataFrameHeader,                      Cross-host
     SetupFrame,                           routed path
     StatusMessage,                        uses routing
     NakFrame,                             defined here
     NetworkIo,
     parseFrame
```

---

*Previous: [05 — Send Path](05-send-path.md) · Next: [07 — Flow Control](07-flow-control.md)*