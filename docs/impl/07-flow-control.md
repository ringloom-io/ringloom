# 07 — Flow Control

> **Depends on:** [05 — Send Path](05-send-path.md) (sender event loop, retransmit buffer), [06 — Receive Path](06-receive-path.md) (receive log buffer, loss detection, message routing)
>
> **Depended on by:** [08 — Service IPC](08-service-ipc.md) (back-pressure propagation to services)

This document describes the window-based flow control subsystem that governs data
transfer between broker peers over UDP. It covers sender-side admission control,
receiver-side window calculation, Status Message timing, end-to-end back-pressure
propagation, zero-window probing, and the counters needed for observability.

All code targets **Zig 0.14.x** stable.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Sender-Side Flow Control](#2-sender-side-flow-control)
3. [Receiver-Side Flow Control](#3-receiver-side-flow-control)
4. [Status Message Encoding & Transmission](#4-status-message-encoding--transmission)
5. [Back-Pressure Propagation](#5-back-pressure-propagation)
6. [Zero-Window Probing](#6-zero-window-probing)
7. [Flow Control Counters](#7-flow-control-counters)
8. [Edge Cases & Recovery](#8-edge-cases--recovery)
9. [Configuration](#9-configuration)
10. [Testing](#10-testing)
11. [File Structure](#11-file-structure)

---

## 1. Overview

BRZ uses a **single-receiver, window-based** flow control model. Each peer link
(Broker A → Broker B) has exactly one sender and one receiver. The receiver
communicates its capacity via Status Messages (SMs), and the sender respects the
advertised window. There is no congestion control — the sender either has permission
to send or it does not.

```
Sender (Broker A)                          Receiver (Broker B)
┌──────────────────────┐                  ┌──────────────────────┐
│                      │   DATA frames    │                      │
│  send_position  ─────┼─────────────────►│  recv_position       │
│                      │                  │  (tail of recv log)  │
│  send_limit    ◄─────┼──────────────────┼── Status Message     │
│  = consumption_pos   │  (consumption_pos│  (consumption_pos    │
│    + receiver_window │   + recv_window) │   + recv_window)     │
│                      │                  │                      │
└──────────────────────┘                  └──────────────────────┘

Invariants:
  send_position  <= send_limit               (never send beyond permission)
  send_limit      = consumption_position     (set by receiver via SM)
                    + receiver_window
  receiver_window <= recv_log.capacity / 2   (safety cap — see §3.1)
```

The full feedback loop:

1. **Receiver** calculates how much buffer space remains and encodes it in a Status
   Message (`consumption_position` + `receiver_window`).
2. **Sender** receives the SM and updates `send_limit`.
3. **Sender** checks `canSend()` before draining each frame from the send ring buffer.
   If the window is exhausted, the sender stops — the send ring buffer fills up —
   services get `error.BufferFull` on write.
4. **Receiver** consumes data, frees buffer space, and eventually sends a new SM with a
   larger window.

This is a **credit-based** scheme. The receiver grants credits (bytes of window);
the sender spends them. No data is sent without credits.

---

## 2. Sender-Side Flow Control

**File: `src/flow_control/sender_flow_control.zig`**

### 2.1 State Per Peer

Every outbound peer connection carries flow control state that tracks how far the
sender has written and how far the receiver has granted permission:

```zig
const std = @import("std");
const constants = @import("../platform/constants.zig");
const Clock = @import("../platform/clock.zig").Clock;

/// Sender-side flow control state for a single peer link.
///
/// All positions are monotonically increasing byte offsets. They never wrap.
/// The send ring buffer position maps into the circular buffer via
/// `position & (capacity - 1)`.
pub const SenderFlowControl = struct {
    /// Current send position — the next byte offset to be written on the wire.
    /// Updated by the sender event loop after each frame is transmitted.
    send_position: i64 = 0,

    /// Maximum send position permitted by the receiver. The sender MUST NOT
    /// transmit any frame whose `send_position + frame_length` would exceed
    /// this value. Set to 0 initially (don't send until first SM arrives).
    send_limit: i64 = 0,

    /// Last known receiver consumption position. Extracted from the most
    /// recent Status Message. Represents the highest contiguous byte offset
    /// the receiver has successfully routed to a downstream service.
    consumption_position: i64 = 0,

    /// Last advertised receiver window (bytes). Extracted from the most
    /// recent Status Message.
    receiver_window: i32 = 0,

    /// Timestamp (monotonic ns) of the last Status Message received from
    /// this peer. Used to detect stale/unreachable peers.
    last_sm_received_ns: i64 = 0,

    /// Number of times canSend() returned false since the last successful
    /// send. Used to drive the zero-window probe logic.
    consecutive_flow_control_failures: u64 = 0,

    /// Timestamp (monotonic ns) of the last zero-window heartbeat probe
    /// sent to this peer. Used for rate-limiting probes.
    last_probe_sent_ns: i64 = 0,

    const Self = @This();

    // ── public API ────────────────────────────────────────────────────

    /// Returns true if the sender has enough window to transmit a frame
    /// of `frame_length` bytes (including the 40-byte data frame header).
    ///
    /// This is the hot-path admission check. It MUST be called before
    /// every frame transmission. If it returns false, the sender must
    /// not send — this creates back-pressure that propagates upstream
    /// through the send ring buffer to the originating service.
    pub inline fn canSend(self: *const Self, frame_length: usize) bool {
        return self.send_position + @as(i64, @intCast(frame_length)) <= self.send_limit;
    }

    /// Returns the number of bytes remaining in the current window.
    /// May be negative if the receiver's consumption position moved
    /// backwards (should not happen, but defensive).
    pub inline fn remainingWindow(self: *const Self) i64 {
        return self.send_limit - self.send_position;
    }

    /// Advances the send position after a frame has been successfully
    /// transmitted. Called by the sender event loop after each sendmsg().
    pub inline fn onFrameSent(self: *Self, frame_length: usize) void {
        self.send_position += @as(i64, @intCast(frame_length));
        self.consecutive_flow_control_failures = 0;
    }

    /// Updates the send limit from an incoming Status Message. This is
    /// the only path through which the sender learns about new capacity.
    ///
    /// Monotonicity: send_limit only ever increases. If a stale SM
    /// arrives with a smaller computed limit, it is ignored. This
    /// prevents a reordered SM from shrinking the window after a newer
    /// SM already expanded it.
    pub fn onStatusMessage(
        self: *Self,
        sm_consumption_position: i64,
        sm_receiver_window: i32,
        now_ns: i64,
    ) void {
        const proposed_limit = sm_consumption_position + @as(i64, sm_receiver_window);

        // Only advance — never retreat
        if (proposed_limit > self.send_limit) {
            self.send_limit = proposed_limit;
        }

        // Always update consumption position and window for monitoring,
        // even if the limit didn't advance (the receiver may have
        // consumed data but shrunk its window).
        if (sm_consumption_position > self.consumption_position) {
            self.consumption_position = sm_consumption_position;
        }
        self.receiver_window = sm_receiver_window;
        self.last_sm_received_ns = now_ns;
        self.consecutive_flow_control_failures = 0;
    }

    /// Records a flow control failure (canSend returned false). Called
    /// by the sender event loop when it cannot drain the send ring
    /// buffer due to window exhaustion.
    pub inline fn onFlowControlReject(self: *Self) void {
        self.consecutive_flow_control_failures += 1;
    }

    /// Returns true if no Status Message has been received from this
    /// peer within `timeout_ns` nanoseconds. Indicates the peer may be
    /// unreachable and should be reported to the control loop.
    pub inline fn isStale(self: *const Self, now_ns: i64, timeout_ns: i64) bool {
        // A peer that has never sent an SM (last_sm_received_ns == 0)
        // is not considered stale — it hasn't connected yet.
        if (self.last_sm_received_ns == 0) return false;
        return (now_ns - self.last_sm_received_ns) > timeout_ns;
    }

    /// Resets all state. Called when a peer connection is torn down and
    /// will be re-established.
    pub fn reset(self: *Self) void {
        self.send_position = 0;
        self.send_limit = 0;
        self.consumption_position = 0;
        self.receiver_window = 0;
        self.last_sm_received_ns = 0;
        self.consecutive_flow_control_failures = 0;
        self.last_probe_sent_ns = 0;
    }
};
```

### 2.2 Send Check Integration

The `canSend` check integrates into the sender event loop's drain cycle. This is
the critical decision point where flow control applies back-pressure:

```zig
/// Called by the sender event loop for each frame read from the send
/// ring buffer. Returns the number of frames successfully sent.
fn drainSendRingBuffer(
    self: *SenderEventLoop,
    limit: u32,
) u32 {
    var frames_sent: u32 = 0;
    var frames_read: u32 = 0;

    while (frames_read < limit) {
        // Peek next message from ring buffer without consuming
        const msg = self.send_ring_buffer.peek() orelse break;
        frames_read += 1;

        const target_node_id = parseTargetNodeId(msg);
        const peer = self.peer_registry.lookup(target_node_id) orelse {
            // Unknown peer — consume and drop
            self.send_ring_buffer.consume(msg.length);
            self.counters.increment(.unknown_peer_drops);
            continue;
        };

        const frame_length = msg.length + constants.data_frame_header_length;

        // ── FLOW CONTROL GATE ──
        if (!peer.flow_control.canSend(frame_length)) {
            // Window exhausted for this peer. We cannot consume this
            // message because it would be lost. Leave it in the ring
            // buffer and stop draining.
            //
            // Other messages to *different* peers could potentially be
            // sent, but the ring buffer is ordered — we cannot skip.
            // This is the fundamental back-pressure point.
            peer.flow_control.onFlowControlReject();
            self.counters.increment(.send_rb_back_pressure);
            break;
        }

        // Consume from ring buffer, fragment if needed, transmit
        self.send_ring_buffer.consume(msg.length);
        self.fragmentAndSend(peer, msg);
        peer.flow_control.onFrameSent(frame_length);
        frames_sent += 1;
    }

    return frames_sent;
}
```

**Key design point:** The send ring buffer is a single ordered MPSC queue. When
flow control blocks a message to peer X, it also blocks subsequent messages to
peer Y that sit behind it in the queue. This is a simplification trade-off — BRZ
does not implement per-peer send queues. In practice, cross-host messages are the
minority case, and a flow-controlled peer should recover quickly (within one SM
round-trip).

### 2.3 Initial State & Connection Establishment

When a new peer connection is being established:

```
Timeline:
  t=0  Broker A sends SETUP frame to Broker B
       flow_control.send_position = 0
       flow_control.send_limit    = 0    ← cannot send any data yet

  t=1  Broker B receives SETUP, responds with initial Status Message
       SM.consumption_position = 0
       SM.receiver_window      = recv_log.capacity / 2

  t=2  Broker A receives SM, calls onStatusMessage()
       flow_control.send_limit = 0 + (capacity / 2) = 2MB  (for 4MB log)
       → canSend() now returns true for frames up to 2MB total
```

This ensures zero data is sent before the receiver has advertised capacity. The
receiver controls the conversation from the start.

```zig
/// Called by the control loop when a new peer is added to the cluster.
/// Initializes flow control state and sends a SETUP frame.
fn onPeerConnected(self: *SenderEventLoop, peer: *PeerSender) void {
    peer.flow_control.reset();
    // send_limit stays at 0 — no data until first SM
    self.sendSetupFrame(peer);
}
```

---

## 3. Receiver-Side Flow Control

**File: `src/flow_control/receiver_flow_control.zig`**

### 3.1 Receiver Window Calculation

The receiver window tells the sender how many bytes it may send ahead of the
current consumption position. It is derived from the receive log buffer's
available capacity:

```zig
const std = @import("std");
const constants = @import("../platform/constants.zig");
const Clock = @import("../platform/clock.zig").Clock;
const ReceiveLogBuffer = @import("../memory/receive_log.zig").ReceiveLogBuffer;

/// Receiver-side flow control state for a single peer link.
pub const ReceiverFlowControl = struct {
    /// The highest contiguous byte position that has been successfully
    /// routed to a downstream service (or acknowledged by the broker's
    /// internal routing). This advances as the receiver event loop
    /// delivers messages.
    consumption_position: i64 = 0,

    /// The consumption_position value at the time the last Status
    /// Message was sent. Used to determine whether an eager SM is
    /// warranted (see §4).
    last_sm_consumption_position: i64 = 0,

    /// The receiver_window value advertised in the last Status Message.
    /// Cached for eager-SM threshold calculation.
    last_sm_receiver_window: i32 = 0,

    /// Timestamp (monotonic ns) of the last Status Message sent to
    /// this peer. Used for periodic SM timing.
    last_sm_sent_ns: i64 = 0,

    /// Reference to the peer's receive log buffer. Used to read
    /// tail_position and capacity for window calculation.
    recv_log: *ReceiveLogBuffer,

    /// The log buffer's total capacity in bytes. Cached at init time
    /// to avoid a pointer chase on every window calculation.
    log_capacity: i64,

    const Self = @This();

    pub fn init(recv_log: *ReceiveLogBuffer) Self {
        return .{
            .recv_log = recv_log,
            .log_capacity = @intCast(recv_log.capacity),
        };
    }

    /// Calculates the receiver window — the number of bytes the sender
    /// is allowed to write ahead of consumption_position.
    ///
    /// The window is capped at `log_capacity / 2` to provide a safety
    /// margin. Without the cap, the sender could fill the entire log
    /// buffer before the receiver processes any data, leaving zero
    /// headroom for processing latency. The half-buffer cap guarantees
    /// that even if the sender fills its entire grant, the receiver
    /// still has half the buffer available for concurrent consumption.
    ///
    /// ```
    /// |<── consumed ──>|<── buffered ──>|<── available ──>|
    /// 0          consumption_pos    tail_pos          capacity
    ///                                   |<── window ────>|
    ///                                   (capped at cap/2)
    /// ```
    pub fn calculateReceiverWindow(self: *const Self) i32 {
        const tail = self.recv_log.loadTailPosition();
        const buffered = tail - self.consumption_position;

        // Available = total capacity minus data sitting in the log
        // waiting to be consumed. Clamp to zero (can go negative
        // momentarily if tail races ahead during calculation).
        const available = @max(0, self.log_capacity - buffered);

        // Cap at half capacity
        const half_capacity = @divFloor(self.log_capacity, 2);
        const window = @min(available, half_capacity);

        return @intCast(window);
    }

    /// Returns the initial window to advertise in the first Status
    /// Message after a SETUP is received. This is always half the log
    /// capacity, regardless of current buffer state (which should be
    /// empty at connection time).
    pub fn initialWindow(self: *const Self) i32 {
        return @intCast(@divFloor(self.log_capacity, 2));
    }

    /// Resets state for a new connection.
    pub fn reset(self: *Self) void {
        self.consumption_position = 0;
        self.last_sm_consumption_position = 0;
        self.last_sm_receiver_window = 0;
        self.last_sm_sent_ns = 0;
    }
};
```

### 3.2 Consumption Position Tracking

The consumption position represents the **highest contiguous byte position** that
the receiver has fully processed. It advances as the receiver event loop routes
messages to target services.

The update algorithm walks forward from the current consumption position, checking
each frame slot in the receive log. A frame slot is considered consumed when:
1. The frame was successfully routed to the target service, AND
2. All preceding frames have also been consumed (contiguity requirement).

```zig
/// Frame status values in the receive log buffer.
/// The frame_length field doubles as a status marker:
///   > 0  → frame present, not yet consumed
///   = 0  → slot empty (gap)
///  -1   → frame consumed (routed to service)
const frame_consumed_marker: i32 = -1;

/// Advances the consumption position by scanning forward through
/// the receive log for contiguously consumed frames.
///
/// This function is called after each successful route-to-service
/// operation. It is O(k) where k is the number of contiguous
/// consumed frames ahead of the current position — typically 1
/// in the common case, but may batch-advance after a burst.
pub fn updateConsumptionPosition(self: *ReceiverFlowControl) void {
    const mask: i64 = @intCast(self.recv_log.capacity - 1);
    var pos = self.consumption_position;

    while (true) {
        const idx: usize = @intCast(pos & mask);
        const frame_ptr: *align(1) volatile i32 = @ptrCast(&self.recv_log.data[idx]);
        const frame_length = @atomicLoad(i32, frame_ptr, .acquire);

        if (frame_length != frame_consumed_marker) {
            // Either an empty slot (gap), an unconsumed frame, or
            // we've reached the tail. Stop advancing.
            break;
        }

        // Frame was consumed. We need to know how large it was to
        // advance past it. The original frame_length was overwritten
        // with the consumed marker, so we use the aligned-length
        // stored in a secondary field (see §3.3).
        const aligned_length = readAlignedFrameLength(self.recv_log, idx);
        if (aligned_length <= 0) break; // safety: corrupted or uninitialized

        pos += @as(i64, aligned_length);
    }

    self.consumption_position = pos;
}
```

### 3.3 Frame Consumption Protocol

When the receiver successfully routes a frame to a service, it must mark the frame
as consumed in the receive log so that `updateConsumptionPosition` can advance
past it. The protocol preserves the aligned frame length (needed to calculate the
stride) while marking the slot as consumed:

```zig
const frame_alignment = 32;

/// Marks a frame as consumed in the receive log buffer.
/// Called after a successful route-to-service.
///
/// The aligned frame length is stored at a secondary offset within
/// the frame header (bytes 4..8, which overlap with the version/
/// flags/frame_type fields that are no longer needed post-routing).
/// The primary frame_length field (bytes 0..4) is then overwritten
/// with the consumed marker.
pub fn markFrameConsumed(log: *ReceiveLogBuffer, position: i64, frame_length: i32) void {
    const mask: i64 = @intCast(log.capacity - 1);
    const idx: usize = @intCast(position & mask);

    // Calculate aligned length (how far to stride in the log)
    const aligned_len = constants.alignUp(@intCast(frame_length), frame_alignment);

    // Store aligned length at secondary offset (bytes 4..8)
    const secondary_ptr: *align(1) volatile i32 = @ptrCast(&log.data[idx + 4]);
    @atomicStore(i32, secondary_ptr, @intCast(aligned_len), .release);

    // Mark as consumed (overwrite frame_length with marker)
    const primary_ptr: *align(1) volatile i32 = @ptrCast(&log.data[idx]);
    @atomicStore(i32, primary_ptr, frame_consumed_marker, .release);
}

/// Reads the aligned frame length from a consumed frame slot.
/// Used by updateConsumptionPosition to stride past consumed frames.
fn readAlignedFrameLength(log: *ReceiveLogBuffer, idx: usize) i32 {
    const secondary_ptr: *align(1) volatile i32 = @ptrCast(&log.data[idx + 4]);
    return @atomicLoad(i32, secondary_ptr, .acquire);
}
```

### 3.4 Receiver Window Dynamics

A worked example showing how the window changes as data flows:

```
Configuration: recv_log capacity = 4MB (4,194,304 bytes)
               max window = capacity / 2 = 2MB

Time  Event                            consumption_pos   tail_pos   buffered   window
────  ─────────────────────────────    ───────────────   ────────   ────────   ──────
 t0   Connection established                0               0         0        2MB
 t1   Received 500KB of data                0            512000     512KB     1.5MB
 t2   Consumed 256KB                     262144          512000     ~250KB    1.75MB
 t3   Received 1MB more data             262144         1572864    ~1.25MB   ~0.75MB
 t4   Consumed all pending data         1572864         1572864        0       2MB
 t5   Burst: received 2MB               1572864         3670016       2MB       0
      (window=0 → sender must stop)
 t6   Consumed 1MB                      2621440         3670016       1MB      1MB
      (SM sent → sender resumes)
```

Note how at t5 the window drops to zero. The sender must stop transmitting. This
is the **zero-window** condition, handled in §6.

---

## 4. Status Message Encoding & Transmission

**File: `src/flow_control/status_message.zig`**

### 4.1 Status Message Wire Format

The Status Message is defined in the UDP wire protocol
([architecture §7.5](../architecture.md#75-status-message-28-bytes)):

```
Offset  Size  Type    Field
──────────────────────────────────
0       4     i32     frame_length (= 28)
4       1     u8      version (0)
5       1     u8      flags
6       2     u16     frame_type (SM = 0x03)
8       1     u8      node_id
9       1     u8      reserved
10      2     u16     reserved
12      8     i64     consumption_position
20      4     i32     receiver_window
24      4     i32     reserved
```

### 4.2 Flyweight Accessor

```zig
const constants = @import("../platform/constants.zig");

/// Flyweight overlay for a Status Message frame. Zero-copy — reads
/// and writes go directly to the underlying buffer bytes.
pub const StatusMessageFlyweight = struct {
    buffer: [*]u8,

    const frame_length_offset = 0;
    const version_offset = 4;
    const flags_offset = 5;
    const frame_type_offset = 6;
    const node_id_offset = 8;
    const consumption_position_offset = 12;
    const receiver_window_offset = 20;

    pub const encoded_length: usize = 28;

    pub fn wrap(buffer: [*]u8) StatusMessageFlyweight {
        return .{ .buffer = buffer };
    }

    // ── Getters ──

    pub inline fn frameLength(self: StatusMessageFlyweight) i32 {
        return readLittleI32(self.buffer + frame_length_offset);
    }

    pub inline fn flags(self: StatusMessageFlyweight) u8 {
        return self.buffer[flags_offset];
    }

    pub inline fn nodeId(self: StatusMessageFlyweight) u8 {
        return self.buffer[node_id_offset];
    }

    pub inline fn consumptionPosition(self: StatusMessageFlyweight) i64 {
        return readLittleI64(self.buffer + consumption_position_offset);
    }

    pub inline fn receiverWindow(self: StatusMessageFlyweight) i32 {
        return readLittleI32(self.buffer + receiver_window_offset);
    }

    // ── Setters ──

    pub fn encode(
        self: StatusMessageFlyweight,
        node_id: u8,
        consumption_pos: i64,
        recv_window: i32,
        sm_flags: u8,
    ) void {
        writeLittleI32(self.buffer + frame_length_offset, @intCast(encoded_length));
        self.buffer[version_offset] = constants.frame_header_version;
        self.buffer[flags_offset] = sm_flags;
        writeLittleU16(self.buffer + frame_type_offset, constants.frame_type_sm);
        self.buffer[node_id_offset] = node_id;
        self.buffer[node_id_offset + 1] = 0; // reserved
        writeLittleU16(self.buffer + node_id_offset + 2, 0); // reserved
        writeLittleI64(self.buffer + consumption_position_offset, consumption_pos);
        writeLittleI32(self.buffer + receiver_window_offset, recv_window);
        writeLittleI32(self.buffer + receiver_window_offset + 4, 0); // reserved
    }

    // ── Little-endian helpers ──

    inline fn readLittleI32(ptr: [*]const u8) i32 {
        return @bitCast(std.mem.readInt(u32, ptr[0..4], .little));
    }

    inline fn readLittleI64(ptr: [*]const u8) i64 {
        return @bitCast(std.mem.readInt(u64, ptr[0..8], .little));
    }

    inline fn writeLittleI32(ptr: [*]u8, val: i32) void {
        std.mem.writeInt(u32, ptr[0..4], @bitCast(val), .little);
    }

    inline fn writeLittleI64(ptr: [*]u8, val: i64) void {
        std.mem.writeInt(u64, ptr[0..8], @bitCast(val), .little);
    }

    inline fn writeLittleU16(ptr: [*]u8, val: u16) void {
        std.mem.writeInt(u16, ptr[0..2], val, .little);
    }

    const std = @import("std");
};
```

### 4.3 SM Flags

```zig
pub const sm_flags = struct {
    /// No special flags — normal periodic or eager SM.
    pub const none: u8 = 0x00;

    /// Request the sender to re-send its SETUP frame. Used when the
    /// receiver detects loss of the initial connection state (e.g.,
    /// after a restart or after detecting a gap at position 0).
    pub const send_setup: u8 = 0x01;
};
```

### 4.4 Status Message Timing

Status Messages are sent under four triggers, each serving a different purpose:

| Trigger | Condition | Purpose |
|---------|-----------|---------|
| **Eager** | `consumption_position` advanced by ≥ `receiver_window / 4` since last SM | Open the window quickly when the receiver is keeping up — reduces sender stalls |
| **Periodic** | No SM sent within `sm_timeout_ns` (200ms) | Guarantee liveness even when no consumption progress is being made |
| **Loss** | Loss detector found a gap | Alert sender that retransmission is needed; SM carries `send_setup` flag if gap is at position 0 |
| **Initial** | SETUP frame received from peer | Establish the initial window so the sender can begin transmitting |

The timing logic lives in the receiver event loop and delegates to the flow
control state for threshold checks:

```zig
/// Status Message timing controller. Called once per receiver event
/// loop iteration for each peer.
pub const StatusMessageScheduler = struct {
    /// Pre-allocated SM encode buffer (one per peer, avoids allocation).
    sm_buffer: [StatusMessageFlyweight.encoded_length]u8 = undefined,

    const Self = @This();

    /// Check all SM triggers and send if any fire.
    /// Returns 1 if an SM was sent, 0 otherwise.
    pub fn maybeSendStatusMessage(
        self: *Self,
        fc: *ReceiverFlowControl,
        peer_node_id: u8,
        local_node_id: u8,
        now_ns: i64,
        sendFn: *const fn (buf: []const u8, peer_node_id: u8) void,
    ) u32 {
        // Trigger 1: Eager — consumption advanced significantly
        const advance = fc.consumption_position - fc.last_sm_consumption_position;
        const threshold = @divFloor(@as(i64, fc.last_sm_receiver_window), 4);
        const eager = advance >= threshold and threshold > 0;

        // Trigger 2: Periodic — timeout expired
        const periodic = (now_ns - fc.last_sm_sent_ns) >= constants.sm_timeout_ns;

        if (!eager and !periodic) return 0;

        // Calculate current window and encode
        const window = fc.calculateReceiverWindow();
        const flyweight = StatusMessageFlyweight.wrap(&self.sm_buffer);
        flyweight.encode(
            local_node_id,
            fc.consumption_position,
            window,
            sm_flags.none,
        );

        // Send
        sendFn(&self.sm_buffer, peer_node_id);

        // Update tracking state
        fc.last_sm_consumption_position = fc.consumption_position;
        fc.last_sm_receiver_window = window;
        fc.last_sm_sent_ns = now_ns;

        return 1;
    }

    /// Send an SM immediately (e.g., on SETUP received or loss detected).
    pub fn sendImmediate(
        self: *Self,
        fc: *ReceiverFlowControl,
        peer_node_id: u8,
        local_node_id: u8,
        now_ns: i64,
        sm_flag: u8,
        sendFn: *const fn (buf: []const u8, peer_node_id: u8) void,
    ) void {
        const window = fc.calculateReceiverWindow();
        const flyweight = StatusMessageFlyweight.wrap(&self.sm_buffer);
        flyweight.encode(
            local_node_id,
            fc.consumption_position,
            window,
            sm_flag,
        );

        sendFn(&self.sm_buffer, peer_node_id);

        fc.last_sm_consumption_position = fc.consumption_position;
        fc.last_sm_receiver_window = window;
        fc.last_sm_sent_ns = now_ns;
    }
};
```

### 4.5 Eager SM Threshold: Why Window / 4?

The eager threshold of `window / 4` is a balance between two extremes:

- **Too small** (e.g., every byte consumed → send SM): Floods the network with
  Status Messages and wastes CPU encoding/decoding them.
- **Too large** (e.g., window / 2): The sender may stall for extended periods
  waiting for an SM, even though the receiver has freed substantial capacity.

At `window / 4`, the sender will see an SM after approximately 25% of the window
has been freed. In the worst case, the sender stalls briefly and then receives a
burst of window credits. In the common case, SMs arrive well before the sender
exhausts its window, and flow control is invisible.

```
Window = 2MB, Eager threshold = 512KB

Sender transmits: ████████████████████████░░░░░░░░  (1.5MB sent, 500KB remaining)
Receiver consumes:  ██████████                       (consumed 512KB → eager SM fires)
                    ↑
                    SM sent: consumption_pos=512KB, window=2MB
                    → send_limit advances to 2.5MB
                    → sender can continue without stalling
```

---

## 5. Back-Pressure Propagation

### 5.1 The Full Chain

Back-pressure propagates backwards through the entire system, from a slow consumer
to the originating service. No data is silently dropped — the chain preserves the
signal so that the originating service can decide how to respond.

```
                         FORWARD (data flow)
  ════════════════════════════════════════════════►

  ┌───────────┐     ┌──────────────┐     ┌──────────────┐     ┌───────────┐
  │ Service A │ ──► │  Send Ring   │ ──► │  UDP / Flow  │ ──► │  Recv Log │
  │  (writer) │     │   Buffer     │     │   Control    │     │  Buffer   │
  └───────────┘     └──────────────┘     └──────────────┘     └───────────┘
                                                                    │
                                                                    ▼
                                                              ┌───────────┐
                                                              │ Route to  │
                                                              │ Service B │
                                                              │  msg RB   │
                                                              └───────────┘

  ◄════════════════════════════════════════════════
                     BACKWARD (back-pressure)

  Step 4              Step 3              Step 2              Step 1
  BufferFull ◄── RB fills up ◄── send_limit ◄── window ◄── msg RB full
  (to Service A)     (send RB       (canSend       (SM with      (Service B
                      stalls)       = false)        smaller       too slow)
                                                    window)
```

### 5.2 Step-by-Step Propagation

**Step 1: Service B's message ring buffer fills up.**

The receiver event loop calls `route_to_service()`, which tries to write the
inbound message into Service B's messages ring buffer. The MPSC ring buffer
returns `error.BufferFull`:

```zig
fn routeToService(
    self: *ReceiverEventLoop,
    target_service_id: u16,
    frame: []const u8,
    frame_length: i32,
    position: i64,
) void {
    const service = self.service_registry.lookup(target_service_id) orelse {
        self.counters.increment(.unknown_service_drops);
        // Still mark consumed — we can't hold up the entire recv log
        // for an unknown service.
        markFrameConsumed(self.recv_log, position, frame_length);
        return;
    };

    const payload = frame[constants.data_frame_header_length..];
    const result = service.messages_ring_buffer.write(
        constants.msg_type_application,
        payload,
    );

    switch (result) {
        .success => {
            // Frame routed — mark consumed so consumption_position can advance
            markFrameConsumed(self.recv_log, position, frame_length);
            self.counters.increment(.messages_routed);
        },
        .buffer_full => {
            // DO NOT mark consumed. The frame stays in the recv log.
            // consumption_position will NOT advance past this point.
            // The receiver window will shrink on the next SM.
            self.counters.increment(.service_back_pressure);
        },
    }
}
```

**Step 2: Receiver window shrinks.**

Because the frame was not marked consumed, `consumption_position` does not advance.
Meanwhile, `tail_position` continues to advance as more data arrives. The buffered
amount grows, and `calculateReceiverWindow()` returns a smaller value:

```
Before back-pressure:
  consumption_pos = 1,000,000    tail = 1,500,000
  buffered = 500,000             window = min(3,694,304, 2,097,152) = 2,097,152

After 1MB more data arrives without consumption:
  consumption_pos = 1,000,000    tail = 2,500,000
  buffered = 1,500,000           window = min(2,694,304, 2,097,152) = 2,097,152

After 2.5MB more:
  consumption_pos = 1,000,000    tail = 4,000,000
  buffered = 3,000,000           window = min(1,194,304, 2,097,152) = 1,194,304  ← shrinking

After 3.2MB more:
  consumption_pos = 1,000,000    tail = 4,194,304
  buffered = 3,194,304           window = min(1,000,000, 2,097,152) = 1,000,000  ← still shrinking

Eventually:
  buffered ≈ capacity            window ≈ 0  ← zero window, sender stops
```

**Step 3: Sender sees reduced send_limit.**

The next Status Message carries the smaller `receiver_window`. On the sender side:

```zig
// In sender event loop, on receiving SM:
peer.flow_control.onStatusMessage(
    sm.consumptionPosition(),
    sm.receiverWindow(),
    now_ns,
);
// send_limit = consumption_position + receiver_window
// If receiver_window = 0, send_limit doesn't advance
// → canSend() returns false for any non-zero frame
```

**Step 4: Send ring buffer fills up.**

The sender event loop stops draining the send ring buffer (it can't send).
Services writing to the send ring buffer via `tryClaim()` / `write()` eventually
find the buffer full:

```zig
// In a service's send path:
const claim = broker_send_rb.tryClaim(message_length);
if (claim.offset < 0) {
    // Ring buffer full — sender event loop is flow-controlled
    return error.BufferFull;
}
```

The service must then decide how to handle back-pressure.

### 5.3 Service-Level Back-Pressure Strategies

```zig
/// Back-pressure strategies available to services when the send ring
/// buffer is full.
pub const BackPressureStrategy = enum {
    /// Return error.BufferFull immediately. The service must handle it
    /// (drop the message, queue it internally, or report to the user).
    /// Lowest latency impact — no blocking, no spinning.
    fail_fast,

    /// Spin-retry with yield. The service thread spins on tryClaim(),
    /// calling std.atomic.spinLoopHint() between attempts, up to a
    /// configurable maximum number of retries.
    spin_retry,

    /// Block until space is available. Only valid when the ring buffer
    /// is in blocking mode (has a blocking trailer with writer_wait_state).
    /// The thread parks on a futex/ulock and is woken by the consumer
    /// (sender event loop) when it advances the head position.
    blocking,
};
```

For blocking mode ring buffers, the write path parks on the `writer_wait_state`
futex in the blocking trailer (see
[02 — Memory Layout §4.2](02-memory-layout-and-shared-memory.md#42-blocking-trailer-detail)):

```zig
/// Blocking write to a ring buffer with back-pressure support.
/// Parks the calling thread if the buffer is full, up to timeout_ns.
fn blockingWrite(
    rb: *MpscRingBuffer,
    msg_type: i32,
    payload: []const u8,
    timeout_ns: i64,
) !void {
    while (true) {
        const claim = rb.tryClaim(@intCast(payload.len));
        if (claim.offset >= 0) {
            // Got a slot — write and commit
            @memcpy(rb.buffer[@intCast(claim.offset)..], payload);
            rb.commit(claim);
            return;
        }

        // Buffer full — park on the writer_wait_state futex
        const trailer = rb.blockingTrailer() orelse return error.BufferFull;
        const sync = platform.ProcessSynchronizer.getPlatformInstance();
        const wait_result = sync.wait(
            trailer.writer_wait_state.ptr(),
            0, // expected value: "not waiting"
            timeout_ns,
        );

        switch (wait_result) {
            .woken, .value_changed => continue, // retry
            .timed_out => return error.TimedOut,
            .interrupted => continue,
        }
    }
}
```

---

## 6. Zero-Window Probing

### 6.1 The Problem

When the receiver advertises `receiver_window = 0`, the sender must stop all data
transmission. But the sender still needs to maintain the connection and detect when
the receiver clears space. Without a mechanism, the sender would wait forever — the
receiver's next SM might be lost (UDP is unreliable), and no data flow means no
trigger for a new SM.

### 6.2 The Solution: Zero-Length Heartbeat Probes

When the sender detects a zero-window condition, it switches to probe mode:
periodic zero-length DATA frames (heartbeats) that carry no payload but keep the
connection alive and prompt the receiver to re-evaluate its window.

```zig
/// Zero-window probe logic. Called by the sender event loop when
/// canSend() returns false.
pub const ZeroWindowProbe = struct {
    /// Interval between probes when in zero-window state.
    const probe_interval_ns: i64 = 100 * std.time.ns_per_ms; // 100ms

    /// Check if we should send a zero-window probe.
    /// Returns true if:
    ///   1. The window is zero (or effectively zero — less than one MTU)
    ///   2. Enough time has passed since the last probe
    pub fn shouldProbe(fc: *const SenderFlowControl, now_ns: i64) bool {
        const remaining = fc.remainingWindow();
        if (remaining > constants.default_mtu_length) return false;

        return (now_ns - fc.last_probe_sent_ns) >= probe_interval_ns;
    }

    /// Send a zero-length heartbeat DATA frame to the peer.
    /// This frame carries:
    ///   - frame_length = 40 (header only, no payload)
    ///   - flags = UNFRAGMENTED
    ///   - The current sequence_number (no increment)
    ///
    /// The receiver will:
    ///   1. Confirm the sender is alive
    ///   2. Re-evaluate its receiver window
    ///   3. Send a Status Message if window > 0
    pub fn sendProbe(
        fc: *SenderFlowControl,
        peer: *PeerSender,
        now_ns: i64,
        sendFrameFn: *const fn (peer: *PeerSender, buf: []const u8) void,
    ) void {
        var probe_buf: [constants.data_frame_header_length]u8 = undefined;
        encodeHeartbeatFrame(&probe_buf, peer);
        sendFrameFn(peer, &probe_buf);
        fc.last_probe_sent_ns = now_ns;
    }

    const std = @import("std");
};
```

### 6.3 Integration in Sender Event Loop

```zig
/// Sender event loop duty cycle — flow control section.
fn senderDoCycle(self: *SenderEventLoop) u32 {
    var work_count: u32 = 0;
    const now_ns = Clock.monotonicNanos();

    // ... (steps 1-2: drain completions, process incoming SMs/NAKs) ...

    // Step 3: Drain send ring buffer
    work_count += self.drainSendRingBuffer(constants.send_batch_limit);

    // Step 4: If flow controlled, consider zero-window probes
    for (self.peers) |*peer| {
        if (!peer.flow_control.canSend(constants.default_mtu_length)) {
            // Window exhausted for this peer
            if (ZeroWindowProbe.shouldProbe(&peer.flow_control, now_ns)) {
                ZeroWindowProbe.sendProbe(
                    &peer.flow_control,
                    peer,
                    now_ns,
                    self.sendFrameFn,
                );
                work_count += 1;
            }
        }
    }

    // Step 5: Regular heartbeats (even when not flow controlled)
    if (now_ns >= self.next_heartbeat_ns) {
        self.sendHeartbeatsToAllPeers();
        self.next_heartbeat_ns = now_ns + constants.udp_heartbeat_interval_ns;
        work_count += 1;
    }

    return work_count;
}
```

### 6.4 Receiver Response to Probes

On the receiver side, a zero-length heartbeat DATA frame is handled like any
other DATA frame — it goes through `insertPacket` (which is a no-op for zero
payload) and triggers the receiver event loop to re-evaluate the SM schedule.

The periodic SM trigger (200ms timeout) will fire regardless, but the probe also
causes the receiver to check the eager SM condition. If consumption has advanced
since the last SM, the eager threshold may be met, and an SM is sent immediately.

This creates a feedback loop:

```
Sender:  zero-window → probe every 100ms
                            ↓
Receiver: receives probe → checks eager SM → if consumption advanced → sends SM
                                                                         ↓
Sender:   receives SM → updates send_limit → if window > 0 → resumes sending
```

---

## 7. Flow Control Counters

**File: `src/flow_control/counters.zig`**

Track these counters for monitoring and debugging. All counters are monotonically
increasing `i64` values, atomically incremented. They live in the broker's shared
counter values buffer (see
[architecture §19](../architecture.md#19-counters--monitoring)).

```zig
/// Flow-control-related counter IDs.
pub const FlowControlCounterId = enum(u16) {
    /// Send ring buffer back-pressure events. Incremented when the
    /// sender event loop cannot drain a message from the send ring
    /// buffer because canSend() returned false.
    ///
    /// High values indicate: receiver is slow, receiver window is too
    /// small, or network latency is causing SM delays.
    send_rb_back_pressure = 0x0100,

    /// Service message ring buffer back-pressure events. Incremented
    /// when the receiver event loop cannot route a message to a target
    /// service because the service's ring buffer is full.
    ///
    /// High values indicate: the target service is consuming too slowly.
    service_back_pressure = 0x0101,

    /// Flow control under-runs. Incremented when a received DATA frame
    /// has a position below the receiver's current consumption position.
    /// This means the frame is stale — either a duplicate retransmit or
    /// a severely delayed packet.
    ///
    /// Occasional under-runs are normal (retransmits after NAK). Frequent
    /// under-runs suggest aggressive retransmit timing or network issues.
    flow_control_under_runs = 0x0102,

    /// Flow control over-runs. Incremented when a received DATA frame
    /// has a position beyond `consumption_position + log_capacity`.
    /// This means the sender violated its flow control contract — it
    /// sent data beyond the advertised window.
    ///
    /// This should NEVER happen. Any non-zero value is a bug in the
    /// sender's flow control logic.
    flow_control_over_runs = 0x0103,

    /// Status Messages sent. Total count of SMs sent by the receiver
    /// to all peers.
    status_messages_sent = 0x0104,

    /// Status Messages received. Total count of SMs received by the
    /// sender from all peers.
    status_messages_received = 0x0105,

    /// Zero-window probes sent. Total count of zero-length heartbeat
    /// probes sent while in flow-controlled state.
    zero_window_probes_sent = 0x0106,

    /// Total bytes sent (across all peers). Useful for throughput
    /// calculation.
    bytes_sent = 0x0110,

    /// Total bytes received (across all peers).
    bytes_received = 0x0111,
};
```

### 7.1 Counter Validation Checks

Under-run and over-run detection is performed at packet insertion time in the
receiver:

```zig
/// Validate a received frame's position against flow control bounds.
/// Called before inserting into the receive log buffer.
pub fn validateFramePosition(
    fc: *const ReceiverFlowControl,
    frame_position: i64,
    frame_length: i32,
    counters: *CounterManager,
) enum { valid, under_run, over_run } {
    // Under-run: frame is behind consumption position
    if (frame_position + @as(i64, frame_length) <= fc.consumption_position) {
        counters.increment(.flow_control_under_runs);
        return .under_run;
    }

    // Over-run: frame is beyond receivable range
    const max_receivable = fc.consumption_position + fc.log_capacity;
    if (frame_position >= max_receivable) {
        counters.increment(.flow_control_over_runs);
        return .over_run;
    }

    return .valid;
}
```

---

## 8. Edge Cases & Recovery

### 8.1 Receiver Falls Behind (Log Buffer Overwrite)

If the receiver's consumption position falls behind by more than `log_capacity`,
the circular receive log buffer would overwrite unconsumed data. This is a
**critical error** — data integrity is lost.

**Detection:**

```zig
/// Check for receive log buffer overwrite condition.
/// Must be called periodically by the receiver event loop.
fn checkForLogOverwrite(fc: *const ReceiverFlowControl) bool {
    const tail = fc.recv_log.loadTailPosition();
    const max_valid_distance = fc.log_capacity;

    // If tail has advanced more than one full buffer past consumption,
    // data has been (or is about to be) overwritten.
    return (tail - fc.consumption_position) > max_valid_distance;
}
```

**Recovery:**

1. Log a critical error with both positions and the capacity.
2. Reset the peer connection (tear down, re-SETUP).
3. Increment a `log_overwrite` error counter.
4. This should **never happen** if flow control is functioning correctly — the
   half-capacity window cap ensures the sender cannot outrun the receiver by more
   than half a buffer, and the receiver won't advance `tail_position` beyond what
   it has received.

If this error occurs, investigate:
- Is the receiver failing to send SMs? (Check `status_messages_sent` counter)
- Is the sender ignoring the window? (Check `flow_control_over_runs` counter)
- Is there a bug in `calculateReceiverWindow()`?

### 8.2 Sender Stalls (No Status Messages)

If the sender receives no Status Messages from a peer for an extended period, the
peer may be unreachable.

```zig
/// Stale peer detection constants.
const peer_sm_timeout_ns: i64 = 5 * std.time.ns_per_s; // 5 seconds

/// Called by the sender event loop's periodic health check.
fn checkPeerHealth(self: *SenderEventLoop, now_ns: i64) void {
    for (self.peers) |*peer| {
        if (peer.flow_control.isStale(now_ns, peer_sm_timeout_ns)) {
            // Report to control loop for disconnect handling
            self.control_commands.enqueue(.{
                .type = .peer_unreachable,
                .node_id = peer.node_id,
                .last_sm_ns = peer.flow_control.last_sm_received_ns,
            });
        }
    }
}
```

The control loop then decides whether to tear down the connection, attempt
reconnection, or report the peer as dead to the cluster manager.

### 8.3 Stale Status Messages

UDP can deliver packets out of order. A stale SM (with an older
`consumption_position`) could arrive after a newer one. The monotonicity check
in `onStatusMessage()` handles this:

```zig
// Only advance — never retreat
if (proposed_limit > self.send_limit) {
    self.send_limit = proposed_limit;
}
```

This ensures the sender never shrinks its window due to a reordered SM. The
worst case is that the sender briefly has a slightly larger window than the
receiver intended — but this is bounded by one SM's worth of consumption
progress and is self-correcting on the next SM.

### 8.4 Receiver Restarts Mid-Connection

If Broker B crashes and restarts, its consumption position resets to 0 while
Broker A's `send_position` is well advanced. The first SM from the restarted
Broker B will carry `consumption_position = 0`:

```
Broker A state before crash:
  send_position = 5,000,000
  send_limit    = 6,000,000

Broker B restarts, sends SM:
  consumption_position = 0
  receiver_window      = 2,097,152

Broker A receives SM:
  proposed_limit = 0 + 2,097,152 = 2,097,152
  Current send_limit = 6,000,000
  → proposed < current → IGNORED (monotonicity)
```

This is correct behavior — Broker A cannot "go back" to an old position. The
resolution requires a full connection reset:

1. Broker B sends SM with `send_setup` flag.
2. Broker A sees the flag, resets its peer state, and sends a new SETUP.
3. Both sides start fresh with `send_position = 0`, `send_limit = 0`.
4. Broker B responds with initial SM, and flow resumes.

```zig
/// Handle SM with send_setup flag — receiver is requesting a reset.
fn onStatusMessageWithSetupFlag(
    self: *SenderEventLoop,
    peer: *PeerSender,
    sm: StatusMessageFlyweight,
    now_ns: i64,
) void {
    // Reset all sender state for this peer
    peer.flow_control.reset();
    peer.retransmit_buffer.reset();
    peer.sequence_number = 0;

    // Re-establish connection
    self.sendSetupFrame(peer);

    // The next SM from the receiver will set the initial window
}
```

### 8.5 Network Partition and Reconnection

During a network partition, both sides stop receiving from the other:

- **Sender:** Gets no SMs → `send_limit` freezes → eventually detected as stale.
- **Receiver:** Gets no DATA frames → tail doesn't advance → window stays open
  (nothing to consume).

On reconnection:
1. The sender is detected as stale and the control loop initiates reconnection.
2. A new SETUP is sent; flow control state is reset on both sides.
3. Any in-flight data from before the partition is lost (by design — BRZ provides
   at-most-once delivery across network failures).

---

## 9. Configuration

All flow control parameters are defined in `src/platform/constants.zig` and can
be overridden via the broker configuration file.

| Parameter | Constant Name | Default | Description |
|-----------|---------------|---------|-------------|
| Receive log buffer length | `default_recv_log_buffer_length` | 4 MB | Determines the maximum possible receiver window (`capacity / 2` = 2 MB) |
| MTU length | `default_mtu_length` | 1,408 bytes | Affects per-frame overhead. Frames larger than `mtu - 40` are fragmented |
| SM timeout | `sm_timeout_ns` | 200 ms | Maximum interval between periodic Status Messages |
| Initial receiver window | (computed) | `capacity / 2` | Window advertised on connection setup |
| Eager SM threshold | (computed) | `window / 4` | Consumption advance that triggers an immediate SM |
| Zero-window probe interval | `ZeroWindowProbe.probe_interval_ns` | 100 ms | Interval between heartbeat probes when window is zero |
| Peer SM stale timeout | `peer_sm_timeout_ns` | 5 s | Time without SM before a peer is considered unreachable |

### 9.1 Buffer Sizing Guidelines

The receive log buffer length directly controls throughput and latency
characteristics:

| Buffer Size | Max Window | Characteristics |
|-------------|------------|-----------------|
| 1 MB | 512 KB | Low memory footprint; back-pressure kicks in quickly; suitable for low-throughput links |
| 4 MB (default) | 2 MB | Good balance for most workloads; supports burst absorption |
| 16 MB | 8 MB | High throughput; large burst tolerance; higher memory cost per peer |
| 64 MB | 32 MB | Extreme throughput scenarios; significant memory commitment |

**Rule of thumb:** The buffer should be large enough to absorb one round-trip
time's worth of data at peak throughput. For a 1 Gbps link with 1ms RTT, that's
~125 KB — well within the default 4 MB. The default is intentionally conservative
to handle bursty workloads and slow consumers.

---

## 10. Testing

**File: `src/flow_control/test_flow_control.zig`**

### 10.1 Unit Tests: Sender Flow Control

```zig
const std = @import("std");
const testing = std.testing;
const SenderFlowControl = @import("sender_flow_control.zig").SenderFlowControl;

test "canSend returns false when window is exhausted" {
    // Given
    var fc = SenderFlowControl{};
    fc.send_position = 1000;
    fc.send_limit = 1500;

    // When / Then
    try testing.expect(fc.canSend(500));      // exactly at limit
    try testing.expect(!fc.canSend(501));      // one byte over
    try testing.expect(!fc.canSend(1000));     // well over
}

test "canSend returns false before first SM" {
    // Given — fresh state, no SM received yet
    var fc = SenderFlowControl{};

    // When / Then
    try testing.expect(!fc.canSend(1));        // can't send anything
    try testing.expectEqual(@as(i64, 0), fc.send_limit);
}

test "onStatusMessage advances send_limit" {
    // Given
    var fc = SenderFlowControl{};
    fc.send_position = 0;

    // When — first SM arrives
    fc.onStatusMessage(0, 2_097_152, 1000);

    // Then
    try testing.expectEqual(@as(i64, 2_097_152), fc.send_limit);
    try testing.expectEqual(@as(i64, 0), fc.consumption_position);
    try testing.expectEqual(@as(i32, 2_097_152), fc.receiver_window);
    try testing.expect(fc.canSend(1_000_000));
}

test "onStatusMessage never decreases send_limit (monotonicity)" {
    // Given
    var fc = SenderFlowControl{};
    fc.onStatusMessage(1000, 5000, 100);
    try testing.expectEqual(@as(i64, 6000), fc.send_limit);

    // When — stale SM arrives with lower limit
    fc.onStatusMessage(500, 2000, 200);

    // Then — send_limit unchanged
    try testing.expectEqual(@as(i64, 6000), fc.send_limit);
}

test "onStatusMessage advances send_limit with progressing consumption" {
    // Given
    var fc = SenderFlowControl{};
    fc.onStatusMessage(0, 5000, 100);
    try testing.expectEqual(@as(i64, 5000), fc.send_limit);

    // When — receiver consumed 3000 bytes, window still 5000
    fc.onStatusMessage(3000, 5000, 200);

    // Then — send_limit = 3000 + 5000 = 8000
    try testing.expectEqual(@as(i64, 8000), fc.send_limit);
    try testing.expectEqual(@as(i64, 3000), fc.consumption_position);
}

test "onFrameSent advances send_position" {
    // Given
    var fc = SenderFlowControl{};
    fc.send_limit = 10000;

    // When
    fc.onFrameSent(1500);
    fc.onFrameSent(2000);

    // Then
    try testing.expectEqual(@as(i64, 3500), fc.send_position);
    try testing.expect(fc.canSend(6500));   // exactly remaining
    try testing.expect(!fc.canSend(6501));
}

test "remainingWindow returns correct value" {
    // Given
    var fc = SenderFlowControl{};
    fc.send_position = 3000;
    fc.send_limit = 10000;

    // Then
    try testing.expectEqual(@as(i64, 7000), fc.remainingWindow());
}

test "isStale returns false when no SM has ever been received" {
    // Given
    var fc = SenderFlowControl{};

    // Then — never received an SM, so not "stale" — just unconnected
    try testing.expect(!fc.isStale(1_000_000_000, 5_000_000_000));
}

test "isStale returns true when SM is overdue" {
    // Given
    var fc = SenderFlowControl{};
    fc.last_sm_received_ns = 1_000_000_000; // 1 second

    // When — 7 seconds later with a 5 second timeout
    const now_ns: i64 = 7_000_000_000;
    const timeout_ns: i64 = 5_000_000_000;

    // Then
    try testing.expect(fc.isStale(now_ns, timeout_ns));
}

test "reset clears all state" {
    // Given
    var fc = SenderFlowControl{};
    fc.send_position = 5000;
    fc.send_limit = 10000;
    fc.consumption_position = 3000;
    fc.receiver_window = 7000;
    fc.last_sm_received_ns = 999;
    fc.consecutive_flow_control_failures = 42;

    // When
    fc.reset();

    // Then
    try testing.expectEqual(@as(i64, 0), fc.send_position);
    try testing.expectEqual(@as(i64, 0), fc.send_limit);
    try testing.expectEqual(@as(i64, 0), fc.consumption_position);
    try testing.expectEqual(@as(i32, 0), fc.receiver_window);
    try testing.expectEqual(@as(i64, 0), fc.last_sm_received_ns);
    try testing.expectEqual(@as(u64, 0), fc.consecutive_flow_control_failures);
}
```

### 10.2 Unit Tests: Receiver Flow Control

```zig
const std = @import("std");
const testing = std.testing;
const ReceiverFlowControl = @import("receiver_flow_control.zig").ReceiverFlowControl;
const ReceiveLogBuffer = @import("../memory/receive_log.zig").ReceiveLogBuffer;

test "calculateReceiverWindow returns half capacity when buffer is empty" {
    // Given
    var log = try ReceiveLogBuffer.allocate(std.heap.page_allocator, 4 * 1024 * 1024);
    defer log.close();
    var fc = ReceiverFlowControl.init(&log);

    // When
    const window = fc.calculateReceiverWindow();

    // Then — empty buffer: available = capacity, capped at capacity/2
    try testing.expectEqual(@as(i32, 2 * 1024 * 1024), window);
}

test "calculateReceiverWindow shrinks as data accumulates" {
    // Given
    const capacity: i64 = 4 * 1024 * 1024;
    var log = try ReceiveLogBuffer.allocate(std.heap.page_allocator, @intCast(capacity));
    defer log.close();
    var fc = ReceiverFlowControl.init(&log);

    // Simulate: 3MB of data sitting in the buffer (tail advanced,
    // consumption has not)
    const buffered: i64 = 3 * 1024 * 1024;
    log.storeTailPosition(buffered);
    fc.consumption_position = 0;

    // When
    const window = fc.calculateReceiverWindow();

    // Then — available = 4MB - 3MB = 1MB, < cap/2, so window = 1MB
    try testing.expectEqual(@as(i32, 1 * 1024 * 1024), window);
}

test "calculateReceiverWindow returns zero when buffer is full" {
    // Given
    const capacity: i64 = 4 * 1024 * 1024;
    var log = try ReceiveLogBuffer.allocate(std.heap.page_allocator, @intCast(capacity));
    defer log.close();
    var fc = ReceiverFlowControl.init(&log);

    // Simulate: entire buffer is occupied
    log.storeTailPosition(capacity);
    fc.consumption_position = 0;

    // When
    const window = fc.calculateReceiverWindow();

    // Then
    try testing.expectEqual(@as(i32, 0), window);
}

test "calculateReceiverWindow grows as data is consumed" {
    // Given
    const capacity: i64 = 4 * 1024 * 1024;
    var log = try ReceiveLogBuffer.allocate(std.heap.page_allocator, @intCast(capacity));
    defer log.close();
    var fc = ReceiverFlowControl.init(&log);

    // Buffer was full, now consumed 2MB
    log.storeTailPosition(capacity);
    fc.consumption_position = 2 * 1024 * 1024;

    // When
    const window = fc.calculateReceiverWindow();

    // Then — available = 4MB - (4MB - 2MB) = 2MB, equals cap/2
    try testing.expectEqual(@as(i32, 2 * 1024 * 1024), window);
}

test "initialWindow is half capacity" {
    // Given
    var log = try ReceiveLogBuffer.allocate(std.heap.page_allocator, 4 * 1024 * 1024);
    defer log.close();
    var fc = ReceiverFlowControl.init(&log);

    // Then
    try testing.expectEqual(@as(i32, 2 * 1024 * 1024), fc.initialWindow());
}
```

### 10.3 Unit Tests: Status Message Flyweight

```zig
const std = @import("std");
const testing = std.testing;
const StatusMessageFlyweight = @import("status_message.zig").StatusMessageFlyweight;
const constants = @import("../platform/constants.zig");

test "StatusMessageFlyweight round-trips all fields" {
    // Given
    var buf: [StatusMessageFlyweight.encoded_length]u8 = undefined;
    const sm = StatusMessageFlyweight.wrap(&buf);

    // When
    sm.encode(
        7,           // node_id
        1_500_000,   // consumption_position
        2_097_152,   // receiver_window
        0x01,        // flags (send_setup)
    );

    // Then
    try testing.expectEqual(@as(i32, 28), sm.frameLength());
    try testing.expectEqual(@as(u8, 0x01), sm.flags());
    try testing.expectEqual(@as(u8, 7), sm.nodeId());
    try testing.expectEqual(@as(i64, 1_500_000), sm.consumptionPosition());
    try testing.expectEqual(@as(i32, 2_097_152), sm.receiverWindow());
}

test "StatusMessageFlyweight encodes correct frame type" {
    // Given
    var buf: [StatusMessageFlyweight.encoded_length]u8 = undefined;
    const sm = StatusMessageFlyweight.wrap(&buf);

    // When
    sm.encode(1, 0, 1000, 0);

    // Then — frame_type at offset 6, little-endian u16
    const frame_type = std.mem.readInt(u16, buf[6..8], .little);
    try testing.expectEqual(constants.frame_type_sm, frame_type);
}
```

### 10.4 Unit Tests: Status Message Timing

```zig
const std = @import("std");
const testing = std.testing;
const ReceiverFlowControl = @import("receiver_flow_control.zig").ReceiverFlowControl;
const StatusMessageScheduler = @import("status_message.zig").StatusMessageScheduler;
const constants = @import("../platform/constants.zig");
const ReceiveLogBuffer = @import("../memory/receive_log.zig").ReceiveLogBuffer;

test "eager SM fires when consumption advances by window/4" {
    // Given
    var log = try ReceiveLogBuffer.allocate(std.heap.page_allocator, 4 * 1024 * 1024);
    defer log.close();
    var fc = ReceiverFlowControl.init(&log);
    var scheduler = StatusMessageScheduler{};
    var sm_sent = false;

    // Initial SM sent: window = 2MB, consumption = 0
    fc.last_sm_consumption_position = 0;
    fc.last_sm_receiver_window = 2 * 1024 * 1024;
    fc.last_sm_sent_ns = 0;

    // Consumption advances by exactly window/4 = 512KB
    fc.consumption_position = 512 * 1024;

    const sendFn = struct {
        fn send(_: []const u8, _: u8) void {
            // captured via pointer
        }
    }.send;

    // When
    const work = scheduler.maybeSendStatusMessage(
        &fc, 1, 0, 1_000_000, // 1ms — well within periodic timeout
        sendFn,
    );

    // Then — eager SM should have fired
    try testing.expectEqual(@as(u32, 1), work);
    try testing.expectEqual(@as(i64, 512 * 1024), fc.last_sm_consumption_position);
}

test "periodic SM fires after timeout even without consumption advance" {
    // Given
    var log = try ReceiveLogBuffer.allocate(std.heap.page_allocator, 4 * 1024 * 1024);
    defer log.close();
    var fc = ReceiverFlowControl.init(&log);
    var scheduler = StatusMessageScheduler{};

    fc.last_sm_consumption_position = 0;
    fc.last_sm_receiver_window = 2 * 1024 * 1024;
    fc.last_sm_sent_ns = 0;
    fc.consumption_position = 0; // no advance

    const sendFn = struct {
        fn send(_: []const u8, _: u8) void {}
    }.send;

    // When — 300ms later (> 200ms timeout)
    const now_ns: i64 = 300 * std.time.ns_per_ms;
    const work = scheduler.maybeSendStatusMessage(&fc, 1, 0, now_ns, sendFn);

    // Then — periodic SM should have fired
    try testing.expectEqual(@as(u32, 1), work);
}

test "no SM sent when neither eager nor periodic threshold met" {
    // Given
    var log = try ReceiveLogBuffer.allocate(std.heap.page_allocator, 4 * 1024 * 1024);
    defer log.close();
    var fc = ReceiverFlowControl.init(&log);
    var scheduler = StatusMessageScheduler{};

    fc.last_sm_consumption_position = 0;
    fc.last_sm_receiver_window = 2 * 1024 * 1024;
    fc.last_sm_sent_ns = 0;
    fc.consumption_position = 100; // tiny advance, well below window/4

    const sendFn = struct {
        fn send(_: []const u8, _: u8) void {}
    }.send;

    // When — 50ms later (< 200ms timeout)
    const now_ns: i64 = 50 * std.time.ns_per_ms;
    const work = scheduler.maybeSendStatusMessage(&fc, 1, 0, now_ns, sendFn);

    // Then — no SM
    try testing.expectEqual(@as(u32, 0), work);
}
```

### 10.5 Integration Tests

These tests exercise the full flow control loop across sender and receiver states.

```zig
test "full flow control cycle: send → consume → SM → send more" {
    // Given — sender with initial window from first SM
    var sender_fc = SenderFlowControl{};
    sender_fc.onStatusMessage(0, 2_097_152, 100); // 2MB window

    // Simulate sending 1.5MB
    sender_fc.onFrameSent(1_500_000);
    try testing.expectEqual(@as(i64, 1_500_000), sender_fc.send_position);
    try testing.expect(sender_fc.canSend(500_000)); // 597,152 remaining

    // Simulate sending remaining window
    sender_fc.onFrameSent(597_152);
    try testing.expect(!sender_fc.canSend(1)); // window exhausted

    // Receiver consumed 1MB, sends new SM
    sender_fc.onStatusMessage(1_000_000, 2_097_152, 200);
    // new send_limit = 1_000_000 + 2_097_152 = 3_097_152
    try testing.expectEqual(@as(i64, 3_097_152), sender_fc.send_limit);

    // Sender can now send again
    const remaining = sender_fc.send_limit - sender_fc.send_position;
    try testing.expect(remaining > 0);
    try testing.expect(sender_fc.canSend(1));
}

test "back-pressure: zero window stops sender" {
    // Given
    var sender_fc = SenderFlowControl{};
    sender_fc.onStatusMessage(0, 1_000_000, 100);

    // Send up to limit
    sender_fc.onFrameSent(1_000_000);
    try testing.expect(!sender_fc.canSend(1));

    // Receiver sends SM with zero window (buffer full)
    sender_fc.onStatusMessage(0, 0, 200);
    // send_limit = max(1_000_000, 0 + 0) = 1_000_000 (monotonicity)
    try testing.expect(!sender_fc.canSend(1)); // still blocked

    // Receiver consumes everything, sends SM with full window
    sender_fc.onStatusMessage(1_000_000, 2_097_152, 300);
    // send_limit = 1_000_000 + 2_097_152 = 3_097_152
    try testing.expect(sender_fc.canSend(1)); // unblocked
}

test "receiver restart detected via send_setup flag" {
    // Given — established connection
    var sender_fc = SenderFlowControl{};
    sender_fc.onStatusMessage(0, 2_097_152, 100);
    sender_fc.onFrameSent(1_000_000);
    try testing.expectEqual(@as(i64, 1_000_000), sender_fc.send_position);

    // When — receiver restarts, sends SM with consumption=0
    // The proposed_limit = 0 + 2_097_152 = 2_097_152 <= current 2_097_152
    // → send_limit doesn't advance (monotonicity protects us)
    sender_fc.onStatusMessage(0, 2_097_152, 200);
    try testing.expectEqual(@as(i64, 2_097_152), sender_fc.send_limit);

    // Recovery: reset (triggered by send_setup flag handling)
    sender_fc.reset();
    try testing.expectEqual(@as(i64, 0), sender_fc.send_position);
    try testing.expectEqual(@as(i64, 0), sender_fc.send_limit);

    // New initial SM after SETUP
    sender_fc.onStatusMessage(0, 2_097_152, 300);
    try testing.expectEqual(@as(i64, 2_097_152), sender_fc.send_limit);
    try testing.expect(sender_fc.canSend(1_000_000));
}
```

---

## 11. File Structure

```
src/
  flow_control/
    sender_flow_control.zig      # SenderFlowControl struct — send_position, send_limit, canSend()
    receiver_flow_control.zig    # ReceiverFlowControl struct — consumption tracking, window calc
    status_message.zig           # StatusMessageFlyweight + StatusMessageScheduler + SM flags
    zero_window_probe.zig        # ZeroWindowProbe — heartbeat probing when window = 0
    back_pressure.zig            # BackPressureStrategy enum, blocking write helpers
    counters.zig                 # FlowControlCounterId enum, validation checks
    test_flow_control.zig        # All unit and integration tests
  flow_control.zig               # Public re-exports
```

### Re-exports

```zig
// src/flow_control.zig

pub const SenderFlowControl = @import("flow_control/sender_flow_control.zig").SenderFlowControl;
pub const ReceiverFlowControl = @import("flow_control/receiver_flow_control.zig").ReceiverFlowControl;
pub const StatusMessageFlyweight = @import("flow_control/status_message.zig").StatusMessageFlyweight;
pub const StatusMessageScheduler = @import("flow_control/status_message.zig").StatusMessageScheduler;
pub const sm_flags = @import("flow_control/status_message.zig").sm_flags;
pub const ZeroWindowProbe = @import("flow_control/zero_window_probe.zig").ZeroWindowProbe;
pub const BackPressureStrategy = @import("flow_control/back_pressure.zig").BackPressureStrategy;
pub const FlowControlCounterId = @import("flow_control/counters.zig").FlowControlCounterId;
pub const validateFramePosition = @import("flow_control/counters.zig").validateFramePosition;
```

---

## Summary of Dependencies

```
┌─────────────────────────────────────────────────────────────────────┐
│                        07 — Flow Control                            │
├─────────────────────────────────────────────────────────────────────┤
│                                                                     │
│  DEPENDS ON:                                                        │
│    01 — Platform Abstraction                                        │
│         AtomicI32, AtomicI64     (atomic load/store in recv log)    │
│         Clock.monotonicNanos()   (SM timing, probe timing)          │
│         ProcessSynchronizer      (blocking back-pressure)           │
│         constants                (frame types, timing, MTU)         │
│                                                                     │
│    02 — Memory Layout                                               │
│         ReceiveLogBuffer         (capacity, tail_position access)   │
│         BlockingTrailer          (writer_wait_state for blocking)   │
│                                                                     │
│    05 — Send Path                                                   │
│         SenderEventLoop          (integration point for canSend)    │
│         RetransmitBuffer         (reset on peer reconnection)       │
│         Send ring buffer         (back-pressure propagation)        │
│                                                                     │
│    06 — Receive Path                                                │
│         ReceiverEventLoop        (SM scheduling, frame routing)     │
│         LossDetector             (SM with send_setup on loss)       │
│         insertPacket             (frame position validation)        │
│                                                                     │
│  DEPENDED ON BY:                                                    │
│    08 — Service IPC              (back-pressure to services)        │
│    09 — Control Plane            (peer health → disconnect)         │
│    10 — Threading Model          (duty cycle integration)           │
│                                                                     │
└─────────────────────────────────────────────────────────────────────┘
```

---

*Previous: [06 — Receive Path](06-receive-path.md) · Next: [08 — Service IPC](08-service-ipc.md)*