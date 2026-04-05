//! Sender-side flow control for a single peer link.
//!
//! Tracks how far the sender has written (`send_position`) and how far the
//! receiver has granted permission (`send_limit`). The sender MUST NOT
//! transmit any frame whose `send_position + frame_length` would exceed
//! `send_limit`. All positions are monotonically increasing byte offsets
//! that never wrap.

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
