//! Receiver-side flow control for a single peer link.
//!
//! Tracks the consumption position (highest contiguous byte position that
//! has been successfully routed to a downstream service) and calculates
//! the receiver window — how many bytes the sender is allowed to write
//! ahead of the consumption position.

const std = @import("std");
const constants = @import("../platform/constants.zig");
const Clock = @import("../platform/clock.zig").Clock;
const ReceiveLogBuffer = @import("../memory/receive_log.zig").ReceiveLogBuffer;

/// Frame status values in the receive log buffer.
/// The frame_length field doubles as a status marker:
///   > 0  → frame present, not yet consumed
///   = 0  → slot empty (gap)
///  -1   → frame consumed (routed to service)
pub const frame_consumed_marker: i32 = -1;

/// Frame alignment within the receive log.
const frame_alignment: usize = 32;

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

    /// Advances the consumption position by scanning forward through
    /// the receive log for contiguously consumed frames.
    ///
    /// This function is called after each successful route-to-service
    /// operation. It is O(k) where k is the number of contiguous
    /// consumed frames ahead of the current position — typically 1
    /// in the common case, but may batch-advance after a burst.
    pub fn updateConsumptionPosition(self: *Self) void {
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

    /// Resets state for a new connection.
    pub fn reset(self: *Self) void {
        self.consumption_position = 0;
        self.last_sm_consumption_position = 0;
        self.last_sm_receiver_window = 0;
        self.last_sm_sent_ns = 0;
    }
};

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
