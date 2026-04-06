//! IPC Producer — writes messages to a target service's messages ring buffer.
//!
//! This is the `BrzProducer` implementation for same-host IPC. Each producer
//! wraps a ring buffer backed by a target service's messages buffer region.

const std = @import("std");
const brz_common = @import("brz_common");
const RingBuffer = brz_common.concurrent.ring_buffer.RingBuffer;
const constants = brz_common.memory.constants;

pub const IpcProducer = struct {
    ring_buffer: RingBuffer,

    const Self = @This();

    /// Initialize an IpcProducer over a target service's messages buffer.
    /// The buffer must be properly aligned and sized for a ring buffer
    /// (data capacity must be a power of two, with trailer appended).
    pub fn init(messages_buffer: []align(constants.ring_buffer_alignment) u8) !Self {
        return .{
            .ring_buffer = try RingBuffer.init(messages_buffer, false, null, null),
        };
    }

    /// Write a complete message into the target service's ring buffer.
    /// The message is copied into the ring buffer as a single record.
    ///
    /// Returns error.BufferFull if the ring buffer is full.
    /// Returns error.InvalidMsgTypeId if msg_type < 1.
    /// Returns error.MessageTooLong if the payload exceeds the max message length.
    pub fn write(self: *Self, msg_type: i32, payload: []const u8) RingBuffer.WriteError!void {
        return self.ring_buffer.write(msg_type, payload);
    }

    /// Claim a contiguous region in the ring buffer for zero-copy writing.
    ///
    /// Returns a `RingBuffer.Claim` with a `.buffer` slice pointing directly
    /// into shared memory. The caller writes the payload, then calls
    /// `claim.commit()` to make it visible to the consumer.
    ///
    /// Returns null if insufficient space is available or invalid parameters.
    pub fn tryClaim(self: *Self, msg_type: i32, length: usize) ?RingBuffer.Claim {
        return self.ring_buffer.tryClaim(msg_type, length);
    }

    /// Returns the ring buffer's current remaining capacity in bytes.
    /// Useful for back-pressure decisions.
    pub fn remainingCapacity(self: *Self) usize {
        return self.ring_buffer.getCapacity() - self.ring_buffer.size();
    }
};
