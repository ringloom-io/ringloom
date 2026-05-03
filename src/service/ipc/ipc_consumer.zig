//! IPC Consumer — polls a service's messages ring buffer for incoming messages.
//!
//! Each service has one IpcConsumer on its messages ring buffer, driven by
//! the MessageConsumer agent.

const std = @import("std");
const ringloom_common = @import("ringloom_common");
const RingBuffer = ringloom_common.concurrent.ring_buffer.RingBuffer;
const constants = ringloom_common.memory.constants;

/// Callback signature for processing messages read from the ring buffer.
pub const MessageHandler = RingBuffer.MessageHandler;

pub const IpcConsumer = struct {
    ring_buffer: RingBuffer,

    const Self = @This();

    /// Initialize an IpcConsumer over this service's messages buffer.
    /// The buffer must be properly aligned and sized for a ring buffer.
    pub fn init(messages_buffer: []align(constants.ring_buffer_alignment) u8) !Self {
        return .{
            .ring_buffer = try RingBuffer.init(messages_buffer, false, null, null),
        };
    }

    /// Poll the ring buffer for available messages.
    ///
    /// Calls `handler` for each message, up to `limit` messages per poll.
    /// Returns the number of messages processed (the "work count" for the
    /// duty-cycle event loop).
    ///
    /// This is the single-consumer side of the MPSC ring buffer. Only one
    /// thread may call `poll` at a time.
    pub fn poll(self: *Self, handler: MessageHandler, limit: u32) u32 {
        return self.ring_buffer.read(handler, limit);
    }
};
