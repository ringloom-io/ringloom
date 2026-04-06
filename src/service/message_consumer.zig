//! MessageConsumer — agent that polls the service's messages ring buffer (Channel 4).
//!
//! Delegates each message to the application's registered handler.

const std = @import("std");
const brz_common = @import("brz_common");
const RingBuffer = brz_common.concurrent.ring_buffer.RingBuffer;
const constants = brz_common.memory.constants;

/// Default number of messages to read per poll cycle.
const read_limit: u32 = 256;

pub const MessageConsumer = struct {
    ring_buffer: RingBuffer,
    handler: ?RingBuffer.MessageHandler,

    const Self = @This();

    pub fn init(messages_buffer: []align(constants.ring_buffer_alignment) u8) !Self {
        return .{
            .ring_buffer = try RingBuffer.init(messages_buffer, false, null, null),
            .handler = null,
        };
    }

    pub fn setHandler(self: *Self, handler: RingBuffer.MessageHandler) void {
        self.handler = handler;
    }

    /// Duty-cycle function. Called by the ThreadRunner's event loop.
    /// Returns the number of messages processed (work count).
    pub fn doWork(self: *Self) u32 {
        const h = self.handler orelse return 0;
        return self.ring_buffer.read(h, read_limit);
    }

    /// EventLoop-compatible function pointer (casts context to *Self).
    pub fn doWorkFn(ctx: *anyopaque) u32 {
        const self: *Self = @ptrCast(@alignCast(ctx));
        return self.doWork();
    }

    /// No-op close function for EventLoop compatibility.
    pub fn onCloseFn(_: *anyopaque) void {}
};
