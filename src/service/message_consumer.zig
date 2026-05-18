//! MessageConsumer — agent that polls the service's messages ring buffer (Channel 4).
//!
//! Delegates each message to the application's registered handler.

const std = @import("std");
const ringloom_common = @import("ringloom_common");
const RingBuffer = ringloom_common.concurrent.ring_buffer.RingBuffer;
const constants = ringloom_common.memory.constants;
const message_header = ringloom_common.message.message_header;
const ServiceCounters = ringloom_common.monitoring.ServiceCounters;

/// Default number of messages to read per poll cycle.
const read_limit: u32 = 256;

pub const MessageConsumer = struct {
    ring_buffer: RingBuffer,
    handler: ?RingBuffer.MessageHandler,
    service_counters: ?*ServiceCounters,
    batch_bytes_received: u64 = 0,

    const Self = @This();

    pub fn init(
        messages_buffer: []align(constants.ring_buffer_alignment) u8,
        service_counters: ?*ServiceCounters,
    ) !Self {
        return .{
            .ring_buffer = try RingBuffer.init(messages_buffer, false, null, null),
            .handler = null,
            .service_counters = service_counters,
        };
    }

    pub fn setHandler(self: *Self, handler: RingBuffer.MessageHandler) void {
        self.handler = handler;
    }

    /// Duty-cycle function. Called by the ThreadRunner's event loop.
    /// Returns the number of messages processed (work count).
    pub fn doWork(self: *Self) u32 {
        if (self.handler == null) return 0;

        self.batch_bytes_received = 0;
        const messages_read = self.ring_buffer.readWithContext(
            @ptrCast(self),
            onMessage,
            read_limit,
        );

        if (messages_read > 0) {
            if (self.service_counters) |counters| {
                counters.add(.messages_received, messages_read);
                counters.add(.bytes_received, @intCast(self.batch_bytes_received));
            }
        }

        return messages_read;
    }

    /// EventLoop-compatible function pointer (casts context to *Self).
    pub fn doWorkFn(ctx: *anyopaque) u32 {
        const self: *Self = @ptrCast(@alignCast(ctx));
        return self.doWork();
    }

    /// No-op close function for EventLoop compatibility.
    pub fn onCloseFn(_: *anyopaque) void {}
};

fn onMessage(context: *anyopaque, msg_type_id: i32, payload: []const u8) void {
    const consumer: *MessageConsumer = @ptrCast(@alignCast(context));
    consumer.batch_bytes_received += payload.len;

    if (consumer.handler) |handler| {
        if (message_header.tryDecodeEnvelope(msg_type_id, payload)) |envelope| {
            handler(message_header.msgTypeFromTemplateId(envelope.header.template_id), envelope.payload);
        } else {
            handler(msg_type_id, payload);
        }
    }
}
