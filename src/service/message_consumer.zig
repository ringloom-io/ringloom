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
        const h = self.handler orelse return 0;
        tls_consumer = self;
        tls_handler = h;
        defer {
            tls_consumer = null;
            tls_handler = null;
        }
        return self.ring_buffer.read(onMessage, read_limit);
    }

    /// EventLoop-compatible function pointer (casts context to *Self).
    pub fn doWorkFn(ctx: *anyopaque) u32 {
        const self: *Self = @ptrCast(@alignCast(ctx));
        return self.doWork();
    }

    /// No-op close function for EventLoop compatibility.
    pub fn onCloseFn(_: *anyopaque) void {}
};

threadlocal var tls_consumer: ?*MessageConsumer = null;
threadlocal var tls_handler: ?RingBuffer.MessageHandler = null;

fn onMessage(msg_type_id: i32, payload: []const u8) void {
    if (tls_consumer) |consumer| {
        if (consumer.service_counters) |counters| {
            counters.increment(.messages_received);
            counters.add(.bytes_received, @intCast(payload.len));
        }
    }
    if (tls_handler) |handler| {
        if (message_header.tryDecodeEnvelope(msg_type_id, payload)) |envelope| {
            handler(message_header.msgTypeFromTemplateId(envelope.header.template_id), envelope.payload);
        } else {
            handler(msg_type_id, payload);
        }
    }
}
