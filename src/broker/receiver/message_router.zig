//! Message routing for the RingLoom broker TCP receive path.
//!
//! Routes application payloads to target service ring buffers. The TCP transport
//! frame is stripped before delivery, while the logical template ID is preserved
//! as the service-visible ring-buffer message type.
//!
//! TCP provides reliable ordered delivery, so there is no receive log buffer,
//! no consumption position tracking, and no frame consumed marking.

const std = @import("std");
const ringloom_common = @import("ringloom_common");
const constants = ringloom_common.platform.constants;
const RingBuffer = ringloom_common.concurrent.ring_buffer.RingBuffer;
const message_header = ringloom_common.message.message_header;
const MessageHeader = ringloom_common.message.MessageHeader;
const frame_parser = ringloom_common.protocol.frame_parser;
const TcpFrameHeader = frame_parser.TcpFrameHeader;

/// Result of a route attempt.
pub const RouteResult = enum {
    /// Payload successfully written to the service's ring buffer.
    success,
    /// Target service not found in the registry.
    unknown_service,
    /// Target service's ring buffer is full (back-pressure).
    service_full,
};

/// Route an application payload to the target service's messages ring buffer.
///
/// The caller is responsible for stripping the 24-byte TcpFrameHeader before
/// calling this function. The service receives only the application-layer
/// payload, while `template_id` is mapped to the ring-buffer `msg_type_id` so
/// remote `ServiceClient.tryClaim(template_id, ...)` matches the local path.
///
/// If the target service is unknown, the payload is dropped.
/// If the service's ring buffer is full, the payload is dropped (always-read model).
pub fn routeToService(
    registry: *const ServiceRegistry,
    header: TcpFrameHeader,
    payload: []const u8,
) RouteResult {
    const target_service_id = header.target_service_id;
    const service = registry.lookup(target_service_id) orelse {
        return .unknown_service;
    };

    if (payload.len > std.math.maxInt(i32)) return .unknown_service;
    const total_len = MessageHeader.encoded_length + payload.len;
    var claim = service.messages_ring_buffer.tryClaim(constants.message_envelope_msg_type_id, total_len) orelse {
        return .service_full;
    };
    MessageHeader.encode(claim.buffer[0..MessageHeader.encoded_length], .{
        .source_node_id = @intCast(header.source_node_id),
        .source_service_id = @intCast(header.source_service_id),
        .target_node_id = @intCast(header.target_node_id),
        .target_service_id = @intCast(header.target_service_id),
        .template_id = header.template_id,
        .correlation_id = header.correlation_id,
        .flags = header.flags,
        .payload_length = @intCast(payload.len),
    });
    if (payload.len > 0) {
        @memcpy(claim.buffer[MessageHeader.encoded_length..][0..payload.len], payload);
    }
    claim.commit();

    return .success;
}

pub fn routePlainToService(
    registry: *const ServiceRegistry,
    target_service_id: u16,
    template_id: u16,
    payload: []const u8,
) RouteResult {
    const service = registry.lookup(target_service_id) orelse {
        return .unknown_service;
    };

    service.messages_ring_buffer.write(message_header.msgTypeFromTemplateId(template_id), payload) catch |err| {
        switch (err) {
            error.BufferFull => return .service_full,
            else => return .unknown_service,
        }
    };

    return .success;
}

/// Service registry — maps serviceId to service state.
pub const ServiceRegistry = struct {
    entries: [constants.default_max_services]?ServiceEntry,

    pub const ServiceEntry = struct {
        service_id: u16,
        service_name: []const u8,
        node_id: u8,
        messages_ring_buffer: *RingBuffer,
    };

    pub fn init() ServiceRegistry {
        return .{
            .entries = [_]?ServiceEntry{null} ** constants.default_max_services,
        };
    }

    pub fn lookup(self: *const ServiceRegistry, service_id: u16) ?*const ServiceEntry {
        if (service_id >= constants.default_max_services) return null;
        if (self.entries[service_id]) |*entry| {
            return entry;
        }
        return null;
    }

    pub fn register(self: *ServiceRegistry, entry: ServiceEntry) void {
        if (entry.service_id < constants.default_max_services) {
            self.entries[entry.service_id] = entry;
        }
    }

    pub fn deregister(self: *ServiceRegistry, service_id: u16) void {
        if (service_id < constants.default_max_services) {
            self.entries[service_id] = null;
        }
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "ServiceRegistry lookup returns null for unregistered service" {
    const registry = ServiceRegistry.init();
    try testing.expect(registry.lookup(5) == null);
    try testing.expect(registry.lookup(0) == null);
    try testing.expect(registry.lookup(255) == null);
}

test "ServiceRegistry lookup returns null for out-of-range service ID" {
    const registry = ServiceRegistry.init();
    try testing.expect(registry.lookup(256) == null);
    try testing.expect(registry.lookup(1000) == null);
}

test "ServiceRegistry register and lookup" {
    var registry = ServiceRegistry.init();

    var rb_buf: [4096 + 768]u8 align(8) = undefined;
    @memset(&rb_buf, 0);
    var rb = RingBuffer.init(&rb_buf, false, null, null) catch unreachable;

    registry.register(.{
        .service_id = 5,
        .service_name = "test-service",
        .node_id = 0,
        .messages_ring_buffer = &rb,
    });

    const entry = registry.lookup(5);
    try testing.expect(entry != null);
    try testing.expectEqual(@as(u16, 5), entry.?.service_id);
    try testing.expectEqualStrings("test-service", entry.?.service_name);
}

test "ServiceRegistry deregister removes entry" {
    var registry = ServiceRegistry.init();

    var rb_buf: [4096 + 768]u8 align(8) = undefined;
    @memset(&rb_buf, 0);
    var rb = RingBuffer.init(&rb_buf, false, null, null) catch unreachable;

    registry.register(.{
        .service_id = 5,
        .service_name = "test-service",
        .node_id = 0,
        .messages_ring_buffer = &rb,
    });

    registry.deregister(5);
    try testing.expect(registry.lookup(5) == null);
}

test "routeToService writes to service ring buffer" {
    var rb_buf: [4096 + 768]u8 align(8) = undefined;
    @memset(&rb_buf, 0);
    var rb = RingBuffer.init(&rb_buf, false, null, null) catch unreachable;

    var registry = ServiceRegistry.init();
    registry.register(.{
        .service_id = 5,
        .service_name = "test-service",
        .node_id = 0,
        .messages_ring_buffer = &rb,
    });

    // Route an application payload (header already stripped by caller).
    const payload = "hello-world-payload";
    const result = routePlainToService(&registry, 5, 0, payload);
    try testing.expect(result == .success);

    // Verify the ring buffer received the correct msg_type_id and exact payload.
    test_received_msg_type = 0;
    test_received_payload_len = 0;
    const messages_read = rb.read(&testCaptureHandler, 10);
    try testing.expectEqual(@as(u32, 1), messages_read);
    try testing.expectEqual(constants.application_msg_type_id, test_received_msg_type);
    try testing.expectEqual(payload.len, test_received_payload_len);
    try testing.expectEqualSlices(u8, payload, test_received_payload_buf[0..test_received_payload_len]);
}

test "routeToService preserves non-zero template ID as message type" {
    var rb_buf: [4096 + 768]u8 align(8) = undefined;
    @memset(&rb_buf, 0);
    var rb = RingBuffer.init(&rb_buf, false, null, null) catch unreachable;

    var registry = ServiceRegistry.init();
    registry.register(.{
        .service_id = 5,
        .service_name = "test-service",
        .node_id = 0,
        .messages_ring_buffer = &rb,
    });

    const payload = "templated-payload";
    const result = routePlainToService(&registry, 5, 42, payload);
    try testing.expect(result == .success);

    test_received_msg_type = 0;
    test_received_payload_len = 0;
    const messages_read = rb.read(&testCaptureHandler, 10);
    try testing.expectEqual(@as(u32, 1), messages_read);
    try testing.expectEqual(@as(i32, 42), test_received_msg_type);
    try testing.expectEqual(payload.len, test_received_payload_len);
    try testing.expectEqualSlices(u8, payload, test_received_payload_buf[0..test_received_payload_len]);
}

test "routeToService returns unknown_service for unregistered service" {
    const registry = ServiceRegistry.init();
    const result = routePlainToService(&registry, 99, 0, "test-payload");
    try testing.expect(result == .unknown_service);
}

test "routeToService wraps remote frame metadata in envelope" {
    var rb_buf: [4096 + 768]u8 align(8) = undefined;
    @memset(&rb_buf, 0);
    var rb = RingBuffer.init(&rb_buf, false, null, null) catch unreachable;

    var registry = ServiceRegistry.init();
    registry.register(.{
        .service_id = 5,
        .service_name = "test-service",
        .node_id = 1,
        .messages_ring_buffer = &rb,
    });

    const payload = "response";
    const result = routeToService(&registry, .{
        .frame_length = TcpFrameHeader.size + payload.len,
        .flags = constants.flag_unfragmented,
        .source_node_id = 7,
        .target_node_id = 1,
        .source_service_id = 11,
        .target_service_id = 5,
        .template_id = 42,
        .correlation_id = 1234,
    }, payload);
    try testing.expect(result == .success);

    test_received_msg_type = 0;
    test_received_payload_len = 0;
    const messages_read = rb.read(&testCaptureHandler, 10);
    try testing.expectEqual(@as(u32, 1), messages_read);
    try testing.expectEqual(constants.message_envelope_msg_type_id, test_received_msg_type);
    const envelope = message_header.tryDecodeEnvelope(test_received_msg_type, test_received_payload_buf[0..test_received_payload_len]).?;
    try testing.expectEqual(@as(i64, 1234), envelope.header.correlation_id);
    try testing.expectEqual(@as(i16, 11), envelope.header.source_service_id);
    try testing.expectEqual(@as(u16, 42), envelope.header.template_id);
    try testing.expectEqualStrings(payload, envelope.payload);
}

// ── Test helpers ──────────────────────────────────────────────────────

var test_received_msg_type: i32 = 0;
var test_received_payload_len: usize = 0;
var test_received_payload_buf: [256]u8 = undefined;

fn testCaptureHandler(msg_type_id: i32, payload: []const u8) void {
    test_received_msg_type = msg_type_id;
    test_received_payload_len = payload.len;
    if (payload.len <= test_received_payload_buf.len) {
        @memcpy(test_received_payload_buf[0..payload.len], payload);
    }
}
