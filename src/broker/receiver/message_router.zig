//! Message routing for the BRZ broker TCP receive path.
//!
//! Routes complete TCP frames to target service ring buffers. Also provides
//! the ServiceRegistry for service lookup.
//!
//! TCP provides reliable ordered delivery, so there is no receive log buffer,
//! no consumption position tracking, and no frame consumed marking.

const std = @import("std");
const brz_common = @import("brz_common");
const constants = brz_common.platform.constants;
const RingBuffer = brz_common.concurrent.ring_buffer.RingBuffer;
const frame_parser = brz_common.protocol.frame_parser;
const TcpFrameHeader = frame_parser.TcpFrameHeader;

/// Message type ID for application messages written to service ring buffers.
pub const msg_type_application: i32 = 1;

/// Result of a route attempt.
pub const RouteResult = enum {
    /// Frame successfully written to the service's ring buffer.
    success,
    /// Target service not found in the registry.
    unknown_service,
    /// Target service's ring buffer is full (back-pressure).
    service_full,
};

/// Route a complete TCP frame to the target service's messages ring buffer.
///
/// The frame includes the 24-byte TcpFrameHeader. The header is preserved
/// in the ring buffer write so that the service can read routing fields
/// (source_node_id, source_service_id, correlation_id, template_id, etc.).
///
/// If the target service is unknown, the frame is dropped.
/// If the service's ring buffer is full, the frame is dropped (always-read model).
pub fn routeToService(
    registry: *const ServiceRegistry,
    target_service_id: u16,
    frame: []const u8,
    frame_length: u32,
) RouteResult {
    const service = registry.lookup(target_service_id) orelse {
        return .unknown_service;
    };

    const write_len: usize = @intCast(frame_length);
    const write_data = frame[0..write_len];

    service.messages_ring_buffer.write(msg_type_application, write_data) catch |err| {
        switch (err) {
            error.BufferFull => {
                return .service_full;
            },
            else => {
                return .unknown_service;
            },
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

    // Build a TCP frame
    var frame_buf: [64]u8 align(8) = [_]u8{0} ** 64;
    const header: *TcpFrameHeader = @ptrCast(@alignCast(&frame_buf));
    header.* = .{
        .frame_length = 64,
        .target_service_id = 5,
        .source_node_id = 2,
        .source_service_id = 1,
    };

    const result = routeToService(&registry, 5, &frame_buf, 64);
    try testing.expect(result == .success);

    // Verify ring buffer received the message
    var messages_read: u32 = 0;
    messages_read = rb.read(struct {
        fn handler(_: i32, _: []const u8) void {}
    }.handler, 10);
    try testing.expectEqual(@as(u32, 1), messages_read);
}

test "routeToService returns unknown_service for unregistered service" {
    const registry = ServiceRegistry.init();
    var frame_buf: [64]u8 align(8) = [_]u8{0} ** 64;
    const result = routeToService(&registry, 99, &frame_buf, 64);
    try testing.expect(result == .unknown_service);
}
