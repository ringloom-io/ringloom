//! Message routing for the BRZ broker receive path.
//!
//! Routes complete messages (unfragmented or fully reassembled) to target
//! service ring buffers. Also provides the ServiceRegistry for service
//! lookup and the frame consumption marking mechanism.

const std = @import("std");
const constants = @import("../platform/constants.zig");
const frames = @import("../protocol/frames.zig");
const RingBuffer = @import("../concurrent/ring_buffer.zig").RingBuffer;
const ReceiveLogBuffer = @import("../memory/receive_log.zig").ReceiveLogBuffer;

/// Message type ID for application messages written to service ring buffers.
/// Must be >= 1 (ring buffer requires positive msg_type_id).
pub const msg_type_application: i32 = 1;

/// Sentinel value written to the length prefix of a consumed frame.
/// The consumption position scanner checks for this value to know
/// that a slot has been processed and can be counted as consumed.
pub const frame_consumed_marker: i32 = -1;

/// Result of a route attempt.
pub const RouteResult = enum {
    /// Frame successfully written to the service's ring buffer.
    success,
    /// Target service not found in the registry.
    unknown_service,
    /// Target service's ring buffer is full (back-pressure).
    back_pressure,
};

/// Route a complete message to the target service's messages ring buffer.
///
/// The frame includes the 40-byte DataFrameHeader. The header is preserved
/// in the ring buffer write so that the service can read routing fields
/// (source_node_id, source_service_id, correlation_id, template_id, etc.)
/// without a separate metadata channel.
///
/// If the target service is unknown, the frame is marked consumed.
/// If the service's ring buffer is full, the frame is NOT marked consumed,
/// allowing consumption_position to stall and apply back-pressure upstream.
pub fn routeToService(
    registry: *const ServiceRegistry,
    recv_log: ?*ReceiveLogBuffer,
    target_service_id: u16,
    frame: []const u8,
    frame_length: i32,
    position: i64,
) RouteResult {
    // ── Look up the target service ────────────────────────────────────
    const service = registry.lookup(target_service_id) orelse {
        // Unknown service — mark as consumed to avoid stalling the
        // entire recv log for one unknown service.
        if (recv_log) |log| {
            markFrameConsumed(log, position);
        }
        return .unknown_service;
    };

    // ── Write frame to service ring buffer ────────────────────────────
    const write_len = @as(usize, @intCast(frame_length));
    const write_data = frame[0..write_len];

    service.messages_ring_buffer.write(msg_type_application, write_data) catch |err| {
        switch (err) {
            error.BufferFull => {
                // Service ring buffer is full — DO NOT mark consumed.
                // The consumption_position will NOT advance past this point,
                // which shrinks the receiver window and applies back-pressure.
                return .back_pressure;
            },
            else => {
                // Other write errors (shouldn't happen with valid data).
                // Mark consumed to avoid stalling.
                if (recv_log) |log| {
                    markFrameConsumed(log, position);
                }
                return .unknown_service;
            },
        }
    };

    // Frame successfully written — mark consumed in recv log.
    if (recv_log) |log| {
        markFrameConsumed(log, position);
    }
    return .success;
}

/// Mark a frame at the given position as consumed.
/// This overwrites the length prefix with the consumed marker (-1).
/// The aligned-length field (written during insertion) is preserved
/// so that the consumption position scanner can advance past it.
pub fn markFrameConsumed(recv_log: *ReceiveLogBuffer, position: i64) void {
    const index: usize = @intCast(@as(u64, @bitCast(position)) & recv_log.mask);
    const length_ptr: *volatile i32 = @ptrCast(@alignCast(&recv_log.data[index]));
    @atomicStore(i32, length_ptr, frame_consumed_marker, .release);
}

/// Service registry — maps serviceId to service state.
/// Updated by the control loop via commands. Read by the receiver for routing.
pub const ServiceRegistry = struct {
    /// Fixed-size array indexed by service ID. Max 256 services per broker.
    entries: [constants.default_max_services]?ServiceEntry,

    pub const ServiceEntry = struct {
        service_id: u16,
        service_name: []const u8,
        node_id: u8,
        messages_ring_buffer: *RingBuffer,
    };

    /// Initialize with all entries set to null.
    pub fn init() ServiceRegistry {
        return .{
            .entries = [_]?ServiceEntry{null} ** constants.default_max_services,
        };
    }

    /// Look up a service by ID. Returns null if the service is not registered.
    pub fn lookup(self: *const ServiceRegistry, service_id: u16) ?*const ServiceEntry {
        if (service_id >= constants.default_max_services) return null;
        if (self.entries[service_id]) |*entry| {
            return entry;
        }
        return null;
    }

    /// Register a service entry.
    pub fn register(self: *ServiceRegistry, entry: ServiceEntry) void {
        if (entry.service_id < constants.default_max_services) {
            self.entries[entry.service_id] = entry;
        }
    }

    /// Deregister a service by ID.
    pub fn deregister(self: *ServiceRegistry, service_id: u16) void {
        if (service_id < constants.default_max_services) {
            self.entries[service_id] = null;
        }
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "ServiceRegistry lookup returns null for unregistered service" {
    // Given
    const registry = ServiceRegistry.init();

    // When / Then
    try testing.expect(registry.lookup(5) == null);
    try testing.expect(registry.lookup(0) == null);
    try testing.expect(registry.lookup(255) == null);
}

test "ServiceRegistry lookup returns null for out-of-range service ID" {
    // Given
    const registry = ServiceRegistry.init();

    // When / Then
    try testing.expect(registry.lookup(256) == null);
    try testing.expect(registry.lookup(1000) == null);
}

test "ServiceRegistry register and lookup" {
    // Given
    var registry = ServiceRegistry.init();

    // Create a ring buffer for the service
    var rb_buf: [4096 + 768]u8 align(8) = undefined;
    @memset(&rb_buf, 0);
    var rb = RingBuffer.init(&rb_buf, false, null, null) catch unreachable;

    registry.register(.{
        .service_id = 5,
        .service_name = "test-service",
        .node_id = 0,
        .messages_ring_buffer = &rb,
    });

    // When
    const entry = registry.lookup(5);

    // Then
    try testing.expect(entry != null);
    try testing.expectEqual(@as(u16, 5), entry.?.service_id);
    try testing.expectEqualStrings("test-service", entry.?.service_name);
}

test "ServiceRegistry deregister removes entry" {
    // Given
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

    // When
    registry.deregister(5);

    // Then
    try testing.expect(registry.lookup(5) == null);
}

test "routeToService writes to service ring buffer" {
    // Given
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

    // Build a frame
    var frame_buf: [128]u8 align(8) = [_]u8{0} ** 128;
    const header: *frames.DataFrameHeader = @ptrCast(@alignCast(&frame_buf));
    header.* = .{
        .frame_length = 80,
        .flags = constants.flag_unfragmented,
        .target_service_id = 5,
        .source_node_id = 2,
        .source_service_id = 1,
    };

    // When
    const result = routeToService(&registry, null, 5, frame_buf[0..80], 80, 0);

    // Then
    try testing.expect(result == .success);

    // Verify the ring buffer received the message by reading it back
    var messages_read: u32 = 0;
    messages_read = rb.read(struct {
        fn handler(_: i32, _: []const u8) void {}
    }.handler, 10);
    try testing.expectEqual(@as(u32, 1), messages_read);
}

test "routeToService returns unknown_service for unregistered service" {
    // Given
    const registry = ServiceRegistry.init();

    var frame_buf: [128]u8 align(8) = [_]u8{0} ** 128;

    // When
    const result = routeToService(&registry, null, 99, &frame_buf, 80, 0);

    // Then
    try testing.expect(result == .unknown_service);
}

test "markFrameConsumed writes sentinel to length prefix" {
    // Given
    var log = try ReceiveLogBuffer.allocate(4096);
    defer log.close();

    // Insert a packet first
    const recv_log_buf = @import("receive_log_buffer.zig");
    var frame_buf: [128]u8 align(8) = [_]u8{0} ** 128;
    const header: *frames.DataFrameHeader = @ptrCast(@alignCast(&frame_buf));
    header.* = .{
        .frame_length = 80,
        .flags = constants.flag_unfragmented,
        .sequence_number = 0,
    };
    recv_log_buf.insertPacket(&log, frame_buf[0..80]);

    // Verify frame is readable
    try testing.expect(recv_log_buf.readFrame(&log, 0) != null);

    // When — mark as consumed
    markFrameConsumed(&log, 0);

    // Then — readFrame should return null (length is now -1, which is <= 0)
    try testing.expect(recv_log_buf.readFrame(&log, 0) == null);

    // Verify the sentinel value is written
    const length_ptr: *const volatile i32 = @ptrCast(@alignCast(&log.data[0]));
    const val = @atomicLoad(i32, length_ptr, .acquire);
    try testing.expectEqual(frame_consumed_marker, val);
}
