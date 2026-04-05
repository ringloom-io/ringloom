//! Control message encoding and decoding.
//!
//! Control messages are small, schema-fixed messages exchanged between
//! services and the broker over the control ring buffers.
//! Each message starts with a 2-byte template_id followed by the payload.

const std = @import("std");

/// Read the template ID from the start of a control message payload.
pub fn readTemplateId(payload: []const u8) u16 {
    if (payload.len < 2) return 0;
    return std.mem.readInt(u16, payload[0..2], .little);
}

// ── RegisterService ───────────────────────────────────────────────────

pub const RegisterServiceData = struct {
    service_id: i32,
    service_name: []const u8,
    leader_election_enabled: bool,
};

/// Encode a RegisterService control message.
/// Layout: [template_id: u16][service_id: i32][leader_election: u8][name_len: u16][name: bytes]
pub fn encodeRegisterService(buf: []u8, data: RegisterServiceData) usize {
    var offset: usize = 0;

    // Template ID = 1.
    std.mem.writeInt(u16, buf[offset..][0..2], 1, .little);
    offset += 2;

    // Service ID.
    std.mem.writeInt(i32, buf[offset..][0..4], data.service_id, .little);
    offset += 4;

    // Leader election enabled.
    buf[offset] = if (data.leader_election_enabled) 1 else 0;
    offset += 1;

    // Service name (length-prefixed).
    const name_len: u16 = @intCast(data.service_name.len);
    std.mem.writeInt(u16, buf[offset..][0..2], name_len, .little);
    offset += 2;

    @memcpy(buf[offset..][0..data.service_name.len], data.service_name);
    offset += data.service_name.len;

    return offset;
}

/// Decode a RegisterService control message.
pub fn decodeRegisterService(payload: []const u8) RegisterServiceData {
    var offset: usize = 2; // skip template_id

    const service_id = std.mem.readInt(i32, payload[offset..][0..4], .little);
    offset += 4;

    const leader_election_enabled = payload[offset] != 0;
    offset += 1;

    const name_len: usize = std.mem.readInt(u16, payload[offset..][0..2], .little);
    offset += 2;

    return .{
        .service_id = service_id,
        .service_name = payload[offset..][0..name_len],
        .leader_election_enabled = leader_election_enabled,
    };
}

// ── RegistrationResponse ──────────────────────────────────────────────

pub const RegistrationResponseData = struct {
    service_id: i32,
    node_id: i16,
    success: bool,
};

/// Encode a RegistrationResponse control message.
/// Layout: [template_id: u16][service_id: i32][node_id: i16][success: u8]
pub fn encodeRegistrationResponse(buf: []u8, data: RegistrationResponseData) usize {
    var offset: usize = 0;

    std.mem.writeInt(u16, buf[offset..][0..2], 2, .little);
    offset += 2;

    std.mem.writeInt(i32, buf[offset..][0..4], data.service_id, .little);
    offset += 4;

    std.mem.writeInt(i16, buf[offset..][0..2], data.node_id, .little);
    offset += 2;

    buf[offset] = if (data.success) 1 else 0;
    offset += 1;

    return offset;
}

/// Decode a RegistrationResponse control message.
pub fn decodeRegistrationResponse(payload: []const u8) RegistrationResponseData {
    var offset: usize = 2; // skip template_id

    const service_id = std.mem.readInt(i32, payload[offset..][0..4], .little);
    offset += 4;

    const node_id = std.mem.readInt(i16, payload[offset..][0..2], .little);
    offset += 2;

    const success = payload[offset] != 0;

    return .{
        .service_id = service_id,
        .node_id = node_id,
        .success = success,
    };
}

// ── SubscribeToServiceUpdates ─────────────────────────────────────────

pub const SubscribeData = struct {
    subscriber_service_id: i32,
    target_service_name: []const u8,
};

/// Encode a SubscribeToServiceUpdates control message.
/// Layout: [template_id: u16][subscriber_service_id: i32][name_len: u16][name: bytes]
pub fn encodeSubscribeToServiceUpdates(buf: []u8, data: SubscribeData) usize {
    var offset: usize = 0;

    std.mem.writeInt(u16, buf[offset..][0..2], 3, .little);
    offset += 2;

    std.mem.writeInt(i32, buf[offset..][0..4], data.subscriber_service_id, .little);
    offset += 4;

    const name_len: u16 = @intCast(data.target_service_name.len);
    std.mem.writeInt(u16, buf[offset..][0..2], name_len, .little);
    offset += 2;

    @memcpy(buf[offset..][0..data.target_service_name.len], data.target_service_name);
    offset += data.target_service_name.len;

    return offset;
}

/// Decode a SubscribeToServiceUpdates control message.
pub fn decodeSubscribeToServiceUpdates(payload: []const u8) SubscribeData {
    var offset: usize = 2;

    const subscriber_service_id = std.mem.readInt(i32, payload[offset..][0..4], .little);
    offset += 4;

    const name_len: usize = std.mem.readInt(u16, payload[offset..][0..2], .little);
    offset += 2;

    return .{
        .subscriber_service_id = subscriber_service_id,
        .target_service_name = payload[offset..][0..name_len],
    };
}

// ── ServiceInstances ──────────────────────────────────────────────────

pub const ServiceInstanceData = struct {
    service_id: i32,
    service_name: []const u8,
    node_id: i16,
    is_leader: bool,
};

/// Encode a ServiceInstances control message (one instance per message).
/// Layout: [template_id: u16][service_id: i32][node_id: i16][is_leader: u8][name_len: u16][name: bytes]
pub fn encodeServiceInstance(buf: []u8, data: ServiceInstanceData) usize {
    var offset: usize = 0;

    std.mem.writeInt(u16, buf[offset..][0..2], 4, .little);
    offset += 2;

    std.mem.writeInt(i32, buf[offset..][0..4], data.service_id, .little);
    offset += 4;

    std.mem.writeInt(i16, buf[offset..][0..2], data.node_id, .little);
    offset += 2;

    buf[offset] = if (data.is_leader) 1 else 0;
    offset += 1;

    const name_len: u16 = @intCast(data.service_name.len);
    std.mem.writeInt(u16, buf[offset..][0..2], name_len, .little);
    offset += 2;

    @memcpy(buf[offset..][0..data.service_name.len], data.service_name);
    offset += data.service_name.len;

    return offset;
}

/// Decode a ServiceInstances control message.
pub fn decodeServiceInstance(payload: []const u8) ServiceInstanceData {
    var offset: usize = 2;

    const service_id = std.mem.readInt(i32, payload[offset..][0..4], .little);
    offset += 4;

    const node_id = std.mem.readInt(i16, payload[offset..][0..2], .little);
    offset += 2;

    const is_leader = payload[offset] != 0;
    offset += 1;

    const name_len: usize = std.mem.readInt(u16, payload[offset..][0..2], .little);
    offset += 2;

    return .{
        .service_id = service_id,
        .service_name = payload[offset..][0..name_len],
        .node_id = node_id,
        .is_leader = is_leader,
    };
}

// ── UnregisterService ─────────────────────────────────────────────────

pub const UnregisterServiceData = struct {
    service_id: i32,
};

/// Encode an UnregisterService control message.
/// Layout: [template_id: u16][service_id: i32]
pub fn encodeUnregisterService(buf: []u8, data: UnregisterServiceData) usize {
    var offset: usize = 0;

    std.mem.writeInt(u16, buf[offset..][0..2], 5, .little);
    offset += 2;

    std.mem.writeInt(i32, buf[offset..][0..4], data.service_id, .little);
    offset += 4;

    return offset;
}

/// Decode an UnregisterService control message.
pub fn decodeUnregisterService(payload: []const u8) UnregisterServiceData {
    const service_id = std.mem.readInt(i32, payload[2..][0..4], .little);
    return .{ .service_id = service_id };
}

// ── LeaderChanged ─────────────────────────────────────────────────────

pub const LeaderChangedData = struct {
    leader_service_id: i32,
    service_name: []const u8,
};

/// Encode a LeaderChanged control message.
/// Layout: [template_id: u16][leader_service_id: i32][name_len: u16][name: bytes]
pub fn encodeLeaderChanged(buf: []u8, data: LeaderChangedData) usize {
    var offset: usize = 0;

    std.mem.writeInt(u16, buf[offset..][0..2], 6, .little);
    offset += 2;

    std.mem.writeInt(i32, buf[offset..][0..4], data.leader_service_id, .little);
    offset += 4;

    const name_len: u16 = @intCast(data.service_name.len);
    std.mem.writeInt(u16, buf[offset..][0..2], name_len, .little);
    offset += 2;

    @memcpy(buf[offset..][0..data.service_name.len], data.service_name);
    offset += data.service_name.len;

    return offset;
}

/// Decode a LeaderChanged control message.
pub fn decodeLeaderChanged(payload: []const u8) LeaderChangedData {
    var offset: usize = 2;

    const leader_service_id = std.mem.readInt(i32, payload[offset..][0..4], .little);
    offset += 4;

    const name_len: usize = std.mem.readInt(u16, payload[offset..][0..2], .little);
    offset += 2;

    return .{
        .leader_service_id = leader_service_id,
        .service_name = payload[offset..][0..name_len],
    };
}

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "readTemplateId on empty payload returns 0" {
    try testing.expectEqual(@as(u16, 0), readTemplateId(&.{}));
}

test "RegisterService encode and decode roundtrip" {
    var buf: [256]u8 = undefined;
    const len = encodeRegisterService(&buf, .{
        .service_id = 42,
        .service_name = "test-service",
        .leader_election_enabled = true,
    });

    try testing.expectEqual(@as(u16, 1), readTemplateId(buf[0..len]));

    const decoded = decodeRegisterService(buf[0..len]);
    try testing.expectEqual(@as(i32, 42), decoded.service_id);
    try testing.expectEqualStrings("test-service", decoded.service_name);
    try testing.expect(decoded.leader_election_enabled);
}

test "RegistrationResponse encode and decode roundtrip" {
    var buf: [256]u8 = undefined;
    const len = encodeRegistrationResponse(&buf, .{
        .service_id = 7,
        .node_id = 3,
        .success = true,
    });

    try testing.expectEqual(@as(u16, 2), readTemplateId(buf[0..len]));

    const decoded = decodeRegistrationResponse(buf[0..len]);
    try testing.expectEqual(@as(i32, 7), decoded.service_id);
    try testing.expectEqual(@as(i16, 3), decoded.node_id);
    try testing.expect(decoded.success);
}

test "SubscribeToServiceUpdates encode and decode roundtrip" {
    var buf: [256]u8 = undefined;
    const len = encodeSubscribeToServiceUpdates(&buf, .{
        .subscriber_service_id = 10,
        .target_service_name = "pricing",
    });

    try testing.expectEqual(@as(u16, 3), readTemplateId(buf[0..len]));

    const decoded = decodeSubscribeToServiceUpdates(buf[0..len]);
    try testing.expectEqual(@as(i32, 10), decoded.subscriber_service_id);
    try testing.expectEqualStrings("pricing", decoded.target_service_name);
}

test "ServiceInstance encode and decode roundtrip" {
    var buf: [256]u8 = undefined;
    const len = encodeServiceInstance(&buf, .{
        .service_id = 5,
        .service_name = "risk-engine",
        .node_id = 2,
        .is_leader = true,
    });

    try testing.expectEqual(@as(u16, 4), readTemplateId(buf[0..len]));

    const decoded = decodeServiceInstance(buf[0..len]);
    try testing.expectEqual(@as(i32, 5), decoded.service_id);
    try testing.expectEqual(@as(i16, 2), decoded.node_id);
    try testing.expect(decoded.is_leader);
    try testing.expectEqualStrings("risk-engine", decoded.service_name);
}

test "UnregisterService encode and decode roundtrip" {
    var buf: [256]u8 = undefined;
    const len = encodeUnregisterService(&buf, .{ .service_id = 99 });

    try testing.expectEqual(@as(u16, 5), readTemplateId(buf[0..len]));

    const decoded = decodeUnregisterService(buf[0..len]);
    try testing.expectEqual(@as(i32, 99), decoded.service_id);
}

test "LeaderChanged encode and decode roundtrip" {
    var buf: [256]u8 = undefined;
    const len = encodeLeaderChanged(&buf, .{
        .leader_service_id = 3,
        .service_name = "pricing",
    });

    try testing.expectEqual(@as(u16, 6), readTemplateId(buf[0..len]));

    const decoded = decodeLeaderChanged(buf[0..len]);
    try testing.expectEqual(@as(i32, 3), decoded.leader_service_id);
    try testing.expectEqualStrings("pricing", decoded.service_name);
}
