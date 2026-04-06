//! Control message encoding and decoding for the broker control plane.
//!
//! All control messages flow through MPSC ring buffers. Services write to the
//! broker's control ring buffer. The broker writes responses to each service's
//! control ring buffer.
//!
//! Messages use extern struct flyweight overlays directly on ring buffer memory —
//! no intermediate copies, no serialization frameworks. Extern structs guarantee
//! C ABI layout so @sizeOf matches the wire size exactly.
//!
//! Template IDs:
//!   1  RegisterService          Service → Broker
//!   2  RegistrationResponse     Broker → Service
//!   3  SubscribeToServiceUpdates Service → Broker
//!   4  ServiceInstances         Broker → Service
//!   5  UnregisterService        Service → Broker
//!   6  LeaderChanged            Broker → Service

const std = @import("std");

// ── Common Header ─────────────────────────────────────────────────────

/// 4-byte header prefixed to every control message.
/// The ring buffer's own record header (8 bytes) wraps this — the control
/// message header is the first thing inside the record payload.
pub const ControlMessageHeader = extern struct {
    /// Identifies the message type (see template_id table above).
    template_id: u16,

    /// Length of the message body AFTER this header, in bytes.
    /// Total message size = @sizeOf(ControlMessageHeader) + body_length.
    body_length: u16,
};

pub const header_size: usize = @sizeOf(ControlMessageHeader); // 4

// ── Message Definitions ───────────────────────────────────────────────

/// RegisterService (templateId = 1) — Service → Broker
/// Sent after the service creates its metadata file.
pub const RegisterServiceMsg = extern struct {
    header: ControlMessageHeader, // template_id = 1
    service_id: i32, // Assigned by the service from broker's nextServiceId
    node_id: i16, // 0 on send (broker fills in the real value)
    leader_election_enabled: u8, // 1 = enabled, 0 = disabled
    service_name_length: u8, // Length of the service name that follows
    // Followed by `service_name_length` bytes of UTF-8 service name.
};

/// RegistrationResponse (templateId = 2) — Broker → Service
/// Written to the service's control ring buffer.
pub const RegistrationResponseMsg = extern struct {
    header: ControlMessageHeader, // template_id = 2
    service_id: i32, // Confirmed service ID
    node_id: i16, // Broker's node ID (so the service knows its own nodeId)
    is_leader: u8, // 1 = this instance is the leader, 0 = not
    _padding: u8 = 0,
};

/// SubscribeToServiceUpdates (templateId = 3) — Service → Broker
/// The service wants to be notified when instances of a named service change.
pub const SubscribeMsg = extern struct {
    header: ControlMessageHeader, // template_id = 3
    local_service_id: i32, // The subscribing service's own ID
    service_name_length: u16, // Length of the target service name
    _padding: u16 = 0,
    // Followed by `service_name_length` bytes of UTF-8 target service name.
};

/// ServiceInstances (templateId = 4) — Broker → Service
/// Contains the COMPLETE current set of instances for a service name.
/// Not a delta — the receiver replaces its entire instance list.
pub const ServiceInstancesMsg = extern struct {
    header: ControlMessageHeader, // template_id = 4
    subscriber_service_id: i32, // The subscribing service's ID (for routing)
    instance_count: u16, // Number of ServiceInstanceEntry structs that follow
    service_name_length: u16, // Length of the service name
    // Followed by:
    //   1. `service_name_length` bytes of UTF-8 service name
    //   2. `instance_count` × @sizeOf(ServiceInstanceEntry) bytes of instance data
};

/// One entry inside a ServiceInstances message.
/// Fields use `align(1)` so entries can appear at arbitrary byte offsets
/// (they follow a variable-length service name in ServiceInstancesMsg).
pub const ServiceInstanceEntry = extern struct {
    service_id: i32 align(1),
    node_id: i16 align(1),
    is_leader: u8,
    _padding: u8 = 0,
};

/// UnregisterService (templateId = 5) — Service → Broker
/// Sent when a service shuts down gracefully.
pub const UnregisterServiceMsg = extern struct {
    header: ControlMessageHeader, // template_id = 5
    service_id: i32, // The service being unregistered
    node_id: i16, // 0 on send (broker uses local_node_id)
    _padding: i16 = 0,
};

/// LeaderChanged (templateId = 6) — Broker → Service
/// Sent to all local subscribers of a service name when the leader changes.
pub const LeaderChangedMsg = extern struct {
    header: ControlMessageHeader, // template_id = 6
    leader_service_id: i32, // The new leader's service ID
    leader_node_id: i16, // The new leader's node ID
    service_name_length: u16, // Length of the service name
    // Followed by `service_name_length` bytes of UTF-8 service name.
};

// ── Encoding Helpers ──────────────────────────────────────────────────

/// Encode a RegisterService message into `buf`. Returns the total encoded length.
pub fn encodeRegisterService(
    buf: []u8,
    service_id: i32,
    leader_election_enabled: bool,
    service_name: []const u8,
) u16 {
    const fixed_len = @sizeOf(RegisterServiceMsg);
    const total_len: u16 = @intCast(fixed_len + service_name.len);
    std.debug.assert(total_len <= buf.len);

    const msg_ptr: *RegisterServiceMsg = @ptrCast(@alignCast(buf.ptr));
    msg_ptr.* = .{
        .header = .{
            .template_id = 1,
            .body_length = total_len - @as(u16, header_size),
        },
        .service_id = service_id,
        .node_id = 0, // broker fills this in
        .leader_election_enabled = if (leader_election_enabled) 1 else 0,
        .service_name_length = @intCast(service_name.len),
    };

    @memcpy(buf[fixed_len..][0..service_name.len], service_name);
    return total_len;
}

/// Encode a RegistrationResponse message into `buf`. Returns the total encoded length.
pub fn encodeRegistrationResponse(
    buf: []u8,
    service_id: i32,
    node_id: i16,
    is_leader: bool,
) u16 {
    const total_len: u16 = @sizeOf(RegistrationResponseMsg);
    std.debug.assert(total_len <= buf.len);

    const msg_ptr: *RegistrationResponseMsg = @ptrCast(@alignCast(buf.ptr));
    msg_ptr.* = .{
        .header = .{
            .template_id = 2,
            .body_length = total_len - @as(u16, header_size),
        },
        .service_id = service_id,
        .node_id = node_id,
        .is_leader = if (is_leader) 1 else 0,
    };
    return total_len;
}

/// Encode a SubscribeToServiceUpdates message into `buf`. Returns the total encoded length.
pub fn encodeSubscribe(
    buf: []u8,
    local_service_id: i32,
    service_name: []const u8,
) u16 {
    const fixed_len = @sizeOf(SubscribeMsg);
    const total_len: u16 = @intCast(fixed_len + service_name.len);
    std.debug.assert(total_len <= buf.len);

    const msg_ptr: *SubscribeMsg = @ptrCast(@alignCast(buf.ptr));
    msg_ptr.* = .{
        .header = .{
            .template_id = 3,
            .body_length = total_len - @as(u16, header_size),
        },
        .local_service_id = local_service_id,
        .service_name_length = @intCast(service_name.len),
    };

    @memcpy(buf[fixed_len..][0..service_name.len], service_name);
    return total_len;
}

/// Encode a ServiceInstances message into `buf`. Returns the total encoded length.
/// `instances` is the full set of currently known instances for `service_name`.
pub fn encodeServiceInstances(
    buf: []u8,
    subscriber_service_id: i32,
    service_name: []const u8,
    instances: []const ServiceInstanceEntry,
) u16 {
    const fixed_len = @sizeOf(ServiceInstancesMsg);
    const name_end = fixed_len + service_name.len;
    const entries_size = instances.len * @sizeOf(ServiceInstanceEntry);
    const total_len: u16 = @intCast(name_end + entries_size);
    std.debug.assert(total_len <= buf.len);

    const msg_ptr: *ServiceInstancesMsg = @ptrCast(@alignCast(buf.ptr));
    msg_ptr.* = .{
        .header = .{
            .template_id = 4,
            .body_length = total_len - @as(u16, header_size),
        },
        .subscriber_service_id = subscriber_service_id,
        .instance_count = @intCast(instances.len),
        .service_name_length = @intCast(service_name.len),
    };

    @memcpy(buf[fixed_len..][0..service_name.len], service_name);

    if (instances.len > 0) {
        const entries_dst = buf[name_end..][0..entries_size];
        const entries_src: [*]const u8 = @ptrCast(instances.ptr);
        @memcpy(entries_dst, entries_src[0..entries_size]);
    }

    return total_len;
}

/// Encode an UnregisterService message into `buf`. Returns the total encoded length.
pub fn encodeUnregisterService(buf: []u8, service_id: i32) u16 {
    const total_len: u16 = @sizeOf(UnregisterServiceMsg);
    std.debug.assert(total_len <= buf.len);

    const msg_ptr: *UnregisterServiceMsg = @ptrCast(@alignCast(buf.ptr));
    msg_ptr.* = .{
        .header = .{
            .template_id = 5,
            .body_length = total_len - @as(u16, header_size),
        },
        .service_id = service_id,
        .node_id = 0,
    };
    return total_len;
}

/// Encode a LeaderChanged message into `buf`. Returns the total encoded length.
pub fn encodeLeaderChanged(
    buf: []u8,
    leader_service_id: i32,
    leader_node_id: i16,
    service_name: []const u8,
) u16 {
    const fixed_len = @sizeOf(LeaderChangedMsg);
    const total_len: u16 = @intCast(fixed_len + service_name.len);
    std.debug.assert(total_len <= buf.len);

    const msg_ptr: *LeaderChangedMsg = @ptrCast(@alignCast(buf.ptr));
    msg_ptr.* = .{
        .header = .{
            .template_id = 6,
            .body_length = total_len - @as(u16, header_size),
        },
        .leader_service_id = leader_service_id,
        .leader_node_id = leader_node_id,
        .service_name_length = @intCast(service_name.len),
    };

    @memcpy(buf[fixed_len..][0..service_name.len], service_name);
    return total_len;
}

// ── Decoding Helpers ──────────────────────────────────────────────────

/// Extract the service name from a RegisterService payload.
pub fn decodeRegisterServiceName(payload: []const u8) []const u8 {
    const msg_ptr: *const RegisterServiceMsg = @ptrCast(@alignCast(payload.ptr));
    const offset = @sizeOf(RegisterServiceMsg);
    return payload[offset..][0..msg_ptr.service_name_length];
}

/// Extract the service name from a Subscribe payload.
pub fn decodeSubscribeServiceName(payload: []const u8) []const u8 {
    const msg_ptr: *const SubscribeMsg = @ptrCast(@alignCast(payload.ptr));
    const offset = @sizeOf(SubscribeMsg);
    return payload[offset..][0..msg_ptr.service_name_length];
}

/// Extract the service name from a LeaderChanged payload.
pub fn decodeLeaderChangedServiceName(payload: []const u8) []const u8 {
    const msg_ptr: *const LeaderChangedMsg = @ptrCast(@alignCast(payload.ptr));
    const offset = @sizeOf(LeaderChangedMsg);
    return payload[offset..][0..msg_ptr.service_name_length];
}

/// Extract the service name and instance entries from a ServiceInstances payload.
pub fn decodeServiceInstances(payload: []const u8) struct {
    service_name: []const u8,
    entries: []const ServiceInstanceEntry,
} {
    const msg_ptr: *const ServiceInstancesMsg = @ptrCast(@alignCast(payload.ptr));
    const name_offset = @sizeOf(ServiceInstancesMsg);
    const name = payload[name_offset..][0..msg_ptr.service_name_length];
    const entries_offset = name_offset + msg_ptr.service_name_length;
    const entries_ptr: [*]const ServiceInstanceEntry = @ptrCast(payload[entries_offset..].ptr);
    return .{
        .service_name = name,
        .entries = entries_ptr[0..msg_ptr.instance_count],
    };
}

// ── Size Verification ─────────────────────────────────────────────────

comptime {
    std.debug.assert(@sizeOf(ControlMessageHeader) == 4);
    std.debug.assert(@sizeOf(RegisterServiceMsg) == 12);
    std.debug.assert(@sizeOf(RegistrationResponseMsg) == 12);
    std.debug.assert(@sizeOf(SubscribeMsg) == 12);
    std.debug.assert(@sizeOf(ServiceInstancesMsg) == 12);
    std.debug.assert(@sizeOf(ServiceInstanceEntry) == 8);
    std.debug.assert(@sizeOf(UnregisterServiceMsg) == 12);
    std.debug.assert(@sizeOf(LeaderChangedMsg) == 12);
}

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "RegisterService encode/decode roundtrip" {
    // Given
    var buf: [256]u8 align(4) = undefined;
    const service_name = "pricing-service";
    const service_id: i32 = 42;

    // When
    const len = encodeRegisterService(&buf, service_id, true, service_name);

    // Then
    const header: *const ControlMessageHeader = @ptrCast(@alignCast(&buf));
    try testing.expectEqual(@as(u16, 1), header.template_id);

    const register: *const RegisterServiceMsg = @ptrCast(@alignCast(&buf));
    try testing.expectEqual(service_id, register.service_id);
    try testing.expectEqual(@as(u8, 1), register.leader_election_enabled);
    try testing.expectEqual(@as(u8, service_name.len), register.service_name_length);

    const decoded_name = decodeRegisterServiceName(buf[0..len]);
    try testing.expectEqualStrings(service_name, decoded_name);
}

test "RegistrationResponse encode/decode roundtrip" {
    // Given
    var buf: [256]u8 align(4) = undefined;

    // When
    const len = encodeRegistrationResponse(&buf, 42, 1, true);

    // Then
    const response: *const RegistrationResponseMsg = @ptrCast(@alignCast(&buf));
    try testing.expectEqual(@as(u16, 2), response.header.template_id);
    try testing.expectEqual(@as(i32, 42), response.service_id);
    try testing.expectEqual(@as(i16, 1), response.node_id);
    try testing.expectEqual(@as(u8, 1), response.is_leader);
    try testing.expectEqual(@as(u16, len), @sizeOf(RegistrationResponseMsg));
}

test "ServiceInstances encode/decode roundtrip" {
    // Given
    var buf: [4096]u8 align(4) = undefined;
    const service_name = "order-service";
    const entries = [_]ServiceInstanceEntry{
        .{ .service_id = 3, .node_id = 1, .is_leader = 1 },
        .{ .service_id = 7, .node_id = 2, .is_leader = 0 },
        .{ .service_id = 12, .node_id = 1, .is_leader = 0 },
    };

    // When
    const len = encodeServiceInstances(&buf, 1, service_name, &entries);

    // Then
    const decoded = decodeServiceInstances(buf[0..len]);
    try testing.expectEqualStrings(service_name, decoded.service_name);
    try testing.expectEqual(@as(usize, 3), decoded.entries.len);
    try testing.expectEqual(@as(i32, 3), decoded.entries[0].service_id);
    try testing.expectEqual(@as(i32, 7), decoded.entries[1].service_id);
    try testing.expectEqual(@as(i32, 12), decoded.entries[2].service_id);
    try testing.expectEqual(@as(u8, 1), decoded.entries[0].is_leader);
    try testing.expectEqual(@as(u8, 0), decoded.entries[1].is_leader);
}

test "LeaderChanged encode/decode roundtrip" {
    // Given
    var buf: [256]u8 align(4) = undefined;
    const service_name = "gateway";

    // When
    const len = encodeLeaderChanged(&buf, 5, 2, service_name);

    // Then
    const leader_msg: *const LeaderChangedMsg = @ptrCast(@alignCast(&buf));
    try testing.expectEqual(@as(u16, 6), leader_msg.header.template_id);
    try testing.expectEqual(@as(i32, 5), leader_msg.leader_service_id);
    try testing.expectEqual(@as(i16, 2), leader_msg.leader_node_id);

    const decoded_name = decodeLeaderChangedServiceName(buf[0..len]);
    try testing.expectEqualStrings(service_name, decoded_name);
}

test "Subscribe encode/decode roundtrip" {
    // Given
    var buf: [256]u8 align(4) = undefined;
    const service_name = "market-data";

    // When
    const len = encodeSubscribe(&buf, 10, service_name);

    // Then
    const sub_msg: *const SubscribeMsg = @ptrCast(@alignCast(&buf));
    try testing.expectEqual(@as(u16, 3), sub_msg.header.template_id);
    try testing.expectEqual(@as(i32, 10), sub_msg.local_service_id);

    const decoded_name = decodeSubscribeServiceName(buf[0..len]);
    try testing.expectEqualStrings(service_name, decoded_name);
}

test "UnregisterService encode/decode roundtrip" {
    // Given
    var buf: [256]u8 align(4) = undefined;

    // When
    const len = encodeUnregisterService(&buf, 99);
    _ = len;

    // Then
    const unreg_msg: *const UnregisterServiceMsg = @ptrCast(@alignCast(&buf));
    try testing.expectEqual(@as(u16, 5), unreg_msg.header.template_id);
    try testing.expectEqual(@as(i32, 99), unreg_msg.service_id);
}

test "control message struct sizes are deterministic" {
    // Given / When / Then
    try testing.expectEqual(@as(usize, 4), @sizeOf(ControlMessageHeader));
    try testing.expectEqual(@as(usize, 12), @sizeOf(RegisterServiceMsg));
    try testing.expectEqual(@as(usize, 12), @sizeOf(RegistrationResponseMsg));
    try testing.expectEqual(@as(usize, 12), @sizeOf(SubscribeMsg));
    try testing.expectEqual(@as(usize, 12), @sizeOf(ServiceInstancesMsg));
    try testing.expectEqual(@as(usize, 8), @sizeOf(ServiceInstanceEntry));
    try testing.expectEqual(@as(usize, 12), @sizeOf(UnregisterServiceMsg));
    try testing.expectEqual(@as(usize, 12), @sizeOf(LeaderChangedMsg));
}
