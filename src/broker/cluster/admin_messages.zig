//! Admin message wire format — packed struct overlays for broker-to-broker messages.
//!
//! Admin messages are framed inside DATA frames (see protocol/frames.zig) with the
//! ADMIN flag (0x20). The payload starts with an 8-byte AdminMessageHeader followed
//! by the message-specific body.
//!
//! Wire layout:
//!   Offset  Size   Type     Field
//!   0       2      u16      block_length     — size of the message body (excluding header)
//!   2       2      u16      template_id      — message type (1–5)
//!   4       2      u16      schema_id        — always 688
//!   6       2      u16      version          — schema version (1)
//!   8       N      bytes    message body     — template-specific fields

const std = @import("std");

pub const SCHEMA_ID: u16 = 688;
pub const SCHEMA_VERSION: u16 = 1;

// ── Template IDs ──────────────────────────────────────────────────────

pub const TEMPLATE_BROKER_HEARTBEAT: u16 = 1;
pub const TEMPLATE_CLUSTER_STATE_SNAPSHOT: u16 = 2;
pub const TEMPLATE_SERVICE_ADDED: u16 = 3;
pub const TEMPLATE_SERVICE_REMOVED: u16 = 4;
pub const TEMPLATE_SERVICE_LEADER_DESIGNATED: u16 = 5;
pub const TEMPLATE_REMAINING_BYTES_UPDATE: u16 = 9;
pub const TEMPLATE_FLOW_CONTROL_SNAPSHOT: u16 = 10;
pub const TEMPLATE_SERVICE_CAPACITY_UPDATE: u16 = 11;

// ── Admin Message Header ──────────────────────────────────────────────

/// Wire header for all admin messages. Matches the Java `brokerMessageHeader`.
/// 8 bytes, little-endian, overlaid directly on the DATA frame payload.
pub const AdminMessageHeader = packed struct(u64) {
    block_length: u16,
    template_id: u16,
    schema_id: u16,
    version: u16,
};

comptime {
    std.debug.assert(@sizeOf(AdminMessageHeader) == 8);
}

/// Aeron UDP envelope for broker-to-broker admin traffic.
///
/// The envelope carries the peer routing metadata that previously lived in the
/// TCP frame header. The payload remains the existing SBE-style admin message
/// (`AdminMessageHeader` followed by its body) so existing dispatch code can be
/// reused during the transport migration.
pub const AeronAdminHeader = extern struct {
    frame_length: u32,
    source_node_id: u8,
    target_node_id: u8,
    template_id: u16 align(1),
    epoch: i64 align(1),
    payload_length: u32 align(1),
    reserved: u32 align(1) = 0,

    pub const size: usize = @sizeOf(AeronAdminHeader);
};

comptime {
    std.debug.assert(@sizeOf(AeronAdminHeader) == 24);
}

pub const AeronAdminFrame = struct {
    header: AeronAdminHeader,
    payload: []const u8,
};

pub const AeronAdminError = error{
    BufferTooSmall,
    PayloadTooLarge,
    FrameLengthMismatch,
    TargetNodeMismatch,
    TemplateMismatch,
};

// ── Message Body Layouts ──────────────────────────────────────────────

/// templateId = 1: BrokerHeartbeat
/// Serves as both liveness keepalive and leadership priority assertion.
/// `topics_enabled` bit (0x01 in flags) advertises that this broker
/// participates in the persistent topics subsystem (spec 08).
pub const BrokerHeartbeatBody = extern struct {
    node_id: u8,
    host_and_port: [22]u8,
    topics_enabled: u8 = 0, // 0 = false, 1 = true
};

pub const BROKER_FLAG_TOPICS_ENABLED: u8 = 0x01;

comptime {
    std.debug.assert(@sizeOf(BrokerHeartbeatBody) == 24);
}

/// templateId = 3: ServiceAdded
/// Broadcast when a service registers on the local broker.
pub const ServiceAddedBody = extern struct {
    node_id: u8,
    service_id: u16 align(1),
    service_name: [32]u8,
    leader_election_enabled: u8, // 0 = false, 1 = true
};

comptime {
    std.debug.assert(@sizeOf(ServiceAddedBody) == 36);
}

/// templateId = 4: ServiceRemoved
/// Broadcast when a service deregisters from the local broker.
pub const ServiceRemovedBody = extern struct {
    node_id: u8,
    service_id: u16 align(1),
    service_name: [32]u8,
};

comptime {
    std.debug.assert(@sizeOf(ServiceRemovedBody) == 35);
}

/// templateId = 5: ServiceLeaderDesignated
/// Sent by the cluster leader when a service leader is elected.
pub const ServiceLeaderDesignatedBody = extern struct {
    node_id: u8,
    service_id: u16 align(1),
    service_name: [32]u8,
};

comptime {
    std.debug.assert(@sizeOf(ServiceLeaderDesignatedBody) == 35);
}

// ── ClusterStateSnapshot Group Header ─────────────────────────────────

/// Header for the repeating group inside ClusterStateSnapshot.
pub const GroupHeader = packed struct(u32) {
    block_length: u16,
    num_in_group: u16,
};

comptime {
    std.debug.assert(@sizeOf(GroupHeader) == 4);
}

/// A single entry in the ClusterStateSnapshot repeating group.
pub const SnapshotEntry = extern struct {
    service_id: u16 align(1),
    service_name: [32]u8,
    leader_election_enabled: u8,
};

comptime {
    std.debug.assert(@sizeOf(SnapshotEntry) == 35);
}

// ── Encoding Helper ───────────────────────────────────────────────────

/// Encode an admin message header + body into the provided buffer.
/// Returns the total encoded length (header + body).
pub fn encodeAdminMessage(
    buf: []u8,
    comptime BodyType: type,
    template_id: u16,
    body: BodyType,
) usize {
    const header_len = @sizeOf(AdminMessageHeader);
    const body_len = @sizeOf(BodyType);
    std.debug.assert(buf.len >= header_len + body_len);

    // Write header — use byte-level copy to avoid alignment issues
    const header = AdminMessageHeader{
        .block_length = @intCast(body_len),
        .template_id = template_id,
        .schema_id = SCHEMA_ID,
        .version = SCHEMA_VERSION,
    };
    const header_bytes: *const [header_len]u8 = @ptrCast(&header);
    @memcpy(buf[0..header_len], header_bytes);

    // Write body — use byte-level copy to avoid alignment issues
    const body_bytes: *const [body_len]u8 = @ptrCast(&body);
    @memcpy(buf[header_len..][0..body_len], body_bytes);

    return header_len + body_len;
}

/// Decode the AdminMessageHeader from a payload buffer.
/// Returns null if the buffer is too small.
pub fn decodeHeader(payload: []const u8) ?AdminMessageHeader {
    if (payload.len < @sizeOf(AdminMessageHeader)) return null;
    var header: AdminMessageHeader = undefined;
    const header_bytes: *[8]u8 = @ptrCast(&header);
    @memcpy(header_bytes, payload[0..8]);
    return header;
}

/// Returns the body portion of an admin message payload (after the header).
pub fn bodySlice(payload: []const u8) []const u8 {
    const header_len = @sizeOf(AdminMessageHeader);
    if (payload.len <= header_len) return payload[0..0];
    return payload[header_len..];
}

/// Wrap an existing admin payload in an Aeron admin envelope.
pub fn encodeAeronAdminFrame(
    buf: []u8,
    source_node_id: u8,
    target_node_id: u8,
    epoch: i64,
    payload: []const u8,
) AeronAdminError!usize {
    const admin_header = decodeHeader(payload) orelse return error.BufferTooSmall;
    if (payload.len > std.math.maxInt(u32)) return error.PayloadTooLarge;

    const frame_len = AeronAdminHeader.size + payload.len;
    if (buf.len < frame_len) return error.BufferTooSmall;

    const header = AeronAdminHeader{
        .frame_length = @intCast(frame_len),
        .source_node_id = source_node_id,
        .target_node_id = target_node_id,
        .template_id = admin_header.template_id,
        .epoch = epoch,
        .payload_length = @intCast(payload.len),
    };
    const header_bytes: *const [AeronAdminHeader.size]u8 = @ptrCast(&header);
    @memcpy(buf[0..AeronAdminHeader.size], header_bytes);
    const payload_dst = buf[AeronAdminHeader.size..][0..payload.len];
    if (payload_dst.ptr != payload.ptr) {
        @memcpy(payload_dst, payload);
    }
    return frame_len;
}

/// Decode and validate an Aeron admin frame addressed to `local_node_id`.
pub fn decodeAeronAdminFrame(
    bytes: []const u8,
    local_node_id: u8,
) AeronAdminError!AeronAdminFrame {
    if (bytes.len < AeronAdminHeader.size) return error.BufferTooSmall;

    var header: AeronAdminHeader = undefined;
    const header_bytes: *[AeronAdminHeader.size]u8 = @ptrCast(&header);
    @memcpy(header_bytes, bytes[0..AeronAdminHeader.size]);

    if (header.frame_length != bytes.len) return error.FrameLengthMismatch;
    if (header.target_node_id != local_node_id) return error.TargetNodeMismatch;

    const payload_len: usize = @intCast(header.payload_length);
    if (AeronAdminHeader.size + payload_len != bytes.len) return error.FrameLengthMismatch;
    const payload = bytes[AeronAdminHeader.size..];

    const admin_header = decodeHeader(payload) orelse return error.BufferTooSmall;
    if (admin_header.template_id != header.template_id) return error.TemplateMismatch;

    return .{
        .header = header,
        .payload = payload,
    };
}

// ── Service Name Utilities ────────────────────────────────────────────

/// Pad a service name into a fixed 32-byte array (null-padded).
pub fn padServiceName(name: []const u8) [32]u8 {
    var result: [32]u8 = [_]u8{0} ** 32;
    const len = @min(name.len, 32);
    @memcpy(result[0..len], name[0..len]);
    return result;
}

/// Trim trailing null bytes from a 32-byte service name.
pub fn trimServiceName(name: *const [32]u8) []const u8 {
    var len: usize = 32;
    while (len > 0 and name[len - 1] == 0) {
        len -= 1;
    }
    return name[0..len];
}

/// Pad a host:port string into a fixed 22-byte array (null-padded).
pub fn padHostPort(host_port: []const u8) [22]u8 {
    var result: [22]u8 = [_]u8{0} ** 22;
    const len = @min(host_port.len, 22);
    @memcpy(result[0..len], host_port[0..len]);
    return result;
}

/// Trim trailing null bytes from a 22-byte host:port string.
pub fn trimHostPort(hp: *const [22]u8) []const u8 {
    var len: usize = 22;
    while (len > 0 and hp[len - 1] == 0) {
        len -= 1;
    }
    return hp[0..len];
}

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "AdminMessageHeader is 8 bytes" {
    try testing.expectEqual(@as(usize, 8), @sizeOf(AdminMessageHeader));
}

test "encodeAdminMessage BrokerHeartbeat roundtrip" {
    // Given
    var buf: [64]u8 = undefined;
    const body = BrokerHeartbeatBody{
        .node_id = 1,
        .host_and_port = padHostPort("localhost:40456"),
    };

    // When
    const len = encodeAdminMessage(&buf, BrokerHeartbeatBody, TEMPLATE_BROKER_HEARTBEAT, body);

    // Then
    try testing.expectEqual(@as(usize, 8 + 24), len);

    const header = decodeHeader(&buf).?;
    try testing.expectEqual(@as(u16, 24), header.block_length);
    try testing.expectEqual(TEMPLATE_BROKER_HEARTBEAT, header.template_id);
    try testing.expectEqual(SCHEMA_ID, header.schema_id);
    try testing.expectEqual(SCHEMA_VERSION, header.version);

    // Decode body
    const body_data = bodySlice(buf[0..len]);
    var decoded_body: BrokerHeartbeatBody = undefined;
    const decoded_bytes: *[@sizeOf(BrokerHeartbeatBody)]u8 = @ptrCast(&decoded_body);
    @memcpy(decoded_bytes, body_data[0..@sizeOf(BrokerHeartbeatBody)]);
    try testing.expectEqual(@as(u8, 1), decoded_body.node_id);
    try testing.expectEqualStrings("localhost:40456", trimHostPort(&decoded_body.host_and_port));
}

test "encodeAdminMessage ServiceAdded roundtrip" {
    // Given
    var buf: [64]u8 = undefined;
    const body = ServiceAddedBody{
        .node_id = 2,
        .service_id = 42,
        .service_name = padServiceName("pricing"),
        .leader_election_enabled = 1,
    };

    // When
    const len = encodeAdminMessage(&buf, ServiceAddedBody, TEMPLATE_SERVICE_ADDED, body);

    // Then
    try testing.expectEqual(@as(usize, 8 + 36), len);

    const header = decodeHeader(&buf).?;
    try testing.expectEqual(TEMPLATE_SERVICE_ADDED, header.template_id);
}

test "padServiceName and trimServiceName roundtrip" {
    // Given
    const name = "my-service";

    // When
    const padded = padServiceName(name);
    const trimmed = trimServiceName(&padded);

    // Then
    try testing.expectEqualStrings(name, trimmed);
}

test "padHostPort and trimHostPort roundtrip" {
    // Given
    const hp = "192.168.1.1:40456";

    // When
    const padded = padHostPort(hp);
    const trimmed = trimHostPort(&padded);

    // Then
    try testing.expectEqualStrings(hp, trimmed);
}

test "decodeHeader returns null for short buffer" {
    // Given
    const buf = [_]u8{ 0, 1, 2 };

    // When / Then
    try testing.expect(decodeHeader(&buf) == null);
}

test "bodySlice returns empty for header-only buffer" {
    // Given
    var buf: [8]u8 = undefined;

    // When / Then
    try testing.expectEqual(@as(usize, 0), bodySlice(&buf).len);
}

test "Aeron admin frame wraps existing admin payload" {
    var payload_buf: [64]u8 = undefined;
    const payload_len = encodeAdminMessage(
        &payload_buf,
        ServiceAddedBody,
        TEMPLATE_SERVICE_ADDED,
        ServiceAddedBody{
            .node_id = 1,
            .service_id = 42,
            .service_name = padServiceName("pricing"),
            .leader_election_enabled = 1,
        },
    );

    var frame_buf: [128]u8 = undefined;
    const frame_len = try encodeAeronAdminFrame(frame_buf[0..], 1, 2, 99, payload_buf[0..payload_len]);
    const frame = try decodeAeronAdminFrame(frame_buf[0..frame_len], 2);

    try testing.expectEqual(@as(u8, 1), frame.header.source_node_id);
    try testing.expectEqual(@as(u8, 2), frame.header.target_node_id);
    try testing.expectEqual(@as(i64, 99), frame.header.epoch);
    try testing.expectEqual(TEMPLATE_SERVICE_ADDED, frame.header.template_id);
    try testing.expectEqualSlices(u8, payload_buf[0..payload_len], frame.payload);
}

test "Aeron admin frame rejects malformed target and template mismatch" {
    var payload_buf: [64]u8 = undefined;
    const payload_len = encodeAdminMessage(
        &payload_buf,
        ServiceRemovedBody,
        TEMPLATE_SERVICE_REMOVED,
        ServiceRemovedBody{
            .node_id = 1,
            .service_id = 7,
            .service_name = padServiceName("orders"),
        },
    );

    var frame_buf: [128]u8 = undefined;
    const frame_len = try encodeAeronAdminFrame(frame_buf[0..], 1, 2, 1, payload_buf[0..payload_len]);
    try testing.expectError(error.TargetNodeMismatch, decodeAeronAdminFrame(frame_buf[0..frame_len], 3));

    var header: AeronAdminHeader = undefined;
    @memcpy(@as(*[AeronAdminHeader.size]u8, @ptrCast(&header)), frame_buf[0..AeronAdminHeader.size]);
    header.template_id = TEMPLATE_SERVICE_ADDED;
    @memcpy(frame_buf[0..AeronAdminHeader.size], @as(*const [AeronAdminHeader.size]u8, @ptrCast(&header)));
    try testing.expectError(error.TemplateMismatch, decodeAeronAdminFrame(frame_buf[0..frame_len], 2));
}
