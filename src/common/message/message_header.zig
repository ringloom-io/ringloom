//! RingLoom message header — fixed-size prefix on every application message.
//!
//! Layout (32 bytes total, extern struct for precise memory layout):
//!   +0:   correlation_id       (i64)
//!   +8:   source_node_id       (i16)
//!   +10:  source_service_id    (i16)
//!   +12:  target_node_id       (i16)
//!   +14:  target_service_id    (i16)
//!   +16:  template_id          (u16)
//!   +18:  flags                (u8)
//!   +19:  reserved             (1 byte)
//!   +20:  payload_length       (i32)
//!   +24:  reserved             (4 bytes, alignment padding)
//!
//! Total: 28 bytes, but padded to 32 for alignment.

const std = @import("std");
const constants = @import("../platform/constants.zig");

pub const MessageHeader = extern struct {
    correlation_id: i64 = 0,
    source_node_id: i16 = 0,
    source_service_id: i16 = 0,
    target_node_id: i16 = 0,
    target_service_id: i16 = 0,
    template_id: u16 = 0,
    flags: u8 = 0,
    _reserved0: u8 = 0,
    payload_length: i32 = 0,
    _reserved1: u32 = 0,

    pub const encoded_length: usize = @sizeOf(MessageHeader);

    comptime {
        // Verify the size is what we expect. extern struct with these fields
        // should be exactly 32 bytes due to padding after the last i32.
        std.debug.assert(@sizeOf(MessageHeader) == 32);
    }

    /// Flyweight encode — writes the header directly into the buffer.
    pub fn encode(dest: []u8, fields: struct {
        source_node_id: i16 = 0,
        source_service_id: i16 = 0,
        target_node_id: i16 = 0,
        target_service_id: i16 = 0,
        template_id: u16 = 0,
        correlation_id: i64 = 0,
        flags: u8 = 0,
        payload_length: i32 = 0,
    }) void {
        std.debug.assert(dest.len >= encoded_length);
        const header: *MessageHeader = @ptrCast(@alignCast(dest.ptr));
        header.* = .{
            .correlation_id = fields.correlation_id,
            .source_node_id = fields.source_node_id,
            .source_service_id = fields.source_service_id,
            .target_node_id = fields.target_node_id,
            .target_service_id = fields.target_service_id,
            .template_id = fields.template_id,
            .flags = fields.flags,
            .payload_length = fields.payload_length,
        };
    }

    /// Flyweight decode — overlays a read-only header onto the buffer.
    pub fn decode(src: []const u8) *const MessageHeader {
        std.debug.assert(src.len >= encoded_length);
        return @ptrCast(@alignCast(src.ptr));
    }
};

pub const EnvelopeView = struct {
    header: *const MessageHeader,
    payload: []const u8,
};

pub fn msgTypeFromTemplateId(template_id: u16) i32 {
    if (template_id == 0) return constants.application_msg_type_id;
    return @intCast(template_id);
}

pub fn templateIdFromMsgTypeId(msg_type_id: i32) u16 {
    if (msg_type_id == constants.application_msg_type_id) return 0;
    if (msg_type_id <= 0 or msg_type_id > std.math.maxInt(u16)) return 0;
    return @intCast(msg_type_id);
}

pub fn tryDecodeEnvelope(msg_type_id: i32, record_payload: []const u8) ?EnvelopeView {
    if (msg_type_id != constants.message_envelope_msg_type_id) return null;
    if (record_payload.len < MessageHeader.encoded_length) return null;

    const header = MessageHeader.decode(record_payload[0..MessageHeader.encoded_length]);
    if (header.payload_length < 0) return null;

    const payload_len: usize = @intCast(header.payload_length);
    if (payload_len != record_payload.len - MessageHeader.encoded_length) return null;

    return .{
        .header = header,
        .payload = record_payload[MessageHeader.encoded_length..],
    };
}

comptime {
    std.debug.assert(constants.message_envelope_msg_type_id > std.math.maxInt(u16));
    std.debug.assert(constants.message_envelope_msg_type_id != constants.application_msg_type_id);
    std.debug.assert(constants.message_envelope_msg_type_id != constants.control_msg_type_id);
}

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "MessageHeader encoded_length is 32" {
    try testing.expectEqual(@as(usize, 32), MessageHeader.encoded_length);
}

test "MessageHeader encode and decode roundtrip" {
    var buf: [MessageHeader.encoded_length]u8 align(@alignOf(MessageHeader)) = undefined;
    @memset(&buf, 0);

    MessageHeader.encode(&buf, .{
        .source_node_id = 1,
        .source_service_id = 5,
        .target_node_id = 2,
        .target_service_id = 10,
        .template_id = 42,
        .correlation_id = 12345,
        .flags = 0x80,
        .payload_length = 100,
    });

    const decoded = MessageHeader.decode(&buf);
    try testing.expectEqual(@as(i16, 1), decoded.source_node_id);
    try testing.expectEqual(@as(i16, 5), decoded.source_service_id);
    try testing.expectEqual(@as(i16, 2), decoded.target_node_id);
    try testing.expectEqual(@as(i16, 10), decoded.target_service_id);
    try testing.expectEqual(@as(u16, 42), decoded.template_id);
    try testing.expectEqual(@as(i64, 12345), decoded.correlation_id);
    try testing.expectEqual(@as(u8, 0x80), decoded.flags);
    try testing.expectEqual(@as(i32, 100), decoded.payload_length);
}

test "MessageHeader default fields are zero" {
    var buf: [MessageHeader.encoded_length]u8 align(@alignOf(MessageHeader)) = undefined;
    @memset(&buf, 0);

    MessageHeader.encode(&buf, .{});

    const decoded = MessageHeader.decode(&buf);
    try testing.expectEqual(@as(i16, 0), decoded.source_node_id);
    try testing.expectEqual(@as(i64, 0), decoded.correlation_id);
    try testing.expectEqual(@as(u8, 0), decoded.flags);
}

test "envelope decode validates message type and payload length" {
    var buf: [MessageHeader.encoded_length + 5]u8 align(@alignOf(MessageHeader)) = undefined;
    @memset(&buf, 0);
    MessageHeader.encode(buf[0..MessageHeader.encoded_length], .{
        .template_id = 42,
        .correlation_id = 99,
        .payload_length = 5,
    });
    @memcpy(buf[MessageHeader.encoded_length..], "hello");

    const decoded = tryDecodeEnvelope(constants.message_envelope_msg_type_id, &buf).?;
    try testing.expectEqual(@as(u16, 42), decoded.header.template_id);
    try testing.expectEqual(@as(i64, 99), decoded.header.correlation_id);
    try testing.expectEqualStrings("hello", decoded.payload);
    try testing.expect(tryDecodeEnvelope(constants.application_msg_type_id, &buf) == null);
}
