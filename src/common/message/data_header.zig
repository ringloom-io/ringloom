// SPDX-License-Identifier: Apache-2.0
//! RingLoom v2 transport-neutral data header.
//!
//! This header prefixes application payloads carried by Aeron IPC ingress and
//! broker-to-broker UDP publications. Local service delivery still converts it
//! into the existing service ring-buffer `MessageHeader` envelope.

const std = @import("std");
const constants = @import("../memory/constants.zig");
const message_header = @import("message_header.zig");

pub const magic_bytes = [_]u8{ 'R', 'L', 'M', '2' };
pub const header_version: u8 = 2;
pub const legal_flags: u8 = constants.flag_admin | constants.flag_topic | constants.flag_begin | constants.flag_end;
pub const max_node_id: u16 = 255;
pub const max_service_id: u32 = std.math.maxInt(u16);

pub const RingLoomDataHeader = extern struct {
    magic: [4]u8 = magic_bytes,
    version: u8 = header_version,
    flags: u8 = constants.flag_unfragmented,
    header_length: u16 = encoded_length,
    correlation_id: i64 = 0,
    source_node_id: u16 = 0,
    source_service_id: u16 = 0,
    target_node_id: u16 = 0,
    target_service_id: u16 = 0,
    template_id: u16 = 0,
    _reserved0: u16 = 0,
    payload_length: u32 = 0,

    pub const encoded_length: u16 = @sizeOf(RingLoomDataHeader);

    comptime {
        std.debug.assert(@sizeOf(RingLoomDataHeader) == 32);
        std.debug.assert(@offsetOf(RingLoomDataHeader, "correlation_id") == 8);
        std.debug.assert(@offsetOf(RingLoomDataHeader, "payload_length") == 28);
    }

    pub fn aeronReservedValue(self: RingLoomDataHeader) i64 {
        return self.correlation_id;
    }
};

pub const EncodeFields = struct {
    source_node_id: u16,
    source_service_id: u32,
    target_node_id: u16,
    target_service_id: u32,
    template_id: u16,
    correlation_id: i64 = 0,
    flags: u8 = constants.flag_unfragmented,
    payload_length: usize,
};

pub const DecodedFrame = struct {
    header: RingLoomDataHeader,
    payload: []const u8,
};

pub const CodecError = error{
    BufferTooSmall,
    BadMagic,
    UnsupportedVersion,
    InvalidHeaderLength,
    InvalidFlags,
    InvalidNodeId,
    InvalidServiceId,
    InvalidPayloadLength,
    MessageTooLong,
};

pub fn maxPayloadLength(aeron_term_length: usize, app_policy_max: usize) usize {
    if (aeron_term_length <= RingLoomDataHeader.encoded_length) return 0;
    return @min(app_policy_max, aeron_term_length - RingLoomDataHeader.encoded_length);
}

pub fn encodeHeader(dest: []u8, fields: EncodeFields, max_message_length: usize) CodecError!void {
    if (dest.len < RingLoomDataHeader.encoded_length) return error.BufferTooSmall;
    const header = try buildHeader(fields, max_message_length);
    @memcpy(dest[0..RingLoomDataHeader.encoded_length], std.mem.asBytes(&header));
}

pub fn encodeFrame(
    dest: []u8,
    payload: []const u8,
    fields: EncodeFields,
    max_message_length: usize,
) CodecError!usize {
    const total_len = RingLoomDataHeader.encoded_length + payload.len;
    if (dest.len < total_len) return error.BufferTooSmall;
    var adjusted = fields;
    adjusted.payload_length = payload.len;
    try encodeHeader(dest[0..RingLoomDataHeader.encoded_length], adjusted, max_message_length);
    if (payload.len > 0) {
        @memcpy(dest[RingLoomDataHeader.encoded_length..][0..payload.len], payload);
    }
    return total_len;
}

pub fn decodeHeader(src: []const u8, max_message_length: usize) CodecError!RingLoomDataHeader {
    if (src.len < RingLoomDataHeader.encoded_length) return error.BufferTooSmall;
    const header = std.mem.bytesToValue(RingLoomDataHeader, src[0..RingLoomDataHeader.encoded_length]);
    try validateHeader(header, max_message_length);
    return header;
}

pub fn decodeFrame(src: []const u8, max_message_length: usize) CodecError!DecodedFrame {
    const header = try decodeHeader(src, max_message_length);
    const payload_len: usize = @intCast(header.payload_length);
    if (src.len != RingLoomDataHeader.encoded_length + payload_len) {
        return error.InvalidPayloadLength;
    }
    return .{
        .header = header,
        .payload = src[RingLoomDataHeader.encoded_length..],
    };
}

pub fn dataHeaderToEnvelope(header: RingLoomDataHeader) message_header.MessageHeader {
    return .{
        .correlation_id = header.correlation_id,
        .source_node_id = @intCast(header.source_node_id),
        .source_service_id = @intCast(header.source_service_id),
        .target_node_id = @intCast(header.target_node_id),
        .target_service_id = @intCast(header.target_service_id),
        .template_id = header.template_id,
        .flags = header.flags,
        .payload_length = @intCast(header.payload_length),
    };
}

fn buildHeader(fields: EncodeFields, max_message_length: usize) CodecError!RingLoomDataHeader {
    if (fields.source_node_id > max_node_id or fields.target_node_id > max_node_id) {
        return error.InvalidNodeId;
    }
    if (fields.source_service_id > max_service_id or fields.target_service_id > max_service_id) {
        return error.InvalidServiceId;
    }
    if (fields.payload_length > std.math.maxInt(u32)) return error.InvalidPayloadLength;
    if (fields.payload_length > max_message_length) return error.MessageTooLong;
    try validateFlags(fields.flags);

    return .{
        .flags = fields.flags,
        .correlation_id = fields.correlation_id,
        .source_node_id = fields.source_node_id,
        .source_service_id = @intCast(fields.source_service_id),
        .target_node_id = fields.target_node_id,
        .target_service_id = @intCast(fields.target_service_id),
        .template_id = fields.template_id,
        .payload_length = @intCast(fields.payload_length),
    };
}

fn validateHeader(header: RingLoomDataHeader, max_message_length: usize) CodecError!void {
    if (!std.mem.eql(u8, &header.magic, &magic_bytes)) return error.BadMagic;
    if (header.version != header_version) return error.UnsupportedVersion;
    if (header.header_length != RingLoomDataHeader.encoded_length) return error.InvalidHeaderLength;
    try validateFlags(header.flags);
    if (header.source_node_id > max_node_id or header.target_node_id > max_node_id) {
        return error.InvalidNodeId;
    }
    if (@as(usize, @intCast(header.payload_length)) > max_message_length) {
        return error.MessageTooLong;
    }
}

fn validateFlags(flags: u8) CodecError!void {
    if (flags & ~legal_flags != 0) return error.InvalidFlags;
    const begin = flags & constants.flag_begin != 0;
    const end = flags & constants.flag_end != 0;
    if (!begin and end) return error.InvalidFlags;
}

const testing = std.testing;

test "RingLoomDataHeader layout is stable" {
    try testing.expectEqual(@as(usize, 32), @sizeOf(RingLoomDataHeader));
    try testing.expectEqual(@as(usize, 8), @offsetOf(RingLoomDataHeader, "correlation_id"));
    try testing.expectEqual(@as(usize, 28), @offsetOf(RingLoomDataHeader, "payload_length"));
}

test "encode and decode data frame round trip" {
    var buf: [128]u8 = [_]u8{0} ** 128;
    const payload = "hello-aeron";
    const written = try encodeFrame(&buf, payload, .{
        .source_node_id = 1,
        .source_service_id = 5,
        .target_node_id = 2,
        .target_service_id = 7,
        .template_id = 42,
        .correlation_id = 99,
        .payload_length = payload.len,
    }, 1024);

    const decoded = try decodeFrame(buf[0..written], 1024);
    try testing.expectEqualSlices(u8, &magic_bytes, &decoded.header.magic);
    try testing.expectEqual(@as(u16, 1), decoded.header.source_node_id);
    try testing.expectEqual(@as(u16, 7), decoded.header.target_service_id);
    try testing.expectEqual(@as(u16, 42), decoded.header.template_id);
    try testing.expectEqual(@as(i64, 99), decoded.header.correlation_id);
    try testing.expectEqualStrings(payload, decoded.payload);
}

test "request response and leader routed fields round trip" {
    var buf: [96]u8 = [_]u8{0} ** 96;
    const written = try encodeFrame(&buf, "rpc", .{
        .source_node_id = 4,
        .source_service_id = 100,
        .target_node_id = 5,
        .target_service_id = 200,
        .template_id = 0,
        .correlation_id = 123456789,
        .flags = constants.flag_admin | constants.flag_unfragmented,
        .payload_length = 3,
    }, 1024);

    const decoded = try decodeFrame(buf[0..written], 1024);
    const envelope = dataHeaderToEnvelope(decoded.header);
    try testing.expectEqual(@as(i16, 4), envelope.source_node_id);
    try testing.expectEqual(@as(i16, 100), envelope.source_service_id);
    try testing.expectEqual(@as(i16, 5), envelope.target_node_id);
    try testing.expectEqual(@as(i16, 200), envelope.target_service_id);
    try testing.expectEqual(@as(i64, 123456789), envelope.correlation_id);
    try testing.expectEqual(constants.flag_admin | constants.flag_unfragmented, envelope.flags);
}

test "invalid payload length illegal flags and node/service ids are rejected" {
    var buf: [64]u8 = [_]u8{0} ** 64;
    try testing.expectError(error.InvalidFlags, encodeHeader(&buf, .{
        .source_node_id = 1,
        .source_service_id = 1,
        .target_node_id = 2,
        .target_service_id = 2,
        .template_id = 1,
        .flags = 0x02,
        .payload_length = 0,
    }, 1024));
    try testing.expectError(error.InvalidNodeId, encodeHeader(&buf, .{
        .source_node_id = 256,
        .source_service_id = 1,
        .target_node_id = 2,
        .target_service_id = 2,
        .template_id = 1,
        .payload_length = 0,
    }, 1024));
    try testing.expectError(error.InvalidServiceId, encodeHeader(&buf, .{
        .source_node_id = 1,
        .source_service_id = 65_536,
        .target_node_id = 2,
        .target_service_id = 2,
        .template_id = 1,
        .payload_length = 0,
    }, 1024));
    try testing.expectError(error.MessageTooLong, encodeHeader(&buf, .{
        .source_node_id = 1,
        .source_service_id = 1,
        .target_node_id = 2,
        .target_service_id = 2,
        .template_id = 1,
        .payload_length = 2048,
    }, 1024));
}

test "decode rejects mismatched frame payload length" {
    var buf: [64]u8 = [_]u8{0} ** 64;
    const written = try encodeFrame(&buf, "abc", .{
        .source_node_id = 1,
        .source_service_id = 1,
        .target_node_id = 2,
        .target_service_id = 2,
        .template_id = 1,
        .payload_length = 3,
    }, 1024);
    try testing.expectError(error.InvalidPayloadLength, decodeFrame(buf[0 .. written - 1], 1024));
}

test "correlation id is Aeron reserved value" {
    const header = try buildHeader(.{
        .source_node_id = 1,
        .source_service_id = 1,
        .target_node_id = 2,
        .target_service_id = 2,
        .template_id = 9,
        .correlation_id = -123,
        .payload_length = 0,
    }, 1024);
    try testing.expectEqual(@as(i64, -123), header.aeronReservedValue());
}

test "max payload length derives from Aeron term and policy" {
    try testing.expectEqual(@as(usize, 4096 - RingLoomDataHeader.encoded_length), maxPayloadLength(4096, 10_000));
    try testing.expectEqual(@as(usize, 512), maxPayloadLength(4096, 512));
}
