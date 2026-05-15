// SPDX-License-Identifier: Apache-2.0
const std = @import("std");

pub const magic: u32 = 0x3244_5552; // "RUD2" on little-endian hosts.
pub const version: u8 = 2;
pub const default_mtu: u16 = 1408;

pub const StreamId = u32;

pub const FrameType = enum(u8) {
    setup = 1,
    setup_response = 2,
    data = 3,
    status = 4,
    nak = 5,
    rttm = 6,
    heartbeat = 7,
    protocol_error = 8,
};

pub const DecodeError = error{
    FrameTooSmall,
    FrameTooLarge,
    InvalidMagic,
    InvalidVersion,
    InvalidFrameType,
    InvalidHeaderLength,
    InvalidFrameLength,
    InvalidReservedField,
    InvalidRoute,
    InvalidNakRange,
    InvalidTargetNode,
    InvalidGroupHash,
};

pub const CommonHeader = extern struct {
    magic_value: u32 = magic,
    version_value: u8 = version,
    frame_type: u8 = 0,
    flags: u16 = 0,
    header_length: u16 = @sizeOf(CommonHeader),
    frame_length: u16 = @sizeOf(CommonHeader),
    session_id: u32 = 0,

    pub const encoded_length: usize = @sizeOf(CommonHeader);

    comptime {
        std.debug.assert(@sizeOf(CommonHeader) == 16);
    }

    pub fn init(frame_type: FrameType, header_length: usize, frame_length: usize, session_id: u32, flags: u16) CommonHeader {
        return .{
            .frame_type = @intFromEnum(frame_type),
            .flags = flags,
            .header_length = @intCast(header_length),
            .frame_length = @intCast(frame_length),
            .session_id = session_id,
        };
    }

    pub fn kind(self: CommonHeader) DecodeError!FrameType {
        return switch (self.frame_type) {
            1 => .setup,
            2 => .setup_response,
            3 => .data,
            4 => .status,
            5 => .nak,
            6 => .rttm,
            7 => .heartbeat,
            8 => .protocol_error,
            else => DecodeError.InvalidFrameType,
        };
    }
};

pub const StreamKey = extern struct {
    source_node_id: u8,
    target_node_id: u8,
    target_service_id: u16,

    pub fn streamId(self: StreamKey, generation: u16) StreamId {
        var hash: u32 = 2166136261;
        inline for (.{ self.source_node_id, self.target_node_id, self.target_service_id, generation }) |value| {
            for (std.mem.asBytes(&value)) |byte| {
                hash = (hash ^ byte) *% 16777619;
            }
        }
        return if (hash == 0) 1 else hash;
    }
};

pub const DataHeader = extern struct {
    common: CommonHeader,
    stream_id: u32,
    term_id: i32,
    term_offset: u32,
    message_id: u64 align(4),
    fragment_offset: u32,
    message_length: u32,
    source_node_id: u8,
    target_node_id: u8,
    route_flags: u16,
    source_service_id: u16,
    target_service_id: u16,
    template_id: u16,
    reserved: u16 = 0,
    correlation_id: i64 align(4) = 0,

    pub const encoded_length: usize = @sizeOf(DataHeader);

    comptime {
        std.debug.assert(@sizeOf(DataHeader) == 64);
    }

    pub fn init(fields: struct {
        session_id: u32,
        stream_id: u32,
        term_id: i32,
        term_offset: u32,
        message_id: u64 = 0,
        fragment_offset: u32 = 0,
        message_length: u32,
        payload_length: usize,
        source_node_id: u8,
        target_node_id: u8,
        route_flags: u16,
        source_service_id: u16,
        target_service_id: u16,
        template_id: u16,
        correlation_id: i64 = 0,
    }) DataHeader {
        return .{
            .common = CommonHeader.init(.data, encoded_length, encoded_length + fields.payload_length, fields.session_id, 0),
            .stream_id = fields.stream_id,
            .term_id = fields.term_id,
            .term_offset = fields.term_offset,
            .message_id = fields.message_id,
            .fragment_offset = fields.fragment_offset,
            .message_length = fields.message_length,
            .source_node_id = fields.source_node_id,
            .target_node_id = fields.target_node_id,
            .route_flags = fields.route_flags,
            .source_service_id = fields.source_service_id,
            .target_service_id = fields.target_service_id,
            .template_id = fields.template_id,
            .correlation_id = fields.correlation_id,
        };
    }
};

pub const SetupHeader = extern struct {
    common: CommonHeader,
    source_node_id: u8,
    target_node_id: u8,
    reserved: u16 = 0,
    stream_id: u32,
    initial_term_id: i32,
    active_term_id: i32,
    term_length: u32,
    mtu: u32,
    sender_epoch: u64,
    group_name_hash: u32,
    token_length: u32 = 0,

    pub const encoded_length: usize = @sizeOf(SetupHeader);
};

pub const SetupResponseHeader = extern struct {
    common: CommonHeader,
    stream_id: u32,
    receiver_id: u64,
    initial_term_id: i32,
    active_term_id: i32,
    term_length: u32,
    mtu: u32,
};

pub const StatusHeader = extern struct {
    common: CommonHeader,
    stream_id: u32,
    consumption_term_id: i32,
    consumption_term_offset: u32,
    receiver_window: u32,
    receiver_id: u64,
    highest_contiguous_message_id: u64,
};

pub const NakHeader = extern struct {
    common: CommonHeader,
    stream_id: u32,
    term_id: i32,
    term_offset: u32,
    length: u32,
};

pub const RttmHeader = extern struct {
    common: CommonHeader,
    stream_id: u32,
    echo_timestamp_ns: i64,
    receive_timestamp_ns: i64,
};

pub const HeartbeatHeader = extern struct {
    common: CommonHeader,
    stream_id: u32,
    term_id: i32,
    term_offset: u32,
};

pub const ErrorHeader = extern struct {
    common: CommonHeader,
    stream_id: u32,
    error_code: u32,
    offending_frame_type: u8,
    reserved: [3]u8 = [_]u8{0} ** 3,
};

pub fn decodeCommon(bytes: []const u8, mtu: usize) DecodeError!CommonHeader {
    if (bytes.len < CommonHeader.encoded_length) return DecodeError.FrameTooSmall;
    const common: *const CommonHeader = @ptrCast(@alignCast(bytes.ptr));
    if (common.magic_value != magic) return DecodeError.InvalidMagic;
    if (common.version_value != version) return DecodeError.InvalidVersion;
    _ = try common.kind();
    if (common.header_length < CommonHeader.encoded_length) return DecodeError.InvalidHeaderLength;
    if (common.frame_length < common.header_length) return DecodeError.InvalidFrameLength;
    if (common.frame_length > mtu) return DecodeError.FrameTooLarge;
    if (common.frame_length > bytes.len) return DecodeError.InvalidFrameLength;
    return common.*;
}

pub fn decodeData(bytes: []const u8, mtu: usize) DecodeError!*const DataHeader {
    const common = try decodeCommon(bytes, mtu);
    if (try common.kind() != .data) return DecodeError.InvalidFrameType;
    if (common.header_length != DataHeader.encoded_length) return DecodeError.InvalidHeaderLength;
    const header: *const DataHeader = @ptrCast(@alignCast(bytes.ptr));
    if (header.reserved != 0) return DecodeError.InvalidReservedField;
    if (header.source_node_id == 0 or header.target_node_id == 0 or header.target_service_id == 0) {
        return DecodeError.InvalidRoute;
    }
    if (header.fragment_offset > header.message_length) return DecodeError.InvalidRoute;
    return header;
}

pub fn decodeSetup(bytes: []const u8, mtu: usize, local_node_id: u8, expected_group_hash: u32) DecodeError!*const SetupHeader {
    const common = try decodeCommon(bytes, mtu);
    if (try common.kind() != .setup) return DecodeError.InvalidFrameType;
    if (common.header_length != SetupHeader.encoded_length) return DecodeError.InvalidHeaderLength;
    const header: *const SetupHeader = @ptrCast(@alignCast(bytes.ptr));
    if (header.reserved != 0) return DecodeError.InvalidReservedField;
    if (header.target_node_id != local_node_id) return DecodeError.InvalidTargetNode;
    if (header.group_name_hash != expected_group_hash) return DecodeError.InvalidGroupHash;
    if (header.term_length == 0 or !std.math.isPowerOfTwo(header.term_length)) return DecodeError.InvalidFrameLength;
    if (header.mtu < DataHeader.encoded_length or header.mtu > mtu) return DecodeError.FrameTooLarge;
    return header;
}

pub fn decodeNak(bytes: []const u8, mtu: usize, term_length: u32) DecodeError!*const NakHeader {
    const common = try decodeCommon(bytes, mtu);
    if (try common.kind() != .nak) return DecodeError.InvalidFrameType;
    if (common.header_length != @sizeOf(NakHeader)) return DecodeError.InvalidHeaderLength;
    const header: *const NakHeader = @ptrCast(@alignCast(bytes.ptr));
    if (header.length == 0 or header.term_offset + header.length > term_length) {
        return DecodeError.InvalidNakRange;
    }
    return header;
}

fn bytesOf(value: anytype) []const u8 {
    return std.mem.asBytes(&value);
}

test "header sizes match architecture" {
    try std.testing.expectEqual(@as(usize, 16), @sizeOf(CommonHeader));
    try std.testing.expectEqual(@as(usize, 64), @sizeOf(DataHeader));
    try std.testing.expectEqual(@as(usize, 56), @sizeOf(SetupHeader));
    try std.testing.expectEqual(@as(usize, 32), @sizeOf(NakHeader));
}

test "DATA encode/decode round trip validates route fields" {
    const header = DataHeader.init(.{
        .session_id = 9,
        .stream_id = 11,
        .term_id = 3,
        .term_offset = 64,
        .message_id = 77,
        .message_length = 5,
        .payload_length = 5,
        .source_node_id = 1,
        .target_node_id = 2,
        .route_flags = 0xc0,
        .source_service_id = 10,
        .target_service_id = 20,
        .template_id = 42,
        .correlation_id = 99,
    });
    var buf: [DataHeader.encoded_length + 5]u8 align(@alignOf(DataHeader)) = undefined;
    @memcpy(buf[0..DataHeader.encoded_length], bytesOf(header));
    @memcpy(buf[DataHeader.encoded_length..], "hello");

    const decoded = try decodeData(&buf, default_mtu);
    try std.testing.expectEqual(@as(u32, 11), decoded.stream_id);
    try std.testing.expectEqual(@as(u16, 20), decoded.target_service_id);
    try std.testing.expectEqual(@as(i64, 99), decoded.correlation_id);
}

test "invalid common header fields are rejected" {
    var header = CommonHeader.init(.heartbeat, CommonHeader.encoded_length, CommonHeader.encoded_length, 1, 0);
    var buf: [CommonHeader.encoded_length]u8 align(@alignOf(CommonHeader)) = undefined;
    header.magic_value = 1;
    @memcpy(&buf, bytesOf(header));
    try std.testing.expectError(error.InvalidMagic, decodeCommon(&buf, default_mtu));

    header.magic_value = magic;
    header.version_value = 1;
    @memcpy(&buf, bytesOf(header));
    try std.testing.expectError(error.InvalidVersion, decodeCommon(&buf, default_mtu));
}

test "frame length above MTU is rejected" {
    const header = CommonHeader.init(.heartbeat, CommonHeader.encoded_length, default_mtu + 1, 1, 0);
    var buf: [CommonHeader.encoded_length]u8 align(@alignOf(CommonHeader)) = undefined;
    @memcpy(&buf, bytesOf(header));
    try std.testing.expectError(error.FrameTooLarge, decodeCommon(&buf, default_mtu));
}

test "SETUP validates group hash and target node" {
    const setup = SetupHeader{
        .common = CommonHeader.init(.setup, SetupHeader.encoded_length, SetupHeader.encoded_length, 1, 0),
        .source_node_id = 1,
        .target_node_id = 2,
        .stream_id = 10,
        .initial_term_id = 1,
        .active_term_id = 1,
        .term_length = 4096,
        .mtu = default_mtu,
        .sender_epoch = 123,
        .group_name_hash = 0xabcd,
    };
    var buf: [SetupHeader.encoded_length]u8 align(@alignOf(SetupHeader)) = undefined;
    @memcpy(&buf, bytesOf(setup));
    _ = try decodeSetup(&buf, default_mtu, 2, 0xabcd);
    try std.testing.expectError(error.InvalidTargetNode, decodeSetup(&buf, default_mtu, 3, 0xabcd));
    try std.testing.expectError(error.InvalidGroupHash, decodeSetup(&buf, default_mtu, 2, 1));
}

test "NAK range validation rejects zero and out-of-term requests" {
    var nak = NakHeader{
        .common = CommonHeader.init(.nak, @sizeOf(NakHeader), @sizeOf(NakHeader), 1, 0),
        .stream_id = 1,
        .term_id = 1,
        .term_offset = 32,
        .length = 64,
    };
    var buf: [@sizeOf(NakHeader)]u8 align(@alignOf(NakHeader)) = undefined;
    @memcpy(&buf, bytesOf(nak));
    _ = try decodeNak(&buf, default_mtu, 128);
    nak.length = 0;
    @memcpy(&buf, bytesOf(nak));
    try std.testing.expectError(error.InvalidNakRange, decodeNak(&buf, default_mtu, 128));
    nak.length = 128;
    @memcpy(&buf, bytesOf(nak));
    try std.testing.expectError(error.InvalidNakRange, decodeNak(&buf, default_mtu, 128));
}
