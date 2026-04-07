//! TCP frame parser — encoding and validation helpers for the TCP wire protocol.
//!
//! The TCP framing layer uses a 24-byte length-prefixed header (defined in
//! brz_tcp). This module provides broker-level helpers for encoding outbound
//! data frames and parsing/validating inbound frame headers.

const std = @import("std");
const constants = @import("../platform/constants.zig");

/// 24-byte TCP frame header (matches brz_tcp FrameHeader layout).
pub const TcpFrameHeader = extern struct {
    frame_length: u32 = 0,
    flags: u8 = 0,
    source_node_id: u8 = 0,
    target_node_id: u8 = 0,
    reserved_1: u8 = 0,
    source_service_id: u16 = 0,
    target_service_id: u16 = 0,
    template_id: u16 = 0,
    reserved_2: u16 = 0,
    correlation_id: i64 align(4) = 0,

    pub const size: u32 = @sizeOf(TcpFrameHeader);

    comptime {
        std.debug.assert(@sizeOf(TcpFrameHeader) == 24);
    }

    pub fn payloadLength(self: TcpFrameHeader) u32 {
        return self.frame_length - size;
    }

    pub fn isHeartbeat(self: TcpFrameHeader) bool {
        return self.flags & 0x01 != 0;
    }

    pub fn isAdmin(self: TcpFrameHeader) bool {
        return self.flags & constants.flag_admin != 0;
    }

    /// Return the payload portion of a frame buffer.
    pub fn payloadSlice(frame_buf: []const u8) []const u8 {
        if (frame_buf.len <= size) return &.{};
        return frame_buf[size..];
    }
};

/// Parsed frame — a typed view over a raw TCP frame.
pub const ParsedFrame = union(enum) {
    /// Data frame — carries application or admin payload.
    data: struct {
        header: TcpFrameHeader,
        payload: []const u8,
    },
    /// Heartbeat — header-only frame with no payload.
    heartbeat: TcpFrameHeader,
    /// Invalid or unrecognized frame.
    invalid: void,
};

/// Parse a complete frame buffer (header + payload) into a ParsedFrame.
pub fn parseFrame(buf: []const u8) ?ParsedFrame {
    if (buf.len < TcpFrameHeader.size) return null;

    const header_bytes: *const [TcpFrameHeader.size]u8 = @ptrCast(buf[0..TcpFrameHeader.size]);
    const header: TcpFrameHeader = @as(*const TcpFrameHeader, @ptrCast(@alignCast(header_bytes))).*;

    if (header.frame_length < TcpFrameHeader.size) return .{ .invalid = {} };
    if (header.frame_length > buf.len) return .{ .invalid = {} };

    if (header.isHeartbeat()) {
        return .{ .heartbeat = header };
    }

    return .{ .data = .{
        .header = header,
        .payload = buf[TcpFrameHeader.size..header.frame_length],
    } };
}

/// Encode a complete data frame (header + payload) into a buffer.
/// Returns the total number of bytes written, or null if the buffer is too small.
pub fn encodeDataFrame(
    buf: []u8,
    payload: []const u8,
    source_node_id: u8,
    target_node_id: u8,
    source_service_id: u16,
    target_service_id: u16,
    template_id: u16,
    correlation_id: i64,
    flags: u8,
) ?usize {
    const total_len = TcpFrameHeader.size + payload.len;
    if (buf.len < total_len) return null;

    const header: *TcpFrameHeader = @ptrCast(@alignCast(buf.ptr));
    header.* = .{
        .frame_length = @intCast(total_len),
        .flags = flags,
        .source_node_id = source_node_id,
        .target_node_id = target_node_id,
        .source_service_id = source_service_id,
        .target_service_id = target_service_id,
        .template_id = template_id,
        .correlation_id = correlation_id,
    };

    if (payload.len > 0) {
        @memcpy(buf[TcpFrameHeader.size..][0..payload.len], payload);
    }

    return total_len;
}

// ── Tests ─────────────────────────────────────────────────────────────

test "TcpFrameHeader size is 24 bytes" {
    try std.testing.expectEqual(@as(usize, 24), @sizeOf(TcpFrameHeader));
}

test "parseFrame heartbeat" {
    var buf: [24]u8 align(4) = [_]u8{0} ** 24;
    const header: *TcpFrameHeader = @ptrCast(@alignCast(&buf));
    header.* = .{
        .frame_length = 24,
        .flags = 0x01,
        .source_node_id = 1,
        .target_node_id = 2,
        .source_service_id = 0,
        .target_service_id = 0,
        .template_id = 0xFFFF,
    };

    const parsed = parseFrame(&buf);
    try std.testing.expect(parsed != null);
    try std.testing.expect(parsed.? == .heartbeat);
    try std.testing.expectEqual(@as(u8, 1), parsed.?.heartbeat.source_node_id);
}

test "parseFrame data" {
    var buf: [32]u8 align(4) = [_]u8{0} ** 32;
    const header: *TcpFrameHeader = @ptrCast(@alignCast(&buf));
    header.* = .{
        .frame_length = 32,
        .flags = 0,
        .source_node_id = 1,
        .target_node_id = 2,
        .source_service_id = 10,
        .target_service_id = 20,
        .template_id = 42,
    };
    @memcpy(buf[24..32], "testdata");

    const parsed = parseFrame(&buf);
    try std.testing.expect(parsed != null);
    try std.testing.expect(parsed.? == .data);
    try std.testing.expectEqual(@as(u8, 1), parsed.?.data.header.source_node_id);
    try std.testing.expectEqualStrings("testdata", parsed.?.data.payload);
}

test "parseFrame returns null for undersized buffer" {
    var buf: [4]u8 = [_]u8{0} ** 4;
    try std.testing.expect(parseFrame(&buf) == null);
}

test "encodeDataFrame roundtrip" {
    var buf: [64]u8 align(4) = [_]u8{0} ** 64;
    const payload = "hello";

    const total = encodeDataFrame(
        &buf,
        payload,
        1,
        2,
        100,
        200,
        42,
        999,
        0,
    );

    try std.testing.expect(total != null);
    try std.testing.expectEqual(@as(usize, 24 + payload.len), total.?);

    const parsed = parseFrame(buf[0..total.?]);
    try std.testing.expect(parsed != null);
    try std.testing.expect(parsed.? == .data);
    try std.testing.expectEqual(@as(u16, 100), parsed.?.data.header.source_service_id);
    try std.testing.expectEqualStrings(payload, parsed.?.data.payload);
}

test "encodeDataFrame rejects undersized buffer" {
    var buf: [20]u8 align(4) = [_]u8{0} ** 20;
    try std.testing.expect(encodeDataFrame(&buf, "x", 0, 0, 0, 0, 0, 0, 0) == null);
}
