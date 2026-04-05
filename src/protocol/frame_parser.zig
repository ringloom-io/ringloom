//! Frame parser — flyweight overlays and dispatch for the UDP wire protocol.
//!
//! All `read*` functions overlay packed structs onto byte slices via `@ptrCast`
//! for zero-copy access. The `parseFrame` function dispatches on frame_type
//! and returns a tagged union.

const std = @import("std");
const frames = @import("frames.zig");
const constants = @import("../platform/constants.zig");

/// Overlay a DataFrameHeader onto a byte slice (read-only, zero-copy).
/// Returns null if the buffer is too small or frame_length is invalid.
pub fn readDataFrame(buf: []const u8) ?*const frames.DataFrameHeader {
    if (buf.len < @sizeOf(frames.DataFrameHeader)) return null;

    const header: *const frames.DataFrameHeader = @ptrCast(@alignCast(buf.ptr));

    // Validate frame_length is at least the header size
    if (header.frame_length < @as(i32, @intCast(@sizeOf(frames.DataFrameHeader)))) return null;

    return header;
}

/// Overlay a DataFrameHeader onto a mutable byte slice (write, zero-copy).
/// The caller writes fields directly through the returned pointer.
pub fn writeDataFrame(buf: []u8) ?*frames.DataFrameHeader {
    if (buf.len < @sizeOf(frames.DataFrameHeader)) return null;

    const header: *frames.DataFrameHeader = @ptrCast(@alignCast(buf.ptr));
    header.* = .{
        .frame_length = 0, // caller must set
    };

    return header;
}

/// Parse just the common frame header to determine the frame type.
/// This is the first step in the receive path dispatch.
pub fn readFrameHeader(buf: []const u8) ?*const frames.FrameHeader {
    if (buf.len < @sizeOf(frames.FrameHeader)) return null;
    return @ptrCast(@alignCast(buf.ptr));
}

/// Overlay a SetupFrame onto a byte slice.
pub fn readSetupFrame(buf: []const u8) ?*const frames.SetupFrame {
    if (buf.len < @sizeOf(frames.SetupFrame)) return null;
    return @ptrCast(@alignCast(buf.ptr));
}

/// Overlay a StatusMessage onto a byte slice.
pub fn readStatusMessage(buf: []const u8) ?*const frames.StatusMessage {
    if (buf.len < @sizeOf(frames.StatusMessage)) return null;
    return @ptrCast(@alignCast(buf.ptr));
}

/// Overlay a NakFrame onto a byte slice.
pub fn readNakFrame(buf: []const u8) ?*const frames.NakFrame {
    if (buf.len < @sizeOf(frames.NakFrame)) return null;
    return @ptrCast(@alignCast(buf.ptr));
}

/// Dispatch on frame_type and return the specific frame type.
pub const ParsedFrame = union(enum) {
    data: *const frames.DataFrameHeader,
    setup: *const frames.SetupFrame,
    sm: *const frames.StatusMessage,
    nak: *const frames.NakFrame,
    pad: *const frames.FrameHeader,
    unknown: u16,
};

/// Parse a raw byte buffer into a typed frame. Returns null if the buffer
/// is too small for even the common header.
pub fn parseFrame(buf: []const u8) ?ParsedFrame {
    const header = readFrameHeader(buf) orelse return null;
    const frame_type = frames.FrameType.fromU16(header.frame_type);

    return switch (frame_type) {
        .data, .heartbeat => if (readDataFrame(buf)) |f| .{ .data = f } else null,
        .setup => if (readSetupFrame(buf)) |f| .{ .setup = f } else null,
        .sm => if (readStatusMessage(buf)) |f| .{ .sm = f } else null,
        .nak => if (readNakFrame(buf)) |f| .{ .nak = f } else null,
        .pad => .{ .pad = header },
        _ => .{ .unknown = header.frame_type },
    };
}

/// Encode a complete data frame (header + payload) into a buffer.
///
/// Returns the total number of bytes written, or null if the buffer is too small.
/// This is the primary send-path encoding function.
pub fn encodeDataFrame(
    buf: []u8,
    payload: []const u8,
    source_node_id: u8,
    target_node_id: u8,
    source_service_id: u16,
    target_service_id: u16,
    template_id: u16,
    correlation_id: i32,
    msg_flags: u8,
    sequence_number: i64,
) ?usize {
    const header_len = @sizeOf(frames.DataFrameHeader);
    const total_len = header_len + payload.len;
    if (buf.len < total_len) return null;

    const header: *frames.DataFrameHeader = @ptrCast(@alignCast(buf.ptr));
    header.* = .{
        .frame_length = @intCast(total_len),
        .flags = constants.flag_unfragmented,
        .source_node_id = source_node_id,
        .target_node_id = target_node_id,
        .source_service_id = source_service_id,
        .target_service_id = target_service_id,
        .template_id = template_id,
        .correlation_id = correlation_id,
        .msg_flags = msg_flags,
        .sequence_number = sequence_number,
    };

    // Copy payload immediately after header
    if (payload.len > 0) {
        @memcpy(buf[header_len..][0..payload.len], payload);
    }

    return total_len;
}

// ── Tests ─────────────────────────────────────────────────────────────

test "parseFrame dispatches DATA correctly" {
    // Given
    var buf: [64]u8 align(8) = [_]u8{0} ** 64;
    const header: *frames.DataFrameHeader = @ptrCast(@alignCast(&buf));
    header.* = .{ .frame_length = 40 };

    // When
    const parsed = parseFrame(&buf);

    // Then
    try std.testing.expect(parsed != null);
    try std.testing.expect(parsed.? == .data);
}

test "parseFrame dispatches SETUP correctly" {
    // Given
    var buf: [32]u8 align(8) = [_]u8{0} ** 32;
    const header: *frames.SetupFrame = @ptrCast(@alignCast(&buf));
    header.* = .{
        .frame_length = 24,
        .frame_type = @intFromEnum(frames.FrameType.setup),
        .source_node_id = 5,
        .log_buffer_length = 4 * 1024 * 1024,
        .mtu_length = 1408,
        .initial_sequence = 0,
    };

    // When
    const parsed = parseFrame(&buf);

    // Then
    try std.testing.expect(parsed != null);
    try std.testing.expect(parsed.? == .setup);
    try std.testing.expectEqual(@as(u8, 5), parsed.?.setup.source_node_id);
}

test "parseFrame dispatches SM correctly" {
    // Given
    var buf: [32]u8 align(8) = [_]u8{0} ** 32;
    const header: *frames.StatusMessage = @ptrCast(@alignCast(&buf));
    header.* = .{
        .frame_type = @intFromEnum(frames.FrameType.sm),
        .node_id = 3,
        .consumption_position = 1000,
        .receiver_window = 65536,
    };

    // When
    const parsed = parseFrame(&buf);

    // Then
    try std.testing.expect(parsed != null);
    try std.testing.expect(parsed.? == .sm);
    try std.testing.expectEqual(@as(u8, 3), parsed.?.sm.node_id);
}

test "parseFrame dispatches NAK correctly" {
    // Given
    var buf: [32]u8 align(8) = [_]u8{0} ** 32;
    const header: *frames.NakFrame = @ptrCast(@alignCast(&buf));
    header.* = .{
        .frame_type = @intFromEnum(frames.FrameType.nak),
        .node_id = 7,
        .position = 500,
        .length = 100,
    };

    // When
    const parsed = parseFrame(&buf);

    // Then
    try std.testing.expect(parsed != null);
    try std.testing.expect(parsed.? == .nak);
    try std.testing.expectEqual(@as(u8, 7), parsed.?.nak.node_id);
}

test "parseFrame dispatches PAD correctly" {
    // Given
    var buf: [8]u8 align(8) = [_]u8{0} ** 8;
    const header: *frames.FrameHeader = @ptrCast(@alignCast(&buf));
    header.* = .{
        .frame_length = 8,
        .frame_type = @intFromEnum(frames.FrameType.pad),
    };

    // When
    const parsed = parseFrame(&buf);

    // Then
    try std.testing.expect(parsed != null);
    try std.testing.expect(parsed.? == .pad);
}

test "parseFrame returns unknown for unrecognized frame type" {
    // Given
    var buf: [8]u8 align(8) = [_]u8{0} ** 8;
    const header: *frames.FrameHeader = @ptrCast(@alignCast(&buf));
    header.* = .{
        .frame_length = 8,
        .frame_type = 0xFF,
    };

    // When
    const parsed = parseFrame(&buf);

    // Then
    try std.testing.expect(parsed != null);
    try std.testing.expect(parsed.? == .unknown);
    try std.testing.expectEqual(@as(u16, 0xFF), parsed.?.unknown);
}

test "parseFrame returns null for undersized buffer" {
    // Given: buffer too small for even a frame header
    var buf: [4]u8 = [_]u8{0} ** 4;

    // When / Then
    try std.testing.expect(parseFrame(&buf) == null);
}

test "readDataFrame rejects undersized buffer" {
    // Given: buffer too small for a data frame header
    var buf: [20]u8 = [_]u8{0} ** 20;

    // When / Then
    try std.testing.expect(readDataFrame(&buf) == null);
}

test "readDataFrame rejects invalid frame_length" {
    // Given: frame_length smaller than header size
    var buf: [64]u8 align(8) = [_]u8{0} ** 64;
    const header: *frames.DataFrameHeader = @ptrCast(@alignCast(&buf));
    header.* = .{ .frame_length = 10 }; // too small

    // When / Then
    try std.testing.expect(readDataFrame(&buf) == null);
}

test "writeDataFrame initializes header in mutable buffer" {
    // Given
    var buf: [64]u8 align(8) = [_]u8{0xFF} ** 64;

    // When
    const header = writeDataFrame(&buf);

    // Then
    try std.testing.expect(header != null);
    try std.testing.expectEqual(@as(i32, 0), header.?.frame_length);
}

test "writeDataFrame rejects undersized buffer" {
    // Given
    var buf: [20]u8 = [_]u8{0} ** 20;

    // When / Then
    try std.testing.expect(writeDataFrame(&buf) == null);
}

test "encodeDataFrame roundtrip" {
    // Given
    var buf: [128]u8 align(8) = [_]u8{0} ** 128;
    const payload = "test-payload";

    // When
    const total = encodeDataFrame(
        &buf,
        payload,
        1, // source_node_id
        2, // target_node_id
        100, // source_service_id
        200, // target_service_id
        42, // template_id
        999, // correlation_id
        0, // msg_flags
        77, // sequence_number
    );

    // Then
    try std.testing.expect(total != null);
    try std.testing.expectEqual(@as(usize, 40 + payload.len), total.?);

    // Verify via read
    const read = readDataFrame(&buf).?;
    try std.testing.expectEqual(@as(i32, @intCast(total.?)), read.frame_length);
    try std.testing.expectEqual(@as(u8, 1), read.source_node_id);
    try std.testing.expectEqual(@as(u8, 2), read.target_node_id);
    try std.testing.expectEqual(@as(u16, 100), read.source_service_id);
    try std.testing.expectEqual(@as(u16, 200), read.target_service_id);
    try std.testing.expectEqual(@as(u16, 42), read.template_id);
    try std.testing.expectEqual(@as(i32, 999), read.correlation_id);
    try std.testing.expectEqual(@as(i64, 77), read.sequence_number);

    // Verify payload
    const read_payload = frames.DataFrameHeader.payloadSlice(buf[0..total.?]);
    try std.testing.expectEqualStrings(payload, read_payload);
}

test "encodeDataFrame rejects undersized buffer" {
    // Given
    var buf: [30]u8 align(8) = [_]u8{0} ** 30;

    // When / Then
    try std.testing.expect(encodeDataFrame(&buf, "x", 0, 0, 0, 0, 0, 0, 0, 0) == null);
}
