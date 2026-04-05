//! UDP wire protocol frame definitions.
//!
//! All on-wire frames use little-endian byte order and are aligned to 4 bytes.
//! Every frame begins with a common 8-byte header. Specific frame types extend
//! this header with additional fields.
//!
//! All frame structs use `extern struct` for C-compatible memory layout that
//! matches the wire format. Fields of type i64 use `align(4)` to prevent the
//! compiler from inserting padding that would break the wire layout.
//!
//! The flyweight pattern: these structs can be overlaid directly onto byte
//! buffers via `@ptrCast` for zero-copy access.

const std = @import("std");
const constants = @import("../platform/constants.zig");

// ── Frame Types ───────────────────────────────────────────────────────

/// Discriminates the frame type in the common header.
pub const FrameType = enum(u16) {
    /// Padding frame — receiver skips.
    pad = 0x00,
    /// Data frame carrying a BRZ message.
    data = 0x01,
    /// Negative acknowledgement (retransmit request).
    nak = 0x02,
    /// Status message (flow control + receiver window).
    sm = 0x03,
    /// Connection establishment.
    setup = 0x04,
    /// Keepalive (zero-length data frame).
    heartbeat = 0x05,
    /// Allow unknown values for forward compatibility.
    _,

    pub fn fromU16(value: u16) FrameType {
        return @enumFromInt(value);
    }
};

// ── Common Frame Header (8 bytes) ────────────────────────────────────

/// Common header shared by all frame types. 8 bytes, 4-byte aligned.
pub const FrameHeader = extern struct {
    /// Total frame size in bytes, including this header and any payload.
    /// Must be >= 8 (header only) and <= MTU.
    frame_length: i32,

    /// Protocol version. Always 0 for current protocol.
    version: u8 = constants.frame_header_version,

    /// Flags. Interpretation depends on frame_type.
    flags: u8 = 0,

    /// Discriminates the frame type. See FrameType enum.
    frame_type: u16,

    comptime {
        std.debug.assert(@sizeOf(FrameHeader) == 8);
    }
};

// ── Data Frame Header (40 bytes) ─────────────────────────────────────

/// Data frame header — 40 bytes. Carries a BRZ message between brokers.
///
/// Layout is designed so the receiver can route a message with a single
/// read of this header: source/target node and service IDs are at fixed
/// offsets, and the payload immediately follows at byte 40.
pub const DataFrameHeader = extern struct {
    // --- common header (8 bytes) ---
    frame_length: i32,
    version: u8 = constants.frame_header_version,
    flags: u8 = 0,
    frame_type: u16 = @intFromEnum(FrameType.data),

    // --- transport fields ---
    term_offset: i32 = 0,
    source_node_id: u8 = 0,
    target_node_id: u8 = 0,
    source_service_id: u16 = 0,
    target_service_id: u16 = 0,

    // --- routing fields ---
    template_id: u16 = 0,
    correlation_id: i32 = 0,
    msg_flags: u8 = 0,
    reserved: [7]u8 = [_]u8{0} ** 7,

    // --- ordering ---
    // align(4) prevents the compiler from padding to 8-byte alignment,
    // which would break the wire format (sequence_number is at offset 32).
    sequence_number: i64 align(4) = 0,

    comptime {
        std.debug.assert(@sizeOf(DataFrameHeader) == constants.data_frame_header_length);
        std.debug.assert(@sizeOf(DataFrameHeader) == 40);
    }

    /// Returns the payload portion of a frame buffer, assuming the buffer
    /// starts at the beginning of this header.
    pub fn payloadSlice(buf: []const u8) []const u8 {
        const header_len = @sizeOf(DataFrameHeader);
        if (buf.len <= header_len) return buf[0..0];
        return buf[header_len..];
    }

    /// Returns the frame length from raw bytes without constructing the full header.
    pub fn peekFrameLength(buf: []const u8) ?i32 {
        if (buf.len < 4) return null;
        return std.mem.readInt(i32, buf[0..4], .little);
    }

    /// Returns true if this is an unfragmented (complete) message.
    pub fn isUnfragmented(self: DataFrameHeader) bool {
        return (self.flags & constants.flag_unfragmented) == constants.flag_unfragmented;
    }

    /// Returns true if this is the first fragment (BEGIN set).
    pub fn isBegin(self: DataFrameHeader) bool {
        return (self.flags & constants.flag_begin) != 0;
    }

    /// Returns true if this is the last fragment (END set).
    pub fn isEnd(self: DataFrameHeader) bool {
        return (self.flags & constants.flag_end) != 0;
    }

    /// Returns true if this is an admin/cluster message.
    pub fn isAdmin(self: DataFrameHeader) bool {
        return (self.flags & constants.flag_admin) != 0;
    }
};

// ── Setup Frame (24 bytes) ───────────────────────────────────────────

/// Setup frame — 24 bytes. Sent to establish a connection with a peer.
pub const SetupFrame = extern struct {
    // --- common header ---
    frame_length: i32 = @sizeOf(SetupFrame),
    version: u8 = constants.frame_header_version,
    flags: u8 = 0,
    frame_type: u16 = @intFromEnum(FrameType.setup),

    // --- setup fields ---
    source_node_id: u8 = 0,
    _reserved0: u8 = 0,
    _reserved1: u16 = 0,
    log_buffer_length: i32 = 0,
    mtu_length: i32 = 0,
    initial_sequence: i32 = 0,

    comptime {
        std.debug.assert(@sizeOf(SetupFrame) == 24);
    }
};

// ── Status Message (28 bytes) ────────────────────────────────────────

/// Status Message — 28 bytes. Receiver → Sender flow control.
pub const StatusMessage = extern struct {
    // --- common header ---
    frame_length: i32 = @sizeOf(StatusMessage),
    version: u8 = constants.frame_header_version,
    flags: u8 = 0,
    frame_type: u16 = @intFromEnum(FrameType.sm),

    // --- status fields ---
    node_id: u8 = 0,
    _reserved0: u8 = 0,
    _reserved1: u16 = 0,
    // align(4) to prevent 8-byte padding (wire offset must be 12, not 16).
    consumption_position: i64 align(4) = 0,
    receiver_window: i32 = 0,
    _reserved2: i32 = 0,

    comptime {
        std.debug.assert(@sizeOf(StatusMessage) == 28);
    }
};

// ── NAK Frame (24 bytes) ─────────────────────────────────────────────

/// NAK frame — 24 bytes. Receiver → Sender retransmit request.
pub const NakFrame = extern struct {
    // --- common header ---
    frame_length: i32 = @sizeOf(NakFrame),
    version: u8 = constants.frame_header_version,
    flags: u8 = 0,
    frame_type: u16 = @intFromEnum(FrameType.nak),

    // --- nak fields ---
    node_id: u8 = 0,
    _reserved0: u8 = 0,
    _reserved1: u16 = 0,
    // align(4) to prevent 8-byte padding (wire offset must be 12, not 16).
    position: i64 align(4) = 0,
    length: i32 = 0,

    comptime {
        std.debug.assert(@sizeOf(NakFrame) == 24);
    }
};

// ── Heartbeat Helper ─────────────────────────────────────────────────

/// Create a heartbeat frame for the given peer link.
///
/// A heartbeat is a zero-length data frame:
/// - `frame_length = 40` (header only)
/// - `flags = UNFRAGMENTED` (0xC0)
/// - `frame_type = DATA` (0x01)
/// - `sequence_number = current_sequence` (no increment — heartbeats don't consume sequence space)
pub fn makeHeartbeat(
    source_node_id: u8,
    target_node_id: u8,
    current_sequence: i64,
) DataFrameHeader {
    return .{
        .frame_length = @sizeOf(DataFrameHeader),
        .flags = constants.flag_unfragmented,
        .frame_type = @intFromEnum(FrameType.data),
        .source_node_id = source_node_id,
        .target_node_id = target_node_id,
        .sequence_number = current_sequence,
    };
}

// ── Tests ─────────────────────────────────────────────────────────────

test "FrameHeader is exactly 8 bytes" {
    try std.testing.expectEqual(@as(usize, 8), @sizeOf(FrameHeader));
}

test "DataFrameHeader is exactly 40 bytes" {
    try std.testing.expectEqual(@as(usize, 40), @sizeOf(DataFrameHeader));
}

test "SetupFrame is exactly 24 bytes" {
    try std.testing.expectEqual(@as(usize, 24), @sizeOf(SetupFrame));
}

test "StatusMessage is exactly 28 bytes" {
    try std.testing.expectEqual(@as(usize, 28), @sizeOf(StatusMessage));
}

test "NakFrame is exactly 24 bytes" {
    try std.testing.expectEqual(@as(usize, 24), @sizeOf(NakFrame));
}

test "DataFrameHeader roundtrip through byte buffer" {
    // Given: a buffer large enough for a data frame with payload
    var buf: [128]u8 align(8) = [_]u8{0} ** 128;
    const payload = "hello, world";
    const total_len: i32 = @intCast(@sizeOf(DataFrameHeader) + payload.len);

    // When: we write a data frame header + payload
    const header: *DataFrameHeader = @ptrCast(@alignCast(&buf));
    header.* = .{
        .frame_length = total_len,
        .flags = 0xC0, // UNFRAGMENTED
        .source_node_id = 1,
        .target_node_id = 2,
        .source_service_id = 100,
        .target_service_id = 200,
        .template_id = 42,
        .correlation_id = 12345,
        .msg_flags = 0,
        .sequence_number = 99,
    };
    @memcpy(buf[40..][0..payload.len], payload);

    // Then: reading back yields the same values
    const read_header: *const DataFrameHeader = @ptrCast(@alignCast(&buf));
    try std.testing.expectEqual(@as(i32, total_len), read_header.frame_length);
    try std.testing.expectEqual(@as(u8, 1), read_header.source_node_id);
    try std.testing.expectEqual(@as(u8, 2), read_header.target_node_id);
    try std.testing.expectEqual(@as(u16, 100), read_header.source_service_id);
    try std.testing.expectEqual(@as(u16, 200), read_header.target_service_id);
    try std.testing.expectEqual(@as(u16, 42), read_header.template_id);
    try std.testing.expectEqual(@as(i32, 12345), read_header.correlation_id);
    try std.testing.expectEqual(@as(i64, 99), read_header.sequence_number);
    try std.testing.expect(read_header.isUnfragmented());
    try std.testing.expect(!read_header.isAdmin());

    // Verify payload is accessible
    const read_payload = DataFrameHeader.payloadSlice(buf[0..@intCast(total_len)]);
    try std.testing.expectEqualStrings(payload, read_payload);
}

test "makeHeartbeat produces valid data frame" {
    // Given / When
    const hb = makeHeartbeat(1, 2, 42);

    // Then
    try std.testing.expectEqual(@as(i32, 40), hb.frame_length);
    try std.testing.expectEqual(@as(u8, 0xC0), hb.flags); // UNFRAGMENTED
    try std.testing.expectEqual(@as(u8, 1), hb.source_node_id);
    try std.testing.expectEqual(@as(u8, 2), hb.target_node_id);
    try std.testing.expectEqual(@as(i64, 42), hb.sequence_number);
}

test "DataFrameHeader flags helpers" {
    // Given
    var header: DataFrameHeader = .{ .frame_length = 40 };

    // When: unfragmented
    header.flags = 0xC0;
    try std.testing.expect(header.isUnfragmented());
    try std.testing.expect(header.isBegin());
    try std.testing.expect(header.isEnd());
    try std.testing.expect(!header.isAdmin());

    // When: begin-only fragment
    header.flags = 0x80;
    try std.testing.expect(!header.isUnfragmented());
    try std.testing.expect(header.isBegin());
    try std.testing.expect(!header.isEnd());

    // When: admin message
    header.flags = 0xE0; // UNFRAGMENTED | ADMIN
    try std.testing.expect(header.isAdmin());
    try std.testing.expect(header.isUnfragmented());
}

test "DataFrameHeader byte-level layout" {
    const header = DataFrameHeader{
        .frame_length = 0x01020304,
        .version = 0x00,
        .flags = 0xC0,
        .frame_type = 0x0001,
        .term_offset = 0,
        .source_node_id = 0x0A,
        .target_node_id = 0x0B,
        .source_service_id = 0x0064,
        .target_service_id = 0x00C8,
        .template_id = 0x002A,
        .correlation_id = 0x00003039,
        .msg_flags = 0,
        .reserved = [_]u8{0} ** 7,
        .sequence_number = 0x63,
    };

    const bytes: *const [40]u8 = @ptrCast(&header);

    // frame_length at offset 0 (little-endian i32)
    try std.testing.expectEqual(@as(u8, 0x04), bytes[0]);
    try std.testing.expectEqual(@as(u8, 0x03), bytes[1]);
    try std.testing.expectEqual(@as(u8, 0x02), bytes[2]);
    try std.testing.expectEqual(@as(u8, 0x01), bytes[3]);

    // version at offset 4
    try std.testing.expectEqual(@as(u8, 0x00), bytes[4]);

    // flags at offset 5
    try std.testing.expectEqual(@as(u8, 0xC0), bytes[5]);

    // frame_type at offset 6 (little-endian u16)
    try std.testing.expectEqual(@as(u8, 0x01), bytes[6]);
    try std.testing.expectEqual(@as(u8, 0x00), bytes[7]);

    // source_node_id at offset 12
    try std.testing.expectEqual(@as(u8, 0x0A), bytes[12]);

    // target_node_id at offset 13
    try std.testing.expectEqual(@as(u8, 0x0B), bytes[13]);

    // sequence_number at offset 32 (little-endian i64)
    try std.testing.expectEqual(@as(u8, 0x63), bytes[32]);
}

test "FrameType fromU16 known and unknown values" {
    try std.testing.expectEqual(FrameType.data, FrameType.fromU16(0x01));
    try std.testing.expectEqual(FrameType.setup, FrameType.fromU16(0x04));

    // Unknown type — should not crash
    const unknown = FrameType.fromU16(0xFF);
    _ = unknown;
}

test "DataFrameHeader peekFrameLength" {
    var buf: [64]u8 align(8) = [_]u8{0} ** 64;
    const header: *DataFrameHeader = @ptrCast(@alignCast(&buf));
    header.* = .{ .frame_length = 52 };

    try std.testing.expectEqual(@as(i32, 52), DataFrameHeader.peekFrameLength(&buf).?);

    // Too small buffer
    var tiny: [2]u8 = undefined;
    try std.testing.expect(DataFrameHeader.peekFrameLength(&tiny) == null);
}

test "DataFrameHeader payloadSlice with no payload" {
    var buf: [40]u8 align(8) = [_]u8{0} ** 40;
    const result = DataFrameHeader.payloadSlice(&buf);
    try std.testing.expectEqual(@as(usize, 0), result.len);
}

test "DataFrameHeader field offsets match wire format" {
    // Verify critical field offsets
    try std.testing.expectEqual(@as(usize, 0), @offsetOf(DataFrameHeader, "frame_length"));
    try std.testing.expectEqual(@as(usize, 4), @offsetOf(DataFrameHeader, "version"));
    try std.testing.expectEqual(@as(usize, 5), @offsetOf(DataFrameHeader, "flags"));
    try std.testing.expectEqual(@as(usize, 6), @offsetOf(DataFrameHeader, "frame_type"));
    try std.testing.expectEqual(@as(usize, 8), @offsetOf(DataFrameHeader, "term_offset"));
    try std.testing.expectEqual(@as(usize, 12), @offsetOf(DataFrameHeader, "source_node_id"));
    try std.testing.expectEqual(@as(usize, 13), @offsetOf(DataFrameHeader, "target_node_id"));
    try std.testing.expectEqual(@as(usize, 14), @offsetOf(DataFrameHeader, "source_service_id"));
    try std.testing.expectEqual(@as(usize, 16), @offsetOf(DataFrameHeader, "target_service_id"));
    try std.testing.expectEqual(@as(usize, 18), @offsetOf(DataFrameHeader, "template_id"));
    try std.testing.expectEqual(@as(usize, 20), @offsetOf(DataFrameHeader, "correlation_id"));
    try std.testing.expectEqual(@as(usize, 24), @offsetOf(DataFrameHeader, "msg_flags"));
    try std.testing.expectEqual(@as(usize, 25), @offsetOf(DataFrameHeader, "reserved"));
    try std.testing.expectEqual(@as(usize, 32), @offsetOf(DataFrameHeader, "sequence_number"));
}
