//! TCP message framing layer.
//!
//! After the handshake, all data on a TCP stream is framed using a
//! length-prefixed protocol. Every message is preceded by a 24-byte header.
//! The FrameReader handles partial reads (TCP is a byte stream), and the
//! FrameWriter handles partial writes under backpressure.

const std = @import("std");

// ── Frame Header (24 bytes, little-endian) ───────────────────────────

/// 24-byte frame header for TCP message framing.
///
/// Wire layout:
///   Offset  Size  Type   Field
///   0       4     u32    frame_length (total including header)
///   4       1     u8     flags
///   5       1     u8     source_node_id
///   6       1     u8     target_node_id
///   7       1     u8     reserved_1
///   8       2     u16    source_service_id
///   10      2     u16    target_service_id
///   12      2     u16    template_id
///   14      2     u16    reserved_2
///   16      8     i64    correlation_id
pub const FrameHeader = extern struct {
    frame_length: u32,
    flags: u8,
    source_node_id: u8,
    target_node_id: u8,
    reserved_1: u8 = 0,
    source_service_id: u16,
    target_service_id: u16,
    template_id: u16,
    reserved_2: u16 = 0,
    correlation_id: i64 align(4) = 0,

    pub const size: u32 = @sizeOf(FrameHeader);
    pub const max_frame_length: u32 = 1_048_576; // 1 MB
    pub const min_frame_length: u32 = size; // 24 bytes (header-only = heartbeat)

    comptime {
        std.debug.assert(@sizeOf(FrameHeader) == 24);
    }

    pub const Flags = struct {
        pub const heartbeat: u8 = 0x01;
        pub const admin: u8 = 0x02;
    };

    pub fn isHeartbeat(self: FrameHeader) bool {
        return self.flags & Flags.heartbeat != 0;
    }

    pub fn isAdmin(self: FrameHeader) bool {
        return self.flags & Flags.admin != 0;
    }

    pub fn payloadLength(self: FrameHeader) u32 {
        return self.frame_length - size;
    }

    pub fn toBytes(self: *const FrameHeader) *const [size]u8 {
        return @ptrCast(self);
    }

    pub fn fromBytes(bytes: *const [size]u8) *const FrameHeader {
        return @ptrCast(@alignCast(bytes));
    }

    /// Build a heartbeat frame header.
    pub fn buildHeartbeat(source_node_id: u8, target_node_id: u8) FrameHeader {
        return .{
            .frame_length = size,
            .flags = Flags.heartbeat,
            .source_node_id = source_node_id,
            .target_node_id = target_node_id,
            .source_service_id = 0,
            .target_service_id = 0,
            .template_id = 0xFFFF,
        };
    }

    /// Build a data frame header.
    pub fn buildData(
        payload_len: u32,
        source_node_id: u8,
        target_node_id: u8,
        source_service_id: u16,
        target_service_id: u16,
        template_id: u16,
        correlation_id: i64,
        flags: u8,
    ) FrameHeader {
        return .{
            .frame_length = size + payload_len,
            .flags = flags,
            .source_node_id = source_node_id,
            .target_node_id = target_node_id,
            .source_service_id = source_service_id,
            .target_service_id = target_service_id,
            .template_id = template_id,
            .correlation_id = correlation_id,
        };
    }
};

fn validateFrameLength(frame_length: u32) !void {
    if (frame_length < FrameHeader.min_frame_length) {
        return error.FrameTooSmall;
    }
    if (frame_length > FrameHeader.max_frame_length) {
        return error.FrameTooLarge;
    }
}

/// Validate a received frame header.
pub fn validateHeader(header: FrameHeader) !void {
    try validateFrameLength(header.frame_length);
    if (header.reserved_1 != 0 or header.reserved_2 != 0) {
        return error.InvalidReservedField;
    }
    if (header.source_node_id == 0) {
        return error.InvalidSourceNode;
    }
}

// ── Partial Read State Machine ───────────────────────────────────────

/// Handles partial TCP reads, accumulating bytes until a complete frame
/// (header + payload) is available.
pub const FrameReader = struct {
    state: State,
    header_buf: [FrameHeader.size]u8 align(@alignOf(FrameHeader)),
    header_bytes_read: u8,
    payload_buf: ?[]u8,
    payload_bytes_read: u32,
    current_header: ?FrameHeader,
    allocator: std.mem.Allocator,

    pub const State = enum(u8) {
        read_header,
        read_payload,
        frame_ready,
    };

    pub fn init(allocator: std.mem.Allocator) FrameReader {
        return .{
            .state = .read_header,
            .header_buf = undefined,
            .header_bytes_read = 0,
            .payload_buf = null,
            .payload_bytes_read = 0,
            .current_header = null,
            .allocator = allocator,
        };
    }

    pub fn reset(self: *FrameReader) void {
        if (self.payload_buf) |buf| self.allocator.free(buf);
        self.* = init(self.allocator);
    }

    /// Feed received bytes into the reader. Returns the number of bytes
    /// consumed and whether a complete frame is now available.
    pub fn feed(self: *FrameReader, data: []const u8) !struct { consumed: u32, frame_ready: bool } {
        var offset: u32 = 0;
        var ready = false;

        while (offset < data.len and !ready) {
            switch (self.state) {
                .read_header => {
                    const need = FrameHeader.size - @as(u32, self.header_bytes_read);
                    const avail = @as(u32, @intCast(data.len)) - offset;
                    const take = @min(need, avail);
                    const dst_start = self.header_bytes_read;
                    @memcpy(
                        self.header_buf[dst_start..][0..take],
                        data[offset..][0..take],
                    );
                    self.header_bytes_read += @intCast(take);
                    offset += take;

                    if (self.header_bytes_read == FrameHeader.size) {
                        const header = FrameHeader.fromBytes(&self.header_buf).*;
                        try validateFrameLength(header.frame_length);
                        self.current_header = header;

                        const payload_len = header.payloadLength();
                        if (payload_len == 0) {
                            self.state = .frame_ready;
                            ready = true;
                        } else {
                            self.payload_buf = try self.allocator.alloc(u8, payload_len);
                            self.payload_bytes_read = 0;
                            self.state = .read_payload;
                        }
                    }
                },
                .read_payload => {
                    const header = self.current_header.?;
                    const payload_len = header.payloadLength();
                    const need = payload_len - self.payload_bytes_read;
                    const avail = @as(u32, @intCast(data.len)) - offset;
                    const take = @min(need, avail);
                    const dst_start = self.payload_bytes_read;
                    @memcpy(
                        self.payload_buf.?[dst_start..][0..take],
                        data[offset..][0..take],
                    );
                    self.payload_bytes_read += take;
                    offset += take;

                    if (self.payload_bytes_read == payload_len) {
                        self.state = .frame_ready;
                        ready = true;
                    }
                },
                .frame_ready => {
                    ready = true;
                },
            }
        }

        return .{ .consumed = offset, .frame_ready = ready };
    }

    /// Retrieve the completed frame. Caller takes ownership of the payload buffer.
    pub fn takeFrame(self: *FrameReader) struct { header: FrameHeader, payload: ?[]u8 } {
        std.debug.assert(self.state == .frame_ready);
        const header = self.current_header.?;
        const payload = self.payload_buf;
        // Reset for next frame without freeing the payload (caller owns it).
        self.state = .read_header;
        self.header_bytes_read = 0;
        self.payload_buf = null;
        self.payload_bytes_read = 0;
        self.current_header = null;
        return .{
            .header = header,
            .payload = payload,
        };
    }

    /// Get the buffer to read into for the current state.
    pub fn currentBuffer(self: *FrameReader) []u8 {
        return switch (self.state) {
            .read_header => self.header_buf[self.header_bytes_read..],
            .read_payload => if (self.payload_buf) |buf|
                buf[self.payload_bytes_read..]
            else
                &.{},
            .frame_ready => &.{},
        };
    }
};

// ── Partial Write State Machine ──────────────────────────────────────

/// Tracks partial TCP writes for a single frame (header + payload).
pub const FrameWriter = struct {
    state: State,
    header_buf: [FrameHeader.size]u8 align(@alignOf(FrameHeader)),
    header_bytes_sent: u8,
    payload: ?[]const u8,
    payload_bytes_sent: u32,

    pub const State = enum(u8) {
        idle,
        write_header,
        write_payload,
    };

    pub fn init() FrameWriter {
        return .{
            .state = .idle,
            .header_buf = undefined,
            .header_bytes_sent = 0,
            .payload = null,
            .payload_bytes_sent = 0,
        };
    }

    pub fn reset(self: *FrameWriter) void {
        self.* = init();
    }

    /// Begin writing a new frame. The header is serialized immediately.
    pub fn beginFrame(self: *FrameWriter, header: FrameHeader, payload: ?[]const u8) void {
        std.debug.assert(self.state == .idle);
        self.header_buf = header.toBytes().*;
        self.header_bytes_sent = 0;
        self.payload = payload;
        self.payload_bytes_sent = 0;
        self.state = .write_header;
    }

    /// Get the next slice of bytes to send. Returns null when the frame
    /// is fully written.
    pub fn pendingBytes(self: *FrameWriter) ?[]const u8 {
        return switch (self.state) {
            .idle => null,
            .write_header => self.header_buf[self.header_bytes_sent..],
            .write_payload => if (self.payload) |p|
                p[self.payload_bytes_sent..]
            else
                null,
        };
    }

    /// Record that `n` bytes were successfully sent.
    pub fn advance(self: *FrameWriter, n: u32) void {
        switch (self.state) {
            .write_header => {
                self.header_bytes_sent += @intCast(n);
                if (self.header_bytes_sent == FrameHeader.size) {
                    if (self.payload != null and self.payload.?.len > 0) {
                        self.state = .write_payload;
                    } else {
                        self.state = .idle;
                    }
                }
            },
            .write_payload => {
                self.payload_bytes_sent += n;
                const total = if (self.payload) |p| @as(u32, @intCast(p.len)) else 0;
                if (self.payload_bytes_sent == total) {
                    self.state = .idle;
                }
            },
            .idle => unreachable,
        }
    }

    pub fn isIdle(self: *const FrameWriter) bool {
        return self.state == .idle;
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "FrameHeader size is 24 bytes" {
    try testing.expectEqual(@as(usize, 24), @sizeOf(FrameHeader));
}

test "FrameHeader field offsets match wire format" {
    try testing.expectEqual(@as(usize, 0), @offsetOf(FrameHeader, "frame_length"));
    try testing.expectEqual(@as(usize, 4), @offsetOf(FrameHeader, "flags"));
    try testing.expectEqual(@as(usize, 5), @offsetOf(FrameHeader, "source_node_id"));
    try testing.expectEqual(@as(usize, 6), @offsetOf(FrameHeader, "target_node_id"));
    try testing.expectEqual(@as(usize, 7), @offsetOf(FrameHeader, "reserved_1"));
    try testing.expectEqual(@as(usize, 8), @offsetOf(FrameHeader, "source_service_id"));
    try testing.expectEqual(@as(usize, 10), @offsetOf(FrameHeader, "target_service_id"));
    try testing.expectEqual(@as(usize, 12), @offsetOf(FrameHeader, "template_id"));
    try testing.expectEqual(@as(usize, 14), @offsetOf(FrameHeader, "reserved_2"));
    try testing.expectEqual(@as(usize, 16), @offsetOf(FrameHeader, "correlation_id"));
}

test "FrameHeader roundtrip" {
    const header = FrameHeader{
        .frame_length = 48,
        .flags = 0,
        .source_node_id = 1,
        .target_node_id = 2,
        .source_service_id = 100,
        .target_service_id = 200,
        .template_id = 42,
        .correlation_id = 0x1234_5678_9ABC_DEF0,
    };
    const bytes = header.toBytes();
    const decoded = FrameHeader.fromBytes(bytes).*;

    try testing.expectEqual(header.frame_length, decoded.frame_length);
    try testing.expectEqual(header.source_node_id, decoded.source_node_id);
    try testing.expectEqual(header.target_node_id, decoded.target_node_id);
    try testing.expectEqual(header.source_service_id, decoded.source_service_id);
    try testing.expectEqual(header.target_service_id, decoded.target_service_id);
    try testing.expectEqual(header.template_id, decoded.template_id);
    try testing.expectEqual(header.correlation_id, decoded.correlation_id);
}

test "FrameHeader heartbeat builder" {
    const hb = FrameHeader.buildHeartbeat(1, 2);
    try testing.expectEqual(FrameHeader.size, hb.frame_length);
    try testing.expect(hb.isHeartbeat());
    try testing.expectEqual(@as(u8, 1), hb.source_node_id);
    try testing.expectEqual(@as(u8, 2), hb.target_node_id);
    try testing.expectEqual(@as(u16, 0xFFFF), hb.template_id);
    try testing.expectEqual(@as(u32, 0), hb.payloadLength());
}

test "FrameReader partial header" {
    var reader = FrameReader.init(testing.allocator);
    defer reader.reset();

    const header = FrameHeader.buildHeartbeat(1, 2);
    const bytes = header.toBytes();

    // Feed first 10 bytes.
    const r1 = try reader.feed(bytes[0..10]);
    try testing.expectEqual(@as(u32, 10), r1.consumed);
    try testing.expect(!r1.frame_ready);

    // Feed remaining 14 bytes.
    const r2 = try reader.feed(bytes[10..]);
    try testing.expectEqual(@as(u32, 14), r2.consumed);
    try testing.expect(r2.frame_ready);

    const frame = reader.takeFrame();
    try testing.expectEqual(FrameHeader.size, frame.header.frame_length);
    try testing.expect(frame.header.isHeartbeat());
    try testing.expectEqual(@as(?[]u8, null), frame.payload);
}

test "FrameReader with payload" {
    var reader = FrameReader.init(testing.allocator);
    defer reader.reset();

    const payload = "hello";
    const header = FrameHeader.buildData(
        @intCast(payload.len),
        1,
        2,
        10,
        20,
        1,
        42,
        0,
    );

    // Feed header + payload in one go.
    const header_bytes = header.toBytes();
    var buf: [29]u8 = undefined;
    @memcpy(buf[0..24], header_bytes);
    @memcpy(buf[24..], payload);

    const r = try reader.feed(&buf);
    try testing.expectEqual(@as(u32, 29), r.consumed);
    try testing.expect(r.frame_ready);

    const frame = reader.takeFrame();
    try testing.expectEqual(@as(u32, 29), frame.header.frame_length);
    try testing.expectEqualStrings(payload, frame.payload.?);

    // Caller must free.
    testing.allocator.free(frame.payload.?);
}

test "FrameReader rejects frame too large" {
    var reader = FrameReader.init(testing.allocator);
    defer reader.reset();

    // Manually construct a header with frame_length = 2 MB.
    var header_bytes: [24]u8 = [_]u8{0} ** 24;
    std.mem.writeInt(u32, header_bytes[0..4], 2_097_152, .little);
    header_bytes[5] = 1; // source_node_id

    const result = reader.feed(&header_bytes);
    try testing.expectError(error.FrameTooLarge, result);
}

test "FrameReader rejects frame too small" {
    var reader = FrameReader.init(testing.allocator);
    defer reader.reset();

    var header_bytes: [24]u8 = [_]u8{0} ** 24;
    std.mem.writeInt(u32, header_bytes[0..4], 10, .little); // too small
    header_bytes[5] = 1;

    const result = reader.feed(&header_bytes);
    try testing.expectError(error.FrameTooSmall, result);
}

test "FrameWriter partial send" {
    var writer = FrameWriter.init();

    const header = FrameHeader.buildData(4, 1, 2, 0, 0, 0, 0, 0);
    const payload = [_]u8{ 0xDE, 0xAD, 0xBE, 0xEF };
    writer.beginFrame(header, &payload);

    try testing.expect(!writer.isIdle());
    try testing.expect(writer.pendingBytes() != null);

    // Partial header send (10 bytes).
    writer.advance(10);
    try testing.expect(!writer.isIdle());

    // Remaining header (14 bytes).
    writer.advance(14);
    try testing.expect(!writer.isIdle());

    // Payload (4 bytes).
    writer.advance(4);
    try testing.expect(writer.isIdle());
}

test "FrameWriter header-only frame" {
    var writer = FrameWriter.init();
    const header = FrameHeader.buildHeartbeat(1, 2);
    writer.beginFrame(header, null);

    try testing.expect(!writer.isIdle());
    writer.advance(FrameHeader.size);
    try testing.expect(writer.isIdle());
}

test "validateHeader rejects zero source_node_id" {
    const header = FrameHeader{
        .frame_length = 24,
        .flags = 0,
        .source_node_id = 0,
        .target_node_id = 2,
        .source_service_id = 0,
        .target_service_id = 0,
        .template_id = 0,
    };
    try testing.expectError(error.InvalidSourceNode, validateHeader(header));
}

test "validateHeader rejects non-zero reserved" {
    const header = FrameHeader{
        .frame_length = 24,
        .flags = 0,
        .source_node_id = 1,
        .target_node_id = 2,
        .reserved_1 = 0xFF,
        .source_service_id = 0,
        .target_service_id = 0,
        .template_id = 0,
    };
    try testing.expectError(error.InvalidReservedField, validateHeader(header));
}
