// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const protocol = @import("protocol.zig");
const Position = @import("position.zig").Position;

pub const LossRange = extern struct {
    term_id: i32,
    term_offset: u32,
    length: u32,
};

pub const RetransmitAction = struct {
    range: LossRange,
    active: bool = false,
    deadline_ns: i64 = 0,
};

pub const ScannedFrame = struct {
    header: *const protocol.DataHeader,
    payload: []const u8,
};

pub const TermLog = struct {
    allocator: std.mem.Allocator,
    terms: [3][]align(32) u8,
    initial_term_id: i32,
    active_term_id: i32,
    term_length: u32,
    mtu: u32,
    sender_position: u64,
    sender_limit: u64,

    pub fn init(
        allocator: std.mem.Allocator,
        initial_term_id: i32,
        term_length: u32,
        mtu: u32,
        sender_limit: u64,
    ) !TermLog {
        if (term_length == 0 or !std.math.isPowerOfTwo(term_length)) return error.TermLengthNotPowerOfTwo;
        if (mtu <= protocol.DataHeader.encoded_length) return error.InvalidMtu;

        var terms: [3][]align(32) u8 = undefined;
        var allocated: usize = 0;
        errdefer {
            for (terms[0..allocated]) |term| allocator.free(term);
        }
        for (&terms) |*term| {
            term.* = try allocator.alignedAlloc(u8, .@"32", term_length);
            @memset(term.*, 0);
            allocated += 1;
        }

        return .{
            .allocator = allocator,
            .terms = terms,
            .initial_term_id = initial_term_id,
            .active_term_id = initial_term_id,
            .term_length = term_length,
            .mtu = mtu,
            .sender_position = 0,
            .sender_limit = sender_limit,
        };
    }

    pub fn deinit(self: *TermLog) void {
        for (self.terms) |term| self.allocator.free(term);
        self.* = undefined;
    }

    pub fn appendData(self: *TermLog, header: protocol.DataHeader, payload: []const u8) !u64 {
        const frame_len = protocol.DataHeader.encoded_length + payload.len;
        if (frame_len > self.mtu) return error.FrameExceedsMtu;
        const aligned_len = Position.alignFrameLength(frame_len);
        if (self.sender_position + aligned_len > self.sender_limit) return error.SenderLimitReached;

        self.active_term_id = self.initial_term_id + @as(i32, @intCast(self.sender_position / self.term_length));
        var term_offset: u32 = @intCast(self.sender_position % self.term_length);
        if (term_offset + aligned_len > self.term_length) {
            try self.writePadding(term_offset);
            self.sender_position += self.term_length - term_offset;
            self.active_term_id = self.initial_term_id + @as(i32, @intCast(self.sender_position / self.term_length));
            term_offset = 0;
        }

        const partition = Position.partitionIndex(self.initial_term_id, self.active_term_id);
        const term = self.terms[partition];
        var frame_header = header;
        frame_header.term_id = self.active_term_id;
        frame_header.term_offset = term_offset;
        frame_header.common.frame_length = 0;

        const start_position = self.sender_position;
        @memcpy(term[term_offset..][0..protocol.DataHeader.encoded_length], std.mem.asBytes(&frame_header));
        if (payload.len > 0) {
            @memcpy(term[term_offset + protocol.DataHeader.encoded_length ..][0..payload.len], payload);
        }
        const committed_len_ptr: *u16 = @ptrCast(@alignCast(term[term_offset + @offsetOf(protocol.CommonHeader, "frame_length") ..].ptr));
        @atomicStore(u16, committed_len_ptr, @intCast(frame_len), .release);

        self.sender_position += aligned_len;
        return start_position;
    }

    pub fn scan(self: *const TermLog, term_id: i32, term_offset: u32, max_length: u32, out: []ScannedFrame) u32 {
        if (term_offset >= self.term_length) return 0;
        const partition = Position.partitionIndex(self.initial_term_id, term_id);
        const term = self.terms[partition];
        const limit = @min(self.term_length, term_offset + max_length);
        var offset = term_offset;
        var count: u32 = 0;
        while (offset + protocol.DataHeader.encoded_length <= limit and count < out.len) {
            const header: *const protocol.DataHeader = @ptrCast(@alignCast(term[offset..].ptr));
            const frame_len = @atomicLoad(u16, @as(*const u16, @ptrCast(@alignCast(term[offset + @offsetOf(protocol.CommonHeader, "frame_length") ..].ptr))), .acquire);
            if (frame_len == 0) break;
            if (frame_len == 0xffff) break;
            if (frame_len < protocol.DataHeader.encoded_length) break;
            const aligned = Position.alignFrameLength(frame_len);
            if (offset + aligned > limit) break;
            out[count] = .{
                .header = header,
                .payload = term[offset + protocol.DataHeader.encoded_length ..][0 .. frame_len - protocol.DataHeader.encoded_length],
            };
            count += 1;
            offset += @intCast(aligned);
        }
        return count;
    }

    pub fn retransmitScan(self: *const TermLog, range: LossRange, max_length: u32, out: []ScannedFrame) u32 {
        const clamped = @min(@min(range.length, max_length), self.term_length - range.term_offset);
        return self.scan(range.term_id, range.term_offset, clamped, out);
    }

    pub fn canRotate(self: *const TermLog, acknowledged_position: u64) bool {
        return acknowledged_position + @as(u64, self.term_length * 2) >= self.sender_position;
    }

    fn writePadding(self: *TermLog, term_offset: u32) !void {
        if (term_offset >= self.term_length) return;
        const partition = Position.partitionIndex(self.initial_term_id, self.active_term_id);
        const term = self.terms[partition];
        const remaining = self.term_length - term_offset;
        if (remaining >= @sizeOf(u16)) {
            const ptr: *u16 = @ptrCast(@alignCast(term[term_offset + @offsetOf(protocol.CommonHeader, "frame_length") ..].ptr));
            @atomicStore(u16, ptr, 0xffff, .release);
        }
    }
};

test "append single frame and scan it back" {
    var log = try TermLog.init(std.testing.allocator, 1, 1024, 256, 1024);
    defer log.deinit();
    const header = protocol.DataHeader.init(.{
        .session_id = 1,
        .stream_id = 2,
        .term_id = 1,
        .term_offset = 0,
        .message_length = 5,
        .payload_length = 5,
        .source_node_id = 1,
        .target_node_id = 2,
        .route_flags = 0xc0,
        .source_service_id = 10,
        .target_service_id = 20,
        .template_id = 42,
    });
    _ = try log.appendData(header, "hello");
    var out: [2]ScannedFrame = undefined;
    const n = log.scan(1, 0, 256, &out);
    try std.testing.expectEqual(@as(u32, 1), n);
    try std.testing.expectEqualStrings("hello", out[0].payload);
    try std.testing.expectEqual(
        @intFromPtr(out[0].header) + protocol.DataHeader.encoded_length,
        @intFromPtr(out[0].payload.ptr),
    );
}

test "append fragments preserve message id and offsets" {
    var log = try TermLog.init(std.testing.allocator, 1, 1024, 256, 1024);
    defer log.deinit();
    var header = protocol.DataHeader.init(.{
        .session_id = 1,
        .stream_id = 2,
        .term_id = 1,
        .term_offset = 0,
        .message_id = 99,
        .message_length = 10,
        .payload_length = 5,
        .fragment_offset = 0,
        .source_node_id = 1,
        .target_node_id = 2,
        .route_flags = 0x80,
        .source_service_id = 10,
        .target_service_id = 20,
        .template_id = 42,
    });
    _ = try log.appendData(header, "hello");
    header.fragment_offset = 5;
    header.route_flags = 0x40;
    _ = try log.appendData(header, "world");
    var out: [2]ScannedFrame = undefined;
    const n = log.scan(1, 0, 512, &out);
    try std.testing.expectEqual(@as(u32, 2), n);
    try std.testing.expectEqual(@as(u64, 99), out[1].header.message_id);
    try std.testing.expectEqual(@as(u32, 5), out[1].header.fragment_offset);
}

test "reject append when sender position reaches sender limit" {
    var log = try TermLog.init(std.testing.allocator, 1, 1024, 256, 64);
    defer log.deinit();
    const header = protocol.DataHeader.init(.{
        .session_id = 1,
        .stream_id = 2,
        .term_id = 1,
        .term_offset = 0,
        .message_length = 5,
        .payload_length = 5,
        .source_node_id = 1,
        .target_node_id = 2,
        .route_flags = 0xc0,
        .source_service_id = 10,
        .target_service_id = 20,
        .template_id = 42,
    });
    try std.testing.expectError(error.SenderLimitReached, log.appendData(header, "hello"));
}

test "retransmit scan clamps range to term boundary and max length" {
    var log = try TermLog.init(std.testing.allocator, 1, 1024, 256, 1024);
    defer log.deinit();
    const header = protocol.DataHeader.init(.{
        .session_id = 1,
        .stream_id = 2,
        .term_id = 1,
        .term_offset = 0,
        .message_length = 5,
        .payload_length = 5,
        .source_node_id = 1,
        .target_node_id = 2,
        .route_flags = 0xc0,
        .source_service_id = 10,
        .target_service_id = 20,
        .template_id = 42,
    });
    _ = try log.appendData(header, "hello");
    var out: [2]ScannedFrame = undefined;
    const n = log.retransmitScan(.{ .term_id = 1, .term_offset = 0, .length = 1024 }, 128, &out);
    try std.testing.expectEqual(@as(u32, 1), n);
}
