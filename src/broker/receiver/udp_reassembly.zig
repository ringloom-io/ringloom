// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const udp = @import("ringloom_udp");

pub const route_flag_begin: u16 = 0x80;
pub const route_flag_end: u16 = 0x40;
pub const route_flag_unfragmented: u16 = route_flag_begin | route_flag_end;

pub const InsertResult = enum {
    inserted,
    duplicate,
    stale,
    wrong_session,
    wrong_source,
    wrong_stream,
};

pub const Delivery = struct {
    header: udp.DataHeader,
    payload: []const u8,
};

pub const StreamReceiverOptions = struct {
    term_length: u32 = 64 * 1024,
    window_length: u32 = 64 * 1024,
    max_message_length: u32 = 64 * 1024,
    initial_nak_delay_ns: i64 = 50 * std.time.ns_per_us,
    nak_retry_delay_ns: i64 = 250 * std.time.ns_per_us,
    status_interval_ns: i64 = 500 * std.time.ns_per_ms,
};

pub const StreamReceiver = struct {
    allocator: std.mem.Allocator,
    stream_id: u32,
    source_node_id: u8,
    session_id: u32,
    initial_term_id: i32,
    receive_window: udp.ReceiveWindow,
    reassembly_buf: []u8,
    reassembly_header: ?udp.DataHeader = null,
    reassembly_message_id: u64 = 0,
    reassembly_received: u32 = 0,
    duplicates: u64 = 0,
    stale_session_packets: u64 = 0,
    next_status_ns: i64 = 0,
    next_nak_ns: i64 = 0,
    last_status_position: u64 = 0,
    options: StreamReceiverOptions,

    pub fn init(
        allocator: std.mem.Allocator,
        stream_id: u32,
        source_node_id: u8,
        session_id: u32,
        initial_term_id: i32,
        options: StreamReceiverOptions,
    ) !StreamReceiver {
        const window = try udp.ReceiveWindow.init(
            allocator,
            initial_term_id,
            options.term_length,
            options.window_length,
        );
        errdefer {
            var mutable = window;
            mutable.deinit();
        }
        const reassembly_buf = try allocator.alloc(u8, options.max_message_length);
        errdefer allocator.free(reassembly_buf);
        return .{
            .allocator = allocator,
            .stream_id = stream_id,
            .source_node_id = source_node_id,
            .session_id = session_id,
            .initial_term_id = initial_term_id,
            .receive_window = window,
            .reassembly_buf = reassembly_buf,
            .options = options,
        };
    }

    pub fn deinit(self: *StreamReceiver) void {
        self.receive_window.deinit();
        self.allocator.free(self.reassembly_buf);
        self.* = undefined;
    }

    pub fn insertData(self: *StreamReceiver, header: udp.DataHeader, payload: []const u8, now_ns: i64) !InsertResult {
        if (header.common.session_id != self.session_id) {
            self.stale_session_packets += 1;
            return .wrong_session;
        }
        if (header.source_node_id != self.source_node_id) return .wrong_source;
        if (header.stream_id != self.stream_id) return .wrong_stream;

        const position = udp.Position.absolute(
            self.initial_term_id,
            header.term_id,
            header.term_offset,
            self.options.term_length,
        );
        if (position < self.receive_window.consumed_position) return .stale;
        if (self.receive_window.hasCommittedPosition(position)) {
            self.duplicates += 1;
            return .duplicate;
        }

        self.receive_window.insert(header, payload) catch |err| switch (err) {
            error.DuplicateFrame => {
                self.duplicates += 1;
                return .duplicate;
            },
            error.StaleFrame => return .stale,
            else => return err,
        };

        if (self.receive_window.high_water_mark > self.receive_window.rebuild_position and
            self.receive_window.nextContiguous() == null and
            self.next_nak_ns == 0)
        {
            self.next_nak_ns = now_ns + self.options.initial_nak_delay_ns;
        }
        return .inserted;
    }

    pub fn rebuildNext(self: *StreamReceiver) !?Delivery {
        while (true) {
            const frame = self.receive_window.nextContiguous() orelse {
                if (self.receive_window.skipTermTailIfNoFrameCanFit()) continue;
                return null;
            };
            const header = frame.header.*;
            const payload = frame.payload;
            if ((header.route_flags & route_flag_unfragmented) == route_flag_unfragmented) {
                _ = self.receive_window.advanceRebuild();
                return .{ .header = header, .payload = payload };
            }

            const delivery = try self.acceptFragment(header, payload);
            _ = self.receive_window.advanceRebuild();
            if (delivery) |ready| return ready;
        }
        return null;
    }

    pub fn statusDue(self: *StreamReceiver, now_ns: i64) bool {
        if (udp.flow_control.shouldForceStatus(
            self.last_status_position,
            self.receive_window.consumed_position,
            self.options.window_length,
        )) return true;
        return now_ns >= self.next_status_ns;
    }

    pub fn makeStatus(self: *StreamReceiver, now_ns: i64, receiver_window: u32) udp.StatusHeader {
        self.next_status_ns = now_ns + self.options.status_interval_ns;
        self.last_status_position = self.receive_window.consumed_position;
        const term_id = self.initial_term_id + @as(i32, @intCast(self.receive_window.consumed_position / self.options.term_length));
        const term_offset: u32 = @intCast(self.receive_window.consumed_position % self.options.term_length);
        return .{
            .common = udp.CommonHeader.init(.status, @sizeOf(udp.StatusHeader), @sizeOf(udp.StatusHeader), self.session_id, 0),
            .stream_id = self.stream_id,
            .consumption_term_id = term_id,
            .consumption_term_offset = term_offset,
            .receiver_window = receiver_window,
            .receiver_id = 1,
            .highest_contiguous_message_id = 0,
        };
    }

    pub fn nakDue(self: *StreamReceiver, now_ns: i64) bool {
        return self.next_nak_ns != 0 and now_ns >= self.next_nak_ns and self.receive_window.nextContiguous() == null;
    }

    pub fn makeNak(self: *StreamReceiver, now_ns: i64) udp.NakHeader {
        const term_id = self.initial_term_id + @as(i32, @intCast(self.receive_window.rebuild_position / self.options.term_length));
        const term_offset: u32 = @intCast(self.receive_window.rebuild_position % self.options.term_length);
        self.next_nak_ns = now_ns + self.options.nak_retry_delay_ns;
        return .{
            .common = udp.CommonHeader.init(.nak, @sizeOf(udp.NakHeader), @sizeOf(udp.NakHeader), self.session_id, 0),
            .stream_id = self.stream_id,
            .term_id = term_id,
            .term_offset = term_offset,
            .length = @min(self.options.window_length, self.options.term_length - term_offset),
        };
    }

    fn acceptFragment(self: *StreamReceiver, header: udp.DataHeader, payload: []const u8) !?Delivery {
        if (header.message_length > self.reassembly_buf.len) return error.MessageTooLong;
        if ((header.route_flags & route_flag_begin) != 0) {
            self.reassembly_header = header;
            self.reassembly_message_id = header.message_id;
            self.reassembly_received = 0;
        }

        const base = self.reassembly_header orelse return error.MissingBeginFragment;
        if (header.message_id != self.reassembly_message_id or
            header.source_service_id != base.source_service_id or
            header.target_service_id != base.target_service_id or
            header.target_node_id != base.target_node_id or
            header.template_id != base.template_id)
        {
            self.reassembly_header = null;
            self.reassembly_received = 0;
            return error.InconsistentRouteMetadata;
        }
        if (header.fragment_offset + payload.len > header.message_length) return error.FragmentOutOfBounds;

        @memcpy(self.reassembly_buf[header.fragment_offset..][0..payload.len], payload);
        self.reassembly_received += @intCast(payload.len);
        if ((header.route_flags & route_flag_end) == 0) return null;
        if (self.reassembly_received != header.message_length) return error.IncompleteMessage;

        var ready_header = base;
        ready_header.route_flags = route_flag_unfragmented;
        ready_header.fragment_offset = 0;
        ready_header.common.frame_length = @intCast(udp.DataHeader.encoded_length + header.message_length);
        self.reassembly_header = null;
        return .{
            .header = ready_header,
            .payload = self.reassembly_buf[0..header.message_length],
        };
    }
};

fn testHeader(fields: struct {
    session_id: u32 = 7,
    stream_id: u32 = 11,
    term_offset: u32 = 0,
    message_id: u64 = 1,
    fragment_offset: u32 = 0,
    message_length: u32,
    payload_length: usize,
    route_flags: u16 = route_flag_unfragmented,
}) udp.DataHeader {
    return udp.DataHeader.init(.{
        .session_id = fields.session_id,
        .stream_id = fields.stream_id,
        .term_id = 1,
        .term_offset = fields.term_offset,
        .message_id = fields.message_id,
        .fragment_offset = fields.fragment_offset,
        .message_length = fields.message_length,
        .payload_length = fields.payload_length,
        .source_node_id = 1,
        .target_node_id = 2,
        .route_flags = fields.route_flags,
        .source_service_id = 10,
        .target_service_id = 20,
        .template_id = 42,
    });
}

test "in-order DATA advances rebuild and consumed positions" {
    var receiver = try StreamReceiver.init(std.testing.allocator, 11, 1, 7, 1, .{
        .term_length = 1024,
        .window_length = 1024,
        .max_message_length = 1024,
    });
    defer receiver.deinit();

    try std.testing.expectEqual(InsertResult.inserted, try receiver.insertData(testHeader(.{
        .message_length = 5,
        .payload_length = 5,
    }), "hello", 100));
    const delivery = (try receiver.rebuildNext()).?;
    try std.testing.expectEqualStrings("hello", delivery.payload);
    try std.testing.expectEqual(@as(u64, 96), receiver.receive_window.consumed_position);
}

test "rebuild does not mask forced status threshold" {
    var receiver = try StreamReceiver.init(std.testing.allocator, 11, 1, 7, 1, .{
        .term_length = 1024,
        .window_length = 256,
        .max_message_length = 128,
    });
    defer receiver.deinit();

    const payload = "abcdefghijklmnop";
    try std.testing.expectEqual(InsertResult.inserted, try receiver.insertData(testHeader(.{
        .message_length = payload.len,
        .payload_length = payload.len,
    }), payload, 1));
    const delivery = (try receiver.rebuildNext()).?;
    try std.testing.expectEqualStrings(payload, delivery.payload);

    try std.testing.expect(receiver.statusDue(1));
}

test "out-of-order DATA schedules NAK and duplicate is counted" {
    var receiver = try StreamReceiver.init(std.testing.allocator, 11, 1, 7, 1, .{
        .term_length = 1024,
        .window_length = 1024,
        .max_message_length = 1024,
        .initial_nak_delay_ns = 50,
    });
    defer receiver.deinit();

    const header = testHeader(.{
        .term_offset = 96,
        .message_length = 5,
        .payload_length = 5,
    });
    try std.testing.expectEqual(InsertResult.inserted, try receiver.insertData(header, "world", 100));
    try std.testing.expectEqual(InsertResult.duplicate, try receiver.insertData(header, "world", 110));
    try std.testing.expectEqual(@as(u64, 1), receiver.duplicates);
    try std.testing.expect(!receiver.nakDue(149));
    try std.testing.expect(receiver.nakDue(150));
    const nak = receiver.makeNak(150);
    try std.testing.expectEqual(@as(u32, 0), nak.term_offset);
}

test "fragmented message reassembles after contiguous fragments" {
    var receiver = try StreamReceiver.init(std.testing.allocator, 11, 1, 7, 1, .{
        .term_length = 1024,
        .window_length = 1024,
        .max_message_length = 1024,
    });
    defer receiver.deinit();

    try std.testing.expectEqual(InsertResult.inserted, try receiver.insertData(testHeader(.{
        .message_id = 9,
        .message_length = 10,
        .payload_length = 5,
        .route_flags = route_flag_begin,
    }), "hello", 100));
    try std.testing.expectEqual(InsertResult.inserted, try receiver.insertData(testHeader(.{
        .term_offset = 96,
        .message_id = 9,
        .fragment_offset = 5,
        .message_length = 10,
        .payload_length = 5,
        .route_flags = route_flag_end,
    }), "world", 100));

    const delivery = (try receiver.rebuildNext()).?;
    try std.testing.expectEqualStrings("helloworld", delivery.payload);
    try std.testing.expect((try receiver.rebuildNext()) == null);
}

test "wrong session DATA is rejected and counted" {
    var receiver = try StreamReceiver.init(std.testing.allocator, 11, 1, 7, 1, .{
        .term_length = 1024,
        .window_length = 1024,
        .max_message_length = 1024,
    });
    defer receiver.deinit();

    try std.testing.expectEqual(InsertResult.wrong_session, try receiver.insertData(testHeader(.{
        .session_id = 6,
        .message_length = 5,
        .payload_length = 5,
    }), "hello", 100));
    try std.testing.expectEqual(@as(u64, 1), receiver.stale_session_packets);
}
