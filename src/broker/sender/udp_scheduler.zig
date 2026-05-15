// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const ringloom_common = @import("ringloom_common");
const udp = @import("ringloom_udp");

const SendBufferEntry = ringloom_common.memory.SendBufferEntry;
const SendBufferPressureState = ringloom_common.memory.SendBufferPressureState;
const SendBufferEntryState = ringloom_common.memory.SendBufferEntryState;
const TermLog = udp.TermLog;
const LossRange = udp.LossRange;
const RetransmitAction = udp.RetransmitAction;
const StaticCongestionControl = udp.StaticCongestionControl;

pub const SetupState = enum {
    pending,
    confirmed,
    exhausted,
};

pub const StreamSenderOptions = struct {
    term_length: u32 = 64 * 1024,
    mtu: u32 = udp.protocol.default_mtu,
    initial_sender_limit: u64 = 0,
    congestion_window: u32 = 16 * 1024,
    max_retransmits: usize = 8,
    heartbeat_interval_ns: i64 = 500 * std.time.ns_per_ms,
    setup_interval_ns: i64 = 500 * std.time.ns_per_ms,
    setup_retry_limit: u8 = 5,
    retransmit_linger_ns: i64 = 10 * std.time.ns_per_ms,
};

pub const StreamSender = struct {
    stream_id: u32,
    target_node_id: u8,
    target_service_id: u16,
    session_id: u32,
    initial_term_id: i32,
    term_log: TermLog,
    acknowledged_position: u64 = 0,
    congestion: StaticCongestionControl,
    retransmit_actions: []RetransmitAction,
    setup_state: SetupState = .pending,
    setup_attempts: u8 = 0,
    next_setup_ns: i64 = 0,
    next_heartbeat_ns: i64 = 0,
    last_frame_sent_ns: i64 = 0,
    options: StreamSenderOptions,

    pub fn init(
        allocator: std.mem.Allocator,
        stream_id: u32,
        target_node_id: u8,
        target_service_id: u16,
        session_id: u32,
        initial_term_id: i32,
        options: StreamSenderOptions,
    ) !StreamSender {
        const max_retransmits = @max(options.max_retransmits, 1);
        const actions = try allocator.alloc(RetransmitAction, max_retransmits);
        @memset(actions, .{ .range = .{ .term_id = 0, .term_offset = 0, .length = 0 } });
        errdefer allocator.free(actions);

        return .{
            .stream_id = stream_id,
            .target_node_id = target_node_id,
            .target_service_id = target_service_id,
            .session_id = session_id,
            .initial_term_id = initial_term_id,
            .term_log = try TermLog.init(
                allocator,
                initial_term_id,
                options.term_length,
                options.mtu,
                options.initial_sender_limit,
            ),
            .congestion = StaticCongestionControl.init(.{
                .initial_window = options.congestion_window,
                .max_window = options.congestion_window,
                .term_length = options.term_length,
            }),
            .retransmit_actions = actions,
            .options = options,
        };
    }

    pub fn deinit(self: *StreamSender, allocator: std.mem.Allocator) void {
        self.term_log.deinit();
        allocator.free(self.retransmit_actions);
        self.* = undefined;
    }

    pub fn applyStatus(self: *StreamSender, status: udp.StatusHeader) void {
        self.term_log.sender_limit = udp.flow_control.senderLimitFromStatus(
            self.term_log.sender_limit,
            self.initial_term_id,
            self.options.term_length,
            status,
        );
        self.acknowledged_position = @max(
            self.acknowledged_position,
            udp.Position.absolute(
                self.initial_term_id,
                status.consumption_term_id,
                status.consumption_term_offset,
                self.options.term_length,
            ),
        );
        if (status.stream_id == self.stream_id) {
            self.setup_state = .confirmed;
        }
    }

    pub fn pressureState(self: *const StreamSender, entry_state: SendBufferEntryState) SendBufferPressureState {
        if (entry_state == .closed) return .closed;
        if (entry_state == .draining) return .draining;
        if (self.setup_state != .confirmed) return .peer_down;
        if (self.term_log.sender_position >= self.term_log.sender_limit) return .flow_blocked;
        if (!self.congestion.canSend(
            self.acknowledged_position,
            self.term_log.sender_position,
            udp.protocol.DataHeader.encoded_length,
        )) return .congested;
        if (!self.term_log.canRotate(self.acknowledged_position)) return .term_blocked;
        return .normal;
    }

    pub fn setupDue(self: *StreamSender, now_ns: i64) bool {
        if (self.setup_state != .pending) return false;
        if (self.setup_attempts >= self.options.setup_retry_limit) {
            self.setup_state = .exhausted;
            return false;
        }
        if (now_ns < self.next_setup_ns) return false;
        self.setup_attempts += 1;
        self.next_setup_ns = now_ns + self.options.setup_interval_ns;
        return true;
    }

    pub fn heartbeatDue(self: *StreamSender, now_ns: i64) bool {
        if (self.setup_state != .confirmed) return false;
        if (now_ns < self.next_heartbeat_ns) return false;
        if (self.last_frame_sent_ns != 0 and now_ns - self.last_frame_sent_ns < self.options.heartbeat_interval_ns) {
            return false;
        }
        self.next_heartbeat_ns = now_ns + self.options.heartbeat_interval_ns;
        return true;
    }

    pub fn scheduleNak(self: *StreamSender, nak: udp.NakHeader, now_ns: i64) bool {
        if (nak.stream_id != self.stream_id) return false;
        const range: LossRange = .{
            .term_id = nak.term_id,
            .term_offset = nak.term_offset,
            .length = nak.length,
        };
        for (self.retransmit_actions) |action| {
            if (!action.active) continue;
            if (rangesOverlap(action.range, range)) return false;
        }
        for (self.retransmit_actions) |*action| {
            if (action.active) continue;
            action.* = .{
                .range = range,
                .active = true,
                .deadline_ns = now_ns,
            };
            return true;
        }
        return false;
    }

    pub fn retransmitDue(
        self: *StreamSender,
        now_ns: i64,
        max_length: u32,
        out: []udp.term_log.ScannedFrame,
    ) u32 {
        for (self.retransmit_actions) |*action| {
            if (!action.active or now_ns < action.deadline_ns) continue;
            const count = self.term_log.retransmitScan(action.range, max_length, out);
            action.deadline_ns = now_ns + self.options.retransmit_linger_ns;
            action.active = false;
            return count;
        }
        return 0;
    }
};

pub const SchedulerEntry = struct {
    entry: *SendBufferEntry,
    deficit: u32 = 0,
};

pub const DestinationScheduler = struct {
    entries: []SchedulerEntry,
    next_index: usize = 0,
    quantum: u32 = 1024,
    max_deficit: u32 = 64 * 1024,

    pub fn nextDrainable(self: *DestinationScheduler, message_cost: u32) ?*SendBufferEntry {
        if (self.entries.len == 0) return null;
        var visited: usize = 0;
        while (visited < self.entries.len) : (visited += 1) {
            const index = self.next_index;
            self.next_index = (self.next_index + 1) % self.entries.len;
            const sched_entry = &self.entries[index];
            if (!isDrainable(sched_entry.entry)) continue;
            sched_entry.deficit = @min(sched_entry.deficit + self.quantum, self.max_deficit);
            if (sched_entry.deficit < message_cost) continue;
            sched_entry.deficit -= message_cost;
            return sched_entry.entry;
        }
        return null;
    }

    pub fn isDrainable(entry: *const SendBufferEntry) bool {
        const state = entry.loadState();
        if (state != .active and state != .draining) return false;
        return switch (entry.loadPressureState()) {
            .unknown, .normal => true,
            .flow_blocked,
            .congested,
            .term_blocked,
            .peer_down,
            .draining,
            .closed,
            => false,
        };
    }
};

fn rangesOverlap(a: LossRange, b: LossRange) bool {
    if (a.term_id != b.term_id) return false;
    const a_end = a.term_offset +| a.length;
    const b_end = b.term_offset +| b.length;
    return a.term_offset < b_end and b.term_offset < a_end;
}

test "scheduler visits active destinations fairly and skips blocked" {
    var entries = [_]SendBufferEntry{ .{}, .{}, .{} };
    for (&entries) |*entry| {
        entry.storeState(.active);
        entry.storePressureState(.normal);
    }
    entries[1].storePressureState(.flow_blocked);
    var sched_entries = [_]SchedulerEntry{
        .{ .entry = &entries[0] },
        .{ .entry = &entries[1] },
        .{ .entry = &entries[2] },
    };
    var scheduler = DestinationScheduler{ .entries = &sched_entries, .quantum = 128 };

    try std.testing.expect(scheduler.nextDrainable(64) == &entries[0]);
    try std.testing.expect(scheduler.nextDrainable(64) == &entries[2]);
    try std.testing.expect(scheduler.nextDrainable(64) == &entries[0]);
}

test "sender status increases limit and confirms setup" {
    var sender = try StreamSender.init(std.testing.allocator, 11, 2, 20, 7, 1, .{
        .term_length = 1024,
        .mtu = 256,
        .initial_sender_limit = 0,
    });
    defer sender.deinit(std.testing.allocator);

    sender.applyStatus(.{
        .common = udp.CommonHeader.init(.status, @sizeOf(udp.StatusHeader), @sizeOf(udp.StatusHeader), 7, 0),
        .stream_id = 11,
        .consumption_term_id = 1,
        .consumption_term_offset = 128,
        .receiver_window = 512,
        .receiver_id = 1,
        .highest_contiguous_message_id = 0,
    });

    try std.testing.expectEqual(SetupState.confirmed, sender.setup_state);
    try std.testing.expectEqual(@as(u64, 640), sender.term_log.sender_limit);
}

test "NAK duplicate suppression and retransmit scan use term log" {
    var sender = try StreamSender.init(std.testing.allocator, 11, 2, 20, 7, 1, .{
        .term_length = 1024,
        .mtu = 256,
        .initial_sender_limit = 1024,
    });
    defer sender.deinit(std.testing.allocator);

    const header = udp.DataHeader.init(.{
        .session_id = 7,
        .stream_id = 11,
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
    _ = try sender.term_log.appendData(header, "hello");
    const nak = udp.NakHeader{
        .common = udp.CommonHeader.init(.nak, @sizeOf(udp.NakHeader), @sizeOf(udp.NakHeader), 7, 0),
        .stream_id = 11,
        .term_id = 1,
        .term_offset = 0,
        .length = 128,
    };

    try std.testing.expect(sender.scheduleNak(nak, 100));
    try std.testing.expect(!sender.scheduleNak(nak, 100));

    var out: [4]udp.term_log.ScannedFrame = undefined;
    const count = sender.retransmitDue(100, 256, &out);
    try std.testing.expectEqual(@as(u32, 1), count);
    try std.testing.expectEqualStrings("hello", out[0].payload);
}

test "heartbeat and setup due timers gate sends" {
    var sender = try StreamSender.init(std.testing.allocator, 11, 2, 20, 7, 1, .{
        .term_length = 1024,
        .mtu = 256,
        .initial_sender_limit = 1024,
        .heartbeat_interval_ns = 100,
        .setup_interval_ns = 100,
    });
    defer sender.deinit(std.testing.allocator);

    try std.testing.expect(sender.setupDue(10));
    try std.testing.expect(!sender.setupDue(50));
    try std.testing.expect(sender.setupDue(110));

    sender.setup_state = .confirmed;
    try std.testing.expect(sender.heartbeatDue(200));
    try std.testing.expect(!sender.heartbeatDue(250));
    try std.testing.expect(sender.heartbeatDue(301));
}
