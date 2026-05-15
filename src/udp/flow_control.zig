// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const protocol = @import("protocol.zig");
const Position = @import("position.zig").Position;

pub const ReceiverWindow = struct {
    max_length: u32,
    low_watermark: u32 = 0,

    pub fn init(max_length: u32) !ReceiverWindow {
        if (max_length == 0) return error.InvalidReceiverWindow;
        return .{ .max_length = max_length };
    }

    pub fn advertisedLength(
        self: ReceiverWindow,
        service_remaining: usize,
        retention_remaining: usize,
        pending_capacity_remaining: usize,
    ) u32 {
        const bounded = @min(
            @min(service_remaining, retention_remaining),
            @min(pending_capacity_remaining, self.max_length),
        );
        if (bounded <= self.low_watermark) return 0;
        return @intCast(@min(bounded, std.math.maxInt(u32)));
    }

    pub fn receiverEdge(consumed_position: u64, receiver_window: u32) u64 {
        return consumed_position +| receiver_window;
    }
};

pub fn senderLimitFromStatus(
    current_limit: u64,
    initial_term_id: i32,
    term_length: u32,
    status: protocol.StatusHeader,
) u64 {
    const receiver_position = Position.absolute(
        initial_term_id,
        status.consumption_term_id,
        status.consumption_term_offset,
        term_length,
    );
    return @max(current_limit, ReceiverWindow.receiverEdge(receiver_position, status.receiver_window));
}

pub fn shouldForceStatus(
    previous_consumed_position: u64,
    consumed_position: u64,
    receiver_window: u32,
) bool {
    if (consumed_position <= previous_consumed_position) return false;
    const threshold = @max(@as(u64, receiver_window / 4), 1);
    return consumed_position - previous_consumed_position >= threshold;
}

test "receiver window is capped by service and retention capacity" {
    const window = try ReceiverWindow.init(1024);
    try std.testing.expectEqual(@as(u32, 256), window.advertisedLength(512, 256, 1024));
    try std.testing.expectEqual(@as(u32, 0), window.advertisedLength(0, 256, 1024));
}

test "STATUS moves sender limit forward only" {
    const status = protocol.StatusHeader{
        .common = protocol.CommonHeader.init(.status, @sizeOf(protocol.StatusHeader), @sizeOf(protocol.StatusHeader), 7, 0),
        .stream_id = 11,
        .consumption_term_id = 1,
        .consumption_term_offset = 128,
        .receiver_window = 256,
        .receiver_id = 1,
        .highest_contiguous_message_id = 0,
    };

    try std.testing.expectEqual(@as(u64, 384), senderLimitFromStatus(0, 1, 1024, status));
    try std.testing.expectEqual(@as(u64, 4096), senderLimitFromStatus(4096, 1, 1024, status));
}

test "status scheduling follows quarter window movement" {
    try std.testing.expect(!shouldForceStatus(0, 63, 256));
    try std.testing.expect(shouldForceStatus(0, 64, 256));
}
