// SPDX-License-Identifier: Apache-2.0
const std = @import("std");

pub const StaticCongestionControl = struct {
    min_window: u32,
    max_window: u32,
    current_window: u32,
    rtt_ewma_ns: i64 = 0,
    loss_observed: bool = false,

    pub fn init(fields: struct {
        initial_window: u32,
        max_window: u32,
        min_window: u32 = 1,
        term_length: u32,
    }) StaticCongestionControl {
        const capped_initial = @min(fields.initial_window, fields.term_length / 2);
        const current = @max(fields.min_window, @min(capped_initial, fields.max_window));
        return .{
            .min_window = fields.min_window,
            .max_window = fields.max_window,
            .current_window = current,
        };
    }

    pub fn congestionLimit(self: StaticCongestionControl, acknowledged_position: u64) u64 {
        return acknowledged_position +| self.current_window;
    }

    pub fn canSend(
        self: StaticCongestionControl,
        acknowledged_position: u64,
        sender_position: u64,
        frame_length: usize,
    ) bool {
        return sender_position + frame_length <= self.congestionLimit(acknowledged_position);
    }

    pub fn onRttmSample(self: *StaticCongestionControl, sample_ns: i64) void {
        if (sample_ns <= 0) return;
        if (self.rtt_ewma_ns == 0) {
            self.rtt_ewma_ns = sample_ns;
            return;
        }
        self.rtt_ewma_ns = self.rtt_ewma_ns + @divTrunc(sample_ns - self.rtt_ewma_ns, 8);
    }

    pub fn onLoss(self: *StaticCongestionControl) void {
        self.loss_observed = true;
        self.current_window = @max(self.current_window, self.min_window);
    }
};

test "static congestion window is capped by half term length" {
    const cc = StaticCongestionControl.init(.{
        .initial_window = 4096,
        .max_window = 8192,
        .term_length = 4096,
    });
    try std.testing.expectEqual(@as(u32, 2048), cc.current_window);
    try std.testing.expectEqual(@as(u64, 3048), cc.congestionLimit(1000));
}

test "static congestion canSend checks bytes in flight" {
    const cc = StaticCongestionControl.init(.{
        .initial_window = 1024,
        .max_window = 1024,
        .term_length = 4096,
    });
    try std.testing.expect(cc.canSend(0, 900, 124));
    try std.testing.expect(!cc.canSend(0, 901, 124));
}

test "RTT EWMA updates without shrinking static window on loss" {
    var cc = StaticCongestionControl.init(.{
        .initial_window = 1024,
        .max_window = 1024,
        .min_window = 128,
        .term_length = 4096,
    });
    cc.onRttmSample(800);
    cc.onRttmSample(1600);
    try std.testing.expectEqual(@as(i64, 900), cc.rtt_ewma_ns);
    cc.onLoss();
    try std.testing.expect(cc.loss_observed);
    try std.testing.expectEqual(@as(u32, 1024), cc.current_window);
}
