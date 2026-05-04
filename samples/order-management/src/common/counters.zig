// SPDX-License-Identifier: Apache-2.0

const std = @import("std");

pub const Counters = struct {
    orders_generated: u64 = 0,
    orders_validated: u64 = 0,
    orders_rejected: u64 = 0,
    risk_accepted: u64 = 0,
    risk_rejected: u64 = 0,
    fills_emitted: u64 = 0,
    execution_reports_emitted: u64 = 0,
    portfolio_updates_applied: u64 = 0,
    send_buffer_full: u64 = 0,
    back_pressure: u64 = 0,
    no_available_instance: u64 = 0,
    bad_payload: u64 = 0,
    unknown_template: u64 = 0,
    max_handler_ns: u64 = 0,
    messages_received: u64 = 0,
    messages_sent: u64 = 0,
    send_failures: u64 = 0,

    pub fn observeHandlerNs(self: *Counters, elapsed_ns: u64) void {
        if (elapsed_ns > self.max_handler_ns) {
            self.max_handler_ns = elapsed_ns;
        }
    }

    pub fn recordSendError(self: *Counters, err: anyerror) void {
        self.send_failures += 1;
        switch (err) {
            error.SendBufferFull => self.send_buffer_full += 1,
            error.NoAvailableInstance, error.NoLeaderAvailable => self.no_available_instance += 1,
            error.BackPressure,
            error.BackPressureTimeout,
            error.PeerCongested,
            error.PeerDisconnected,
            => self.back_pressure += 1,
            else => {},
        }
    }
};

pub fn writeJson(
    io: std.Io,
    path: []const u8,
    service_name: []const u8,
    counters: Counters,
    extra_name: []const u8,
    extra_value: u64,
) !void {
    const file = if (std.fs.path.isAbsolute(path))
        try std.Io.Dir.createFileAbsolute(io, path, .{ .truncate = true })
    else
        try std.Io.Dir.cwd().createFile(io, path, .{ .truncate = true });
    defer file.close(io);

    var buf: [4096]u8 = undefined;
    var writer_state = file.writer(io, &buf);
    const writer = &writer_state.interface;
    try writer.print(
        \\{{
        \\  "service": "{s}",
        \\  "orders_generated": {d},
        \\  "orders_validated": {d},
        \\  "orders_rejected": {d},
        \\  "risk_accepted": {d},
        \\  "risk_rejected": {d},
        \\  "fills_emitted": {d},
        \\  "execution_reports_emitted": {d},
        \\  "portfolio_updates_applied": {d},
        \\  "send_buffer_full": {d},
        \\  "back_pressure": {d},
        \\  "no_available_instance": {d},
        \\  "bad_payload": {d},
        \\  "unknown_template": {d},
        \\  "max_handler_ns": {d},
        \\  "messages_received": {d},
        \\  "messages_sent": {d},
        \\  "send_failures": {d},
        \\  "{s}": {d}
        \\}}
        \\
    , .{
        service_name,
        counters.orders_generated,
        counters.orders_validated,
        counters.orders_rejected,
        counters.risk_accepted,
        counters.risk_rejected,
        counters.fills_emitted,
        counters.execution_reports_emitted,
        counters.portfolio_updates_applied,
        counters.send_buffer_full,
        counters.back_pressure,
        counters.no_available_instance,
        counters.bad_payload,
        counters.unknown_template,
        counters.max_handler_ns,
        counters.messages_received,
        counters.messages_sent,
        counters.send_failures,
        extra_name,
        extra_value,
    });
    try writer.flush();
}

test "recordSendError classifies RingLoom send errors" {
    var c: Counters = .{};
    c.recordSendError(error.SendBufferFull);
    c.recordSendError(error.NoAvailableInstance);
    c.recordSendError(error.BackPressure);
    try std.testing.expectEqual(@as(u64, 3), c.send_failures);
    try std.testing.expectEqual(@as(u64, 1), c.send_buffer_full);
    try std.testing.expectEqual(@as(u64, 1), c.no_available_instance);
    try std.testing.expectEqual(@as(u64, 1), c.back_pressure);
}
