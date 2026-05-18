//! Typed wrapper around CountersManager for service-runtime counters.

const std = @import("std");
const CountersManager = @import("../concurrent/counters.zig").CountersManager;

pub const ServiceCounter = enum(u8) {
    messages_sent = 0,
    bytes_sent = 1,
    messages_received = 2,
    bytes_received = 3,
    send_buffer_full = 4,
    backpressure = 5,
    backpressure_timeouts = 6,
    no_available_instance = 7,
    control_messages_received = 8,
    heartbeats_sent = 9,
    registrations_sent = 10,
    unregisters_sent = 11,
    subscriptions_sent = 12,
    remote_transport_unavailable = 13,
    aeron_remote_claimed = 14,
    aeron_remote_not_connected = 15,
    aeron_remote_back_pressured = 16,
    aeron_remote_admin_action = 17,
    aeron_remote_closed = 18,
    aeron_remote_max_position_exceeded = 19,
    aeron_remote_failed = 20,

    pub const count: usize = 21;

    pub fn label(self: ServiceCounter) []const u8 {
        return switch (self) {
            .messages_sent => "service_messages_sent_total",
            .bytes_sent => "service_bytes_sent_total",
            .messages_received => "service_messages_received_total",
            .bytes_received => "service_bytes_received_total",
            .send_buffer_full => "service_send_buffer_full_total",
            .backpressure => "service_backpressure_total",
            .backpressure_timeouts => "service_backpressure_timeouts_total",
            .no_available_instance => "service_no_available_instance_total",
            .control_messages_received => "service_control_messages_received_total",
            .heartbeats_sent => "service_heartbeats_sent_total",
            .registrations_sent => "service_registrations_sent_total",
            .unregisters_sent => "service_unregisters_sent_total",
            .subscriptions_sent => "service_subscriptions_sent_total",
            .remote_transport_unavailable => "service_remote_transport_unavailable_total",
            .aeron_remote_claimed => "service_aeron_remote_claimed_total",
            .aeron_remote_not_connected => "service_aeron_remote_not_connected_total",
            .aeron_remote_back_pressured => "service_aeron_remote_back_pressured_total",
            .aeron_remote_admin_action => "service_aeron_remote_admin_action_total",
            .aeron_remote_closed => "service_aeron_remote_closed_total",
            .aeron_remote_max_position_exceeded => "service_aeron_remote_max_position_exceeded_total",
            .aeron_remote_failed => "service_aeron_remote_failed_total",
        };
    }
};

pub const ServiceCounters = struct {
    counters: *CountersManager,
    ids: [ServiceCounter.count]usize,

    pub fn init(counters_mgr: *CountersManager) !ServiceCounters {
        var self = ServiceCounters{
            .counters = counters_mgr,
            .ids = undefined,
        };

        inline for (0..ServiceCounter.count) |i| {
            const sc: ServiceCounter = @enumFromInt(i);
            self.ids[i] = counters_mgr.allocate(@intCast(i), sc.label()) orelse
                return error.CounterAllocationFailed;
        }

        return self;
    }

    pub inline fn increment(self: *const ServiceCounters, counter: ServiceCounter) void {
        self.counters.increment(self.ids[@intFromEnum(counter)]);
    }

    pub inline fn add(self: *const ServiceCounters, counter: ServiceCounter, delta: i64) void {
        self.counters.add(self.ids[@intFromEnum(counter)], delta);
    }

    pub inline fn get(self: *const ServiceCounters, counter: ServiceCounter) i64 {
        return self.counters.get(self.ids[@intFromEnum(counter)]);
    }
};

test "ServiceCounters init allocates all service counters" {
    var values_buf: [128 * 32]u8 align(128) = [_]u8{0} ** (128 * 32);
    var meta_buf: [256 * 32]u8 align(4) = [_]u8{0} ** (256 * 32);
    var manager = CountersManager.init(&values_buf, &meta_buf);

    const service_counters = try ServiceCounters.init(&manager);
    service_counters.increment(.messages_sent);
    service_counters.add(.bytes_sent, 42);

    try std.testing.expectEqual(@as(i64, 1), service_counters.get(.messages_sent));
    try std.testing.expectEqual(@as(i64, 42), service_counters.get(.bytes_sent));
}
