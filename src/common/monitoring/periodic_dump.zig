//! Optional periodic monitoring dump to stderr for development/debugging.

const std = @import("std");
const clock = @import("../platform/clock.zig");
const MonitoringSnapshot = @import("monitoring.zig").MonitoringSnapshot;
const SystemCounters = @import("system_counters.zig").SystemCounters;
const ErrorLog = @import("../concurrent/error_log.zig").ErrorLog;

pub const PeriodicMonitoringDump = struct {
    enabled: bool,
    interval_ns: i64,
    next_dump_ns: i64,
    node_id: u8,
    counters: *const SystemCounters,
    error_log: *const ErrorLog,

    const default_interval_ns: i64 = 10 * std.time.ns_per_s;

    pub fn init(
        node_id: u8,
        counters: *const SystemCounters,
        error_log: *const ErrorLog,
    ) PeriodicMonitoringDump {
        const enabled = std.posix.getenv("BRZ_MONITORING_DUMP") != null;
        return .{
            .enabled = enabled,
            .interval_ns = default_interval_ns,
            .next_dump_ns = clock.Clock.monotonicNanos() + default_interval_ns,
            .node_id = node_id,
            .counters = counters,
            .error_log = error_log,
        };
    }

    /// Called once per control loop duty cycle. Returns 1 if a dump was written, 0 otherwise.
    pub fn doWork(self: *PeriodicMonitoringDump) u32 {
        if (!self.enabled) return 0;

        const now = clock.Clock.monotonicNanos();
        if (now < self.next_dump_ns) return 0;

        self.next_dump_ns = now + self.interval_ns;

        const snapshot = MonitoringSnapshot.take(self.node_id, self.counters, self.error_log);
        var dump_buf: [4096]u8 = undefined;
        var stderr_w = std.fs.File.stderr().writer(&dump_buf);
        snapshot.dump(&stderr_w.interface) catch {};
        stderr_w.interface.flush() catch {};

        return 1;
    }
};
