//! Point-in-time snapshot of all system counter values.

const SystemCounter = @import("system_counter.zig").SystemCounter;
const SystemCounters = @import("system_counters.zig").SystemCounters;

pub const CounterValue = struct {
    id: u8,
    label: []const u8,
    value: i64,
};

pub const CounterSnapshot = struct {
    values: [SystemCounter.count]CounterValue,
    timestamp_ms: i64,

    pub fn take(counters: *const SystemCounters, timestamp_ms: i64) CounterSnapshot {
        var snapshot: CounterSnapshot = .{
            .values = undefined,
            .timestamp_ms = timestamp_ms,
        };

        inline for (0..SystemCounter.count) |i| {
            const sc: SystemCounter = @enumFromInt(i);
            snapshot.values[i] = .{
                .id = @intCast(i),
                .label = sc.label(),
                .value = counters.get(sc),
            };
        }

        return snapshot;
    }
};
