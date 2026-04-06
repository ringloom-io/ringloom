//! Per-event-loop cycle time tracking.

const clock = @import("../platform/clock.zig");
const SystemCounters = @import("system_counters.zig").SystemCounters;
const SystemCounter = @import("system_counter.zig").SystemCounter;

pub const CycleTimeTracker = struct {
    counter: SystemCounter,
    counters: *const SystemCounters,
    reset_interval_ns: i64,
    last_reset_ns: i64,

    pub fn init(
        counter: SystemCounter,
        counters: *const SystemCounters,
        reset_interval_ns: i64,
    ) CycleTimeTracker {
        return .{
            .counter = counter,
            .counters = counters,
            .reset_interval_ns = reset_interval_ns,
            .last_reset_ns = clock.Clock.monotonicNanos(),
        };
    }

    /// Call at the start of each duty cycle. Returns the start timestamp.
    pub inline fn start(_: *const CycleTimeTracker) i64 {
        return clock.Clock.monotonicNanos();
    }

    /// Call at the end of each duty cycle with the start timestamp.
    /// Updates the max cycle time counter.
    pub inline fn stop(self: *CycleTimeTracker, start_ns: i64) void {
        const now = clock.Clock.monotonicNanos();
        const elapsed = now - start_ns;

        self.counters.updateMax(self.counter, elapsed);

        // Periodically reset the max so stale spikes don't persist forever.
        if (now - self.last_reset_ns > self.reset_interval_ns) {
            self.counters.set(self.counter, elapsed);
            self.last_reset_ns = now;
        }
    }
};
