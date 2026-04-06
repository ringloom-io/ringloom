//! Monitoring subsystem for the BRZ common library.
//!
//! This is the single import point for all monitoring-related functionality.
//! It re-exports counters, cycle time tracking, snapshots, and periodic dump types.

pub const system_counter = @import("monitoring/system_counter.zig");
pub const SystemCounter = system_counter.SystemCounter;

pub const system_counters = @import("monitoring/system_counters.zig");
pub const SystemCounters = system_counters.SystemCounters;

pub const cycle_time = @import("monitoring/cycle_time.zig");
pub const CycleTimeTracker = cycle_time.CycleTimeTracker;

pub const counter_snapshot = @import("monitoring/counter_snapshot.zig");
pub const CounterSnapshot = counter_snapshot.CounterSnapshot;

pub const monitoring_snapshot = @import("monitoring/monitoring.zig");
pub const MonitoringSnapshot = monitoring_snapshot.MonitoringSnapshot;

pub const periodic_dump = @import("monitoring/periodic_dump.zig");
pub const PeriodicMonitoringDump = periodic_dump.PeriodicMonitoringDump;

// Ensure all monitoring module tests are discovered by `zig build test`.
comptime {
    _ = @import("monitoring/system_counter.zig");
    _ = @import("monitoring/system_counters.zig");
    _ = @import("monitoring/cycle_time.zig");
    _ = @import("monitoring/counter_snapshot.zig");
    _ = @import("monitoring/monitoring.zig");
    _ = @import("monitoring/periodic_dump.zig");
}
