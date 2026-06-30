//! RingLoom Performance Benchmarks — module root.
//!
//! This file re-exports all performance benchmark modules so that
//! `zig build test` (or a dedicated perf step) discovers every
//! benchmark test defined in the suite.
//!
//! Each sub-module contains one or more `test` blocks that use the
//! `ringloom_testing` harness to spin up broker/service topologies,
//! drive traffic, and emit structured JSON results.

comptime {
    _ = @import("ring_buffer_bench.zig");
    _ = @import("local_latency_bench.zig");
    _ = @import("local_throughput_bench.zig");
    _ = @import("remote_latency_bench.zig");
    _ = @import("backpressure_bench.zig");
    _ = @import("recovery_bench.zig");
    _ = @import("topic_bench.zig");
}

test "perf module compiles" {
    // Intentionally empty — the comptime block above forces
    // semantic analysis of every benchmark file.
}
