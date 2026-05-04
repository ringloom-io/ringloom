//! End-to-end test suite for the RingLoom broker.
//!
//! These tests spawn real broker and service processes, wait for readiness,
//! and assert correctness of the integrated system.
//!
//! Run with: `zig build e2e`

const std = @import("std");

comptime {
    _ = @import("broker_startup_test.zig");
    _ = @import("registration_test.zig");
    _ = @import("local_ipc_test.zig");
    _ = @import("discovery_test.zig");
    _ = @import("cross_broker_test.zig");
    _ = @import("fragmentation_test.zig");
    _ = @import("heartbeat_timeout_test.zig");
    _ = @import("restart_test.zig");
    _ = @import("leader_election_test.zig");
    _ = @import("graceful_unregister_test.zig");
    _ = @import("backpressure_test.zig");
    _ = @import("observability_test.zig");
}
