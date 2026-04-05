//! Cluster management module for the BRZ broker.
//!
//! This is the single import point for cluster-related functionality.
//! The full implementation will arrive in task 11 — for now this
//! re-exports the stub ClusterManager needed by the control plane.

pub const cluster_manager = @import("cluster/cluster_manager.zig");
pub const ClusterManager = cluster_manager.ClusterManager;

// Ensure all cluster module tests are discovered by `zig build test`.
comptime {
    _ = @import("cluster/cluster_manager.zig");
}
