//! Cluster Manager — stub for the control plane.
//!
//! This module will be fully implemented in task 11 (Cluster Management).
//! For now, it provides the minimal interface that the control loop needs.

const std = @import("std");
const log = std.log.scoped(.cluster_manager);

pub const ClusterManager = struct {
    local_node_id: u8,
    is_leader: bool,

    const Self = @This();

    pub fn init(local_node_id: u8) Self {
        return .{
            .local_node_id = local_node_id,
            // Single-node mode: this broker is always the cluster leader.
            .is_leader = true,
        };
    }

    /// Returns true if this broker is the cluster leader.
    pub fn isClusterLeader(self: *const Self) bool {
        return self.is_leader;
    }

    /// Periodic work — drives leader election, state sync, broker heartbeats.
    /// Called by the control loop on every timeout check interval.
    pub fn doWork(self: *Self, now_ns: i64) void {
        _ = self;
        _ = now_ns;
        // Stub — will be implemented in task 11.
    }

    /// Broadcast that a service was added on this node.
    pub fn broadcastServiceAdded(
        self: *Self,
        service_id: i32,
        service_name: []const u8,
        leader_election_enabled: bool,
    ) void {
        _ = self;
        _ = service_id;
        _ = service_name;
        _ = leader_election_enabled;
        // Stub — will be implemented in task 11.
    }

    /// Broadcast that a service was removed from this node.
    pub fn broadcastServiceRemoved(
        self: *Self,
        service_id: i32,
        service_name: []const u8,
    ) void {
        _ = self;
        _ = service_id;
        _ = service_name;
        // Stub — will be implemented in task 11.
    }

    /// Called when a peer broker connects.
    pub fn onPeerConnected(self: *Self, node_id: u8) void {
        _ = self;
        log.info("peer connected: nodeId={}", .{node_id});
        // Stub — will be implemented in task 11.
    }

    /// Called when a peer broker disconnects.
    pub fn onPeerDisconnected(self: *Self, node_id: u8) void {
        _ = self;
        log.info("peer disconnected: nodeId={}", .{node_id});
        // Stub — will be implemented in task 11.
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "ClusterManager init defaults to leader" {
    // Given / When
    const mgr = ClusterManager.init(1);

    // Then
    try testing.expect(mgr.isClusterLeader());
    try testing.expectEqual(@as(u8, 1), mgr.local_node_id);
}
