//! Cluster Manager — read-only facade for cluster state queries.
//!
//! Other subsystems (message routing, control plane) use this to query
//! cluster state without accessing internal cluster data structures directly.
//! It holds references to NodeMembership but exposes no mutation methods.
//!
//! This matches the Java ClusterManager class.

const std = @import("std");
const NodeMembership = @import("node_membership.zig").NodeMembership;
const Node = @import("node_membership.zig").Node;

const log = std.log.scoped(.cluster_manager);

pub const ClusterManager = struct {
    cluster_name: []const u8,
    node_membership: *NodeMembership,
    single_node_cluster: bool,

    /// Local node ID — cached for fast access.
    local_node_id: u8,

    /// Legacy compatibility: direct leader flag for single-node mode.
    /// In multi-node mode, leadership is determined by NodeMembership.
    is_leader: bool,

    const Self = @This();

    pub fn init(
        cluster_name: []const u8,
        node_membership: *NodeMembership,
        single_node_cluster: bool,
    ) Self {
        const local_node_id = node_membership.local_node_id;
        return .{
            .cluster_name = cluster_name,
            .node_membership = node_membership,
            .single_node_cluster = single_node_cluster,
            .local_node_id = local_node_id,
            .is_leader = single_node_cluster, // single-node is always leader
        };
    }

    /// Simplified init for single-node mode (backwards compatible with
    /// the control loop's existing init(local_node_id) call pattern).
    pub fn initSingleNode(local_node_id: u8) Self {
        return .{
            .cluster_name = "brz-default",
            .node_membership = undefined,
            .single_node_cluster = true,
            .local_node_id = local_node_id,
            .is_leader = true,
        };
    }

    pub fn hasNode(self: *const Self, node_id: u8) bool {
        if (self.single_node_cluster) return node_id == self.local_node_id;
        return self.node_membership.hasNode(node_id);
    }

    pub fn hasLeader(self: *const Self) bool {
        if (self.single_node_cluster) return true;
        return self.node_membership.getLeader() != null;
    }

    pub fn getLeader(self: *const Self) ?u8 {
        if (self.single_node_cluster) return self.local_node_id;
        return self.node_membership.getLeader();
    }

    /// Returns true if the local node is the current cluster leader.
    pub fn isLeader(self: *const Self) bool {
        if (self.single_node_cluster) return true;
        return self.node_membership.isLeader();
    }

    /// Returns true if this broker is the cluster leader.
    /// Alias for isLeader() — backwards compatible with control loop.
    pub fn isClusterLeader(self: *const Self) bool {
        return self.isLeader();
    }

    /// True if the cluster has enough members to make progress.
    /// Single-node clusters always have consensus.
    pub fn hasConsensus(self: *const Self) bool {
        if (self.single_node_cluster) return true;
        return self.node_membership.count > 1;
    }

    /// Periodic work — drives leader election, state sync, broker heartbeats.
    /// Called by the control loop on every timeout check interval.
    ///
    /// In single-node mode this is a no-op. In multi-node mode, the actual
    /// work is driven by ClusterEventHandler.doWork() which is called from
    /// the broker-agent event loop.
    pub fn doWork(self: *Self, now_ns: i64) void {
        _ = self;
        _ = now_ns;
        // In the current architecture, cluster work is driven by
        // ClusterEventHandler.doWork() on the broker-agent thread.
        // This method exists for backwards compatibility with the
        // control loop's periodic task invocation.
    }

    /// Broadcast that a service was added on this node.
    /// In multi-node mode, this is handled by ClusterEventHandler via
    /// ClusterState.broadcastServiceAdded(). This method exists for
    /// backwards compatibility with the control loop.
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
        // In multi-node mode, the control loop posts a command to the
        // broker-agent thread which calls ClusterState.broadcastServiceAdded().
        // In single-node mode, no broadcast is needed.
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
        // Same pattern as broadcastServiceAdded — delegated to
        // ClusterEventHandler in multi-node mode.
    }

    /// Called when a peer broker connects.
    pub fn onPeerConnected(self: *Self, node_id: u8) void {
        _ = self;
        log.info("peer connected: nodeId={}", .{node_id});
        // Connection handling is managed by ClusterEventHandler.onPeerConnected().
    }

    /// Called when a peer broker disconnects.
    pub fn onPeerDisconnected(self: *Self, node_id: u8) void {
        _ = self;
        log.info("peer disconnected: nodeId={}", .{node_id});
        // Disconnection handling is managed by ClusterEventHandler.handlePeerDisconnected().
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;
const nm_mod = @import("node_membership.zig");
const padHostPort = nm_mod.padHostPort;

test "ClusterManager initSingleNode defaults to leader" {
    // Given / When
    const mgr = ClusterManager.initSingleNode(1);

    // Then
    try testing.expect(mgr.isClusterLeader());
    try testing.expect(mgr.isLeader());
    try testing.expect(mgr.hasLeader());
    try testing.expect(mgr.hasConsensus());
    try testing.expectEqual(@as(u8, 1), mgr.local_node_id);
    try testing.expectEqual(@as(?u8, 1), mgr.getLeader());
}

test "ClusterManager initSingleNode hasNode only for local" {
    // Given
    const mgr = ClusterManager.initSingleNode(1);

    // When / Then
    try testing.expect(mgr.hasNode(1));
    try testing.expect(!mgr.hasNode(2));
}

test "ClusterManager init with NodeMembership" {
    // Given
    var membership = nm_mod.NodeMembership.init(1, padHostPort("localhost:40456"));
    membership.addNode(2, padHostPort("host2:40456"), null);

    // When
    const mgr = ClusterManager.init("test-cluster", &membership, false);

    // Then
    try testing.expect(!mgr.isLeader()); // no leader elected yet
    try testing.expect(!mgr.hasLeader());
    try testing.expect(mgr.hasNode(1));
    try testing.expect(mgr.hasNode(2));
    try testing.expect(!mgr.hasNode(3));
    try testing.expect(mgr.hasConsensus()); // count > 1
    try testing.expectEqualStrings("test-cluster", mgr.cluster_name);
}

test "ClusterManager reflects NodeMembership leader" {
    // Given
    var membership = nm_mod.NodeMembership.init(1, padHostPort("localhost:40456"));
    membership.addNode(2, padHostPort("host2:40456"), null);
    var mgr = ClusterManager.init("test-cluster", &membership, false);

    // When: elect node 1 as leader
    membership.electLeader(1);

    // Then
    try testing.expect(mgr.isLeader());
    try testing.expect(mgr.isClusterLeader());
    try testing.expect(mgr.hasLeader());
    try testing.expectEqual(@as(?u8, 1), mgr.getLeader());
}

test "ClusterManager single-node always has consensus" {
    // Given
    const mgr = ClusterManager.initSingleNode(1);

    // When / Then
    try testing.expect(mgr.hasConsensus());
}

test "ClusterManager multi-node needs more than one member for consensus" {
    // Given
    var membership = nm_mod.NodeMembership.init(1, padHostPort("localhost:40456"));
    const mgr = ClusterManager.init("test-cluster", &membership, false);

    // When / Then — only local node, count == 1
    try testing.expect(!mgr.hasConsensus());
}

test "ClusterManager doWork is safe to call" {
    // Given
    var mgr = ClusterManager.initSingleNode(1);

    // When / Then — should not crash
    mgr.doWork(1_000_000_000);
}

test "ClusterManager broadcast stubs are safe to call" {
    // Given
    var mgr = ClusterManager.initSingleNode(1);

    // When / Then — should not crash
    mgr.broadcastServiceAdded(1, "svc", false);
    mgr.broadcastServiceRemoved(1, "svc");
    mgr.onPeerConnected(2);
    mgr.onPeerDisconnected(2);
}
