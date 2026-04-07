//! Node Membership — tracks all known brokers in the cluster.
//!
//! Each node is identified by a `u8` nodeId (0–255). A flat array of 256
//! optional `Node` slots gives O(1) lookups with no hashing and is
//! allocation-free after init.
//!
//! Connection lifecycle fields (connection_state, last_setup_sent_ns,
//! setup_attempt_count) are merged directly into the Node struct,
//! eliminating the need for a separate PeerConnection tracking structure.

const std = @import("std");

pub const ConnectionState = enum(u8) {
    /// No connection attempt in progress.
    disconnected,
    /// SETUP frame sent, waiting for SM.
    setup_sent,
    /// SM received, traffic can flow.
    connected,
};

pub const Node = struct {
    id: u8,
    /// "host:port" string, null-padded to 22 bytes.
    host_and_port: [22]u8,
    /// Resolved address for this peer. Null for the local node.
    address: ?std.net.Address = null,
    /// True if this is the local broker.
    is_local: bool,
    /// True if this node is the current cluster leader.
    is_leader: bool = false,
    /// Monotonic timestamp of last heartbeat from this node.
    last_heartbeat_ns: i64 = 0,

    // ── Connection lifecycle (merged from PeerConnection) ────────────

    /// Current connection state. Written only on broker-agent thread.
    connection_state: ConnectionState = .disconnected,
    /// Monotonic timestamp of last SETUP attempt (for retry backoff).
    last_setup_sent_ns: i64 = 0,
    /// Number of consecutive SETUP attempts without a successful SM response.
    setup_attempt_count: u32 = 0,

    const SETUP_RETRY_INTERVAL_NS: i64 = 1 * std.time.ns_per_s;

    /// Returns true if a SETUP retry is due.
    pub fn shouldRetrySetup(self: *const Node, now_ns: i64) bool {
        if (self.is_local) return false;
        if (self.connection_state != .setup_sent and
            self.connection_state != .disconnected) return false;
        return (now_ns - self.last_setup_sent_ns) >= SETUP_RETRY_INTERVAL_NS;
    }

    pub fn markSetupSent(self: *Node, now_ns: i64) void {
        self.connection_state = .setup_sent;
        self.last_setup_sent_ns = now_ns;
        self.setup_attempt_count += 1;
    }

    pub fn markConnected(self: *Node, now_ns: i64) void {
        self.connection_state = .connected;
        self.last_heartbeat_ns = now_ns;
        self.setup_attempt_count = 0;
    }

    pub fn markDisconnected(self: *Node) void {
        self.connection_state = .disconnected;
        self.setup_attempt_count = 0;
    }
};

pub const NodeMembership = struct {
    /// Node storage indexed by nodeId. Since nodeId is u8, a flat array
    /// of 256 optional slots is more efficient than a hash map.
    nodes: [256]?Node = [_]?Node{null} ** 256,
    /// nodeId of the local broker (set at init, never changes).
    local_node_id: u8,
    /// Number of active nodes.
    count: u16 = 0,

    pub fn init(local_node_id: u8, local_host_and_port: [22]u8) NodeMembership {
        var self = NodeMembership{
            .local_node_id = local_node_id,
        };
        self.nodes[local_node_id] = .{
            .id = local_node_id,
            .host_and_port = local_host_and_port,
            .is_local = true,
            .connection_state = .connected, // local node is always "connected"
        };
        self.count = 1;
        return self;
    }

    pub fn addNode(self: *NodeMembership, node_id: u8, host_and_port: [22]u8, address: ?std.net.Address) void {
        if (self.nodes[node_id] != null) return; // already known
        self.nodes[node_id] = .{
            .id = node_id,
            .host_and_port = host_and_port,
            .address = address,
            .is_local = false,
        };
        self.count += 1;
    }

    pub fn removeNode(self: *NodeMembership, node_id: u8) void {
        if (self.nodes[node_id]) |_| {
            self.nodes[node_id] = null;
            self.count -= 1;
        }
    }

    pub fn hasNode(self: *const NodeMembership, node_id: u8) bool {
        return self.nodes[node_id] != null;
    }

    pub fn getNode(self: *NodeMembership, node_id: u8) ?*Node {
        if (self.nodes[node_id] != null) {
            return &(self.nodes[node_id].?);
        }
        return null;
    }

    pub fn getNodeConst(self: *const NodeMembership, node_id: u8) ?*const Node {
        if (self.nodes[node_id] != null) {
            return &(self.nodes[node_id].?);
        }
        return null;
    }

    pub fn getLocalNode(self: *const NodeMembership) *const Node {
        return &(self.nodes[self.local_node_id].?);
    }

    /// Set the leader flag on the given node. Clears the flag on all others.
    pub fn electLeader(self: *NodeMembership, leader_node_id: u8) void {
        for (&self.nodes) |*slot| {
            if (slot.*) |*node| {
                node.is_leader = (node.id == leader_node_id);
            }
        }
    }

    /// Returns the nodeId of the current leader, or null if no leader is set.
    pub fn getLeader(self: *const NodeMembership) ?u8 {
        for (self.nodes) |slot| {
            if (slot) |node| {
                if (node.is_leader) return node.id;
            }
        }
        return null;
    }

    /// True if the local node is the current cluster leader.
    pub fn isLeader(self: *const NodeMembership) bool {
        if (self.nodes[self.local_node_id]) |node| {
            return node.is_leader;
        }
        return false;
    }

    /// Returns the lowest nodeId among all known nodes (highest priority).
    pub fn findHighestPriorityNodeId(self: *const NodeMembership) u8 {
        for (self.nodes, 0..) |slot, i| {
            if (slot != null) return @intCast(i);
        }
        return self.local_node_id; // fallback
    }

    /// Iterate all active nodes. Calls `callback` for each.
    pub fn forEach(
        self: *const NodeMembership,
        context: anytype,
        callback: fn (@TypeOf(context), *const Node) void,
    ) void {
        for (self.nodes) |slot| {
            if (slot) |*node| {
                callback(context, node);
            }
        }
    }

    /// Returns the number of connected (non-local) peers.
    pub fn connectedPeerCount(self: *const NodeMembership) u16 {
        var connected: u16 = 0;
        for (self.nodes) |slot| {
            if (slot) |node| {
                if (!node.is_local and node.connection_state == .connected) {
                    connected += 1;
                }
            }
        }
        return connected;
    }
};

// ── Helper ────────────────────────────────────────────────────────────

/// Pad a host:port string into a fixed 22-byte array (null-padded).
pub fn padHostPort(host_port: []const u8) [22]u8 {
    var result: [22]u8 = [_]u8{0} ** 22;
    const len = @min(host_port.len, 22);
    @memcpy(result[0..len], host_port[0..len]);
    return result;
}

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "init creates local node" {
    // Given / When
    const nm = NodeMembership.init(1, padHostPort("localhost:40456"));

    // Then
    try testing.expect(nm.hasNode(1));
    try testing.expectEqual(@as(u16, 1), nm.count);
    try testing.expectEqual(@as(u8, 1), nm.getLocalNode().id);
    try testing.expect(nm.getLocalNode().is_local);
}

test "addNode and removeNode" {
    // Given
    var nm = NodeMembership.init(1, padHostPort("localhost:40456"));

    // When
    nm.addNode(2, padHostPort("host2:40456"), null);

    // Then
    try testing.expect(nm.hasNode(2));
    try testing.expectEqual(@as(u16, 2), nm.count);

    // When
    nm.removeNode(2);

    // Then
    try testing.expect(!nm.hasNode(2));
    try testing.expectEqual(@as(u16, 1), nm.count);
}

test "addNode ignores duplicates" {
    // Given
    var nm = NodeMembership.init(1, padHostPort("localhost:40456"));
    nm.addNode(2, padHostPort("host2:40456"), null);

    // When — add same node again
    nm.addNode(2, padHostPort("host2:40456"), null);

    // Then — count unchanged
    try testing.expectEqual(@as(u16, 2), nm.count);
}

test "removeNode of unknown node is no-op" {
    // Given
    var nm = NodeMembership.init(1, padHostPort("localhost:40456"));

    // When
    nm.removeNode(99);

    // Then
    try testing.expectEqual(@as(u16, 1), nm.count);
}

test "electLeader sets leader flag" {
    // Given
    var nm = NodeMembership.init(1, padHostPort("localhost:40456"));
    nm.addNode(2, padHostPort("host2:40456"), null);

    // When
    nm.electLeader(2);

    // Then
    try testing.expectEqual(@as(?u8, 2), nm.getLeader());
    try testing.expect(!nm.isLeader()); // local node 1 is not the leader
}

test "electLeader clears previous leader" {
    // Given
    var nm = NodeMembership.init(1, padHostPort("localhost:40456"));
    nm.addNode(2, padHostPort("host2:40456"), null);
    nm.electLeader(2);

    // When — elect a different leader
    nm.electLeader(1);

    // Then
    try testing.expectEqual(@as(?u8, 1), nm.getLeader());
    try testing.expect(nm.isLeader());
}

test "findHighestPriorityNodeId returns lowest" {
    // Given
    var nm = NodeMembership.init(3, padHostPort("localhost:40456"));
    nm.addNode(1, padHostPort("host1:40456"), null);
    nm.addNode(5, padHostPort("host5:40456"), null);

    // When / Then
    try testing.expectEqual(@as(u8, 1), nm.findHighestPriorityNodeId());
}

test "getNode returns mutable pointer" {
    // Given
    var nm = NodeMembership.init(1, padHostPort("localhost:40456"));
    nm.addNode(2, padHostPort("host2:40456"), null);

    // When
    const node = nm.getNode(2).?;

    // Then
    try testing.expectEqual(@as(u8, 2), node.id);
    try testing.expect(!node.is_local);
}

test "getNode returns null for unknown node" {
    // Given
    var nm = NodeMembership.init(1, padHostPort("localhost:40456"));

    // When / Then
    try testing.expect(nm.getNode(99) == null);
}

test "merged connection state on Node" {
    // Given
    var nm = NodeMembership.init(1, padHostPort("localhost:40456"));
    nm.addNode(2, padHostPort("host2:40456"), null);

    // Initially disconnected
    const node = nm.getNode(2).?;
    try testing.expectEqual(ConnectionState.disconnected, node.connection_state);

    // Mark setup sent
    node.markSetupSent(1_000_000_000);
    try testing.expectEqual(ConnectionState.setup_sent, node.connection_state);
    try testing.expectEqual(@as(u32, 1), node.setup_attempt_count);

    // Mark connected
    node.markConnected(2_000_000_000);
    try testing.expectEqual(ConnectionState.connected, node.connection_state);
    try testing.expectEqual(@as(u32, 0), node.setup_attempt_count);

    // Mark disconnected
    node.markDisconnected();
    try testing.expectEqual(ConnectionState.disconnected, node.connection_state);
    try testing.expectEqual(@as(u32, 0), node.setup_attempt_count);
}

test "shouldRetrySetup respects interval" {
    // Given
    var nm = NodeMembership.init(1, padHostPort("localhost:40456"));
    nm.addNode(2, padHostPort("host2:40456"), null);
    const node = nm.getNode(2).?;

    // Initially — should retry (disconnected, never sent)
    try testing.expect(node.shouldRetrySetup(1_000_000_000));

    // After marking setup sent
    node.markSetupSent(1_000_000_000);
    try testing.expect(!node.shouldRetrySetup(1_500_000_000)); // too soon
    try testing.expect(node.shouldRetrySetup(2_000_000_000)); // 1 second later
}

test "shouldRetrySetup returns false for local node" {
    // Given
    const nm = NodeMembership.init(1, padHostPort("localhost:40456"));

    // When / Then — local node never retries
    try testing.expect(!nm.getLocalNode().shouldRetrySetup(1_000_000_000));
}

test "shouldRetrySetup returns false when connected" {
    // Given
    var nm = NodeMembership.init(1, padHostPort("localhost:40456"));
    nm.addNode(2, padHostPort("host2:40456"), null);
    const node = nm.getNode(2).?;
    node.markConnected(1_000_000_000);

    // When / Then
    try testing.expect(!node.shouldRetrySetup(5_000_000_000));
}

test "connectedPeerCount" {
    // Given
    var nm = NodeMembership.init(1, padHostPort("localhost:40456"));
    nm.addNode(2, padHostPort("host2:40456"), null);
    nm.addNode(3, padHostPort("host3:40456"), null);

    // Initially — no connected peers
    try testing.expectEqual(@as(u16, 0), nm.connectedPeerCount());

    // After connecting one
    nm.getNode(2).?.markConnected(1_000_000_000);
    try testing.expectEqual(@as(u16, 1), nm.connectedPeerCount());

    // After connecting both
    nm.getNode(3).?.markConnected(2_000_000_000);
    try testing.expectEqual(@as(u16, 2), nm.connectedPeerCount());
}

test "getLeader returns null when no leader set" {
    // Given
    const nm = NodeMembership.init(1, padHostPort("localhost:40456"));

    // When / Then
    try testing.expect(nm.getLeader() == null);
}
