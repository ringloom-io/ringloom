//! Cluster Event Handler — central coordinator for all cluster subsystems.
//!
//! This is the Zig equivalent of the Java ClusterEventHandler. It lives on
//! the broker-agent thread and orchestrates reactions to cluster events:
//! heartbeats, peer connections/disconnections, leader changes, etc.
//!
//! All cluster state mutations happen on the broker-agent thread. The
//! receiver event loop posts admin commands via the AdminCommandQueue.

const std = @import("std");
const LeaderElection = @import("leader_election.zig").LeaderElection;
const NodeMembership = @import("node_membership.zig").NodeMembership;
const Node = @import("node_membership.zig").Node;
const ConnectionState = @import("node_membership.zig").ConnectionState;
const ClusterState = @import("cluster_state.zig").ClusterState;
const RemoteServiceInstance = @import("cluster_state.zig").RemoteServiceInstance;
const ServiceLeaderElectionManager = @import("service_leader_election.zig").ServiceLeaderElectionManager;
const BrokerHeartbeatSender = @import("broker_heartbeat.zig").BrokerHeartbeatSender;
const admin_dispatch = @import("admin_dispatch.zig");
const AdminCommand = admin_dispatch.AdminCommand;
const admin = @import("admin_messages.zig");

const log = std.log.scoped(.cluster_event_handler);

pub const ClusterEventHandler = struct {
    // ── Subsystem references ─────────────────────────────────────────
    leader_election: *LeaderElection,
    node_membership: *NodeMembership,
    cluster_state: *ClusterState,
    service_leader_election: *ServiceLeaderElectionManager,
    heartbeat_sender: *BrokerHeartbeatSender,

    // ── Inter-loop communication ─────────────────────────────────────
    admin_cmd_queue: *admin_dispatch.AdminCommandQueue(64),

    // ── Config ───────────────────────────────────────────────────────
    local_node_id: u8,

    // ── Callbacks ────────────────────────────────────────────────────
    broadcast_admin_fn: *const fn (buf: []const u8) void,

    // ── Constants ────────────────────────────────────────────────────
    pub const MASTER_DOWN_INTERVAL_NS: i64 = 3 * std.time.ns_per_s;

    // ── Public API ───────────────────────────────────────────────────

    /// Handle an inbound BrokerHeartbeat. Unified handler for liveness,
    /// peer discovery, and leader election.
    pub fn onBrokerHeartbeat(
        self: *ClusterEventHandler,
        node_id: u8,
        host_and_port: [22]u8,
        now_ns: i64,
    ) void {
        // 1. Register unknown peers (discovery via heartbeat)
        if (!self.node_membership.hasNode(node_id)) {
            self.node_membership.addNode(node_id, host_and_port, null);
            log.info("discovered peer via heartbeat: nodeId={}", .{node_id});
        }

        // 2. Update liveness timestamp
        if (self.node_membership.getNode(node_id)) |node| {
            node.last_heartbeat_ns = now_ns;
        }

        // 3. Leader election via VRRP-style priority comparison
        const result = self.leader_election.onBrokerHeartbeat(node_id, now_ns);
        if (result.changed) {
            log.info("leader changed to nodeId={}", .{result.leader.?});
            self.handleLeaderChange(result.leader.?);
        }
    }

    /// Called when a new peer connection is established (SETUP handshake complete).
    pub fn onPeerConnected(self: *ClusterEventHandler, node_id: u8, now_ns: i64) void {
        if (self.node_membership.getNode(node_id)) |node| {
            node.markConnected(now_ns);
        }

        log.info("peer connected: nodeId={}", .{node_id});
    }

    /// Called on the broker-agent thread when a peer is determined to be dead.
    /// This is the single point of disconnection handling.
    pub fn handlePeerDisconnected(self: *ClusterEventHandler, node_id: u8, now_ns: i64) void {
        const node = self.node_membership.getNode(node_id) orelse return;
        if (node.connection_state == .disconnected) return;

        // 1. Mark disconnected
        node.markDisconnected();

        // 2. Remove the node from membership
        self.node_membership.removeNode(node_id);

        // 3. Remove service instances registered on that node from the
        //    service leader election manager
        _ = self.service_leader_election.removeByNodeId(node_id);

        // 4. Notify leader election of the departure
        const election_result = self.leader_election.onPeerDisconnected(node_id, now_ns);
        if (election_result.changed) {
            log.info("leader cleared after peer {} disconnected", .{node_id});
        }

        // 5. If we are the broker leader, re-evaluate service leaders
        if (self.leader_election.isLocalNodeLeader()) {
            self.reEvaluateAndBroadcast();
        }

        log.info("peer disconnected: nodeId={}", .{node_id});
    }

    /// Drain all pending admin commands from the receiver event loop.
    pub fn processAdminCommands(self: *ClusterEventHandler, now_ns: i64) u32 {
        var work_count: u32 = 0;
        while (self.admin_cmd_queue.dequeue()) |cmd| {
            switch (cmd) {
                .broker_heartbeat => |e| self.onBrokerHeartbeat(
                    e.node_id,
                    e.host_and_port,
                    e.received_ns,
                ),
                .cluster_state_snapshot => |_| {
                    // Snapshot handling will be integrated with service registry
                    // when the full control plane integration is complete.
                },
                .service_added => |_| {
                    // ServiceAdded handling will be integrated with service registry.
                },
                .service_removed => |_| {
                    // ServiceRemoved handling will be integrated with service registry.
                },
                .service_leader_designated => |_| {
                    // ServiceLeaderDesignated handling will be integrated.
                },
                .peer_connected => |_| {
                    // Handled by the control loop, not the event handler.
                },
            }
            work_count += 1;
        }

        // Also check master-down timer (VRRP-style election)
        const election_result = self.leader_election.checkMasterDown(now_ns);
        if (election_result.changed) {
            log.info("master-down timer fired, new leader: nodeId={}", .{election_result.leader.?});
            self.handleLeaderChange(election_result.leader.?);
            work_count += 1;
        }

        return work_count;
    }

    // ── Duty cycle ───────────────────────────────────────────────────

    /// Top-level duty-cycle function for cluster management.
    /// Called once per broker-agent iteration.
    pub fn doWork(self: *ClusterEventHandler, now_ns: i64) u32 {
        var work_count: u32 = 0;

        // 1. Drain admin commands from receiver event loop
        work_count += self.processAdminCommands(now_ns);

        // 2. Check peer liveness (heartbeat timeout)
        work_count += self.checkPeerLiveness(now_ns);

        // 3. Send broker heartbeats
        work_count += self.heartbeat_sender.sendIfDue(now_ns);

        // 4. Retry SETUP for disconnected peers
        work_count += self.retrySetups(now_ns);

        return work_count;
    }

    // ── Private ──────────────────────────────────────────────────────

    /// Called when a leadership change is detected.
    /// Handles both the NodeMembership update and downstream effects.
    fn handleLeaderChange(self: *ClusterEventHandler, leader_node_id: u8) void {
        self.node_membership.electLeader(leader_node_id);

        if (self.node_membership.isLeader()) {
            // This broker is the new leader — re-evaluate all service leaders
            self.reEvaluateAndBroadcast();
        }
    }

    fn reEvaluateAndBroadcast(self: *ClusterEventHandler) void {
        var changes: [256]ServiceLeaderElectionManager.ServiceLeaderChange = undefined;
        const count = self.service_leader_election.reEvaluateAllLeaders(&changes);

        for (changes[0..count]) |change| {
            const result = change.result;

            self.cluster_state.broadcastServiceLeaderDesignated(
                result.service_id,
                change.service_name,
                result.node_id,
                self.broadcast_admin_fn,
            );

            log.info("service leader designated: service={s}, serviceId={}, nodeId={}", .{
                admin.trimServiceName(&change.service_name),
                result.service_id,
                result.node_id,
            });
        }
    }

    /// Check all connected peers for heartbeat timeout.
    fn checkPeerLiveness(self: *ClusterEventHandler, now_ns: i64) u32 {
        var work_count: u32 = 0;
        // Collect node IDs to disconnect (avoid mutating while iterating)
        var to_disconnect: [256]u8 = undefined;
        var disconnect_count: u8 = 0;

        for (self.node_membership.nodes) |slot| {
            if (slot) |node| {
                if (node.is_local) continue;
                if (node.connection_state != .connected) continue;
                if (now_ns - node.last_heartbeat_ns > MASTER_DOWN_INTERVAL_NS) {
                    if (disconnect_count < 255) {
                        to_disconnect[disconnect_count] = node.id;
                        disconnect_count += 1;
                    }
                }
            }
        }

        for (to_disconnect[0..disconnect_count]) |nid| {
            self.handlePeerDisconnected(nid, now_ns);
            work_count += 1;
        }

        return work_count;
    }

    fn retrySetups(self: *ClusterEventHandler, now_ns: i64) u32 {
        var work_count: u32 = 0;
        for (&self.node_membership.nodes) |*slot| {
            if (slot.*) |*node| {
                if (node.is_local) continue;
                if (node.shouldRetrySetup(now_ns)) {
                    node.markSetupSent(now_ns);
                    work_count += 1;
                }
            }
        }
        return work_count;
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

// Test broadcast capture
var test_broadcast_count: u32 = 0;
var test_last_broadcast: [256]u8 = undefined;
var test_last_broadcast_len: usize = 0;

fn testBroadcast(buf: []const u8) void {
    test_broadcast_count += 1;
    test_last_broadcast_len = @min(buf.len, test_last_broadcast.len);
    @memcpy(test_last_broadcast[0..test_last_broadcast_len], buf[0..test_last_broadcast_len]);
}

/// All subsystem instances stored by value in a struct that the test owns.
/// The ClusterEventHandler takes pointers into these fields, which remain
/// at stable addresses because the struct itself lives on the test's stack.
const TestContext = struct {
    leader_election: LeaderElection,
    node_membership: NodeMembership,
    cluster_state: ClusterState,
    service_leader_election: ServiceLeaderElectionManager,
    heartbeat_sender: BrokerHeartbeatSender,
    admin_cmd_queue: admin_dispatch.AdminCommandQueue(64),
    handler: ClusterEventHandler,

    fn init() TestContext {
        // Initialize with placeholder values — the handler pointers will be
        // fixed up by `initHandler()` once the struct has a stable address.
        return .{
            .leader_election = LeaderElection.initWithDeadline(1, 10_000_000_000),
            .node_membership = NodeMembership.init(1, admin.padHostPort("localhost:40456")),
            .cluster_state = ClusterState.init(),
            .service_leader_election = ServiceLeaderElectionManager.init(testing.allocator, 1),
            .heartbeat_sender = BrokerHeartbeatSender.init(1, admin.padHostPort("localhost:40456"), &testBroadcast),
            .admin_cmd_queue = .{},
            // Temporary zero-init — will be replaced by initHandler()
            .handler = undefined,
        };
    }

    /// Must be called after the struct has settled at its final stack address.
    fn initHandler(self: *TestContext) void {
        self.handler = .{
            .leader_election = &self.leader_election,
            .node_membership = &self.node_membership,
            .cluster_state = &self.cluster_state,
            .service_leader_election = &self.service_leader_election,
            .heartbeat_sender = &self.heartbeat_sender,
            .admin_cmd_queue = &self.admin_cmd_queue,
            .local_node_id = 1,
            .broadcast_admin_fn = &testBroadcast,
        };
    }

    fn deinit(self: *TestContext) void {
        self.service_leader_election.deinit();
    }
};

test "onBrokerHeartbeat registers unknown peer" {
    // Given
    var ctx = TestContext.init();
    defer ctx.deinit();
    ctx.initHandler();

    // When
    ctx.handler.onBrokerHeartbeat(2, admin.padHostPort("host2:40456"), 1_000_000_000);

    // Then
    try testing.expect(ctx.node_membership.hasNode(2));
}

test "onBrokerHeartbeat updates last_heartbeat_ns" {
    // Given
    var ctx = TestContext.init();
    defer ctx.deinit();
    ctx.initHandler();
    ctx.node_membership.addNode(2, admin.padHostPort("host2:40456"), null);

    // When
    ctx.handler.onBrokerHeartbeat(2, admin.padHostPort("host2:40456"), 5_000_000_000);

    // Then
    try testing.expectEqual(@as(i64, 5_000_000_000), ctx.node_membership.getNode(2).?.last_heartbeat_ns);
}

test "onBrokerHeartbeat triggers leader election" {
    // Given: node 1 is the local node, no leader yet
    var ctx = TestContext.init();
    defer ctx.deinit();
    ctx.initHandler();

    // When: receive heartbeat from node 2 (local is 1, so node 2 has worse priority)
    ctx.handler.onBrokerHeartbeat(2, admin.padHostPort("host2:40456"), 1_000_000_000);

    // Then: node 2 was registered and became leader (first heartbeat, no prior leader)
    try testing.expect(ctx.node_membership.hasNode(2));
    // The leader election accepted node 2 as leader (it was the first heartbeat)
    try testing.expectEqual(@as(?u8, 2), ctx.leader_election.getLeader());
}

test "onPeerConnected marks node connected" {
    // Given
    var ctx = TestContext.init();
    defer ctx.deinit();
    ctx.initHandler();
    ctx.node_membership.addNode(2, admin.padHostPort("host2:40456"), null);

    // When
    ctx.handler.onPeerConnected(2, 1_000_000_000);

    // Then
    try testing.expectEqual(ConnectionState.connected, ctx.node_membership.getNode(2).?.connection_state);
}

test "onPeerConnected on unknown node is a no-op" {
    // Given
    var ctx = TestContext.init();
    defer ctx.deinit();
    ctx.initHandler();

    // When — no crash expected
    ctx.handler.onPeerConnected(99, 1_000_000_000);

    // Then
    try testing.expect(!ctx.node_membership.hasNode(99));
}

test "handlePeerDisconnected removes node" {
    // Given
    var ctx = TestContext.init();
    defer ctx.deinit();
    ctx.initHandler();
    ctx.node_membership.addNode(2, admin.padHostPort("host2:40456"), null);
    ctx.node_membership.getNode(2).?.markConnected(1_000_000_000);

    // When
    ctx.handler.handlePeerDisconnected(2, 2_000_000_000);

    // Then
    try testing.expect(!ctx.node_membership.hasNode(2));
}

test "handlePeerDisconnected is idempotent" {
    // Given
    var ctx = TestContext.init();
    defer ctx.deinit();
    ctx.initHandler();
    ctx.node_membership.addNode(2, admin.padHostPort("host2:40456"), null);
    ctx.node_membership.getNode(2).?.markConnected(1_000_000_000);

    // When — disconnect twice
    ctx.handler.handlePeerDisconnected(2, 2_000_000_000);
    ctx.handler.handlePeerDisconnected(2, 3_000_000_000);

    // Then — no crash, node still removed
    try testing.expect(!ctx.node_membership.hasNode(2));
}

test "handlePeerDisconnected skips already-disconnected node" {
    // Given — node exists but is in disconnected state
    var ctx = TestContext.init();
    defer ctx.deinit();
    ctx.initHandler();
    ctx.node_membership.addNode(2, admin.padHostPort("host2:40456"), null);
    // node starts in .disconnected state by default (never connected)

    // When
    ctx.handler.handlePeerDisconnected(2, 2_000_000_000);

    // Then — node not removed because it was already disconnected
    try testing.expect(ctx.node_membership.hasNode(2));
}

test "handlePeerDisconnected on unknown node is a no-op" {
    // Given
    var ctx = TestContext.init();
    defer ctx.deinit();
    ctx.initHandler();

    // When — no crash expected
    ctx.handler.handlePeerDisconnected(99, 1_000_000_000);

    // Then
    try testing.expect(!ctx.node_membership.hasNode(99));
}

test "doWork sends heartbeat" {
    // Given
    test_broadcast_count = 0;
    var ctx = TestContext.init();
    defer ctx.deinit();
    ctx.initHandler();

    // When
    _ = ctx.handler.doWork(1_000_000_000);

    // Then — at least one heartbeat sent
    try testing.expect(test_broadcast_count > 0);
}

test "doWork returns cumulative work count" {
    // Given
    test_broadcast_count = 0;
    var ctx = TestContext.init();
    defer ctx.deinit();
    ctx.initHandler();

    // When — first call will send a heartbeat (next_heartbeat_ns starts at 0)
    const work = ctx.handler.doWork(1_000_000_000);

    // Then — at least the heartbeat counts as work
    try testing.expect(work >= 1);
}

test "processAdminCommands drains queue" {
    // Given
    var ctx = TestContext.init();
    defer ctx.deinit();
    ctx.initHandler();

    // Enqueue a heartbeat command
    _ = ctx.admin_cmd_queue.enqueue(.{
        .broker_heartbeat = .{
            .node_id = 3,
            .host_and_port = admin.padHostPort("host3:40456"),
            .received_ns = 1_000_000_000,
        },
    });

    // When
    const work = ctx.handler.processAdminCommands(1_000_000_000);

    // Then
    try testing.expect(work >= 1);
    try testing.expect(ctx.node_membership.hasNode(3));
}

test "processAdminCommands drains multiple commands" {
    // Given
    var ctx = TestContext.init();
    defer ctx.deinit();
    ctx.initHandler();

    _ = ctx.admin_cmd_queue.enqueue(.{
        .broker_heartbeat = .{
            .node_id = 3,
            .host_and_port = admin.padHostPort("host3:40456"),
            .received_ns = 1_000_000_000,
        },
    });
    _ = ctx.admin_cmd_queue.enqueue(.{
        .broker_heartbeat = .{
            .node_id = 4,
            .host_and_port = admin.padHostPort("host4:40456"),
            .received_ns = 1_000_000_000,
        },
    });

    // When
    const work = ctx.handler.processAdminCommands(1_000_000_000);

    // Then
    try testing.expect(work >= 2);
    try testing.expect(ctx.node_membership.hasNode(3));
    try testing.expect(ctx.node_membership.hasNode(4));
}

test "processAdminCommands handles empty queue" {
    // Given
    var ctx = TestContext.init();
    defer ctx.deinit();
    ctx.initHandler();

    // When — nothing in the queue
    const work = ctx.handler.processAdminCommands(1_000_000_000);

    // Then — only master-down check ran (no change expected with far deadline)
    try testing.expectEqual(@as(u32, 0), work);
}

test "processAdminCommands triggers master-down self-election" {
    // Given — deadline is in the past so master-down fires
    var ctx = TestContext.init();
    defer ctx.deinit();
    ctx.leader_election = LeaderElection.initWithDeadline(1, 0);
    ctx.initHandler();

    // When — now_ns > deadline
    const work = ctx.handler.processAdminCommands(5_000_000_000);

    // Then — master-down triggered self-election
    try testing.expect(work >= 1);
    try testing.expect(ctx.leader_election.isLocalNodeLeader());
}

test "checkPeerLiveness disconnects timed-out peer" {
    // Given
    var ctx = TestContext.init();
    defer ctx.deinit();
    ctx.initHandler();
    ctx.node_membership.addNode(2, admin.padHostPort("host2:40456"), null);
    const node = ctx.node_membership.getNode(2).?;
    node.markConnected(1_000_000_000);
    node.last_heartbeat_ns = 1_000_000_000;

    // When — time advances past the master-down interval (3s)
    const work = ctx.handler.checkPeerLiveness(5_000_000_000);

    // Then — peer was disconnected
    try testing.expect(work >= 1);
    try testing.expect(!ctx.node_membership.hasNode(2));
}

test "checkPeerLiveness does not disconnect healthy peer" {
    // Given
    var ctx = TestContext.init();
    defer ctx.deinit();
    ctx.initHandler();
    ctx.node_membership.addNode(2, admin.padHostPort("host2:40456"), null);
    const node = ctx.node_membership.getNode(2).?;
    node.markConnected(1_000_000_000);
    node.last_heartbeat_ns = 1_000_000_000;

    // When — time is within the interval
    const work = ctx.handler.checkPeerLiveness(2_000_000_000);

    // Then — peer still present
    try testing.expectEqual(@as(u32, 0), work);
    try testing.expect(ctx.node_membership.hasNode(2));
}

test "checkPeerLiveness skips local node" {
    // Given
    var ctx = TestContext.init();
    defer ctx.deinit();
    ctx.initHandler();
    // Local node (1) is always present and connected — it should never be
    // disconnected by the liveness check, even with a zero heartbeat timestamp.

    // When — time well past any threshold
    const work = ctx.handler.checkPeerLiveness(100_000_000_000);

    // Then — local node untouched
    try testing.expectEqual(@as(u32, 0), work);
    try testing.expect(ctx.node_membership.hasNode(1));
}

test "retrySetups sends setup for disconnected peer" {
    // Given
    var ctx = TestContext.init();
    defer ctx.deinit();
    ctx.initHandler();
    ctx.node_membership.addNode(2, admin.padHostPort("host2:40456"), null);
    // Node starts in disconnected state with last_setup_sent_ns = 0

    // When — enough time has passed for a retry
    const work = ctx.handler.retrySetups(2_000_000_000);

    // Then
    try testing.expect(work >= 1);
    try testing.expectEqual(ConnectionState.setup_sent, ctx.node_membership.getNode(2).?.connection_state);
}

test "retrySetups skips local node" {
    // Given
    var ctx = TestContext.init();
    defer ctx.deinit();
    ctx.initHandler();

    // When
    const work = ctx.handler.retrySetups(2_000_000_000);

    // Then — only local node exists, no setups needed
    try testing.expectEqual(@as(u32, 0), work);
}

test "leader change triggers electLeader on membership" {
    // Given
    var ctx = TestContext.init();
    defer ctx.deinit();
    ctx.initHandler();
    ctx.node_membership.addNode(2, admin.padHostPort("host2:40456"), null);

    // When — heartbeat from node 2 causes it to become leader (first HB)
    ctx.handler.onBrokerHeartbeat(2, admin.padHostPort("host2:40456"), 1_000_000_000);

    // Then — node 2 is marked as leader in membership
    try testing.expectEqual(@as(?u8, 2), ctx.node_membership.getLeader());
    try testing.expect(!ctx.node_membership.isLeader()); // local node 1 is not leader
}

test "local node becomes leader and re-evaluates service leaders" {
    // Given — master-down deadline in the past, forcing self-election
    test_broadcast_count = 0;
    var ctx = TestContext.init();
    defer ctx.deinit();
    ctx.leader_election = LeaderElection.initWithDeadline(1, 0);
    ctx.initHandler();

    // When — processAdminCommands triggers master-down
    _ = ctx.handler.processAdminCommands(5_000_000_000);

    // Then — local node is the leader
    try testing.expect(ctx.leader_election.isLocalNodeLeader());
    try testing.expect(ctx.node_membership.isLeader());
}

test "full lifecycle: discover, connect, heartbeat, disconnect" {
    // Given
    test_broadcast_count = 0;
    var ctx = TestContext.init();
    defer ctx.deinit();
    ctx.initHandler();

    // 1. Discover peer via heartbeat
    ctx.handler.onBrokerHeartbeat(2, admin.padHostPort("host2:40456"), 1_000_000_000);
    try testing.expect(ctx.node_membership.hasNode(2));

    // 2. Peer completes connection
    ctx.handler.onPeerConnected(2, 1_500_000_000);
    try testing.expectEqual(ConnectionState.connected, ctx.node_membership.getNode(2).?.connection_state);

    // 3. More heartbeats keep the peer alive
    ctx.handler.onBrokerHeartbeat(2, admin.padHostPort("host2:40456"), 2_000_000_000);
    try testing.expectEqual(@as(i64, 2_000_000_000), ctx.node_membership.getNode(2).?.last_heartbeat_ns);

    // 4. Peer disconnects
    ctx.handler.handlePeerDisconnected(2, 6_000_000_000);
    try testing.expect(!ctx.node_membership.hasNode(2));
}
