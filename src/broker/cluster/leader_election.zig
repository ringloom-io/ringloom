//! VRRP-style broker leader election via heartbeat priority.
//!
//! The cluster leader is the broker with the lowest nodeId among all known
//! alive brokers. Leadership is determined implicitly: every broker's heartbeat
//! is simultaneously a claim to leadership priority. If a broker with a lower
//! nodeId is heard from, it is automatically accepted as the leader.
//!
//! There is no election phase, no election window, no acknowledgment round.
//! The heartbeat *is* the election. Leadership is an emergent property of
//! "which node with the lowest ID am I hearing from?"

const std = @import("std");
const Clock = @import("brz_common").platform.clock.Clock;

pub const LeaderElection = struct {
    /// Result returned by methods that may change leadership state.
    pub const Result = struct {
        /// The leader nodeId after this operation, or null if no leader.
        leader: ?u8 = null,
        /// True if the leader changed as a result of this operation.
        changed: bool = false,
    };

    /// This broker's node ID (immutable after init).
    local_node_id: u8,

    /// The currently accepted cluster leader. `null` = no leader known.
    current_leader: ?u8 = null,

    /// Monotonic deadline: if `Clock.monotonicNanos()` exceeds this value
    /// without a heartbeat from the current leader, the leader is presumed dead.
    master_down_deadline_ns: i64 = 0,

    /// 3 × heartbeat interval. The leader must send at least one heartbeat
    /// within this window or it is considered dead.
    pub const MASTER_DOWN_INTERVAL_NS: i64 = 3 * std.time.ns_per_s;

    pub fn init(local_node_id: u8) LeaderElection {
        return .{
            .local_node_id = local_node_id,
            // Set initial deadline so the first check after startup triggers
            // self-election if no peers respond within the interval.
            .master_down_deadline_ns = Clock.monotonicNanos() + MASTER_DOWN_INTERVAL_NS,
        };
    }

    /// Create a LeaderElection with a specific initial deadline (useful for testing).
    pub fn initWithDeadline(local_node_id: u8, deadline_ns: i64) LeaderElection {
        return .{
            .local_node_id = local_node_id,
            .master_down_deadline_ns = deadline_ns,
        };
    }

    // ── Heartbeat handling (the core of VRRP-style election) ─────────

    /// Called when a BrokerHeartbeat is received from a peer.
    /// Returns a Result indicating whether the leader changed.
    ///
    /// This is the primary election mechanism: if the sender has equal or
    /// better priority (lower nodeId) than the current leader, it becomes
    /// the new leader. The master-down timer is reset.
    pub fn onBrokerHeartbeat(self: *LeaderElection, sender_id: u8, now_ns: i64) Result {
        const current = self.current_leader orelse std.math.maxInt(u8);

        if (sender_id <= current) {
            // Sender has equal or better priority — accept as leader
            const changed = self.current_leader == null or self.current_leader.? != sender_id;
            self.current_leader = sender_id;
            self.master_down_deadline_ns = now_ns + MASTER_DOWN_INTERVAL_NS;
            return .{ .leader = sender_id, .changed = changed };
        }

        // Sender has worse priority — not a leadership change, but still
        // a valid heartbeat for liveness purposes (handled by caller).
        return .{ .leader = self.current_leader, .changed = false };
    }

    // ── Master-down timer check ──────────────────────────────────────

    /// Called once per broker-agent duty cycle. Checks if the master-down
    /// timer has expired. If so, determines the new leader.
    ///
    /// Returns a Result. If `changed == true`, the caller must invoke
    /// post-election logic (service leader re-evaluation, etc.).
    pub fn checkMasterDown(self: *LeaderElection, now_ns: i64) Result {
        // No timeout if we are the leader (we don't need our own heartbeats)
        if (self.current_leader != null and self.current_leader.? == self.local_node_id) {
            return .{ .leader = self.current_leader, .changed = false };
        }

        // Timer hasn't expired yet
        if (now_ns < self.master_down_deadline_ns) {
            return .{ .leader = self.current_leader, .changed = false };
        }

        // Master-down timer expired — the current leader is presumed dead.
        // Become leader if we have the lowest nodeId among known-alive nodes.
        // (The caller provides alive-node information via NodeMembership;
        //  here we optimistically self-elect. If a better node is alive,
        //  its next heartbeat will preempt us within 1 second.)
        const previous = self.current_leader;
        self.current_leader = self.local_node_id;
        self.master_down_deadline_ns = now_ns + MASTER_DOWN_INTERVAL_NS;

        const changed = previous == null or previous.? != self.local_node_id;
        return .{ .leader = self.local_node_id, .changed = changed };
    }

    // ── Peer departure ───────────────────────────────────────────────

    /// Called when a peer is known to have disconnected (e.g. heartbeat
    /// timeout or TEARDOWN frame received). If the departed peer was the
    /// leader, resets the master-down timer to trigger immediate
    /// re-election on the next duty cycle.
    pub fn onPeerDisconnected(self: *LeaderElection, departed_id: u8, now_ns: i64) Result {
        if (self.current_leader != null and self.current_leader.? == departed_id) {
            // Leader is gone — expire the timer immediately
            self.current_leader = null;
            self.master_down_deadline_ns = now_ns; // triggers on next checkMasterDown()
            return .{ .leader = null, .changed = true };
        }
        return .{ .leader = self.current_leader, .changed = false };
    }

    // ── Queries ──────────────────────────────────────────────────────

    pub fn isLocalNodeLeader(self: *const LeaderElection) bool {
        return self.current_leader != null and self.current_leader.? == self.local_node_id;
    }

    pub fn getLeader(self: *const LeaderElection) ?u8 {
        return self.current_leader;
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "lowest nodeId wins via heartbeat — 3 nodes" {
    // Given: node 2 is the local node
    var election = LeaderElection.initWithDeadline(2, 10_000_000_000);

    // When: receive heartbeats from nodes 1 and 3
    const now_ns: i64 = 1_000_000_000;
    const result1 = election.onBrokerHeartbeat(3, now_ns);
    const result2 = election.onBrokerHeartbeat(1, now_ns);

    // Then: node 1 should be the leader (lowest nodeId)
    try testing.expectEqual(@as(?u8, 1), election.current_leader);
    try testing.expect(!election.isLocalNodeLeader());
    try testing.expect(result1.changed); // 3 was first leader (no leader before)
    try testing.expect(result2.changed); // changed when 1 beat 3
}

test "heartbeat from higher nodeId does not change leader" {
    // Given: node 1 is already the leader
    var election = LeaderElection.initWithDeadline(2, 10_000_000_000);
    _ = election.onBrokerHeartbeat(1, 1_000_000_000);

    // When: receive heartbeat from node 3 (worse priority)
    const result = election.onBrokerHeartbeat(3, 2_000_000_000);

    // Then: leader unchanged
    try testing.expectEqual(@as(?u8, 1), election.current_leader);
    try testing.expect(!result.changed);
}

test "master-down timer triggers self-election" {
    // Given: node 2 is the local node, node 1 was the leader
    var election = LeaderElection.initWithDeadline(2, 10_000_000_000);
    _ = election.onBrokerHeartbeat(1, 1_000_000_000);
    try testing.expectEqual(@as(?u8, 1), election.current_leader);

    // When: master-down timer expires (3 seconds, no heartbeat from node 1)
    const result = election.checkMasterDown(5_000_000_000);

    // Then: node 2 self-elects
    try testing.expectEqual(@as(?u8, 2), election.current_leader);
    try testing.expect(result.changed);
    try testing.expect(election.isLocalNodeLeader());
}

test "preemption — lower nodeId heartbeat overrides current leader" {
    // Given: node 3 is the local node, self-elected as leader
    var election = LeaderElection.initWithDeadline(3, 0);
    _ = election.checkMasterDown(5_000_000_000); // self-elects
    try testing.expect(election.isLocalNodeLeader());

    // When: node 1 comes back and sends a heartbeat
    const result = election.onBrokerHeartbeat(1, 6_000_000_000);

    // Then: node 1 preempts node 3
    try testing.expectEqual(@as(?u8, 1), election.current_leader);
    try testing.expect(!election.isLocalNodeLeader());
    try testing.expect(result.changed);
}

test "single-node cluster self-elects on master-down" {
    // Given: only node 1, no peers
    var election = LeaderElection.initWithDeadline(1, 0);

    // When: master-down timer expires (no heartbeats from anyone)
    const result = election.checkMasterDown(5_000_000_000);

    // Then: node 1 is the leader
    try testing.expectEqual(@as(?u8, 1), election.current_leader);
    try testing.expect(election.isLocalNodeLeader());
    try testing.expect(result.changed);
}

test "peer disconnection clears leader and triggers re-election" {
    // Given: node 1 was the leader
    var election = LeaderElection.initWithDeadline(2, 10_000_000_000);
    _ = election.onBrokerHeartbeat(1, 1_000_000_000);

    // When: node 1 disconnects
    const result = election.onPeerDisconnected(1, 2_000_000_000);

    // Then: leader is cleared, change flagged
    try testing.expectEqual(@as(?u8, null), election.current_leader);
    try testing.expect(result.changed);

    // And: next checkMasterDown causes self-election
    const result2 = election.checkMasterDown(2_000_000_000);
    try testing.expectEqual(@as(?u8, 2), result2.leader);
    try testing.expect(result2.changed);
}

test "heartbeat resets master-down timer" {
    // Given: node 2 is the local node, node 1 is the leader
    var election = LeaderElection.initWithDeadline(2, 10_000_000_000);
    _ = election.onBrokerHeartbeat(1, 1_000_000_000);

    // When: heartbeat received just before timeout
    _ = election.onBrokerHeartbeat(1, 3_500_000_000);

    // Then: master-down timer reset — no election at t=4s
    const result = election.checkMasterDown(4_000_000_000);
    try testing.expect(!result.changed);
    try testing.expectEqual(@as(?u8, 1), election.current_leader);
}

test "no timeout when local node is leader" {
    // Given: node 1 is the local node and self-elected leader
    var election = LeaderElection.initWithDeadline(1, 0);
    _ = election.checkMasterDown(1_000_000_000); // self-elects

    // When: time passes well beyond master-down interval
    const result = election.checkMasterDown(100_000_000_000);

    // Then: no change — we don't time ourselves out
    try testing.expect(!result.changed);
    try testing.expectEqual(@as(?u8, 1), election.current_leader);
}

test "disconnection of non-leader has no effect" {
    // Given: node 1 is the leader
    var election = LeaderElection.initWithDeadline(2, 10_000_000_000);
    _ = election.onBrokerHeartbeat(1, 1_000_000_000);

    // When: node 3 (not the leader) disconnects
    const result = election.onPeerDisconnected(3, 2_000_000_000);

    // Then: leader unchanged, no change flagged
    try testing.expectEqual(@as(?u8, 1), election.current_leader);
    try testing.expect(!result.changed);
}

test "getLeader returns null initially" {
    // Given
    var election = LeaderElection.initWithDeadline(1, 10_000_000_000);

    // When / Then
    try testing.expect(election.getLeader() == null);
    try testing.expect(!election.isLocalNodeLeader());
}
