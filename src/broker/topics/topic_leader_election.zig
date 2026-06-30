// SPDX-License-Identifier: Apache-2.0
//! TopicLeaderElection — a separate single topic-leader election among the
//! topics-enabled brokers (spec 08), decoupled from the cluster master.
//!
//! Reuses the broker's lowest-nodeId-alive algorithm but only over candidates
//! whose heartbeat advertises `topics_enabled`. The single topic leader sequences
//! ALL topics. On a leadership change the new leader begins a new monotonic term
//! (`leader_epoch`) used for fencing, and must clear the failover catch-up
//! barrier (sync to the most-advanced replica) before accepting writes.

const std = @import("std");

pub const LeaderDownIntervalNs: i64 = 3 * std.time.ns_per_s;

pub const Result = struct {
    leader: ?u8 = null,
    changed: bool = false,
    /// Set when this node just became the leader and started a new term.
    became_leader: bool = false,
    /// The term in effect after this operation (0 = no leader).
    leader_epoch: u64 = 0,
};

pub const TopicLeaderElection = struct {
    local_node_id: u8,
    /// Whether THIS broker has topics enabled (only enabled brokers are candidates).
    local_topics_enabled: bool,

    current_leader: ?u8 = null,
    /// Monotonic term of the current leadership. Learned from announcements or
    /// minted locally when self-electing (always strictly increases).
    leader_epoch: u64 = 0,
    /// Highest epoch ever observed, so a new term is always greater.
    highest_epoch_seen: u64 = 0,
    leader_down_deadline_ns: i64 = 0,

    pub fn init(local_node_id: u8, local_topics_enabled: bool, now_ns: i64) TopicLeaderElection {
        return .{
            .local_node_id = local_node_id,
            .local_topics_enabled = local_topics_enabled,
            .leader_down_deadline_ns = now_ns + LeaderDownIntervalNs,
        };
    }

    pub fn initWithDeadline(local_node_id: u8, local_topics_enabled: bool, deadline_ns: i64) TopicLeaderElection {
        return .{
            .local_node_id = local_node_id,
            .local_topics_enabled = local_topics_enabled,
            .leader_down_deadline_ns = deadline_ns,
        };
    }

    /// A topics-enabled peer heartbeat. Heartbeats from topics-disabled peers are
    /// ignored for leadership (the caller passes the advertised bit).
    pub fn onTopicHeartbeat(self: *TopicLeaderElection, sender_id: u8, sender_topics_enabled: bool, now_ns: i64) Result {
        if (!sender_topics_enabled) {
            return .{ .leader = self.current_leader, .leader_epoch = self.leader_epoch };
        }
        // Lowest node-id among: sender, current leader, and the local node
        // (if topics-enabled). This ensures a low-id local node is never
        // preempted merely because it hasn't received its own heartbeat.
        var best: u8 = sender_id;
        if (self.local_topics_enabled and self.local_node_id < best) {
            best = self.local_node_id;
        }
        if (self.current_leader) |current| {
            if (current < best) best = current;
        }
        if (self.current_leader == null or self.current_leader.? != best) {
            const changed = self.current_leader == null or self.current_leader.? != best;
            self.current_leader = best;
            self.leader_down_deadline_ns = now_ns + LeaderDownIntervalNs;
            return .{ .leader = best, .changed = changed, .leader_epoch = self.leader_epoch };
        }
        // Refresh leader deadline on every heartbeat to prevent false timeouts
        // triggered by the barrier destroying an existing valid sink.
        self.leader_down_deadline_ns = now_ns + LeaderDownIntervalNs;
        return .{ .leader = self.current_leader, .leader_epoch = self.leader_epoch };
    }

    /// Apply a TOPIC_LEADER_CHANGED announcement (new leader minted an epoch).
    pub fn onLeaderAnnouncement(self: *TopicLeaderElection, new_leader: u8, epoch: u64, now_ns: i64) Result {
        if (epoch > self.highest_epoch_seen) self.highest_epoch_seen = epoch;
        // Accept the announced leadership if its epoch is at least as new.
        if (epoch >= self.leader_epoch) {
            const changed = self.current_leader == null or self.current_leader.? != new_leader or self.leader_epoch != epoch;
            self.current_leader = new_leader;
            self.leader_epoch = epoch;
            self.leader_down_deadline_ns = now_ns + LeaderDownIntervalNs;
            return .{ .leader = new_leader, .changed = changed, .leader_epoch = epoch };
        }
        return .{ .leader = self.current_leader, .leader_epoch = self.leader_epoch };
    }

    /// Duty-cycle check: self-elect if the leader is presumed dead and this node
    /// is a topics-enabled candidate. Mints a new term on transition to leader.
    /// Also mints epoch 1 when heartbeat-derived leadership has epoch 0.
    pub fn checkLeaderDown(self: *TopicLeaderElection, now_ns: i64) Result {
        if (self.current_leader != null and self.current_leader.? == self.local_node_id) {
            if (self.leader_epoch == 0) {
                self.highest_epoch_seen = @max(self.highest_epoch_seen, self.leader_epoch);
                self.leader_epoch = self.highest_epoch_seen + 1;
                self.highest_epoch_seen = self.leader_epoch;
                return .{ .leader = self.local_node_id, .changed = true, .became_leader = true, .leader_epoch = self.leader_epoch };
            }
            return .{ .leader = self.current_leader, .became_leader = false, .leader_epoch = self.leader_epoch };
        }
        if (!self.local_topics_enabled) {
            return .{ .leader = self.current_leader, .leader_epoch = self.leader_epoch };
        }
        if (now_ns < self.leader_down_deadline_ns) {
            return .{ .leader = self.current_leader, .leader_epoch = self.leader_epoch };
        }
        // Leader presumed dead — self-elect and start a new, strictly-greater term.
        const previous = self.current_leader;
        self.current_leader = self.local_node_id;
        self.highest_epoch_seen = @max(self.highest_epoch_seen, self.leader_epoch);
        self.leader_epoch = self.highest_epoch_seen + 1;
        self.highest_epoch_seen = self.leader_epoch;
        self.leader_down_deadline_ns = now_ns + LeaderDownIntervalNs;
        const changed = previous == null or previous.? != self.local_node_id;
        return .{ .leader = self.local_node_id, .changed = changed, .became_leader = changed, .leader_epoch = self.leader_epoch };
    }

    pub fn onPeerDisconnected(self: *TopicLeaderElection, departed_id: u8, now_ns: i64) Result {
        if (self.current_leader != null and self.current_leader.? == departed_id) {
            self.current_leader = null;
            self.leader_down_deadline_ns = now_ns;
            return .{ .leader = null, .changed = true, .leader_epoch = self.leader_epoch };
        }
        return .{ .leader = self.current_leader, .leader_epoch = self.leader_epoch };
    }

    pub fn isLocalLeader(self: *const TopicLeaderElection) bool {
        return self.current_leader != null and self.current_leader.? == self.local_node_id;
    }

    pub fn getLeader(self: *const TopicLeaderElection) ?u8 {
        return self.current_leader;
    }

    pub fn getEpoch(self: *const TopicLeaderElection) u64 {
        return self.leader_epoch;
    }
};

// ── Tests ────────────────────────────────────────────────────────────────────

const testing = std.testing;

test "only topics-enabled peers are leadership candidates" {
    // Local node 4: high enough that peer heartbeats dictate leadership.
    var e = TopicLeaderElection.initWithDeadline(4, true, 10_000_000_000);
    // A topics-disabled node 1 must NOT become leader despite lower id.
    _ = e.onTopicHeartbeat(1, false, 1_000_000_000);
    try testing.expect(e.getLeader() == null);
    // A topics-enabled node 3 becomes leader (3 < local 4).
    _ = e.onTopicHeartbeat(3, true, 1_000_000_000);
    try testing.expectEqual(@as(?u8, 3), e.getLeader());
    // A topics-enabled node 1 preempts (1 < 3).
    _ = e.onTopicHeartbeat(1, true, 1_000_000_000);
    try testing.expectEqual(@as(?u8, 1), e.getLeader());
}

test "local node recognized as leader when it has lowest id" {
    // Local node 1, topics-enabled: must be leader when a higher-id peer
    // heartbeat arrives, because 1 < 2.
    var e = TopicLeaderElection.initWithDeadline(1, true, 10_000_000_000);
    _ = e.onTopicHeartbeat(2, true, 1_000_000_000);
    try testing.expectEqual(@as(?u8, 1), e.getLeader());
}

test "local node not a candidate when topics disabled" {
    // Local node 1, topics-disabled: must NOT be leader despite lowest id.
    var e = TopicLeaderElection.initWithDeadline(1, false, 10_000_000_000);
    _ = e.onTopicHeartbeat(2, true, 1_000_000_000);
    try testing.expectEqual(@as(?u8, 2), e.getLeader());
}

test "self-election mints a strictly greater term" {
    var e = TopicLeaderElection.initWithDeadline(2, true, 0);
    _ = e.onTopicHeartbeat(1, true, 1_000_000_000); // node 1 leads, epoch 0
    // node 1 dies, node 2 self-elects.
    const r = e.checkLeaderDown(5_000_000_000);
    try testing.expect(r.became_leader);
    try testing.expect(e.isLocalLeader());
    try testing.expect(e.getEpoch() >= 1);
}

test "topics-disabled local node never self-elects" {
    var e = TopicLeaderElection.initWithDeadline(2, false, 0);
    const r = e.checkLeaderDown(5_000_000_000);
    try testing.expect(!r.became_leader);
    try testing.expect(!e.isLocalLeader());
}

test "leader announcement with newer epoch is accepted and fences old term" {
    var e = TopicLeaderElection.initWithDeadline(3, true, 10_000_000_000);
    _ = e.checkLeaderDown(0); // not yet due
    // Announcement: node 1 is leader at epoch 7.
    const r = e.onLeaderAnnouncement(1, 7, 1_000_000_000);
    try testing.expect(r.changed);
    try testing.expectEqual(@as(?u8, 1), e.getLeader());
    try testing.expectEqual(@as(u64, 7), e.getEpoch());
    // A subsequent self-election must mint epoch > 7.
    const r2 = e.checkLeaderDown(20_000_000_000);
    try testing.expect(r2.became_leader);
    try testing.expect(e.getEpoch() > 7);
}

test "peer disconnect of leader triggers re-election" {
    var e = TopicLeaderElection.initWithDeadline(2, true, 10_000_000_000);
    _ = e.onTopicHeartbeat(1, true, 1_000_000_000);
    const r = e.onPeerDisconnected(1, 2_000_000_000);
    try testing.expect(r.changed);
    try testing.expect(e.getLeader() == null);
    const r2 = e.checkLeaderDown(2_000_000_000);
    try testing.expect(r2.became_leader);
}
