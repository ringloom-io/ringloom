// SPDX-License-Identifier: Apache-2.0
//! Thread-safe registry of local topic publishers keyed by `publisher_id`.
//!
//! Publishers are registered from the C ABI (caller thread) and receive
//! ack-feedback / leader-change frames on the service control thread. A service
//! may hold several publishers on the same topic, so feedback is matched by
//! `publisher_id` and leader changes go to every publisher on the topic.
//!
//! Updates are applied while the registry lock is held. Unregistering takes the
//! same lock, so once `unregister` returns the control thread no longer touches
//! that publisher and the caller may free it. The hot publish path never takes
//! this lock.

const std = @import("std");
const topic_publisher_mod = @import("topic_publisher.zig");
const TopicPublisher = topic_publisher_mod.TopicPublisher;

pub const TopicPublisherRegistry = struct {
    allocator: std.mem.Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    by_publisher_id: std.AutoHashMap(u64, *TopicPublisher),

    pub fn init(allocator: std.mem.Allocator) TopicPublisherRegistry {
        return .{
            .allocator = allocator,
            .by_publisher_id = std.AutoHashMap(u64, *TopicPublisher).init(allocator),
        };
    }

    pub fn deinit(self: *TopicPublisherRegistry) void {
        self.by_publisher_id.deinit();
        self.* = undefined;
    }

    fn lock(self: *TopicPublisherRegistry) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    fn unlock(self: *TopicPublisherRegistry) void {
        self.mutex.unlock();
    }

    pub fn register(self: *TopicPublisherRegistry, publisher: *TopicPublisher) !void {
        self.lock();
        defer self.unlock();
        try self.by_publisher_id.put(publisher.publisher_id, publisher);
    }

    pub fn unregister(self: *TopicPublisherRegistry, publisher: *const TopicPublisher) void {
        self.lock();
        defer self.unlock();
        _ = self.by_publisher_id.remove(publisher.publisher_id);
    }

    /// Control thread: route a TopicAckFeedback frame to its publisher, if it is
    /// one of ours. Returns true if a publisher accepted it.
    pub fn applyAckFeedback(
        self: *TopicPublisherRegistry,
        topic_id: u64,
        publisher_id: u64,
        leader_epoch: u64,
        replicated_hwm: u64,
        acked_seq: u64,
    ) bool {
        self.lock();
        defer self.unlock();
        const publisher = self.by_publisher_id.get(publisher_id) orelse return false;
        if (publisher.topic_id != topic_id) return false;
        publisher.applyAckFeedback(leader_epoch, publisher_id, replicated_hwm, acked_seq);
        return true;
    }

    /// Control thread: re-target every publisher on `topic_id` at a new leader.
    pub fn applyLeaderChanged(self: *TopicPublisherRegistry, topic_id: u64, leader_node_id: u8, leader_epoch: u64) void {
        self.lock();
        defer self.unlock();
        var it = self.by_publisher_id.valueIterator();
        while (it.next()) |p| {
            if (p.*.topic_id == topic_id) p.*.applyLeaderChanged(leader_node_id, leader_epoch);
        }
    }

    pub fn count(self: *TopicPublisherRegistry) u32 {
        self.lock();
        defer self.unlock();
        return self.by_publisher_id.count();
    }
};

// ── Tests ────────────────────────────────────────────────────────────────────

test "feedback reaches only the addressed publisher on a shared topic" {
    var reg = TopicPublisherRegistry.init(std.testing.allocator);
    defer reg.deinit();

    var a = TopicPublisher{ .topic_id = 7, .publisher_id = 0xA, .leader_node_id = 1, .leader_epoch = 1 };
    var b = TopicPublisher{ .topic_id = 7, .publisher_id = 0xB, .leader_node_id = 1, .leader_epoch = 1 };
    try reg.register(&a);
    try reg.register(&b);

    try std.testing.expect(reg.applyAckFeedback(7, 0xB, 1, 100, 3));
    try std.testing.expect(!a.isAcked(1));
    try std.testing.expect(b.isAcked(3));
    try std.testing.expect(!b.isAcked(4));

    // Wrong topic for that publisher id is ignored.
    try std.testing.expect(!reg.applyAckFeedback(8, 0xA, 1, 100, 9));
    try std.testing.expect(!a.isAcked(1));

    // Leader change reaches both publishers on the topic.
    reg.applyLeaderChanged(7, 2, 5);
    try std.testing.expectEqual(@as(u8, 2), a.leader_node_id);
    try std.testing.expectEqual(@as(u64, 5), b.leader_epoch);

    reg.unregister(&a);
    try std.testing.expectEqual(@as(u32, 1), reg.count());
    try std.testing.expect(!reg.applyAckFeedback(7, 0xA, 5, 100, 9));
}
