// SPDX-License-Identifier: Apache-2.0
//! TopicRegistry — control-loop-owned, cluster-replicated map of topic metadata
//! (spec 02). Lives on the control thread (the metadata authority); the receiver
//! engine learns of changes through the command queue (spec 02 §4).
//!
//! Ownership mirrors ServiceRegistry: the registry owns each topic's name slice
//! and frees it on removal. AP consistency: first-wins, deterministic topic_id.

const std = @import("std");
const topic_id_mod = @import("topic_id.zig");
const topic_config_mod = @import("topic_config.zig");

const TopicId = topic_id_mod.TopicId;
const TopicConfig = topic_config_mod.TopicConfig;

pub const LocalRole = enum(u8) { none = 0, leader = 1, replica = 2 };

pub const TopicRecord = struct {
    topic_id: TopicId,
    name: []const u8, // owned (duped)
    config: TopicConfig,
    leader_node_id: u8,
    leader_epoch: u64,
    created_ns: i64,
    // local-only fields (not propagated):
    local_role: LocalRole = .none,
    local_queue_open: bool = false,
    local_subscriber_count: u32 = 0,
};

pub const RegisterError = error{
    TopicIdCollision,
    TopicConfigMismatch,
} || std.mem.Allocator.Error;

pub const RegisterOutcome = enum { created, idempotent };

pub const TopicRegistry = struct {
    allocator: std.mem.Allocator,
    by_id: std.AutoHashMap(TopicId, TopicRecord),
    by_name: std.StringHashMap(TopicId),

    pub fn init(allocator: std.mem.Allocator) TopicRegistry {
        return .{
            .allocator = allocator,
            .by_id = std.AutoHashMap(TopicId, TopicRecord).init(allocator),
            .by_name = std.StringHashMap(TopicId).init(allocator),
        };
    }

    pub fn deinit(self: *TopicRegistry) void {
        var it = self.by_id.valueIterator();
        while (it.next()) |rec| self.allocator.free(@constCast(rec.name));
        self.by_id.deinit();
        self.by_name.deinit();
        self.* = undefined;
    }

    pub fn count(self: *const TopicRegistry) u32 {
        return self.by_id.count();
    }

    pub fn getById(self: *TopicRegistry, topic_id: TopicId) ?*TopicRecord {
        return self.by_id.getPtr(topic_id);
    }

    pub fn getByName(self: *TopicRegistry, name: []const u8) ?*TopicRecord {
        const id = self.by_name.get(name) orelse return null;
        return self.by_id.getPtr(id);
    }

    /// Insert or validate a topic (first-wins). Returns whether a new record was
    /// created or an identical one already existed. Mismatched name/config for an
    /// existing id, or a name reused under a different id, are rejected.
    pub fn upsert(
        self: *TopicRegistry,
        topic_id: TopicId,
        name: []const u8,
        config: TopicConfig,
        leader_node_id: u8,
        leader_epoch: u64,
        created_ns: i64,
    ) RegisterError!RegisterOutcome {
        if (self.by_id.getPtr(topic_id)) |existing| {
            // First-wins immutable config validation (spec 01).
            if (!std.mem.eql(u8, existing.name, name)) return error.TopicIdCollision;
            if (!existing.config.eqlForReplication(&config)) return error.TopicConfigMismatch;
            // Refresh leadership metadata (epoch may advance on failover).
            if (leader_epoch >= existing.leader_epoch) {
                existing.leader_node_id = leader_node_id;
                existing.leader_epoch = leader_epoch;
            }
            return .idempotent;
        }
        // Guard against a name reused under a different id.
        if (self.by_name.get(name)) |other_id| {
            if (other_id != topic_id) return error.TopicIdCollision;
        }

        const owned_name = try self.allocator.dupe(u8, name);
        errdefer self.allocator.free(owned_name);

        try self.by_id.put(topic_id, .{
            .topic_id = topic_id,
            .name = owned_name,
            .config = config,
            .leader_node_id = leader_node_id,
            .leader_epoch = leader_epoch,
            .created_ns = created_ns,
        });
        errdefer _ = self.by_id.remove(topic_id);
        try self.by_name.put(owned_name, topic_id);
        return .created;
    }

    pub fn remove(self: *TopicRegistry, topic_id: TopicId) void {
        if (self.by_id.fetchRemove(topic_id)) |kv| {
            _ = self.by_name.remove(kv.value.name);
            self.allocator.free(@constCast(kv.value.name));
        }
    }

    /// Set the leader for every topic (single topic-leader sequences all topics).
    pub fn setLeaderForAll(self: *TopicRegistry, leader_node_id: u8, leader_epoch: u64) void {
        var it = self.by_id.valueIterator();
        while (it.next()) |rec| {
            if (leader_epoch >= rec.leader_epoch) {
                rec.leader_node_id = leader_node_id;
                rec.leader_epoch = leader_epoch;
            }
        }
    }

    pub const Iterator = std.AutoHashMap(TopicId, TopicRecord).ValueIterator;
    pub fn iterator(self: *TopicRegistry) Iterator {
        return self.by_id.valueIterator();
    }
};

// ── Tests ────────────────────────────────────────────────────────────────────

const testing = std.testing;

test "registry insert by id and name; idempotent re-create" {
    var reg = TopicRegistry.init(testing.allocator);
    defer reg.deinit();

    const cfg = TopicConfig.fromName("FAST_DAILY", 7, false);
    const id = topic_id_mod.topicIdOf("orders");

    const o1 = try reg.upsert(id, "orders", cfg, 1, 1, 1000);
    try testing.expectEqual(RegisterOutcome.created, o1);
    try testing.expectEqual(@as(u32, 1), reg.count());
    try testing.expect(reg.getById(id) != null);
    try testing.expectEqualStrings("orders", reg.getByName("orders").?.name);

    const o2 = try reg.upsert(id, "orders", cfg, 1, 1, 2000);
    try testing.expectEqual(RegisterOutcome.idempotent, o2);
    try testing.expectEqual(@as(u32, 1), reg.count());
}

test "first-wins: config mismatch and id/name collisions rejected" {
    var reg = TopicRegistry.init(testing.allocator);
    defer reg.deinit();

    const cfg = TopicConfig.fromName("FAST_DAILY", 7, false);
    const cfg2 = TopicConfig.fromName("FAST_DAILY", 99, false); // different retention
    const id = topic_id_mod.topicIdOf("orders");

    _ = try reg.upsert(id, "orders", cfg, 1, 1, 1000);

    try testing.expectError(error.TopicConfigMismatch, reg.upsert(id, "orders", cfg2, 1, 1, 1000));
    // Same id, different name → collision.
    try testing.expectError(error.TopicIdCollision, reg.upsert(id, "other", cfg, 1, 1, 1000));
    // Same name, different id → collision.
    try testing.expectError(error.TopicIdCollision, reg.upsert(id +% 1, "orders", cfg, 1, 1, 1000));
}

test "leadership refresh advances with epoch; setLeaderForAll" {
    var reg = TopicRegistry.init(testing.allocator);
    defer reg.deinit();
    const cfg = TopicConfig.fromName("FAST_DAILY", 7, false);
    _ = try reg.upsert(1, "a", cfg, 1, 1, 0);
    _ = try reg.upsert(2, "b", cfg, 1, 1, 0);

    reg.setLeaderForAll(3, 5);
    try testing.expectEqual(@as(u8, 3), reg.getById(1).?.leader_node_id);
    try testing.expectEqual(@as(u64, 5), reg.getById(2).?.leader_epoch);

    // Older epoch does not regress leadership.
    _ = try reg.upsert(1, "a", cfg, 9, 2, 0);
    try testing.expectEqual(@as(u8, 3), reg.getById(1).?.leader_node_id);
}

test "remove frees name and clears both indexes" {
    var reg = TopicRegistry.init(testing.allocator);
    defer reg.deinit();
    const cfg = TopicConfig.fromName("FAST_DAILY", 7, false);
    _ = try reg.upsert(1, "a", cfg, 1, 1, 0);
    reg.remove(1);
    try testing.expectEqual(@as(u32, 0), reg.count());
    try testing.expect(reg.getById(1) == null);
    try testing.expect(reg.getByName("a") == null);
}
