// SPDX-License-Identifier: Apache-2.0
//! POD command channels between the control loop (registry authority) and the
//! receiver-loop topic engine (sole writer of every topic queue).
//!
//! Two directions, each a bounded SPSC ring of POD values (ids + config copies,
//! never borrowed slices) so there are no cross-thread lifetime hazards. Mirrors
//! the `AdminCommandQueue` SPSC pattern used elsewhere in the broker.

const std = @import("std");
const topic_id_mod = @import("topic_id.zig");
const topic_config_mod = @import("topic_config.zig");

const TopicId = topic_id_mod.TopicId;
const TopicConfig = topic_config_mod.TopicConfig;

/// Session lifecycle state reported engine -> control for observability and the
/// failover catch-up barrier.
pub const SessionState = enum(u8) {
    connecting = 0,
    handshaking = 1,
    live = 2,
    disconnected = 3,
};

/// Commands posted control loop -> receiver topic engine.
pub const EngineCommand = union(enum) {
    /// Open (or refresh) a master queue for a topic this node now leads.
    open_master: struct { topic_id: TopicId, config: TopicConfig, epoch: u64 },
    /// Open a replica queue (full mesh: eager on TopicCreated).
    open_replica: struct { topic_id: TopicId, config: TopicConfig, epoch: u64 },
    /// Close and release a topic queue.
    close_queue: struct { topic_id: TopicId },
    /// Update the fencing epoch for a topic (spec 08).
    set_epoch: struct { topic_id: TopicId, epoch: u64 },
    /// Start (or restart) a replica sink toward the given leader node.
    start_sink: struct { topic_id: TopicId, leader_node: u8 },
    /// Failover catch-up barrier: pull up to target_index from from_node before
    /// promotion (spec 08). accepting_writes stays false until cleared.
    catch_up_barrier: struct { topic_id: TopicId, from_node: u8, target_index: u64 },
    /// Promote a (caught-up) master to accept writes (clears the barrier).
    promote_to_leader: struct { topic_id: TopicId },
    /// Reset a replica queue (sink-ahead / divergence recovery, spec 08 §4).
    /// Closes the local queue, removes the dir, recreates empty, and restarts sink.
    reset_replica: struct { topic_id: TopicId, leader_node: u8, config: TopicConfig, epoch: u64 },
};

/// Status posted receiver topic engine -> control loop.
pub const EngineStatus = union(enum) {
    /// Master append high-water-mark advanced.
    master_hwm: struct { topic_id: TopicId, index: u64 },
    /// A replica applied up to index (sink progress).
    replica_applied: struct { topic_id: TopicId, index: u64 },
    /// Highest index applied by >=1 replica (drives replicate_once acks, spec 04).
    replicated_hwm: struct { topic_id: TopicId, index: u64 },
    /// Replication session state change (peer + state).
    session_state: struct { topic_id: TopicId, peer: u8, state: SessionState },
    /// Barrier completion: master has caught up and is ready for promotion.
    barrier_complete: struct { topic_id: TopicId, index: u64 },
};

/// Bounded SPSC ring of POD `T`. Single producer, single consumer, no locks.
pub fn SpscQueue(comptime T: type, comptime capacity: u32) type {
    return struct {
        buffer: [capacity]T = undefined,
        write_pos: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
        read_pos: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

        const Self = @This();
        const mask: u32 = capacity - 1;

        comptime {
            std.debug.assert(capacity > 0 and (capacity & (capacity - 1)) == 0);
        }

        pub fn enqueue(self: *Self, item: T) bool {
            const wp = self.write_pos.load(.acquire);
            const rp = self.read_pos.load(.acquire);
            if (wp - rp >= capacity) return false;
            self.buffer[wp & mask] = item;
            self.write_pos.store(wp + 1, .release);
            return true;
        }

        pub fn dequeue(self: *Self) ?T {
            const rp = self.read_pos.load(.acquire);
            const wp = self.write_pos.load(.acquire);
            if (rp == wp) return null;
            const item = self.buffer[rp & mask];
            self.read_pos.store(rp + 1, .release);
            return item;
        }

        pub fn size(self: *const Self) u32 {
            return self.write_pos.load(.acquire) -% self.read_pos.load(.acquire);
        }
    };
}

pub const default_capacity: u32 = 256;
pub const EngineCommandQueue = SpscQueue(EngineCommand, default_capacity);
pub const EngineStatusQueue = SpscQueue(EngineStatus, default_capacity);

test "engine command queue POD round-trip" {
    var q = EngineCommandQueue{};
    const cfg = TopicConfig.fromName("FAST_DAILY", 7, false);
    try std.testing.expect(q.enqueue(.{ .open_master = .{ .topic_id = 42, .config = cfg, .epoch = 3 } }));
    try std.testing.expect(q.enqueue(.{ .close_queue = .{ .topic_id = 42 } }));
    try std.testing.expectEqual(@as(u32, 2), q.size());

    const a = q.dequeue().?;
    switch (a) {
        .open_master => |m| {
            try std.testing.expectEqual(@as(TopicId, 42), m.topic_id);
            try std.testing.expectEqual(@as(u64, 3), m.epoch);
        },
        else => return error.WrongVariant,
    }
    const b = q.dequeue().?;
    try std.testing.expect(b == .close_queue);
    try std.testing.expect(q.dequeue() == null);
}

test "spsc queue reports full via backpressure" {
    var q = SpscQueue(u32, 2){};
    try std.testing.expect(q.enqueue(1));
    try std.testing.expect(q.enqueue(2));
    try std.testing.expect(!q.enqueue(3)); // full
    try std.testing.expectEqual(@as(u32, 1), q.dequeue().?);
    try std.testing.expect(q.enqueue(3));
}

test "engine status round-trip" {
    var q = EngineStatusQueue{};
    try std.testing.expect(q.enqueue(.{ .replicated_hwm = .{ .topic_id = 9, .index = 100 } }));
    const s = q.dequeue().?;
    switch (s) {
        .replicated_hwm => |h| try std.testing.expectEqual(@as(u64, 100), h.index),
        else => return error.WrongVariant,
    }
}
