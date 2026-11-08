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
const TopicQueue = @import("topic_store.zig").TopicQueue;

/// Session lifecycle state reported engine -> control for observability and the
/// failover catch-up barrier.
pub const SessionState = enum(u8) {
    connecting = 0,
    handshaking = 1,
    live = 2,
    disconnected = 3,
};

/// Maximum inline payload of a `kv_leader_append` command. KV leader-authored
/// frames are at most 32 (header) + 80 (`KvStoreConfig`) = 112 bytes. Keeping it
/// inline means the control thread never allocates and the engine never reads
/// borrowed memory. Bumping this grows every `EngineCommand` slot (see the
/// size assertion below).
pub const kv_append_max_payload: usize = 160;

/// Outcome of a `kv_leader_append`.
pub const KvAppendResult = enum(u8) {
    /// Appended; `index` is the assigned topic index.
    ok = 0,
    /// The topic's queue is not open on this node.
    topic_missing = 1,
    /// The local queue is a replica (not the topic leader here).
    not_leader = 2,
    /// Leader, but the failover catch-up barrier has not cleared yet.
    barrier_active = 3,
    /// The queue rejected the append (I/O, payload empty or too large).
    append_failed = 4,
};

/// Commands posted control loop -> receiver topic engine.
pub const EngineCommand = union(enum) {
    /// Open (or refresh) a master queue for a topic this node now leads.
    open_master: struct { topic_id: TopicId, config: TopicConfig, epoch: u64 },
    /// Open a replica queue (full mesh: eager on TopicCreated).
    open_replica: struct { topic_id: TopicId, config: TopicConfig, epoch: u64 },
    /// Add a queue the control loop already created on disk
    /// (`TopicStore.openDetached`). Ownership passes to the engine, which adopts
    /// it, or destroys it if the topic is already open. The control loop opens
    /// on disk itself so local subscribers can open the files at once.
    adopt_queue: struct { queue: *TopicQueue },
    /// Close and release a topic queue.
    close_queue: struct { topic_id: TopicId },
    /// Update the fencing epoch for a topic (spec 08). Epochs only advance.
    set_epoch: struct { topic_id: TopicId, epoch: u64 },
    /// Advance the fencing epoch of every open queue (new leadership term).
    set_epoch_all: struct { epoch: u64 },
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
    /// KV only (docs/kv-store-spec.md §6.1): append an opaque leader-authored
    /// payload (store header, writer grant/release) to a topic this node leads.
    /// It takes the same sequencing and replication path as a publish: the
    /// engine appends to the master queue, and replication ships it to replicas.
    /// The engine answers with `EngineStatus.kv_append_done` carrying
    /// `correlation_id`. Because commands are FIFO, an append posted after
    /// `adopt_queue`/`promote_to_leader` of the same topic sees the open queue.
    kv_leader_append: KvLeaderAppend,
};

/// Payload of `EngineCommand.kv_leader_append`. Use `KvLeaderAppend.init`.
pub const KvLeaderAppend = struct {
    topic_id: TopicId,
    /// Caller-chosen id echoed in `kv_append_done`.
    correlation_id: u64,
    len: u16,
    payload: [kv_append_max_payload]u8,

    /// Copies `data` inline. Returns null if empty or longer than
    /// `kv_append_max_payload`.
    pub fn init(topic_id: TopicId, correlation_id: u64, data: []const u8) ?KvLeaderAppend {
        if (data.len == 0 or data.len > kv_append_max_payload) return null;
        var out: KvLeaderAppend = .{
            .topic_id = topic_id,
            .correlation_id = correlation_id,
            .len = @intCast(data.len),
            .payload = undefined,
        };
        @memcpy(out.payload[0..data.len], data);
        return out;
    }

    pub fn bytes(self: *const KvLeaderAppend) []const u8 {
        return self.payload[0..self.len];
    }
};

comptime {
    // The command ring holds `command_capacity` of these; keep slots small.
    std.debug.assert(@sizeOf(EngineCommand) <= 256);
}

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
    /// Result of an `EngineCommand.kv_leader_append`. `index` is the assigned
    /// topic index when `result == .ok`, otherwise 0.
    kv_append_done: struct {
        topic_id: TopicId,
        correlation_id: u64,
        result: KvAppendResult,
        index: u64,
    },
    /// A replica queue was reset (docs/kv-store-spec.md §8.4). The old local log
    /// is gone and a fresh empty replica was reopened (the sink restarts from
    /// the leader). Informational copy of the synchronous listener callbacks
    /// (`TopicEngine.addResetListener`); a consumer that holds a tailer on the
    /// topic must reopen it and rebuild derived state.
    replica_reset: struct { topic_id: TopicId, kv_store: bool, reset_seq: u64 },
    /// A producer's `replicate_once` publishes are replicated up to `acked_seq`
    /// (spec 05 §3.1). Carries the queue's epoch and replicated HWM at the time
    /// so the control loop can build feedback without reading receiver state.
    producer_acked: struct {
        topic_id: TopicId,
        publisher_id: u64,
        acked_seq: u64,
        leader_epoch: u64,
        replicated_hwm: u64,
    },
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
/// Larger than the status queue: the control loop issues several commands per
/// topic on registration and failover, and a dropped open/promote command
/// leaves a topic unusable.
pub const command_capacity: u32 = 1024;
pub const EngineCommandQueue = SpscQueue(EngineCommand, command_capacity);
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

test "kv_leader_append stays inline and bounded" {
    try std.testing.expect(KvLeaderAppend.init(1, 1, "") == null);
    var big: [kv_append_max_payload + 1]u8 = undefined;
    try std.testing.expect(KvLeaderAppend.init(1, 1, &big) == null);
    var q = EngineCommandQueue{};
    const a = KvLeaderAppend.init(77, 9, "frame-bytes").?;
    try std.testing.expect(q.enqueue(.{ .kv_leader_append = a }));
    const got = q.dequeue().?;
    try std.testing.expectEqual(@as(u64, 9), got.kv_leader_append.correlation_id);
    try std.testing.expectEqualStrings("frame-bytes", got.kv_leader_append.bytes());
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
