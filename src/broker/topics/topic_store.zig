// SPDX-License-Identifier: Apache-2.0
//! TopicStore — owns the per-topic ringloom-queue handles on this broker.
//!
//! The receiver loop is the sole writer of every topic queue (master queues when
//! this node is the topic leader, replica queues otherwise). Queues are opened
//! with `spawn_helper_threads = false` so the broker's single prefetcher thread
//! (spec 05 §4) owns all maintenance work.

const std = @import("std");
const rq = @import("ringloom_queue");

const topic_id_mod = @import("topic_id.zig");
const topic_config_mod = @import("topic_config.zig");

const TopicId = topic_id_mod.TopicId;
const TopicConfig = topic_config_mod.TopicConfig;

const RawQueue = rq.Queue([]const u8);

pub const TopicStoreError = error{
    UnknownRollScheme,
    QueueAlreadyOpen,
} || std.mem.Allocator.Error;

pub const Role = enum { leader, replica };

/// A single open topic queue plus the leader/replica bookkeeping the engine needs.
pub const TopicQueue = struct {
    topic_id: TopicId,
    role: Role,
    queue: RawQueue,
    dir: []u8, // owned
    epoch: u64,
    hwm_index: u64 = 0,
    replicated_hwm: u64 = 0,
    /// Leader only: false until the failover catch-up barrier clears (spec 08).
    /// Replicas never accept publishes, so this stays false for them.
    accepting_writes: bool = false,

    /// CoreQueue pointer for binding replication Source/Sink (spec 06).
    pub fn coreQueue(self: *TopicQueue) *rq.CoreQueue {
        return self.queue.inner;
    }

    /// Leader append on the receiver thread. Returns the assigned total-order index.
    pub fn append(self: *TopicQueue, payload: []const u8) !u64 {
        const idx = try self.queue.append(payload);
        if (idx > self.hwm_index) self.hwm_index = idx;
        return idx;
    }

    pub fn maintenancePoll(self: *TopicQueue, budget: u32) !rq.StepResult {
        return self.queue.maintenancePoll(budget);
    }
};

pub const TopicStore = struct {
    allocator: std.mem.Allocator,
    base_dir: []const u8, // borrowed; caller keeps it alive
    queues: std.AutoHashMap(TopicId, *TopicQueue),
    write_runway_bytes: u64,
    read_runway_bytes: u64,

    pub fn init(
        allocator: std.mem.Allocator,
        base_dir: []const u8,
        write_runway_bytes: u64,
        read_runway_bytes: u64,
    ) TopicStore {
        return .{
            .allocator = allocator,
            .base_dir = base_dir,
            .queues = std.AutoHashMap(TopicId, *TopicQueue).init(allocator),
            .write_runway_bytes = write_runway_bytes,
            .read_runway_bytes = read_runway_bytes,
        };
    }

    pub fn deinit(self: *TopicStore) void {
        var it = self.queues.valueIterator();
        while (it.next()) |tqp| {
            const tq = tqp.*;
            tq.queue.deinit();
            self.allocator.free(tq.dir);
            self.allocator.destroy(tq);
        }
        self.queues.deinit();
        self.* = undefined;
    }

    pub fn get(self: *TopicStore, topic_id: TopicId) ?*TopicQueue {
        return self.queues.get(topic_id);
    }

    pub fn count(self: *const TopicStore) u32 {
        return self.queues.count();
    }

    /// Builds `<base_dir>/t_<topicIdHex>` into an owned slice.
    fn buildDir(self: *TopicStore, topic_id: TopicId) ![]u8 {
        return std.fmt.allocPrint(self.allocator, "{s}/t_{x:0>16}", .{ self.base_dir, topic_id });
    }

    fn openInternal(
        self: *TopicStore,
        topic_id: TopicId,
        config: TopicConfig,
        epoch: u64,
        role: Role,
        create: bool,
    ) TopicStoreError!*TopicQueue {
        if (self.queues.contains(topic_id)) return error.QueueAlreadyOpen;

        const scheme = rq.roll.findSchemeByName(config.rollSchemeName()) orelse
            return error.UnknownRollScheme;

        const dir = try self.buildDir(topic_id);
        errdefer self.allocator.free(dir);

        // Replicas are written via writeAtIndex (out of the normal sequential
        // append order), so write-runway pre-touch provides no benefit and races
        // the prefetcher thread against the sink's cycle rolls (use-after-unmap
        // segfault). Disable the write runway for replicas; keep read prefetch.
        const is_replica = role == .replica;
        const queue = RawQueue.open(.{
            .dir = dir,
            .roll_scheme = scheme,
            .create = create,
            .use_huge_pages = config.useHugePages(),
            .enable_prefetcher = true,
            .prefetch_runway_bytes = if (is_replica) 0 else self.write_runway_bytes,
            .read_prefetch_runway_bytes = self.read_runway_bytes,
            .enable_cleaner = true,
            .spawn_helper_threads = false,
            .retention_cycles = config.retention_cycles,
            .allocator = self.allocator,
        }, rq.codec.RawCodec) catch |err| {
            // Map ringloom-queue open errors to allocator/unknown without leaking.
            return switch (err) {
                error.OutOfMemory => error.OutOfMemory,
                else => error.UnknownRollScheme,
            };
        };

        const tq = try self.allocator.create(TopicQueue);
        errdefer self.allocator.destroy(tq);
        tq.* = .{
            .topic_id = topic_id,
            .role = role,
            .queue = queue,
            .dir = dir,
            .epoch = epoch,
        };

        try self.queues.put(topic_id, tq);
        return tq;
    }

    /// Opens (creating if needed) a master queue for a topic this node leads.
    pub fn openMaster(self: *TopicStore, topic_id: TopicId, config: TopicConfig, epoch: u64) TopicStoreError!*TopicQueue {
        return self.openInternal(topic_id, config, epoch, .leader, true);
    }

    /// Opens (creating if needed) a replica queue. Full mesh: eager on TopicCreated.
    pub fn openReplica(self: *TopicStore, topic_id: TopicId, config: TopicConfig, epoch: u64) TopicStoreError!*TopicQueue {
        return self.openInternal(topic_id, config, epoch, .replica, true);
    }

    pub fn close(self: *TopicStore, topic_id: TopicId) void {
        if (self.queues.fetchRemove(topic_id)) |kv| {
            const tq = kv.value;
            tq.queue.deinit();
            self.allocator.free(tq.dir);
            self.allocator.destroy(tq);
        }
    }

    pub const Iterator = std.AutoHashMap(TopicId, *TopicQueue).ValueIterator;

    /// Iterates all open queues (used by the prefetcher thread, read-only).
    pub fn openQueues(self: *TopicStore) Iterator {
        return self.queues.valueIterator();
    }
};

test "store opens master and replica queues with distinct dirs" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const base = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path[0..]});
    defer allocator.free(base);

    var store = TopicStore.init(allocator, base, 1 << 20, 1 << 20);
    defer store.deinit();

    const cfg = TopicConfig.fromName("FAST_DAILY", 4, false);
    const m = try store.openMaster(1001, cfg, 1);
    try std.testing.expectEqual(Role.leader, m.role);
    try std.testing.expect(!m.accepting_writes);

    const r = try store.openReplica(2002, cfg, 1);
    try std.testing.expectEqual(Role.replica, r.role);

    try std.testing.expectEqual(@as(u32, 2), store.count());
    try std.testing.expect(store.get(1001) != null);
    try std.testing.expectError(error.QueueAlreadyOpen, store.openMaster(1001, cfg, 1));
}

test "leader append advances hwm and is durable/replayable in order" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const base = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path[0..]});
    defer allocator.free(base);

    var store = TopicStore.init(allocator, base, 1 << 20, 1 << 20);
    defer store.deinit();

    const cfg = TopicConfig.fromName("FAST_DAILY", 4, false);
    const m = try store.openMaster(7, cfg, 1);

    var last: u64 = 0;
    var i: usize = 0;
    while (i < 5) : (i += 1) {
        var buf: [16]u8 = undefined;
        const msg = try std.fmt.bufPrint(&buf, "msg-{d}", .{i});
        const idx = try m.append(msg);
        if (i > 0) try std.testing.expect(idx > last);
        last = idx;
    }
    try std.testing.expectEqual(last, m.hwm_index);

    // Replay reproduces append order.
    var tailer = try m.queue.tailer(0);
    defer tailer.deinit();
    var seen: usize = 0;
    while (try tailer.poll()) |entry| {
        var buf: [16]u8 = undefined;
        const want = try std.fmt.bufPrint(&buf, "msg-{d}", .{seen});
        try std.testing.expectEqualStrings(want, entry.message);
        seen += 1;
        if (seen == 5) break;
    }
    try std.testing.expectEqual(@as(usize, 5), seen);
}

test "unknown roll scheme rejected" {
    const allocator = std.testing.allocator;
    var tmp = std.testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const base = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path[0..]});
    defer allocator.free(base);

    var store = TopicStore.init(allocator, base, 1 << 20, 1 << 20);
    defer store.deinit();

    var cfg = TopicConfig.fromName("FAST_DAILY", 4, false);
    @memset(&cfg.roll_scheme_name, 0);
    @memcpy(cfg.roll_scheme_name[0.."NOPE".len], "NOPE");
    try std.testing.expectError(error.UnknownRollScheme, store.openMaster(5, cfg, 1));
}
