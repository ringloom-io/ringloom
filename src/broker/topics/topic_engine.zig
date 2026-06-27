// SPDX-License-Identifier: Apache-2.0
//! TopicEngine — the receiver-loop topic state machine (spec 05).
//!
//! The receiver loop is the sole writer of every topic queue. The engine appends
//! publish frames directly (no writer thread), steps the ReplHub each iteration,
//! applies control-loop commands, and surfaces status (HWMs, session state) back
//! to the control loop. The separate prefetcher thread (spec 05 §4) keeps pages
//! resident; the engine never calls maintenancePoll.

const std = @import("std");
const topic_id_mod = @import("topic_id.zig");
const topic_config_mod = @import("topic_config.zig");
const topic_store = @import("topic_store.zig");
const repl_session = @import("repl_session.zig");
const commands = @import("topic_commands.zig");

const TopicId = topic_id_mod.TopicId;
const AckMode = topic_config_mod.AckMode;
const TopicStore = topic_store.TopicStore;
const ReplHub = repl_session.ReplHub;
const EngineCommandQueue = commands.EngineCommandQueue;
const EngineStatusQueue = commands.EngineStatusQueue;

const repl_budget: u32 = 64;

/// View of a single topic-publish frame, borrowed from the Aeron fragment buffer.
/// The payload is consumed (copied into the queue) before `onPublish` returns.
pub const PublishView = struct {
    topic_id: TopicId,
    leader_epoch: u64,
    correlation_id: i64,
    source_node: u8,
    source_service: u16,
    ack_mode: AckMode,
    payload: []const u8,
};

/// Lock-free observability counters (spec 10).
pub const TopicCounters = struct {
    publish_accepted: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    publish_dropped_unknown: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    publish_dropped_not_leader: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    publish_dropped_barrier: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    publish_dropped_stale_epoch: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    append_errors: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    fn inc(v: *std.atomic.Value(u64)) void {
        _ = v.fetchAdd(1, .monotonic);
    }
};

pub const TopicEngine = struct {
    allocator: std.mem.Allocator,
    store: *TopicStore,
    repl: *ReplHub,
    ctrl_in: *EngineCommandQueue,
    ctrl_out: *EngineStatusQueue,
    counters: TopicCounters = .{},

    /// Per-topic last replicated_hwm we reported, to throttle status spam.
    last_reported_repl_hwm: std.AutoHashMap(TopicId, u64),
    /// Failover barrier targets keyed by topic; cleared on barrier completion.
    barrier_targets: std.AutoHashMap(TopicId, u64),

    pub fn init(
        allocator: std.mem.Allocator,
        store: *TopicStore,
        repl: *ReplHub,
        ctrl_in: *EngineCommandQueue,
        ctrl_out: *EngineStatusQueue,
    ) TopicEngine {
        return .{
            .allocator = allocator,
            .store = store,
            .repl = repl,
            .ctrl_in = ctrl_in,
            .ctrl_out = ctrl_out,
            .last_reported_repl_hwm = std.AutoHashMap(TopicId, u64).init(allocator),
            .barrier_targets = std.AutoHashMap(TopicId, u64).init(allocator),
        };
    }

    pub fn deinit(self: *TopicEngine) void {
        self.last_reported_repl_hwm.deinit();
        self.barrier_targets.deinit();
        self.* = undefined;
    }

    /// Called from ReceiverEventLoop.doWork() each iteration.
    pub fn step(self: *TopicEngine) u32 {
        var w: u32 = 0;
        w += self.applyControlCommands();
        const summary = self.repl.stepAll(repl_budget);
        w += summary.work;
        w += self.reportStatus();
        w += self.checkBarriers();
        return w;
    }

    fn applyControlCommands(self: *TopicEngine) u32 {
        var w: u32 = 0;
        while (self.ctrl_in.dequeue()) |cmd| {
            w += 1;
            switch (cmd) {
                .open_master => |m| {
                    _ = self.store.openMaster(m.topic_id, m.config, m.epoch) catch {};
                },
                .open_replica => |m| {
                    _ = self.store.openReplica(m.topic_id, m.config, m.epoch) catch {};
                },
                .close_queue => |c| {
                    self.repl.closeTopic(c.topic_id);
                    self.store.close(c.topic_id);
                },
                .set_epoch => |e| {
                    if (self.store.get(e.topic_id)) |tq| tq.epoch = e.epoch;
                },
                .start_sink => |s| {
                    if (self.store.get(s.topic_id)) |tq| {
                        self.repl.ensureSink(s.topic_id, s.leader_node, tq.epoch) catch {};
                        _ = self.ctrl_out.enqueue(.{ .session_state = .{
                            .topic_id = s.topic_id,
                            .peer = s.leader_node,
                            .state = .connecting,
                        } });
                    }
                },
                .catch_up_barrier => |b| {
                    if (self.store.get(b.topic_id)) |tq| {
                        tq.accepting_writes = false;
                        self.repl.ensureSink(b.topic_id, b.from_node, tq.epoch) catch {};
                        self.barrier_targets.put(b.topic_id, b.target_index) catch {};
                    }
                },
                .promote_to_leader => |p| {
                    self.promote(p.topic_id);
                },
                .reset_replica => |r| {
                    self.resetReplica(r.topic_id, r.leader_node, r.config, r.epoch);
                },
            }
        }
        return w;
    }

    fn promote(self: *TopicEngine, topic_id: TopicId) void {
        if (self.store.get(topic_id)) |tq| {
            // Stop mirroring; become the sole writer/sequencer.
            self.repl.removeSink(topic_id);
            tq.role = .leader;
            tq.accepting_writes = true;
            _ = self.barrier_targets.remove(topic_id);
        }
    }

    /// Reset a replica queue due to sink-ahead divergence (spec 08 §4).
    /// Closes the local queue, removes the directory, recreates empty,
    /// and restarts the sink.
    fn resetReplica(self: *TopicEngine, topic_id: TopicId, leader_node: u8, config: topic_config_mod.TopicConfig, epoch: u64) void {
        // Close and destroy the current replica.
        self.repl.removeSink(topic_id);
        self.store.close(topic_id);
        // Recreate a fresh replica queue.
        _ = self.store.openReplica(topic_id, config, epoch) catch return;
        // Restart sink toward the leader.
        self.repl.ensureSink(topic_id, leader_node, epoch) catch {};
    }

    fn checkBarriers(self: *TopicEngine) u32 {
        var w: u32 = 0;
        var done: [256]TopicId = undefined;
        var n: usize = 0;
        var it = self.barrier_targets.iterator();
        while (it.next()) |kv| {
            const topic_id = kv.key_ptr.*;
            const target = kv.value_ptr.*;
            if (self.repl.sinks.get(topic_id)) |s| {
                if (s.sink.last_applied_index >= 0 and
                    @as(u64, @intCast(s.sink.last_applied_index)) >= target)
                {
                    _ = self.ctrl_out.enqueue(.{ .barrier_complete = .{ .topic_id = topic_id, .index = target } });
                    if (n < done.len) {
                        done[n] = topic_id;
                        n += 1;
                    }
                    w += 1;
                }
            }
        }
        for (done[0..n]) |t| _ = self.barrier_targets.remove(t);
        return w;
    }

    fn reportStatus(self: *TopicEngine) u32 {
        var w: u32 = 0;
        var it = self.store.openQueues();
        while (it.next()) |tqp| {
            const tq = tqp.*;
            if (tq.role != .leader) continue;
            const prev = self.last_reported_repl_hwm.get(tq.topic_id) orelse 0;
            if (tq.replicated_hwm != prev) {
                if (self.ctrl_out.enqueue(.{ .replicated_hwm = .{ .topic_id = tq.topic_id, .index = tq.replicated_hwm } })) {
                    self.last_reported_repl_hwm.put(tq.topic_id, tq.replicated_hwm) catch {};
                    w += 1;
                }
            }
        }
        return w;
    }

    /// Direct append path (leader), invoked inline by the receiver demux (spec 05 §3).
    pub fn onPublish(self: *TopicEngine, view: PublishView) void {
        const tq = self.store.get(view.topic_id) orelse {
            TopicCounters.inc(&self.counters.publish_dropped_unknown);
            return;
        };
        if (tq.role != .leader) {
            TopicCounters.inc(&self.counters.publish_dropped_not_leader);
            return;
        }
        if (!tq.accepting_writes) {
            TopicCounters.inc(&self.counters.publish_dropped_barrier);
            return;
        }
        if (view.leader_epoch != tq.epoch) {
            TopicCounters.inc(&self.counters.publish_dropped_stale_epoch);
            return;
        }
        const idx = tq.append(view.payload) catch {
            TopicCounters.inc(&self.counters.append_errors);
            return;
        };
        tq.hwm_index = idx;
        TopicCounters.inc(&self.counters.publish_accepted);
        _ = self.ctrl_out.enqueue(.{ .master_hwm = .{ .topic_id = view.topic_id, .index = idx } });

        if (view.ack_mode == .replicate_once) {
            // Single-node carve-out: nothing to replicate to → acked on append.
            if (self.repl.sourceCount() == 0 and self.replicaPeerCount(view.topic_id) == 0) {
                tq.replicated_hwm = idx;
            }
        }
    }

    fn replicaPeerCount(self: *TopicEngine, topic_id: TopicId) u32 {
        var c: u32 = 0;
        var it = self.repl.sources.keyIterator();
        while (it.next()) |k| {
            if (k.topic_id == topic_id) c += 1;
        }
        return c;
    }
};

// ── Tests ────────────────────────────────────────────────────────────────────

const testing = std.testing;
const TopicConfig = topic_config_mod.TopicConfig;
const channel = @import("repl_channel.zig");

fn nullPublisher() channel.Publisher {
    const Impl = struct {
        fn offer(_: *anyopaque, _: u8, _: []const u8) i64 {
            return 1;
        }
        fn isConn(_: *anyopaque, _: u8) bool {
            return false;
        }
        fn isBp(_: *anyopaque, _: u8) bool {
            return false;
        }
    };
    return .{ .ctx = undefined, .offerFn = Impl.offer, .isConnectedFn = Impl.isConn, .isBackPressuredFn = Impl.isBp };
}

fn tmpBase(allocator: std.mem.Allocator, tmp: *std.testing.TmpDir) ![]u8 {
    return std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path[0..]});
}

const Fixture = struct {
    store: TopicStore,
    repl: ReplHub,
    ci: EngineCommandQueue,
    co: EngineStatusQueue,
    engine: TopicEngine,
};

test "onPublish: unknown topic dropped" {
    const allocator = testing.allocator;
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const base = try tmpBase(allocator, &tmp);
    defer allocator.free(base);

    var store = TopicStore.init(allocator, base, 1 << 20, 1 << 20);
    defer store.deinit();
    var repl = ReplHub.init(allocator, 1, nullPublisher(), &store);
    defer repl.deinit();
    var ci = EngineCommandQueue{};
    var co = EngineStatusQueue{};
    var engine = TopicEngine.init(allocator, &store, &repl, &ci, &co);
    defer engine.deinit();

    engine.onPublish(.{
        .topic_id = 999,
        .leader_epoch = 1,
        .correlation_id = 0,
        .source_node = 1,
        .source_service = 1,
        .ack_mode = .fire_and_forget,
        .payload = "x",
    });
    try testing.expectEqual(@as(u64, 1), engine.counters.publish_dropped_unknown.load(.monotonic));
}

test "onPublish: leader accepts, replica/barrier/stale-epoch rejected" {
    const allocator = testing.allocator;
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const base = try tmpBase(allocator, &tmp);
    defer allocator.free(base);

    var store = TopicStore.init(allocator, base, 1 << 20, 1 << 20);
    defer store.deinit();
    var repl = ReplHub.init(allocator, 1, nullPublisher(), &store);
    defer repl.deinit();
    var ci = EngineCommandQueue{};
    var co = EngineStatusQueue{};
    var engine = TopicEngine.init(allocator, &store, &repl, &ci, &co);
    defer engine.deinit();

    const cfg = TopicConfig.fromName("FAST_DAILY", 4, false);

    // Replica role rejects publishes.
    _ = try store.openReplica(10, cfg, 1);
    engine.onPublish(.{ .topic_id = 10, .leader_epoch = 1, .correlation_id = 0, .source_node = 1, .source_service = 1, .ack_mode = .fire_and_forget, .payload = "x" });
    try testing.expectEqual(@as(u64, 1), engine.counters.publish_dropped_not_leader.load(.monotonic));

    // Master, but barrier not cleared.
    const m = try store.openMaster(20, cfg, 3);
    engine.onPublish(.{ .topic_id = 20, .leader_epoch = 3, .correlation_id = 0, .source_node = 1, .source_service = 1, .ack_mode = .fire_and_forget, .payload = "x" });
    try testing.expectEqual(@as(u64, 1), engine.counters.publish_dropped_barrier.load(.monotonic));

    // Clear barrier; stale epoch dropped.
    m.accepting_writes = true;
    engine.onPublish(.{ .topic_id = 20, .leader_epoch = 2, .correlation_id = 0, .source_node = 1, .source_service = 1, .ack_mode = .fire_and_forget, .payload = "x" });
    try testing.expectEqual(@as(u64, 1), engine.counters.publish_dropped_stale_epoch.load(.monotonic));

    // Correct epoch accepted.
    engine.onPublish(.{ .topic_id = 20, .leader_epoch = 3, .correlation_id = 0, .source_node = 1, .source_service = 1, .ack_mode = .fire_and_forget, .payload = "hello" });
    try testing.expectEqual(@as(u64, 1), engine.counters.publish_accepted.load(.monotonic));
}

test "replicate_once single-node acks on append (replicated_hwm tracks hwm)" {
    const allocator = testing.allocator;
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const base = try tmpBase(allocator, &tmp);
    defer allocator.free(base);

    var store = TopicStore.init(allocator, base, 1 << 20, 1 << 20);
    defer store.deinit();
    var repl = ReplHub.init(allocator, 1, nullPublisher(), &store);
    defer repl.deinit();
    var ci = EngineCommandQueue{};
    var co = EngineStatusQueue{};
    var engine = TopicEngine.init(allocator, &store, &repl, &ci, &co);
    defer engine.deinit();

    const cfg = TopicConfig.fromName("FAST_DAILY", 4, false);
    const m = try store.openMaster(30, cfg, 1);
    m.accepting_writes = true;

    engine.onPublish(.{ .topic_id = 30, .leader_epoch = 1, .correlation_id = 7, .source_node = 1, .source_service = 1, .ack_mode = .replicate_once, .payload = "ackme" });
    try testing.expectEqual(m.hwm_index, m.replicated_hwm);
}

test "control command open_master then promote enables writes" {
    const allocator = testing.allocator;
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const base = try tmpBase(allocator, &tmp);
    defer allocator.free(base);

    var store = TopicStore.init(allocator, base, 1 << 20, 1 << 20);
    defer store.deinit();
    var repl = ReplHub.init(allocator, 1, nullPublisher(), &store);
    defer repl.deinit();
    var ci = EngineCommandQueue{};
    var co = EngineStatusQueue{};
    var engine = TopicEngine.init(allocator, &store, &repl, &ci, &co);
    defer engine.deinit();

    const cfg = TopicConfig.fromName("FAST_DAILY", 4, false);
    try testing.expect(ci.enqueue(.{ .open_master = .{ .topic_id = 40, .config = cfg, .epoch = 1 } }));
    try testing.expect(ci.enqueue(.{ .promote_to_leader = .{ .topic_id = 40 } }));
    _ = engine.step();

    const tq = store.get(40).?;
    try testing.expect(tq.accepting_writes);
    try testing.expectEqual(topic_store.Role.leader, tq.role);
}
