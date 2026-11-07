// SPDX-License-Identifier: Apache-2.0
//! TopicEngine — the receiver-loop topic state machine (spec 05).
//!
//! The receiver loop is the sole writer of every topic queue. The engine appends
//! publish frames directly (no writer thread), steps the ReplHub each iteration,
//! applies control-loop commands, and surfaces status (HWMs, session state) back
//! to the control loop. The separate prefetcher thread (spec 05 §4) keeps pages
//! resident; the engine never calls maintenancePoll.
//!
//! The engine (receiver thread) is the only code that opens or closes store
//! queues; the control loop asks for that through `ctrl_in` commands.

const std = @import("std");
const topic_id_mod = @import("topic_id.zig");
const topic_config_mod = @import("topic_config.zig");
const topic_store = @import("topic_store.zig");
const repl_session = @import("repl_session.zig");
const commands = @import("topic_commands.zig");
const topic_ack_tracker = @import("topic_ack_tracker.zig");

const TopicId = topic_id_mod.TopicId;
const AckMode = topic_config_mod.AckMode;
const TopicStore = topic_store.TopicStore;
const ReplHub = repl_session.ReplHub;
const EngineCommandQueue = commands.EngineCommandQueue;
const EngineStatusQueue = commands.EngineStatusQueue;
const AckTracker = topic_ack_tracker.AckTracker;

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
    /// Producer identity and per-producer sequence (TopicPublishHeader); used
    /// for replicate_once ack accounting. publish_seq == 0 means "not tracked".
    publisher_id: u64 = 0,
    publish_seq: u64 = 0,
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

    /// Failover barrier targets keyed by topic; cleared on barrier completion.
    barrier_targets: std.AutoHashMap(TopicId, u64),
    /// Per-producer replicate_once ack accounting (spec 05 §3.1).
    acks: AckTracker,

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
            .barrier_targets = std.AutoHashMap(TopicId, u64).init(allocator),
            .acks = AckTracker.init(allocator),
        };
    }

    pub fn deinit(self: *TopicEngine) void {
        self.barrier_targets.deinit();
        self.acks.deinit();
        self.* = undefined;
    }

    /// Called from ReceiverEventLoop.doWork() each iteration.
    pub fn step(self: *TopicEngine) u32 {
        var w: u32 = 0;
        w += self.applyControlCommands();
        const summary = self.repl.stepAll(repl_budget);
        w += summary.work;
        w += self.reportAcks();
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
                .adopt_queue => |a| {
                    self.store.adopt(a.queue) catch self.store.destroyDetached(a.queue);
                },
                .close_queue => |c| {
                    self.repl.closeTopic(c.topic_id);
                    self.acks.removeTopic(c.topic_id);
                    self.store.close(c.topic_id);
                },
                .set_epoch => |e| {
                    // Epochs only move forward (spec 08 fencing).
                    if (self.store.get(e.topic_id)) |tq| {
                        if (e.epoch > tq.epoch) tq.epoch = e.epoch;
                    }
                },
                .set_epoch_all => |e| {
                    var it = self.store.openQueues();
                    while (it.next()) |tqp| {
                        if (e.epoch > tqp.*.epoch) tqp.*.epoch = e.epoch;
                    }
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
        self.acks.removeTopic(topic_id);
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

    /// Hands each producer's newly replicated `acked_seq` to the control loop,
    /// which turns it into throttled TopicAckFeedback (spec 05 §3.1).
    fn reportAcks(self: *TopicEngine) u32 {
        var w: u32 = 0;
        var it = self.acks.iterator();
        while (it.next()) |kv| {
            const key = kv.key_ptr.*;
            const p = kv.value_ptr.*;
            const tq = self.store.get(key.topic_id) orelse continue;
            if (tq.role != .leader) continue;
            _ = p.advance(tq.replicated_hwm);
            if (!p.needsReport()) continue;
            const sent = self.ctrl_out.enqueue(.{ .producer_acked = .{
                .topic_id = key.topic_id,
                .publisher_id = key.publisher_id,
                .acked_seq = p.acked_seq,
                .leader_epoch = tq.epoch,
                .replicated_hwm = tq.replicated_hwm,
            } });
            // Queue full: retry on the next step.
            if (!sent) break;
            p.reported_seq = p.acked_seq;
            w += 1;
        }
        return w;
    }

    /// Direct append path (leader), invoked inline by the receiver demux (spec 05 §3).
    pub fn onPublish(self: *TopicEngine, view: PublishView) void {
        // The control loop opens, promotes and re-epochs queues by command, then
        // answers the producer. The producer's first publish can reach us before
        // this loop's next step(), so apply any pending commands first.
        if (self.ctrl_in.size() > 0) _ = self.applyControlCommands();
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

        if (view.ack_mode == .replicate_once) {
            if (view.publish_seq != 0) {
                self.acks.note(.{ .topic_id = view.topic_id, .publisher_id = view.publisher_id }, view.publish_seq, idx);
            }
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

fn drainProducerAcks(co: *EngineStatusQueue, out: *std.AutoHashMap(u64, u64)) !void {
    while (co.dequeue()) |st| switch (st) {
        .producer_acked => |a| try out.put(a.publisher_id, a.acked_seq),
        else => {},
    };
}

test "replicate_once acks are tracked per producer, not per topic" {
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
    const m = try store.openMaster(50, cfg, 1);
    m.accepting_writes = true;

    const pub_a: u64 = 0xAAAA;
    const pub_b: u64 = 0xBBBB;
    const Pub = struct {
        fn send(e: *TopicEngine, publisher: u64, seq: u64) void {
            e.onPublish(.{ .topic_id = 50, .leader_epoch = 1, .correlation_id = 0, .source_node = 1, .source_service = 1, .ack_mode = .replicate_once, .publisher_id = publisher, .publish_seq = seq, .payload = "m" });
        }
    };

    // Arrival order: A1, B1, B2, A2.
    Pub.send(&engine, pub_a, 1);
    const idx_a1 = m.hwm_index;
    Pub.send(&engine, pub_b, 1);
    Pub.send(&engine, pub_b, 2);
    const idx_b2 = m.hwm_index;
    Pub.send(&engine, pub_a, 2);
    try testing.expect(idx_b2 > idx_a1);

    // Pretend replicas have applied only through B2 (multi-node behaviour).
    m.replicated_hwm = idx_b2;
    _ = engine.step();

    var acked = std.AutoHashMap(u64, u64).init(allocator);
    defer acked.deinit();
    try drainProducerAcks(&co, &acked);
    // Three topic messages are replicated, but only one of A's. The old
    // topic-wide count would have reported 3 and acked A's second publish.
    try testing.expectEqual(@as(u64, 1), acked.get(pub_a).?);
    try testing.expectEqual(@as(u64, 2), acked.get(pub_b).?);

    // Once A2 is replicated, A advances and B is not re-reported.
    m.replicated_hwm = m.hwm_index;
    _ = engine.step();
    acked.clearRetainingCapacity();
    try drainProducerAcks(&co, &acked);
    try testing.expectEqual(@as(u64, 2), acked.get(pub_a).?);
    try testing.expect(acked.get(pub_b) == null);
}

test "publish applies pending open/promote commands before lookup" {
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
    try testing.expect(ci.enqueue(.{ .open_master = .{ .topic_id = 60, .config = cfg, .epoch = 1 } }));
    try testing.expect(ci.enqueue(.{ .promote_to_leader = .{ .topic_id = 60 } }));
    try testing.expect(ci.enqueue(.{ .set_epoch_all = .{ .epoch = 4 } }));

    // No step() yet: the publish itself must apply the commands.
    engine.onPublish(.{ .topic_id = 60, .leader_epoch = 4, .correlation_id = 0, .source_node = 1, .source_service = 1, .ack_mode = .fire_and_forget, .payload = "first" });
    try testing.expectEqual(@as(u64, 1), engine.counters.publish_accepted.load(.monotonic));
    try testing.expectEqual(@as(u64, 4), store.get(60).?.epoch);

    // Epochs never move backwards.
    try testing.expect(ci.enqueue(.{ .set_epoch = .{ .topic_id = 60, .epoch = 2 } }));
    _ = engine.step();
    try testing.expectEqual(@as(u64, 4), store.get(60).?.epoch);
}
