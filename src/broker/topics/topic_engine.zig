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
    /// `kv_leader_append` commands that were appended / rejected (not leader,
    /// barrier active, topic missing, append error).
    kv_leader_appends: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    kv_leader_append_rejected: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    replica_resets: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),

    fn inc(v: *std.atomic.Value(u64)) void {
        _ = v.fetchAdd(1, .monotonic);
    }
};

pub const max_reset_listeners: usize = 4;

/// Synchronous replica-reset listener, invoked ON THE RECEIVER THREAD.
///
/// `resetReplica` (spec 08 §4) closes the local replica queue, deletes its
/// directory and reopens an empty one. A consumer that reads the topic's local
/// log (the KV applier, docs/kv-store-spec.md §8.4) must react:
///
///  - `on_pre_reset` runs BEFORE the queue is closed and its directory deleted.
///    The consumer must stop using any reader it holds on the topic. Tailers
///    opened through `topic_local_reader.LocalTailer` own an independent
///    ringloom-queue handle, so memory stays valid even if this is missed (the
///    unlinked files stay mapped), but the reader would keep returning the OLD
///    history forever. Never share the broker's own `TopicQueue`/tailers with
///    other threads: `Queue.deinit` destroys every tailer registered on it.
///  - `on_post_reset` runs AFTER the empty replica exists and the sink was
///    restarted. The consumer reopens its reader at the start of the log and
///    rebuilds derived state once the replica catches up again.
///
/// Both callbacks must be short and non-blocking (they stall the receiver
/// loop): set an atomic flag or enqueue to the consumer's own SPSC queue.
pub const ResetListener = struct {
    ctx: ?*anyopaque = null,
    on_pre_reset: *const fn (ctx: ?*anyopaque, topic_id: TopicId, kv_store: bool) void,
    on_post_reset: *const fn (ctx: ?*anyopaque, topic_id: TopicId, kv_store: bool, reset_seq: u64) void,
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

    /// Replica-reset listeners; see `ResetListener`. Register before the
    /// receiver loop starts (or from the receiver thread); slots are never
    /// removed.
    reset_listeners: [max_reset_listeners]ResetListener = undefined,
    reset_listener_count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),
    /// Monotonic count of replica resets, passed to `on_post_reset`.
    reset_seq: u64 = 0,
    /// A `kv_leader_append` held back because `ctrl_out` was full, so its
    /// result could not be reported. Retried first on the next command pass.
    deferred_kv: ?commands.KvLeaderAppend = null,
    /// Live topics-enabled peer brokers, published by the control thread
    /// (`TopicSubsystem.refreshTopicPeers`). The replicate_once single-node
    /// carve-out applies only when this is zero: a peer that has not yet
    /// attached a replica session for a new topic must not be mistaken for
    /// "no replica exists", otherwise acked data is lost on failover.
    live_topic_peers: std.atomic.Value(u32) = .init(0),

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

    /// Registers a replica-reset listener. Returns false when all
    /// `max_reset_listeners` slots are taken. Single registrar at a time.
    pub fn addResetListener(self: *TopicEngine, listener: ResetListener) bool {
        const n = self.reset_listener_count.load(.monotonic);
        if (n >= max_reset_listeners) return false;
        self.reset_listeners[n] = listener;
        self.reset_listener_count.store(n + 1, .release);
        return true;
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
        if (self.deferred_kv) |d| {
            if (!self.execKvLeaderAppend(d)) return w;
            self.deferred_kv = null;
            w += 1;
        }
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
                .kv_leader_append => |a| {
                    if (!self.execKvLeaderAppend(a)) {
                        // Status ring full: keep the command, retry next pass,
                        // and preserve ordering by not running later commands.
                        self.deferred_kv = a;
                        return w;
                    }
                },
            }
        }
        return w;
    }

    /// Leader-local append (docs/kv-store-spec.md §6.1). Same path as
    /// `onPublish`: `TopicQueue.append` assigns the index, advances the HWM, and
    /// the Source ships the entry to replicas. Returns false (nothing done) only
    /// when the status ring has no room for the result.
    fn execKvLeaderAppend(self: *TopicEngine, a: commands.KvLeaderAppend) bool {
        if (self.ctrl_out.size() >= commands.default_capacity) return false;
        var result: commands.KvAppendResult = .ok;
        var index: u64 = 0;
        if (self.store.get(a.topic_id)) |tq| {
            if (tq.role != .leader) {
                result = .not_leader;
            } else if (!tq.accepting_writes) {
                result = .barrier_active;
            } else if (tq.append(a.bytes())) |idx| {
                index = idx;
                // Single-node carve-out, as for replicate_once publishes: with
                // nothing to replicate to, the entry is replicated on append.
                if (self.isSingleNode(a.topic_id)) {
                    tq.replicated_hwm = idx;
                }
            } else |_| {
                result = .append_failed;
            }
        } else {
            result = .topic_missing;
        }
        if (result == .ok) {
            TopicCounters.inc(&self.counters.kv_leader_appends);
        } else {
            TopicCounters.inc(&self.counters.kv_leader_append_rejected);
        }
        _ = self.ctrl_out.enqueue(.{ .kv_append_done = .{
            .topic_id = a.topic_id,
            .correlation_id = a.correlation_id,
            .result = result,
            .index = index,
        } });
        return true;
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
    /// Notifies listeners (`ResetListener`), closes the local queue, deletes
    /// its directory, recreates an empty replica, restarts the sink, then
    /// notifies listeners again and posts `EngineStatus.replica_reset`.
    fn resetReplica(self: *TopicEngine, topic_id: TopicId, leader_node: u8, config: topic_config_mod.TopicConfig, epoch: u64) void {
        const kv = config.isKvStore();
        const n_listeners = self.reset_listener_count.load(.acquire);
        for (self.reset_listeners[0..n_listeners]) |l| l.on_pre_reset(l.ctx, topic_id, kv);

        // Remember where the files live before the queue handle goes away.
        var dir_copy: ?[]u8 = null;
        var io: ?std.Io = null;
        if (self.store.get(topic_id)) |tq| {
            dir_copy = self.allocator.dupe(u8, tq.dir) catch null;
            io = tq.queue.inner.io;
        }
        defer if (dir_copy) |d| self.allocator.free(d);

        // Close and destroy the current replica.
        self.repl.removeSink(topic_id);
        self.acks.removeTopic(topic_id);
        self.store.close(topic_id);
        if (dir_copy) |d| {
            std.Io.Dir.cwd().deleteTree(io.?, d) catch {};
        }

        // Recreate a fresh replica queue.
        _ = self.store.openReplica(topic_id, config, epoch) catch return;
        // Restart sink toward the leader.
        self.repl.ensureSink(topic_id, leader_node, epoch) catch {};

        self.reset_seq += 1;
        TopicCounters.inc(&self.counters.replica_resets);
        for (self.reset_listeners[0..n_listeners]) |l| l.on_post_reset(l.ctx, topic_id, kv, self.reset_seq);
        _ = self.ctrl_out.enqueue(.{ .replica_reset = .{ .topic_id = topic_id, .kv_store = kv, .reset_seq = self.reset_seq } });
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
            if (self.isSingleNode(view.topic_id)) {
                tq.replicated_hwm = idx;
            }
        }
    }

    /// Single-node carve-out (spec 05 §3.1): no topics-enabled peer is alive
    /// and no replica session exists, so an append counts as replicated.
    fn isSingleNode(self: *TopicEngine, topic_id: TopicId) bool {
        return self.live_topic_peers.load(.acquire) == 0 and
            self.repl.sourceCount() == 0 and self.replicaPeerCount(topic_id) == 0;
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
    return allocator.print(".zig-cache/tmp/{s}", .{tmp.sub_path[0..]});
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

test "replicate_once is not acked on append while a live peer has not attached a replica yet" {
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
    const m = try store.openMaster(31, cfg, 1);
    m.accepting_writes = true;
    // A topics-enabled peer is alive but has not learned the topic yet
    // (no replica source session): the entry is not replicated anywhere.
    engine.live_topic_peers.store(1, .release);
    engine.onPublish(.{ .topic_id = 31, .leader_epoch = 1, .correlation_id = 0, .source_node = 1, .source_service = 1, .ack_mode = .replicate_once, .payload = "pending" });
    try testing.expect(m.hwm_index > m.replicated_hwm);
    // The peer died: back to the single-node carve-out.
    engine.live_topic_peers.store(0, .release);
    engine.onPublish(.{ .topic_id = 31, .leader_epoch = 1, .correlation_id = 0, .source_node = 1, .source_service = 1, .ack_mode = .replicate_once, .payload = "alone" });
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

fn drainKvAppend(co: *EngineStatusQueue) ?@FieldType(commands.EngineStatus, "kv_append_done") {
    var found: ?@FieldType(commands.EngineStatus, "kv_append_done") = null;
    while (co.dequeue()) |st| switch (st) {
        .kv_append_done => |d| found = d,
        else => {},
    };
    return found;
}

test "kv_leader_append: round trip assigns an index, replays in order" {
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

    const cfg = TopicConfig.fromName("FAST_DAILY", 0, false).withKvStore(true);
    try testing.expect(ci.enqueue(.{ .open_master = .{ .topic_id = 70, .config = cfg, .epoch = 1 } }));
    try testing.expect(ci.enqueue(.{ .promote_to_leader = .{ .topic_id = 70 } }));
    // Posted in the same batch as open/promote: FIFO makes the queue visible.
    try testing.expect(ci.enqueue(.{ .kv_leader_append = commands.KvLeaderAppend.init(70, 11, "store-header").? }));
    try testing.expect(ci.enqueue(.{ .kv_leader_append = commands.KvLeaderAppend.init(70, 12, "writer-grant").? }));
    _ = engine.step();

    const tq = store.get(70).?;
    var first: ?u64 = null;
    var second: ?u64 = null;
    while (co.dequeue()) |st| switch (st) {
        .kv_append_done => |d| {
            try testing.expectEqual(commands.KvAppendResult.ok, d.result);
            if (d.correlation_id == 11) first = d.index;
            if (d.correlation_id == 12) second = d.index;
        },
        else => {},
    };
    try testing.expect(first != null and second != null);
    try testing.expect(second.? > first.?);
    try testing.expectEqual(second.?, tq.hwm_index);
    try testing.expectEqual(@as(u64, 2), engine.counters.kv_leader_appends.load(.monotonic));

    // The payload is a normal log entry: a tailer sees it, and a normal publish
    // after it is sequenced behind it.
    engine.onPublish(.{ .topic_id = 70, .leader_epoch = 1, .correlation_id = 0, .source_node = 1, .source_service = 1, .ack_mode = .fire_and_forget, .payload = "data" });
    var tailer = try tq.queue.tailer(0);
    defer tailer.deinit();
    const e1 = (try tailer.poll()).?;
    try testing.expectEqual(first.?, e1.index);
    try testing.expectEqualStrings("store-header", e1.message);
    try testing.expectEqualStrings("writer-grant", (try tailer.poll()).?.message);
    try testing.expectEqualStrings("data", (try tailer.poll()).?.message);
}

test "kv_leader_append: failures are reported, not appended" {
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

    const cfg = TopicConfig.fromName("FAST_DAILY", 0, false).withKvStore(true);
    _ = try store.openReplica(80, cfg, 1);
    const barrier = try store.openMaster(81, cfg, 1); // accepting_writes == false

    const Case = struct { topic: TopicId, want: commands.KvAppendResult };
    const cases = [_]Case{
        .{ .topic = 999, .want = .topic_missing },
        .{ .topic = 80, .want = .not_leader },
        .{ .topic = 81, .want = .barrier_active },
    };
    for (cases, 0..) |c, i| {
        try testing.expect(ci.enqueue(.{ .kv_leader_append = commands.KvLeaderAppend.init(c.topic, i, "x").? }));
        _ = engine.step();
        const d = drainKvAppend(&co).?;
        try testing.expectEqual(c.want, d.result);
        try testing.expectEqual(@as(u64, i), d.correlation_id);
        try testing.expectEqual(@as(u64, 0), d.index);
    }
    try testing.expectEqual(@as(u64, 0), barrier.hwm_index);
    try testing.expectEqual(@as(u64, 3), engine.counters.kv_leader_append_rejected.load(.monotonic));

    // A full status ring defers the command (nothing appended, nothing lost).
    barrier.accepting_writes = true;
    while (co.size() < commands.default_capacity) _ = co.enqueue(.{ .master_hwm = .{ .topic_id = 1, .index = 1 } });
    try testing.expect(ci.enqueue(.{ .kv_leader_append = commands.KvLeaderAppend.init(81, 42, "late").? }));
    _ = engine.step();
    try testing.expect(engine.deferred_kv != null);
    try testing.expectEqual(@as(u64, 0), barrier.hwm_index);
    while (co.dequeue()) |_| {}
    _ = engine.step();
    try testing.expect(engine.deferred_kv == null);
    const d = drainKvAppend(&co).?;
    try testing.expectEqual(@as(u64, 42), d.correlation_id);
    try testing.expectEqual(commands.KvAppendResult.ok, d.result);
}

const ResetRecorder = struct {
    pre: u32 = 0,
    post: u32 = 0,
    last_seq: u64 = 0,
    kv: bool = false,
    /// Value of `pre` seen by `on_post_reset`: proves pre ran first.
    pre_at_post: u32 = 0,

    fn onPre(ctx: ?*anyopaque, _: TopicId, kv: bool) void {
        const self: *ResetRecorder = @ptrCast(@alignCast(ctx.?));
        self.pre += 1;
        self.kv = kv;
    }
    fn onPost(ctx: ?*anyopaque, _: TopicId, _: bool, seq: u64) void {
        const self: *ResetRecorder = @ptrCast(@alignCast(ctx.?));
        self.post += 1;
        self.pre_at_post = self.pre;
        self.last_seq = seq;
    }
};

test "reset_replica: notifies listeners and status, and really empties the replica" {
    const allocator = testing.allocator;
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const base = try tmpBase(allocator, &tmp);
    defer allocator.free(base);

    var store = TopicStore.init(allocator, base, 1 << 20, 1 << 20);
    defer store.deinit();
    var repl = ReplHub.init(allocator, 2, nullPublisher(), &store);
    defer repl.deinit();
    var ci = EngineCommandQueue{};
    var co = EngineStatusQueue{};
    var engine = TopicEngine.init(allocator, &store, &repl, &ci, &co);
    defer engine.deinit();

    var rec = ResetRecorder{};
    try testing.expect(engine.addResetListener(.{ .ctx = &rec, .on_pre_reset = ResetRecorder.onPre, .on_post_reset = ResetRecorder.onPost }));

    const cfg = TopicConfig.fromName("FAST_DAILY", 0, false).withKvStore(true);
    const replica = try store.openReplica(90, cfg, 1);
    _ = try replica.append("old-history-1");
    _ = try replica.append("old-history-2");

    try testing.expect(ci.enqueue(.{ .reset_replica = .{ .topic_id = 90, .leader_node = 1, .config = cfg, .epoch = 2 } }));
    _ = engine.step();

    try testing.expectEqual(@as(u32, 1), rec.pre);
    try testing.expectEqual(@as(u32, 1), rec.post);
    try testing.expectEqual(@as(u32, 1), rec.pre_at_post);
    try testing.expectEqual(@as(u64, 1), rec.last_seq);
    try testing.expect(rec.kv);
    try testing.expectEqual(@as(u64, 1), engine.counters.replica_resets.load(.monotonic));

    var saw_status = false;
    while (co.dequeue()) |st| switch (st) {
        .replica_reset => |r| {
            saw_status = true;
            try testing.expectEqual(@as(TopicId, 90), r.topic_id);
            try testing.expect(r.kv_store);
            try testing.expectEqual(@as(u64, 1), r.reset_seq);
        },
        else => {},
    };
    try testing.expect(saw_status);

    // A fresh, empty replica is open again with the sink restarted.
    const fresh = store.get(90).?;
    try testing.expectEqual(topic_store.Role.replica, fresh.role);
    try testing.expectEqual(@as(u64, 2), fresh.epoch);
    try testing.expectEqual(@as(u64, 0), fresh.hwm_index);
    try testing.expect(repl.sinkCount() >= 1);
    var tailer = try fresh.queue.tailer(0);
    defer tailer.deinit();
    try testing.expect((try tailer.poll()) == null);
}
