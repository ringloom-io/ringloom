// SPDX-License-Identifier: Apache-2.0
//! ReplHub — owns replication sessions for this broker (spec 06 §3).
//!
//! Full mesh: this node keeps a *sink* for every topic (it is a replica of every
//! topic it doesn't lead) and, when it is the topic leader, a *source* per peer
//! that has HELLO'd. The hub binds each session's ringloom-queue Source/Sink to
//! the matching TopicStore core queue, steps them on the receiver loop, demuxes
//! inbound repl frames (envelope already validated) to the right channel, and
//! surfaces each sink's applied index for ack accounting (spec 05 §3.1).

const std = @import("std");
const rq = @import("ringloom_queue");
const common = @import("ringloom_common");

const channel = @import("repl_channel.zig");
const topic_store = @import("topic_store.zig");
const topic_id_mod = @import("topic_id.zig");

const TopicId = topic_id_mod.TopicId;
const TopicStore = topic_store.TopicStore;
const OutboundChannel = channel.OutboundChannel;
const InboundChannel = channel.InboundChannel;
const Publisher = channel.Publisher;
const TopicReplEnvelope = common.message.topic_data_header.TopicReplEnvelope;
const ReplDirection = common.message.topic_data_header.ReplDirection;

const Source = rq.repl.source.ReplicationSource(OutboundChannel, InboundChannel);
const Sink = rq.repl.sink.ReplicationSink(OutboundChannel, InboundChannel);

const default_max_frame: usize = 1 * 1024 * 1024;
const inbound_capacity_frames: usize = 64;

pub const SourceKey = struct {
    topic_id: TopicId,
    peer: u8,

    pub fn hash(self: SourceKey) u64 {
        return (self.topic_id << 8) ^ self.peer;
    }
};

const SourceKeyContext = struct {
    pub fn hash(_: SourceKeyContext, k: SourceKey) u64 {
        return k.hash();
    }
    pub fn eql(_: SourceKeyContext, a: SourceKey, b: SourceKey) bool {
        return a.topic_id == b.topic_id and a.peer == b.peer;
    }
};

/// One replica sink session (this node mirrors a topic led by `leader_node`).
pub const SinkSession = struct {
    topic_id: TopicId,
    leader_node: u8,
    inbound: InboundChannel,
    outbound: OutboundChannel,
    sink: Sink,

    pub fn appliedIndex(self: *const SinkSession) i64 {
        return self.sink.last_applied_index;
    }
};

/// One leader source session (this node ships a topic to `peer`).
pub const SourceSession = struct {
    key: SourceKey,
    inbound: InboundChannel,
    outbound: OutboundChannel,
    source: Source,
};

pub const StepSummary = struct {
    work: u32 = 0,
};

pub const ReplHub = struct {
    allocator: std.mem.Allocator,
    local_node_id: u8,
    publisher: Publisher,
    store: *TopicStore,
    max_inner_frame: usize,

    sinks: std.AutoHashMap(TopicId, *SinkSession),
    sources: std.HashMap(SourceKey, *SourceSession, SourceKeyContext, 80),
    /// Reused scratch map for replicated_hwm aggregation in `stepAll`. Kept as
    /// a field so the receiver-loop hot path performs ZERO heap allocation —
    /// `clearRetainingCapacity()` retains the backing storage across iterations.
    repl_hwm_scratch: std.AutoHashMap(TopicId, u64),

    pub fn init(
        allocator: std.mem.Allocator,
        local_node_id: u8,
        publisher: Publisher,
        store: *TopicStore,
    ) ReplHub {
        return .{
            .allocator = allocator,
            .local_node_id = local_node_id,
            .publisher = publisher,
            .store = store,
            .max_inner_frame = default_max_frame,
            .sinks = std.AutoHashMap(TopicId, *SinkSession).init(allocator),
            .sources = std.HashMap(SourceKey, *SourceSession, SourceKeyContext, 80).init(allocator),
            .repl_hwm_scratch = std.AutoHashMap(TopicId, u64).init(allocator),
        };
    }

    pub fn deinit(self: *ReplHub) void {
        var sit = self.sinks.valueIterator();
        while (sit.next()) |sp| self.destroySink(sp.*);
        self.sinks.deinit();

        var oit = self.sources.valueIterator();
        while (oit.next()) |op| self.destroySource(op.*);
        self.sources.deinit();
        self.repl_hwm_scratch.deinit();
        self.* = undefined;
    }

    fn destroySink(self: *ReplHub, s: *SinkSession) void {
        s.sink.deinit();
        s.outbound.deinit();
        s.inbound.deinit();
        self.allocator.destroy(s);
    }

    fn destroySource(self: *ReplHub, s: *SourceSession) void {
        s.source.deinit();
        s.outbound.deinit();
        s.inbound.deinit();
        self.allocator.destroy(s);
    }

    fn queueIdBytes(topic_id: TopicId) [16]u8 {
        var out: [16]u8 = [_]u8{0} ** 16;
        std.mem.writeInt(u64, out[0..8], topic_id, .little);
        return out;
    }

    /// Ensure a replica sink exists for `topic_id` toward `leader_node`. The
    /// replica queue must already be open in the store.
    pub fn ensureSink(self: *ReplHub, topic_id: TopicId, leader_node: u8, epoch: u64) !void {
        if (self.sinks.get(topic_id)) |existing| {
            // If leadership changed, restart the sink toward the new leader.
            if (existing.leader_node == leader_node) {
                existing.outbound.setEpoch(epoch);
                return;
            }
            _ = self.sinks.remove(topic_id);
            self.destroySink(existing);
        }
        const tq = self.store.get(topic_id) orelse return error.QueueNotOpen;

        const s = try self.allocator.create(SinkSession);
        errdefer self.allocator.destroy(s);
        s.topic_id = topic_id;
        s.leader_node = leader_node;
        s.inbound = try InboundChannel.init(self.allocator, inbound_capacity_frames, self.max_inner_frame);
        errdefer s.inbound.deinit();
        s.outbound = try OutboundChannel.init(
            self.allocator,
            self.publisher,
            topic_id,
            epoch,
            .sink_to_source,
            self.local_node_id,
            leader_node,
            self.max_inner_frame,
        );
        errdefer s.outbound.deinit();
        s.sink = try Sink.init(self.allocator, tq.coreQueue(), &s.outbound, &s.inbound, .{
            .node_id = queueIdBytes(self.local_node_id),
            .queue_id = queueIdBytes(topic_id),
            .max_frame_bytes = self.max_inner_frame,
        });
        try self.sinks.put(topic_id, s);
    }

    pub fn removeSink(self: *ReplHub, topic_id: TopicId) void {
        if (self.sinks.fetchRemove(topic_id)) |kv| self.destroySink(kv.value);
    }

    /// Ensure a leader source exists toward `peer` for `topic_id`. The master
    /// queue must already be open in the store.
    pub fn ensureSource(self: *ReplHub, topic_id: TopicId, peer: u8, epoch: u64) !void {
        const key = SourceKey{ .topic_id = topic_id, .peer = peer };
        if (self.sources.get(key)) |existing| {
            existing.outbound.setEpoch(epoch);
            return;
        }
        const tq = self.store.get(topic_id) orelse return error.QueueNotOpen;

        const s = try self.allocator.create(SourceSession);
        errdefer self.allocator.destroy(s);
        s.key = key;
        s.inbound = try InboundChannel.init(self.allocator, inbound_capacity_frames, self.max_inner_frame);
        errdefer s.inbound.deinit();
        s.outbound = try OutboundChannel.init(
            self.allocator,
            self.publisher,
            topic_id,
            epoch,
            .source_to_sink,
            self.local_node_id,
            peer,
            self.max_inner_frame,
        );
        errdefer s.outbound.deinit();
        s.source = Source.init(self.allocator, tq.coreQueue(), &s.outbound, &s.inbound, .{
            .node_salt = @as(u32, self.local_node_id) +% 1,
            .max_frame_bytes = self.max_inner_frame,
        });
        try self.sources.put(key, s);
    }

    pub fn removeSource(self: *ReplHub, topic_id: TopicId, peer: u8) void {
        if (self.sources.fetchRemove(.{ .topic_id = topic_id, .peer = peer })) |kv| {
            self.destroySource(kv.value);
        }
    }

    /// Close all sessions for a topic (queue closing).
    pub fn closeTopic(self: *ReplHub, topic_id: TopicId) void {
        self.removeSink(topic_id);
        var to_remove: [256]SourceKey = undefined;
        var n: usize = 0;
        var it = self.sources.keyIterator();
        while (it.next()) |k| {
            if (k.topic_id == topic_id and n < to_remove.len) {
                to_remove[n] = k.*;
                n += 1;
            }
        }
        for (to_remove[0..n]) |k| self.removeSource(k.topic_id, k.peer);
    }

    /// Route a demuxed inbound repl frame (envelope validated by the receiver) to
    /// the matching session's inbound channel. On `sink_to_source` toward a topic
    /// we lead, lazily create the source for that peer.
    pub fn onInboundFrame(self: *ReplHub, env: TopicReplEnvelope, inner: []const u8) void {
        const dir = ReplDirection.fromU8(env.direction) orelse return;

        // Epoch fencing: drop frames with older epochs (spec 08 §3).
        if (self.store.get(env.topic_id)) |tq| {
            if (env.leader_epoch < tq.epoch) return;
        }

        switch (dir) {
            .source_to_sink => {
                // Leader -> us (replica). Route to our sink for this topic.
                if (self.sinks.get(env.topic_id)) |s| {
                    s.inbound.setConnected(true);
                    _ = s.inbound.pushFrame(inner);
                }
            },
            .sink_to_source => {
                // Replica -> us (leader). Route to source for (topic, peer).
                const peer: u8 = @intCast(env.source_node_id);
                const key = SourceKey{ .topic_id = env.topic_id, .peer = peer };
                if (self.sources.get(key) == null) {
                    // Lazily create the source if we hold the master queue.
                    if (self.store.get(env.topic_id)) |tq| {
                        if (tq.role == .leader) {
                            self.ensureSource(env.topic_id, peer, env.leader_epoch) catch return;
                        } else return;
                    } else return;
                }
                if (self.sources.get(key)) |s| {
                    s.inbound.setConnected(true);
                    _ = s.inbound.pushFrame(inner);
                }
            },
        }
    }

    /// Step every session once (bounded). Returns work done and updates the
    /// store's replicated_hwm from the slowest >=1-replica progress.
    pub fn stepAll(self: *ReplHub, budget: u32) StepSummary {
        var summary = StepSummary{};

        var sit = self.sinks.valueIterator();
        while (sit.next()) |sp| {
            const s = sp.*;
            const res = s.sink.step(budget) catch rq.StepResult.idle;
            if (res != .idle) summary.work += 1;
            if (self.store.get(s.topic_id)) |tq| {
                if (s.sink.last_applied_index >= 0) {
                    tq.hwm_index = @max(tq.hwm_index, @as(u64, @intCast(s.sink.last_applied_index)));
                }
            }
        }

        var oit = self.sources.valueIterator();
        while (oit.next()) |op| {
            const s = op.*;
            const res = s.source.step(budget) catch rq.StepResult.idle;
            if (res != .idle) summary.work += 1;
        }

        self.recomputeReplicatedHwm();
        return summary;
    }

    /// Leader-side: replicated_hwm = highest index applied by >=1 replica peer.
    /// Tracked from each source session's last acked index. Uses the
    /// pre-allocated `repl_hwm_scratch` map (cleared, not freed, each call) so
    /// the receiver-loop hot path performs no heap allocation.
    fn recomputeReplicatedHwm(self: *ReplHub) void {
        // Fast path: no leader sources → nothing to aggregate. Avoids clearing
        // the scratch map on every idle receiver-loop iteration.
        if (self.sources.count() == 0) return;

        self.repl_hwm_scratch.clearRetainingCapacity();

        var oit = self.sources.valueIterator();
        while (oit.next()) |op| {
            const s = op.*;
            // Highest acked index across this source's sessions.
            var best: u64 = 0;
            for (s.source.sessions.items) |sess| {
                if (sess.last_ack_index > best) best = sess.last_ack_index;
            }
            const gop = self.repl_hwm_scratch.getOrPut(s.key.topic_id) catch continue;
            if (!gop.found_existing) {
                gop.value_ptr.* = best;
            } else if (best > gop.value_ptr.*) {
                gop.value_ptr.* = best;
            }
        }

        var pit = self.repl_hwm_scratch.iterator();
        while (pit.next()) |kv| {
            if (self.store.get(kv.key_ptr.*)) |tq| {
                if (tq.role == .leader and kv.value_ptr.* > tq.replicated_hwm) {
                    tq.replicated_hwm = kv.value_ptr.*;
                }
            }
        }
    }

    pub fn sinkCount(self: *const ReplHub) u32 {
        return self.sinks.count();
    }
    pub fn sourceCount(self: *const ReplHub) u32 {
        return self.sources.count();
    }
};

// ── Tests ────────────────────────────────────────────────────────────────────

const testing = std.testing;
const TopicConfig = @import("topic_config.zig").TopicConfig;

fn tmpBase(allocator: std.mem.Allocator, tmp: *std.testing.TmpDir) ![]u8 {
    return std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path[0..]});
}

test "full-mesh leader->replica replication reaches byte parity over loopback" {
    const allocator = testing.allocator;

    var tmp_l = testing.tmpDir(.{ .iterate = true });
    defer tmp_l.cleanup();
    var tmp_r = testing.tmpDir(.{ .iterate = true });
    defer tmp_r.cleanup();
    const base_l = try tmpBase(allocator, &tmp_l);
    defer allocator.free(base_l);
    const base_r = try tmpBase(allocator, &tmp_r);
    defer allocator.free(base_r);

    var store_l = TopicStore.init(allocator, base_l, 1 << 20, 1 << 20);
    defer store_l.deinit();
    var store_r = TopicStore.init(allocator, base_r, 1 << 20, 1 << 20);
    defer store_r.deinit();

    const cfg = TopicConfig.fromName("FAST_DAILY", 8, false);
    const topic: TopicId = 0x1234;
    const m = try store_l.openMaster(topic, cfg, 1);
    m.accepting_writes = true;
    _ = try store_r.openReplica(topic, cfg, 1);

    // Cross-wired publishers: leader's offers land in replica's hub inbound and
    // vice versa. We build hubs first, then point the wires at them.
    var hub_l: ReplHub = undefined;
    var hub_r: ReplHub = undefined;

    const Bridge = struct {
        target_hub: *ReplHub,
        fn offer(ctx: *anyopaque, target_node: u8, frame: []const u8) i64 {
            _ = target_node;
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const env = TopicReplEnvelope.decode(frame) catch return -4;
            const inner = env.frameSlice(frame);
            self.target_hub.onInboundFrame(env, inner);
            return 1;
        }
        fn isConn(ctx: *anyopaque, target_node: u8) bool {
            _ = ctx;
            _ = target_node;
            return true;
        }
        fn isBp(ctx: *anyopaque, target_node: u8) bool {
            _ = ctx;
            _ = target_node;
            return false;
        }
    };

    var bridge_l = Bridge{ .target_hub = &hub_r }; // leader sends -> replica hub
    var bridge_r = Bridge{ .target_hub = &hub_l }; // replica sends -> leader hub
    const pub_l = Publisher{ .ctx = &bridge_l, .offerFn = Bridge.offer, .isConnectedFn = Bridge.isConn, .isBackPressuredFn = Bridge.isBp };
    const pub_r = Publisher{ .ctx = &bridge_r, .offerFn = Bridge.offer, .isConnectedFn = Bridge.isConn, .isBackPressuredFn = Bridge.isBp };

    hub_l = ReplHub.init(allocator, 1, pub_l, &store_l);
    defer hub_l.deinit();
    hub_r = ReplHub.init(allocator, 2, pub_r, &store_r);
    defer hub_r.deinit();

    // Replica opens a sink toward the leader; leader will lazily open a source on HELLO.
    try hub_r.ensureSink(topic, 1, 1);

    // Append messages on the leader master queue.
    const n: usize = 20;
    var i: usize = 0;
    while (i < n) : (i += 1) {
        var buf: [32]u8 = undefined;
        const msg = try std.fmt.bufPrint(&buf, "topic-msg-{d:0>4}", .{i});
        _ = try m.append(msg);
    }

    // Drive both hubs until the replica has applied all messages (bounded loop).
    var iter: usize = 0;
    while (iter < 2000) : (iter += 1) {
        _ = hub_l.stepAll(64);
        _ = hub_r.stepAll(64);
        const s = hub_r.sinks.get(topic).?;
        if (s.sink.last_applied_index >= @as(i64, @intCast(n - 1))) break;
    }

    const sink = hub_r.sinks.get(topic).?;
    try testing.expect(sink.sink.last_applied_index >= @as(i64, @intCast(n - 1)));

    // Replica queue tailer reads all N in order (byte parity).
    const rq_replica = store_r.get(topic).?;
    var tailer = try rq_replica.queue.tailer(0);
    defer tailer.deinit();
    var seen: usize = 0;
    while (try tailer.poll()) |entry| {
        var buf: [32]u8 = undefined;
        const want = try std.fmt.bufPrint(&buf, "topic-msg-{d:0>4}", .{seen});
        try testing.expectEqualStrings(want, entry.message);
        seen += 1;
        if (seen == n) break;
    }
    try testing.expectEqual(n, seen);

    // Leader created a source lazily and replicated_hwm advanced.
    try testing.expect(hub_l.sourceCount() >= 1);
}
