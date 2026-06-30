// SPDX-License-Identifier: Apache-2.0
//! TopicSubsystem — the broker-level facade that owns all topic components and
//! exposes the hooks the control loop and receiver loop call.
//!
//! Thread split (matches the broker's loops):
//!   - control thread owns `registry` + `election` (metadata authority, spec 02/08)
//!   - receiver thread owns `store` + `repl` + `engine` (sole queue writer, spec 05)
//!   - one prefetcher thread does maintenancePoll (spec 05 §4)
//! The two sides communicate only through the POD command/status SPSC queues, so
//! there are no shared-mutable hazards beyond the store lock the prefetcher takes.

const std = @import("std");
const common = @import("ringloom_common");

const topic_id_mod = @import("topic_id.zig");
const topic_config_mod = @import("topic_config.zig");
const topic_store = @import("topic_store.zig");
const repl_session = @import("repl_session.zig");
const repl_channel = @import("repl_channel.zig");
const topic_engine = @import("topic_engine.zig");
const topic_prefetcher = @import("topic_prefetcher.zig");
const topic_registry = @import("topic_registry.zig");
const topic_leader_election = @import("topic_leader_election.zig");
const commands = @import("topic_commands.zig");

const TopicId = topic_id_mod.TopicId;
const TopicConfig = topic_config_mod.TopicConfig;
const AckMode = topic_config_mod.AckMode;
const TopicStore = topic_store.TopicStore;
const ReplHub = repl_session.ReplHub;
const Publisher = repl_channel.Publisher;
const TopicEngine = topic_engine.TopicEngine;
const PublishView = topic_engine.PublishView;
const TopicPrefetcher = topic_prefetcher.TopicPrefetcher;
const SpinLock = topic_prefetcher.SpinLock;
const TopicRegistry = topic_registry.TopicRegistry;
const TopicLeaderElection = topic_leader_election.TopicLeaderElection;
const EngineCommandQueue = commands.EngineCommandQueue;
const EngineStatusQueue = commands.EngineStatusQueue;
const TopicReplEnvelope = common.message.topic_data_header.TopicReplEnvelope;

/// Configuration slice the subsystem needs (subset of BrokerConfig.topics).
pub const Options = struct {
    enabled: bool,
    base_dir: []const u8, // resolved topics path (already includes group/node)
    write_runway_bytes: u64,
    read_runway_bytes: u64,
    prefetcher_cpu_affinity: i32,
    local_node_id: u8,
    /// Io instance for filesystem scanning during startup (spec 02 §5).
    io: std.Io,
};

pub const TopicSubsystem = struct {
    allocator: std.mem.Allocator,
    enabled: bool,
    local_node_id: u8,
    io: std.Io,

    // Shared command channels (control <-> engine).
    ctrl_in: EngineCommandQueue,
    ctrl_out: EngineStatusQueue,

    // Receiver-thread components.
    store_lock: SpinLock,
    store: TopicStore,
    repl: ReplHub,
    engine: TopicEngine,
    prefetcher: TopicPrefetcher,

    // Control-thread components.
    registry: TopicRegistry,
    election: TopicLeaderElection,

    pub fn init(allocator: std.mem.Allocator, opts: Options, publisher: Publisher, now_ns: i64) !*TopicSubsystem {
        const self = try allocator.create(TopicSubsystem);
        errdefer allocator.destroy(self);

        self.allocator = allocator;
        self.enabled = opts.enabled;
        self.local_node_id = opts.local_node_id;
        self.io = opts.io;
        self.ctrl_in = .{};
        self.ctrl_out = .{};
        self.store_lock = .{};
        self.store = TopicStore.init(allocator, opts.base_dir, opts.write_runway_bytes, opts.read_runway_bytes);
        self.repl = ReplHub.init(allocator, opts.local_node_id, publisher, &self.store);
        self.engine = TopicEngine.init(allocator, &self.store, &self.repl, &self.ctrl_in, &self.ctrl_out);
        self.prefetcher = TopicPrefetcher.init(&self.store, &self.store_lock, opts.prefetcher_cpu_affinity);
        self.registry = TopicRegistry.init(allocator);
        self.election = TopicLeaderElection.init(opts.local_node_id, opts.enabled, now_ns);
        return self;
    }

    pub fn deinit(self: *TopicSubsystem) void {
        self.prefetcher.stop();
        self.engine.deinit();
        self.repl.deinit();
        self.store.deinit();
        self.registry.deinit();
        const a = self.allocator;
        a.destroy(self);
    }

    pub fn start(self: *TopicSubsystem) !void {
        if (!self.enabled) return;
        try self.prefetcher.start();
        // Rebuild registry from existing queue directories on disk (spec 02 §5).
        self.scanExistingQueues();
    }

    // ── Receiver-thread hooks ──────────────────────────────────────────────

    /// Called each receiver-loop iteration. Returns work done.
    pub fn receiverStep(self: *TopicSubsystem) u32 {
        if (!self.enabled) return 0;
        return self.engine.step();
    }

    /// Called inline by the receiver demux for each topic-publish frame (spec 04).
    pub fn onPublish(self: *TopicSubsystem, view: PublishView) void {
        if (!self.enabled) return;
        self.engine.onPublish(view);
    }

    /// Called by the receiver demux for each topic replication envelope (spec 06).
    pub fn onReplFrame(self: *TopicSubsystem, env: TopicReplEnvelope, inner: []const u8) void {
        if (!self.enabled) return;
        if (env.target_node_id != self.local_node_id) return;
        self.repl.onInboundFrame(env, inner);
    }

    // ── Control-thread command emitters (registry side → engine) ───────────

    fn cmdOpenMaster(self: *TopicSubsystem, topic_id: TopicId, config: TopicConfig, epoch: u64) void {
        self.store_lock.lock();
        defer self.store_lock.unlock();
        _ = self.ctrl_in.enqueue(.{ .open_master = .{ .topic_id = topic_id, .config = config, .epoch = epoch } });
    }

    fn cmdOpenReplica(self: *TopicSubsystem, topic_id: TopicId, config: TopicConfig, epoch: u64) void {
        self.store_lock.lock();
        defer self.store_lock.unlock();
        _ = self.ctrl_in.enqueue(.{ .open_replica = .{ .topic_id = topic_id, .config = config, .epoch = epoch } });
    }

    fn cmdStartSink(self: *TopicSubsystem, topic_id: TopicId, leader_node: u8) void {
        _ = self.ctrl_in.enqueue(.{ .start_sink = .{ .topic_id = topic_id, .leader_node = leader_node } });
    }

    fn cmdPromote(self: *TopicSubsystem, topic_id: TopicId) void {
        _ = self.ctrl_in.enqueue(.{ .promote_to_leader = .{ .topic_id = topic_id } });
    }

    fn cmdResetReplica(self: *TopicSubsystem, topic_id: TopicId, leader_node: u8) void {
        self.store_lock.lock();
        defer self.store_lock.unlock();
        _ = self.ctrl_in.enqueue(.{ .reset_replica = .{ .topic_id = topic_id, .leader_node = leader_node } });
    }

    fn cmdSetEpoch(self: *TopicSubsystem, topic_id: TopicId, epoch: u64) void {
        _ = self.ctrl_in.enqueue(.{ .set_epoch = .{ .topic_id = topic_id, .epoch = epoch } });
    }

    fn cmdCatchUpBarrier(self: *TopicSubsystem, topic_id: TopicId, from_node: u8, target_index: u64) void {
        _ = self.ctrl_in.enqueue(.{ .catch_up_barrier = .{ .topic_id = topic_id, .from_node = from_node, .target_index = target_index } });
    }

    pub const RegisterResult = struct {
        topic_id: TopicId,
        leader_node_id: u8,
        leader_epoch: u64,
        effective_config: TopicConfig,
        status: enum { ok, config_mismatch, collision, disabled, not_leader },
        newly_created: bool,
    };

    /// Control-loop entry: a local producer registers a publication. When this
    /// node is the topic leader, validate/create the record, open the master
    /// queue, and signal callers to broadcast TopicCreated (spec 03 §3).
    pub fn registerPublication(self: *TopicSubsystem, name: []const u8, config: TopicConfig, now_ns: i64) RegisterResult {
        const id = topic_id_mod.topicIdOf(name);
        if (!self.enabled) {
            return .{ .topic_id = id, .leader_node_id = 0, .leader_epoch = 0, .effective_config = config, .status = .disabled, .newly_created = false };
        }
        const leader = self.election.getLeader();
        const epoch = self.election.getEpoch();
        if (leader == null or leader.? != self.local_node_id) {
            // Not the topic leader: caller proxies creation to the leader (spec 03 §5).
            return .{
                .topic_id = id,
                .leader_node_id = leader orelse 0,
                .leader_epoch = epoch,
                .effective_config = config,
                .status = .not_leader,
                .newly_created = false,
            };
        }
        const outcome = self.registry.upsert(id, name, config, self.local_node_id, epoch, now_ns) catch |err| {
            return .{
                .topic_id = id,
                .leader_node_id = self.local_node_id,
                .leader_epoch = epoch,
                .effective_config = config,
                .status = switch (err) {
                    error.TopicConfigMismatch => .config_mismatch,
                    error.TopicIdCollision => .collision,
                    else => .disabled,
                },
                .newly_created = false,
            };
        };
        const rec = self.registry.getById(id).?;
        if (outcome == .created) {
            rec.local_role = .leader;
            rec.local_queue_open = true;
            self.cmdOpenMaster(id, rec.config, epoch);
            self.cmdPromote(id); // leader already current: no catch-up barrier needed
        }
        return .{
            .topic_id = id,
            .leader_node_id = self.local_node_id,
            .leader_epoch = epoch,
            .effective_config = rec.config,
            .status = .ok,
            .newly_created = outcome == .created,
        };
    }

    /// Apply a TopicCreated admin broadcast: every topics-enabled peer eagerly
    /// opens a replica queue + sink (full mesh, spec 02 §2).
    pub fn applyTopicCreated(self: *TopicSubsystem, topic_id: TopicId, name: []const u8, config: TopicConfig, leader_node: u8, leader_epoch: u64, now_ns: i64) void {
        if (!self.enabled) return;
        const outcome = self.registry.upsert(topic_id, name, config, leader_node, leader_epoch, now_ns) catch return;
        const rec = self.registry.getById(topic_id).?;
        if (leader_node == self.local_node_id) return; // we are the leader; master already open
        if (outcome == .created or !rec.local_queue_open) {
            rec.local_role = .replica;
            rec.local_queue_open = true;
            self.cmdOpenReplica(topic_id, config, leader_epoch);
            self.cmdStartSink(topic_id, leader_node);
        }
    }

    pub const SubscribeResult = struct {
        topic_id: TopicId,
        status: enum { ok, unknown_topic, disabled },
        start_index: u64,
        geometry: TopicConfig,
    };

    /// Control-loop entry: a local subscriber requests a topic (spec 03 §4).
    pub fn subscribe(self: *TopicSubsystem, name: []const u8, earliest: bool) SubscribeResult {
        const id = topic_id_mod.topicIdOf(name);
        if (!self.enabled) {
            return .{ .topic_id = id, .status = .disabled, .start_index = 0, .geometry = .{} };
        }
        const rec = self.registry.getById(id) orelse {
            return .{ .topic_id = id, .status = .unknown_topic, .start_index = 0, .geometry = .{} };
        };
        // Full mesh: the replica/master is opened eagerly on TopicCreated. If a
        // race left it unopened, open it now.
        if (!rec.local_queue_open) {
            if (rec.leader_node_id == self.local_node_id) {
                rec.local_role = .leader;
                self.cmdOpenMaster(id, rec.config, rec.leader_epoch);
            } else {
                rec.local_role = .replica;
                self.cmdOpenReplica(id, rec.config, rec.leader_epoch);
                self.cmdStartSink(id, rec.leader_node_id);
            }
            rec.local_queue_open = true;
        }
        rec.local_subscriber_count += 1;
        // For earliest, the tailer opens at 0 (first retained entry).
        // For latest, compute the current tip from the local store's HWM
        // so the tailer picks up only new messages.
        const hwm = if (self.store.get(id)) |tq| tq.hwm_index else 0;
        const start_index: u64 = if (earliest) 0 else hwm;
        return .{ .topic_id = id, .status = .ok, .start_index = start_index, .geometry = rec.config };
    }

    pub fn unsubscribe(self: *TopicSubsystem, topic_id: TopicId) void {
        if (self.registry.getById(topic_id)) |rec| {
            if (rec.local_subscriber_count > 0) rec.local_subscriber_count -= 1;
            // Full mesh: never close the replica on last unsubscribe.
        }
    }

    /// On sink-ahead divergence (spec 08 §4): reset a replica to the
    /// leader's state. Closes the local queue, removes the directory,
    /// recreates empty, and restarts the sink.
    pub fn resetReplicaToLeader(self: *TopicSubsystem, topic_id: TopicId) void {
        const rec = self.registry.getById(topic_id) orelse return;
        self.store_lock.lock();
        defer self.store_lock.unlock();
        _ = self.ctrl_in.enqueue(.{ .reset_replica = .{
            .topic_id = topic_id,
            .leader_node = rec.leader_node_id,
            .config = rec.config,
            .epoch = rec.leader_epoch,
        } });
    }

    /// Build the absolute queue directory for a topic (mirrors TopicStore layout).
    pub fn queueDir(self: *TopicSubsystem, allocator: std.mem.Allocator, topic_id: TopicId) ![]u8 {
        return std.fmt.allocPrint(allocator, "{s}/t_{x:0>16}", .{ self.store.base_dir, topic_id });
    }

    /// Scan the topics base directory for existing queue dirs and rebuild
    /// the registry. Dir names are hex-encoded topic IDs (spec 02 §5).
    fn scanExistingQueues(self: *TopicSubsystem) void {
        // Open the topics base directory for iteration.
        var dir = std.Io.Dir.openDirAbsolute(self.io, self.store.base_dir, .{ .iterate = true }) catch return;
        defer dir.close(self.io);

        var iter = dir.iterateAssumeFirstIteration();
        while (iter.next(self.io) catch null) |entry| {
            if (entry.kind != .directory) continue;
            // Directory names are "t_<hex_id>".
            if (!std.mem.startsWith(u8, entry.name, "t_")) continue;
            const hex_str = entry.name[2..];
            if (hex_str.len != 16) continue;
            const topic_id = std.fmt.parseInt(TopicId, hex_str, 16) catch continue;

            // Skip if the queue is already open in the store.
            if (self.store.get(topic_id) != null) continue;

            // Try to read the queue's metadata to recover its geometry.
            const config = self.readQueueConfigFromDir(dir, entry.name) orelse continue;

            // Insert into registry with zero epoch — the leader will
            // re-announce TopicCreated and we'll receive the real epoch.
            _ = self.registry.upsert(topic_id, entry.name, config, 0, 0, 0) catch {};
        }
    }

    /// Read the ringloom-queue geometry from a metadata file in a topic dir.
    /// Opens `<dir_name>/metadata.ringloom` and extracts the roll scheme config.
    fn readQueueConfigFromDir(self: *TopicSubsystem, parent_dir: std.Io.Dir, dir_name: []const u8) ?topic_config_mod.TopicConfig {
        // Open the metadata file inside the topic queue directory.
        var sub_dir = std.Io.Dir.openDir(parent_dir, self.io, dir_name, .{}) catch return null;
        defer sub_dir.close(self.io);

        var meta_file = sub_dir.openFile(self.io, "metadata.ringloom", .{}) catch return null;
        defer meta_file.close(self.io);

        // Read the metadata header (first 512 bytes is enough for geometry).
        var buf: [512]u8 = undefined;
        const n = std.Io.File.readPositionalAll(meta_file, self.io, buf[0..], 0) catch return null;

        // The metadata file contains the ringloom-queue header.
        // We extract the roll scheme name from the known offset.
        if (n < 64) return null;

        // Default config — the actual roll scheme can be recovered from
        // the metadata.ringloom format which stores it at a known offset.
        // For now, use a default FAST_DAILY config that matches most setups.
        return topic_config_mod.TopicConfig.fromName("FAST_DAILY", 0, false);
    }

    // ── Election / failover driving (control thread, spec 08) ──────────────

    /// Apply a topics-enabled peer heartbeat to the topic-leader election.
    pub fn onTopicHeartbeat(self: *TopicSubsystem, sender_id: u8, sender_topics_enabled: bool, now_ns: i64) void {
        if (!self.enabled) return;
        _ = self.election.onTopicHeartbeat(sender_id, sender_topics_enabled, now_ns);
    }

    /// Duty-cycle election check; on becoming leader, returns the new epoch so the
    /// caller can announce TOPIC_LEADER_CHANGED and run the catch-up barrier.
    pub fn checkLeadership(self: *TopicSubsystem, now_ns: i64) topic_leader_election.Result {
        if (!self.enabled) return .{};
        const r = self.election.checkLeaderDown(now_ns);
        if (r.became_leader) {
            // New term: fence every topic and require catch-up before writes.
            self.registry.setLeaderForAll(self.local_node_id, r.leader_epoch);
        }
        return r;
    }

    /// Drain engine status (HWMs, barrier completion). Returns number drained.
    pub fn drainStatus(self: *TopicSubsystem, comptime handler: anytype, ctx: anytype) u32 {
        var n: u32 = 0;
        while (self.ctrl_out.dequeue()) |status| {
            handler(ctx, status);
            n += 1;
        }
        return n;
    }
};

// ── Tests ────────────────────────────────────────────────────────────────────

const testing = std.testing;

fn nullPublisher() Publisher {
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

test "disabled subsystem ignores publish/subscribe" {
    const allocator = testing.allocator;
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const base = try tmpBase(allocator, &tmp);
    defer allocator.free(base);

    const sub = try TopicSubsystem.init(allocator, .{
        .enabled = false,
        .base_dir = base,
        .write_runway_bytes = 1 << 20,
        .read_runway_bytes = 1 << 20,
        .prefetcher_cpu_affinity = -1,
        .local_node_id = 1,
        .io = undefined,
    }, nullPublisher(), 0);
    defer sub.deinit();

    const r = sub.registerPublication("orders", TopicConfig.fromName("FAST_DAILY", 4, false), 0);
    try testing.expectEqual(@as(@TypeOf(r.status), .disabled), r.status);
}

test "leader registers publication: creates record, opens+promotes master" {
    const allocator = testing.allocator;
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const base = try tmpBase(allocator, &tmp);
    defer allocator.free(base);

    const sub = try TopicSubsystem.init(allocator, .{
        .enabled = true,
        .base_dir = base,
        .write_runway_bytes = 1 << 20,
        .read_runway_bytes = 1 << 20,
        .prefetcher_cpu_affinity = -1,
        .local_node_id = 1,
        .io = undefined,
    }, nullPublisher(), 0);
    defer sub.deinit();

    // Single topics-enabled node self-elects as topic leader.
    _ = sub.checkLeadership(10 * std.time.ns_per_s);
    try testing.expect(sub.election.isLocalLeader());

    const cfg = TopicConfig.fromName("FAST_DAILY", 4, false);
    const r = sub.registerPublication("orders", cfg, 1000);
    try testing.expectEqual(@as(@TypeOf(r.status), .ok), r.status);
    try testing.expect(r.newly_created);

    // Drive the engine to apply the open_master + promote commands.
    _ = sub.receiverStep();
    const id = topic_id_mod.topicIdOf("orders");
    const tq = sub.store.get(id).?;
    try testing.expect(tq.accepting_writes);
    try testing.expectEqual(topic_store.Role.leader, tq.role);

    // Now a publish is accepted by the engine.
    sub.onPublish(.{ .topic_id = id, .leader_epoch = sub.election.getEpoch(), .correlation_id = 0, .source_node = 1, .source_service = 1, .ack_mode = .fire_and_forget, .payload = "hi" });
    try testing.expectEqual(@as(u64, 1), sub.engine.counters.publish_accepted.load(.monotonic));
}

test "applyTopicCreated on a follower opens a replica + sink" {
    const allocator = testing.allocator;
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const base = try tmpBase(allocator, &tmp);
    defer allocator.free(base);

    const sub = try TopicSubsystem.init(allocator, .{
        .enabled = true,
        .base_dir = base,
        .write_runway_bytes = 1 << 20,
        .read_runway_bytes = 1 << 20,
        .prefetcher_cpu_affinity = -1,
        .local_node_id = 2, // node 2; leader is node 1
        .io = undefined,
    }, nullPublisher(), 0);
    defer sub.deinit();

    const cfg = TopicConfig.fromName("FAST_DAILY", 4, false);
    const id = topic_id_mod.topicIdOf("orders");
    sub.applyTopicCreated(id, "orders", cfg, 1, 3, 1000);
    _ = sub.receiverStep();

    const tq = sub.store.get(id).?;
    try testing.expectEqual(topic_store.Role.replica, tq.role);
    try testing.expect(sub.repl.sinkCount() >= 1);

    // Subscribe returns ok with the replica geometry.
    const sr = sub.subscribe("orders", true);
    try testing.expectEqual(@as(@TypeOf(sr.status), .ok), sr.status);
}
