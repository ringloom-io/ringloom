// SPDX-License-Identifier: Apache-2.0
//! TopicSubsystem — the broker-level facade that owns all topic components and
//! exposes the hooks the control loop and receiver loop call.
//!
//! Thread split (matches the broker's loops):
//!   - control thread owns `registry`, `election` and `local_producers`
//!     (metadata authority, spec 02/08)
//!   - receiver thread owns `store` + `repl` + `engine` (sole queue writer, spec 05)
//!   - one prefetcher thread does maintenancePoll (spec 05 §4)
//! The control thread changes receiver state only through the POD command queue
//! (`ctrl_in`) and learns about it through the status queue (`ctrl_out`). When
//! it must read the store directly it holds `store.lock` (see topic_store.zig).

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
const topic_local_reader = @import("topic_local_reader.zig");

const TopicId = topic_id_mod.TopicId;
const TopicConfig = topic_config_mod.TopicConfig;
const AckMode = topic_config_mod.AckMode;
const TopicStore = topic_store.TopicStore;
const ReplHub = repl_session.ReplHub;
const Publisher = repl_channel.Publisher;
const TopicEngine = topic_engine.TopicEngine;
const PublishView = topic_engine.PublishView;
const TopicPrefetcher = topic_prefetcher.TopicPrefetcher;
const TopicRegistry = topic_registry.TopicRegistry;
const TopicLeaderElection = topic_leader_election.TopicLeaderElection;
const EngineCommandQueue = commands.EngineCommandQueue;
const EngineStatusQueue = commands.EngineStatusQueue;
const TopicReplEnvelope = common.message.topic_data_header.TopicReplEnvelope;
const EngineCommand = commands.EngineCommand;

const log = std.log.scoped(.topics);

pub const LocalTailer = topic_local_reader.LocalTailer;
pub const KvAppendResult = commands.KvAppendResult;
pub const ResetListener = topic_engine.ResetListener;

/// Control-thread callbacks for KV-relevant engine status. Set with
/// `TopicSubsystem.setKvHooks`; invoked from `dispatchKvStatus`, which the
/// control loop calls while draining engine status. Both run on the control
/// thread and must not block.
pub const KvHooks = struct {
    ctx: ?*anyopaque = null,
    /// Result of `kvLeaderAppend`, correlated by `correlation_id`.
    on_append_done: ?*const fn (ctx: ?*anyopaque, topic_id: TopicId, correlation_id: u64, result: KvAppendResult, index: u64) void = null,
    /// A replica queue was reset (informational; the reliable, synchronous
    /// channel is `addResetListener`).
    on_replica_reset: ?*const fn (ctx: ?*anyopaque, topic_id: TopicId, kv_store: bool, reset_seq: u64) void = null,
};

/// Point-in-time local view of one topic queue, see `TopicSubsystem.topicStatus`.
pub const TopicLocalStatus = struct {
    role: topic_store.Role,
    /// Leader: failover catch-up barrier (spec 08) has cleared; appends and
    /// publishes are accepted. Always false for replicas.
    barrier_cleared: bool,
    /// Replica: replication sink is in live mode. Leaders: always false here
    /// (use `replicationLive`, which also counts "local node is leader").
    sink_live: bool,
    /// Highest index in the local log (leader: last append; replica: last
    /// applied). 0 when empty.
    hwm_index: u64,
    epoch: u64,
    kv_store: bool,
};

/// A topic with `flag_kv_store` open in the local store.
pub const KvTopicInfo = struct {
    topic_id: TopicId,
    role: topic_store.Role,
};

/// A KV topic from the registry, with its name. Control thread only.
pub const KvTopicRecord = struct {
    topic_id: TopicId,
    /// Full topic name (`__kv.<store>`), borrowed from the registry.
    name: []const u8,
    leader_node_id: u8,
    leader_epoch: u64,
};

/// Options for the registration/subscription entry points.
pub const AccessOptions = struct {
    /// Allow the reserved `__kv.` namespace (KV subsystem only). The control
    /// loop sets it from `topic_messages.msg_flag_allow_reserved`.
    allow_reserved: bool = false,
};

/// A local service that registered a publication on a topic, with the number of
/// publisher handles it holds there. Ack feedback and leader changes for the
/// topic go to every such service; each service picks out its own publishers.
pub const LocalProducer = struct {
    service_id: i32,
    publications: u32,
};
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
    store: TopicStore,
    repl: ReplHub,
    engine: TopicEngine,
    prefetcher: TopicPrefetcher,

    // Control-thread components.
    registry: TopicRegistry,
    election: TopicLeaderElection,
    /// Local producer services per topic, recorded whether or not this node
    /// leads the topic (remote leaders' feedback is relayed to them too).
    local_producers: std.AutoHashMap(TopicId, std.ArrayList(LocalProducer)),
    /// KV status callbacks (control thread); see `KvHooks`.
    kv_hooks: KvHooks = .{},
    /// Last topics-enabled heartbeat per peer node (0 = none); control thread.
    topic_peer_last_ns: [256]i64 = @splat(0),

    /// Returns an iterator over all topic records for periodic re-broadcast
    /// (spec 02 §8). Caller must iterate on the control thread.
    pub fn iterateTopics(self: *TopicSubsystem) TopicRegistry.Iterator {
        return self.registry.iterator();
    }

    pub fn init(allocator: std.mem.Allocator, opts: Options, publisher: Publisher, now_ns: i64) !*TopicSubsystem {
        const self = try allocator.create(TopicSubsystem);
        errdefer allocator.destroy(self);

        self.allocator = allocator;
        self.enabled = opts.enabled;
        self.local_node_id = opts.local_node_id;
        self.io = opts.io;
        self.ctrl_in = .{};
        self.ctrl_out = .{};
        self.store = TopicStore.init(allocator, opts.base_dir, opts.write_runway_bytes, opts.read_runway_bytes);
        self.repl = ReplHub.init(allocator, opts.local_node_id, publisher, &self.store);
        self.engine = TopicEngine.init(allocator, &self.store, &self.repl, &self.ctrl_in, &self.ctrl_out);
        self.prefetcher = TopicPrefetcher.init(&self.store, opts.prefetcher_cpu_affinity);
        self.registry = TopicRegistry.init(allocator);
        self.election = TopicLeaderElection.init(opts.local_node_id, opts.enabled, now_ns);
        self.local_producers = std.AutoHashMap(TopicId, std.ArrayList(LocalProducer)).init(allocator);
        // Field-wise init skips struct defaults; set them explicitly.
        self.kv_hooks = .{};
        self.topic_peer_last_ns = @splat(0);
        return self;
    }

    pub fn deinit(self: *TopicSubsystem) void {
        self.prefetcher.stop();
        // Release queues handed to the engine that it never adopted.
        while (self.ctrl_in.dequeue()) |cmd| switch (cmd) {
            .adopt_queue => |a| self.store.destroyDetached(a.queue),
            else => {},
        };
        self.engine.deinit();
        self.repl.deinit();
        self.store.deinit();
        self.registry.deinit();
        var lp = self.local_producers.valueIterator();
        while (lp.next()) |list| list.deinit(self.allocator);
        self.local_producers.deinit();
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

    /// Posts a command to the receiver-thread engine. The engine applies
    /// pending commands every loop iteration and before handling a publish, so
    /// a producer that publishes right after registration sees the queue.
    fn sendCommand(self: *TopicSubsystem, cmd: EngineCommand) void {
        if (!self.ctrl_in.enqueue(cmd)) {
            log.warn("topic engine command queue full; dropped {s} command", .{@tagName(cmd)});
        }
    }

    fn cmdOpenMaster(self: *TopicSubsystem, topic_id: TopicId, config: TopicConfig, epoch: u64) void {
        self.openOnDiskAndHandOver(topic_id, config, epoch, .leader);
    }

    fn cmdOpenReplica(self: *TopicSubsystem, topic_id: TopicId, config: TopicConfig, epoch: u64) void {
        self.openOnDiskAndHandOver(topic_id, config, epoch, .replica);
    }

    /// Creates the queue on disk now, so a local subscriber told to open its
    /// directory can do so right away, then hands it to the engine, which owns
    /// the queue map.
    fn openOnDiskAndHandOver(self: *TopicSubsystem, topic_id: TopicId, config: TopicConfig, epoch: u64, role: topic_store.Role) void {
        if (self.isQueueOpen(topic_id)) return;
        const tq = self.store.openDetached(topic_id, config, epoch, role) catch |err| {
            log.warn("failed to open topic queue {x}: {s}", .{ topic_id, @errorName(err) });
            return;
        };
        if (!self.ctrl_in.enqueue(.{ .adopt_queue = .{ .queue = tq } })) {
            log.warn("topic engine command queue full; dropped open of topic {x}", .{topic_id});
            self.store.destroyDetached(tq);
        }
    }

    fn cmdStartSink(self: *TopicSubsystem, topic_id: TopicId, leader_node: u8) void {
        self.sendCommand(.{ .start_sink = .{ .topic_id = topic_id, .leader_node = leader_node } });
    }

    fn cmdPromote(self: *TopicSubsystem, topic_id: TopicId) void {
        self.sendCommand(.{ .promote_to_leader = .{ .topic_id = topic_id } });
    }

    // ── Local producers (control thread) ───────────────────────────────────

    pub fn addLocalProducer(self: *TopicSubsystem, topic_id: TopicId, service_id: i32) void {
        const gop = self.local_producers.getOrPut(topic_id) catch return;
        if (!gop.found_existing) gop.value_ptr.* = .empty;
        for (gop.value_ptr.items) |*p| {
            if (p.service_id == service_id) {
                p.publications += 1;
                return;
            }
        }
        gop.value_ptr.append(self.allocator, .{ .service_id = service_id, .publications = 1 }) catch {};
    }

    pub fn removeLocalProducer(self: *TopicSubsystem, topic_id: TopicId, service_id: i32) void {
        const list = self.local_producers.getPtr(topic_id) orelse return;
        for (list.items, 0..) |*p, i| {
            if (p.service_id != service_id) continue;
            p.publications -= 1;
            if (p.publications == 0) _ = list.swapRemove(i);
            break;
        }
        if (list.items.len == 0) {
            list.deinit(self.allocator);
            _ = self.local_producers.remove(topic_id);
        }
    }

    /// Local services that publish to `topic_id` (borrowed; control thread only).
    pub fn localProducers(self: *TopicSubsystem, topic_id: TopicId) []const LocalProducer {
        const list = self.local_producers.getPtr(topic_id) orelse return &.{};
        return list.items;
    }

    // ── Store reads from the control thread ────────────────────────────────

    /// The topic queue's local high-water mark, or 0 if it is not open.
    pub fn localHwm(self: *TopicSubsystem, topic_id: TopicId) u64 {
        self.store.lock.lock();
        defer self.store.lock.unlock();
        return if (self.store.get(topic_id)) |tq| tq.hwm_index else 0;
    }

    /// Whether the topic's queue is open locally.
    pub fn isQueueOpen(self: *TopicSubsystem, topic_id: TopicId) bool {
        self.store.lock.lock();
        defer self.store.lock.unlock();
        return self.store.get(topic_id) != null;
    }

    pub const RegisterResult = struct {
        topic_id: TopicId,
        leader_node_id: u8,
        leader_epoch: u64,
        effective_config: TopicConfig,
        status: enum { ok, config_mismatch, collision, disabled, not_leader, name_reserved },
        newly_created: bool,
    };

    /// Control-loop entry: a local producer registers a publication. When this
    /// node is the topic leader, validate/create the record, open the master
    /// queue, and signal callers to broadcast TopicCreated (spec 03 §3).
    /// On success (or not_leader) `producer_service_id` is added to the topic's
    /// local producers, so ack-feedback and leader-change frames reach it.
    ///
    /// Ordinary entry point: names in the reserved `__kv.` namespace, and
    /// configs with `flag_kv_store`, are rejected with `.name_reserved`. The KV
    /// subsystem uses `registerPublicationOpts` / `createKvStoreTopic`.
    pub fn registerPublication(self: *TopicSubsystem, name: []const u8, config: TopicConfig, now_ns: i64, producer_service_id: i32) RegisterResult {
        return self.registerPublicationOpts(name, config, now_ns, producer_service_id, .{});
    }

    /// As `registerPublication` with explicit access options.
    ///
    /// With `allow_reserved`, the name and config must agree: a `__kv.` name
    /// requires `flag_kv_store` and vice versa (otherwise `.config_mismatch`),
    /// so a KV store topic can never be created without its flag.
    pub fn registerPublicationOpts(self: *TopicSubsystem, name: []const u8, config: TopicConfig, now_ns: i64, producer_service_id: i32, access: AccessOptions) RegisterResult {
        return self.registerImpl(name, config, now_ns, producer_service_id, access);
    }

    /// Broker-internal creation of a KV store's backing topic on the topic
    /// leader (docs/kv-store-spec.md §4.2): name must be `__kv.<store>`,
    /// `retention_cycles = 0`, `flag_kv_store` set. No producer is recorded.
    /// Idempotent: an existing identical topic yields `.ok`, `newly_created =
    /// false`. Returns `.not_leader` when this node is not the topic leader (the
    /// KV control code must forward to the leader), `.name_reserved` if `name`
    /// lacks the prefix, `.config_mismatch` if the roll scheme is unknown or
    /// differs from the existing topic.
    ///
    /// On `newly_created` the queue is already on disk and its open/promote
    /// commands are queued ahead of any later `kvLeaderAppend`, so an append
    /// issued right after this call sees the topic. The caller (control thread)
    /// should trigger a TopicCreated rebroadcast so replicas open it; use
    /// `ControlLoop.createKvStoreTopic`, which does.
    pub fn createKvStoreTopic(self: *TopicSubsystem, name: []const u8, roll_scheme: []const u8, now_ns: i64) RegisterResult {
        const config = TopicConfig.fromName(roll_scheme, 0, false).withKvStore(true);
        if (!topic_id_mod.isReservedName(name) or name.len == topic_id_mod.reserved_kv_prefix.len) {
            return .{ .topic_id = topic_id_mod.topicIdOf(name), .leader_node_id = 0, .leader_epoch = 0, .effective_config = config, .status = .name_reserved, .newly_created = false };
        }
        const result = self.registerImpl(name, config, now_ns, null, .{ .allow_reserved = true });
        if (result.status == .ok) self.persistKvIdentity(result.topic_id, name, result.effective_config);
        return result;
    }

    fn registerImpl(self: *TopicSubsystem, name: []const u8, config: TopicConfig, now_ns: i64, producer_service_id: ?i32, access: AccessOptions) RegisterResult {
        const id = topic_id_mod.topicIdOf(name);
        if (!self.enabled) {
            return .{ .topic_id = id, .leader_node_id = 0, .leader_epoch = 0, .effective_config = config, .status = .disabled, .newly_created = false };
        }
        const reserved_name = topic_id_mod.isReservedName(name);
        if (!access.allow_reserved) {
            if (reserved_name or config.isKvStore()) {
                return .{ .topic_id = id, .leader_node_id = 0, .leader_epoch = 0, .effective_config = config, .status = .name_reserved, .newly_created = false };
            }
        } else if (reserved_name != config.isKvStore()) {
            return .{ .topic_id = id, .leader_node_id = 0, .leader_epoch = 0, .effective_config = config, .status = .config_mismatch, .newly_created = false };
        }
        // Reject a config no queue can be opened with, before it is recorded:
        // otherwise the topic exists without a queue and later registrations
        // with a valid config fail as mismatched.
        if (!topic_store.isKnownRollScheme(config)) {
            log.warn("rejecting topic '{s}': unknown roll scheme '{s}'", .{ name, config.rollSchemeName() });
            return .{ .topic_id = id, .leader_node_id = 0, .leader_epoch = 0, .effective_config = config, .status = .config_mismatch, .newly_created = false };
        }
        // If no topic leader has been elected yet (e.g. a freshly-started
        // single-node broker before the leader-down interval elapses), let this
        // topics-enabled node self-elect now so registration proceeds. This is
        // a no-op once leadership is established.
        self.ensureLeadershipElected(now_ns);
        const leader = self.election.getLeader();
        const epoch = self.election.getEpoch();
        if (leader == null or leader.? != self.local_node_id) {
            // Not the topic leader: caller proxies creation to the leader (spec 03 §5).
            if (producer_service_id) |svc| self.addLocalProducer(id, svc);
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
        if (producer_service_id) |svc| self.addLocalProducer(id, svc);
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
        if (config.isKvStore()) self.persistKvIdentity(topic_id, name, config);
    }

    pub const SubscribeResult = struct {
        topic_id: TopicId,
        status: enum { ok, unknown_topic, disabled, name_reserved },
        start_index: u64,
        geometry: TopicConfig,
    };

    /// Control-loop entry: a local subscriber requests a topic (spec 03 §4).
    ///
    /// Names in the reserved `__kv.` namespace are rejected with
    /// `.name_reserved`; the KV subsystem uses `subscribeOpts`.
    pub fn subscribe(self: *TopicSubsystem, name: []const u8, earliest: bool) SubscribeResult {
        return self.subscribeOpts(name, earliest, .{});
    }

    /// As `subscribe` with explicit access options.
    pub fn subscribeOpts(self: *TopicSubsystem, name: []const u8, earliest: bool, access: AccessOptions) SubscribeResult {
        const id = topic_id_mod.topicIdOf(name);
        if (!self.enabled) {
            return .{ .topic_id = id, .status = .disabled, .start_index = 0, .geometry = .{} };
        }
        if (!access.allow_reserved and topic_id_mod.isReservedName(name)) {
            return .{ .topic_id = id, .status = .name_reserved, .start_index = 0, .geometry = .{} };
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
        // For latest, start past the current tip so the tailer skips
        // all pre-existing messages. The ringloom-queue tailer dispatches
        // entries with index > start_index-1, so hwm+1 skips the last
        // entry at hwm. When hwm is 0 the queue is empty; 0 is the
        // correct start (tailer auto-seeks forward to the first cycle).
        const hwm = self.localHwm(id);
        const start_index: u64 = if (earliest) 0 else if (hwm == 0) 0 else hwm + 1;
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
        self.sendCommand(.{ .reset_replica = .{
            .topic_id = topic_id,
            .leader_node = rec.leader_node_id,
            .config = rec.config,
            .epoch = rec.leader_epoch,
        } });
    }

    // ── KV hooks (docs/kv-store-spec.md §17 K04) ───────────────────────────

    pub fn setKvHooks(self: *TopicSubsystem, hooks: KvHooks) void {
        self.kv_hooks = hooks;
    }

    /// Registers a synchronous replica-reset listener (receiver thread
    /// callbacks; see `topic_engine.ResetListener` for what a consumer must do).
    /// Returns false if all listener slots are used.
    pub fn addResetListener(self: *TopicSubsystem, listener: ResetListener) bool {
        return self.engine.addResetListener(listener);
    }

    /// Control thread: asks the topic engine to append an opaque leader-authored
    /// payload to `topic_id`, a topic this node leads. Non-blocking and
    /// allocation-free. The outcome (assigned index or failure) arrives later
    /// through `KvHooks.on_append_done` with the same `correlation_id`.
    pub const AppendEnqueue = enum { queued, payload_invalid, queue_full };
    pub fn kvLeaderAppend(self: *TopicSubsystem, topic_id: TopicId, correlation_id: u64, payload: []const u8) AppendEnqueue {
        const cmd = commands.KvLeaderAppend.init(topic_id, correlation_id, payload) orelse return .payload_invalid;
        if (!self.ctrl_in.enqueue(.{ .kv_leader_append = cmd })) return .queue_full;
        return .queued;
    }

    /// Control thread: routes KV-relevant engine status to `kv_hooks`. Called by
    /// the control loop's status handler for every engine status.
    pub fn dispatchKvStatus(self: *TopicSubsystem, status: commands.EngineStatus) void {
        switch (status) {
            .kv_append_done => |d| if (self.kv_hooks.on_append_done) |f| f(self.kv_hooks.ctx, d.topic_id, d.correlation_id, d.result, d.index),
            .replica_reset => |r| {
                if (r.kv_store) {
                    if (self.registry.getById(r.topic_id)) |rec| self.persistKvIdentity(r.topic_id, rec.name, rec.config);
                }
                if (self.kv_hooks.on_replica_reset) |f| f(self.kv_hooks.ctx, r.topic_id, r.kv_store, r.reset_seq);
            },
            else => {},
        }
    }

    /// Any thread: snapshot of the local queue state, or null if the topic is
    /// not open here. Takes `store.lock` briefly.
    pub fn topicStatus(self: *TopicSubsystem, topic_id: TopicId) ?TopicLocalStatus {
        self.store.lock.lock();
        defer self.store.lock.unlock();
        const tq = self.store.get(topic_id) orelse return null;
        return .{
            .role = tq.role,
            .barrier_cleared = tq.role == .leader and tq.accepting_writes,
            .sink_live = tq.role == .replica and tq.repl_live.load(.acquire),
            .hwm_index = tq.hwm_index,
            .epoch = tq.epoch,
            .kv_store = tq.config.isKvStore(),
        };
    }

    /// Any thread: the local node leads the topic (its master queue is open).
    pub fn isLocalLeader(self: *TopicSubsystem, topic_id: TopicId) bool {
        const st = self.topicStatus(topic_id) orelse return false;
        return st.role == .leader;
    }

    /// Any thread: `KvControlHeader.replication_live` (§5.1): the replica's sink
    /// is in live mode, or the local node is the topic leader.
    pub fn replicationLive(self: *TopicSubsystem, topic_id: TopicId) bool {
        const st = self.topicStatus(topic_id) orelse return false;
        return st.role == .leader or st.sink_live;
    }

    /// Any thread: the local node leads the topic AND the failover catch-up
    /// barrier (spec 08) has cleared, i.e. appends are accepted. Leader lease
    /// arbitration (§9.2) must wait for this.
    pub fn isBarrierCleared(self: *TopicSubsystem, topic_id: TopicId) bool {
        const st = self.topicStatus(topic_id) orelse return false;
        return st.barrier_cleared;
    }

    /// Any thread: opens an independent read-only tailer on the topic's local
    /// queue, starting at `start_index` (0 = beginning of the log). The caller
    /// owns it and must `deinit` it, and must drop and reopen it on a replica
    /// reset (see `topic_local_reader`). Setup path; allocates. Fails with
    /// `error.QueueNotOpen` if the topic is not open locally; a concurrent
    /// reset may also make the open fail, in which case retry later.
    pub fn openLocalTailer(self: *TopicSubsystem, allocator: std.mem.Allocator, topic_id: TopicId, start_index: u64) !LocalTailer {
        var dir_buf: [std.fs.max_path_bytes]u8 = undefined;
        var config: TopicConfig = undefined;
        var dir_len: usize = 0;
        {
            self.store.lock.lock();
            defer self.store.lock.unlock();
            const tq = self.store.get(topic_id) orelse return error.QueueNotOpen;
            if (tq.dir.len > dir_buf.len) return error.NameTooLong;
            @memcpy(dir_buf[0..tq.dir.len], tq.dir);
            dir_len = tq.dir.len;
            config = tq.config;
        }
        return LocalTailer.open(allocator, dir_buf[0..dir_len], config, start_index);
    }

    /// Any thread: fills `out` with open topics that have `flag_kv_store`.
    /// Returns the number written (at most `out.len`).
    pub fn collectKvTopics(self: *TopicSubsystem, out: []KvTopicInfo) u32 {
        self.store.lock.lock();
        defer self.store.lock.unlock();
        var n: u32 = 0;
        var it = self.store.openQueues();
        while (it.next()) |tqp| {
            if (n >= out.len) break;
            const tq = tqp.*;
            if (!tq.config.isKvStore()) continue;
            out[n] = .{ .topic_id = tq.topic_id, .role = tq.role };
            n += 1;
        }
        return n;
    }

    /// Control thread only: fills `out` with registry topics that have
    /// `flag_kv_store` (open locally or not), with their names. Slices borrow
    /// the registry; copy them if they must outlive the next registry change.
    pub fn collectKvTopicRecords(self: *TopicSubsystem, out: []KvTopicRecord) u32 {
        var n: u32 = 0;
        var it = self.registry.iterator();
        while (it.next()) |rec| {
            if (n >= out.len) break;
            if (!rec.config.isKvStore()) continue;
            out[n] = .{ .topic_id = rec.topic_id, .name = rec.name, .leader_node_id = rec.leader_node_id, .leader_epoch = rec.leader_epoch };
            n += 1;
        }
        return n;
    }

    /// Build the absolute queue directory for a topic (mirrors TopicStore layout).
    pub fn queueDir(self: *TopicSubsystem, allocator: std.mem.Allocator, topic_id: TopicId) ![]u8 {
        return allocator.print("{s}/t_{x:0>16}", .{ self.store.base_dir, topic_id });
    }

    /// Scan the topics base directory for existing queue dirs and rebuild
    /// the registry. Dir names are hex-encoded topic IDs (spec 02 §5).
    fn scanExistingQueues(self: *TopicSubsystem) void {
        // Open the topics base directory for iteration.
        var dir = std.Io.Dir.cwd().openDir(self.io, self.store.base_dir, .{ .iterate = true }) catch return;
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
            if (self.isQueueOpen(topic_id)) continue;

            // KV identity and flags must survive a restart, including when
            // tmpfs materializations have vanished. Queue geometry alone does
            // not preserve either the reserved name or flag_kv_store.
            if (self.readKvIdentity(topic_id)) |identity| {
                const name = identity.name[0..identity.name_len];
                _ = self.registry.upsert(topic_id, name, identity.config, 0, 0, 0) catch continue;
                continue;
            }

            // Try to read the queue's metadata to recover its geometry.
            const config = self.readQueueConfigFromDir(dir, entry.name) orelse continue;

            // Insert into registry with zero epoch — the leader will
            // re-announce TopicCreated and we'll receive the real epoch.
            _ = self.registry.upsert(topic_id, entry.name, config, 0, 0, 0) catch {};
        }
    }

    const KvDiskIdentity = extern struct {
        magic: u32 = 0x3149564b,
        name_len: u32 = 0,
        config: TopicConfig,
        name: [72]u8 = @splat(0),
    };

    fn persistKvIdentity(self: *TopicSubsystem, id: TopicId, name: []const u8, config: TopicConfig) void {
        if (name.len < 6 or name.len > 69 or !common.kv.format.hasKvTopicPrefix(name) or !common.kv.format.isValidStoreName(name[5..])) return;
        var identity: KvDiskIdentity = .{ .config = config, .name_len = @intCast(name.len) };
        @memcpy(identity.name[0..name.len], name);
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const path = std.fmt.bufPrint(&path_buf, "{s}/t_{x:0>16}.kv-identity", .{ self.store.base_dir, id }) catch return;
        const io = common.platform.defaultIo();
        var temp_buf: [std.fs.max_path_bytes]u8 = undefined;
        const temporary = std.fmt.bufPrint(&temp_buf, "{s}.tmp", .{path}) catch return;
        const file = std.Io.Dir.cwd().createFile(io, temporary, .{}) catch |err| {
            log.warn("cannot persist KV identity: {s}", .{@errorName(err)});
            return;
        };
        defer file.close(io);
        file.writeStreamingAll(io, std.mem.asBytes(&identity)) catch |err| {
            log.warn("cannot write KV identity: {s}", .{@errorName(err)});
            return;
        };
        file.sync(io) catch return;
        // Keep identity beside the queue, so replica reset cannot remove it.
        std.Io.Dir.rename(.cwd(), temporary, .cwd(), path, io) catch |err| log.warn("cannot publish KV identity: {s}", .{@errorName(err)});
    }

    fn readKvIdentity(self: *TopicSubsystem, id: TopicId) ?KvDiskIdentity {
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const path = std.fmt.bufPrint(&path_buf, "{s}/t_{x:0>16}.kv-identity", .{ self.store.base_dir, id }) catch return null;
        const io = common.platform.defaultIo();
        const file = std.Io.Dir.cwd().openFile(io, path, .{}) catch return null;
        defer file.close(io);
        var identity: KvDiskIdentity = undefined;
        const n = file.readPositionalAll(io, std.mem.asBytes(&identity), 0) catch return null;
        if (n != @sizeOf(KvDiskIdentity) or identity.magic != 0x3149564b or identity.name_len < 6 or identity.name_len > 69) return null;
        const name = identity.name[0..identity.name_len];
        if (!common.kv.format.hasKvTopicPrefix(name) or !common.kv.format.isValidStoreName(name[5..]) or topic_id_mod.topicIdOf(name) != id or !identity.config.isKvStore() or !topic_store.isKnownRollScheme(identity.config)) return null;
        return identity;
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
        if (sender_id != self.local_node_id) {
            self.topic_peer_last_ns[sender_id] = if (sender_topics_enabled) now_ns else 0;
            self.refreshTopicPeers(now_ns);
        }
    }

    /// Publish the number of topics-enabled peers heard from within the
    /// leader-down interval to the engine's replicate_once carve-out. Uses the
    /// same interval as leader failover, so a promoted survivor of a dead
    /// cluster partner acks on append again.
    pub fn refreshTopicPeers(self: *TopicSubsystem, now_ns: i64) void {
        var live: u32 = 0;
        for (self.topic_peer_last_ns) |last| {
            if (last != 0 and now_ns - last <= topic_leader_election.LeaderDownIntervalNs) live += 1;
        }
        self.engine.live_topic_peers.store(live, .release);
    }

    /// Duty-cycle election check; on becoming leader, returns the new epoch so the
    /// caller can announce TOPIC_LEADER_CHANGED and run the catch-up barrier.
    pub fn checkLeadership(self: *TopicSubsystem, now_ns: i64) topic_leader_election.Result {
        if (!self.enabled) return .{};
        self.refreshTopicPeers(now_ns);
        const r = self.election.checkLeaderDown(now_ns);
        if (r.became_leader) {
            // New term: fence every topic and require catch-up before writes.
            self.applyNewLeadershipEpoch(r.leader_epoch);
        }
        return r;
    }

    /// Eagerly self-elect a topics-enabled local node when no leader is set. Used
    /// at registration time so a freshly-started single-node broker (or the first
    /// topics-enabled node) doesn't reject the first publication while waiting for
    /// the leader-down interval to elapse. No-op once leadership is established.
    fn ensureLeadershipElected(self: *TopicSubsystem, now_ns: i64) void {
        if (!self.enabled) return;
        if (self.election.getLeader() != null) return;
        // Force a leadership decision now by checking with an expired deadline.
        const r = self.election.checkLeaderDown(now_ns + 2 * topic_leader_election.LeaderDownIntervalNs);
        if (r.became_leader) {
            self.applyNewLeadershipEpoch(r.leader_epoch);
        }
    }

    /// Propagate a newly-minted leadership term to both the registry records and
    /// the open store queues. The store's `TopicQueue.epoch` stamps the
    /// `leader_epoch` carried in ack-feedback (template 15) and fences inbound
    /// repl frames; it must match the epoch producers see via TopicLeaderChanged,
    /// or producers reject the feedback as stale and acks never complete.
    fn applyNewLeadershipEpoch(self: *TopicSubsystem, leader_epoch: u64) void {
        self.registry.setLeaderForAll(self.local_node_id, leader_epoch);
        // Advance every open queue's epoch to the new term. Queues are opened with
        // the epoch known at registration time, which may be 0 (before this node
        // self-elects). The engine owns queue state, so ask it to do this; it
        // applies the command before any later publish.
        self.sendCommand(.{ .set_epoch_all = .{ .epoch = leader_epoch } });
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
    return allocator.print(".zig-cache/tmp/{s}", .{tmp.sub_path[0..]});
}

comptime {
    _ = topic_local_reader;
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

    const r = sub.registerPublication("orders", TopicConfig.fromName("FAST_DAILY", 4, false), 0, 1);
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
    const r = sub.registerPublication("orders", cfg, 1000, 1);
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

fn testSubsystem(allocator: std.mem.Allocator, base: []const u8, node: u8) !*TopicSubsystem {
    return TopicSubsystem.init(allocator, .{
        .enabled = true,
        .base_dir = base,
        .write_runway_bytes = 1 << 20,
        .read_runway_bytes = 1 << 20,
        .prefetcher_cpu_affinity = -1,
        .local_node_id = node,
        .io = undefined,
    }, nullPublisher(), 0);
}

test "every local producer of a topic is tracked, not just the latest" {
    const allocator = testing.allocator;
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const base = try tmpBase(allocator, &tmp);
    defer allocator.free(base);
    const sub = try testSubsystem(allocator, base, 1);
    defer sub.deinit();
    _ = sub.checkLeadership(10 * std.time.ns_per_s);

    const cfg = TopicConfig.fromName("FAST_DAILY", 4, false);
    const id = topic_id_mod.topicIdOf("orders");
    _ = sub.registerPublication("orders", cfg, 1000, 3);
    _ = sub.registerPublication("orders", cfg, 1000, 4);
    _ = sub.registerPublication("orders", cfg, 1000, 4); // second handle in service 4

    try testing.expectEqual(@as(usize, 2), sub.localProducers(id).len);

    // Service 4 closes one of its two publishers: still a producer.
    sub.removeLocalProducer(id, 4);
    try testing.expectEqual(@as(usize, 2), sub.localProducers(id).len);
    sub.removeLocalProducer(id, 4);
    try testing.expectEqual(@as(usize, 1), sub.localProducers(id).len);
    try testing.expectEqual(@as(i32, 3), sub.localProducers(id)[0].service_id);
    sub.removeLocalProducer(id, 3);
    try testing.expectEqual(@as(usize, 0), sub.localProducers(id).len);
}

test "producers registering on a non-leader node are tracked for feedback relay" {
    const allocator = testing.allocator;
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const base = try tmpBase(allocator, &tmp);
    defer allocator.free(base);
    const sub = try testSubsystem(allocator, base, 2);
    defer sub.deinit();
    // Node 1 is the topic leader.
    _ = sub.election.onLeaderAnnouncement(1, 5, 0);

    const r = sub.registerPublication("orders", TopicConfig.fromName("FAST_DAILY", 4, false), 1000, 9);
    try testing.expectEqual(@as(@TypeOf(r.status), .not_leader), r.status);
    try testing.expectEqual(@as(usize, 1), sub.localProducers(r.topic_id).len);
}

test "live topics-enabled peers gate the replicate_once single-node carve-out" {
    const allocator = testing.allocator;
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const base = try tmpBase(allocator, &tmp);
    defer allocator.free(base);
    const sub = try testSubsystem(allocator, base, 1);
    defer sub.deinit();
    try testing.expectEqual(@as(u32, 0), sub.engine.live_topic_peers.load(.acquire));
    // A topics-disabled peer is not a replica target.
    sub.onTopicHeartbeat(3, false, 1000);
    try testing.expectEqual(@as(u32, 0), sub.engine.live_topic_peers.load(.acquire));
    sub.onTopicHeartbeat(2, true, 1000);
    try testing.expectEqual(@as(u32, 1), sub.engine.live_topic_peers.load(.acquire));
    // The periodic leadership check expires the peer with the leader-down interval.
    _ = sub.checkLeadership(1000 + topic_leader_election.LeaderDownIntervalNs + 1);
    try testing.expectEqual(@as(u32, 0), sub.engine.live_topic_peers.load(.acquire));
}

test "registration only queues store changes; the receiver thread applies them" {
    const allocator = testing.allocator;
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const base = try tmpBase(allocator, &tmp);
    defer allocator.free(base);
    const sub = try testSubsystem(allocator, base, 1);
    defer sub.deinit();
    _ = sub.checkLeadership(10 * std.time.ns_per_s);

    const r = sub.registerPublication("orders", TopicConfig.fromName("FAST_DAILY", 4, false), 1000, 1);
    try testing.expectEqual(@as(@TypeOf(r.status), .ok), r.status);
    // The control thread did not touch the queue map, but the queue already
    // exists on disk so a local subscriber can open it straight away.
    try testing.expectEqual(@as(u32, 0), sub.store.count());
    const dir = try sub.queueDir(allocator, r.topic_id);
    defer allocator.free(dir);
    var qdir = try std.Io.Dir.cwd().openDir(testing.io, dir, .{});
    qdir.close(testing.io);

    // The producer's first publish arrives before the receiver's next step().
    sub.onPublish(.{ .topic_id = r.topic_id, .leader_epoch = r.leader_epoch, .correlation_id = 0, .source_node = 1, .source_service = 1, .ack_mode = .fire_and_forget, .payload = "first" });
    try testing.expectEqual(@as(u64, 1), sub.engine.counters.publish_accepted.load(.monotonic));
    try testing.expectEqual(r.leader_epoch, sub.store.get(r.topic_id).?.epoch);
}

test "store open/close on the receiver thread is safe against prefetcher and control readers" {
    const allocator = testing.allocator;
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const base = try tmpBase(allocator, &tmp);
    defer allocator.free(base);
    const sub = try testSubsystem(allocator, base, 1);
    defer sub.deinit();

    // Real prefetcher thread iterating the store and polling every queue.
    sub.prefetcher.idle_sleep_ns = 0;
    try sub.prefetcher.start();

    // A "control thread" reading the store the way the control loop does.
    const Reader = struct {
        fn run(s: *TopicSubsystem, stop: *std.atomic.Value(bool)) void {
            while (!stop.load(.acquire)) {
                var t: TopicId = 1;
                while (t <= 4) : (t += 1) {
                    _ = s.localHwm(t);
                    _ = s.isQueueOpen(t);
                }
            }
        }
    };
    var stop = std.atomic.Value(bool).init(false);
    const reader = try std.Thread.spawn(.{}, Reader.run, .{ sub, &stop });

    // "Receiver thread": open, append to, and close queues repeatedly. Before
    // the fix, close freed queues the prefetcher could still be polling.
    const cfg = TopicConfig.fromName("FAST_DAILY", 4, false);
    var round: u32 = 0;
    while (round < 50) : (round += 1) {
        var t: TopicId = 1;
        while (t <= 4) : (t += 1) {
            try testing.expect(sub.ctrl_in.enqueue(.{ .open_master = .{ .topic_id = t, .config = cfg, .epoch = 1 } }));
        }
        _ = sub.receiverStep();
        t = 1;
        while (t <= 4) : (t += 1) _ = try sub.store.get(t).?.append("x");
        t = 1;
        while (t <= 4) : (t += 1) {
            try testing.expect(sub.ctrl_in.enqueue(.{ .close_queue = .{ .topic_id = t } }));
        }
        _ = sub.receiverStep();
    }

    stop.store(true, .release);
    reader.join();
    sub.prefetcher.stop();
    try testing.expectEqual(@as(u32, 0), sub.store.count());
}

test "registration with an unknown roll scheme is rejected and not recorded" {
    const allocator = testing.allocator;
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const base = try tmpBase(allocator, &tmp);
    defer allocator.free(base);
    const sub = try testSubsystem(allocator, base, 1);
    defer sub.deinit();
    _ = sub.checkLeadership(10 * std.time.ns_per_s);

    // An all-zero config (empty roll scheme name) used to be accepted.
    const bad = sub.registerPublication("orders", .{}, 1000, 1);
    try testing.expectEqual(@as(@TypeOf(bad.status), .config_mismatch), bad.status);
    try testing.expect(sub.registry.getById(bad.topic_id) == null);
    try testing.expectEqual(@as(usize, 0), sub.localProducers(bad.topic_id).len);

    // A valid registration for the same topic then succeeds.
    const good = sub.registerPublication("orders", TopicConfig.fromName("FAST_DAILY", 4, false), 1000, 1);
    try testing.expectEqual(@as(@TypeOf(good.status), .ok), good.status);
}

// ── KV hook tests ────────────────────────────────────────────────────────────

test "reserved __kv. names are rejected on ordinary paths, allowed with opt-in" {
    const allocator = testing.allocator;
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const base = try tmpBase(allocator, &tmp);
    defer allocator.free(base);
    const sub = try testSubsystem(allocator, base, 1);
    defer sub.deinit();
    _ = sub.checkLeadership(10 * std.time.ns_per_s);

    const plain = TopicConfig.fromName("FAST_DAILY", 0, false);
    const kv = plain.withKvStore(true);

    // Ordinary registration: reserved name, with or without the flag.
    const r1 = sub.registerPublication("__kv.prices", plain, 1000, 1);
    try testing.expectEqual(@as(@TypeOf(r1.status), .name_reserved), r1.status);
    const r2 = sub.registerPublication("__kv.prices", kv, 1000, 1);
    try testing.expectEqual(@as(@TypeOf(r2.status), .name_reserved), r2.status);
    // The flag alone on an ordinary name is also reserved.
    const r3 = sub.registerPublication("orders", kv, 1000, 1);
    try testing.expectEqual(@as(@TypeOf(r3.status), .name_reserved), r3.status);
    try testing.expect(sub.registry.getById(r1.topic_id) == null);
    try testing.expectEqual(@as(usize, 0), sub.localProducers(r1.topic_id).len);

    // Ordinary subscribe.
    const s1 = sub.subscribe("__kv.prices", true);
    try testing.expectEqual(@as(@TypeOf(s1.status), .name_reserved), s1.status);

    // Opt-in registration needs name and flag to agree.
    const bad = sub.registerPublicationOpts("__kv.prices", plain, 1000, 1, .{ .allow_reserved = true });
    try testing.expectEqual(@as(@TypeOf(bad.status), .config_mismatch), bad.status);
    const ok = sub.registerPublicationOpts("__kv.prices", kv, 1000, 7, .{ .allow_reserved = true });
    try testing.expectEqual(@as(@TypeOf(ok.status), .ok), ok.status);
    try testing.expect(ok.newly_created);
    try testing.expect(ok.effective_config.isKvStore());
    try testing.expectEqual(@as(usize, 1), sub.localProducers(ok.topic_id).len);
    const s2 = sub.subscribeOpts("__kv.prices", true, .{ .allow_reserved = true });
    try testing.expectEqual(@as(@TypeOf(s2.status), .ok), s2.status);
    // Ordinary subscribe still refuses the now-existing topic.
    const s3 = sub.subscribe("__kv.prices", true);
    try testing.expectEqual(@as(@TypeOf(s3.status), .name_reserved), s3.status);

    // Non-reserved topics are unaffected.
    const normal = sub.registerPublication("orders", plain, 1000, 1);
    try testing.expectEqual(@as(@TypeOf(normal.status), .ok), normal.status);
}

const KvTestHooks = struct {
    done_count: u32 = 0,
    last_corr: u64 = 0,
    last_result: KvAppendResult = .ok,
    last_index: u64 = 0,
    resets: u32 = 0,

    fn onDone(ctx: ?*anyopaque, _: TopicId, corr: u64, result: KvAppendResult, index: u64) void {
        const self: *KvTestHooks = @ptrCast(@alignCast(ctx.?));
        self.done_count += 1;
        self.last_corr = corr;
        self.last_result = result;
        self.last_index = index;
    }
    fn onReset(ctx: ?*anyopaque, _: TopicId, _: bool, _: u64) void {
        const self: *KvTestHooks = @ptrCast(@alignCast(ctx.?));
        self.resets += 1;
    }
    fn handler(ts: *TopicSubsystem, status: commands.EngineStatus) void {
        ts.dispatchKvStatus(status);
    }
};

test "createKvStoreTopic + kvLeaderAppend + local status/tailer/iteration" {
    const allocator = testing.allocator;
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const base = try tmpBase(allocator, &tmp);
    defer allocator.free(base);
    const sub = try testSubsystem(allocator, base, 1);
    defer sub.deinit();
    _ = sub.checkLeadership(10 * std.time.ns_per_s);

    var hooks = KvTestHooks{};
    sub.setKvHooks(.{ .ctx = &hooks, .on_append_done = KvTestHooks.onDone, .on_replica_reset = KvTestHooks.onReset });

    // Names without the prefix cannot be KV stores.
    const nope = sub.createKvStoreTopic("orders", "FAST_DAILY", 1000);
    try testing.expectEqual(@as(@TypeOf(nope.status), .name_reserved), nope.status);

    const r = sub.createKvStoreTopic("__kv.prices", "FAST_DAILY", 1000);
    try testing.expectEqual(@as(@TypeOf(r.status), .ok), r.status);
    try testing.expect(r.newly_created);
    try testing.expect(r.effective_config.isKvStore());
    try testing.expectEqual(@as(u32, 0), r.effective_config.retention_cycles);
    // Idempotent.
    const again = sub.createKvStoreTopic("__kv.prices", "FAST_DAILY", 1000);
    try testing.expectEqual(@as(@TypeOf(again.status), .ok), again.status);
    try testing.expect(!again.newly_created);
    // No producer recorded for the internal create.
    try testing.expectEqual(@as(usize, 0), sub.localProducers(r.topic_id).len);

    // Append right after create: FIFO commands make the queue available.
    try testing.expectEqual(TopicSubsystem.AppendEnqueue.queued, sub.kvLeaderAppend(r.topic_id, 5, "STORE_HEADER"));
    try testing.expectEqual(TopicSubsystem.AppendEnqueue.payload_invalid, sub.kvLeaderAppend(r.topic_id, 6, ""));
    _ = sub.receiverStep();
    try testing.expectEqual(@as(u32, 1), sub.drainStatus(KvTestHooks.handler, sub));
    try testing.expectEqual(@as(u32, 1), hooks.done_count);
    try testing.expectEqual(@as(u64, 5), hooks.last_corr);
    try testing.expectEqual(KvAppendResult.ok, hooks.last_result);

    // Local views.
    try testing.expect(sub.isLocalLeader(r.topic_id));
    try testing.expect(sub.isBarrierCleared(r.topic_id));
    try testing.expect(sub.replicationLive(r.topic_id));
    const st = sub.topicStatus(r.topic_id).?;
    try testing.expect(st.kv_store);
    try testing.expectEqual(hooks.last_index, st.hwm_index);
    try testing.expect(sub.topicStatus(12345) == null);
    try testing.expect(!sub.isLocalLeader(12345));

    // KV topic enumeration (thread-safe store view and control-thread registry view).
    var infos: [4]KvTopicInfo = undefined;
    try testing.expectEqual(@as(u32, 1), sub.collectKvTopics(&infos));
    try testing.expectEqual(r.topic_id, infos[0].topic_id);
    var recs: [4]KvTopicRecord = undefined;
    try testing.expectEqual(@as(u32, 1), sub.collectKvTopicRecords(&recs));
    try testing.expectEqualStrings("__kv.prices", recs[0].name);

    // An ordinary topic is not listed.
    _ = sub.registerPublication("orders", TopicConfig.fromName("FAST_DAILY", 0, false), 1000, 1);
    _ = sub.receiverStep();
    try testing.expectEqual(@as(u32, 1), sub.collectKvTopics(&infos));

    // Independent local tailer from "another thread" (here: same thread, own handle).
    var t = try sub.openLocalTailer(allocator, r.topic_id, 0);
    defer t.deinit();
    const e = (try t.poll()).?;
    try testing.expectEqualStrings("STORE_HEADER", e.message);
    try testing.expectEqual(hooks.last_index, e.index);
    try testing.expect((try t.poll()) == null);
    try testing.expectError(error.QueueNotOpen, sub.openLocalTailer(allocator, 12345, 0));
}

test "replica status: not leader, sink not live, barrier not cleared" {
    const allocator = testing.allocator;
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const base = try tmpBase(allocator, &tmp);
    defer allocator.free(base);
    const sub = try testSubsystem(allocator, base, 2);
    defer sub.deinit();

    const kv = TopicConfig.fromName("FAST_DAILY", 0, false).withKvStore(true);
    const id = topic_id_mod.topicIdOf("__kv.prices");
    sub.applyTopicCreated(id, "__kv.prices", kv, 1, 3, 1000);
    _ = sub.receiverStep();

    try testing.expect(!sub.isLocalLeader(id));
    try testing.expect(!sub.isBarrierCleared(id));
    try testing.expect(!sub.replicationLive(id)); // sink is connecting, not live
    const st = sub.topicStatus(id).?;
    try testing.expectEqual(topic_store.Role.replica, st.role);
    try testing.expect(st.kv_store);

    // Flip the mirrored flag the receiver sets when the sink goes live.
    sub.store.get(id).?.repl_live.store(true, .release);
    try testing.expect(sub.replicationLive(id));

    // Reset notifications reach the control-thread hook.
    var hooks = KvTestHooks{};
    sub.setKvHooks(.{ .ctx = &hooks, .on_replica_reset = KvTestHooks.onReset });
    sub.resetReplicaToLeader(id);
    _ = sub.receiverStep();
    _ = sub.drainStatus(KvTestHooks.handler, sub);
    try testing.expectEqual(@as(u32, 1), hooks.resets);
    try testing.expect(!sub.replicationLive(id)); // fresh replica is not live yet
}

test "KV topic identity and immutable geometry survive restart scanning" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const base = try tmpBase(a, &tmp);
    defer a.free(base);
    const first = try testSubsystem(a, base, 1);
    _ = first.checkLeadership(10 * std.time.ns_per_s);
    const created = first.createKvStoreTopic("__kv.restart", "FAST_HOURLY", 1);
    try testing.expectEqual(@TypeOf(created.status).ok, created.status);
    _ = first.receiverStep();
    first.deinit();
    const second = try testSubsystem(a, base, 1);
    defer second.deinit();
    second.io = common.platform.defaultIo();
    second.scanExistingQueues();
    var records: [1]KvTopicRecord = undefined;
    try testing.expectEqual(@as(u32, 1), second.collectKvTopicRecords(&records));
    try testing.expectEqualStrings("__kv.restart", records[0].name);
    const rec = second.registry.getById(created.topic_id).?;
    try testing.expect(rec.config.isKvStore());
    try testing.expectEqualStrings("FAST_HOURLY", rec.config.rollSchemeName());
}
