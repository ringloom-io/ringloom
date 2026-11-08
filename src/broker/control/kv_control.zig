// SPDX-License-Identifier: Apache-2.0
//! KV control-plane driver (docs/kv-store-spec.md §9, §12): glue between the
//! control loop, the topic subsystem, the grant tracker and the lease state
//! machine (`kv/kv_writer_lease.zig`). Runs entirely on the **control-loop
//! thread**.
//!
//! Message routing:
//!   service ──(control ring)──▶ local broker `handleServiceMessage`
//!       local broker is topic leader  → `LeaseTable.submit`
//!       otherwise                     → admin `KvAdminRequest` ──▶ leader broker
//!                                         `handleAdminRequest` → `LeaseTable.submit`
//!   replies (`deliver`): local service → its control ring; remote → admin
//!       `KvAdminReply` ──▶ origin broker `handleAdminReply` → service control ring.
//!   `OpenKvReader` is answered locally from `KvLocalStores`, never forwarded.
//!
//! The grant tracker runs on every topics-enabled broker (see its docs for the
//! threading/latency rationale); arbitration only acts while this node is the
//! topic leader.

const std = @import("std");
const common = @import("ringloom_common");
const kv = common.kv;
const platform = common.platform;
const msgs = common.message.kv_control_messages;
const kv_messages = @import("../kv/kv_messages.zig");
const lease = @import("../kv/kv_writer_lease.zig");
const tracker_mod = @import("../kv/kv_grant_tracker.zig");
const local_stores_mod = @import("../kv/kv_local_stores.zig");
const topics = @import("../topics.zig");
const topic_config = @import("../topics/topic_config.zig");
const topic_commands = @import("../topics/topic_commands.zig");
const topic_store = @import("../topics/topic_store.zig");

const log = std.log.scoped(.kv_control);

/// Services the control loop provides to the driver.
pub const Host = struct {
    ctx: *anyopaque,
    /// Write a control message into a *local* service's control buffer.
    write_to_service: *const fn (ctx: *anyopaque, service_id: i32, bytes: []const u8) bool,
    /// Unicast admin message to a peer broker.
    send_admin_request: *const fn (ctx: *anyopaque, node_id: u8, body: kv_messages.KvAdminRequestBody) void,
    send_admin_reply: *const fn (ctx: *anyopaque, node_id: u8, body: kv_messages.KvAdminReplyBody) void,
    /// Is the service (node, id) in the broker's service registry?
    is_service_registered: *const fn (ctx: *anyopaque, node_id: u8, service_id: i32) bool,
    /// Create `__kv.<store>` on this (leader) node; schedules the replica broadcast.
    create_kv_topic: *const fn (ctx: *anyopaque, topic_name: []const u8, roll_scheme: []const u8) topics.TopicSubsystem.RegisterResult,
};

pub const Options = struct {
    max_stores: u32 = 64,
    /// Broker maximum, including TopicPublishHeader.
    max_frame_length: u32 = common.platform.constants.default_max_frame_length,
    lease: lease.Config = .{},

    fn payloadBudget(self: Options) u32 {
        return self.max_frame_length -| @as(u32, common.message.topic_data_header.TopicPublishHeader.encoded_length);
    }
};

pub const KvControl = struct {
    const Tracker = tracker_mod.GrantTracker(tracker_mod.TopicSource);

    allocator: std.mem.Allocator,
    ts: *topics.TopicSubsystem,
    local_node_id: u8,
    host: Host,
    opts: Options,
    local_stores: local_stores_mod.KvLocalStores = .unavailable,
    source: tracker_mod.TopicSource,
    tracker: Tracker,
    table: lease.LeaseTable,
    was_leader: bool = false,
    next_idle_step_ns: i64 = 0,
    buf: [kv_messages.kv_admin_bytes_max + 64]u8 = undefined,

    pub fn create(allocator: std.mem.Allocator, ts: *topics.TopicSubsystem, local_node_id: u8, host: Host, opts: Options) !*KvControl {
        const self = try allocator.create(KvControl);
        errdefer allocator.destroy(self);
        self.* = .{
            .allocator = allocator,
            .ts = ts,
            .local_node_id = local_node_id,
            .host = host,
            .opts = opts,
            .source = .{ .ts = ts },
            .tracker = undefined,
            .table = undefined,
        };
        self.tracker = try Tracker.init(allocator, &self.source, .{ .max_stores = opts.max_stores });
        errdefer self.tracker.deinit();
        self.table = try lease.LeaseTable.init(allocator, opts.max_stores, opts.lease);
        errdefer self.table.deinit();
        ts.setKvHooks(.{ .ctx = self, .on_append_done = onAppendDoneThunk });
        _ = tracker_mod.TopicSource.attachResetListener(ts, &self.tracker);
        return self;
    }

    pub fn destroy(self: *KvControl) void {
        self.table.deinit();
        self.tracker.deinit();
        self.allocator.destroy(self);
    }

    pub fn setLocalStores(self: *KvControl, ls: local_stores_mod.KvLocalStores) void {
        self.local_stores = ls;
    }

    fn isLeader(self: *KvControl) bool {
        const l = self.ts.election.getLeader() orelse return false;
        return l == self.local_node_id;
    }

    // ── Entry points ─────────────────────────────────────────────────────

    /// A KV message from a local service (templates 16–24).
    pub fn handleServiceMessage(self: *KvControl, template_id: u16, payload: []const u8, now_ns: i64) void {
        switch (template_id) {
            msgs.TEMPLATE_REGISTER_KV_WRITER => self.onRegister(payload, now_ns),
            msgs.TEMPLATE_KV_WRITER_REASSERT => self.onReassert(payload, now_ns),
            msgs.TEMPLATE_UNREGISTER_KV_WRITER => self.onUnregister(payload, now_ns),
            msgs.TEMPLATE_OPEN_KV_READER => self.onOpenReader(payload),
            else => log.warn("unexpected KV template {} from service", .{template_id}),
        }
    }

    /// Admin: a forwarded service request arrived at this (supposed) leader.
    pub fn handleAdminRequest(self: *KvControl, body: kv_messages.KvAdminRequestBody, now_ns: i64) void {
        const payload = body.payload();
        const template = kv_messages.templateOf(payload) orelse return;
        switch (template) {
            msgs.TEMPLATE_REGISTER_KV_WRITER => {
                const m = msgs.decode(msgs.RegisterKvWriterMsg, payload) orelse return;
                self.routeRegister(m, body.origin_node_id, now_ns);
            },
            msgs.TEMPLATE_KV_WRITER_REASSERT => {
                const m = msgs.decode(msgs.KvWriterReassertMsg, payload) orelse return;
                self.routeReassert(m, body.origin_node_id, now_ns);
            },
            msgs.TEMPLATE_UNREGISTER_KV_WRITER => {
                const m = msgs.decode(msgs.UnregisterKvWriterMsg, payload) orelse return;
                self.routeUnregister(m, body.origin_node_id, now_ns);
            },
            else => {},
        }
    }

    /// Admin: the leader's reply for one of our local services.
    pub fn handleAdminReply(self: *KvControl, body: kv_messages.KvAdminReplyBody) void {
        if (body.dest_node_id != self.local_node_id) return;
        _ = self.host.write_to_service(self.host.ctx, body.dest_service_id, body.payload());
    }

    /// A service (local or remote) disappeared (removed, or its broker lost).
    pub fn onServiceGone(self: *KvControl, node_id: u8, service_id: i32) void {
        self.table.serviceGone(.{ .node_id = node_id, .service_id = @bitCast(service_id) });
    }

    // ── Duty cycle ───────────────────────────────────────────────────────

    /// Called every control-loop iteration. Returns work count.
    pub fn doWork(self: *KvControl, now_ns: i64) u32 {
        var work: u32 = self.tracker.poll(now_ns);
        defer self.publishMetrics();
        const leader = self.isLeader();
        var out = lease.Outbox{};
        if (!leader) {
            if (self.was_leader) {
                for (self.table.slots) |*s| if (s.used) s.abandon(&out);
                self.was_leader = false;
                work += self.run(&out, now_ns);
            }
            return work;
        }
        self.was_leader = true;
        const idle_due = now_ns >= self.next_idle_step_ns;
        if (idle_due) self.next_idle_step_ns = now_ns + 10 * std.time.ns_per_ms;
        for (self.table.slots) |*s| {
            if (!s.used) continue;
            if (!idle_due and !s.needsStep()) continue;
            out.clear();
            s.step(self.buildView(s), now_ns, wallNs(), self.opts.lease, &out);
            if (out.len > 0) work += self.run(&out, now_ns);
        }
        return work;
    }

    fn publishMetrics(self: *KvControl) void {
        for (self.tracker.slots) |*slot| {
            if (!slot.used) continue;
            const metrics = @import("../kv/kv_subsystem.zig").KvSubsystem.metricsForLocalStores(self.local_stores, slot.topic_id) orelse continue;
            metrics.set(.writer_epoch, @intCast(slot.track.lastEpoch()));
            // Holder identity packs node_id in the high word and service_id in
            // the low word; zero means that the replicated lease is released.
            metrics.set(.writer_holder, if (slot.track.holder()) |h| @intCast((@as(u64, h.node_id) << 32) | h.service_id) else 0);
            metrics.set(.writer_waiters, if (self.table.find(slot.topic_id)) |store| store.waiterCount() else 0);
        }
    }

    fn wallNs() u64 {
        // Informational (`granted_at_ns`): ms resolution is enough.
        const ms = platform.Clock.epochMillis();
        return if (ms < 0) 0 else @as(u64, @intCast(ms)) * std.time.ns_per_ms;
    }

    fn buildView(self: *KvControl, s: *lease.StoreLease) lease.View {
        const id = s.store_id;
        var v = lease.View{ .ready = false };
        const known = self.ts.registry.getById(id) != null;
        if (!known) {
            // Nothing exists yet: this leader may create the store.
            v.ready = true;
            return v;
        }
        const st = self.ts.topicStatus(id) orelse return v;
        const slot = self.tracker.find(id) orelse return v;
        // A cached caught_up flag describes the last tracker poll, which can
        // precede a newly replicated grant, barrier completion, or a reset.
        // Require the current local tip and reset generation as well.
        var reset_current = false;
        for (self.tracker.slot_ids, self.tracker.reset_seqs) |*slot_id, *reset_seq| {
            if (slot_id.load(.acquire) == id) {
                reset_current = slot.seen_reset == reset_seq.load(.acquire);
                break;
            }
        }
        v.ready = st.role == .leader and st.barrier_cleared and slot.caught_up and
            slot.track.tracked_index >= st.hwm_index and reset_current;
        v.header_seen = slot.track.config != null;
        if (slot.track.config) |c| v.config = c;
        v.holder = slot.track.holder();
        v.last_epoch = slot.track.lastEpoch();
        v.tracked_index = slot.track.tracked_index;
        if (v.holder) |h| {
            v.holder_registered = self.host.is_service_registered(self.host.ctx, @intCast(h.node_id), @bitCast(h.service_id));
        }
        return v;
    }

    // ── Action execution ─────────────────────────────────────────────────

    fn run(self: *KvControl, out: *lease.Outbox, now_ns: i64) u32 {
        var i: usize = 0;
        while (i < out.len) : (i += 1) self.exec(out.items[i], out, now_ns);
        return @intCast(out.len);
    }

    fn exec(self: *KvControl, a: lease.Action, out: *lease.Outbox, now_ns: i64) void {
        switch (a) {
            .reply => |r| self.deliver(r),
            .create_store => |c| self.execCreate(c.store_id, c.corr, c.config, out, now_ns),
            .append_grant => |g| {
                var b: [kv.format.frame_header_size + @sizeOf(kv.KvWriterGrant)]u8 = undefined;
                const frame = kv.format.encodeWriterGrant(&b, self.ts.election.getEpoch(), g.grant) catch unreachable;
                self.append(g.store_id, g.corr, frame, out, now_ns);
            },
            .append_release => |g| {
                var b: [kv.format.frame_header_size + @sizeOf(kv.KvWriterRelease)]u8 = undefined;
                const frame = kv.format.encodeWriterRelease(&b, self.ts.election.getEpoch(), g.release) catch unreachable;
                self.append(g.store_id, g.corr, frame, out, now_ns);
            },
        }
    }

    fn execCreate(self: *KvControl, store_id: u64, corr: u64, config: kv.KvStoreConfig, out: *lease.Outbox, now_ns: i64) void {
        const s = self.table.find(store_id) orelse return;
        var nb: [kv.format.max_topic_name_len]u8 = undefined;
        const topic_name = kv.format.topicNameFor(s.storeName(), &nb) catch {
            self.table.onAppendDone(store_id, corr, false, 0, now_ns, out);
            return;
        };
        const roll = std.mem.sliceTo(&config.topic_roll_scheme, 0);
        const res = self.host.create_kv_topic(self.host.ctx, topic_name, roll);
        if (res.status != .ok) {
            log.warn("create KV topic '{s}' failed: {s}", .{ topic_name, @tagName(res.status) });
            self.table.onAppendDone(store_id, corr, false, 0, now_ns, out);
            return;
        }
        self.tracker.next_discovery_ns = 0; // discover the new topic promptly
        var b: [kv.format.frame_header_size + @sizeOf(kv.KvStoreConfig)]u8 = undefined;
        const frame = kv.format.encodeStoreHeader(&b, self.ts.election.getEpoch(), config) catch unreachable;
        self.append(store_id, corr, frame, out, now_ns);
    }

    fn append(self: *KvControl, store_id: u64, corr: u64, frame: []const u8, out: *lease.Outbox, now_ns: i64) void {
        switch (self.ts.kvLeaderAppend(store_id, corr, frame)) {
            .queued => {},
            .payload_invalid, .queue_full => self.table.onAppendDone(store_id, corr, false, 0, now_ns, out),
        }
    }

    fn onAppendDoneThunk(ctx: ?*anyopaque, topic_id: u64, corr: u64, result: topics.subsystem.KvAppendResult, index: u64) void {
        const self: *KvControl = @ptrCast(@alignCast(ctx.?));
        var out = lease.Outbox{};
        const now_ns = platform.Clock.monotonicNanos();
        self.table.onAppendDone(topic_id, corr, result == .ok, index, now_ns, &out);
        _ = self.run(&out, now_ns);
    }

    // ── Reply delivery ───────────────────────────────────────────────────

    fn leaderNode(self: *KvControl) i16 {
        return if (self.ts.election.getLeader()) |l| @intCast(l) else -1;
    }

    fn deliver(self: *KvControl, r: lease.Reply) void {
        const leader_epoch = self.ts.election.getEpoch();
        const leader = self.leaderNode();
        const n: usize = switch (r.kind) {
            .writer_response => msgs.encodeKvWriterResponse(&self.buf, r.status, r.store_id, r.nonce, r.epoch, leader_epoch, leader, r.config),
            .writer_granted => msgs.encodeKvWriterGranted(&self.buf, r.store_id, r.nonce, r.epoch, leader_epoch, leader),
            .writer_revoked => msgs.encodeKvWriterRevoked(&self.buf, r.reason, r.store_id, r.nonce, r.epoch, r.current_epoch),
            .reassert_response => msgs.encodeKvWriterReassertResponse(&self.buf, r.status, r.store_id, r.nonce, r.epoch, leader_epoch, leader),
        };
        const bytes = self.buf[0..n];
        const sid: i32 = @bitCast(r.to.service_id);
        if (r.to.node_id == self.local_node_id) {
            _ = self.host.write_to_service(self.host.ctx, sid, bytes);
        } else if (kv_messages.makeReply(@intCast(r.to.node_id), sid, bytes)) |body| {
            self.host.send_admin_reply(self.host.ctx, @intCast(r.to.node_id), body);
        }
    }

    /// Direct failure reply (no lease slot involved).
    fn fail(self: *KvControl, kind: lease.ReplyKind, to: lease.Party, store_id: u64, nonce: u64, status: msgs.KvStatus, config: kv.KvStoreConfig) void {
        self.deliver(.{ .to = to, .kind = kind, .store_id = store_id, .status = status, .nonce = nonce, .config = config });
    }

    // ── Service requests ─────────────────────────────────────────────────

    fn validName(name: []const u8) bool {
        return kv.format.isValidStoreName(name);
    }

    fn onRegister(self: *KvControl, payload: []const u8, now_ns: i64) void {
        const m = msgs.decode(msgs.RegisterKvWriterMsg, payload) orelse return;
        const party = lease.Party{ .node_id = self.local_node_id, .service_id = @bitCast(m.local_service_id) };
        const name = msgs.nameOf(&m.name, m.name_length);
        const sid = kv.format.storeIdOf(name) catch 0;
        if (!validName(name)) return self.fail(.writer_response, party, sid, m.session_nonce, .invalid_name, m.config);
        if (!self.ts.enabled) return self.fail(.writer_response, party, sid, m.session_nonce, .topics_disabled, m.config);
        m.config.validate(self.opts.payloadBudget()) catch
            return self.fail(.writer_response, party, sid, m.session_nonce, .invalid_config, m.config);
        if (!topic_store.isKnownRollScheme(topic_config.TopicConfig.fromName(std.mem.sliceTo(&m.config.topic_roll_scheme, 0), 0, false)))
            return self.fail(.writer_response, party, sid, m.session_nonce, .invalid_config, m.config);
        if (m.acquire_mode > @backingInt(msgs.AcquireMode.wait))
            return self.fail(.writer_response, party, sid, m.session_nonce, .bad_request, m.config);

        const leader = self.ts.election.getLeader() orelse
            return self.fail(.writer_response, party, sid, m.session_nonce, .leader_unavailable, m.config);
        if (leader == self.local_node_id) return self.routeRegister(m, self.local_node_id, now_ns);
        self.forward(leader, payload, .writer_response, party, sid, m.session_nonce);
    }

    fn forward(self: *KvControl, leader: u8, payload: []const u8, kind: lease.ReplyKind, party: lease.Party, sid: u64, nonce: u64) void {
        const body = kv_messages.makeRequest(self.local_node_id, payload) orelse
            return self.fail(kind, party, sid, nonce, .bad_request, .{});
        self.host.send_admin_request(self.host.ctx, leader, body);
    }

    /// Runs on the leader.
    fn routeRegister(self: *KvControl, m: msgs.RegisterKvWriterMsg, origin_node: u8, now_ns: i64) void {
        const party = lease.Party{ .node_id = origin_node, .service_id = @bitCast(m.local_service_id) };
        const name = msgs.nameOf(&m.name, m.name_length);
        const sid = kv.format.storeIdOf(name) catch return;
        if (!self.isLeader()) return self.fail(.writer_response, party, sid, m.session_nonce, .leader_unavailable, m.config);
        // Forwarded requests were checked by their origin broker, whose
        // transport frame limit can differ from this leader's limit.
        m.config.validate(self.opts.payloadBudget()) catch
            return self.fail(.writer_response, party, sid, m.session_nonce, .invalid_config, m.config);
        if (!topic_store.isKnownRollScheme(topic_config.TopicConfig.fromName(std.mem.sliceTo(&m.config.topic_roll_scheme, 0), 0, false)))
            return self.fail(.writer_response, party, sid, m.session_nonce, .invalid_config, m.config);
        if (m.acquire_mode > @backingInt(msgs.AcquireMode.wait))
            return self.fail(.writer_response, party, sid, m.session_nonce, .bad_request, m.config);
        const res = self.table.submit(sid, name, .{
            .kind = .register,
            .who = .{ .party = party, .nonce = m.session_nonce },
            .mode = @fromBackingInt(@intCast(m.acquire_mode)),
            .config = m.config,
            .received_ns = now_ns,
        });
        switch (res) {
            .accepted => {},
            .store_limit => self.fail(.writer_response, party, sid, m.session_nonce, .store_limit, m.config),
            .inbox_full => self.fail(.writer_response, party, sid, m.session_nonce, .not_ready, m.config),
        }
    }

    fn onReassert(self: *KvControl, payload: []const u8, now_ns: i64) void {
        const m = msgs.decode(msgs.KvWriterReassertMsg, payload) orelse return;
        const party = lease.Party{ .node_id = self.local_node_id, .service_id = @bitCast(m.local_service_id) };
        const name = msgs.nameOf(&m.name, m.name_length);
        const sid = kv.format.storeIdOf(name) catch 0;
        if (!validName(name)) return self.fail(.reassert_response, party, sid, m.session_nonce, .invalid_name, .{});
        const leader = self.ts.election.getLeader() orelse
            return self.fail(.reassert_response, party, sid, m.session_nonce, .leader_unavailable, .{});
        if (leader == self.local_node_id) return self.routeReassert(m, self.local_node_id, now_ns);
        self.forward(leader, payload, .reassert_response, party, sid, m.session_nonce);
    }

    fn routeReassert(self: *KvControl, m: msgs.KvWriterReassertMsg, origin_node: u8, now_ns: i64) void {
        const party = lease.Party{ .node_id = origin_node, .service_id = @bitCast(m.local_service_id) };
        const name = msgs.nameOf(&m.name, m.name_length);
        const sid = kv.format.storeIdOf(name) catch return;
        if (!self.isLeader()) return self.fail(.reassert_response, party, sid, m.session_nonce, .leader_unavailable, .{});
        const res = self.table.submit(sid, name, .{
            .kind = .reassert,
            .who = .{ .party = party, .nonce = m.session_nonce },
            .epoch = m.writer_epoch,
            .received_ns = now_ns,
        });
        switch (res) {
            .accepted => {},
            .store_limit => self.fail(.reassert_response, party, sid, m.session_nonce, .store_limit, .{}),
            .inbox_full => self.fail(.reassert_response, party, sid, m.session_nonce, .not_ready, .{}),
        }
    }

    fn onUnregister(self: *KvControl, payload: []const u8, now_ns: i64) void {
        const m = msgs.decode(msgs.UnregisterKvWriterMsg, payload) orelse return;
        const name = msgs.nameOf(&m.name, m.name_length);
        if (!validName(name)) return;
        const leader = self.ts.election.getLeader() orelse return;
        if (leader == self.local_node_id) return self.routeUnregister(m, self.local_node_id, now_ns);
        const party = lease.Party{ .node_id = self.local_node_id, .service_id = @bitCast(m.local_service_id) };
        self.forward(leader, payload, .writer_response, party, 0, m.session_nonce);
    }

    fn routeUnregister(self: *KvControl, m: msgs.UnregisterKvWriterMsg, origin_node: u8, now_ns: i64) void {
        if (!self.isLeader()) return;
        const name = msgs.nameOf(&m.name, m.name_length);
        const sid = kv.format.storeIdOf(name) catch return;
        _ = self.table.submit(sid, name, .{
            .kind = .unregister,
            .who = .{ .party = .{ .node_id = origin_node, .service_id = @bitCast(m.local_service_id) }, .nonce = m.session_nonce },
            .epoch = m.writer_epoch,
            .received_ns = now_ns,
        });
    }

    fn onOpenReader(self: *KvControl, payload: []const u8) void {
        const m = msgs.decode(msgs.OpenKvReaderMsg, payload) orelse return;
        const name = msgs.nameOf(&m.name, m.name_length);
        var out: [@sizeOf(msgs.KvReaderResponseMsg)]u8 = undefined;
        var dir: [msgs.kv_store_dir_max]u8 = undefined;
        if (!validName(name)) {
            const n = msgs.encodeKvReaderResponse(&out, .invalid_name, 0, 0, .{}, "").?;
            _ = self.host.write_to_service(self.host.ctx, m.local_service_id, out[0..n]);
            return;
        }
        const sid = kv.format.storeIdOf(name) catch 0;
        const r = self.local_stores.open(name, &dir);
        const n = if (r.store) |s|
            msgs.encodeKvReaderResponse(&out, r.status, s.store_id, s.engine_kind, s.config, dir[0..s.dir_len]) orelse
                msgs.encodeKvReaderResponse(&out, .unknown_store, sid, 0, .{}, "").?
        else
            msgs.encodeKvReaderResponse(&out, r.status, sid, 0, .{}, "").?;
        _ = self.host.write_to_service(self.host.ctx, m.local_service_id, out[0..n]);
    }
};

const testing = std.testing;

test "party mapping preserves negative-free service ids through bitcast" {
    const sid: i32 = 123;
    const p = lease.Party{ .node_id = 2, .service_id = @bitCast(sid) };
    const back: i32 = @bitCast(p.service_id);
    try testing.expectEqual(sid, back);
}

test "registration validation subtracts publish header from configured frame limit" {
    const opts: Options = .{ .max_frame_length = 1024 };
    try testing.expectEqual(@as(u32, 976), opts.payloadBudget());
    var config: kv.KvStoreConfig = .{ .max_value_len = 400 };
    try config.validate(opts.payloadBudget());
    config.max_value_len = 430;
    try config.validate(opts.max_frame_length);
    try testing.expectError(error.InvalidValueLen, config.validate(opts.payloadBudget()));
    try testing.expectEqual(@as(u32, 0), (Options{ .max_frame_length = 1 }).payloadBudget());
}

test "arbitration rejects cached caught-up state after a new grant, promotion, or reset" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const base = try a.print(".zig-cache/tmp/{s}", .{tmp.sub_path});
    defer a.free(base);
    const Impl = struct {
        fn offer(_: *anyopaque, _: u8, _: []const u8) i64 {
            return 1;
        }
        fn connected(_: *anyopaque, _: u8) bool {
            return false;
        }
        fn registered(_: *anyopaque, _: u8, _: i32) bool {
            return true;
        }
    };
    const ts = try topics.TopicSubsystem.init(a, .{ .enabled = true, .base_dir = base, .write_runway_bytes = 1 << 20, .read_runway_bytes = 1 << 20, .prefetcher_cpu_affinity = -1, .local_node_id = 1, .io = platform.defaultIo() }, .{ .ctx = undefined, .offerFn = Impl.offer, .isConnectedFn = Impl.connected, .isBackPressuredFn = Impl.connected }, 0);
    defer ts.deinit();
    _ = ts.checkLeadership(10 * std.time.ns_per_s);
    const created = ts.createKvStoreTopic("__kv.lease-view", "FAST_DAILY", 0);
    try testing.expectEqual(@TypeOf(created.status).ok, created.status);
    var bytes: [256]u8 = undefined;
    const header = try kv.format.encodeStoreHeader(&bytes, 1, .{});
    try testing.expectEqual(topics.TopicSubsystem.AppendEnqueue.queued, ts.kvLeaderAppend(created.topic_id, 1, header));
    _ = ts.receiverStep();
    const control = try KvControl.create(a, ts, 1, .{ .ctx = undefined, .write_to_service = undefined, .send_admin_request = undefined, .send_admin_reply = undefined, .is_service_registered = Impl.registered, .create_kv_topic = undefined }, .{ .max_stores = 2 });
    defer control.destroy();
    _ = control.tracker.poll(1);
    const ks = try @import("../kv/kv_subsystem.zig").KvSubsystem.create(a, ts, .{ .max_stores = 2 }, base, "metrics", 1);
    defer ks.destroy();
    var values: [128 * 80]u8 align(128) = @splat(0);
    var metadata: [256 * 80]u8 align(4) = @splat(0);
    var manager = common.concurrent.counters.CountersManager.init(&values, &metadata);
    try ks.bindCounters(&manager);
    _ = ks.doWork(1);
    control.setLocalStores(ks.localStores());
    const store = control.table.getOrCreate(created.topic_id, "lease-view").?;
    try testing.expect(control.buildView(store).ready);
    const grant = try kv.format.encodeWriterGrant(&bytes, 1, .{ .writer_epoch = 7, .holder_session_nonce = 99, .holder_service_id = 3, .holder_node_id = 2, .granted_at_ns = 0 });
    try testing.expectEqual(topics.TopicSubsystem.AppendEnqueue.queued, ts.kvLeaderAppend(created.topic_id, 2, grant));
    _ = ts.receiverStep();
    const tracked = control.tracker.find(created.topic_id).?;
    try testing.expect(tracked.caught_up); // intentionally stale cached flag
    try testing.expect(!control.buildView(store).ready);
    const queue = ts.store.get(created.topic_id).?;
    queue.role = .replica;
    queue.accepting_writes = false;
    try testing.expect(!control.buildView(store).ready);
    queue.role = .leader;
    queue.accepting_writes = true; // promotion/barrier completion
    try testing.expect(!control.buildView(store).ready);
    _ = control.tracker.poll(control.tracker.next_poll_ns);
    const fresh = control.buildView(store);
    try testing.expect(fresh.ready);
    try testing.expectEqual(@as(u64, 7), fresh.holder.?.epoch);
    control.publishMetrics();
    const metrics = &ks.slots[0].metrics.?;
    try testing.expectEqual(7, metrics.get(.writer_epoch));
    try testing.expectEqual(@as(i64, (2 << 32) | 3), metrics.get(.writer_holder));
    store.waiters_len = 2;
    control.publishMetrics();
    try testing.expectEqual(2, metrics.get(.writer_waiters));
    store.waiters_len = 0;
    control.tracker.noteReset(created.topic_id);
    try testing.expect(tracked.caught_up);
    try testing.expect(!control.buildView(store).ready);
    _ = control.tracker.poll(control.tracker.next_poll_ns);
    try testing.expect(control.buildView(store).ready);
}
