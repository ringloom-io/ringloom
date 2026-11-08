// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const common = @import("ringloom_common");
const types = @import("kv_types.zig");
const Reader = @import("kv_reader.zig").KvReader;
const Writer = @import("kv_writer.zig").KvWriter;
const Publisher = @import("../topics/topic_publisher.zig").TopicPublisher;
const Runtime = @import("../aeron_runtime.zig").ServiceAeronRuntime;
const msgs = common.message.kv_control_messages;
const topic_msgs = common.message.topic_control_messages;
const format = common.kv.format;
const Clock = common.platform.Clock;
const RingBuffer = common.concurrent.ring_buffer.RingBuffer;

/// Per-engine control state. The control ring has one consumer: threaded opens
/// wait for dispatch; manual opens call the engine's existing poller. Opens are
/// serialized because reader replies have no request nonce on the wire.
pub const KvContext = struct {
    allocator: std.mem.Allocator,
    broker: ?*common.memory.BrokerMetadataFile = null,
    service_id: i32 = 0,
    node_id: i16 = 0,
    mutex: std.atomic.Mutex = .unlocked,
    open_mutex: std.atomic.Mutex = .unlocked,
    writers: ?*Writer = null,
    runtime: ?Runtime = null,
    active: bool = true,
    reader_response: ?msgs.KvReaderResponseMsg = null,
    reader_waiting: bool = false,
    reader_store_id: u64 = 0,
    topic_response: ?topic_msgs.TopicPublicationResponseMsg = null,
    topic_waiting: ?u64 = null,
    poll_ctx: ?*anyopaque = null,
    poll_fn: ?*const fn (*anyopaque) void = null,

    pub fn testContext(a: std.mem.Allocator) KvContext {
        return .{ .allocator = a };
    }
    pub fn lock(self: *KvContext) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }
    pub fn unlock(self: *KvContext) void {
        self.mutex.unlock();
    }
    fn openLock(self: *KvContext) void {
        while (!self.open_mutex.tryLock()) common.platform.sleepNanos(std.time.ns_per_ms);
    }
    fn send(self: *KvContext, bytes: []const u8) !void {
        const b = self.broker orelse return error.EngineNotRunning;
        var rb = try RingBuffer.init(@alignCast(b.getControlBuffer()), false, null, null);
        try rb.write(common.memory.constants.control_msg_type_id, bytes);
    }
    fn waitStep(self: *KvContext) void {
        if (self.poll_fn) |poll| poll(self.poll_ctx.?);
        common.platform.sleepNanos(std.time.ns_per_ms);
    }
    fn deadline(ms: u32) i64 {
        return Clock.monotonicNanos() + @as(i64, ms) * std.time.ns_per_ms;
    }

    pub fn openReader(self: *KvContext, name: []const u8, opts: types.KvReaderOptions) !*Reader {
        if (!format.isValidStoreName(name)) return error.InvalidName;
        self.openLock();
        defer self.open_mutex.unlock();
        const end = deadline(if (opts.wait_ready_timeout_ms == 0) 5000 else opts.wait_ready_timeout_ms);
        while (true) {
            self.lock();
            if (!self.active) {
                self.unlock();
                return error.EngineNotRunning;
            }
            self.reader_store_id = format.storeIdOf(name) catch unreachable;
            self.reader_response = null;
            self.reader_waiting = true;
            var buf: [@sizeOf(msgs.OpenKvReaderMsg)]u8 = undefined;
            const n = msgs.encodeOpenKvReader(&buf, self.service_id, name);
            self.send(buf[0..n]) catch {
                self.reader_waiting = false;
                self.unlock();
                return error.SendFailed;
            };
            self.unlock();
            const response = while (true) {
                self.lock();
                const response = self.reader_response;
                if (response != null) self.reader_waiting = false;
                self.unlock();
                if (response) |r| break r;
                if (Clock.monotonicNanos() >= end) {
                    self.lock();
                    self.reader_waiting = false;
                    self.unlock();
                    return error.Timeout;
                }
                self.waitStep();
            };
            const status = msgs.statusOf(response.status) orelse return error.BadRequest;
            if ((status == .not_ready or status == .unknown_store) and opts.wait_ready_timeout_ms > 0 and Clock.monotonicNanos() < end) {
                self.waitStep();
                continue;
            }
            // not_ready can still carry a valid local control directory.
            if (status != .ok and !(status == .not_ready and response.store_dir_length > 0)) try statusError(status);
            if (response.engine_kind != common.kv.engine.engine_kind) return error.EngineMismatch;
            const r = try Reader.openLocal(self.allocator, msgs.readerStoreDir(&response), response.config, opts);
            errdefer r.close();
            while (opts.wait_ready_timeout_ms > 0 and !r.isReady()) {
                if (Clock.monotonicNanos() >= end) return error.NotReady;
                self.waitStep();
            }
            if (opts.prefault) r.prefault();
            return r;
        }
    }

    pub fn openWriter(self: *KvContext, name: []const u8, cfg: types.KvStoreConfig, opts: types.KvWriterOptions) !*Writer {
        if (!format.isValidStoreName(name)) return error.InvalidName;
        // TopicPublishHeader is inside the broker's max_frame_length budget.
        const overhead = common.message.topic_data_header.TopicPublishHeader.encoded_length;
        if (opts.max_frame_length <= overhead + format.frame_header_size) return error.InvalidConfig;
        const payload_limit = opts.max_frame_length - overhead;
        cfg.validate(@intCast(payload_limit)) catch return error.InvalidConfig;
        self.openLock();
        defer self.open_mutex.unlock();
        const w = try self.allocator.create(Writer);
        errdefer self.allocator.destroy(w);
        const buffer = try self.allocator.alloc(u8, payload_limit);
        errdefer self.allocator.free(buffer);
        const scratch = try self.allocator.alloc(u8, opts.max_frame_length + common.message.data_header.RingLoomDataHeader.encoded_length);
        errdefer self.allocator.free(scratch);
        const id = try format.storeIdOf(name);
        w.* = .{ .allocator = self.allocator, .context = self, .name_len = name.len, .nonce = Publisher.newPublisherId(), .config = cfg, .options = opts, .publisher = .{ .topic_id = id, .publisher_id = Publisher.newPublisherId(), .source_node_id = @intCast(self.node_id), .source_service_id = @intCast(self.service_id), .leader_node_id = 0, .leader_epoch = 0, .scratch = scratch }, .buffer = buffer, .scratch = scratch, .builder = format.DataFrameBuilder.init(buffer, 0, 0).withLimits(cfg.max_key_len, cfg.max_value_len) };
        @memcpy(w.name_buf[0..name.len], name);
        self.lock();
        if (!self.active) {
            self.unlock();
            return error.EngineNotRunning;
        }
        if (self.runtime == null) {
            self.runtime = Runtime.connect(self.allocator, self.broker.?.getAeronDiscovery()) catch {
                self.unlock();
                return error.LeaderPublicationFailed;
            };
        }
        w.next = self.writers;
        self.writers = w;
        var buf: [@sizeOf(msgs.RegisterKvWriterMsg)]u8 = undefined;
        const n = msgs.encodeRegisterKvWriter(&buf, self.service_id, w.nonce, cfg, opts.acquire, name);
        self.send(buf[0..n]) catch {
            self.writers = w.next;
            self.unlock();
            return error.SendFailed;
        };
        self.unlock();
        errdefer self.closeWriter(w);
        const end = deadline(opts.open_timeout_ms);
        while (true) {
            self.lock();
            const raw = w.response_status;
            self.unlock();
            if (raw) |s| {
                try statusError(msgs.statusOf(s) orelse return error.BadRequest);
                break;
            }
            if (Clock.monotonicNanos() >= end) return error.Timeout;
            self.waitStep();
        }
        // Register as a topic producer as well: the broker uses this membership
        // for leader-change broadcasts and routes per-publisher ack feedback.
        var name_buf: [format.max_topic_name_len]u8 = undefined;
        const topic_name = try format.topicNameFor(name, &name_buf);
        const tc = common.topics.TopicConfig{ .roll_scheme_name = w.config.topic_roll_scheme, .flags = common.topics.flag_kv_store };
        var tb: [512]u8 = undefined;
        const tn = topic_msgs.encodeRegisterTopicPublicationFlags(&tb, self.service_id, tc, topic_name, topic_msgs.msg_flag_allow_reserved);
        self.lock();
        self.topic_waiting = id;
        self.topic_response = null;
        self.send(tb[0..tn]) catch {
            self.topic_waiting = null;
            self.unlock();
            return error.SendFailed;
        };
        self.unlock();
        defer {
            self.lock();
            self.topic_waiting = null;
            self.unlock();
        }
        while (true) {
            self.lock();
            const response = self.topic_response;
            self.unlock();
            if (response) |r| {
                if (r.status != @backingInt(topic_msgs.PublicationStatus.ok)) return error.LeaderPublicationFailed;
                break;
            }
            if (Clock.monotonicNanos() >= end) return error.Timeout;
            self.waitStep();
        }
        return w;
    }

    fn retarget(self: *KvContext, w: *Writer, node: i16, epoch: u64) void {
        if (epoch < w.publisher.leader_epoch) return;
        if (node == w.publisher.leader_node_id and w.publisher.leader_pub != null) {
            w.publisher.leader_epoch = epoch;
            return;
        }
        if (w.publisher.leader_pub) |*p| p.close() catch {};
        w.publisher.leader_pub = null;
        w.publisher.leader_epoch = epoch;
        if (node <= 0 or node > 255) return;
        w.publisher.leader_node_id = @intCast(node);
        const runtime = if (self.runtime) |*r| r else return;
        var channel_buf: [common.memory.constants.max_aeron_channel_length + 1]u8 = undefined;
        const channel: [:0]const u8 = if (node == self.node_id) "aeron:ipc" else blk: {
            const peer = self.broker.?.getAeronDiscovery().peerByNodeId(@intCast(node)) orelse return;
            break :blk std.mem.printSentinel(&channel_buf, "{s}", .{peer.dataChannel()}, 0) catch return;
        };
        // Remote topic frames use the broker data ingress stream; local topic
        // frames have a dedicated IPC stream (same convention as topic API).
        const stream: i32 = if (node == self.node_id) 50_000 + @as(i32, node) else blk: {
            const peer = self.broker.?.getAeronDiscovery().peerByNodeId(@intCast(node)) orelse return;
            break :blk peer.data_stream_id;
        };
        w.publisher.leader_pub = runtime.client.addExclusivePublication(channel, stream, null) catch null;
    }

    /// Called on the single control consumer. Callback code must not re-enter
    /// the KV API; transitions are delivered under the context lock.
    pub fn dispatch(self: *KvContext, payload: []const u8) void {
        if (payload.len < 4) return;
        const template = std.mem.readInt(u16, payload[0..2], .little);
        self.lock();
        defer self.unlock();
        switch (template) {
            msgs.TEMPLATE_KV_READER_RESPONSE => {
                const m = msgs.decode(msgs.KvReaderResponseMsg, payload) orelse return;
                if (self.reader_waiting and (m.store_id == self.reader_store_id or m.store_id == 0)) self.reader_response = m;
                return;
            },
            topic_msgs.TEMPLATE_TOPIC_PUBLICATION_RESPONSE => {
                const m = topic_msgs.decode(topic_msgs.TopicPublicationResponseMsg, payload) orelse return;
                if (self.topic_waiting != null and self.topic_waiting.? == m.topic_id) self.topic_response = m;
                return;
            },
            else => {},
        }
        var iter = self.writers;
        while (iter) |w| : (iter = w.next) {
            switch (template) {
                msgs.TEMPLATE_KV_WRITER_RESPONSE => {
                    const m = msgs.decode(msgs.KvWriterResponseMsg, payload) orelse return;
                    if (m.session_nonce != w.nonce or (m.store_id != 0 and m.store_id != w.publisher.topic_id)) continue;
                    if (w.response_status != null and w.writer_state != .queued) continue;
                    if (m.leader_epoch < w.publisher.leader_epoch) continue;
                    w.response_status = m.status;
                    if (m.status == @backingInt(msgs.KvStatus.ok) or m.status == @backingInt(msgs.KvStatus.queued)) {
                        w.config = m.config;
                        w.builder.max_key_len = m.config.max_key_len;
                        w.builder.max_value_len = m.config.max_value_len;
                        self.retarget(w, m.leader_node_id, m.leader_epoch);
                        w.transition(if (m.status == 0) .granted else .queued, m.writer_epoch);
                    }
                },
                msgs.TEMPLATE_KV_WRITER_GRANTED => {
                    const m = msgs.decode(msgs.KvWriterGrantedMsg, payload) orelse return;
                    if (m.session_nonce != w.nonce or m.store_id != w.publisher.topic_id or m.leader_epoch < w.publisher.leader_epoch or m.writer_epoch < w.epoch) continue;
                    self.retarget(w, m.leader_node_id, m.leader_epoch);
                    w.transition(.granted, m.writer_epoch);
                },
                msgs.TEMPLATE_KV_WRITER_REVOKED => {
                    const m = msgs.decode(msgs.KvWriterRevokedMsg, payload) orelse return;
                    if (m.session_nonce != w.nonce or m.store_id != w.publisher.topic_id or (m.writer_epoch != 0 and m.writer_epoch != w.epoch)) continue;
                    w.transition(.revoked, w.epoch);
                },
                msgs.TEMPLATE_KV_WRITER_REASSERT_RESPONSE => {
                    const m = msgs.decode(msgs.KvWriterReassertResponseMsg, payload) orelse return;
                    if (m.session_nonce != w.nonce or m.store_id != w.publisher.topic_id or m.leader_epoch < w.publisher.leader_epoch or w.writer_state != .reasserting) continue;
                    if (m.status == @backingInt(msgs.KvStatus.ok) and m.writer_epoch != 0) {
                        self.retarget(w, m.leader_node_id, m.leader_epoch);
                        w.transition(.granted, m.writer_epoch);
                    } else if (m.status == @backingInt(msgs.KvStatus.not_writer)) w.transition(.revoked, w.epoch);
                },
                topic_msgs.TEMPLATE_TOPIC_LEADER_CHANGED => {
                    const m = topic_msgs.decode(topic_msgs.TopicLeaderChangedMsg, payload) orelse return;
                    if (m.topic_id != w.publisher.topic_id or m.leader_epoch <= w.publisher.leader_epoch or w.writer_state == .revoked) continue;
                    self.retarget(w, m.leader_node_id, m.leader_epoch);
                    if (w.writer_state == .queued) {
                        // Re-register the same nonce on the new leader to
                        // recover queue membership lost with the old leader.
                        w.next_retry_ns = 0;
                    } else {
                        w.transition(.reasserting, w.epoch);
                        w.next_retry_ns = 0;
                    }
                },
                topic_msgs.TEMPLATE_TOPIC_ACK_FEEDBACK => {
                    const m = topic_msgs.decode(topic_msgs.TopicAckFeedbackMsg, payload) orelse return;
                    if (m.topic_id == w.publisher.topic_id) w.publisher.applyAckFeedback(m.leader_epoch, m.publisher_id, m.replicated_hwm, m.acked_seq);
                },
                else => {},
            }
        }
    }

    pub fn doWork(self: *KvContext) void {
        self.lock();
        defer self.unlock();
        if (!self.active) return;
        if (self.runtime) |*rt| _ = rt.doWork() catch {};
        const now = Clock.monotonicNanos();
        var iter = self.writers;
        while (iter) |w| : (iter = w.next) {
            if (w.writer_state == .granted and w.publisher.leader_pub == null and now >= w.next_retry_ns) {
                w.next_retry_ns = now + 100 * std.time.ns_per_ms;
                self.retarget(w, w.publisher.leader_node_id, w.publisher.leader_epoch);
            }
            if (w.writer_state != .reasserting and w.writer_state != .queued) continue;
            if (now < w.next_retry_ns) continue;
            w.next_retry_ns = now + 100 * std.time.ns_per_ms;
            var b: [@sizeOf(msgs.RegisterKvWriterMsg)]u8 = undefined;
            const n = if (w.writer_state == .reasserting)
                msgs.encodeKvWriterReassert(&b, self.service_id, w.nonce, w.epoch, w.name())
            else
                msgs.encodeRegisterKvWriter(&b, self.service_id, w.nonce, w.config, w.options.acquire, w.name());
            self.send(b[0..n]) catch {};
        }
    }

    pub fn closeWriter(self: *KvContext, w: *Writer) void {
        self.lock();
        defer self.unlock();
        var link = &self.writers;
        while (link.*) |p| {
            if (p == w) {
                link.* = p.next;
                break;
            }
            link = &p.next;
        }
        if (self.active) {
            var buf: [@sizeOf(msgs.UnregisterKvWriterMsg)]u8 = undefined;
            const n = msgs.encodeUnregisterKvWriter(&buf, self.service_id, w.nonce, w.epoch, w.name());
            self.send(buf[0..n]) catch {};
            var same_topic = false;
            var it = self.writers;
            while (it) |other| : (it = other.next) {
                if (other.publisher.topic_id == w.publisher.topic_id) {
                    same_topic = true;
                    break;
                }
            }
            if (!same_topic) {
                const count = topic_msgs.encodeUnregisterTopicPublication(&buf, self.service_id, w.publisher.topic_id);
                self.send(buf[0..count]) catch {};
            }
        }
        if (w.publisher.leader_pub) |*p| p.close() catch {};
        w.publisher.leader_pub = null;
        w.context = null;
        w.writer_state = .revoked;
    }
    pub fn stop(self: *KvContext) void {
        while (self.writers) |w| self.closeWriter(w);
        self.lock();
        defer self.unlock();
        self.active = false;
        if (self.runtime) |*r| r.deinit();
        self.runtime = null;
    }
};

fn statusError(s: msgs.KvStatus) !void {
    return switch (s) {
        .ok, .queued => {},
        .held_by_other => error.HeldByOther,
        .config_mismatch => error.ConfigMismatch,
        .not_writer => error.NotWriter,
        .leader_unavailable => error.LeaderUnavailable,
        .kv_not_available_locally => error.KvNotAvailableLocally,
        .not_ready => error.NotReady,
        .invalid_name => error.InvalidName,
        .invalid_config => error.InvalidConfig,
        .topics_disabled => error.TopicsDisabled,
        .unknown_store => error.UnknownStore,
        .store_limit => error.StoreLimit,
        .bad_request => error.BadRequest,
    };
}

test "KV dispatch matches nonce, fences on leader change, retries queues, ignores stale grants and revokes" {
    const t = std.testing;
    var ctx = KvContext.testContext(t.allocator);
    var storage: [256]u8 = undefined;
    const id: u64 = 77;
    var w = Writer{ .allocator = t.failing_allocator, .context = &ctx, .name_len = 1, .nonce = 123, .config = .{}, .options = .{}, .publisher = .{ .topic_id = id, .publisher_id = 456, .leader_node_id = 1, .leader_epoch = 3 }, .buffer = &storage, .scratch = &.{}, .builder = format.DataFrameBuilder.init(&storage, 0, 3) };
    ctx.writers = &w;
    var buf: [1024]u8 = undefined;
    var n = msgs.encodeKvWriterGranted(&buf, id, 999, 2, 3, 1);
    ctx.dispatch(buf[0..n]);
    try t.expectEqual(types.KvWriterState.queued, w.state());
    n = msgs.encodeKvWriterGranted(&buf, id, 123, 2, 3, 1);
    ctx.dispatch(buf[0..n]);
    try t.expectEqual(types.KvWriterState.granted, w.state());
    w.beginBatch();
    _ = w.put("k", "v", .{});
    n = topic_msgs.encodeTopicLeaderChanged(&buf, id, 2, 4);
    ctx.dispatch(buf[0..n]);
    try t.expectEqual(types.KvWriterState.reasserting, w.state());
    try t.expectEqual(types.KvWriteResult.leader_unavailable, w.commitBatch(.{}));
    // Confirmation from the old leader cannot un-fence us.
    n = msgs.encodeKvWriterReassertResponse(&buf, .ok, id, 123, 2, 3, 1);
    ctx.dispatch(buf[0..n]);
    try t.expectEqual(types.KvWriterState.reasserting, w.state());
    n = msgs.encodeKvWriterReassertResponse(&buf, .ok, id, 123, 3, 4, 2);
    ctx.dispatch(buf[0..n]);
    try t.expectEqual(types.KvWriterState.granted, w.state());
    try t.expectEqual(@as(u64, 3), w.epoch);
    try t.expectEqual(@as(u32, 1), w.builder.recordCount()); // unsent records survive reassert
    n = msgs.encodeKvWriterRevoked(&buf, .superseded, id, 123, 2, 3);
    ctx.dispatch(buf[0..n]);
    try t.expectEqual(types.KvWriterState.granted, w.state());
    n = msgs.encodeKvWriterRevoked(&buf, .superseded, id, 123, 3, 4);
    ctx.dispatch(buf[0..n]);
    try t.expectEqual(types.KvWriterState.revoked, w.state());
    n = msgs.encodeKvWriterGranted(&buf, id, 123, 4, 4, 2);
    ctx.dispatch(buf[0..n]);
    try t.expectEqual(types.KvWriterState.revoked, w.state());
    // Late and malformed replies cannot overwrite a pending reader.
    ctx.reader_waiting = true;
    ctx.reader_store_id = id;
    n = msgs.encodeKvReaderResponse(&buf, .ok, id, common.kv.engine.engine_kind, .{}, "/tmp/store").?;
    ctx.dispatch(buf[0..n]);
    @memset(&buf, 0);
    try t.expectEqualStrings("/tmp/store", msgs.readerStoreDir(&ctx.reader_response.?));
    ctx.dispatch(&.{ 24, 0 });
    try t.expectEqualStrings("/tmp/store", msgs.readerStoreDir(&ctx.reader_response.?));
}
