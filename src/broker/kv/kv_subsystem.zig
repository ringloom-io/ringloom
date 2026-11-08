// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const common = @import("ringloom_common");
const kv = common.kv;
const topics = @import("../topics.zig");
const local = @import("kv_local_stores.zig");
const StoreSlot = @import("kv_store_slot.zig").StoreSlot;
const Config = common.config.broker_config.KvConfig;
const msgs = common.message.kv_control_messages;

pub const KvSubsystem = struct {
    allocator: std.mem.Allocator,
    ts: *topics.TopicSubsystem,
    cfg: Config,
    root: [:0]u8,
    slots: []StoreSlot,
    records: []topics.subsystem.KvTopicRecord,
    workers: []Worker,
    running: std.atomic.Value(bool) = .init(false),
    next_discovery: i64 = 0,
    counters: ?*common.concurrent.counters.CountersManager = null,
    const Worker = struct { owner: *KvSubsystem, index: u32, thread: ?std.Thread = null };

    pub fn create(allocator: std.mem.Allocator, ts: *topics.TopicSubsystem, cfg: Config, path: []const u8, group: []const u8, node: u32) !*KvSubsystem {
        const self = try allocator.create(KvSubsystem);
        errdefer allocator.destroy(self);
        const relative_root = try allocator.print("{s}/{s}/node-{d}", .{ path, group, node });
        defer allocator.free(relative_root);
        try std.Io.Dir.cwd().createDirPath(common.platform.defaultIo(), relative_root);
        const root = try std.Io.Dir.cwd().realPathFileAlloc(common.platform.defaultIo(), relative_root, allocator);
        errdefer allocator.free(root);
        const slots = try allocator.alloc(StoreSlot, cfg.max_stores);
        errdefer allocator.free(slots);
        for (slots) |*s| s.* = .{};
        const records = try allocator.alloc(topics.subsystem.KvTopicRecord, cfg.max_stores);
        errdefer allocator.free(records);
        const workers = try allocator.alloc(Worker, cfg.apply_threads);
        self.* = .{ .allocator = allocator, .ts = ts, .cfg = cfg, .root = root, .slots = slots, .records = records, .workers = workers };
        for (workers, 0..) |*w, i| w.* = .{ .owner = self, .index = @intCast(i) };
        return self;
    }
    /// Bind before start/discovery. Fail explicitly if configured stores cannot
    /// all register; registration must never fall back to counter id zero.
    pub fn bindCounters(self: *KvSubsystem, manager: *common.concurrent.counters.CountersManager) !void {
        const capacity = @min(manager.values_buffer.len / 128, manager.metadata_buffer.len / 256);
        if (capacity -| manager.allocatedCount() < @as(usize, self.cfg.max_stores) * common.monitoring.kv_metrics.metric_count)
            return error.InsufficientKvCounterCapacity;
        self.counters = manager;
    }
    /// Control-thread accessor: identity and metric handles are immutable after
    /// publication. No engine/applier state is read here.
    pub fn metricsForLocalStores(ls: local.KvLocalStores, id: u64) ?*const common.monitoring.KvMetrics {
        if (ls.lookup_fn != lookup or ls.ctx == null) return null;
        const self: *KvSubsystem = @ptrCast(@alignCast(ls.ctx.?));
        for (self.slots) |*s| if (s.registered.load(.acquire) and s.id == id) {
            return if (s.metrics) |*m| m else null;
        };
        return null;
    }
    pub fn start(self: *KvSubsystem) !void {
        self.running.store(true, .release);
        errdefer self.stop();
        for (self.workers) |*w| w.thread = try std.Thread.spawn(.{}, run, .{w});
        if (!self.ts.addResetListener(.{ .ctx = self, .on_pre_reset = preReset, .on_post_reset = postReset })) return error.ResetListenersFull;
    }
    pub fn stop(self: *KvSubsystem) void {
        self.running.store(false, .release);
        for (self.workers) |*w| if (w.thread) |t| {
            t.join();
            w.thread = null;
        };
    }
    /// Destroy only after the receiver and control threads have stopped: their
    /// reset listener and local-reader facade borrow this subsystem.
    pub fn destroy(self: *KvSubsystem) void {
        self.stop();
        for (self.slots) |*s| s.close();
        self.allocator.free(self.slots);
        self.allocator.free(self.records);
        self.allocator.free(self.workers);
        self.allocator.free(self.root);
        self.allocator.destroy(self);
    }
    /// Control-thread discovery publishes immutable slot identity once. Slots
    /// form a bounded SPSC registration mailbox for their assigned worker;
    /// generation readiness returns via release/acquire publication.
    pub fn doWork(self: *KvSubsystem, now: i64) u32 {
        if (now < self.next_discovery) return 0;
        self.next_discovery = now + 200 * std.time.ns_per_ms;
        const n = self.ts.collectKvTopicRecords(self.records);
        var work: u32 = 0;
        for (self.records[0..n]) |r| {
            if (!std.mem.startsWith(u8, r.name, "__kv.")) continue;
            const name = r.name[5..];
            if (!kv.format.isValidStoreName(name)) continue;
            var found = false;
            for (self.slots) |*s| if (s.registered.load(.acquire) and s.id == r.topic_id) {
                found = true;
                break;
            };
            if (found) continue;
            for (self.slots) |*s| {
                if (s.registered.load(.acquire)) continue;
                const dir = std.fmt.bufPrint(&s.dir, "{s}/{s}", .{ self.root, name }) catch break;
                s.dir_len = dir.len;
                s.name_len = name.len;
                @memcpy(s.name[0..name.len], name);
                s.id = r.topic_id;
                if (self.counters) |manager| {
                    s.metrics = common.monitoring.KvMetrics.init(manager, s.id, name) catch |err| {
                        std.log.err("KV metrics registration failed for {s}: {s}", .{ name, @errorName(err) });
                        break;
                    };
                    s.metrics.?.rebuild(.startup);
                    // These gauges need additional engine/service/topic interfaces.
                    // Unsupported counters remain unobserved until their owning
                    // interfaces are available; do not seed counters with -1.
                    // Mark unavailable rather than reporting invented zeroes.
                    inline for (.{ .oldest_reader_age_ms, .readers_active, .live_bytes, .log_bytes, .apply_lag_entries }) |metric|
                        s.metrics.?.set(metric, -1);
                }
                s.registered.store(true, .release);
                work += 1;
                break;
            }
        }
        return work;
    }
    pub fn localStores(self: *KvSubsystem) local.KvLocalStores {
        return .{ .enabled = true, .ctx = self, .lookup_fn = lookup };
    }
    fn lookup(ctx: ?*anyopaque, name: []const u8, out: *[msgs.kv_store_dir_max]u8) ?local.LocalStore {
        const self: *KvSubsystem = @ptrCast(@alignCast(ctx.?));
        for (self.slots) |*s| {
            if (!s.available.load(.acquire)) continue;
            if (!std.mem.eql(u8, name, s.name[0..s.name_len]) or s.dir_len > out.len) continue;
            @memcpy(out[0..s.dir_len], s.dir[0..s.dir_len]);
            return .{ .store_id = s.id, .dir_len = @intCast(s.dir_len), .engine_kind = kv.engine.engine_kind, .config = s.config, .ready = s.control.?.loadGeneration() != 0 };
        }
        return null;
    }
    /// Control/admin callers can request a rebuild without touching engines.
    pub fn requestRebuild(self: *KvSubsystem, id: u64) bool {
        for (self.slots) |*s| if (s.registered.load(.acquire) and s.id == id) {
            s.rebuild_requested.store(true, .release);
            return true;
        };
        return false;
    }
    fn preReset(ctx: ?*anyopaque, id: u64, is_kv: bool) void {
        if (!is_kv) return;
        const self: *KvSubsystem = @ptrCast(@alignCast(ctx.?));
        for (self.slots) |*s| if (s.registered.load(.acquire) and s.id == id) {
            s.resetting.store(true, .release);
            _ = s.reset.fetchAdd(1, .release);
        };
    }
    fn postReset(ctx: ?*anyopaque, id: u64, is_kv: bool, _: u64) void {
        if (!is_kv) return;
        const self: *KvSubsystem = @ptrCast(@alignCast(ctx.?));
        for (self.slots) |*s| if (s.registered.load(.acquire) and s.id == id) {
            s.resetting.store(false, .release);
        };
    }
    fn run(w: *Worker) void {
        const self = w.owner;
        if (self.cfg.cpuForThread(w.index)) |cpu| {
            var set: std.os.linux.cpu_set_t = std.mem.zeroes(std.os.linux.cpu_set_t);
            const word = cpu / @bitSizeOf(usize);
            if (word < set.len) set[word] |= @as(usize, 1) << @intCast(cpu % @bitSizeOf(usize));
            std.os.linux.sched_setaffinity(0, &set) catch {};
        }
        while (self.running.load(.acquire)) {
            var work: u32 = 0;
            const now: u64 = @intCast(@max(0, std.Io.Clock.real.now(common.platform.defaultIo()).nanoseconds));
            for (self.slots) |*s| {
                if (!s.registered.load(.acquire) or s.id % self.cfg.apply_threads != w.index) continue;
                work += s.step(self.allocator, self.ts, self.cfg, now) catch |err| blk: {
                    s.recover(err, self.cfg);
                    break :blk 0;
                };
            }
            if (work == 0) switch (self.cfg.apply_idle_strategy) {
                .busy_spin => std.atomic.spinLoopHint(),
                .yield => std.Thread.yield() catch {},
                .backoff => common.platform.sleepNanos(50 * std.time.ns_per_us),
            };
        }
    }
};

const testing = std.testing;
const TestPublisher = struct {
    fn offer(_: *anyopaque, _: u8, _: []const u8) i64 {
        return 1;
    }
    fn connected(_: *anyopaque, _: u8) bool {
        return false;
    }
};
fn appendTest(ts: *topics.TopicSubsystem, id: u64, bytes: []const u8) !void {
    if (kv.peekHeader(bytes)) |h| {
        if (h.kind == .data) {
            ts.onPublish(.{ .topic_id = id, .leader_epoch = ts.election.getEpoch(), .correlation_id = 0, .source_node = 1, .source_service = 1, .ack_mode = .fire_and_forget, .payload = bytes });
            return;
        }
    }
    try testing.expectEqual(topics.TopicSubsystem.AppendEnqueue.queued, ts.kvLeaderAppend(id, 0, bytes));
    _ = ts.receiverStep();
    while (ts.ctrl_out.dequeue()) |_| {}
}
fn driveTest(s: *StoreSlot, ts: *topics.TopicSubsystem, cfg: Config, now: u64) !void {
    for (0..2000) |_| {
        _ = s.step(testing.allocator, ts, cfg, now) catch |err| {
            s.recover(err, cfg);
            if (s.failed) return err;
            continue;
        };
        if (!s.building) return;
    }
    return error.TestDidNotCatchUp;
}
fn expectValue(s: *StoreSlot, key: []const u8, value: ?[]const u8) !void {
    var txn = try s.env.?.begin();
    defer txn.abort();
    const header = try txn.getHeader(key);
    if (value == null) return testing.expect(header == null);
    try testing.expect(header != null);
    var path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const path = try kv.control.genDir(&path_buf, s.dir[0..s.dir_len], s.generation);
    txn.abort();
    var env = try kv.engine.ReadEnv.open(path, 1024);
    defer env.close();
    var session = try env.newSession();
    defer session.deinit();
    try session.begin();
    defer session.end();
    const raw = (try session.get(key)).?;
    try testing.expectEqualStrings(value.?, raw[kv.engine.value_header_len..]);
}

test "applier real topic replay, TTL, fencing, restart continuity, growth and local lookup" {
    const a = testing.allocator;
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const base = try a.print(".zig-cache/tmp/{s}/topics", .{tmp.sub_path});
    defer a.free(base);
    const root = try a.print(".zig-cache/tmp/{s}/kv", .{tmp.sub_path});
    defer a.free(root);
    const ts = try topics.TopicSubsystem.init(a, .{ .enabled = true, .base_dir = base, .write_runway_bytes = 1 << 20, .read_runway_bytes = 1 << 20, .prefetcher_cpu_affinity = -1, .local_node_id = 1, .io = common.platform.defaultIo() }, .{ .ctx = undefined, .offerFn = TestPublisher.offer, .isConnectedFn = TestPublisher.connected, .isBackPressuredFn = TestPublisher.connected }, 0);
    defer ts.deinit();
    _ = ts.checkLeadership(10 * std.time.ns_per_s);
    const r = ts.createKvStoreTopic("__kv.prices", "FAST_DAILY", 1000);
    try testing.expectEqual(@TypeOf(r.status).ok, r.status);
    var buf: [8192]u8 = undefined;
    const store_cfg: kv.KvStoreConfig = .{ .map_size_hint = 256 * 1024 };
    try appendTest(ts, r.topic_id, try kv.format.encodeStoreHeader(&buf, 1, store_cfg));
    try appendTest(ts, r.topic_id, try kv.format.encodeWriterGrant(&buf, 1, .{ .writer_epoch = 1, .holder_session_nonce = 2, .holder_service_id = 1, .holder_node_id = 1, .granted_at_ns = 0 }));
    var builder = kv.DataFrameBuilder.init(&buf, 1, 1);
    try builder.addPut("keep", "value", 0);
    try builder.addPut("expiry", "gone", 100);
    try builder.addPut("ttl", "later", 1000);
    try appendTest(ts, r.topic_id, builder.finish());
    builder = kv.DataFrameBuilder.init(&buf, 9, 1);
    try builder.addPut("keep", "fenced", 0);
    try appendTest(ts, r.topic_id, builder.finish());
    try appendTest(ts, r.topic_id, "malformed");
    const cfg: Config = .{ .enabled = true, .max_stores = 2, .map_size_max = 8 << 20, .apply_batch_max_entries = 2, .sweep_interval_ms = 1, .generation_grace_ms = 1 };
    const ks = try KvSubsystem.create(a, ts, cfg, root, "group", 1);
    defer ks.destroy();
    var values: [128 * 80]u8 align(128) = @splat(0);
    var metadata: [256 * 80]u8 align(4) = @splat(0);
    var manager = common.concurrent.counters.CountersManager.init(&values, &metadata);
    var short_values: [128]u8 align(128) = @splat(0);
    var short_metadata: [256]u8 align(4) = @splat(0);
    var short_manager = common.concurrent.counters.CountersManager.init(&short_values, &short_metadata);
    try testing.expectError(error.InsufficientKvCounterCapacity, ks.bindCounters(&short_manager));
    try testing.expectEqual(0, short_manager.allocatedCount());
    try ks.bindCounters(&manager);
    _ = ks.doWork(1);
    const s = &ks.slots[0];
    var out: [msgs.kv_store_dir_max]u8 = undefined;
    var ls = ks.localStores();
    try testing.expectEqual(msgs.KvStatus.unknown_store, ls.open("prices", &out).status);
    _ = try s.step(a, ts, cfg, 200);
    try testing.expectEqual(msgs.KvStatus.not_ready, ls.open("prices", &out).status);
    try driveTest(s, ts, cfg, 200);
    try testing.expectEqual(msgs.KvStatus.ok, ls.open("prices", &out).status);
    try expectValue(s, "keep", "value");
    try expectValue(s, "expiry", null);
    try expectValue(s, "ttl", "later");
    try testing.expectEqual(@as(u64, 1), s.applier.?.fenced);
    try testing.expectEqual(@as(u64, 1), s.applier.?.malformed);
    const metrics = &s.metrics.?;
    try testing.expectEqual(1, metrics.get(.frames_fenced));
    try testing.expectEqual(1, metrics.get(.frames_malformed));
    try testing.expectEqual(3, metrics.get(.records_put));
    try testing.expectEqual(1, metrics.get(.rebuild_startup));
    try testing.expectEqual(metrics.get(.apply_batches), metrics.get(.apply_txn_ns_count));
    try testing.expectEqual(2, metrics.get(.entries));
    try testing.expectEqual(17, metrics.get(.live_bytes));
    try testing.expect(metrics.get(.engine_allocs) > 0);
    var export_buf: [512]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&export_buf);
    const metadata_entry = common.monitoring.metadata_reader.readCounter(&values, &metadata, metrics.ids[@backingInt(common.monitoring.kv_metrics.Metric.records_put)]).?;
    try common.monitoring.kv_metrics.writeSeries(&writer, common.monitoring.kv_metrics.decode(metadata_entry.type_id, metadata_entry.label).?, metrics.get(.records_put), "");
    try testing.expect(std.mem.indexOf(u8, writer.buffered(), "store_name=\"prices\"} 3") != null);
    // Replace TTL with an immortal value; stale expiry must not delete it.
    builder = kv.DataFrameBuilder.init(&buf, 1, 1);
    try builder.addPut("ttl", "immortal", 0);
    try builder.addPut("sweep", "expired", 2000);
    try appendTest(ts, r.topic_id, builder.finish());
    _ = try s.step(a, ts, cfg, 500);
    _ = try s.step(a, ts, cfg, 2 * std.time.ns_per_ms);
    try expectValue(s, "ttl", "immortal");
    try expectValue(s, "sweep", null);
    try testing.expect(s.expired_swept > 0);
    try testing.expectEqual(@as(i64, @intCast(s.expired_swept)), metrics.get(.expired_swept));
    // Hold an old engine snapshot while a new generation replays in slices.
    var old_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const old_path = try kv.control.genDir(&old_path_buf, s.dir[0..s.dir_len], s.generation);
    var old = try kv.engine.ReadEnv.open(old_path, 1024);
    defer old.close();
    var old_session = try old.newSession();
    defer old_session.deinit();
    try old_session.begin();
    defer old_session.end();
    const old_gen = s.control.?.loadGeneration();
    try testing.expect(ks.requestRebuild(r.topic_id));
    _ = try s.step(a, ts, cfg, 3 * std.time.ns_per_ms);
    try testing.expectEqual(old_gen, s.control.?.loadGeneration());
    try testing.expect((try old_session.get("keep")) != null);
    try driveTest(s, ts, cfg, 3 * std.time.ns_per_ms);
    try testing.expect(s.control.?.loadGeneration() > old_gen);
    // Force growth with enough distinct large values to fill the tiny map.
    const payload: [4096]u8 = @splat('x');
    for (0..60) |i| {
        var key: [32]u8 = undefined;
        const k = try std.fmt.bufPrint(&key, "large-{d}", .{i});
        builder = kv.DataFrameBuilder.init(&buf, 1, 1);
        try builder.addPut(k, &payload, 0);
        try appendTest(ts, r.topic_id, builder.finish());
    }
    s.rebuild();
    try driveTest(s, ts, cfg, 4 * std.time.ns_per_ms);
    try testing.expect(s.map_size > store_cfg.map_size_hint);
    try testing.expect(metrics.get(.rebuild_map_full) > 0);
    try testing.expect(metrics.get(.rebuild_operator) > 0);
    try expectValue(s, "large-59", &payload);
    try testing.expect((try old_session.get("keep")) != null);
    // Restart always builds a fresh generation, preserving published progress.
    const published = s.control.?.loadGeneration();
    const ks2 = try KvSubsystem.create(a, ts, cfg, root, "group", 1);
    defer ks2.destroy();
    _ = ks2.doWork(1);
    const restarted = &ks2.slots[0];
    _ = try restarted.step(a, ts, cfg, 5 * std.time.ns_per_ms);
    try testing.expectEqual(published, restarted.control.?.loadGeneration());
    try driveTest(restarted, ts, cfg, 5 * std.time.ns_per_ms);
    try testing.expect(restarted.control.?.loadGeneration() > published);
    try expectValue(restarted, "keep", "value");
    // A gap triggers replay; a cycle roll is accepted by Applier.follows.
    restarted.applier.?.index = restarted.applier.?.index.? - 2;
    builder = kv.DataFrameBuilder.init(&buf, 1, 1);
    try builder.addDelete("keep");
    try appendTest(ts, r.topic_id, builder.finish());
    try testing.expectError(error.IndexDiscontinuity, restarted.step(a, ts, cfg, 6 * std.time.ns_per_ms));
    restarted.recover(error.IndexDiscontinuity, cfg);
    try driveTest(restarted, ts, cfg, 6 * std.time.ns_per_ms);
    try expectValue(restarted, "keep", null);
    builder = kv.DataFrameBuilder.init(&buf, 1, 1);
    try builder.addClear();
    try builder.addPut("after-clear", "yes", 0);
    try appendTest(ts, r.topic_id, builder.finish());
    _ = try restarted.step(a, ts, cfg, 7 * std.time.ns_per_ms);
    try expectValue(restarted, "large-59", null);
    try expectValue(restarted, "after-clear", "yes");
    try appendTest(ts, r.topic_id, try kv.format.encodeWriterRelease(&buf, 1, .{ .writer_epoch = 1, .reason = 0 }));
    builder = kv.DataFrameBuilder.init(&buf, 1, 1);
    try builder.addClear();
    try appendTest(ts, r.topic_id, builder.finish());
    _ = try restarted.step(a, ts, cfg, 8 * std.time.ns_per_ms);
    try expectValue(restarted, "after-clear", "yes");
    // Replica reset waits for live replication before rebuilding.
    const before_reset = restarted.control.?.loadGeneration();
    ts.store.get(r.topic_id).?.role = .replica;
    ts.store.get(r.topic_id).?.repl_live.store(false, .release);
    KvSubsystem.preReset(ks2, r.topic_id, true);
    _ = try restarted.step(a, ts, cfg, 9 * std.time.ns_per_ms);
    try testing.expect(restarted.tailer == null);
    KvSubsystem.postReset(ks2, r.topic_id, true, 1);
    _ = try restarted.step(a, ts, cfg, 9 * std.time.ns_per_ms);
    try testing.expect(restarted.tailer == null);
    try testing.expectEqual(before_reset, restarted.control.?.loadGeneration());
    ts.store.get(r.topic_id).?.repl_live.store(true, .release);
    try driveTest(restarted, ts, cfg, 10 * std.time.ns_per_ms);
    try testing.expect(restarted.control.?.loadGeneration() > before_reset);
    // At the capacity cap, preserve the published generation and fail.
    restarted.map_size = cfg.map_size_max;
    const before_full = restarted.control.?.loadGeneration();
    restarted.recover(error.MapFull, cfg);
    try testing.expect(restarted.failed);
    try testing.expectEqual(before_full, restarted.control.?.loadGeneration());
    try expectValue(restarted, "after-clear", "yes");
    try ks2.start();
    ks2.stop();
}
