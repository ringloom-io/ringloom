// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const common = @import("ringloom_common");
const kv = common.kv;
const topics = @import("../topics.zig");
const Applier = @import("kv_applier.zig").Applier;
const Config = common.config.broker_config.KvConfig;

/// Registered once by control; mutable engine/tailer fields belong to one applier.
pub const StoreSlot = struct {
    metrics: ?common.monitoring.KvMetrics = null,
    registered: std.atomic.Value(bool) = .init(false),
    available: std.atomic.Value(bool) = .init(false),
    reset: std.atomic.Value(u64) = .init(0),
    resetting: std.atomic.Value(bool) = .init(false),
    rebuild_requested: std.atomic.Value(bool) = .init(false),
    seen_reset: u64 = 0,
    id: u64 = 0,
    name: [64]u8 = undefined,
    name_len: usize = 0,
    dir: [std.fs.max_path_bytes]u8 = undefined,
    dir_len: usize = 0,
    config: kv.KvStoreConfig = .{},
    control: ?kv.ControlFile = null,
    env: ?kv.engine.WriteEnv = null,
    tailer: ?topics.subsystem.LocalTailer = null,
    applier: ?Applier = null,
    generation: u64 = 0,
    map_size: u64 = 0,
    building: bool = true,
    failed: bool = false,
    wait_live: bool = false,
    retry_at: u64 = 0,
    sweep_at: u64 = 0,
    check_at: u64 = 0,
    cleanup_at: u64 = 0,
    expired_swept: u64 = 0,
    rebuilds: u64 = 0,
    reported_engine_allocs: u64 = 0,

    pub fn close(self: *StoreSlot) void {
        if (self.tailer) |*t| t.deinit();
        if (self.env) |*e| e.close();
        if (self.control) |*c| c.close();
        if (self.metrics) |*m| m.deinit();
    }
    fn reportEngineAllocs(self: *StoreSlot) void {
        if (self.metrics) |*m| if (self.env) |*e| {
            const total = e.engineAllocs();
            m.add(.engine_allocs, @intCast(total - self.reported_engine_allocs));
            self.reported_engine_allocs = total;
        };
    }
    fn closeWorking(self: *StoreSlot) void {
        self.reportEngineAllocs();
        if (self.tailer) |*t| t.deinit();
        self.tailer = null;
        if (self.env) |*e| e.close();
        self.env = null;
        self.applier = null;
    }
    pub fn rebuild(self: *StoreSlot) void {
        self.rebuildFor(.operator);
    }
    fn rebuildFor(self: *StoreSlot, reason: common.monitoring.kv_metrics.RebuildReason) void {
        if (self.metrics) |*m| m.rebuild(reason);
        self.closeWorking();
        self.failed = false;
        self.retry_at = 0;
        self.building = true;
        if (self.metrics) |*m| m.set(.store_state, @backingInt(kv.KvStoreState.rebuilding));
        self.rebuilds += 1;
        if (self.control) |*c| c.setState(if (c.loadGeneration() == 0) .initializing else .rebuilding);
    }
    pub fn fail(self: *StoreSlot, err: anyerror) void {
        self.failed = true;
        if (self.metrics) |*m| m.set(.store_state, @backingInt(kv.KvStoreState.failed));
        if (self.control) |*c| c.setState(.failed);
        std.log.warn("KV store {s}: {s}", .{ self.name[0..self.name_len], @errorName(err) });
    }
    fn createGeneration(self: *StoreSlot, cfg: Config) !void {
        const c = &self.control.?;
        self.generation = @max(self.generation, c.loadGeneration()) + 1;
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const path = try kv.control.genDir(&path_buf, self.dir[0..self.dir_len], self.generation);
        // An interrupted unpublished generation is disposable. Published ones
        // are never reused or truncated.
        try std.Io.Dir.cwd().deleteTree(common.platform.defaultIo(), path);
        try std.Io.Dir.cwd().createDirPath(common.platform.defaultIo(), path);
        self.env = try kv.engine.WriteEnv.create(path, self.map_size, cfg.max_readers);
        self.reported_engine_allocs = 0;
        self.applier = .{ .config = self.config, .store_id = self.id };
    }
    fn initialize(self: *StoreSlot, allocator: std.mem.Allocator, ts: *topics.TopicSubsystem, cfg: Config) !bool {
        if (self.tailer == null) self.tailer = try ts.openLocalTailer(allocator, self.id, 0);
        if (self.control == null) {
            const entry = (try self.tailer.?.poll()) orelse return false;
            const frame = try kv.decodeFrame(entry.message, 511, std.math.maxInt(u32));
            if (frame.payload != .store_header) return error.MissingStoreHeader;
            self.config = frame.payload.store_header;
            try self.config.validate(std.math.maxInt(u32));
            self.control = try kv.ControlFile.createOrOpen(self.dir[0..self.dir_len], self.id, kv.engine.engine_kind, self.config.max_key_len, self.config.max_value_len);
            self.control.?.setState(if (self.control.?.loadGeneration() == 0) .initializing else .rebuilding);
            self.map_size = @min(@max(self.config.map_size_hint, 64 * 1024), cfg.map_size_max);
            if (self.control.?.loadGeneration() != 0) {
                var old_buf: [std.fs.max_path_bytes]u8 = undefined;
                const old_dir = try kv.control.genDir(&old_buf, self.dir[0..self.dir_len], self.control.?.loadGeneration());
                var data_buf: [std.fs.max_path_bytes]u8 = undefined;
                const data = try std.fmt.bufPrint(&data_buf, "{s}/data.mdb", .{old_dir});
                if (std.Io.Dir.cwd().openFile(common.platform.defaultIo(), data, .{})) |file| {
                    defer file.close(common.platform.defaultIo());
                    const size = try file.length(common.platform.defaultIo());
                    self.map_size = @min(@max(self.map_size, size *| 2), cfg.map_size_max);
                } else |_| {}
            }
            self.available.store(true, .release);
            // Replay the header in the same stream as every other record.
            self.tailer.?.deinit();
            self.tailer = try ts.openLocalTailer(allocator, self.id, 0);
        }
        if (self.env == null) try self.createGeneration(cfg);
        return true;
    }
    pub fn step(self: *StoreSlot, allocator: std.mem.Allocator, ts: *topics.TopicSubsystem, cfg: Config, now: u64) !u32 {
        const seq = self.reset.load(.acquire);
        if (seq != self.seen_reset or self.rebuild_requested.swap(false, .acq_rel)) {
            self.rebuildFor(if (seq != self.seen_reset) .replica_reset else .operator);
            self.seen_reset = seq;
            self.wait_live = seq != 0;
        }
        if (self.resetting.load(.acquire)) return 0;
        if (self.failed) return 0;
        const live = ts.replicationLive(self.id);
        if (self.wait_live) {
            if (!live) return 0;
            self.wait_live = false;
        }
        if (self.env == null) {
            if (now < self.retry_at) return 0;
            self.retry_at = now + 200 * std.time.ns_per_ms;
            if (!try self.initialize(allocator, ts, cfg)) return 0;
        }
        const c = &self.control.?;
        const tip = ts.localHwm(self.id);
        c.storeHeartbeat(now, tip, live);
        var count: u32 = 0;
        var data_changed = false;
        if (try self.tailer.?.poll()) |first| {
            const txn_start = common.platform.Clock.monotonicNanos();
            var txn = try self.env.?.begin();
            defer txn.abort();
            var pending = self.applier.?;
            var entry = first;
            const start = common.platform.Clock.monotonicNanos();
            while (true) {
                try pending.apply(&txn, entry.index, entry.message, now);
                count += 1;
                if (count >= cfg.apply_batch_max_entries or common.platform.Clock.monotonicNanos() - start >= @as(i64, cfg.apply_batch_max_us) * std.time.ns_per_us) break;
                entry = (try self.tailer.?.poll()) orelse break;
            }
            try pending.setMeta(&txn);
            try txn.commit();
            if (self.metrics) |*m| {
                const previous = self.applier.?;
                m.increment(.apply_batches);
                m.add(.frames_applied, @intCast(pending.frames - previous.frames));
                m.add(.frames_fenced, @intCast(pending.fenced - previous.fenced));
                m.add(.frames_malformed, @intCast(pending.malformed - previous.malformed));
                m.add(.records_put, @intCast(pending.puts - previous.puts));
                m.add(.records_delete, @intCast(pending.deletes - previous.deletes));
                m.add(.records_clear, @intCast(pending.clears - previous.clears));
                m.observeApplyTxn(@intCast(@max(0, common.platform.Clock.monotonicNanos() - txn_start)));
            }
            self.applier = pending;
            data_changed = true;
        }
        const next = self.applier.?;
        if (self.reset.load(.acquire) != seq or self.resetting.load(.acquire)) return count;
        if (self.building and next.index != null and next.index.? >= tip) {
            c.storeProgress(next.index.?, next.leader_epoch, now);
            c.publishGeneration(self.generation);
            self.building = false;
            self.cleanup_at = now + @as(u64, cfg.generation_grace_ms) * std.time.ns_per_ms;
        }
        if (!self.building) {
            if (count != 0) c.storeProgress(next.index.?, next.leader_epoch, now);
            c.setState(if (next.index != null and next.index.? >= tip and live) .live else .catching_up);
            if (now >= self.sweep_at) {
                var sweep = try self.env.?.begin();
                defer sweep.abort();
                const n = try sweep.sweepExpired(now, cfg.sweep_batch);
                try sweep.commit();
                self.expired_swept += n;
                data_changed = data_changed or n != 0;
                if (self.metrics) |*m| m.add(.expired_swept, n);
                self.sweep_at = now + @as(u64, cfg.sweep_interval_ms) * std.time.ns_per_ms;
            }
            if (self.cleanup_at != 0 and now >= self.cleanup_at) {
                _ = try kv.control.removeGenerationsBefore(self.dir[0..self.dir_len], self.generation);
                self.cleanup_at = 0;
            }
        }
        if (now >= self.check_at) {
            _ = try self.env.?.readerCheck();
            if (self.metrics) |*m| {
                const stats = try self.env.?.stats();
                m.set(.readers_active, stats.readers_in_use);
                m.set(.oldest_reader_age_ms, if (stats.oldest_reader_age_ms) |age| @intCast(age) else -1);
                m.set(.map_used_pct, @intCast(@min(100, @as(u128, stats.used_bytes) * 100 / stats.map_size)));
            }
            self.check_at = now + @as(u64, cfg.reader_check_ms) * std.time.ns_per_ms;
        }
        if (self.metrics) |*m| {
            m.set(.applied_index, @bitCast(next.index orelse 0));
            m.set(.generation, @intCast(c.loadGeneration()));
            m.set(.store_state, @backingInt(c.getState()));
            if (data_changed) {
                const data = try self.env.?.measureData();
                m.set(.entries, @intCast(data.entries));
                m.set(.live_bytes, @intCast(data.live_bytes));
            }
            // Queue indices are cycle/sequence pairs: only a same-cycle
            // difference is an exact count. -1 explicitly means unavailable.
            m.set(.apply_lag_entries, if (next.index) |index| (if (tip >> 32 == index >> 32) @intCast(tip -| index) else -1) else -1);
        }
        self.reportEngineAllocs();
        return count;
    }
    pub fn recover(self: *StoreSlot, err: anyerror, cfg: Config) void {
        self.reportEngineAllocs();
        switch (err) {
            error.MapFull => {
                if (self.map_size >= cfg.map_size_max) return self.fail(err);
                self.map_size = @min(self.map_size *| 2, cfg.map_size_max);
                self.rebuildFor(.map_full);
            },
            error.IndexDiscontinuity => self.rebuildFor(.index_discontinuity),
            error.QueueNotOpen, error.FileNotFound => {},
            else => self.fail(err),
        }
    }
};
