// SPDX-License-Identifier: Apache-2.0
//! KV reader (docs/kv-store-spec.md §11.1): maps the store's `control.shm`
//! read-only, opens the current generation through the storage engine and
//! serves lookups without involving the broker.
//!
//! * `KvReader` is the per-store handle (control mapping, generation
//!   environments shared process-wide, config, options). Heap-allocated so its
//!   address is stable; close it after all sessions.
//! * `KvReadSession` is one per thread and not thread-safe. Every `begin`
//!   performs a single acquire load of `control.generation`; if it changed the
//!   session moves to the new generation (the only allocation on the read path,
//!   counted in `generation_switches`).
//! * Values are `KvValueHeader ‖ bytes` in the engine (unaligned header, read
//!   with `KvValueHeader.read`). A value whose `expire_at_ns` is non-zero and
//!   `<= now` is treated as absent.
//!
//! The hot paths (`get`, `view`, `release`, `forEach`) perform no heap
//! allocation: `get` is `begin → lookup → memcpy → end`; `view` keeps the
//! engine snapshot open until `release`.

const std = @import("std");
const ringloom_common = @import("ringloom_common");
const kv = ringloom_common.kv;
const engine = kv.engine;
const format = kv.format;
const control = kv.control;
const types = @import("kv_types.zig");

const KvValueHeader = kv.KvValueHeader;
const KvStoreConfig = kv.KvStoreConfig;
const platform_io = ringloom_common.platform.io;

pub const TtlClock = types.TtlClock;
pub const KvStatus = types.KvStatus;
pub const KvGetResult = types.KvGetResult;
pub const KvViewResult = types.KvViewResult;
pub const KvIterResult = types.KvIterResult;
pub const KvReaderOptions = types.KvReaderOptions;

/// Backoff after a failed generation open before it is retried.
const open_retry_backoff_ns: u64 = 50 * std.time.ns_per_ms;

const GenEnv = struct {
    env: engine.ReadEnv,
    generation: u64,
    /// Guarded by generation_envs_lock; one ref per session.
    refs: u32,
    next: ?*GenEnv = null,
    path_buf: [600]u8 = undefined,
    path_len: usize,
};

// LMDB environments must be unique per process/path, including across distinct
// reader handles. Closing a duplicate environment can release process-wide
// file locks still needed by another handle. This registry is touched only at
// session setup, generation changes and teardown, never on steady-state reads.
var generation_envs_lock: std.atomic.Mutex = .unlocked;
var generation_envs: ?*GenEnv = null;

fn lockEnvironments() void {
    while (!generation_envs_lock.tryLock()) std.atomic.spinLoopHint();
}

pub const OpenLocalError = error{
    ControlFileInvalid,
    EngineMismatch,
    OutOfMemory,
    StoreDirTooLong,
};

pub const KvReader = struct {
    allocator: std.mem.Allocator,
    control_file: control.ControlFile,
    store_dir_buf: [512]u8 = undefined,
    store_dir_len: usize = 0,
    store_id: u64,
    config: KvStoreConfig,
    options: KvReaderOptions,

    lock: std.atomic.Mutex = .unlocked,
    open_sessions: u32 = 0,
    switches: std.atomic.Value(u64) = .init(0),

    /// Open the store directory `store_dir` (as returned by the broker in
    /// `KvReaderResponse`) directly, without the broker round trip. Used by
    /// `openKvReader` after the response and by unit tests.
    pub fn openLocal(
        allocator: std.mem.Allocator,
        store_dir: []const u8,
        config: KvStoreConfig,
        options: KvReaderOptions,
    ) OpenLocalError!*KvReader {
        const self = try allocator.create(KvReader);
        errdefer allocator.destroy(self);
        const canonical_dir = std.Io.Dir.cwd().realPathFileAlloc(platform_io.default(), store_dir, allocator) catch return error.ControlFileInvalid;
        defer allocator.free(canonical_dir);
        if (canonical_dir.len > self.store_dir_buf.len) return error.StoreDirTooLong;

        var cf = control.ControlFile.openReadOnlyInDir(canonical_dir) catch return error.ControlFileInvalid;
        errdefer cf.close();
        if (cf.engineKind() != engine.engine_kind) return error.EngineMismatch;

        self.* = .{
            .allocator = allocator,
            .control_file = cf,
            .store_id = cf.storeId(),
            .config = config,
            .options = options,
        };
        // The control header is authoritative for the key/value limits.
        self.config.max_key_len = @intCast(@min(cf.maxKeyLen(), std.math.maxInt(u16)));
        self.config.max_value_len = cf.maxValueLen();
        @memcpy(self.store_dir_buf[0..canonical_dir.len], canonical_dir);
        self.store_dir_len = canonical_dir.len;
        return self;
    }

    /// Closes the reader. All sessions must be `deinit`ed first.
    pub fn close(self: *KvReader) void {
        std.debug.assert(self.open_sessions == 0);
        self.control_file.close();
        const allocator = self.allocator;
        allocator.destroy(self);
    }

    pub fn storeDir(self: *const KvReader) []const u8 {
        return self.store_dir_buf[0..self.store_dir_len];
    }

    pub fn storeId(self: *const KvReader) u64 {
        return self.store_id;
    }

    pub fn effectiveConfig(self: *const KvReader) KvStoreConfig {
        return self.config;
    }

    /// True once the applier has published a generation.
    pub fn isReady(self: *const KvReader) bool {
        return self.control_file.loadGeneration() != 0;
    }

    pub fn status(self: *const KvReader) KvStatus {
        const cf = &self.control_file;
        const p = cf.loadProgress();
        const tip = cf.loadLogTipIndex();
        const hb = cf.loadHeartbeatNs();
        const now = types.realtimeNs(.precise);
        return .{
            .state = cf.getState(),
            .generation = cf.loadGeneration(),
            .applied_index = p.applied_index,
            .applied_leader_epoch = p.applied_leader_epoch,
            .applied_at_ns = p.applied_at_ns,
            .local_log_tip_index = tip,
            .lag_entries = tip -| p.applied_index,
            .replication_live = cf.loadReplicationLive(),
            .heartbeat_age_ms = if (hb == 0) std.math.maxInt(u64) else (now -| hb) / std.time.ns_per_ms,
            .generation_switches = self.switches.load(.monotonic),
        };
    }

    /// Setup: claims an engine reader slot. Works before the first generation
    /// is published; such a session returns `not_ready` until then.
    pub fn newSession(self: *KvReader) !KvReadSession {
        self.lockShared();
        self.open_sessions += 1;
        self.unlockShared();
        var session = KvReadSession{ .reader = self };
        const generation = self.control_file.loadGeneration();
        if (generation != 0 and !session.switchTo(generation)) {
            self.lockShared();
            self.open_sessions -= 1;
            self.unlockShared();
            return session.last_open_error;
        }
        return session;
    }

    /// Warm the page cache for the current generation (`madvise(WILLNEED)` on
    /// the data file) and touch the engine's own mapping by walking the
    /// B-tree once, so first reads do not fault. Setup path; allocates.
    pub fn prefault(self: *KvReader) void {
        const g = self.control_file.loadGeneration();
        if (g == 0) return;
        adviseDataFile(self.storeDir(), g);
        var s = self.newSession() catch return;
        defer s.deinit();
        if (s.begin() != .ok) return;
        defer s.endSnapshot();
        var e = s.rs.cursorFirst() catch return;
        var touched: u8 = 0;
        while (e) |entry| {
            touched +%= entry.value[0];
            e = s.rs.cursorNext() catch break;
        }
        std.mem.doNotOptimizeAway(touched);
    }

    // ── internals ────────────────────────────────────────────────────

    fn lockShared(self: *KvReader) void {
        while (!self.lock.tryLock()) std.atomic.spinLoopHint();
    }

    fn unlockShared(self: *KvReader) void {
        self.lock.unlock();
    }

    /// Setup/switch path. The global registry shares the engine environment,
    /// while each session still claims its own engine snapshot/reader slot.
    fn acquireGeneration(self: *KvReader, g: u64) ?*GenEnv {
        var dir_buf: [600]u8 = undefined;
        const dir = control.genDir(&dir_buf, self.storeDir(), g) catch return null;
        lockEnvironments();
        defer generation_envs_lock.unlock();
        var it = generation_envs;
        while (it) |cur| : (it = cur.next) {
            if (std.mem.eql(u8, cur.path_buf[0..cur.path_len], dir)) {
                cur.refs += 1;
                return cur;
            }
        }
        // Process-owned allocation: one reader's allocator may be torn down
        // while another reader still references this environment.
        const ng = std.heap.page_allocator.create(GenEnv) catch return null;
        ng.* = .{
            .env = engine.ReadEnv.open(dir, self.options.max_readers) catch {
                std.heap.page_allocator.destroy(ng);
                return null;
            },
            .generation = g,
            .refs = 1,
            .next = generation_envs,
            .path_len = dir.len,
        };
        @memcpy(ng.path_buf[0..dir.len], dir);
        generation_envs = ng;
        return ng;
    }

    fn releaseGeneration(_: *KvReader, g: *GenEnv) void {
        lockEnvironments();
        defer generation_envs_lock.unlock();
        g.refs -= 1;
        if (g.refs != 0) return;
        var link = &generation_envs;
        while (link.*) |cur| {
            if (cur == g) {
                link.* = cur.next;
                g.env.close();
                std.heap.page_allocator.destroy(g);
                return;
            }
            link = &cur.next;
        }
        unreachable;
    }
};

fn adviseDataFile(store_dir: []const u8, generation: u64) void {
    var dir_buf: [600]u8 = undefined;
    const gdir = control.genDir(&dir_buf, store_dir, generation) catch return;
    var path_buf: [640]u8 = undefined;
    const path = std.fmt.bufPrint(&path_buf, "{s}/data.mdb", .{gdir}) catch return;
    const io = platform_io.default();
    const file = std.Io.Dir.cwd().openFile(io, path, .{ .mode = .read_only }) catch return;
    defer file.close(io);
    const len = file.length(io) catch return;
    if (len == 0) return;
    const mapped = std.posix.mmap(null, @intCast(len), .{ .READ = true }, .{ .TYPE = .SHARED }, file.handle, 0) catch return;
    defer std.posix.munmap(mapped);
    std.posix.madvise(mapped.ptr, mapped.len, std.posix.MADV.WILLNEED) catch {};
}

pub const KvReadSession = struct {
    reader: *KvReader,
    gen: ?*GenEnv = null,
    rs: engine.ReadSession = undefined,
    /// A snapshot is open and handed out through `view` (until `release`).
    held: bool = false,
    failed_gen: u64 = 0,
    retry_after_ns: u64 = 0,
    last_open_error: engine.Error = error.EngineFailure,

    const BeginResult = enum { ok, not_ready };

    /// Releases the engine reader slot. Call `release()` first if a view is
    /// outstanding (done implicitly).
    pub fn deinit(self: *KvReadSession) void {
        self.release();
        if (self.gen) |g| {
            self.rs.deinit();
            self.reader.releaseGeneration(g);
            self.gen = null;
        }
        self.reader.lockShared();
        self.reader.open_sessions -= 1;
        self.reader.unlockShared();
    }

    /// Ends a snapshot opened by `begin`. No allocation.
    fn endSnapshot(self: *KvReadSession) void {
        self.rs.end();
    }

    /// Start a snapshot: one acquire load of the generation (switching if it
    /// changed), then the engine's `begin`. Allocation only when switching.
    fn begin(self: *KvReadSession) BeginResult {
        const g = self.reader.control_file.loadGeneration();
        if (g == 0) return .not_ready;
        if (self.gen == null or self.gen.?.generation != g) {
            if (!self.switchTo(g) and self.gen == null) return .not_ready;
        }
        self.rs.begin() catch return .not_ready;
        return .ok;
    }

    fn switchTo(self: *KvReadSession, g: u64) bool {
        if (self.failed_gen == g and types.realtimeNs(.coarse) < self.retry_after_ns) return false;
        const ng = self.reader.acquireGeneration(g) orelse return self.noteFailure(g);
        const ns = ng.env.newSession() catch |err| {
            self.last_open_error = err;
            self.reader.releaseGeneration(ng);
            return self.noteFailure(g);
        };
        if (self.gen) |old| {
            self.rs.deinit();
            self.reader.releaseGeneration(old);
            _ = self.reader.switches.fetchAdd(1, .monotonic);
        }
        self.gen = ng;
        self.rs = ns;
        self.failed_gen = 0;
        return true;
    }

    fn noteFailure(self: *KvReadSession, g: u64) bool {
        self.failed_gen = g;
        self.retry_after_ns = types.realtimeNs(.coarse) + open_retry_backoff_ns;
        return false;
    }

    inline fn keyOk(self: *const KvReadSession, key: []const u8) bool {
        return key.len != 0 and key.len <= self.reader.config.max_key_len;
    }

    /// Looks up `key` in the open snapshot. Null if absent or expired.
    inline fn lookup(self: *KvReadSession, key: []const u8) ?[]const u8 {
        const stored = (self.rs.get(key) catch return null) orelse return null;
        if (stored.len < engine.value_header_len) return null;
        const h = KvValueHeader.read(stored);
        if (h.expire_at_ns != 0 and format.isExpired(h.expire_at_ns, types.realtimeNs(self.reader.options.ttl_clock)))
            return null;
        return stored[engine.value_header_len..];
    }

    /// Copying get: begin, look up, copy, end. If a `view` snapshot is held,
    /// it is reused (and stays held).
    pub fn get(self: *KvReadSession, key: []const u8, out: []u8) KvGetResult {
        if (!self.held) {
            if (self.begin() != .ok) return .not_ready;
        }
        defer if (!self.held) self.endSnapshot();
        if (!self.keyOk(key)) return .missing;
        const v = self.lookup(key) orelse return .missing;
        if (v.len > out.len) return .{ .buffer_too_small = v.len };
        @memcpy(out[0..v.len], v);
        return .{ .found = v.len };
    }

    /// Zero-copy lookup. On `found` the snapshot stays open and the view is
    /// valid until `release()`; later `view`/`get`/`forEach` calls reuse the
    /// snapshot without renewing it. A miss does not hold a snapshot.
    pub fn view(self: *KvReadSession, key: []const u8) KvViewResult {
        const was_held = self.held;
        if (!was_held) {
            if (self.begin() != .ok) return .not_ready;
        }
        if (!self.keyOk(key)) {
            if (!was_held) self.endSnapshot();
            return .missing;
        }
        if (self.lookup(key)) |v| {
            self.held = true;
            return .{ .found = v };
        }
        if (!was_held) self.endSnapshot();
        return .missing;
    }

    /// Ends the snapshot behind outstanding views. No-op if none.
    pub fn release(self: *KvReadSession) void {
        if (!self.held) return;
        self.held = false;
        self.endSnapshot();
    }

    /// Full iteration over one consistent snapshot; expired entries are
    /// skipped. Order is engine-defined. `cb(ctx, key, value)` returns false to
    /// stop. Views are valid only inside the callback.
    pub fn forEach(
        self: *KvReadSession,
        ctx: anytype,
        comptime cb: fn (@TypeOf(ctx), []const u8, []const u8) bool,
    ) KvIterResult {
        if (!self.held) {
            if (self.begin() != .ok) return .{ .status = .not_ready };
        }
        defer if (!self.held) self.endSnapshot();
        const now_clock = self.reader.options.ttl_clock;
        var now: u64 = 0; // lazily read, once per iteration
        var visited: u64 = 0;
        var e = self.rs.cursorFirst() catch return .{ .status = .failed };
        while (e) |entry| {
            if (entry.value.len >= engine.value_header_len) {
                const h = KvValueHeader.read(entry.value);
                var live = true;
                if (h.expire_at_ns != 0) {
                    if (now == 0) now = types.realtimeNs(now_clock);
                    live = !format.isExpired(h.expire_at_ns, now);
                }
                if (live) {
                    visited += 1;
                    if (!cb(ctx, entry.key, entry.value[engine.value_header_len..]))
                        return .{ .status = .stopped, .visited = visited };
                }
            }
            e = self.rs.cursorNext() catch return .{ .status = .failed, .visited = visited };
        }
        return .{ .status = .done, .visited = visited };
    }

    /// Generation this session currently reads (0 if none opened yet).
    pub fn generation(self: *const KvReadSession) u64 {
        return if (self.gen) |g| g.generation else 0;
    }
};

// ── Tests ────────────────────────────────────────────────────────────────

const testing = std.testing;

const test_root = "/tmp/ringloom-test-kv-reader-";

fn cleanDir(path: []const u8) void {
    std.Io.Dir.cwd().deleteTree(platform_io.default(), path) catch {};
}

/// A locally built store: control.shm + generation directories written with
/// the engine's `WriteEnv`, standing in for the broker applier.
const TestStore = struct {
    dir: []const u8,
    cf: control.ControlFile,
    w: ?engine.WriteEnv = null,
    gen: u64 = 0,

    fn init(comptime name: []const u8) !TestStore {
        const dir = test_root ++ name;
        cleanDir(dir);
        const cf = try control.ControlFile.createOrOpen(dir, 0x77, engine.engine_kind, 511, 4096);
        return .{ .dir = dir, .cf = cf };
    }

    fn deinit(self: *TestStore) void {
        if (self.w) |*w| w.close();
        self.cf.close();
        cleanDir(self.dir);
    }

    /// Build generation `n` (not yet published) and keep its WriteEnv open.
    fn build(self: *TestStore, n: u64) !void {
        if (self.w) |*w| w.close();
        var buf: [600]u8 = undefined;
        const gdir = try control.genDir(&buf, self.dir, n);
        self.w = try engine.WriteEnv.create(gdir, 8 * 1024 * 1024, 64);
        self.gen = n;
    }

    fn put(self: *TestStore, key: []const u8, value: []const u8, expire_at_ns: u64) !void {
        var t = try self.w.?.begin();
        errdefer t.abort();
        try t.put(key, .{ .write_index = 7, .expire_at_ns = expire_at_ns }, value);
        try t.commit();
    }

    fn publish(self: *TestStore) void {
        self.cf.publishGeneration(self.gen);
        self.cf.setState(.live);
    }

    fn reader(self: *TestStore, opts: KvReaderOptions) !*KvReader {
        return KvReader.openLocal(testing.allocator, self.dir, .{}, opts);
    }
};

fn collect(list: *std.ArrayList([]const u8), key: []const u8, _: []const u8) bool {
    list.appendAssumeCapacity(key);
    return true;
}

const Counter = struct {
    n: u64 = 0,
    stop_after: u64 = std.math.maxInt(u64),
    fn visit(self: *Counter, _: []const u8, _: []const u8) bool {
        self.n += 1;
        return self.n < self.stop_after;
    }
};

test "reader: not ready until a generation is published, then get/missing/copy semantics" {
    var st = try TestStore.init("basic");
    defer st.deinit();
    try st.build(1);
    try st.put("alpha", "one", 0);
    try st.put("beta", "", 0);

    const r = try st.reader(.{});
    defer r.close();
    var s = try r.newSession();
    defer s.deinit();

    var out: [16]u8 = undefined;
    try testing.expectEqual(KvGetResult.not_ready, s.get("alpha", &out));
    try testing.expectEqual(KvViewResult.not_ready, s.view("alpha"));
    try testing.expectEqual(types.KvStoreState.initializing, r.status().state);
    try testing.expect(!r.isReady());

    st.publish();
    try testing.expect(r.isReady());
    try testing.expectEqual(@as(usize, 3), s.get("alpha", &out).found);
    try testing.expectEqualStrings("one", out[0..3]);
    try testing.expectEqual(@as(usize, 0), s.get("beta", &out).found); // empty value
    try testing.expectEqual(KvGetResult.missing, s.get("gamma", &out));
    try testing.expectEqual(KvGetResult.missing, s.get("", &out)); // empty key
    var big: [600]u8 = @splat('k');
    try testing.expectEqual(KvGetResult.missing, s.get(&big, &out)); // over max_key_len

    // buffer_too_small reports the needed length and copies nothing.
    var tiny: [2]u8 = @splat(0xEE);
    const r2 = s.get("alpha", &tiny);
    try testing.expectEqual(@as(usize, 3), r2.buffer_too_small);
    try testing.expectEqual(@as(u8, 0xEE), tiny[0]);
    // Zero-length out works for empty values.
    try testing.expectEqual(@as(usize, 0), s.get("beta", out[0..0]).found);

    const st_info = r.status();
    try testing.expectEqual(@as(u64, 1), st_info.generation);
    try testing.expectEqual(types.KvStoreState.live, st_info.state);
    try testing.expectEqual(@as(u64, 0), st_info.generation_switches);
    try testing.expectEqual(std.math.maxInt(u64), st_info.heartbeat_age_ms);
}

test "reader: view is zero-copy and stays valid across writes until release" {
    var st = try TestStore.init("view");
    defer st.deinit();
    try st.build(1);
    try st.put("k", "v1", 0);
    st.publish();

    const r = try st.reader(.{});
    defer r.close();
    var s = try r.newSession();
    defer s.deinit();

    const v = s.view("k").found;
    try testing.expectEqualStrings("v1", v);
    // Snapshot isolation: overwrite and delete while the view is held.
    try st.put("k", "v2-longer-value", 0);
    try st.put("other", "x", 0);
    try testing.expectEqualStrings("v1", v);
    // Reusing the held snapshot: get() still sees the old value, new key missing.
    var out: [32]u8 = undefined;
    try testing.expectEqualStrings("v1", out[0..s.get("k", &out).found]);
    try testing.expectEqual(KvGetResult.missing, s.get("other", &out));
    // A miss while held is still "missing" and does not drop the snapshot.
    try testing.expectEqual(KvViewResult.missing, s.view("nope"));
    try testing.expectEqualStrings("v1", v);

    s.release();
    s.release(); // idempotent
    try testing.expectEqualStrings("v2-longer-value", out[0..s.get("k", &out).found]);
    // A miss without a held snapshot holds nothing.
    try testing.expectEqual(KvViewResult.missing, s.view("nope"));
    try testing.expect(!s.held);
}

test "reader: expiry with precise and coarse clocks" {
    var st = try TestStore.init("ttl");
    defer st.deinit();
    try st.build(1);
    const now = types.realtimeNs(.precise);
    const sec = std.time.ns_per_s;
    try st.put("live", "a", now + 3600 * sec);
    try st.put("dead", "b", now - sec);
    try st.put("never", "c", 0);
    st.publish();

    for ([_]TtlClock{ .precise, .coarse }) |clock| {
        const r = try st.reader(.{ .ttl_clock = clock });
        defer r.close();
        var s = try r.newSession();
        defer s.deinit();
        var out: [8]u8 = undefined;
        try testing.expectEqual(@as(usize, 1), s.get("live", &out).found);
        try testing.expectEqual(@as(usize, 1), s.get("never", &out).found);
        try testing.expectEqual(KvGetResult.missing, s.get("dead", &out));
        try testing.expectEqual(KvViewResult.missing, s.view("dead"));
        try testing.expectEqual(@as(usize, 1), s.view("live").found.len);
        s.release();

        // forEach skips the expired entry.
        var c = Counter{};
        const res = s.forEach(&c, Counter.visit);
        try testing.expectEqual(KvIterResult.Status.done, res.status);
        try testing.expectEqual(@as(u64, 2), res.visited);
        try testing.expectEqual(@as(u64, 2), c.n);
    }
}

test "reader: forEach order, stop, snapshot consistency and empty store" {
    var st = try TestStore.init("iter");
    defer st.deinit();
    try st.build(1);
    st.publish();

    const r = try st.reader(.{});
    defer r.close();
    var s = try r.newSession();
    defer s.deinit();

    var c0 = Counter{};
    const empty = s.forEach(&c0, Counter.visit);
    try testing.expectEqual(KvIterResult.Status.done, empty.status);
    try testing.expectEqual(@as(u64, 0), empty.visited);

    for (0..50) |i| {
        var kb: [8]u8 = undefined;
        const k = try std.fmt.bufPrint(&kb, "key{d:0>3}", .{i});
        try st.put(k, k, 0);
    }
    var keys_buf: [64][]const u8 = undefined;
    var keys = std.ArrayList([]const u8).initBuffer(&keys_buf);
    const res = s.forEach(&keys, collect);
    try testing.expectEqual(@as(u64, 50), res.visited);
    try testing.expectEqualStrings("key000", keys.items[0]);
    try testing.expectEqualStrings("key049", keys.items[49]);

    var c = Counter{ .stop_after = 7 };
    const stopped = s.forEach(&c, Counter.visit);
    try testing.expectEqual(KvIterResult.Status.stopped, stopped.status);
    try testing.expectEqual(@as(u64, 7), stopped.visited);

    // Iteration inside a held snapshot is consistent with the view.
    _ = s.view("key001").found;
    try st.put("zzz", "late", 0);
    var c2 = Counter{};
    try testing.expectEqual(@as(u64, 50), s.forEach(&c2, Counter.visit).visited);
    s.release();
    var c3 = Counter{};
    try testing.expectEqual(@as(u64, 51), s.forEach(&c3, Counter.visit).visited);
}

test "reader: generation switch is picked up at the next begin and counted" {
    var st = try TestStore.init("gen");
    defer st.deinit();
    try st.build(1);
    try st.put("k", "gen1", 0);
    try st.put("only1", "x", 0);
    st.publish();

    const r = try st.reader(.{});
    defer r.close();
    var s1 = try r.newSession();
    defer s1.deinit();
    var s2 = try r.newSession(); // a second thread's session
    defer s2.deinit();

    var out: [16]u8 = undefined;
    try testing.expectEqualStrings("gen1", out[0..s1.get("k", &out).found]);
    try testing.expectEqualStrings("gen1", out[0..s2.get("k", &out).found]);
    try testing.expectEqual(@as(u64, 1), s1.generation());

    // Hold a view on generation 1 across the switch.
    const held = s1.view("k").found;

    // The "applier" builds generation 2 while generation 1 keeps serving.
    var w1 = st.w.?;
    st.w = null;
    try st.build(2);
    try st.put("k", "gen2", 0);
    st.publish();
    // Existing snapshot is untouched; the switch happens at the next begin.
    try testing.expectEqualStrings("gen1", held);
    s1.release();

    try testing.expectEqualStrings("gen2", out[0..s1.get("k", &out).found]);
    try testing.expectEqual(KvGetResult.missing, s1.get("only1", &out));
    try testing.expectEqual(@as(u64, 2), s1.generation());
    try testing.expectEqual(@as(u64, 1), r.status().generation_switches);
    try testing.expectEqualStrings("gen2", out[0..s2.get("k", &out).found]);
    try testing.expectEqual(@as(u64, 2), r.status().generation_switches);
    // Steady state: no further switches.
    _ = s1.get("k", &out);
    _ = s2.get("k", &out);
    try testing.expectEqual(@as(u64, 2), r.status().generation_switches);
    w1.close();

    // Old generation directory removed (broker grace expiry): sessions keep
    // working, and a new session opens the current generation.
    _ = try control.removeGenerationsBefore(st.dir, 2);
    try testing.expectEqualStrings("gen2", out[0..s1.get("k", &out).found]);
    var s3 = try r.newSession();
    defer s3.deinit();
    try testing.expectEqualStrings("gen2", out[0..s3.get("k", &out).found]);
}

test "reader: a generation that cannot be opened keeps the old one serving" {
    var st = try TestStore.init("genfail");
    defer st.deinit();
    try st.build(1);
    try st.put("k", "g1", 0);
    st.publish();

    const r = try st.reader(.{});
    defer r.close();
    var s = try r.newSession();
    defer s.deinit();
    var out: [8]u8 = undefined;
    try testing.expectEqual(@as(usize, 2), s.get("k", &out).found);

    st.cf.publishGeneration(9); // no gen-00000009 on disk
    try testing.expectEqualStrings("g1", out[0..s.get("k", &out).found]);
    try testing.expectEqual(@as(u64, 1), s.generation());

    // A brand-new session cannot open it either -> not_ready, not a crash.
    try testing.expectError(error.EngineFailure, r.newSession());
}

test "reader: wrong engine kind and bad control file are rejected" {
    var st = try TestStore.init("badopen");
    defer st.deinit();
    try testing.expectError(error.ControlFileInvalid, KvReader.openLocal(testing.allocator, "/tmp/ringloom-test-kv-reader-nonexistent", .{}, .{}));
    st.cf.header.engine_kind = 99;
    try testing.expectError(error.EngineMismatch, st.reader(.{}));
    st.cf.header.engine_kind = engine.engine_kind;
}

test "reader: status reflects control header" {
    var st = try TestStore.init("status");
    defer st.deinit();
    try st.build(1);
    st.publish();
    const now = types.realtimeNs(.precise);
    st.cf.storeProgress(40, 3, now);
    st.cf.storeHeartbeat(now - 5 * std.time.ns_per_s, 100, true);
    st.cf.setState(.catching_up);

    const r = try st.reader(.{});
    defer r.close();
    const s = r.status();
    try testing.expectEqual(types.KvStoreState.catching_up, s.state);
    try testing.expectEqual(@as(u64, 40), s.applied_index);
    try testing.expectEqual(@as(u64, 3), s.applied_leader_epoch);
    try testing.expectEqual(@as(u64, 60), s.lag_entries);
    try testing.expect(s.replication_live);
    try testing.expect(s.heartbeat_age_ms >= 5000 and s.heartbeat_age_ms < 60_000);
}

test "reader: prefault succeeds and reads still work" {
    var st = try TestStore.init("prefault");
    defer st.deinit();
    try st.build(1);
    for (0..200) |i| {
        var kb: [8]u8 = undefined;
        const k = try std.fmt.bufPrint(&kb, "k{d}", .{i});
        try st.put(k, k, 0);
    }
    st.publish();
    const r = try st.reader(.{ .prefault = true });
    defer r.close();
    r.prefault();
    var s = try r.newSession();
    defer s.deinit();
    var out: [8]u8 = undefined;
    try testing.expectEqual(@as(usize, 2), s.get("k5", &out).found);
}

test "reader: get/view/release/forEach perform zero heap allocations" {
    var st = try TestStore.init("alloc");
    defer st.deinit();
    try st.build(1);
    for (0..100) |i| {
        var kb: [8]u8 = undefined;
        const k = try std.fmt.bufPrint(&kb, "k{d}", .{i});
        try st.put(k, "value-bytes", if (i % 2 == 0) types.realtimeNs(.precise) + std.time.ns_per_s * 3600 else 0);
    }
    st.publish();

    // The reader is created with the testing allocator; opening the session
    // and the first generation open allocate. After warm-up the hot paths must
    // not touch either the Zig allocator or the engine's malloc.
    const r = try st.reader(.{});
    defer r.close();
    var s = try r.newSession();
    defer s.deinit();
    var out: [32]u8 = undefined;
    _ = s.get("k1", &out); // opens the generation
    var c = Counter{};
    _ = s.forEach(&c, Counter.visit);

    // Swap the reader's allocator for a counting one to catch Zig-side allocs.
    var counting = CountingAllocator{ .backing = testing.allocator };
    const saved = r.allocator;
    r.allocator = counting.allocator();
    defer r.allocator = saved;

    const a0 = engine.alloc_stats.allocs();
    const f0 = engine.alloc_stats.frees();
    for (0..2000) |i| {
        var kb: [8]u8 = undefined;
        const k = std.fmt.bufPrint(&kb, "k{d}", .{i % 120}) catch unreachable;
        _ = s.get(k, &out);
        switch (s.view(k)) {
            .found => |v| std.mem.doNotOptimizeAway(v.len),
            else => {},
        }
        s.release();
    }
    var c2 = Counter{};
    for (0..20) |_| _ = s.forEach(&c2, Counter.visit);
    try testing.expectEqual(@as(u64, 0), counting.allocs);
    try testing.expectEqual(@as(u64, 0), engine.alloc_stats.allocs() - a0);
    try testing.expectEqual(@as(u64, 0), engine.alloc_stats.frees() - f0);
}

const CountingAllocator = struct {
    backing: std.mem.Allocator,
    allocs: u64 = 0,

    fn allocator(self: *CountingAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{
            .alloc = alloc,
            .resize = resize,
            .remap = remap,
            .free = free,
        } };
    }

    fn alloc(ctx: *anyopaque, len: usize, a: std.mem.Alignment, ra: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        self.allocs += 1;
        return self.backing.rawAlloc(len, a, ra);
    }
    fn resize(ctx: *anyopaque, m: []u8, a: std.mem.Alignment, n: usize, ra: usize) bool {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        self.allocs += 1;
        return self.backing.rawResize(m, a, n, ra);
    }
    fn remap(ctx: *anyopaque, m: []u8, a: std.mem.Alignment, n: usize, ra: usize) ?[*]u8 {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        self.allocs += 1;
        return self.backing.rawRemap(m, a, n, ra);
    }
    fn free(ctx: *anyopaque, m: []u8, a: std.mem.Alignment, ra: usize) void {
        const self: *CountingAllocator = @ptrCast(@alignCast(ctx));
        self.allocs += 1;
        self.backing.rawFree(m, a, ra);
    }
};

test "reader: unaligned value headers decode correctly" {
    // Engine values may sit at any offset; header decoding must not assume alignment.
    var buf: [40]u8 align(8) = undefined;
    const h = KvValueHeader{ .write_index = 0x0102030405060708, .expire_at_ns = 0x1112131415161718 };
    h.write(buf[3..19]);
    const got = KvValueHeader.read(buf[3..19]);
    try testing.expectEqual(h, got);
}

test "reader: setup reports exhausted engine slots and releases them on deinit" {
    var st = try TestStore.init("slots");
    defer st.deinit();
    try st.build(1);
    st.publish();
    const r = try st.reader(.{ .max_readers = 64 });
    defer r.close();
    var sessions: [128]KvReadSession = undefined;
    var count: usize = 0;
    defer for (sessions[0..count]) |*s| s.deinit();
    while (count < sessions.len) : (count += 1) {
        sessions[count] = r.newSession() catch |err| {
            try testing.expectEqual(error.ReadersFull, err);
            break;
        };
    }
    try testing.expect(count > 0 and count < sessions.len);
    sessions[count - 1].deinit();
    count -= 1;
    var reused = try r.newSession();
    defer reused.deinit();
}

test "reader: distinct handles share a canonical generation environment until last session closes" {
    var st = try TestStore.init("shared-env");
    defer st.deinit();
    try st.build(1);
    try st.put("k", "first", 0);
    st.publish();
    const r1 = try st.reader(.{});
    defer r1.close();
    var s1 = try r1.newSession();
    defer s1.deinit();
    var alias_buf: [600]u8 = undefined;
    const alias = try std.fmt.bufPrint(&alias_buf, "{s}/.", .{st.dir});
    const r2 = try KvReader.openLocal(testing.allocator, alias, .{}, .{});
    var s2 = try r2.newSession();
    try testing.expect(s1.gen.? == s2.gen.?);
    // A borrowed snapshot also survives the other handle closing.
    const borrowed = s1.view("k").found;
    s2.deinit();
    r2.close();
    try st.put("k", "updated", 0);
    try testing.expectEqualStrings("first", borrowed);
    s1.release();
    var out: [16]u8 = undefined;
    try testing.expectEqualStrings("updated", out[0..s1.get("k", &out).found]);
}
