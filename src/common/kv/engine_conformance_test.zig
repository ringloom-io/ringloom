// SPDX-License-Identifier: Apache-2.0
//! Engine conformance suite (docs/kv-store-spec.md §7.1, §14.4). Runs against
//! whichever engine `engine.zig` selects: behaviour, view lifetime, iteration,
//! expiry index and sweep, MapFull/ReadersFull, cross-process and cross-thread
//! reader visibility, and allocation counts on hot-path calls.
const std = @import("std");
const testing = std.testing;
const engine = @import("engine.zig");
const format = @import("kv_format.zig");

const KvValueHeader = format.KvValueHeader;
const WriteEnv = engine.WriteEnv;
const ReadEnv = engine.ReadEnv;
const alloc_stats = engine.alloc_stats;

// ── helpers ──────────────────────────────────────────────────────────────

const TestDir = struct {
    buf: [128]u8 = undefined,
    len: usize = 0,

    fn init(name: []const u8, base: []const u8) TestDir {
        var d: TestDir = .{};
        const s = std.fmt.bufPrint(&d.buf, "{s}/ringloom-kv-conf-{d}-{s}", .{ base, std.c.getpid(), name }) catch unreachable;
        d.len = s.len;
        d.cleanup();
        return d;
    }

    fn path(self: *const TestDir) []const u8 {
        return self.buf[0..self.len];
    }

    fn cleanup(self: *TestDir) void {
        var p: [200]u8 = undefined;
        for ([_][]const u8{ "data.mdb", "lock.mdb" }) |f| {
            const s = bufPrintZ(&p, "{s}/{s}", .{ self.path(), f }) catch unreachable;
            _ = std.c.unlink(s.ptr);
        }
        const s = bufPrintZ(&p, "{s}", .{self.path()}) catch unreachable;
        _ = std.c.rmdir(s.ptr);
    }
};

fn bufPrintZ(buf: []u8, comptime fmt: []const u8, args: anytype) ![:0]u8 {
    const s = try std.fmt.bufPrint(buf[0 .. buf.len - 1], fmt, args);
    buf[s.len] = 0;
    return buf[0..s.len :0];
}

fn hdr(idx: u64, exp: u64) KvValueHeader {
    return .{ .write_index = idx, .expire_at_ns = exp };
}

fn putOne(w: *WriteEnv, key: []const u8, h: KvValueHeader, value: []const u8) !void {
    var t = try w.begin();
    errdefer t.abort();
    try t.put(key, h, value);
    try t.commit();
}

fn expectValue(got: ?[]const u8, h: KvValueHeader, value: []const u8) !void {
    const v = got orelse return error.TestExpectedValue;
    try testing.expectEqual(engine.value_header_len + value.len, v.len);
    try testing.expectEqual(h, KvValueHeader.read(v));
    try testing.expectEqualSlices(u8, value, v[engine.value_header_len..]);
}

const mib = 1024 * 1024;

// ── behaviour ────────────────────────────────────────────────────────────

test "engine: put get delete overwrite clear" {
    var dir = TestDir.init("basic", "/tmp");
    defer dir.cleanup();
    var w = try WriteEnv.create(dir.path(), 16 * mib, 16);
    defer w.close();
    var r = try ReadEnv.open(dir.path(), 16);
    defer r.close();
    var s = try r.newSession();
    defer s.deinit();

    try putOne(&w, "alpha", hdr(1, 0), "one");
    try putOne(&w, "beta", hdr(2, 77), "");
    try putOne(&w, "alpha", hdr(3, 0), "uno-updated");

    try s.begin();
    try expectValue(try s.get("alpha"), hdr(3, 0), "uno-updated");
    try expectValue(try s.get("beta"), hdr(2, 77), "");
    try testing.expect((try s.get("gamma")) == null);
    s.end();

    {
        var t = try w.begin();
        defer t.abort();
        try t.delete("alpha");
        try t.delete("does-not-exist"); // not an error
        try t.commit();
    }
    try s.begin();
    try testing.expect((try s.get("alpha")) == null);
    try testing.expect((try s.get("beta")) != null);
    s.end();

    {
        var t = try w.begin();
        try t.expiryInsert(500, "beta");
        try t.clear();
        try testing.expectEqual(@as(u64, 0), try t.entryCount());
        try testing.expectEqual(@as(u64, 0), try t.expiryCount());
        try t.commit();
    }
    try s.begin();
    try testing.expect((try s.get("beta")) == null);
    try testing.expect((try s.cursorFirst()) == null);
    s.end();

    // Writable again after clear.
    try putOne(&w, "k", hdr(9, 0), "v");
    try testing.expectEqual(@as(u64, 1), try w.entryCount());
}

test "engine: getHeader sees own uncommitted writes" {
    var dir = TestDir.init("getheader", "/tmp");
    defer dir.cleanup();
    var w = try WriteEnv.create(dir.path(), 16 * mib, 16);
    defer w.close();
    var t = try w.begin();
    defer t.abort();
    try testing.expect((try t.getHeader("k")) == null);
    try t.put("k", hdr(5, 1234), "xyz");
    const h = (try t.getHeader("k")).?;
    try testing.expectEqual(hdr(5, 1234), h);
    try t.delete("k");
    try testing.expect((try t.getHeader("k")) == null);
}

test "engine: invalid keys" {
    var dir = TestDir.init("badkey", "/tmp");
    defer dir.cleanup();
    var w = try WriteEnv.create(dir.path(), 16 * mib, 16);
    defer w.close();
    var t = try w.begin();
    defer t.abort();
    try testing.expectError(error.InvalidKey, t.put("", hdr(1, 0), "x"));
    // Maximum configured key length (511) plus the expiry prefix must fit.
    const k511: [511]u8 = @splat('k');
    try t.put(&k511, hdr(1, 10), "x");
    try t.expiryInsert(10, &k511);
    try testing.expect(w.maxKeyLen() >= 511 + 8);
}

// ── view lifetime ────────────────────────────────────────────────────────

test "engine: views are stable snapshots until end()" {
    var dir = TestDir.init("views", "/tmp");
    defer dir.cleanup();
    var w = try WriteEnv.create(dir.path(), 16 * mib, 16);
    defer w.close();
    var r = try ReadEnv.open(dir.path(), 16);
    defer r.close();
    var s = try r.newSession();
    defer s.deinit();

    var payload: [300]u8 = undefined;
    @memset(&payload, 'A');
    try putOne(&w, "key", hdr(1, 0), &payload);

    try s.begin();
    const view = (try s.get("key")).?;
    // The writer overwrites, deletes and adds while the view is held.
    @memset(&payload, 'B');
    try putOne(&w, "key", hdr(2, 0), &payload);
    for (0..200) |i| {
        var kb: [16]u8 = undefined;
        const k = std.fmt.bufPrint(&kb, "fill{d}", .{i}) catch unreachable;
        try putOne(&w, k, hdr(i, 0), &payload);
    }
    // Same snapshot: old value, new keys invisible.
    try testing.expectEqual(@as(u64, 1), KvValueHeader.read(view).write_index);
    for (view[engine.value_header_len..]) |c| try testing.expectEqual(@as(u8, 'A'), c);
    try testing.expect((try s.get("fill0")) == null);
    s.end();

    // A new snapshot sees the latest state.
    try s.begin();
    try expectValue(try s.get("key"), hdr(2, 0), &payload);
    try testing.expect((try s.get("fill199")) != null);
    s.end();
}

// ── iteration ────────────────────────────────────────────────────────────

test "engine: iteration is complete, ordered and repeatable" {
    var dir = TestDir.init("iter", "/tmp");
    defer dir.cleanup();
    var w = try WriteEnv.create(dir.path(), 64 * mib, 16);
    defer w.close();
    var r = try ReadEnv.open(dir.path(), 16);
    defer r.close();
    var s = try r.newSession();
    defer s.deinit();

    const n = 2000;
    {
        var t = try w.begin();
        errdefer t.abort();
        // Insert in a scrambled order.
        var i: usize = 0;
        while (i < n) : (i += 1) {
            const j = (i * 7919) % n;
            var kb: [16]u8 = undefined;
            const k = std.fmt.bufPrint(&kb, "key-{d:0>6}", .{j}) catch unreachable;
            var vb: [8]u8 = undefined;
            std.mem.writeInt(u64, &vb, j, .little);
            try t.put(k, hdr(j, 0), &vb);
        }
        try t.commit();
    }

    inline for (0..2) |_| { // iterate twice: cursor reuse across begin/end
        try s.begin();
        var count: usize = 0;
        var prev: [16]u8 = undefined;
        var prev_len: usize = 0;
        var e = try s.cursorFirst();
        while (e) |entry| : (e = try s.cursorNext()) {
            if (count > 0) try testing.expect(std.mem.order(u8, prev[0..prev_len], entry.key) == .lt);
            @memcpy(prev[0..entry.key.len], entry.key);
            prev_len = entry.key.len;
            var kb: [16]u8 = undefined;
            const expect_key = std.fmt.bufPrint(&kb, "key-{d:0>6}", .{count}) catch unreachable;
            try testing.expectEqualStrings(expect_key, entry.key);
            try testing.expectEqual(@as(u64, count), KvValueHeader.read(entry.value).write_index);
            try testing.expectEqual(@as(u64, count), std.mem.readInt(u64, entry.value[engine.value_header_len..][0..8], .little));
            count += 1;
        }
        try testing.expectEqual(@as(usize, n), count);
        s.end();
    }
}

// ── expiry index ─────────────────────────────────────────────────────────

test "engine: expiry index and sweep (incl. overwritten expiry)" {
    var dir = TestDir.init("expiry", "/tmp");
    defer dir.cleanup();
    var w = try WriteEnv.create(dir.path(), 16 * mib, 16);
    defer w.close();
    var r = try ReadEnv.open(dir.path(), 16);
    defer r.close();
    var s = try r.newSession();
    defer s.deinit();

    {
        var t = try w.begin();
        errdefer t.abort();
        // a: expires at 100; b: at 200; c: never; d: at 100 then re-put with 1000;
        // e: put with 100 then re-put without TTL; f: at 300 then deleted.
        try t.put("a", hdr(1, 100), "A");
        try t.expiryInsert(100, "a");
        try t.put("b", hdr(2, 200), "B");
        try t.expiryInsert(200, "b");
        try t.put("c", hdr(3, 0), "C");
        try t.put("d", hdr(4, 100), "D");
        try t.expiryInsert(100, "d");
        try t.put("e", hdr(5, 100), "E");
        try t.expiryInsert(100, "e");
        try t.put("f", hdr(6, 300), "F");
        try t.expiryInsert(300, "f");
        try t.commit();
    }
    // Overwrites follow the applier protocol (§8.2): read old header, remove old
    // expiry, put, insert new expiry.
    {
        var t = try w.begin();
        errdefer t.abort();
        const old_d = (try t.getHeader("d")).?;
        try testing.expectEqual(@as(u64, 100), old_d.expire_at_ns);
        try t.expiryRemove(old_d.expire_at_ns, "d");
        try t.put("d", hdr(7, 1000), "D2");
        try t.expiryInsert(1000, "d");

        const old_e = (try t.getHeader("e")).?;
        try t.expiryRemove(old_e.expire_at_ns, "e");
        try t.put("e", hdr(8, 0), "E2");

        const old_f = (try t.getHeader("f")).?;
        try t.expiryRemove(old_f.expire_at_ns, "f");
        try t.delete("f");
        try t.commit();
    }
    {
        var t = try w.begin();
        errdefer t.abort();
        try testing.expectEqual(@as(u64, 3), try t.expiryCount()); // a@100, b@200, d@1000
        try testing.expectEqual(@as(u32, 0), try t.sweepExpired(99, 100));
        try testing.expectEqual(@as(u32, 1), try t.sweepExpired(150, 100)); // a only
        try testing.expectEqual(@as(u32, 1), try t.sweepExpired(500, 100)); // b
        try testing.expectEqual(@as(u32, 1), try t.sweepExpired(2000, 1)); // d, bounded by max
        try testing.expectEqual(@as(u32, 0), try t.sweepExpired(2000, 100));
        try testing.expectEqual(@as(u64, 0), try t.expiryCount());
        try t.commit();
    }
    try s.begin();
    try testing.expect((try s.get("a")) == null);
    try testing.expect((try s.get("b")) == null);
    try expectValue(try s.get("c"), hdr(3, 0), "C");
    try testing.expect((try s.get("d")) == null);
    try expectValue(try s.get("e"), hdr(8, 0), "E2");
    try testing.expect((try s.get("f")) == null);
    s.end();
}

test "engine: sweep leaves data whose expiry was replaced without index cleanup" {
    var dir = TestDir.init("sweep-stale", "/tmp");
    defer dir.cleanup();
    var w = try WriteEnv.create(dir.path(), 16 * mib, 16);
    defer w.close();
    var t = try w.begin();
    defer t.abort();
    // Stale expiry entry (@100) for a key whose data now expires at 900.
    try t.put("k", hdr(2, 900), "v");
    try t.expiryInsert(100, "k");
    try t.expiryInsert(900, "k");
    try testing.expectEqual(@as(u32, 1), try t.sweepExpired(500, 10));
    try testing.expect((try t.getHeader("k")) != null); // data kept: expiry did not match
    try testing.expectEqual(@as(u32, 1), try t.sweepExpired(1000, 10));
    try testing.expect((try t.getHeader("k")) == null);
}

test "engine: meta round trip" {
    var dir = TestDir.init("meta", "/tmp");
    defer dir.cleanup();
    var w = try WriteEnv.create(dir.path(), 16 * mib, 16);
    defer w.close();
    var t = try w.begin();
    defer t.abort();
    try t.setMeta(.{
        .engine_kind = engine.engine_kind,
        .store_id = 0xAABBCCDD11223344,
        .applied_index = 42,
        .applied_leader_epoch = 7,
        .writer_epoch = 3,
    });
    try testing.expectEqual(@as(?u64, 42), try t.getMeta("applied_index"));
    try testing.expectEqual(@as(?u64, 7), try t.getMeta("applied_leader_epoch"));
    try testing.expectEqual(@as(?u64, 3), try t.getMeta("writer_epoch"));
    try testing.expectEqual(@as(?u64, 0xAABBCCDD11223344), try t.getMeta("store_id"));
    try testing.expectEqual(@as(?u64, engine.engine_kind), try t.getMeta("engine_kind"));
    try testing.expectEqual(@as(?u64, 1), try t.getMeta("format_version"));
    try testing.expect((try t.getMeta("nope")) == null);
}

// ── capacity and reader slots ────────────────────────────────────────────

test "engine: MapFull on a tiny map, then recovery by abort" {
    var dir = TestDir.init("mapfull", "/tmp");
    defer dir.cleanup();
    var w = try WriteEnv.create(dir.path(), 1 * mib, 16);
    defer w.close();
    try putOne(&w, "keep", hdr(1, 0), "stay");

    var big: [4000]u8 = undefined;
    @memset(&big, 'x');
    var hit_full = false;
    var t = try w.begin();
    var i: usize = 0;
    while (i < 1000) : (i += 1) {
        var kb: [16]u8 = undefined;
        const k = std.fmt.bufPrint(&kb, "big{d}", .{i}) catch unreachable;
        t.put(k, hdr(i, 0), &big) catch |err| {
            try testing.expectEqual(error.MapFull, err);
            hit_full = true;
            break;
        };
    }
    try testing.expect(hit_full);
    t.abort();
    try testing.expect((try w.usedBytes()) <= w.mapSize());

    // The environment still works and the aborted batch left nothing behind.
    try putOne(&w, "after", hdr(2, 0), "ok");
    try testing.expectEqual(@as(u64, 2), try w.entryCount());
}

test "engine: ReadersFull and slot reuse" {
    var dir = TestDir.init("readers", "/tmp");
    defer dir.cleanup();
    var w = try WriteEnv.create(dir.path(), 16 * mib, 4);
    defer w.close();
    try putOne(&w, "k", hdr(1, 0), "v");
    var r = try ReadEnv.open(dir.path(), 4);
    defer r.close();

    var sessions: [4]engine.ReadSession = undefined;
    for (&sessions) |*s| s.* = try r.newSession();
    try testing.expectError(error.ReadersFull, r.newSession());
    const st = try w.stats();
    try testing.expectEqual(@as(u32, 0), st.readers_in_use);
    for (&sessions) |*session| try session.begin();
    try testing.expectEqual(@as(u32, 4), (try w.stats()).readers_in_use);
    for (&sessions) |*session| session.end();
    try testing.expectEqual(@as(u32, 0), (try w.stats()).readers_in_use);

    sessions[0].deinit();
    var again = try r.newSession(); // freed slot is reusable
    try again.begin();
    try testing.expect((try again.get("k")) != null);
    again.end();
    again.deinit();
    for (sessions[1..]) |*s| s.deinit();
    try testing.expectEqual(@as(u32, 0), try w.readerCheck());
}

// ── cross-thread / cross-process visibility ─────────────────────────────

test "engine: second environment handle in another thread sees commits" {
    var dir = TestDir.init("thread", "/tmp");
    defer dir.cleanup();
    var w = try WriteEnv.create(dir.path(), 16 * mib, 16);
    defer w.close();
    try putOne(&w, "k", hdr(1, 0), "first");

    const Ctx = struct {
        dir: []const u8,
        stage: std.atomic.Value(u32) = .init(0),
        ok: bool = false,

        fn run(c: *@This()) void {
            c.ok = c.body() catch false;
            c.stage.store(99, .release);
        }
        fn body(c: *@This()) !bool {
            var r = try ReadEnv.open(c.dir, 16); // separate env handle
            defer r.close();
            var s = try r.newSession();
            defer s.deinit();
            try s.begin();
            const v1 = (try s.get("k")).?;
            if (!std.mem.eql(u8, v1[engine.value_header_len..], "first")) return false;
            s.end();
            c.stage.store(1, .release); // tell the parent to commit again
            while (c.stage.load(.acquire) != 2) std.Thread.yield() catch {};
            try s.begin();
            defer s.end();
            const v2 = (try s.get("k")).?;
            return std.mem.eql(u8, v2[engine.value_header_len..], "second");
        }
    };
    var ctx: Ctx = .{ .dir = dir.path() };
    const th = try std.Thread.spawn(.{}, Ctx.run, .{&ctx});
    while (ctx.stage.load(.acquire) != 1 and ctx.stage.load(.acquire) != 99) std.Thread.yield() catch {};
    try putOne(&w, "k", hdr(2, 0), "second");
    ctx.stage.store(2, .release);
    th.join();
    try testing.expect(ctx.ok);
}

extern "c" fn fork() c_int;

test "engine: reader in a forked child process sees parent commits" {
    const builtin = @import("builtin");
    if (builtin.os.tag != .linux) return error.SkipZigTest;
    var dir = TestDir.init("fork", "/tmp");
    defer dir.cleanup();
    var w = try WriteEnv.create(dir.path(), 16 * mib, 16);
    defer w.close();
    try putOne(&w, "k", hdr(1, 0), "before-fork");

    // Two pipes: child -> parent "ready", parent -> child "go".
    var up: [2]c_int = undefined;
    var down: [2]c_int = undefined;
    if (std.c.pipe(&up) != 0 or std.c.pipe(&down) != 0) return error.SkipZigTest;

    const pid = fork();
    if (pid < 0) return error.SkipZigTest;
    if (pid == 0) {
        // Child: only fixed buffers, leave through _exit.
        var code: u8 = 10;
        child: {
            var r = ReadEnv.open(dir.path(), 16) catch break :child;
            var s = r.newSession() catch break :child;
            code = 11;
            s.begin() catch break :child;
            const v1 = (s.get("k") catch break :child) orelse break :child;
            if (!std.mem.eql(u8, v1[engine.value_header_len..], "before-fork")) break :child;
            s.end();
            var b: [1]u8 = .{'r'};
            _ = std.c.write(up[1], &b, 1);
            _ = std.c.read(down[0], &b, 1); // wait for the parent's second commit
            code = 12;
            s.begin() catch break :child;
            const v2 = (s.get("k") catch break :child) orelse break :child;
            if (!std.mem.eql(u8, v2[engine.value_header_len..], "after-fork-commit")) break :child;
            const v3 = (s.get("new") catch break :child) orelse break :child;
            if (!std.mem.eql(u8, v3[engine.value_header_len..], "n")) break :child;
            s.end();
            code = 0;
        }
        std.c._exit(code);
    }

    var b: [1]u8 = undefined;
    try testing.expectEqual(@as(isize, 1), std.c.read(up[0], &b, 1));
    {
        var t = try w.begin();
        errdefer t.abort();
        try t.put("k", hdr(2, 0), "after-fork-commit");
        try t.put("new", hdr(3, 0), "n");
        try t.commit();
    }
    b[0] = 'g';
    _ = std.c.write(down[1], &b, 1);
    var status: c_int = 0;
    _ = std.c.waitpid(pid, &status, 0);
    // Normal exit with code 0.
    try testing.expect(status & 0x7f == 0);
    try testing.expectEqual(@as(c_int, 0), (status >> 8) & 0xff);
    _ = std.c.close(up[0]);
    _ = std.c.close(up[1]);
    _ = std.c.close(down[0]);
    _ = std.c.close(down[1]);
}

// ── allocation counts (§14.1) ───────────────────────────────────────────

test "engine: zero allocations on reader begin/get/cursor/end" {
    var dir = TestDir.init("alloc-read", "/tmp");
    defer dir.cleanup();
    var w = try WriteEnv.create(dir.path(), 64 * mib, 16);
    defer w.close();
    {
        var t = try w.begin();
        errdefer t.abort();
        for (0..1000) |i| {
            var kb: [16]u8 = undefined;
            const k = std.fmt.bufPrint(&kb, "k{d}", .{i}) catch unreachable;
            try t.put(k, hdr(i, 0), "value-value-value");
        }
        try t.commit();
    }
    var r = try ReadEnv.open(dir.path(), 16);
    defer r.close();
    var s = try r.newSession();
    defer s.deinit();

    // Warm up once, then count.
    try s.begin();
    _ = try s.get("k1");
    _ = try s.cursorFirst();
    s.end();

    const a0 = alloc_stats.allocs();
    const f0 = alloc_stats.frees();
    var hits: usize = 0;
    for (0..2000) |_| {
        try s.begin();
        if ((try s.get("k500")) != null) hits += 1;
        if ((try s.get("missing")) == null) hits += 1;
        var e = try s.cursorFirst();
        var n: usize = 0;
        while (e != null) : (e = try s.cursorNext()) n += 1;
        try testing.expectEqual(@as(usize, 1000), n);
        s.end();
    }
    try testing.expectEqual(@as(usize, 4000), hits);
    try testing.expectEqual(@as(u64, 0), alloc_stats.allocs() - a0);
    try testing.expectEqual(@as(u64, 0), alloc_stats.frees() - f0);
}

test "engine: writer transaction allocation counts (reported)" {
    var dir = TestDir.init("alloc-write", "/tmp");
    defer dir.cleanup();
    var w = try WriteEnv.create(dir.path(), 256 * mib, 16);
    defer w.close();

    var vb: [100]u8 = undefined;
    @memset(&vb, 'v');
    const batch = 256;
    var seq: u64 = 0;
    // Warm-up: grow the map file and exercise page recycling.
    for (0..200) |_| try writeBatch(&w, &seq, batch, &vb);

    const txns = 1000;
    const a0 = alloc_stats.allocs();
    const f0 = alloc_stats.frees();
    const b0 = alloc_stats.bytes();
    for (0..txns) |_| try writeBatch(&w, &seq, batch, &vb);
    const allocs = alloc_stats.allocs() - a0;
    const frees = alloc_stats.frees() - f0;
    const bytes = alloc_stats.bytes() - b0;
    std.debug.print(
        "\n[kv engine] steady-state writer txn ({d} puts/txn, {d} txns, {d} B values): " ++
            "{d} allocs, {d} frees, {d} bytes total => {d:.3} allocs/txn, {d:.5} allocs/put\n",
        .{ batch, txns, vb.len, allocs, frees, bytes, @as(f64, @floatFromInt(allocs)) / txns, @as(f64, @floatFromInt(allocs)) / (txns * batch) },
    );

    // Single-put transactions (worst case for per-txn overhead).
    const a1 = alloc_stats.allocs();
    for (0..txns) |_| try writeBatch(&w, &seq, 1, &vb);
    std.debug.print("[kv engine] steady-state writer txn (1 put/txn): {d} allocs over {d} txns\n", .{ alloc_stats.allocs() - a1, txns });

    // Overwrite-only (no new keys): exercises page reuse via the free list.
    const a2 = alloc_stats.allocs();
    for (0..txns) |_| {
        var t = try w.begin();
        errdefer t.abort();
        for (0..batch) |i| {
            var kb: [16]u8 = undefined;
            const k = std.fmt.bufPrint(&kb, "key{d:0>8}", .{i}) catch unreachable;
            try t.put(k, hdr(seq, 0), &vb);
            seq += 1;
        }
        try t.commit();
    }
    std.debug.print("[kv engine] steady-state writer txn (overwrite {d} keys/txn): {d} allocs over {d} txns\n", .{ batch, alloc_stats.allocs() - a2, txns });
}

fn writeBatch(w: *WriteEnv, seq: *u64, n: usize, value: []const u8) !void {
    var t = try w.begin();
    errdefer t.abort();
    for (0..n) |_| {
        var kb: [16]u8 = undefined;
        const k = std.fmt.bufPrint(&kb, "key{d:0>8}", .{seq.* % 100_000}) catch unreachable;
        try t.put(k, hdr(seq.*, 0), value);
        seq.* += 1;
    }
    try t.commit();
}

// ── sparse map (§18 item 5) ─────────────────────────────────────────────

fn blocksOf(dir: []const u8) !struct { size: u64, bytes: u64 } {
    var p: [256]u8 = undefined;
    const s = try bufPrintZ(&p, "{s}/data.mdb", .{dir});
    const linux = std.os.linux;
    var st: linux.Statx = undefined;
    const rc = linux.statx(linux.AT.FDCWD, s.ptr, 0, .{ .SIZE = true, .BLOCKS = true }, &st);
    if (linux.errno(rc) != .SUCCESS) return error.StatFailed;
    return .{ .size = st.size, .bytes = st.blocks * 512 };
}

fn sparseCheck(base: []const u8, name: []const u8) !void {
    var dir = TestDir.init(name, base);
    defer dir.cleanup();
    const map_size: u64 = 8 * 1024 * mib; // 8 GiB
    var w = try WriteEnv.create(dir.path(), map_size, 16);
    defer w.close();
    var vb: [100]u8 = undefined;
    @memset(&vb, 's');
    var seq: u64 = 0;
    for (0..40) |_| try writeBatch(&w, &seq, 1000, &vb); // ~ 4-5 MiB of pages
    const before = try blocksOf(dir.path());
    const used = try w.usedBytes();
    std.debug.print(
        "[kv engine] sparse map on {s}: map_size={d} B, st_size={d} B, st_blocks*512={d} B, engine used_bytes={d} B\n",
        .{ base, map_size, before.size, before.bytes, used },
    );
    try testing.expect(before.size >= map_size);
    // Only touched pages are charged: far less than the file length.
    try testing.expect(before.bytes < 64 * mib);
    try testing.expect(before.bytes < before.size / 100);
}

test "engine: map stays sparse on /dev/shm (tmpfs)" {
    const linux = std.os.linux;
    var probe: linux.Statx = undefined;
    if (linux.errno(linux.statx(linux.AT.FDCWD, "/dev/shm", 0, .{}, &probe)) != .SUCCESS) return error.SkipZigTest;
    try sparseCheck("/dev/shm", "sparse-shm");
}

test "engine: map stays sparse on /tmp" {
    try sparseCheck("/tmp", "sparse-tmp");
}

test "engine: map stays sparse on the build directory filesystem" {
    // `.zig-cache` lives on the project's (usually disk-backed) filesystem.
    try sparseCheck(".zig-cache", "sparse-cache");
}

test "engine: setup MapFull releases transaction and environment for retry" {
    var dir = TestDir.init("setup-mapfull", "/tmp");
    defer dir.cleanup();
    // Two 4 KiB meta pages leave no room for the named databases.
    try testing.expectError(error.MapFull, WriteEnv.create(dir.path(), 8192, 16));
    var writer = try WriteEnv.create(dir.path(), 16 * mib, 16);
    defer writer.close();
    try putOne(&writer, "retry", hdr(1, 0), "ok");
    try testing.expectEqual(@as(u64, 1), try writer.entryCount());
}

test "engine: metrics count active snapshots and committed user bytes" {
    var dir = TestDir.init("metrics", "/tmp");
    defer dir.cleanup();
    var w = try WriteEnv.create(dir.path(), 16 * mib, 16);
    defer w.close();
    var r = try ReadEnv.open(dir.path(), 16);
    defer r.close();
    var first = try r.newSession();
    defer first.deinit();
    var second = try r.newSession();
    defer second.deinit();
    try testing.expectEqual(@as(u32, 0), (try w.stats()).readers_in_use);
    try first.begin();
    try second.begin();
    try testing.expectEqual(@as(u32, 2), (try w.stats()).readers_in_use);
    first.end();
    try testing.expectEqual(@as(u32, 1), (try w.stats()).readers_in_use);
    second.end();
    try testing.expectEqual(@as(u32, 0), (try w.stats()).readers_in_use);
    try testing.expectEqual(@as(?u64, null), (try w.stats()).oldest_reader_age_ms);
    try putOne(&w, "key", hdr(1, 0), "value");
    try putOne(&w, "empty", hdr(2, 1), "");
    try testing.expectEqual(engine.DataStats{ .entries = 2, .live_bytes = 13 }, try w.measureData());
    {
        var txn = try w.begin();
        defer txn.abort();
        try txn.put("key", hdr(3, 0), "aborted-longer-value");
    }
    try testing.expectEqual(@as(u64, 13), (try w.measureData()).live_bytes);
    try putOne(&w, "key", hdr(4, 0), "v");
    try testing.expectEqual(@as(u64, 9), (try w.measureData()).live_bytes);
    {
        var txn = try w.begin();
        defer txn.abort();
        try txn.expiryInsert(1, "empty");
        _ = try txn.sweepExpired(1, 10);
        try txn.commit();
    }
    try testing.expectEqual(engine.DataStats{ .entries = 1, .live_bytes = 4 }, try w.measureData());
    {
        var txn = try w.begin();
        defer txn.abort();
        try txn.clear();
        try txn.commit();
    }
    try testing.expectEqual(engine.DataStats{ .entries = 0, .live_bytes = 0 }, try w.measureData());
}

test "engine: writer allocation counts exclude independent environments and readers" {
    var first_dir = TestDir.init("alloc-owner-1", "/tmp");
    defer first_dir.cleanup();
    var second_dir = TestDir.init("alloc-owner-2", "/tmp");
    defer second_dir.cleanup();
    var first = try WriteEnv.create(first_dir.path(), 16 * mib, 16);
    defer first.close();
    const before = first.engineAllocs();
    try testing.expect(before > 0);
    var second = try WriteEnv.create(second_dir.path(), 16 * mib, 16);
    defer second.close();
    try putOne(&second, "key", hdr(1, 0), "value");
    var reader = try ReadEnv.open(first_dir.path(), 16);
    defer reader.close();
    var session = try reader.newSession();
    defer session.deinit();
    try session.begin();
    session.end();
    try testing.expectEqual(before, first.engineAllocs());
    _ = try first.measureData();
    try testing.expect(first.engineAllocs() > before);
}
