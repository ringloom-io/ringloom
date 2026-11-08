// SPDX-License-Identifier: Apache-2.0
//! Native service reader at 1k/1M/10M entries. Engine population is setup;
//! these cases deliberately exclude topic transport, visibility and rebuild.
const std = @import("std");
const common = @import("ringloom_common");
const kv = common.kv;
const api = @import("ringloom_service").kv;
const testing = @import("ringloom_testing");
const Clock = common.platform.Clock;
const samples = 20000;
const Worker = struct {
    session: *api.KvReadSession,
    entries: u64,
    gate: *std.atomic.Value(bool),
    failed: bool = false,
    operations: u64 = 0,
    fn run(self: *Worker) void {
        while (!self.gate.load(.acquire)) std.atomic.spinLoopHint();
        var out: [256]u8 = undefined;
        for (0..samples) |i| {
            var key: [8]u8 = undefined;
            std.mem.writeInt(u64, &key, (@as(u64, i) * 7919) % self.entries, .big);
            const r = self.session.get(&key, &out);
            if (r != .found or r.found != 100) {
                self.failed = true;
                return;
            }
            self.operations += 1;
        }
    }
};
fn countEntry(n: *u64, _: []const u8, value: []const u8) bool {
    if (value.len != 100) return false;
    n.* += 1;
    return true;
}
fn runScale(entries: u64) !void {
    const a = std.testing.allocator;
    var h = try testing.TestHarness.init(a, "perf-kv-scale");
    defer h.cleanup();
    errdefer h.markFailed();
    const dir = try a.print("{s}/scale", .{h.env.storage_path});
    defer a.free(dir);
    var control = try kv.ControlFile.createOrOpen(dir, 1, kv.engine.engine_kind, 511, 65536);
    defer control.close();
    const generation = try a.print("{s}/gen-00000001", .{dir});
    defer a.free(generation);
    var env = try kv.engine.WriteEnv.create(generation, 8 << 30, 32);
    defer env.close();
    const value: [100]u8 = @splat('v');
    const setup_start = Clock.monotonicNanos();
    var offset: u64 = 0;
    while (offset < entries) {
        var txn = try env.begin();
        errdefer txn.abort();
        const end = @min(entries, offset + 1000);
        while (offset < end) : (offset += 1) {
            var key: [8]u8 = undefined;
            std.mem.writeInt(u64, &key, offset, .big);
            try txn.put(&key, .{ .write_index = offset, .expire_at_ns = 0 }, &value);
        }
        try txn.commit();
    }
    const setup_ns = Clock.monotonicNanos() - setup_start;
    control.publishGeneration(1);
    var counted = std.testing.FailingAllocator.init(a, .{});
    var reader = try api.KvReader.openLocal(counted.allocator(), dir, .{}, .{});
    defer reader.close();
    var session = try reader.newSession();
    defer session.deinit();
    var out: [256]u8 = undefined;
    const zero: [8]u8 = @splat(0);
    if (session.get(&zero, &out) != .found) return error.WarmupFailed;
    var hit = try testing.Histogram.initCapacity(a, samples);
    defer hit.deinit();
    var miss = try testing.Histogram.initCapacity(a, samples);
    defer miss.deinit();
    var view = try testing.Histogram.initCapacity(a, samples);
    defer view.deinit();
    const before = counted.allocations;
    const c_before = kv.engine.alloc_stats.allocs();
    for (0..samples) |i| {
        var key: [8]u8 = undefined;
        std.mem.writeInt(u64, &key, (@as(u64, i) * 7919) % entries, .big);
        const start = Clock.monotonicNanos();
        const r = session.get(&key, &out);
        try hit.record(@intCast(Clock.monotonicNanos() - start));
        if (r != .found or r.found != 100) return error.BadHit;
        std.mem.writeInt(u64, &key, entries + i, .big);
        const ms = Clock.monotonicNanos();
        const m = session.get(&key, &out);
        try miss.record(@intCast(Clock.monotonicNanos() - ms));
        if (m != .missing) return error.BadMiss;
    }
    if (session.view(&zero) != .found) return error.BadView;
    for (0..samples) |i| {
        var key: [8]u8 = undefined;
        std.mem.writeInt(u64, &key, (@as(u64, i) * 7919) % entries, .big);
        const start = Clock.monotonicNanos();
        const r = session.view(&key);
        try view.record(@intCast(Clock.monotonicNanos() - start));
        if (r != .found or r.found.len != 100) return error.BadView;
    }
    session.release();
    var visited: u64 = 0;
    const iter_start = Clock.monotonicNanos();
    const iter = session.forEach(&visited, countEntry);
    const iter_ns = Clock.monotonicNanos() - iter_start;
    if (iter.status != .done or visited != entries) return error.BadIteration;
    const zig_allocs = counted.allocations - before;
    const engine_allocs = kv.engine.alloc_stats.allocs() - c_before;
    if (zig_allocs != 0 or engine_allocs != 0) return error.ReadAllocated;
    var sessions: [4]api.KvReadSession = undefined;
    var initialized: usize = 0;
    defer for (sessions[0..initialized]) |*s| s.deinit();
    for (&sessions) |*s| {
        s.* = try reader.newSession();
        initialized += 1;
        _ = s.get(&zero, &out);
    }
    var gate: std.atomic.Value(bool) = .init(false);
    var workers: [4]Worker = undefined;
    var threads: [4]std.Thread = undefined;
    var spawned: usize = 0;
    errdefer {
        gate.store(true, .release);
        for (threads[0..spawned]) |t| t.join();
    }
    for (&workers, 0..) |*w, i| {
        w.* = .{ .session = &sessions[i], .entries = entries, .gate = &gate };
        threads[i] = try std.Thread.spawn(.{}, Worker.run, .{w});
        spawned += 1;
    }
    const parallel_start = Clock.monotonicNanos();
    gate.store(true, .release);
    for (threads) |t| t.join();
    spawned = 0;
    const parallel_ns = Clock.monotonicNanos() - parallel_start;
    var completed: u64 = 0;
    for (workers) |w| {
        if (w.failed) return error.ParallelReadFailed;
        completed += w.operations;
    }
    const file = try a.print("kv-scale-{d}.json", .{entries});
    defer a.free(file);
    const path = try @import("persistent_results.zig").scenarioPath(a, "kv", file);
    defer a.free(path);
    var buf: [2048]u8 = undefined;
    const json = try std.fmt.bufPrint(&buf, "{{\"suite\":\"kv\",\"scenario\":\"native-reader-scale\",\"entries\":{d},\"value_bytes\":100,\"samples\":{d},\"setup_ns\":{d},\"get_p50_ns\":{d},\"get_p99_ns\":{d},\"miss_p50_ns\":{d},\"miss_p99_ns\":{d},\"view_p50_ns\":{d},\"view_p99_ns\":{d},\"iteration_ns\":{d},\"iterated\":{d},\"read_zig_allocs\":{d},\"read_engine_allocs\":{d},\"reader_threads\":4,\"parallel_operations\":{d},\"parallel_ns\":{d},\"failed\":0}}\n", .{ entries, samples, setup_ns, hit.percentile(50), hit.percentile(99), miss.percentile(50), miss.percentile(99), view.percentile(50), view.percentile(99), iter_ns, visited, zig_allocs, engine_allocs, completed, parallel_ns });
    try std.Io.Dir.cwd().writeFile(std.testing.io, .{ .sub_path = path, .data = json });
}
test "kv native reader scale 1k" {
    try runScale(1000);
}
test "kv native reader scale 1M" {
    try runScale(1000000);
}
test "kv native reader scale 10M" {
    try runScale(10000000);
}
