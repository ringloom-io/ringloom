// SPDX-License-Identifier: Apache-2.0
//! Real KV client used by correctness and bounded performance scenarios.
const std = @import("std");
const service = @import("ringloom_service");
const common = @import("ringloom_common");
const Clock = common.platform.Clock;
const kv = service.kv;
fn arg(args: []const [:0]const u8, key: []const u8, fallback: []const u8) []const u8 {
    for (args, 0..) |a, i| if (std.mem.eql(u8, a, key) and i + 1 < args.len) return args[i + 1];
    return fallback;
}
fn number(args: []const [:0]const u8, key: []const u8, fallback: u64) u64 {
    return std.fmt.parseUnsigned(u64, arg(args, key, ""), 10) catch fallback;
}
fn pump(engine: *service.RingLoomEngine) void {
    _ = engine.control_agent.poll(64);
    _ = engine.aeron_runtime.doWork() catch {};
    std.atomic.spinLoopHint();
}
fn put(engine: *service.RingLoomEngine, writer: *kv.KvWriter, key: []const u8, value: []const u8, opts: kv.KvPutOpts) !kv.KvWriteResult {
    const deadline = Clock.monotonicNanos() + 15 * std.time.ns_per_s;
    while (true) {
        const result = writer.put(key, value, opts);
        switch (result) {
            .ok => return result,
            .back_pressured, .leader_unavailable => {
                if (Clock.monotonicNanos() > deadline) return error.OfferTimeout;
                pump(engine);
            },
            else => return error.WriteRejected,
        }
    }
}
fn written(engine: *service.RingLoomEngine, writer: *kv.KvWriter, result: kv.KvWriteResult) !void {
    const token = switch (result) {
        .ok => |s| s,
        else => return error.WriteFailed,
    };
    const deadline = Clock.monotonicNanos() + 15 * std.time.ns_per_s;
    while (!writer.isAcked(token)) {
        if (Clock.monotonicNanos() > deadline) return error.AckTimeout;
        pump(engine);
    }
}
fn expectValue(engine: *service.RingLoomEngine, session: *kv.KvReadSession, key: []const u8, expected: ?[]const u8) !void {
    const deadline = Clock.monotonicNanos() + 15 * std.time.ns_per_s;
    var buf: [1024]u8 = undefined;
    while (true) {
        switch (session.get(key, &buf)) {
            .found => |n| if (expected) |value| {
                if (std.mem.eql(u8, buf[0..n], value)) return;
            },
            .missing => if (expected == null) return,
            else => {},
        }
        if (Clock.monotonicNanos() > deadline) return error.VisibilityTimeout;
        pump(engine);
    }
}
fn visit(count: *u64, _: []const u8, _: []const u8) bool {
    count.* += 1;
    return true;
}
fn openReader(engine: *service.RingLoomEngine, name: []const u8) !*kv.KvReader {
    const deadline = Clock.monotonicNanos() + 20 * std.time.ns_per_s;
    while (true) {
        return engine.openKvReader(name, .{ .wait_ready_timeout_ms = 1000 }) catch |err| {
            if (err != error.UnknownStore and err != error.NotReady and err != error.LeaderUnavailable) return err;
            if (Clock.monotonicNanos() > deadline) return err;
            pump(engine);
            continue;
        };
    }
}
fn openWriter(engine: *service.RingLoomEngine, name: []const u8, wait: bool) !*kv.KvWriter {
    const deadline = Clock.monotonicNanos() + 20 * std.time.ns_per_s;
    while (true) {
        return engine.openKvWriter(name, .{ .map_size_hint = 1 << 20 }, .{ .acquire = if (wait) .wait else .fail_if_held }) catch |err| {
            if (err != error.LeaderUnavailable) return err;
            if (Clock.monotonicNanos() > deadline) return err;
            pump(engine);
            continue;
        };
    }
}
pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    var counted = std.testing.FailingAllocator.init(init.gpa, .{});
    var engine = try service.RingLoomEngine.startWithOptions(counted.allocator(), .{
        .service_name = arg(args, "--service-name", "kv-test"),
        .storage_path = arg(args, "--storage-path", "/dev/shm"),
        .group = arg(args, "--group", "ringloom-test"),
        .broker_node_id = @intCast(number(args, "--broker-node-id", 1)),
    }, .{ .control_agent_mode = .manual });
    defer engine.deinit();
    const mode = arg(args, "--mode", "crud");
    const name = arg(args, "--store", "scenario");
    if (std.mem.eql(u8, mode, "read") or std.mem.eql(u8, mode, "continuity") or std.mem.eql(u8, mode, "reader-crash")) {
        var reader = try openReader(engine, name);
        defer reader.close();
        var session = try reader.newSession();
        defer session.deinit();
        try expectValue(engine, &session, "visible", "remote");
        try std.Io.File.stdout().writeStreamingAll(init.io, "service ready\n");
        if (std.mem.eql(u8, mode, "reader-crash")) {
            _ = session.view("visible");
            while (true) pump(engine);
        }
        if (std.mem.eql(u8, mode, "continuity")) {
            const generation = reader.status().generation;
            const deadline = Clock.monotonicNanos() + 30 * std.time.ns_per_s;
            while (reader.status().generation <= generation) {
                try expectValue(engine, &session, "visible", "remote");
                if (Clock.monotonicNanos() > deadline) return error.GenerationTimeout;
            }
            try expectValue(engine, &session, "visible", "remote");
            if (reader.status().generation_switches == 0) return error.NoGenerationSwitch;
        }
        return;
    }
    var writer = try openWriter(engine, name, std.mem.eql(u8, mode, "standby"));
    defer writer.close();
    try std.Io.File.stdout().writeStreamingAll(init.io, "service ready\n");
    const grant_deadline = Clock.monotonicNanos() + 25 * std.time.ns_per_s;
    while (writer.state() != .granted) {
        if (Clock.monotonicNanos() > grant_deadline) return error.GrantTimeout;
        pump(engine);
    }
    try written(engine, writer, try put(engine, writer, "visible", "remote", .{ .ack_mode = .replicate_once }));
    if (std.mem.eql(u8, mode, "write") or std.mem.eql(u8, mode, "standby")) return;
    if (std.mem.eql(u8, mode, "failover")) {
        try std.Io.File.stdout().writeStreamingAll(init.io, "writer before failover\n");
        const deadline = Clock.monotonicNanos() + 30 * std.time.ns_per_s;
        while (writer.publisher.leader_node_id == 1 or writer.state() != .granted) {
            if (Clock.monotonicNanos() > deadline) return error.ReassertTimeout;
            pump(engine);
        }
        try written(engine, writer, try put(engine, writer, "visible", "remote", .{ .ack_mode = .replicate_once }));
        return;
    }
    if (std.mem.eql(u8, mode, "hold")) {
        try std.Io.File.stdout().writeStreamingAll(init.io, "writer holding\n");
        while (true) pump(engine);
    }
    var reader = try openReader(engine, name);
    defer reader.close();
    var session = try reader.newSession();
    defer session.deinit();
    try expectValue(engine, &session, "visible", "remote");
    if (std.mem.eql(u8, mode, "bench") or std.mem.eql(u8, mode, "growth")) {
        const initial_generation = reader.status().generation;
        const count = number(args, "--count", 1000);
        var put_hist = try @import("ringloom_testing").Histogram.initCapacity(init.gpa, @intCast(count));
        defer put_hist.deinit();
        var hit_hist = try @import("ringloom_testing").Histogram.initCapacity(init.gpa, 100000);
        defer hit_hist.deinit();
        var miss_hist = try @import("ringloom_testing").Histogram.initCapacity(init.gpa, 100000);
        defer miss_hist.deinit();
        const start = Clock.monotonicNanos();
        var i: u64 = 0;
        while (i < count) : (i += 1) {
            var key: [32]u8 = undefined;
            const k = try std.fmt.bufPrint(&key, "key-{d}", .{i});
            const put_start = Clock.monotonicNanos();
            const result = try put(engine, writer, k, "100-byte-value.......................................................................................", .{ .ack_mode = .replicate_once });
            try put_hist.record(@intCast(Clock.monotonicNanos() - put_start));
            try written(engine, writer, result);
        }
        const apply_ns = Clock.monotonicNanos() - start;
        var last: [32]u8 = undefined;
        try expectValue(engine, &session, try std.fmt.bufPrint(&last, "key-{d}", .{count - 1}), "100-byte-value.......................................................................................");
        const zig_allocs = counted.allocations;
        const engine_allocs = common.kv.engine.alloc_stats.allocs();
        if (std.mem.eql(u8, mode, "growth")) {
            if (reader.status().generation <= initial_generation) return error.MapDidNotGrow;
            var grown_count: u64 = 0;
            if (session.forEach(&grown_count, visit).status != .done or grown_count != count + 1) return error.GrowthLostEntries;
            return;
        }
        const read_start = Clock.monotonicNanos();
        i = 0;
        var copy: [256]u8 = undefined;
        while (i < 100000) : (i += 1) {
            const hit_start = Clock.monotonicNanos();
            const hit = session.get("visible", &copy);
            try hit_hist.record(@intCast(Clock.monotonicNanos() - hit_start));
            if (hit != .found or !std.mem.eql(u8, copy[0..hit.found], "remote")) return error.BadHit;
            const miss_start = Clock.monotonicNanos();
            const miss = session.get("absent", &copy);
            try miss_hist.record(@intCast(Clock.monotonicNanos() - miss_start));
            if (miss != .missing) return error.BadMiss;
        }
        const read_ns = Clock.monotonicNanos() - read_start;
        const read_zig_allocs = counted.allocations - zig_allocs;
        const read_engine_allocs = common.kv.engine.alloc_stats.allocs() - engine_allocs;
        if (read_zig_allocs != 0 or read_engine_allocs != 0) return error.ReadAllocated;
        var visited: u64 = 0;
        const iter_start = Clock.monotonicNanos();
        const iter = session.forEach(&visited, visit);
        const iter_ns = Clock.monotonicNanos() - iter_start;
        if (iter.status != .done or visited != count + 1) return error.IterationCount;
        var out: [1024]u8 = undefined;
        const json = try std.fmt.bufPrint(&out, "{{\"suite\":\"kv\",\"scenario\":\"kv-local-serial\",\"entries\":{d},\"published\":{d},\"visible\":{d},\"failed\":0,\"apply_ns\":{d},\"get_operations\":100000,\"hit_miss_pair_mean_ns\":{d},\"iteration_ns\":{d},\"iterated\":{d},\"read_zig_allocs\":{d},\"read_engine_allocs\":{d},\"get_p50_ns\":{d},\"get_p99_ns\":{d},\"miss_p50_ns\":{d},\"miss_p99_ns\":{d},\"writer_p50_ns\":{d},\"writer_p99_ns\":{d}}}\n", .{ count + 1, count, count, apply_ns, @divTrunc(read_ns, 100000), iter_ns, visited, read_zig_allocs, read_engine_allocs, hit_hist.percentile(50), hit_hist.percentile(99), miss_hist.percentile(50), miss_hist.percentile(99), put_hist.percentile(50), put_hist.percentile(99) });
        try std.Io.Dir.cwd().writeFile(init.io, .{ .sub_path = arg(args, "--result-file", "/tmp/kv.json"), .data = json });
        return;
    }
    // Fail-if-held must reject another registration, even in the same process.
    if (engine.openKvWriter(name, .{ .map_size_hint = 1 << 20 }, .{})) |other| {
        other.close();
        return error.ContentionNotRejected;
    } else |err| if (err != error.HeldByOther) return err;
    // Publish a syntactically valid DATA frame with a stale lease epoch through
    // the real registered topic publication. The applier must advance past it
    // without changing the value.
    const old_epoch = writer.epoch;
    const standby = try engine.openKvWriter(name, .{ .map_size_hint = 1 << 20 }, .{ .acquire = .wait });
    if (standby.state() != .queued) {
        standby.close();
        return error.WaiterNotQueued;
    }
    writer.close();
    writer = standby;
    const handoff_deadline = Clock.monotonicNanos() + 15 * std.time.ns_per_s;
    while (writer.state() != .granted) {
        if (Clock.monotonicNanos() > handoff_deadline) return error.HandoffTimeout;
        pump(engine);
    }
    if (writer.epoch <= old_epoch) return error.EpochDidNotAdvance;
    var frame_buf: [256]u8 = undefined;
    var stale = common.kv.DataFrameBuilder.init(&frame_buf, old_epoch, writer.publisher.leader_epoch);
    try stale.addPut("visible", "FENCED", 0);
    switch (writer.publisher.publish(stale.finish(), 0, .replicate_once)) {
        .ok => {},
        else => return error.StalePublishFailed,
    }
    const stale_token = writer.publisher.lastAssignedToken();
    const stale_deadline = Clock.monotonicNanos() + 15 * std.time.ns_per_s;
    while (!writer.isAcked(stale_token)) {
        if (Clock.monotonicNanos() > stale_deadline) return error.StaleAckTimeout;
        pump(engine);
    }
    // A later valid marker proves the applier has processed the stale frame.
    try written(engine, writer, writer.put("marker", "after-fence", .{ .ack_mode = .replicate_once }));
    try expectValue(engine, &session, "marker", "after-fence");
    try expectValue(engine, &session, "visible", "remote");
    try written(engine, writer, writer.delete("marker", .{ .ack_mode = .replicate_once }));
    try expectValue(engine, &session, "marker", null);
    switch (session.view("visible")) {
        .found => |v| if (!std.mem.eql(u8, v, "remote")) return error.BadView,
        else => return error.MissingView,
    }
    session.release();
    try written(engine, writer, writer.put("ttl", "expires", .{ .ttl_ms = 50, .ack_mode = .replicate_once }));
    try expectValue(engine, &session, "ttl", null);
    try written(engine, writer, writer.delete("visible", .{ .ack_mode = .replicate_once }));
    try expectValue(engine, &session, "visible", null);
    try written(engine, writer, writer.put("clear-me", "value", .{ .ack_mode = .replicate_once }));
    try expectValue(engine, &session, "clear-me", "value");
    var visited: u64 = 0;
    if (session.forEach(&visited, visit).status != .done or visited != 1) return error.IterationCount;
    try written(engine, writer, writer.clear(.{ .ack_mode = .replicate_once }));
    try expectValue(engine, &session, "clear-me", null);
}
