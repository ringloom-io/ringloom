//! Per-store KV schema. Registration is transactional; owners must stop updates
//! before deinit. Histogram snapshots may span concurrent observations.
const std = @import("std");
const counters = @import("../concurrent/counters.zig");
const prometheus = @import("prometheus.zig");

pub const RebuildReason = enum { startup, replica_reset, index_discontinuity, map_full, operator };
pub const Metric = enum(u8) {
    applied_index,
    apply_lag_entries,
    apply_batches,
    frames_applied,
    frames_fenced,
    frames_malformed,
    records_put,
    records_delete,
    records_clear,
    expired_swept,
    entries,
    live_bytes,
    log_bytes,
    map_used_pct,
    generation,
    store_state,
    readers_active,
    oldest_reader_age_ms,
    reader_generation_switches,
    engine_allocs,
    writer_epoch,
    writer_holder,
    writer_waiters,
    rebuild_startup,
    rebuild_replica_reset,
    rebuild_index_discontinuity,
    rebuild_map_full,
    rebuild_operator,
    apply_txn_ns_bucket_1000,
    apply_txn_ns_bucket_10000,
    apply_txn_ns_bucket_100000,
    apply_txn_ns_bucket_1000000,
    apply_txn_ns_bucket_10000000,
    apply_txn_ns_bucket_inf,
    apply_txn_ns_count,
    apply_txn_ns_sum,

    pub fn kind(self: Metric) counters.MetricKind {
        return switch (self) {
            .applied_index, .apply_lag_entries, .entries, .live_bytes, .log_bytes, .map_used_pct, .generation, .store_state, .readers_active, .oldest_reader_age_ms, .writer_epoch, .writer_holder, .writer_waiters => .gauge,
            else => .counter,
        };
    }
    pub fn typeId(self: Metric) i32 {
        return counters.encodeTypeId(self.kind(), 0x4b000 + @as(u28, @backingInt(self)));
    }
    pub fn name(self: Metric) []const u8 {
        return switch (self) {
            .rebuild_startup, .rebuild_replica_reset, .rebuild_index_discontinuity, .rebuild_map_full, .rebuild_operator => "rebuilds",
            .apply_txn_ns_bucket_1000, .apply_txn_ns_bucket_10000, .apply_txn_ns_bucket_100000, .apply_txn_ns_bucket_1000000, .apply_txn_ns_bucket_10000000, .apply_txn_ns_bucket_inf => "apply_txn_ns_bucket",
            else => @tagName(self),
        };
    }
    pub fn extra(self: Metric) ?struct { key: []const u8, value: []const u8 } {
        return switch (self) {
            .rebuild_startup => .{ .key = "reason", .value = "startup" },
            .rebuild_replica_reset => .{ .key = "reason", .value = "replica_reset" },
            .rebuild_index_discontinuity => .{ .key = "reason", .value = "index_discontinuity" },
            .rebuild_map_full => .{ .key = "reason", .value = "map_full" },
            .rebuild_operator => .{ .key = "reason", .value = "operator" },
            .apply_txn_ns_bucket_1000 => .{ .key = "le", .value = "1000" },
            .apply_txn_ns_bucket_10000 => .{ .key = "le", .value = "10000" },
            .apply_txn_ns_bucket_100000 => .{ .key = "le", .value = "100000" },
            .apply_txn_ns_bucket_1000000 => .{ .key = "le", .value = "1000000" },
            .apply_txn_ns_bucket_10000000 => .{ .key = "le", .value = "10000000" },
            .apply_txn_ns_bucket_inf => .{ .key = "le", .value = "+Inf" },
            else => null,
        };
    }
};
pub const metric_count = @typeInfo(Metric).@"enum".field_names.len;
pub const bucket_bounds = [_]u64{ 1000, 10000, 100000, 1000000, 10000000 };

pub const KvMetrics = struct {
    manager: *counters.CountersManager,
    ids: [metric_count]usize,

    pub fn init(manager: *counters.CountersManager, store_id: u64, store_name: []const u8) !KvMetrics {
        var buf: [counters.max_label_length]u8 = undefined;
        const label = std.fmt.bufPrint(&buf, "kv|{d}|{s}", .{ store_id, store_name }) catch return error.StoreLabelTooLong;
        // CountersManager assumes at least one slot, so guard empty buffers here.
        if (manager.values_buffer.len < counters.counter_value_length or manager.metadata_buffer.len < counters.counter_metadata_length)
            return error.CounterAllocationFailed;
        var self: KvMetrics = .{ .manager = manager, .ids = undefined };
        var allocated: usize = 0;
        errdefer for (self.ids[0..allocated]) |id| manager.free(id);
        for (&self.ids, 0..) |*id, i| {
            const metric: Metric = @fromBackingInt(@as(u8, @intCast(i)));
            id.* = manager.allocate(metric.typeId(), label) orelse return error.CounterAllocationFailed;
            allocated += 1;
        }
        return self;
    }
    pub fn deinit(self: *const KvMetrics) void {
        for (self.ids) |id| self.manager.free(id);
    }
    pub fn increment(self: *const KvMetrics, metric: Metric) void {
        self.add(metric, 1);
    }
    pub fn add(self: *const KvMetrics, metric: Metric, value: i64) void {
        self.manager.add(self.ids[@backingInt(metric)], value);
    }
    pub fn set(self: *const KvMetrics, metric: Metric, value: i64) void {
        self.manager.set(self.ids[@backingInt(metric)], value);
    }
    pub fn get(self: *const KvMetrics, metric: Metric) i64 {
        return self.manager.get(self.ids[@backingInt(metric)]);
    }
    pub fn rebuild(self: *const KvMetrics, reason: RebuildReason) void {
        self.increment(@fromBackingInt(@as(u8, @backingInt(Metric.rebuild_startup)) + @as(u8, @intCast(@backingInt(reason)))));
    }
    /// Nanoseconds; sum saturates each observation at i64 max. Counters may wrap
    /// after i64 max aggregate, like the underlying counter infrastructure.
    pub fn observeApplyTxn(self: *const KvMetrics, ns: u64) void {
        for (bucket_bounds, 0..) |bound, i| {
            if (ns <= bound) self.increment(@fromBackingInt(@as(u8, @backingInt(Metric.apply_txn_ns_bucket_1000)) + @as(u8, @intCast(i))));
        }
        self.increment(.apply_txn_ns_bucket_inf);
        self.increment(.apply_txn_ns_count);
        self.add(.apply_txn_ns_sum, @intCast(@min(ns, std.math.maxInt(i64))));
    }
};

pub const Identity = struct { metric: Metric, store_id: u64, store_name: []const u8 };
pub fn decode(type_id: i32, label: []const u8) ?Identity {
    const raw: u32 = @bitCast(type_id);
    const category = raw & 0x0fff_ffff;
    if (category < 0x4b000 or category >= 0x4b000 + metric_count) return null;
    const metric: Metric = @fromBackingInt(@as(u8, @intCast(category - 0x4b000)));
    if (type_id != metric.typeId() or !std.mem.startsWith(u8, label, "kv|")) return null;
    const end = std.mem.indexOfScalarPos(u8, label, 3, '|') orelse return null;
    const id = std.fmt.parseInt(u64, label[3..end], 10) catch return null;
    return .{ .metric = metric, .store_id = id, .store_name = label[end + 1 ..] };
}

/// Declare families once per scrape, before per-store samples.
pub fn writeHelpTypes(writer: *std.Io.Writer) !void {
    inline for (0..metric_count) |i| {
        const metric: Metric = @fromBackingInt(i);
        if (metric == .rebuild_startup) {
            try prometheus.writeHelpType(writer, "ringloom_kv_rebuilds", "KV rebuilds by reason.", "counter");
        } else if (metric == .apply_txn_ns_bucket_1000) {
            try prometheus.writeHelpType(writer, "ringloom_kv_apply_txn_ns", "KV apply transaction duration in nanoseconds.", "histogram");
        } else if (i < @backingInt(Metric.rebuild_startup)) {
            try writer.print("# HELP ringloom_kv_{s} KV {s}.\n# TYPE ringloom_kv_{s} {s}\n", .{ metric.name(), metric.name(), metric.name(), @tagName(metric.kind()) });
        }
    }
}

/// Writes a series with escaped store labels. Caller supplies additional owner
/// labels inside the braces, ending in a comma (or an empty string).
pub fn writeSeries(writer: *std.Io.Writer, identity: Identity, value: i64, owner_labels: []const u8) !void {
    try writer.print("ringloom_kv_{s}{{{s}store_id=\"{d}\",store_name=\"", .{ identity.metric.name(), owner_labels, identity.store_id });
    try prometheus.writeLabelValue(writer, identity.store_name);
    try writer.writeByte('"');
    if (identity.metric.extra()) |extra| try writer.print(",{s}=\"{s}\"", .{ extra.key, extra.value });
    try writer.print("}} {d}\n", .{value});
}

test "KV registration isolates stores and histogram is cumulative" {
    var values: [128 * 80]u8 align(128) = @splat(0);
    var metadata: [256 * 80]u8 align(4) = @splat(0);
    var manager = counters.CountersManager.init(&values, &metadata);
    const a = try KvMetrics.init(&manager, 7, "orders|west");
    defer a.deinit();
    const b = try KvMetrics.init(&manager, 8, "orders");
    defer b.deinit();
    a.observeApplyTxn(10000);
    a.observeApplyTxn(10000001);
    a.rebuild(.map_full);
    try std.testing.expectEqual(0, a.get(.apply_txn_ns_bucket_1000));
    try std.testing.expectEqual(1, a.get(.apply_txn_ns_bucket_10000));
    try std.testing.expectEqual(2, a.get(.apply_txn_ns_bucket_inf));
    try std.testing.expectEqual(2, a.get(.apply_txn_ns_count));
    try std.testing.expectEqual(10010001, a.get(.apply_txn_ns_sum));
    try std.testing.expectEqual(1, a.get(.rebuild_map_full));
    try std.testing.expectEqual(0, b.get(.rebuild_map_full));
    const meta = @import("metadata_reader.zig").readCounter(&values, &metadata, a.ids[0]).?;
    const identity = decode(meta.type_id, meta.label).?;
    try std.testing.expectEqual(7, identity.store_id);
    try std.testing.expectEqualStrings("orders|west", identity.store_name);
}

test "KV failed registration rolls back without touching existing counter zero" {
    var values: [128 * 4]u8 align(128) = @splat(0);
    var metadata: [256 * 4]u8 align(4) = @splat(0);
    var manager = counters.CountersManager.init(&values, &metadata);
    const existing = manager.allocate(99, "existing").?;
    manager.set(existing, 42);
    try std.testing.expectError(error.CounterAllocationFailed, KvMetrics.init(&manager, 1, "x"));
    try std.testing.expectEqual(1, manager.allocatedCount());
    try std.testing.expectEqual(42, manager.get(existing));
    var long: [245]u8 = @splat('a');
    try std.testing.expectError(error.StoreLabelTooLong, KvMetrics.init(&manager, 1, &long));
}

test "KV export validates type and escapes store name with histogram labels" {
    try std.testing.expect(decode(0, "kv|1|x") == null);
    try std.testing.expect(decode(Metric.entries.typeId(), "kv|bad|x") == null);
    var buf: [512]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&buf);
    const identity = decode(Metric.apply_txn_ns_bucket_inf.typeId(), "kv|12|a\"b\nc").?;
    try writeSeries(&writer, identity, 3, "node_id=\"1\",");
    try std.testing.expectEqualStrings("ringloom_kv_apply_txn_ns_bucket{node_id=\"1\",store_id=\"12\",store_name=\"a\\\"b\\nc\",le=\"+Inf\"} 3\n", writer.buffered());
}

test "KV empty buffers fail explicitly and schema families have declared kinds" {
    var values: [0]u8 align(128) = .{};
    var metadata: [0]u8 align(4) = .{};
    var manager = counters.CountersManager.init(&values, &metadata);
    try std.testing.expectError(error.CounterAllocationFailed, KvMetrics.init(&manager, 0, ""));
    inline for (0..metric_count) |i| {
        const metric: Metric = @fromBackingInt(i);
        try std.testing.expectEqual(metric.kind(), counters.metricKindFromTypeId(metric.typeId()));
        try std.testing.expectEqual(metric, decode(metric.typeId(), "kv|0|store").?.metric);
    }
    var buf: [8192]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&buf);
    try writeHelpTypes(&writer);
    try std.testing.expect(std.mem.indexOf(u8, writer.buffered(), "# TYPE ringloom_kv_apply_txn_ns histogram\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, writer.buffered(), "# TYPE ringloom_kv_entries gauge\n") != null);
    try std.testing.expect(std.mem.indexOf(u8, writer.buffered(), "# TYPE ringloom_kv_rebuilds counter\n") != null);
}
