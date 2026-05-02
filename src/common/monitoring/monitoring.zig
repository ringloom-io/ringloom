//! Monitoring snapshot capturing counters and error log entries.

const std = @import("std");
const clock = @import("../platform/clock.zig");
const CountersManager = @import("../concurrent/counters.zig").CountersManager;
const ErrorLog = @import("../concurrent/error_log.zig").ErrorLog;
const SystemCounter = @import("system_counter.zig").SystemCounter;
const SystemCounters = @import("system_counters.zig").SystemCounters;

pub const CounterEntry = struct {
    id: u8,
    label: []const u8,
    value: i64,
};

pub const ErrorEntry = struct {
    observation_count: i32,
    first_observation_timestamp: i64,
    last_observation_timestamp: i64,
    description: []const u8,
};

pub const MonitoringSnapshot = struct {
    node_id: u8,
    timestamp_ms: i64,
    counters: [SystemCounter.count]CounterEntry,
    error_count: usize,
    errors: [max_snapshot_errors]ErrorEntry,

    const max_snapshot_errors: usize = 64;
    const entry_header_length: usize = 24;

    pub fn take(
        node_id: u8,
        sys_counters: *const SystemCounters,
        error_log: *const ErrorLog,
    ) MonitoringSnapshot {
        var snapshot = MonitoringSnapshot{
            .node_id = node_id,
            .timestamp_ms = clock.Clock.epochMillis(),
            .counters = undefined,
            .error_count = 0,
            .errors = undefined,
        };

        // Capture all system counters.
        inline for (0..SystemCounter.count) |i| {
            const sc: SystemCounter = @enumFromInt(i);
            snapshot.counters[i] = .{
                .id = @intCast(i),
                .label = sc.label(),
                .value = sys_counters.get(sc),
            };
        }

        // Capture error log entries (up to max_snapshot_errors).
        snapshot.collectErrors(error_log);

        return snapshot;
    }

    fn collectErrors(self: *MonitoringSnapshot, error_log: *const ErrorLog) void {
        var offset: usize = 0;
        while (offset < error_log.buffer.len and self.error_count < max_snapshot_errors) {
            const entry_length_ptr: *const i32 = @ptrCast(@alignCast(error_log.buffer.ptr + offset));
            const entry_length = @atomicLoad(i32, entry_length_ptr, .acquire);
            if (entry_length <= 0) break;

            const base = offset;
            const obs_ptr: *const i32 = @ptrCast(@alignCast(error_log.buffer.ptr + base + 4));
            const last_ts_ptr: *const i64 = @ptrCast(@alignCast(error_log.buffer.ptr + base + 8));
            const first_ts_ptr: *const i64 = @ptrCast(@alignCast(error_log.buffer.ptr + base + 16));

            const desc_len: usize = @intCast(entry_length - @as(i32, @intCast(entry_header_length)));

            self.errors[self.error_count] = .{
                .observation_count = @atomicLoad(i32, obs_ptr, .acquire),
                .first_observation_timestamp = first_ts_ptr.*,
                .last_observation_timestamp = @atomicLoad(i64, last_ts_ptr, .acquire),
                .description = error_log.buffer[base + entry_header_length ..][0..desc_len],
            };
            self.error_count += 1;

            offset += std.mem.alignForward(usize, @intCast(entry_length), 8);
        }
    }

    /// Format the snapshot as human-readable text to a writer.
    pub fn dump(self: *const MonitoringSnapshot, writer: *std.Io.Writer) !void {
        try writer.print("=== BRZ Broker Node {d} — Monitoring Snapshot ===\n", .{self.node_id});
        try writer.print("Timestamp: {d} ms\n\n", .{self.timestamp_ms});

        try writer.print("--- Counters ---\n", .{});
        for (self.counters) |c| {
            if (c.value != 0) {
                try writer.print("  [{d:>2}] {s:<40} {d}\n", .{ c.id, c.label, c.value });
            }
        }

        try writer.print("\n--- Error Log ({d} entries) ---\n", .{self.error_count});
        for (self.errors[0..self.error_count]) |e| {
            try writer.print(
                "  [{d}x] {s}\n        first={d}  last={d}\n",
                .{ e.observation_count, e.description, e.first_observation_timestamp, e.last_observation_timestamp },
            );
        }
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

test "snapshot captures current counter values" {
    // Given
    const values = try std.heap.page_allocator.alignedAlloc(u8, @enumFromInt(std.math.log2(@as(usize, 128))), 8192);
    defer std.heap.page_allocator.free(values);
    @memset(values, 0);

    const metadata = try std.heap.page_allocator.alloc(u8, 16384);
    defer std.heap.page_allocator.free(metadata);
    @memset(metadata, 0);

    var manager = CountersManager.init(values, metadata);
    const counters = try SystemCounters.init(&manager);

    counters.add(.bytes_sent, 42_000);
    counters.increment(.messages_routed_local);

    var error_buf: [4096]u8 align(8) = @splat(0);
    const error_log = ErrorLog.init(&error_buf);

    // When
    const snapshot = MonitoringSnapshot.take(1, &counters, &error_log);

    // Then
    try std.testing.expectEqual(@as(u8, 1), snapshot.node_id);
    try std.testing.expectEqual(@as(i64, 42_000), snapshot.counters[@intFromEnum(SystemCounter.bytes_sent)].value);
    try std.testing.expectEqual(@as(i64, 1), snapshot.counters[@intFromEnum(SystemCounter.messages_routed_local)].value);
}

test "snapshot captures error log entries" {
    // Given
    const values = try std.heap.page_allocator.alignedAlloc(u8, @enumFromInt(std.math.log2(@as(usize, 128))), 8192);
    defer std.heap.page_allocator.free(values);
    @memset(values, 0);

    const metadata = try std.heap.page_allocator.alloc(u8, 16384);
    defer std.heap.page_allocator.free(metadata);
    @memset(metadata, 0);

    var manager = CountersManager.init(values, metadata);
    const counters = try SystemCounters.init(&manager);

    var error_buf: [4096]u8 align(8) = @splat(0);
    var error_log = ErrorLog.init(&error_buf);
    _ = error_log.record("test error one", 1000);
    _ = error_log.record("test error two", 2000);

    // When
    const snapshot = MonitoringSnapshot.take(0, &counters, &error_log);

    // Then
    try std.testing.expectEqual(@as(usize, 2), snapshot.error_count);
    try std.testing.expectEqualStrings("test error one", snapshot.errors[0].description);
    try std.testing.expectEqualStrings("test error two", snapshot.errors[1].description);
}

test "snapshot dump produces non-empty output" {
    // Given
    const values = try std.heap.page_allocator.alignedAlloc(u8, @enumFromInt(std.math.log2(@as(usize, 128))), 8192);
    defer std.heap.page_allocator.free(values);
    @memset(values, 0);

    const metadata = try std.heap.page_allocator.alloc(u8, 16384);
    defer std.heap.page_allocator.free(metadata);
    @memset(metadata, 0);

    var manager = CountersManager.init(values, metadata);
    const counters = try SystemCounters.init(&manager);
    counters.increment(.bytes_sent);

    var error_buf: [4096]u8 align(8) = @splat(0);
    const error_log = ErrorLog.init(&error_buf);

    const snapshot = MonitoringSnapshot.take(1, &counters, &error_log);

    // When
    var output_buf: [8192]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&output_buf);
    try snapshot.dump(&writer);

    // Then
    const output = writer.buffered();
    try std.testing.expect(output.len > 0);
    try std.testing.expect(std.mem.indexOf(u8, output, "bytes-sent") != null);
    try std.testing.expect(std.mem.indexOf(u8, output, "Node 1") != null);
}
