//! Typed wrapper around CountersManager for well-known system counters.

const std = @import("std");
const CountersManager = @import("../concurrent/counters.zig").CountersManager;
const SystemCounter = @import("system_counter.zig").SystemCounter;

pub const SystemCounters = struct {
    counters: *CountersManager,
    ids: [SystemCounter.count]usize,

    pub fn init(counters_mgr: *CountersManager) !SystemCounters {
        var self = SystemCounters{
            .counters = counters_mgr,
            .ids = undefined,
        };

        // Allocate all well-known counters at startup.
        inline for (0..SystemCounter.count) |i| {
            const sc: SystemCounter = @enumFromInt(i);
            self.ids[i] = counters_mgr.allocate(@intCast(i), sc.label()) orelse
                return error.CounterAllocationFailed;
        }

        return self;
    }

    /// Atomically increment a counter by 1.
    pub inline fn increment(self: *const SystemCounters, counter: SystemCounter) void {
        self.counters.increment(self.ids[@intFromEnum(counter)]);
    }

    /// Atomically add `delta` to a counter.
    pub inline fn add(self: *const SystemCounters, counter: SystemCounter, delta: i64) void {
        self.counters.add(self.ids[@intFromEnum(counter)], delta);
    }

    /// Atomically set a counter to an absolute value.
    pub inline fn set(self: *const SystemCounters, counter: SystemCounter, value: i64) void {
        self.counters.set(self.ids[@intFromEnum(counter)], value);
    }

    /// Read the current value of a counter (atomic load).
    pub inline fn get(self: *const SystemCounters, counter: SystemCounter) i64 {
        return self.counters.get(self.ids[@intFromEnum(counter)]);
    }

    /// Conditionally update a counter to the maximum of current and new value.
    pub inline fn updateMax(self: *const SystemCounters, counter: SystemCounter, value: i64) void {
        const current = self.get(counter);
        if (value > current) {
            self.set(counter, value);
        }
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

fn createTestBuffers() struct { values: []align(128) u8, metadata: []u8 } {
    // 64 counters × 128 bytes = 8192 bytes for values
    const values = std.heap.page_allocator.alignedAlloc(u8, @enumFromInt(std.math.log2(@as(usize, 128))), 8192) catch @panic("alloc");
    @memset(values, 0);
    // 64 counters × 256 bytes = 16384 bytes for metadata
    const metadata = std.heap.page_allocator.alloc(u8, 16384) catch @panic("alloc");
    @memset(metadata, 0);
    return .{ .values = values, .metadata = metadata };
}

test "SystemCounters init allocates all well-known counters" {
    // Given
    const bufs = createTestBuffers();
    defer std.heap.page_allocator.free(bufs.values);
    defer std.heap.page_allocator.free(bufs.metadata);

    var manager = CountersManager.init(bufs.values, bufs.metadata);

    // When
    const counters = try SystemCounters.init(&manager);

    // Then — all well-known counters should be allocated with value 0
    inline for (0..SystemCounter.count) |i| {
        const sc: SystemCounter = @enumFromInt(i);
        try testing.expectEqual(@as(i64, 0), counters.get(sc));
    }
}

test "increment and get" {
    // Given
    const bufs = createTestBuffers();
    defer std.heap.page_allocator.free(bufs.values);
    defer std.heap.page_allocator.free(bufs.metadata);

    var manager = CountersManager.init(bufs.values, bufs.metadata);
    const counters = try SystemCounters.init(&manager);

    // When
    counters.increment(.bytes_sent);
    counters.increment(.bytes_sent);
    counters.increment(.bytes_sent);

    // Then
    try testing.expectEqual(@as(i64, 3), counters.get(.bytes_sent));
}

test "add delta" {
    // Given
    const bufs = createTestBuffers();
    defer std.heap.page_allocator.free(bufs.values);
    defer std.heap.page_allocator.free(bufs.metadata);

    var manager = CountersManager.init(bufs.values, bufs.metadata);
    const counters = try SystemCounters.init(&manager);

    // When
    counters.add(.bytes_sent, 1500);
    counters.add(.bytes_sent, 2048);

    // Then
    try testing.expectEqual(@as(i64, 3548), counters.get(.bytes_sent));
}

test "set absolute value" {
    // Given
    const bufs = createTestBuffers();
    defer std.heap.page_allocator.free(bufs.values);
    defer std.heap.page_allocator.free(bufs.metadata);

    var manager = CountersManager.init(bufs.values, bufs.metadata);
    const counters = try SystemCounters.init(&manager);

    // When
    counters.set(.control_loop_cycle_time_max, 42_000);

    // Then
    try testing.expectEqual(@as(i64, 42_000), counters.get(.control_loop_cycle_time_max));
}

test "updateMax only updates when new value is larger" {
    // Given
    const bufs = createTestBuffers();
    defer std.heap.page_allocator.free(bufs.values);
    defer std.heap.page_allocator.free(bufs.metadata);

    var manager = CountersManager.init(bufs.values, bufs.metadata);
    const counters = try SystemCounters.init(&manager);

    // When
    counters.updateMax(.receiver_cycle_time_max, 100);
    counters.updateMax(.receiver_cycle_time_max, 50);
    counters.updateMax(.receiver_cycle_time_max, 200);

    // Then
    try testing.expectEqual(@as(i64, 200), counters.get(.receiver_cycle_time_max));
}

test "SystemCounter.label returns non-empty strings for all counters" {
    // Given / When / Then
    inline for (0..SystemCounter.count) |i| {
        const sc: SystemCounter = @enumFromInt(i);
        try testing.expect(sc.label().len > 0);
    }
}

test "concurrent increments from multiple threads converge to correct sum" {
    // Given
    const num_threads = 4;
    const increments_per_thread = 100_000;

    const bufs = createTestBuffers();
    defer std.heap.page_allocator.free(bufs.values);
    defer std.heap.page_allocator.free(bufs.metadata);

    var manager = CountersManager.init(bufs.values, bufs.metadata);
    const counters = try SystemCounters.init(&manager);

    // When — spawn N threads, each incrementing bytes_sent M times
    var threads: [num_threads]std.Thread = undefined;
    for (&threads) |*t| {
        t.* = try std.Thread.spawn(.{}, struct {
            fn run(c: *const SystemCounters) void {
                for (0..increments_per_thread) |_| {
                    c.increment(.bytes_sent);
                }
            }
        }.run, .{&counters});
    }

    for (&threads) |*t| {
        t.join();
    }

    // Then — total should be num_threads × increments_per_thread
    const expected: i64 = num_threads * increments_per_thread;
    try testing.expectEqual(expected, counters.get(.bytes_sent));
}

test "concurrent add from multiple threads" {
    // Given
    const num_threads = 4;
    const adds_per_thread = 50_000;
    const delta: i64 = 100;

    const bufs = createTestBuffers();
    defer std.heap.page_allocator.free(bufs.values);
    defer std.heap.page_allocator.free(bufs.metadata);

    var manager = CountersManager.init(bufs.values, bufs.metadata);
    const counters = try SystemCounters.init(&manager);

    // When
    var threads: [num_threads]std.Thread = undefined;
    for (&threads) |*t| {
        t.* = try std.Thread.spawn(.{}, struct {
            fn run(c: *const SystemCounters) void {
                for (0..adds_per_thread) |_| {
                    c.add(.bytes_received, delta);
                }
            }
        }.run, .{&counters});
    }

    for (&threads) |*t| {
        t.join();
    }

    // Then
    const expected: i64 = num_threads * adds_per_thread * delta;
    try testing.expectEqual(expected, counters.get(.bytes_received));
}
