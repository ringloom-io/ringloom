//! Lightweight latency histogram for BRZ performance benchmarks.
//!
//! `Histogram` collects raw nanosecond samples in a growable array and
//! provides order-statistic queries (percentiles), min/max/mean, and
//! reset.  Sorting is performed lazily on the first statistical query
//! after new values have been recorded, so hot-path `record` calls
//! remain O(1) amortised.
//!
//! This is intentionally simple — no bucket quantisation, no HdrHistogram
//! compression.  It is suitable for benchmarks that capture up to a few
//! million samples where the full value set fits comfortably in memory.

const std = @import("std");
const Allocator = std.mem.Allocator;
const math = std.math;

/// A simple latency histogram backed by a sorted array of nanosecond values.
pub const Histogram = struct {
    values: std.ArrayList(u64),
    allocator: Allocator,
    sorted: bool,

    /// Creates an empty histogram.
    pub fn init(allocator: Allocator) Histogram {
        return .{
            .values = .empty,
            .allocator = allocator,
            .sorted = true,
        };
    }

    /// Creates an empty histogram with pre-allocated capacity for
    /// `capacity` samples, avoiding early re-allocations on the hot path.
    pub fn initCapacity(allocator: Allocator, capacity: usize) !Histogram {
        var list: std.ArrayList(u64) = .empty;
        try list.ensureTotalCapacity(allocator, capacity);
        return .{
            .values = list,
            .allocator = allocator,
            .sorted = true,
        };
    }

    /// Releases all memory owned by the histogram.
    pub fn deinit(self: *Histogram) void {
        self.values.deinit(self.allocator);
    }

    /// Records a single latency sample (in nanoseconds).
    pub fn record(self: *Histogram, value_ns: u64) !void {
        try self.values.append(self.allocator, value_ns);
        self.sorted = false;
    }

    /// Returns the number of recorded samples.
    pub fn count(self: *const Histogram) usize {
        return self.values.items.len;
    }

    /// Returns the value at the given percentile (0.0–100.0).
    ///
    /// Sorts the backing array lazily on first access after a `record`.
    /// Returns `0` when the histogram is empty.
    pub fn percentile(self: *Histogram, pct: f64) u64 {
        const n = self.values.items.len;
        if (n == 0) return 0;

        self.ensureSorted();

        const clamped = math.clamp(pct, 0.0, 100.0);
        const rank = (clamped / 100.0) * @as(f64, @floatFromInt(n));

        // Index is ceil(rank) - 1, clamped to valid bounds.
        const idx_f = @ceil(rank);
        const idx_raw: usize = if (idx_f < 1.0) 0 else @intFromFloat(idx_f - 1.0);
        const idx = @min(idx_raw, n - 1);

        return self.values.items[idx];
    }

    /// Returns the maximum recorded value, or `0` if empty.
    pub fn max(self: *const Histogram) u64 {
        if (self.values.items.len == 0) return 0;

        var result: u64 = 0;
        for (self.values.items) |v| {
            if (v > result) result = v;
        }
        return result;
    }

    /// Returns the minimum recorded value, or `0` if empty.
    pub fn min(self: *const Histogram) u64 {
        if (self.values.items.len == 0) return 0;

        var result: u64 = math.maxInt(u64);
        for (self.values.items) |v| {
            if (v < result) result = v;
        }
        return result;
    }

    /// Returns the arithmetic mean of all recorded values, or `0` if empty.
    pub fn mean(self: *const Histogram) u64 {
        const n = self.values.items.len;
        if (n == 0) return 0;

        var sum: u128 = 0;
        for (self.values.items) |v| {
            sum += v;
        }
        return @intCast(sum / @as(u128, n));
    }

    /// Discards all recorded samples, keeping allocated memory for reuse.
    pub fn reset(self: *Histogram) void {
        self.values.clearRetainingCapacity();
        self.sorted = true;
    }

    /// Convenience: returns (p50, p95, p99, p99_9, max) in a single call.
    pub fn summaryPercentiles(self: *Histogram) struct { p50: u64, p95: u64, p99: u64, p99_9: u64, max_val: u64 } {
        return .{
            .p50 = self.percentile(50.0),
            .p95 = self.percentile(95.0),
            .p99 = self.percentile(99.0),
            .p99_9 = self.percentile(99.9),
            .max_val = self.max(),
        };
    }

    // ── Internal ─────────────────────────────────────────────────

    fn ensureSorted(self: *Histogram) void {
        if (!self.sorted) {
            std.mem.sort(u64, self.values.items, {}, std.sort.asc(u64));
            self.sorted = true;
        }
    }
};

// ── Tests ────────────────────────────────────────────────────────────

test "empty histogram returns zeros" {
    // Given
    var h = Histogram.init(std.testing.allocator);
    defer h.deinit();

    // When / Then
    try std.testing.expectEqual(@as(usize, 0), h.count());
    try std.testing.expectEqual(@as(u64, 0), h.percentile(50.0));
    try std.testing.expectEqual(@as(u64, 0), h.max());
    try std.testing.expectEqual(@as(u64, 0), h.min());
    try std.testing.expectEqual(@as(u64, 0), h.mean());
}

test "single sample returns that sample for all queries" {
    // Given
    var h = Histogram.init(std.testing.allocator);
    defer h.deinit();

    // When
    try h.record(42_000);

    // Then
    try std.testing.expectEqual(@as(usize, 1), h.count());
    try std.testing.expectEqual(@as(u64, 42_000), h.percentile(0.0));
    try std.testing.expectEqual(@as(u64, 42_000), h.percentile(50.0));
    try std.testing.expectEqual(@as(u64, 42_000), h.percentile(100.0));
    try std.testing.expectEqual(@as(u64, 42_000), h.max());
    try std.testing.expectEqual(@as(u64, 42_000), h.min());
    try std.testing.expectEqual(@as(u64, 42_000), h.mean());
}

test "percentile returns correct values for known distribution" {
    // Given — record values 1..100 in arbitrary order.
    var h = Histogram.init(std.testing.allocator);
    defer h.deinit();

    var i: u64 = 100;
    while (i >= 1) : (i -= 1) {
        try h.record(i);
    }

    // When / Then
    try std.testing.expectEqual(@as(usize, 100), h.count());

    // p50 → value at rank 50 → 50
    try std.testing.expectEqual(@as(u64, 50), h.percentile(50.0));
    // p95 → value at rank 95 → 95
    try std.testing.expectEqual(@as(u64, 95), h.percentile(95.0));
    // p99 → value at rank 99 → 99
    try std.testing.expectEqual(@as(u64, 99), h.percentile(99.0));
    // p100 → 100
    try std.testing.expectEqual(@as(u64, 100), h.percentile(100.0));
    // p0 → 1
    try std.testing.expectEqual(@as(u64, 1), h.percentile(0.0));
}

test "min and max with multiple values" {
    // Given
    var h = Histogram.init(std.testing.allocator);
    defer h.deinit();

    try h.record(500);
    try h.record(100);
    try h.record(900);
    try h.record(200);

    // When / Then
    try std.testing.expectEqual(@as(u64, 100), h.min());
    try std.testing.expectEqual(@as(u64, 900), h.max());
}

test "mean computes arithmetic average" {
    // Given
    var h = Histogram.init(std.testing.allocator);
    defer h.deinit();

    try h.record(10);
    try h.record(20);
    try h.record(30);

    // When / Then — (10 + 20 + 30) / 3 = 20
    try std.testing.expectEqual(@as(u64, 20), h.mean());
}

test "reset clears all samples" {
    // Given
    var h = Histogram.init(std.testing.allocator);
    defer h.deinit();

    try h.record(1);
    try h.record(2);
    try h.record(3);
    try std.testing.expectEqual(@as(usize, 3), h.count());

    // When
    h.reset();

    // Then
    try std.testing.expectEqual(@as(usize, 0), h.count());
    try std.testing.expectEqual(@as(u64, 0), h.percentile(50.0));
}

test "record after reset accumulates fresh samples" {
    // Given
    var h = Histogram.init(std.testing.allocator);
    defer h.deinit();

    try h.record(999);
    h.reset();

    // When
    try h.record(7);

    // Then
    try std.testing.expectEqual(@as(usize, 1), h.count());
    try std.testing.expectEqual(@as(u64, 7), h.percentile(50.0));
    try std.testing.expectEqual(@as(u64, 7), h.max());
}

test "initCapacity pre-allocates without affecting count" {
    // Given
    var h = try Histogram.initCapacity(std.testing.allocator, 1024);
    defer h.deinit();

    // Then
    try std.testing.expectEqual(@as(usize, 0), h.count());

    // When — record should not trigger realloc for a while.
    try h.record(42);
    try std.testing.expectEqual(@as(usize, 1), h.count());
}

test "summaryPercentiles returns consistent snapshot" {
    // Given
    var h = Histogram.init(std.testing.allocator);
    defer h.deinit();

    var i: u64 = 1;
    while (i <= 1000) : (i += 1) {
        try h.record(i);
    }

    // When
    const s = h.summaryPercentiles();

    // Then
    try std.testing.expectEqual(@as(u64, 500), s.p50);
    try std.testing.expectEqual(@as(u64, 950), s.p95);
    try std.testing.expectEqual(@as(u64, 990), s.p99);
    try std.testing.expectEqual(@as(u64, 1000), s.p99_9);
    try std.testing.expectEqual(@as(u64, 1000), s.max_val);
}

test "percentile clamps out-of-range inputs" {
    // Given
    var h = Histogram.init(std.testing.allocator);
    defer h.deinit();

    try h.record(10);
    try h.record(20);

    // When / Then — negative clamps to 0th percentile, >100 clamps to 100th.
    try std.testing.expectEqual(@as(u64, 10), h.percentile(-5.0));
    try std.testing.expectEqual(@as(u64, 20), h.percentile(150.0));
}
