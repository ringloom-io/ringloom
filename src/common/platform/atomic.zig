//! Atomic operations for the RingLoom broker.
//!
//! Wraps Zig's atomic builtins with methods named to match RingLoom conventions.
//! All methods inline to single hardware instructions on x86-64.

const std = @import("std");
const constants = @import("constants.zig");

/// A 64-bit atomic integer.
///
/// Wraps Zig's `std.atomic.Value(i64)` with methods named to match RingLoom conventions.
/// This is a thin wrapper — all methods inline to a single instruction on x86-64.
pub const AtomicI64 = struct {
    value: std.atomic.Value(i64),

    const Self = @This();

    pub fn init(initial: i64) Self {
        return .{ .value = std.atomic.Value(i64).init(initial) };
    }

    /// Acquire load — pairs with a prior release store from another thread.
    /// x86-64: plain `mov` + compiler fence. ARM64: `ldar`.
    pub inline fn load(self: *const Self) i64 {
        return self.value.load(.acquire);
    }

    /// Release store — makes all prior writes visible to a subsequent acquire load.
    /// x86-64: compiler fence + plain `mov`. ARM64: `stlr`.
    pub inline fn store(self: *Self, val: i64) void {
        self.value.store(val, .release);
    }

    /// Monotonic load — no ordering guarantees beyond atomicity.
    /// Use only when the value is informational (counters, stats).
    pub inline fn loadMonotonic(self: *const Self) i64 {
        return self.value.load(.monotonic);
    }

    /// Monotonic store — no ordering guarantees beyond atomicity.
    pub inline fn storeMonotonic(self: *Self, val: i64) void {
        self.value.store(val, .monotonic);
    }

    /// Plain (non-atomic) read. Only safe when there is no concurrent access
    /// (e.g., single-consumer reading its own head_position).
    pub inline fn loadRaw(self: *const Self) i64 {
        return self.value.raw;
    }

    /// Plain (non-atomic) write. Only safe when there is no concurrent writer.
    pub inline fn storeRaw(self: *Self, val: i64) void {
        self.value.raw = val;
    }

    /// Fetch-and-add with monotonic ordering.
    /// x86-64: `lock xadd`. ARM64: `ldadd`.
    pub inline fn fetchAdd(self: *Self, delta: i64) i64 {
        return self.value.fetchAdd(delta, .monotonic);
    }

    /// Compare-and-swap (weak). Returns `null` on success, or the actual value on failure.
    /// Success ordering: acquire. Failure ordering: monotonic.
    /// x86-64: `lock cmpxchg`. ARM64: `cas` or LL/SC.
    pub inline fn compareAndSwap(self: *Self, expected: i64, desired: i64) ?i64 {
        return self.value.cmpxchgWeak(expected, desired, .acquire, .monotonic);
    }

    /// Get a pointer to the raw underlying value.
    /// Useful when passing to ProcessSynchronizer or direct memory operations.
    pub inline fn ptr(self: *Self) *i64 {
        return &self.value.raw;
    }
};

/// A 32-bit atomic integer.
///
/// Used for ring buffer record headers (length field) and process synchronization
/// (futex word is i32).
pub const AtomicI32 = struct {
    value: std.atomic.Value(i32),

    const Self = @This();

    pub fn init(initial: i32) Self {
        return .{ .value = std.atomic.Value(i32).init(initial) };
    }

    pub inline fn load(self: *const Self) i32 {
        return self.value.load(.acquire);
    }

    pub inline fn store(self: *Self, val: i32) void {
        self.value.store(val, .release);
    }

    pub inline fn loadMonotonic(self: *const Self) i32 {
        return self.value.load(.monotonic);
    }

    pub inline fn storeMonotonic(self: *Self, val: i32) void {
        self.value.store(val, .monotonic);
    }

    pub inline fn loadRaw(self: *const Self) i32 {
        return self.value.raw;
    }

    pub inline fn storeRaw(self: *Self, val: i32) void {
        self.value.raw = val;
    }

    pub inline fn fetchAdd(self: *Self, delta: i32) i32 {
        return self.value.fetchAdd(delta, .monotonic);
    }

    pub inline fn compareAndSwap(self: *Self, expected: i32, desired: i32) ?i32 {
        return self.value.cmpxchgWeak(expected, desired, .acquire, .monotonic);
    }

    pub inline fn ptr(self: *Self) *i32 {
        return &self.value.raw;
    }
};

/// Atomic boolean — used for the `running` flag in ThreadRunner.
pub const AtomicBool = struct {
    value: std.atomic.Value(bool),

    const Self = @This();

    pub fn init(initial: bool) Self {
        return .{ .value = std.atomic.Value(bool).init(initial) };
    }

    pub inline fn load(self: *const Self) bool {
        return self.value.load(.acquire);
    }

    pub inline fn store(self: *Self, val: bool) void {
        self.value.store(val, .release);
    }
};

/// A 64-bit atomic integer padded to 128 bytes (2 cache lines) to prevent
/// false sharing with adjacent fields.
///
/// Memory layout:
///   [0..8)    = the i64 value
///   [8..128)  = padding (120 bytes)
///
/// Used in the ring buffer trailer where tail_position, head_cache,
/// head_position, correlation_counter, and consumer_heartbeat each
/// occupy their own 128-byte slot.
pub const CacheLinePaddedAtomicI64 = extern struct {
    value: i64 align(constants.cache_line_pad) = 0,
    _padding: [constants.cache_line_pad - @sizeOf(i64)]u8 = [_]u8{0} ** (constants.cache_line_pad - @sizeOf(i64)),

    const Self = @This();

    comptime {
        // Static assertion: this struct must be exactly 128 bytes.
        if (@sizeOf(Self) != constants.cache_line_pad) {
            @compileError("CacheLinePaddedAtomicI64 must be exactly 128 bytes");
        }
    }

    pub fn init(initial: i64) Self {
        return .{
            .value = initial,
            ._padding = [_]u8{0} ** (constants.cache_line_pad - @sizeOf(i64)),
        };
    }

    pub inline fn atomicLoad(self: *const Self) i64 {
        return @atomicLoad(i64, &self.value, .acquire);
    }

    pub inline fn atomicStore(self: *Self, val: i64) void {
        @atomicStore(i64, &self.value, val, .release);
    }

    pub inline fn atomicLoadMonotonic(self: *const Self) i64 {
        return @atomicLoad(i64, &self.value, .monotonic);
    }

    pub inline fn atomicStoreMonotonic(self: *Self, val: i64) void {
        @atomicStore(i64, &self.value, val, .monotonic);
    }

    pub inline fn fetchAdd(self: *Self, delta: i64) i64 {
        return @atomicRmw(i64, &self.value, .Add, delta, .monotonic);
    }

    pub inline fn compareAndSwap(self: *Self, expected: i64, desired: i64) ?i64 {
        return @cmpxchgWeak(i64, &self.value, expected, desired, .acquire, .monotonic);
    }

    /// Returns a pointer to the raw value for direct memory-mapped access.
    pub inline fn ptr(self: *Self) *volatile i64 {
        return &self.value;
    }
};

/// A 32-bit atomic integer padded to 128 bytes. Used for the futex wait-state
/// words in the blocking ring buffer trailer.
pub const CacheLinePaddedAtomicI32 = extern struct {
    value: i32 align(constants.cache_line_pad) = 0,
    _padding: [constants.cache_line_pad - @sizeOf(i32)]u8 = [_]u8{0} ** (constants.cache_line_pad - @sizeOf(i32)),

    const Self = @This();

    comptime {
        if (@sizeOf(Self) != constants.cache_line_pad) {
            @compileError("CacheLinePaddedAtomicI32 must be exactly 128 bytes");
        }
    }

    pub fn init(initial: i32) Self {
        return .{
            .value = initial,
            ._padding = [_]u8{0} ** (constants.cache_line_pad - @sizeOf(i32)),
        };
    }

    pub inline fn atomicLoad(self: *const Self) i32 {
        return @atomicLoad(i32, &self.value, .acquire);
    }

    pub inline fn atomicStore(self: *Self, val: i32) void {
        @atomicStore(i32, &self.value, val, .release);
    }

    pub inline fn compareAndSwap(self: *Self, expected: i32, desired: i32) ?i32 {
        return @cmpxchgWeak(i32, &self.value, expected, desired, .acquire, .monotonic);
    }

    pub inline fn ptr(self: *Self) *volatile i32 {
        return &self.value;
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

test "CacheLinePaddedAtomicI64 is exactly 128 bytes" {
    try std.testing.expect(@sizeOf(CacheLinePaddedAtomicI64) == 128);
    try std.testing.expect(@alignOf(CacheLinePaddedAtomicI64) == 128);
}

test "CacheLinePaddedAtomicI32 is exactly 128 bytes" {
    try std.testing.expect(@sizeOf(CacheLinePaddedAtomicI32) == 128);
    try std.testing.expect(@alignOf(CacheLinePaddedAtomicI32) == 128);
}

test "AtomicI64 load/store" {
    var a = AtomicI64.init(42);
    try std.testing.expectEqual(@as(i64, 42), a.load());
    a.store(100);
    try std.testing.expectEqual(@as(i64, 100), a.load());
}

test "AtomicI64 fetchAdd" {
    var a = AtomicI64.init(10);
    const prev = a.fetchAdd(5);
    try std.testing.expectEqual(@as(i64, 10), prev);
    try std.testing.expectEqual(@as(i64, 15), a.load());
}

test "AtomicI64 compareAndSwap success" {
    var a = AtomicI64.init(42);
    const result = a.compareAndSwap(42, 99);
    try std.testing.expect(result == null); // success
    try std.testing.expectEqual(@as(i64, 99), a.load());
}

test "AtomicI64 compareAndSwap failure" {
    var a = AtomicI64.init(42);
    const result = a.compareAndSwap(0, 99);
    try std.testing.expect(result != null); // failure
    try std.testing.expectEqual(@as(i64, 42), result.?);
    try std.testing.expectEqual(@as(i64, 42), a.load()); // unchanged
}

test "CacheLinePaddedAtomicI64 basic operations" {
    var padded = CacheLinePaddedAtomicI64.init(0);
    padded.atomicStore(42);
    try std.testing.expectEqual(@as(i64, 42), padded.atomicLoad());
    const prev = padded.fetchAdd(8);
    try std.testing.expectEqual(@as(i64, 42), prev);
    try std.testing.expectEqual(@as(i64, 50), padded.atomicLoad());
}

test "adjacent CacheLinePaddedAtomicI64 fields do not share cache lines" {
    // Simulate the ring buffer trailer layout
    const Trailer = extern struct {
        begin_pad: CacheLinePaddedAtomicI64,
        tail_position: CacheLinePaddedAtomicI64,
        head_cache: CacheLinePaddedAtomicI64,
        head_position: CacheLinePaddedAtomicI64,
    };
    try std.testing.expect(@sizeOf(Trailer) == 4 * 128);
    var t: Trailer = undefined;
    _ = &t;
    const tail_addr = @intFromPtr(&t.tail_position);
    const head_cache_addr = @intFromPtr(&t.head_cache);
    try std.testing.expect(head_cache_addr - tail_addr == 128);
}

test "AtomicBool load/store" {
    var b = AtomicBool.init(false);
    try std.testing.expect(!b.load());
    b.store(true);
    try std.testing.expect(b.load());
}

test "AtomicI32 load/store" {
    var a = AtomicI32.init(0);
    try std.testing.expectEqual(@as(i32, 0), a.load());
    a.store(42);
    try std.testing.expectEqual(@as(i32, 42), a.load());
}

test "AtomicI32 fetchAdd" {
    var a = AtomicI32.init(10);
    const prev = a.fetchAdd(3);
    try std.testing.expectEqual(@as(i32, 10), prev);
    try std.testing.expectEqual(@as(i32, 13), a.load());
}

test "AtomicI32 compareAndSwap success" {
    var a = AtomicI32.init(42);
    const result = a.compareAndSwap(42, 99);
    try std.testing.expect(result == null);
    try std.testing.expectEqual(@as(i32, 99), a.load());
}

test "AtomicI32 compareAndSwap failure" {
    var a = AtomicI32.init(42);
    const result = a.compareAndSwap(0, 99);
    try std.testing.expect(result != null);
    try std.testing.expectEqual(@as(i32, 42), result.?);
}
