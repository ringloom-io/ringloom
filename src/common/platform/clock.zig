//! Clock utilities for the BRZ broker.
//!
//! Provides monotonic nanosecond timestamps for timing and wall-clock
//! millisecond timestamps for heartbeats.
//!
//! On x86_64 Linux release builds, `monotonicNanos()` uses RDTSC (~3-5 ns)
//! instead of clock_gettime(CLOCK_MONOTONIC) (~25 ns). The TSC frequency is
//! calibrated once on first use via a short clock_gettime spin.

const std = @import("std");
const builtin = @import("builtin");
const thread = @import("thread.zig");
const platform_io = @import("io.zig");

/// True when we can use RDTSC: x86_64, Linux, non-Debug build.
const use_rdtsc = builtin.cpu.arch == .x86_64 and
    builtin.os.tag == .linux and
    builtin.mode != .Debug;

pub const Clock = struct {
    /// Returns the current monotonic time in nanoseconds.
    ///
    /// This value is relative to an arbitrary epoch (usually boot time).
    /// It never goes backwards and is suitable for measuring elapsed time.
    ///
    /// Performance: ~3-5 ns on x86_64 Linux release (RDTSC),
    ///              ~25 ns on Linux debug (vDSO), ~22 ns on macOS.
    pub fn monotonicNanos() i64 {
        if (comptime use_rdtsc) {
            return rdtscNanos();
        } else if (comptime builtin.os.tag == .linux) {
            return monotonicNanosLinux();
        } else {
            return monotonicNanosStd();
        }
    }

    /// Returns a cross-thread stable monotonic timestamp in nanoseconds.
    ///
    /// Unlike `monotonicNanos()`, this always uses the OS monotonic clock on
    /// Linux instead of the RDTSC fast path. It is intended for benchmark
    /// timestamps that are compared across different threads/cores, where TSC
    /// skew or migration noise can distort very small deltas.
    ///
    /// Performance: ~25 ns on Linux via vDSO-backed `clock_gettime`.
    pub fn monotonicNanosStable() i64 {
        if (comptime builtin.os.tag == .linux) {
            return monotonicNanosLinux();
        } else {
            return monotonicNanosStd();
        }
    }

    // ── RDTSC fast path (x86_64 Linux release only) ──────────────────

    /// One-time calibration state, shared across threads via atomics.
    var tsc_freq_hz: std.atomic.Value(u64) = std.atomic.Value(u64).init(0);
    var tsc_base: std.atomic.Value(u64) = std.atomic.Value(u64).init(0);
    var ns_base: std.atomic.Value(i64) = std.atomic.Value(i64).init(0);

    /// Read the 64-bit TSC register via RDTSC.
    inline fn rdtscRead() u64 {
        return asm volatile (
            \\rdtsc
            \\shlq $32, %%rdx
            \\orq %%rdx, %%rax
            : [ret] "={rax}" (-> u64),
            :
            : .{ .rdx = true });
    }

    /// Convert a TSC reading to nanoseconds since boot.
    fn rdtscNanos() i64 {
        var freq = tsc_freq_hz.load(.acquire);
        if (freq == 0) {
            calibrateTsc();
            freq = tsc_freq_hz.load(.acquire);
            if (freq == 0) return monotonicNanosLinux(); // fallback
        }
        const tsc_now = rdtscRead();
        const base = tsc_base.load(.acquire);
        const base_ns = ns_base.load(.acquire);
        const delta_tsc = tsc_now -% base;
        const delta_ns: i64 = @intCast(@as(u128, delta_tsc) * std.time.ns_per_s / freq);
        return base_ns + delta_ns;
    }

    /// Calibrate TSC frequency by measuring wall-clock time over a spin interval.
    /// Only one thread performs calibration; others will find the result via atomics.
    fn calibrateTsc() void {
        // Double-check after potential race.
        if (tsc_freq_hz.load(.acquire) != 0) return;

        // Use clock_gettime for the reference interval.
        const t0_ns = monotonicNanosLinux();
        const tsc0 = rdtscRead();

        // Spin for ~2ms to get a stable measurement.
        const calibration_ns: i64 = 2_000_000;
        while (true) {
            const now_ns = monotonicNanosLinux();
            if (now_ns - t0_ns >= calibration_ns) break;
        }

        const t1_ns = monotonicNanosLinux();
        const tsc1 = rdtscRead();

        const elapsed_ns: u64 = @intCast(t1_ns - t0_ns);
        const elapsed_tsc = tsc1 -% tsc0;

        if (elapsed_ns == 0 or elapsed_tsc == 0) return;

        const freq: u64 = @intCast(@as(u128, elapsed_tsc) * std.time.ns_per_s / elapsed_ns);

        // Store calibration results (base + freq). Release ordering ensures
        // other threads see base/ns_base before seeing freq != 0.
        tsc_base.store(tsc0, .release);
        ns_base.store(t0_ns, .release);
        tsc_freq_hz.store(freq, .release);
    }

    // ── clock_gettime paths ──────────────────────────────────────────

    /// Linux fast path: clock_gettime(CLOCK_MONOTONIC) via vDSO.
    fn monotonicNanosLinux() i64 {
        var ts: std.os.linux.timespec = undefined;
        const rc = std.os.linux.clock_gettime(.MONOTONIC, &ts);
        if (rc != 0) {
            return monotonicNanosStd();
        }
        return @as(i64, @intCast(ts.sec)) * std.time.ns_per_s +
            @as(i64, @intCast(ts.nsec));
    }

    /// Portable fallback using std.time.
    fn monotonicNanosStd() i64 {
        const io = platform_io.default();
        return @intCast(std.Io.Clock.awake.now(io).nanoseconds);
    }

    /// Returns the current wall-clock time as milliseconds since the Unix epoch.
    ///
    /// On Linux, uses CLOCK_REALTIME_COARSE for minimal overhead (~5ns, ~1ms resolution).
    /// This is what gets written into the heartbeat timestamp field in metadata files.
    ///
    /// Performance: ~5ns on Linux (COARSE), ~25ns on macOS, ~15ns on Windows.
    pub fn epochMillis() i64 {
        if (comptime builtin.os.tag == .linux) {
            return epochMillisLinuxCoarse();
        } else {
            return epochMillisStd();
        }
    }

    /// Linux fast path: CLOCK_REALTIME_COARSE via vDSO.
    /// Resolution is ~1ms (jiffy-based), but the call is ~5× faster than CLOCK_REALTIME.
    fn epochMillisLinuxCoarse() i64 {
        var ts: std.os.linux.timespec = undefined;
        // CLOCK_REALTIME_COARSE = 5 on Linux.
        const CLOCK_REALTIME_COARSE: std.os.linux.clockid_t = @enumFromInt(5);
        const rc = std.os.linux.clock_gettime(CLOCK_REALTIME_COARSE, &ts);
        if (rc != 0) {
            // Fallback to standard clock if COARSE isn't available.
            return epochMillisStd();
        }
        return @as(i64, @intCast(ts.sec)) * std.time.ms_per_s +
            @divTrunc(@as(i64, @intCast(ts.nsec)), std.time.ns_per_ms);
    }

    /// Portable fallback using std.time.
    fn epochMillisStd() i64 {
        const io = platform_io.default();
        return @intCast(@divTrunc(std.Io.Clock.real.now(io).nanoseconds, std.time.ns_per_ms));
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

test "monotonicNanos is monotonically increasing" {
    const t1 = Clock.monotonicNanos();
    // Spin briefly to ensure time passes.
    var i: u32 = 0;
    while (i < 1000) : (i += 1) {
        std.atomic.spinLoopHint();
    }
    const t2 = Clock.monotonicNanos();
    try std.testing.expect(t2 > t1);
}

test "monotonicNanos returns positive values" {
    const t = Clock.monotonicNanos();
    try std.testing.expect(t > 0);
}

test "monotonicNanosStable is monotonically increasing" {
    const t1 = Clock.monotonicNanosStable();
    var i: u32 = 0;
    while (i < 1000) : (i += 1) {
        std.atomic.spinLoopHint();
    }
    const t2 = Clock.monotonicNanosStable();
    try std.testing.expect(t2 > t1);
}

test "monotonicNanosStable agrees with monotonicNanos within 5ms" {
    const stable = Clock.monotonicNanosStable();
    const fast = Clock.monotonicNanos();
    const diff = @abs(stable - fast);
    try std.testing.expect(diff < 5_000_000);
}

test "epochMillis returns reasonable value" {
    const ms = Clock.epochMillis();
    // Should be after 2024-01-01T00:00:00Z (1704067200000 ms since epoch).
    try std.testing.expect(ms > 1704067200000);
    // Should be before 2100-01-01T00:00:00Z.
    try std.testing.expect(ms < 4102444800000);
}

test "epochMillis advances over time" {
    const t1 = Clock.epochMillis();
    thread.sleepNanos(2 * std.time.ns_per_ms);
    const t2 = Clock.epochMillis();
    try std.testing.expect(t2 >= t1);
}

test "monotonicNanos agrees with clock_gettime within 5ms" {
    // Verify RDTSC-based clock (if active) stays close to the reference clock.
    const mono = Clock.monotonicNanos();
    var ts: std.os.linux.timespec = undefined;
    _ = std.os.linux.clock_gettime(.MONOTONIC, &ts);
    const ref = @as(i64, @intCast(ts.sec)) * std.time.ns_per_s +
        @as(i64, @intCast(ts.nsec));
    const diff = @abs(mono - ref);
    try std.testing.expect(diff < 5_000_000); // within 5ms
}
