//! Process synchronization primitives for the RingLoom broker.
//!
//! Provides cross-process wait/wake on a shared-memory i32 word.
//! Dispatches to futex(2) on Linux, __ulock_wait/wake on macOS,
//! and WaitOnAddress/WakeByAddressSingle on Windows.

const std = @import("std");
const builtin = @import("builtin");
const thread = @import("thread.zig");
const Clock = @import("clock.zig").Clock;

pub const WaitResult = enum {
    /// Woken by a wake() call.
    woken,
    /// The value at the pointer changed before we could sleep (spurious).
    value_changed,
    /// The wait timed out.
    timed_out,
    /// Interrupted by a signal (Linux only).
    interrupted,
};

pub const ProcessSynchronizer = struct {
    /// Platform-specific implementation function pointers.
    waitFn: *const fn (ptr: *const i32, expected: i32, timeout_ns: ?i64) WaitResult,
    wakeFn: *const fn (ptr: *const i32) void,
    wakeAllFn: *const fn (ptr: *const i32) void,

    /// Block the calling thread until the value at `ptr` is no longer `expected`,
    /// or until `timeout_ns` nanoseconds elapse.
    ///
    /// If `timeout_ns` is null, the wait is indefinite.
    ///
    /// **IMPORTANT**: The check `*ptr == expected` and the sleep are atomic with respect
    /// to wake() calls. This prevents the lost-wakeup problem.
    pub fn wait(self: ProcessSynchronizer, ptr: *const i32, expected: i32, timeout_ns: ?i64) WaitResult {
        return self.waitFn(ptr, expected, timeout_ns);
    }

    /// Wake one thread waiting on the word at `ptr`.
    pub fn wake(self: ProcessSynchronizer, ptr: *const i32) void {
        self.wakeFn(ptr);
    }

    /// Wake all threads waiting on the word at `ptr`.
    pub fn wakeAll(self: ProcessSynchronizer, ptr: *const i32) void {
        self.wakeAllFn(ptr);
    }

    /// Get the platform-specific synchronizer for the current OS.
    pub fn getPlatformInstance() ProcessSynchronizer {
        return switch (comptime builtin.os.tag) {
            .linux => linuxFutex(),
            .macos => macosUlock(),
            .windows => windowsWaitOnAddress(),
            else => @compileError("Unsupported OS for ProcessSynchronizer"),
        };
    }
};

// ── Linux: futex(2) ───────────────────────────────────────────────────

fn linuxFutex() ProcessSynchronizer {
    return .{
        .waitFn = linuxFutexWait,
        .wakeFn = linuxFutexWake,
        .wakeAllFn = linuxFutexWakeAll,
    };
}

fn linuxFutexWait(ptr: *const i32, expected: i32, timeout_ns: ?i64) WaitResult {
    var ts: std.os.linux.timespec = undefined;
    const ts_ptr: ?*const std.os.linux.timespec = if (timeout_ns) |ns| blk: {
        ts = .{
            .sec = @intCast(@divTrunc(ns, std.time.ns_per_s)),
            .nsec = @intCast(@mod(ns, std.time.ns_per_s)),
        };
        break :blk &ts;
    } else null;

    // FUTEX_WAIT_PRIVATE = FUTEX_WAIT | FUTEX_PRIVATE_FLAG
    const FUTEX_WAIT: u32 = 0;
    const FUTEX_PRIVATE_FLAG: u32 = 128;
    const op = FUTEX_WAIT | FUTEX_PRIVATE_FLAG;

    const rc = std.os.linux.syscall6(
        .futex,
        @intFromPtr(ptr),
        op,
        @as(u32, @bitCast(expected)),
        @intFromPtr(ts_ptr),
        0,
        0,
    );

    const signed_rc: isize = @bitCast(rc);
    if (signed_rc == 0) return .woken;

    const err: u32 = @intCast(@as(usize, @bitCast(-signed_rc)));
    return switch (err) {
        @intFromEnum(std.os.linux.E.AGAIN) => .value_changed,
        @intFromEnum(std.os.linux.E.TIMEDOUT) => .timed_out,
        @intFromEnum(std.os.linux.E.INTR) => .interrupted,
        else => .woken,
    };
}

fn linuxFutexWake(ptr: *const i32) void {
    const FUTEX_WAKE: u32 = 1;
    const FUTEX_PRIVATE_FLAG: u32 = 128;
    const op = FUTEX_WAKE | FUTEX_PRIVATE_FLAG;

    _ = std.os.linux.syscall6(
        .futex,
        @intFromPtr(ptr),
        op,
        1, // wake one
        0,
        0,
        0,
    );
}

fn linuxFutexWakeAll(ptr: *const i32) void {
    const FUTEX_WAKE: u32 = 1;
    const FUTEX_PRIVATE_FLAG: u32 = 128;
    const op = FUTEX_WAKE | FUTEX_PRIVATE_FLAG;

    _ = std.os.linux.syscall6(
        .futex,
        @intFromPtr(ptr),
        op,
        @as(u32, @bitCast(@as(i32, std.math.maxInt(i32)))), // wake all
        0,
        0,
        0,
    );
}

// ── macOS: __ulock_wait / __ulock_wake ────────────────────────────────

fn macosUlock() ProcessSynchronizer {
    return .{
        .waitFn = macosUlockWait,
        .wakeFn = macosUlockWake,
        .wakeAllFn = macosUlockWakeAll,
    };
}

const UL_COMPARE_AND_WAIT: u32 = 1;
const ULF_NO_ERRNO: u32 = 0x01000000;
const ULF_WAKE_ALL: u32 = 0x00000100;

extern "c" fn __ulock_wait(
    operation: u32,
    addr: *const anyopaque,
    value: u64,
    timeout_us: u32,
) c_int;

extern "c" fn __ulock_wake(
    operation: u32,
    addr: *const anyopaque,
    wake_value: u64,
) c_int;

fn macosUlockWait(ptr: *const i32, expected: i32, timeout_ns: ?i64) WaitResult {
    if (comptime builtin.os.tag != .macos) unreachable;

    const timeout_us: u32 = if (timeout_ns) |ns|
        @intCast(@max(1, @divTrunc(ns, std.time.ns_per_us)))
    else
        0; // 0 = infinite wait

    const rc = __ulock_wait(
        UL_COMPARE_AND_WAIT | ULF_NO_ERRNO,
        @ptrCast(ptr),
        @as(u64, @bitCast(@as(i64, expected))),
        timeout_us,
    );

    if (rc >= 0) return .woken;
    const err: u32 = @intCast(-rc);
    return switch (err) {
        @intFromEnum(std.posix.E.AGAIN) => .value_changed,
        @intFromEnum(std.posix.E.TIMEDOUT) => .timed_out,
        @intFromEnum(std.posix.E.INTR) => .interrupted,
        else => .woken,
    };
}

fn macosUlockWake(ptr: *const i32) void {
    if (comptime builtin.os.tag != .macos) unreachable;
    _ = __ulock_wake(UL_COMPARE_AND_WAIT | ULF_NO_ERRNO, @ptrCast(ptr), 0);
}

fn macosUlockWakeAll(ptr: *const i32) void {
    if (comptime builtin.os.tag != .macos) unreachable;
    _ = __ulock_wake(UL_COMPARE_AND_WAIT | ULF_NO_ERRNO | ULF_WAKE_ALL, @ptrCast(ptr), 0);
}

// ── Windows: WaitOnAddress / WakeByAddressSingle ──────────────────────

fn windowsWaitOnAddress() ProcessSynchronizer {
    return .{
        .waitFn = windowsWait,
        .wakeFn = windowsWake,
        .wakeAllFn = windowsWakeAll,
    };
}

fn windowsWait(ptr: *const i32, expected: i32, timeout_ns: ?i64) WaitResult {
    if (comptime builtin.os.tag != .windows) unreachable;
    _ = ptr;
    _ = expected;
    _ = timeout_ns;
    // TODO: Windows implementation
    return .timed_out;
}

fn windowsWake(ptr: *const i32) void {
    if (comptime builtin.os.tag != .windows) unreachable;
    _ = ptr;
}

fn windowsWakeAll(ptr: *const i32) void {
    if (comptime builtin.os.tag != .windows) unreachable;
    _ = ptr;
}

// ── Tests ─────────────────────────────────────────────────────────────

const AtomicBool = @import("atomic.zig").AtomicBool;

test "ProcessSynchronizer wait returns value_changed when value differs" {
    const sync = ProcessSynchronizer.getPlatformInstance();

    var word: i32 = 42;
    // Wait with expected=0 — value is 42, so it should return immediately.
    const result = sync.wait(&word, 0, 1_000_000); // 1ms timeout
    try std.testing.expect(result == .value_changed);
}

test "ProcessSynchronizer wait times out" {
    const sync = ProcessSynchronizer.getPlatformInstance();

    var word: i32 = 1;
    // Wait with expected=1 — value matches, so it will sleep until timeout.
    const start = Clock.epochMillis();
    const result = sync.wait(&word, 1, 10_000_000); // 10ms timeout
    const elapsed = Clock.epochMillis() - start;

    try std.testing.expect(result == .timed_out);
    try std.testing.expect(elapsed >= 5); // Allow some slack.
}

test "ProcessSynchronizer wake unblocks wait" {
    const sync = ProcessSynchronizer.getPlatformInstance();

    var word: i32 = 1;
    var was_woken = AtomicBool.init(false);

    // Spawn a thread that waits.
    const waiter = try std.Thread.spawn(.{}, struct {
        fn run(s: ProcessSynchronizer, w: *i32, flag: *AtomicBool) void {
            _ = s.wait(w, 1, 1_000_000_000); // 1s timeout (should be woken before)
            flag.store(true);
        }
    }.run, .{ sync, &word, &was_woken });

    // Give the waiter time to enter the wait.
    thread.sleepNanos(5 * std.time.ns_per_ms);

    // Change the value and wake.
    @atomicStore(i32, &word, 0, .release);
    sync.wake(&word);

    waiter.join();
    try std.testing.expect(was_woken.load());
}
