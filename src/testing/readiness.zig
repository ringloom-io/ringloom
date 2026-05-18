//! Readiness detection for RingLoom end-to-end tests.
//!
//! Provides polling-based helpers that wait for child processes to reach
//! a known-good state before the test proceeds.  The primary mechanism
//! is scanning the process's stdout pipe (via `ProcessHandle.readAvailableOutput`)
//! for well-known marker lines emitted by the broker and service runtimes.
//!
//! All wait functions poll at a configurable or default interval and
//! return `error.Timeout` if the condition is not met within the
//! specified deadline.
//!
//! ## Marker conventions
//!
//! | Runtime  | Marker line (substring match)            |
//! |----------|------------------------------------------|
//! | Broker   | `"broker started"` or `"broker ready"`   |
//! | Service  | `"service ready"` or `"service registered"` |

const std = @import("std");
const time = std.time;

const test_io = @import("io.zig");
const process_runner = @import("process_runner.zig");
const ProcessHandle = process_runner.ProcessHandle;
const ProcessState = process_runner.ProcessState;
const ringloom_common = @import("ringloom_common");
const Clock = ringloom_common.platform.Clock;
const CncFile = ringloom_common.monitoring.aeron_cnc_reader.CncFile;

/// Default polling interval used by wait functions (50 ms).
pub const default_poll_interval_ms: u64 = 50;

/// Polls the process's stdout capture for a line containing `needle`,
/// returning as soon as the needle is found.
///
/// Each poll iteration performs a non-blocking read of the child's stdout
/// pipe (appending to the in-memory `stdout_capture`), then checks
/// whether the accumulated output contains `needle`.
///
/// Returns `error.Timeout` if the needle is not found within `timeout_ms`.
/// Returns `error.ProcessExited` if the child exits before the needle
/// is detected.
pub fn waitForLogLine(handle: *ProcessHandle, needle: []const u8, timeout_ms: u64) !void {
    const deadline_ns: i128 = @as(i128, Clock.monotonicNanosStable()) + @as(i128, timeout_ms) * time.ns_per_ms;

    while (Clock.monotonicNanosStable() < deadline_ns) {
        // Drain any bytes that have accumulated on the pipe.
        _ = handle.drainAvailableOutput() catch {};

        // Check accumulated output for the marker.
        if (handle.stdout_capture.contains(needle)) {
            handle.markReady();
            return;
        }

        // If the child has already exited we will never see more output.
        if (!handle.isAlive() and handle.state == .exited) {
            // One last drain in case bytes arrived between the last read
            // and the liveness check.
            _ = handle.drainAvailableOutput() catch {};
            if (handle.stdout_capture.contains(needle)) {
                handle.markReady();
                return;
            }
            return error.ProcessExited;
        }

        test_io.sleepMs(default_poll_interval_ms);
    }

    return error.Timeout;
}

/// Polls until the file at `path` exists on disk.
///
/// This is useful for waiting on metadata files that the broker or
/// service creates during startup (e.g. the service metadata `.dat`
/// file under `/dev/shm`).
///
/// Returns `error.Timeout` if the file does not appear within `timeout_ms`.
pub fn waitForFileExists(path: []const u8, timeout_ms: u64) !void {
    const deadline_ns: i128 = @as(i128, Clock.monotonicNanosStable()) + @as(i128, timeout_ms) * time.ns_per_ms;

    while (Clock.monotonicNanosStable() < deadline_ns) {
        if (test_io.access(path)) |_| {
            return;
        } else |_| {}

        test_io.sleepMs(default_poll_interval_ms);
    }

    return error.Timeout;
}

/// Polls until a directory at `path` exists and contains at least
/// `min_entries` entries (files or subdirectories).
///
/// Useful for waiting until the broker has created service metadata
/// files in a group's `services/` directory.
///
/// Returns `error.Timeout` if the condition is not met within `timeout_ms`.
pub fn waitForDirectoryPopulated(path: []const u8, min_entries: usize, timeout_ms: u64) !void {
    const deadline_ns: i128 = @as(i128, Clock.monotonicNanosStable()) + @as(i128, timeout_ms) * time.ns_per_ms;

    while (Clock.monotonicNanosStable() < deadline_ns) {
        if (countDirectoryEntries(path)) |count| {
            if (count >= min_entries) return;
        } else |_| {}

        test_io.sleepMs(default_poll_interval_ms);
    }

    return error.Timeout;
}

/// Generic condition polling.
///
/// Calls `condition_fn` every `poll_interval_ms` milliseconds until it
/// returns `true` or `timeout_ms` elapses.
///
/// Returns `error.Timeout` if the condition never becomes true.
pub fn waitForCondition(
    condition_fn: *const fn () bool,
    timeout_ms: u64,
    poll_interval_ms: u64,
) !void {
    const interval = if (poll_interval_ms == 0) default_poll_interval_ms else poll_interval_ms;
    const deadline_ns: i128 = @as(i128, Clock.monotonicNanosStable()) + @as(i128, timeout_ms) * time.ns_per_ms;

    while (Clock.monotonicNanosStable() < deadline_ns) {
        if (condition_fn()) return;
        test_io.sleepMs(interval);
    }

    return error.Timeout;
}

/// Generic condition polling with a context pointer.
///
/// Like `waitForCondition` but the callback receives a caller-supplied
/// `context` pointer, avoiding the need for global state.
pub fn waitForConditionCtx(
    comptime Context: type,
    context: Context,
    condition_fn: *const fn (Context) bool,
    timeout_ms: u64,
    poll_interval_ms: u64,
) !void {
    const interval = if (poll_interval_ms == 0) default_poll_interval_ms else poll_interval_ms;
    const deadline_ns: i128 = @as(i128, Clock.monotonicNanosStable()) + @as(i128, timeout_ms) * time.ns_per_ms;

    while (Clock.monotonicNanosStable() < deadline_ns) {
        if (condition_fn(context)) return;
        test_io.sleepMs(interval);
    }

    return error.Timeout;
}

// ── Convenience wrappers ─────────────────────────────────────────────

/// Waits for the broker to report that it has started.
///
/// Scans the process's stdout for `"broker started"` or `"broker ready"`.
pub fn waitForBrokerReady(handle: *ProcessHandle, timeout_ms: u64) !void {
    const deadline_ns: i128 = @as(i128, Clock.monotonicNanosStable()) + @as(i128, timeout_ms) * time.ns_per_ms;

    while (Clock.monotonicNanosStable() < deadline_ns) {
        _ = handle.drainAvailableOutput() catch {};

        if (handle.stdout_capture.contains("broker started") or
            handle.stdout_capture.contains("broker ready"))
        {
            if (handle.aeron_directory) |directory| {
                try waitForAeronDriverReady(handle, directory, remainingMillis(deadline_ns));
            }
            handle.markReady();
            return;
        }

        if (!handle.isAlive() and handle.state == .exited) {
            _ = handle.drainAvailableOutput() catch {};
            if (handle.stdout_capture.contains("broker started") or
                handle.stdout_capture.contains("broker ready"))
            {
                if (handle.aeron_directory) |directory| {
                    try waitForAeronDriverReady(handle, directory, remainingMillis(deadline_ns));
                }
                handle.markReady();
                return;
            }
            return error.ProcessExited;
        }

        test_io.sleepMs(default_poll_interval_ms);
    }

    return error.Timeout;
}

/// Waits for a service to report that it has registered with the broker.
///
/// Scans the process's stdout for `"service ready"` or `"service registered"`.
pub fn waitForServiceReady(handle: *ProcessHandle, timeout_ms: u64) !void {
    const deadline_ns: i128 = @as(i128, Clock.monotonicNanosStable()) + @as(i128, timeout_ms) * time.ns_per_ms;

    while (Clock.monotonicNanosStable() < deadline_ns) {
        _ = handle.drainAvailableOutput() catch {};

        if (handle.stdout_capture.contains("service ready") or
            handle.stdout_capture.contains("service registered"))
        {
            handle.markReady();
            return;
        }

        if (!handle.isAlive() and handle.state == .exited) {
            _ = handle.drainAvailableOutput() catch {};
            if (handle.stdout_capture.contains("service ready") or
                handle.stdout_capture.contains("service registered"))
            {
                handle.markReady();
                return;
            }
            return error.ProcessExited;
        }

        test_io.sleepMs(default_poll_interval_ms);
    }

    return error.Timeout;
}

/// Waits for multiple processes to all become ready.
///
/// Polls all handles in round-robin fashion, marking each ready when
/// its marker is found.  Returns as soon as every handle is in the
/// `ready` state, or returns `error.Timeout` if the deadline elapses.
pub fn waitForAllReady(
    handles: []const *ProcessHandle,
    needles: []const []const u8,
    timeout_ms: u64,
) !void {
    std.debug.assert(handles.len == needles.len);

    const deadline_ns: i128 = @as(i128, Clock.monotonicNanosStable()) + @as(i128, timeout_ms) * time.ns_per_ms;

    while (Clock.monotonicNanosStable() < deadline_ns) {
        var all_ready = true;

        for (handles, 0..) |handle, i| {
            if (handle.state == .ready) continue;

            _ = handle.drainAvailableOutput() catch {};
            if (handle.stdout_capture.contains(needles[i])) {
                handle.markReady();
            } else {
                all_ready = false;
            }
        }

        if (all_ready) return;

        test_io.sleepMs(default_poll_interval_ms);
    }

    return error.Timeout;
}

// ── Internal helpers ─────────────────────────────────────────────────

fn countDirectoryEntries(path: []const u8) !usize {
    var dir = try test_io.openDir(path, .{ .iterate = true });
    defer dir.close(test_io.io());

    var count: usize = 0;
    var iter = dir.iterate();
    while (try iter.next(test_io.io())) |_| {
        count += 1;
    }
    return count;
}

fn remainingMillis(deadline_ns: i128) u64 {
    const now = Clock.monotonicNanosStable();
    if (now >= deadline_ns) return 0;
    const remaining_ns: u64 = @intCast(deadline_ns - now);
    return @max(1, remaining_ns / time.ns_per_ms);
}

fn waitForAeronDriverReady(handle: *ProcessHandle, directory: []const u8, timeout_ms: u64) !void {
    const deadline_ns: i128 = @as(i128, Clock.monotonicNanosStable()) + @as(i128, timeout_ms) * time.ns_per_ms;

    while (Clock.monotonicNanosStable() < deadline_ns) {
        var cnc = CncFile.open(test_io.io(), directory) catch |err| switch (err) {
            error.MissingCncFile, error.InvalidCncFile => {
                _ = handle.drainAvailableOutput() catch {};
                if (!handle.isAlive() and handle.state == .exited) return error.ProcessExited;
                test_io.sleepMs(default_poll_interval_ms);
                continue;
            },
            else => return err,
        };
        defer cnc.close(test_io.io());

        if (cnc.metadata.cnc_version > 0 and cnc.metadata.driverAlive()) {
            return;
        }

        _ = handle.drainAvailableOutput() catch {};
        if (!handle.isAlive() and handle.state == .exited) return error.ProcessExited;
        test_io.sleepMs(default_poll_interval_ms);
    }

    return error.Timeout;
}

// ── Tests ────────────────────────────────────────────────────────────

test "waitForFileExists succeeds when file already exists" {
    // Given — create a temporary file.
    const tmp_path = "/tmp/ringloom-test-readiness-exists.marker";
    var file = try test_io.createFile(tmp_path, .{});
    file.close(test_io.io());
    defer test_io.deleteFile(tmp_path) catch {};

    // When / Then — should return immediately (well within timeout).
    try waitForFileExists(tmp_path, 1000);
}

test "waitForFileExists returns Timeout for missing file" {
    // Given — a path that does not exist.
    const bogus_path = "/tmp/ringloom-test-readiness-missing-file-does-not-exist.marker";

    // When
    const result = waitForFileExists(bogus_path, 150);

    // Then
    try std.testing.expectError(error.Timeout, result);
}

test "waitForDirectoryPopulated succeeds when directory has entries" {
    // Given
    const dir_path = "/tmp/ringloom-test-readiness-populated";
    try test_io.createDirPath(dir_path);
    defer test_io.deleteTree(dir_path) catch {};

    // Create two marker files.
    var f1 = try test_io.createFile("/tmp/ringloom-test-readiness-populated/a.txt", .{});
    f1.close(test_io.io());
    var f2 = try test_io.createFile("/tmp/ringloom-test-readiness-populated/b.txt", .{});
    f2.close(test_io.io());

    // When / Then — require at least 2 entries.
    try waitForDirectoryPopulated(dir_path, 2, 1000);
}

test "waitForDirectoryPopulated returns Timeout when not enough entries" {
    // Given
    const dir_path = "/tmp/ringloom-test-readiness-underpop";
    try test_io.createDirPath(dir_path);
    defer test_io.deleteTree(dir_path) catch {};

    // Only one file, but we require 5.
    var f = try test_io.createFile("/tmp/ringloom-test-readiness-underpop/only.txt", .{});
    f.close(test_io.io());

    // When
    const result = waitForDirectoryPopulated(dir_path, 5, 150);

    // Then
    try std.testing.expectError(error.Timeout, result);
}

test "waitForCondition succeeds when condition is immediately true" {
    // Given
    const alwaysTrue = struct {
        fn f() bool {
            return true;
        }
    }.f;

    // When / Then
    try waitForCondition(alwaysTrue, 1000, 10);
}

test "waitForCondition returns Timeout when condition never true" {
    // Given
    const alwaysFalse = struct {
        fn f() bool {
            return false;
        }
    }.f;

    // When
    const result = waitForCondition(alwaysFalse, 150, 10);

    // Then
    try std.testing.expectError(error.Timeout, result);
}

test "waitForConditionCtx receives context" {
    // Given
    const threshold: *const u32 = &@as(u32, 42);
    const checkCtx = struct {
        fn f(ctx: *const u32) bool {
            return ctx.* == 42;
        }
    }.f;

    // When / Then
    try waitForConditionCtx(*const u32, threshold, checkCtx, 1000, 10);
}

test "waitForLogLine detects marker in echo output" {
    // Given
    const allocator = std.testing.allocator;
    const logs_dir = "/tmp/ringloom-test-readiness-logline";
    try test_io.createDirPath(logs_dir);
    defer test_io.deleteTree(logs_dir) catch {};

    // Spawn a process that prints a known marker line.
    var handle = try ProcessHandle.spawn(
        allocator,
        "readiness_echo",
        "/bin/echo",
        &.{"broker started on port 9000"},
        logs_dir,
    );
    defer handle.deinit();

    // When
    try waitForLogLine(&handle, "broker started", 5000);

    // Then — the handle should be marked ready.
    try std.testing.expectEqual(ProcessState.ready, handle.state);
}

test "waitForLogLine returns ProcessExited when child dies without marker" {
    // Given
    const allocator = std.testing.allocator;
    const logs_dir = "/tmp/ringloom-test-readiness-nolog";
    try test_io.createDirPath(logs_dir);
    defer test_io.deleteTree(logs_dir) catch {};

    // Spawn a process that prints something *other* than the expected marker.
    var handle = try ProcessHandle.spawn(
        allocator,
        "readiness_nomatch",
        "/bin/echo",
        &.{"something else entirely"},
        logs_dir,
    );
    defer handle.deinit();

    // Give the process a moment to finish.
    test_io.sleepMs(200);

    // When
    const result = waitForLogLine(&handle, "broker started", 1000);

    // Then
    try std.testing.expectError(error.ProcessExited, result);
}

test "waitForBrokerReady detects broker started marker" {
    // Given
    const allocator = std.testing.allocator;
    const logs_dir = "/tmp/ringloom-test-readiness-broker";
    try test_io.createDirPath(logs_dir);
    defer test_io.deleteTree(logs_dir) catch {};

    var handle = try ProcessHandle.spawn(
        allocator,
        "broker_ready_test",
        "/bin/echo",
        &.{"INFO: broker started successfully"},
        logs_dir,
    );
    defer handle.deinit();

    // When / Then — should find the marker.
    try waitForBrokerReady(&handle, 5000);
    try std.testing.expectEqual(ProcessState.ready, handle.state);
}

test "waitForBrokerReady without Aeron directory keeps legacy marker behavior" {
    // Given
    const allocator = std.testing.allocator;
    const logs_dir = "/tmp/ringloom-test-readiness-broker-no-aeron";
    try test_io.createDirPath(logs_dir);
    defer test_io.deleteTree(logs_dir) catch {};

    var handle = try ProcessHandle.spawn(
        allocator,
        "broker_ready_no_aeron_test",
        "/bin/echo",
        &.{"INFO: broker started successfully"},
        logs_dir,
    );
    defer handle.deinit();

    // When / Then
    try waitForBrokerReady(&handle, 5000);
    try std.testing.expectEqual(ProcessState.ready, handle.state);
}

test "waitForServiceReady detects service registered marker" {
    // Given
    const allocator = std.testing.allocator;
    const logs_dir = "/tmp/ringloom-test-readiness-service";
    try test_io.createDirPath(logs_dir);
    defer test_io.deleteTree(logs_dir) catch {};

    var handle = try ProcessHandle.spawn(
        allocator,
        "service_ready_test",
        "/bin/echo",
        &.{"service registered with broker"},
        logs_dir,
    );
    defer handle.deinit();

    // When / Then
    try waitForServiceReady(&handle, 5000);
    try std.testing.expectEqual(ProcessState.ready, handle.state);
}

test "default_poll_interval_ms is 50" {
    // Given / When / Then
    try std.testing.expectEqual(@as(u64, 50), default_poll_interval_ms);
}
