//! Process runner for BRZ end-to-end tests.
//!
//! `ProcessHandle` manages the lifecycle of a child process spawned by
//! the test harness: starting, liveness checks, graceful stop (SIGTERM),
//! forceful kill (SIGKILL), and exit-code collection.
//!
//! Stdout and stderr are redirected to log files under the harness's
//! `logs/` directory so that failure diagnostics can inspect them after
//! the fact.  A secondary in-memory `LogCapture` buffer is fed from the
//! stdout pipe so that the readiness module can poll for marker lines
//! without touching the filesystem.
//!
//! ## Design decisions
//!
//! *   We use `std.process.Child` with `.Pipe` for stdout so that the
//!     readiness module can do non-blocking reads via `poll(2)`.  Stderr
//!     also uses `.Pipe` and is mirrored to a log file.
//! *   The `readAvailableOutput` helper performs a **non-blocking** read
//!     from the stdout pipe and appends any bytes it finds to
//!     `stdout_capture`.  It is safe to call in a tight polling loop.

const std = @import("std");
const builtin = @import("builtin");
const fs = std.fs;
const mem = std.mem;
const posix = std.posix;
const Allocator = std.mem.Allocator;

const log_capture = @import("log_capture.zig");
const LogCapture = log_capture.LogCapture;

/// The lifecycle state of a managed child process.
pub const ProcessState = enum {
    /// Handle has been constructed but the child has not been spawned.
    created,
    /// The child process is running.
    spawned,
    /// A readiness condition has been detected (e.g. log marker found).
    ready,
    /// A graceful stop has been requested (SIGTERM sent).
    stopping,
    /// The child process has exited (exit code available).
    exited,
};

/// Manages a single child process spawned by the test harness.
pub const ProcessHandle = struct {
    name: []const u8,
    exe_path: []const u8,
    child: ?std.process.Child,
    stdout_path: []const u8,
    stderr_path: []const u8,
    config_path: ?[]const u8,
    state: ProcessState,
    exit_code: ?u32,
    allocator: Allocator,

    /// In-memory capture of stdout for readiness polling.
    stdout_capture: LogCapture,

    /// File handle used to persist stdout content alongside the pipe.
    stdout_file: ?fs.File,
    /// File handle for stderr persistence.
    stderr_file: ?fs.File,

    // ── Construction / spawning ──────────────────────────────────

    /// Spawns a child process and returns a handle for managing it.
    ///
    /// Stdout is captured via a pipe **and** mirrored to
    /// `<logs_dir>/<name>_stdout.log`.  Stderr is captured via pipe
    /// and mirrored to `<logs_dir>/<name>_stderr.log`.
    pub fn spawn(
        allocator: Allocator,
        name: []const u8,
        exe_path: []const u8,
        args: []const []const u8,
        logs_dir: []const u8,
    ) !ProcessHandle {
        const owned_name = try allocator.dupe(u8, name);
        errdefer allocator.free(owned_name);

        const owned_exe = try allocator.dupe(u8, exe_path);
        errdefer allocator.free(owned_exe);

        const stdout_path = try std.fmt.allocPrint(allocator, "{s}/{s}_stdout.log", .{ logs_dir, name });
        errdefer allocator.free(stdout_path);

        const stderr_path = try std.fmt.allocPrint(allocator, "{s}/{s}_stderr.log", .{ logs_dir, name });
        errdefer allocator.free(stderr_path);

        // Open log files for mirroring pipe output.
        const stdout_file = try fs.cwd().createFile(stdout_path, .{ .truncate = true });
        errdefer stdout_file.close();

        const stderr_file = try fs.cwd().createFile(stderr_path, .{ .truncate = true });
        errdefer stderr_file.close();

        // Build the argv array: [exe_path] ++ args.
        var argv: std.ArrayList([]const u8) = .empty;
        defer argv.deinit(allocator);
        try argv.append(allocator, exe_path);
        for (args) |arg| {
            try argv.append(allocator, arg);
        }

        var child = std.process.Child.init(argv.items, allocator);
        child.stdout_behavior = .Pipe;
        child.stderr_behavior = .Pipe;

        try child.spawn();

        // Set stdout pipe to non-blocking so readAvailableOutput never hangs.
        if (child.stdout) |stdout| {
            setNonBlocking(stdout.handle) catch |err| {
                std.log.warn("ProcessHandle: failed to set stdout non-blocking for {s}: {}", .{ name, err });
            };
        }

        // Set stderr pipe to non-blocking too.
        if (child.stderr) |stderr| {
            setNonBlocking(stderr.handle) catch |err| {
                std.log.warn("ProcessHandle: failed to set stderr non-blocking for {s}: {}", .{ name, err });
            };
        }

        return ProcessHandle{
            .name = owned_name,
            .exe_path = owned_exe,
            .child = child,
            .stdout_path = stdout_path,
            .stderr_path = stderr_path,
            .config_path = null,
            .state = .spawned,
            .exit_code = null,
            .allocator = allocator,
            .stdout_capture = LogCapture.init(allocator),
            .stdout_file = stdout_file,
            .stderr_file = stderr_file,
        };
    }

    // ── Queries ──────────────────────────────────────────────────

    /// Returns `true` if the child process is believed to still be running.
    pub fn isAlive(self: *ProcessHandle) bool {
        return switch (self.state) {
            .spawned, .ready, .stopping => self.checkChildAlive(),
            .created, .exited => false,
        };
    }

    // ── Output reading ───────────────────────────────────────────

    /// Performs a non-blocking read from the child's stdout pipe,
    /// appending any available bytes to `stdout_capture` and mirroring
    /// them to the stdout log file.
    ///
    /// Returns the number of bytes read (0 if nothing was available).
    pub fn readAvailableOutput(self: *ProcessHandle) !usize {
        const child = self.child orelse return 0;
        const stdout = child.stdout orelse return 0;

        var buf: [8192]u8 = undefined;
        const n = stdout.read(&buf) catch |err| switch (err) {
            error.WouldBlock => return 0,
            else => return err,
        };

        if (n == 0) return 0;

        const data = buf[0..n];
        try self.stdout_capture.append(data);

        // Mirror to the log file.
        if (self.stdout_file) |f| {
            f.writeAll(data) catch {};
        }

        return n;
    }

    /// Performs a non-blocking read from the child's stderr pipe,
    /// appending to `stdout_capture` (so waitForLogLine can match
    /// log output) and mirroring to the stderr log file.
    ///
    /// Returns the number of bytes read (0 if nothing was available).
    pub fn readAvailableStderr(self: *ProcessHandle) !usize {
        const child = self.child orelse return 0;
        const stderr = child.stderr orelse return 0;

        var buf: [8192]u8 = undefined;
        const n = stderr.read(&buf) catch |err| switch (err) {
            error.WouldBlock => return 0,
            else => return err,
        };

        if (n == 0) return 0;

        const data = buf[0..n];

        // Append to the combined capture so waitForLogLine can match
        // log output (Zig's std.log writes to stderr).
        try self.stdout_capture.append(data);

        // Mirror to the stderr log file.
        if (self.stderr_file) |f| {
            f.writeAll(data) catch {};
        }

        return n;
    }

    /// Reads all available output in a loop until no more bytes are
    /// immediately available, then returns the total bytes read.
    pub fn drainAvailableOutput(self: *ProcessHandle) !usize {
        var total: usize = 0;
        while (true) {
            const n_out = try self.readAvailableOutput();
            const n_err = self.readAvailableStderr() catch 0;
            if (n_out == 0 and n_err == 0) break;
            total += n_out + n_err;
        }
        return total;
    }

    // ── Lifecycle management ─────────────────────────────────────

    /// Sends SIGTERM and transitions to the `stopping` state.
    ///
    /// Does nothing if the process has already exited.
    pub fn stop(self: *ProcessHandle) !void {
        if (self.state == .exited or self.state == .created) return;

        if (self.child) |child| {
            const pid = child.id;
            // Send SIGTERM.
            posix.kill(pid, std.posix.SIG.TERM) catch |err| switch (err) {
                error.ProcessNotFound => {
                    self.state = .exited;
                    return;
                },
                else => return err,
            };
            self.state = .stopping;
        }
    }

    /// Sends SIGKILL (forceful, immediate termination).
    ///
    /// Does nothing if the process has already exited.
    pub fn kill(self: *ProcessHandle) void {
        if (self.state == .exited or self.state == .created) return;

        if (self.child) |child| {
            const pid = child.id;
            posix.kill(pid, std.posix.SIG.KILL) catch {};
            self.state = .exited;
        }
    }

    /// Waits for the process to exit, polling every 50 ms up to
    /// `timeout_ms` milliseconds.
    ///
    /// Returns the exit code on success.  Returns `error.Timeout` if
    /// the child did not exit within the specified duration.
    pub fn waitForExit(self: *ProcessHandle, timeout_ms: u64) !u32 {
        const deadline_ns: i128 = std.time.nanoTimestamp() + @as(i128, timeout_ms) * std.time.ns_per_ms;

        while (std.time.nanoTimestamp() < deadline_ns) {
            // Drain any pending stdout so logs are up-to-date.
            _ = self.drainAvailableOutput() catch {};

            if (!self.checkChildAlive()) {
                return self.collectExitCode();
            }

            std.Thread.sleep(50 * std.time.ns_per_ms);
        }

        return error.Timeout;
    }

    /// Marks the process as `ready` (called by the readiness module
    /// after a marker line has been detected).
    pub fn markReady(self: *ProcessHandle) void {
        if (self.state == .spawned) {
            self.state = .ready;
        }
    }

    /// Associates a config file path with this handle (informational).
    pub fn setConfigPath(self: *ProcessHandle, path: []const u8) !void {
        if (self.config_path) |old| {
            self.allocator.free(old);
        }
        self.config_path = try self.allocator.dupe(u8, path);
    }

    // ── Cleanup ──────────────────────────────────────────────────

    /// Releases all resources owned by this handle.
    ///
    /// If the child is still running it is killed first.
    pub fn deinit(self: *ProcessHandle) void {
        // Ensure the child is not left behind.
        if (self.state != .exited and self.state != .created) {
            self.kill();
            // Best-effort wait so the OS can reclaim the PID.
            if (self.child) |child| {
                _ = posix.waitpid(child.id, 0);
            }
        }

        if (self.stdout_file) |f| f.close();
        if (self.stderr_file) |f| f.close();

        self.stdout_capture.deinit();

        if (self.config_path) |cp| self.allocator.free(cp);
        self.allocator.free(self.stderr_path);
        self.allocator.free(self.stdout_path);
        self.allocator.free(self.exe_path);
        self.allocator.free(self.name);
    }

    // ── Internal helpers ─────────────────────────────────────────

    /// Checks whether the child process is still alive using a
    /// non-blocking `waitpid(WNOHANG)`.  This correctly detects zombie
    /// processes (which `kill(pid, 0)` does not — it returns success for
    /// zombies).  If the child has exited, the exit code is collected
    /// immediately and the state is updated.
    fn checkChildAlive(self: *ProcessHandle) bool {
        if (self.exit_code != null) return false;

        const child = self.child orelse return false;
        const pid = child.id;

        // WNOHANG: return immediately if the child has not exited.
        const result = posix.waitpid(pid, std.os.linux.W.NOHANG);

        if (result.pid != 0) {
            // Child has exited (or been signalled / stopped).
            const status: u32 = @bitCast(result.status);
            const code: u32 = if (std.os.linux.W.IFEXITED(status))
                @as(u32, std.os.linux.W.EXITSTATUS(status))
            else if (std.os.linux.W.IFSIGNALED(status))
                128 + std.os.linux.W.TERMSIG(status)
            else
                255;

            self.exit_code = code;
            self.state = .exited;
            return false;
        }

        // pid == 0 means the child is still running.
        return true;
    }

    /// Returns the exit code, collecting it via a blocking `waitpid` if
    /// it has not been gathered yet (should only be called when
    /// `checkChildAlive` has returned `false`).
    fn collectExitCode(self: *ProcessHandle) !u32 {
        if (self.exit_code) |code| return code;

        const child = self.child orelse return error.ProcessNotFound;
        const pid = child.id;

        // Blocking wait — the child should already be exited.
        const result = posix.waitpid(pid, 0);
        const status: u32 = @bitCast(result.status);
        const code: u32 = if (std.os.linux.W.IFEXITED(status))
            @as(u32, std.os.linux.W.EXITSTATUS(status))
        else if (std.os.linux.W.IFSIGNALED(status))
            128 + std.os.linux.W.TERMSIG(status)
        else
            255;

        self.exit_code = code;
        self.state = .exited;
        return code;
    }
};

// ── Utility ──────────────────────────────────────────────────────────

/// Sets the `O_NONBLOCK` flag on a file descriptor.
fn setNonBlocking(fd: posix.fd_t) !void {
    var fl_flags = posix.fcntl(fd, posix.F.GETFL, 0) catch |err| switch (err) {
        error.FileBusy => unreachable,
        error.Locked => unreachable,
        error.PermissionDenied => unreachable,
        error.DeadLock => unreachable,
        error.LockedRegionLimitExceeded => unreachable,
        else => |e| return e,
    };
    fl_flags |= 1 << @bitOffsetOf(posix.O, "NONBLOCK");
    _ = posix.fcntl(fd, posix.F.SETFL, fl_flags) catch |err| switch (err) {
        error.FileBusy => unreachable,
        error.Locked => unreachable,
        error.PermissionDenied => unreachable,
        error.DeadLock => unreachable,
        error.LockedRegionLimitExceeded => unreachable,
        else => |e| return e,
    };
}

// ── Tests ────────────────────────────────────────────────────────────

test "ProcessState has expected variants" {
    // Given / When / Then — just ensure the enum compiles and all
    // variants are accessible.
    try std.testing.expectEqual(ProcessState.created, ProcessState.created);
    try std.testing.expectEqual(ProcessState.spawned, ProcessState.spawned);
    try std.testing.expectEqual(ProcessState.ready, ProcessState.ready);
    try std.testing.expectEqual(ProcessState.stopping, ProcessState.stopping);
    try std.testing.expectEqual(ProcessState.exited, ProcessState.exited);
}

test "spawn and wait for a trivial child process" {
    // Given
    const allocator = std.testing.allocator;

    const logs_dir = "/tmp/brz-test-process-runner";
    try fs.cwd().makePath(logs_dir);
    defer fs.cwd().deleteTree(logs_dir) catch {};

    // When — spawn `echo hello` which exits immediately.
    var handle = try ProcessHandle.spawn(
        allocator,
        "echo_test",
        "/bin/echo",
        &.{"hello"},
        logs_dir,
    );
    defer handle.deinit();

    // Then — the process should exit quickly with code 0.
    const code = try handle.waitForExit(5000);
    try std.testing.expectEqual(@as(u32, 0), code);
    try std.testing.expectEqual(ProcessState.exited, handle.state);
}

test "stdout capture accumulates output from child" {
    // Given
    const allocator = std.testing.allocator;

    const logs_dir = "/tmp/brz-test-process-runner-capture";
    try fs.cwd().makePath(logs_dir);
    defer fs.cwd().deleteTree(logs_dir) catch {};

    // When — spawn a child that writes a known string to stdout.
    var handle = try ProcessHandle.spawn(
        allocator,
        "capture_test",
        "/bin/echo",
        &.{"brz-marker-ready"},
        logs_dir,
    );
    defer handle.deinit();

    _ = try handle.waitForExit(5000);

    // Drain remaining bytes from the pipe.
    _ = try handle.drainAvailableOutput();

    // Then — the captured output must contain our marker.
    try std.testing.expect(handle.stdout_capture.contains("brz-marker-ready"));
}

test "kill terminates a long-running child" {
    // Given
    const allocator = std.testing.allocator;

    const logs_dir = "/tmp/brz-test-process-runner-kill";
    try fs.cwd().makePath(logs_dir);
    defer fs.cwd().deleteTree(logs_dir) catch {};

    // Spawn `sleep 300` — will run for 5 minutes unless killed.
    var handle = try ProcessHandle.spawn(
        allocator,
        "kill_test",
        "/bin/sleep",
        &.{"300"},
        logs_dir,
    );
    defer handle.deinit();

    try std.testing.expect(handle.isAlive());

    // When
    handle.kill();

    // Then — after kill + short wait the process must be gone.
    std.Thread.sleep(100 * std.time.ns_per_ms);
    try std.testing.expect(!handle.isAlive());
}

test "stop sends SIGTERM to a running child" {
    // Given
    const allocator = std.testing.allocator;

    const logs_dir = "/tmp/brz-test-process-runner-stop";
    try fs.cwd().makePath(logs_dir);
    defer fs.cwd().deleteTree(logs_dir) catch {};

    var handle = try ProcessHandle.spawn(
        allocator,
        "stop_test",
        "/bin/sleep",
        &.{"300"},
        logs_dir,
    );
    defer handle.deinit();

    // When
    try handle.stop();

    // Then — state should transition to stopping or exited.
    try std.testing.expect(
        handle.state == .stopping or handle.state == .exited,
    );

    // Wait for actual exit.
    const code = try handle.waitForExit(5000);
    // SIGTERM = signal 15 → 128 + 15 = 143
    try std.testing.expectEqual(@as(u32, 143), code);
    try std.testing.expectEqual(ProcessState.exited, handle.state);
}

test "markReady transitions state from spawned to ready" {
    // Given
    const allocator = std.testing.allocator;

    const logs_dir = "/tmp/brz-test-process-runner-ready";
    try fs.cwd().makePath(logs_dir);
    defer fs.cwd().deleteTree(logs_dir) catch {};

    var handle = try ProcessHandle.spawn(
        allocator,
        "ready_test",
        "/bin/sleep",
        &.{"300"},
        logs_dir,
    );
    defer handle.deinit();

    try std.testing.expectEqual(ProcessState.spawned, handle.state);

    // When
    handle.markReady();

    // Then
    try std.testing.expectEqual(ProcessState.ready, handle.state);
}

test "setConfigPath stores and replaces config path" {
    // Given
    const allocator = std.testing.allocator;

    const logs_dir = "/tmp/brz-test-process-runner-cfg";
    try fs.cwd().makePath(logs_dir);
    defer fs.cwd().deleteTree(logs_dir) catch {};

    var handle = try ProcessHandle.spawn(
        allocator,
        "cfg_test",
        "/bin/echo",
        &.{"x"},
        logs_dir,
    );
    defer handle.deinit();

    // When
    try handle.setConfigPath("/some/config.properties");

    // Then
    try std.testing.expectEqualStrings("/some/config.properties", handle.config_path.?);

    // When — replace
    try handle.setConfigPath("/other/config.properties");

    // Then
    try std.testing.expectEqualStrings("/other/config.properties", handle.config_path.?);
}

test "waitForExit returns Timeout when child does not exit" {
    // Given
    const allocator = std.testing.allocator;

    const logs_dir = "/tmp/brz-test-process-runner-timeout";
    try fs.cwd().makePath(logs_dir);
    defer fs.cwd().deleteTree(logs_dir) catch {};

    var handle = try ProcessHandle.spawn(
        allocator,
        "timeout_test",
        "/bin/sleep",
        &.{"300"},
        logs_dir,
    );
    defer handle.deinit();

    // When — wait with a very short timeout.
    const result = handle.waitForExit(100);

    // Then
    try std.testing.expectError(error.Timeout, result);
}

test "stdout log file is written alongside pipe capture" {
    // Given
    const allocator = std.testing.allocator;

    const logs_dir = "/tmp/brz-test-process-runner-logfile";
    try fs.cwd().makePath(logs_dir);
    defer fs.cwd().deleteTree(logs_dir) catch {};

    var handle = try ProcessHandle.spawn(
        allocator,
        "logfile_test",
        "/bin/echo",
        &.{"log-output-check"},
        logs_dir,
    );
    defer handle.deinit();

    _ = try handle.waitForExit(5000);
    _ = try handle.drainAvailableOutput();

    // Flush and close the stdout log file so we can read it.
    if (handle.stdout_file) |f| {
        f.close();
        handle.stdout_file = null;
    }

    // Then — the log file should contain the echoed text.
    const file = try fs.cwd().openFile(handle.stdout_path, .{});
    defer file.close();

    var buf: [256]u8 = undefined;
    const n = try file.readAll(&buf);
    try std.testing.expect(mem.indexOf(u8, buf[0..n], "log-output-check") != null);
}
