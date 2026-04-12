//! Top-level test harness for BRZ end-to-end tests and benchmarks.
//!
//! `TestHarness` ties together the temporary environment, configuration
//! generator, process runner, and readiness detection modules into a
//! single entry point that orchestrates multi-process test scenarios.
//!
//! ## Typical usage
//!
//! ```zig
//! var h = try TestHarness.init(allocator, "my_scenario");
//! defer h.cleanup();
//!
//! const broker = try h.startBroker(.{});
//! try h.waitForBrokerReady(broker, 10_000);
//!
//! const svc = try h.startService(.{
//!     .executable_name = "my-service",
//!     .service_name = "my-service",
//! });
//! try h.waitForServiceReady(svc, 10_000);
//!
//! // … run test assertions …
//!
//! try h.stopProcess(svc);
//! try h.stopProcess(broker);
//! ```
//!
//! On failure, call `h.markFailed()` before `cleanup()` to preserve the
//! temporary directory tree for post-mortem inspection.

const std = @import("std");
const fs = std.fs;
const mem = std.mem;
const Allocator = std.mem.Allocator;

const temp_env = @import("temp_env.zig");
const TempEnv = temp_env.TempEnv;

const config_gen = @import("config_gen.zig");
const ConfigGen = config_gen.ConfigGen;

const process_runner = @import("process_runner.zig");
const ProcessHandle = process_runner.ProcessHandle;
const ProcessState = process_runner.ProcessState;

const readiness = @import("readiness.zig");
const log_capture = @import("log_capture.zig");

// ── Spec types ───────────────────────────────────────────────────────

/// Describes a peer broker endpoint (used in `BrokerSpec.peers`).
pub const PeerSpec = struct {
    node_id: u8,
    host: []const u8,
    port: u16,
};

/// Describes the configuration for a broker process to be started by
/// the harness.  All fields have sensible defaults for single-node
/// localhost testing.
pub const BrokerSpec = struct {
    node_id: u8 = 1,
    host: []const u8 = "127.0.0.1",
    port: u16 = 19001,
    peers: []const PeerSpec = &.{},
    group_name: []const u8 = "brz-test",
    threading_mode: []const u8 = "dedicated",
    idle_strategy: []const u8 = "backoff",
    control_buffer_size: u32 = 65_536,
    messages_buffer_size: u32 = 1_048_576,
    sender_cpu_affinity: ?u32 = null,
    receiver_cpu_affinity: ?u32 = null,
};

/// Describes the configuration for a service process to be started by
/// the harness.
pub const ServiceSpec = struct {
    executable_name: []const u8,
    service_name: []const u8,
    broker_node_id: u8 = 1,
    group_name: []const u8 = "brz-test",
    leader_election_enabled: bool = false,
    extra_args: []const []const u8 = &.{},
};

// ── TestHarness ──────────────────────────────────────────────────────

/// Orchestrates multi-process BRZ test scenarios.
///
/// Manages a temporary environment, generates configuration files,
/// spawns broker and service processes, waits for readiness, and
/// performs cleanup (or preservation on failure).
pub const TestHarness = struct {
    allocator: Allocator,
    env: TempEnv,
    config_gen_inst: ConfigGen,
    processes: std.ArrayList(*ProcessHandle),
    failed: bool,
    bin_dir: []const u8,

    /// Creates a new harness for the scenario identified by `test_name`.
    ///
    /// Sets up a `TempEnv`, a `ConfigGen`, and an empty process list.
    /// The `bin_dir` defaults to `"zig-out/bin"` — the standard install
    /// location for `zig build`.
    pub fn init(allocator: Allocator, test_name: []const u8) !TestHarness {
        var env = try TempEnv.init(allocator, test_name);
        errdefer env.deinit();

        const bin_dir = try allocator.dupe(u8, "zig-out/bin");
        errdefer allocator.free(bin_dir);

        return TestHarness{
            .allocator = allocator,
            .env = env,
            .config_gen_inst = ConfigGen.init(allocator),
            .processes = .empty,
            .failed = false,
            .bin_dir = bin_dir,
        };
    }

    /// Releases all resources owned by the harness.
    ///
    /// Any processes still running are killed.  If the harness has been
    /// marked as failed the temporary directory is preserved on disk;
    /// otherwise it is recursively deleted.
    pub fn deinit(self: *TestHarness) void {
        self.killAllProcesses();
        self.freeProcessHandles();
        self.processes.deinit(self.allocator);

        if (self.failed) self.env.preserve();
        self.env.deinit();

        self.allocator.free(self.bin_dir);
    }

    // ── Broker lifecycle ─────────────────────────────────────────

    /// Generates a broker configuration file and spawns the broker
    /// process.
    ///
    /// Returns a handle that can be passed to `waitForBrokerReady`,
    /// `stopProcess`, etc.
    pub fn startBroker(self: *TestHarness, spec: BrokerSpec) !*ProcessHandle {
        // 1. Generate the config file.
        const config_path = try self.config_gen_inst.writeBrokerConfig(
            self.env.config_path,
            spec,
            self.env.storage_path,
        );
        errdefer self.allocator.free(config_path);

        // 2. Build the executable path.
        const exe_path = try std.fmt.allocPrint(self.allocator, "{s}/brz-broker", .{self.bin_dir});
        defer self.allocator.free(exe_path);

        // 3. Build the process name.
        const name = try std.fmt.allocPrint(self.allocator, "broker_{d}", .{spec.node_id});
        defer self.allocator.free(name);

        // 4. Spawn.
        const handle = try self.allocator.create(ProcessHandle);
        errdefer self.allocator.destroy(handle);

        handle.* = try ProcessHandle.spawn(
            self.allocator,
            name,
            exe_path,
            &.{ "--config", config_path },
            self.env.logs_path,
        );

        try handle.setConfigPath(config_path);
        self.allocator.free(config_path);

        try self.processes.append(self.allocator, handle);

        return handle;
    }

    // ── Service lifecycle ────────────────────────────────────────

    /// Spawns a service process.
    ///
    /// The executable is located at `<bin_dir>/<spec.executable_name>`.
    /// Standard arguments (`--storage-path`, `--group`, `--service-name`)
    /// are passed automatically.  Additional arguments from
    /// `spec.extra_args` are appended.
    pub fn startService(self: *TestHarness, spec: ServiceSpec) !*ProcessHandle {
        // 1. Build the executable path.
        const exe_path = try std.fmt.allocPrint(
            self.allocator,
            "{s}/{s}",
            .{ self.bin_dir, spec.executable_name },
        );
        defer self.allocator.free(exe_path);

        // 2. Build argv: base args + extra_args.
        var args: std.ArrayList([]const u8) = .empty;
        defer args.deinit(self.allocator);

        // Format broker_node_id as a string argument.
        var node_id_buf: [8]u8 = undefined;
        const node_id_str = std.fmt.bufPrint(&node_id_buf, "{d}", .{spec.broker_node_id}) catch unreachable;

        try args.appendSlice(self.allocator, &.{
            "--storage-path",   self.env.storage_path,
            "--group",          spec.group_name,
            "--service-name",   spec.service_name,
            "--broker-node-id", node_id_str,
        });

        for (spec.extra_args) |extra| {
            try args.append(self.allocator, extra);
        }

        // 3. Spawn.
        const handle = try self.allocator.create(ProcessHandle);
        errdefer self.allocator.destroy(handle);

        handle.* = try ProcessHandle.spawn(
            self.allocator,
            spec.service_name,
            exe_path,
            args.items,
            self.env.logs_path,
        );

        try self.processes.append(self.allocator, handle);

        return handle;
    }

    // ── Readiness ────────────────────────────────────────────────

    /// Waits for the broker process to report readiness.
    pub fn waitForBrokerReady(self: *TestHarness, handle: *ProcessHandle, timeout_ms: u64) !void {
        _ = self;
        try readiness.waitForBrokerReady(handle, timeout_ms);
    }

    /// Waits for a service process to report readiness.
    pub fn waitForServiceReady(self: *TestHarness, handle: *ProcessHandle, timeout_ms: u64) !void {
        _ = self;
        try readiness.waitForServiceReady(handle, timeout_ms);
    }

    /// Waits for a specific log line to appear in a process's stdout.
    pub fn waitForLogLine(self: *TestHarness, handle: *ProcessHandle, needle: []const u8, timeout_ms: u64) !void {
        _ = self;
        try readiness.waitForLogLine(handle, needle, timeout_ms);
    }

    // ── Process management ───────────────────────────────────────

    /// Sends SIGTERM to the process and waits up to 5 seconds for a
    /// graceful exit.  Falls back to SIGKILL if the timeout elapses.
    pub fn stopProcess(self: *TestHarness, handle: *ProcessHandle) !void {
        _ = self;
        try handle.stop();
        _ = handle.waitForExit(5000) catch {
            handle.kill();
            return;
        };
    }

    /// Sends SIGKILL for immediate termination.
    pub fn killProcess(self: *TestHarness, handle: *ProcessHandle) void {
        _ = self;
        handle.kill();
    }

    // ── Failure handling ─────────────────────────────────────────

    /// Marks the scenario as failed.  The temporary directory will be
    /// preserved on cleanup and failure diagnostics will be printed to
    /// stderr.
    pub fn markFailed(self: *TestHarness) void {
        self.failed = true;
        self.env.preserve();
    }

    /// Dumps failure diagnostics to stderr for all managed processes.
    pub fn dumpDiagnostics(self: *TestHarness, scenario_name: []const u8) void {
        // Build a slice of *ProcessHandle from the ArrayList.
        log_capture.dumpFailureDiagnostics(
            self.allocator,
            scenario_name,
            self.processes.items,
            &self.env,
        );
    }

    // ── Full cleanup ─────────────────────────────────────────────

    /// Stops all running processes and cleans up the harness.
    ///
    /// If the harness is marked as failed the temporary directory is
    /// preserved and diagnostics are printed.  This is the recommended
    /// way to tear down a scenario (typically via `defer h.cleanup()`).
    pub fn cleanup(self: *TestHarness) void {
        if (self.failed) {
            self.dumpDiagnostics("cleanup");
        }
        self.deinit();
    }

    // ── Accessors ────────────────────────────────────────────────

    /// Returns a mutable reference to the temporary environment.
    pub fn getEnv(self: *TestHarness) *TempEnv {
        return &self.env;
    }

    /// Returns the number of managed processes (running or exited).
    pub fn processCount(self: *const TestHarness) usize {
        return self.processes.items.len;
    }

    /// Overrides the binary directory (e.g. for cross-compilation or
    /// custom build paths).
    pub fn setBinDir(self: *TestHarness, path: []const u8) !void {
        self.allocator.free(self.bin_dir);
        self.bin_dir = try self.allocator.dupe(u8, path);
    }

    // ── Internal helpers ─────────────────────────────────────────

    fn killAllProcesses(self: *TestHarness) void {
        for (self.processes.items) |handle| {
            if (handle.state != .exited and handle.state != .created) {
                handle.stop() catch {};
                _ = handle.waitForExit(2000) catch {
                    handle.kill();
                    return;
                };
            }
        }
    }

    fn freeProcessHandles(self: *TestHarness) void {
        for (self.processes.items) |handle| {
            handle.deinit();
            self.allocator.destroy(handle);
        }
    }
};

// ── Tests ────────────────────────────────────────────────────────────

test "BrokerSpec has sensible defaults" {
    // Given
    const spec = BrokerSpec{};

    // Then
    try std.testing.expectEqual(@as(u8, 1), spec.node_id);
    try std.testing.expectEqualStrings("127.0.0.1", spec.host);
    try std.testing.expectEqual(@as(u16, 19001), spec.port);
    try std.testing.expectEqual(@as(usize, 0), spec.peers.len);
    try std.testing.expectEqualStrings("brz-test", spec.group_name);
    try std.testing.expectEqualStrings("dedicated", spec.threading_mode);
    try std.testing.expectEqualStrings("backoff", spec.idle_strategy);
    try std.testing.expectEqual(@as(u32, 65_536), spec.control_buffer_size);
    try std.testing.expectEqual(@as(u32, 1_048_576), spec.messages_buffer_size);
}

test "ServiceSpec has sensible defaults" {
    // Given
    const spec = ServiceSpec{
        .executable_name = "my-svc",
        .service_name = "my-service",
    };

    // Then
    try std.testing.expectEqualStrings("my-svc", spec.executable_name);
    try std.testing.expectEqualStrings("my-service", spec.service_name);
    try std.testing.expectEqual(@as(u8, 1), spec.broker_node_id);
    try std.testing.expectEqualStrings("brz-test", spec.group_name);
    try std.testing.expect(!spec.leader_election_enabled);
    try std.testing.expectEqual(@as(usize, 0), spec.extra_args.len);
}

test "BrokerSpec with peers" {
    // Given
    const peers = [_]PeerSpec{
        .{ .node_id = 2, .host = "10.0.0.2", .port = 19002 },
        .{ .node_id = 3, .host = "10.0.0.3", .port = 19003 },
    };

    // When
    const spec = BrokerSpec{
        .node_id = 1,
        .peers = &peers,
    };

    // Then
    try std.testing.expectEqual(@as(usize, 2), spec.peers.len);
    try std.testing.expectEqual(@as(u8, 2), spec.peers[0].node_id);
    try std.testing.expectEqualStrings("10.0.0.2", spec.peers[0].host);
    try std.testing.expectEqual(@as(u16, 19002), spec.peers[0].port);
}

test "PeerSpec stores correct values" {
    // Given / When
    const peer = PeerSpec{
        .node_id = 5,
        .host = "192.168.1.100",
        .port = 20000,
    };

    // Then
    try std.testing.expectEqual(@as(u8, 5), peer.node_id);
    try std.testing.expectEqualStrings("192.168.1.100", peer.host);
    try std.testing.expectEqual(@as(u16, 20000), peer.port);
}

test "TestHarness init and deinit without processes" {
    // Given
    const allocator = std.testing.allocator;

    // When
    var h = try TestHarness.init(allocator, "harness_init_test");

    // Then — basic properties are set.
    try std.testing.expectEqualStrings("zig-out/bin", h.bin_dir);
    try std.testing.expectEqual(@as(usize, 0), h.processCount());
    try std.testing.expect(!h.failed);

    // Cleanup
    h.deinit();
}

test "TestHarness cleanup is idempotent" {
    // Given
    const allocator = std.testing.allocator;

    var h = try TestHarness.init(allocator, "cleanup_idempotent");

    // When — cleanup should not panic or double-free.
    h.cleanup();
}

test "TestHarness markFailed preserves environment" {
    // Given
    const allocator = std.testing.allocator;

    var h = try TestHarness.init(allocator, "mark_failed_test");
    const base_copy = try allocator.dupe(u8, h.env.base_path);
    defer allocator.free(base_copy);

    // When
    h.markFailed();
    try std.testing.expect(h.failed);

    h.deinit();

    // Then — the directory should still exist because we marked failed.
    var dir = try fs.cwd().openDir(base_copy, .{});
    dir.close();

    // Manual cleanup.
    try fs.cwd().deleteTree(base_copy);
}

test "TestHarness setBinDir overrides default" {
    // Given
    const allocator = std.testing.allocator;

    var h = try TestHarness.init(allocator, "set_bin_dir_test");
    defer h.cleanup();

    // When
    try h.setBinDir("/custom/bin/path");

    // Then
    try std.testing.expectEqualStrings("/custom/bin/path", h.bin_dir);
}

test "TestHarness getEnv returns mutable reference" {
    // Given
    const allocator = std.testing.allocator;

    var h = try TestHarness.init(allocator, "get_env_test");
    defer h.cleanup();

    // When
    const env = h.getEnv();

    // Then — should be the same TempEnv.
    try std.testing.expectEqualStrings(h.env.base_path, env.base_path);
}

test "ServiceSpec with extra args" {
    // Given
    const extras = [_][]const u8{ "--verbose", "--port", "8080" };

    // When
    const spec = ServiceSpec{
        .executable_name = "extra-svc",
        .service_name = "extra-service",
        .extra_args = &extras,
    };

    // Then
    try std.testing.expectEqual(@as(usize, 3), spec.extra_args.len);
    try std.testing.expectEqualStrings("--verbose", spec.extra_args[0]);
    try std.testing.expectEqualStrings("--port", spec.extra_args[1]);
    try std.testing.expectEqualStrings("8080", spec.extra_args[2]);
}

test "ServiceSpec with leader election enabled" {
    // Given / When
    const spec = ServiceSpec{
        .executable_name = "le-svc",
        .service_name = "leader-service",
        .leader_election_enabled = true,
    };

    // Then
    try std.testing.expect(spec.leader_election_enabled);
}

test "BrokerSpec with custom buffer sizes" {
    // Given / When
    const spec = BrokerSpec{
        .control_buffer_size = 131_072,
        .messages_buffer_size = 4_194_304,
    };

    // Then
    try std.testing.expectEqual(@as(u32, 131_072), spec.control_buffer_size);
    try std.testing.expectEqual(@as(u32, 4_194_304), spec.messages_buffer_size);
}
