//! Temporary environment manager for RingLoom end-to-end tests.
//!
//! `TempEnv` creates an isolated directory tree under `/tmp` for each test
//! scenario, ensuring that broker metadata files, log output, generated
//! configs, benchmark results, and other artefacts live in a unique,
//! inspectable location that is cleaned up automatically on success.
//!
//! Directory layout created by `init`:
//! ```
//! /tmp/ringloom-e2e-<test_name>-<pid>/
//! ├── storage/          # shared-memory metadata (replaces /dev/shm)
//! │   └── <group>/
//! │       └── services/
//! ├── logs/             # stdout / stderr captures from child processes
//! ├── config/           # generated .properties files
//! ├── results/          # JSON result files (perf, correctness)
//! └── artifacts/        # arbitrary test artefacts
//! ```

const std = @import("std");
const builtin = @import("builtin");
const Allocator = std.mem.Allocator;
const test_io = @import("io.zig");

/// Returns the current process ID portably.
fn getCurrentPid() i64 {
    if (comptime builtin.os.tag == .linux) {
        return @intCast(std.os.linux.getpid());
    } else {
        const c = struct {
            extern "c" fn getpid() c_int;
        };
        return @intCast(c.getpid());
    }
}

/// Manages an isolated temporary workspace for a single test scenario.
pub const TempEnv = struct {
    allocator: Allocator,
    base_path: []const u8,
    storage_path: []const u8,
    logs_path: []const u8,
    config_path: []const u8,
    results_path: []const u8,
    artifacts_path: []const u8,
    preserved: bool,

    /// Creates a new temporary environment rooted at
    /// `/tmp/ringloom-e2e-<test_name>-<pid>/` with all standard subdirectories.
    ///
    /// The `test_name` is sanitised (slashes replaced with underscores) so
    /// that it is safe for use as a directory component.
    pub fn init(allocator: Allocator, test_name: []const u8) !TempEnv {
        const pid = getCurrentPid();

        // Sanitise test_name: replace '/' with '_' so it can be a dir name.
        const safe_name = try allocator.alloc(u8, test_name.len);
        defer allocator.free(safe_name);
        for (test_name, 0..) |c, i| {
            safe_name[i] = if (c == '/' or c == '\\' or c == ' ') '_' else c;
        }

        const base_path = try std.fmt.allocPrint(allocator, "/tmp/ringloom-e2e-{s}-{d}", .{ safe_name, pid });
        errdefer allocator.free(base_path);

        const storage_path = try std.fmt.allocPrint(allocator, "{s}/storage", .{base_path});
        errdefer allocator.free(storage_path);

        const logs_path = try std.fmt.allocPrint(allocator, "{s}/logs", .{base_path});
        errdefer allocator.free(logs_path);

        const config_path = try std.fmt.allocPrint(allocator, "{s}/config", .{base_path});
        errdefer allocator.free(config_path);

        const results_path = try std.fmt.allocPrint(allocator, "{s}/results", .{base_path});
        errdefer allocator.free(results_path);

        const artifacts_path = try std.fmt.allocPrint(allocator, "{s}/artifacts", .{base_path});
        errdefer allocator.free(artifacts_path);

        // Create the full directory tree.  `makePath` creates intermediate
        // directories, so we can jump straight to the deepest leaves.
        // Create the default group services directory under storage.
        const default_services = try std.fmt.allocPrint(allocator, "{s}/ringloom-test/services", .{storage_path});
        defer allocator.free(default_services);

        try test_io.createDirPath(default_services);
        try test_io.createDirPath(logs_path);
        try test_io.createDirPath(config_path);
        try test_io.createDirPath(results_path);
        try test_io.createDirPath(artifacts_path);

        return TempEnv{
            .allocator = allocator,
            .base_path = base_path,
            .storage_path = storage_path,
            .logs_path = logs_path,
            .config_path = config_path,
            .results_path = results_path,
            .artifacts_path = artifacts_path,
            .preserved = false,
        };
    }

    /// Recursively deletes the temporary tree and frees all owned strings.
    ///
    /// If `preserve()` was called beforehand the directory is kept on disk
    /// (useful when a test fails and the operator wants to inspect logs).
    pub fn deinit(self: *TempEnv) void {
        if (!self.preserved) {
            test_io.deleteTree(self.base_path) catch |err| {
                std.log.warn("TempEnv: failed to delete {s}: {}", .{ self.base_path, err });
            };
        }

        self.allocator.free(self.artifacts_path);
        self.allocator.free(self.results_path);
        self.allocator.free(self.config_path);
        self.allocator.free(self.logs_path);
        self.allocator.free(self.storage_path);
        self.allocator.free(self.base_path);
    }

    /// Marks the environment for preservation — `deinit` will free memory
    /// but will **not** delete the directory tree from disk.
    pub fn preserve(self: *TempEnv) void {
        self.preserved = true;
    }

    /// Returns `<storage_path>/<group>`, creating the directory (and a
    /// `services/` child) if it does not already exist.  The returned
    /// slice is heap-allocated and owned by the caller.
    pub fn groupStoragePath(self: *const TempEnv, group: []const u8) ![]const u8 {
        const group_path = try std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ self.storage_path, group });
        errdefer self.allocator.free(group_path);

        const services_path = try std.fmt.allocPrint(self.allocator, "{s}/services", .{group_path});
        defer self.allocator.free(services_path);

        try test_io.createDirPath(services_path);

        return group_path;
    }

    /// Returns `<logs_path>/<filename>`, heap-allocated and caller-owned.
    pub fn logFilePath(self: *const TempEnv, filename: []const u8) ![]const u8 {
        return std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ self.logs_path, filename });
    }

    /// Returns `<config_path>/<filename>`, heap-allocated and caller-owned.
    pub fn configFilePath(self: *const TempEnv, filename: []const u8) ![]const u8 {
        return std.fmt.allocPrint(self.allocator, "{s}/{s}", .{ self.config_path, filename });
    }
};

// ── Tests ────────────────────────────────────────────────────────────

test "init creates expected directory structure" {
    // Given
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    // When
    var env = try TempEnv.init(allocator, "dir_structure_test");
    defer env.deinit();

    // Then — every subdirectory must exist.
    var storage_dir = try test_io.openDir(env.storage_path, .{});
    storage_dir.close(test_io.io());

    var logs_dir = try test_io.openDir(env.logs_path, .{});
    logs_dir.close(test_io.io());

    var config_dir = try test_io.openDir(env.config_path, .{});
    config_dir.close(test_io.io());

    var results_dir = try test_io.openDir(env.results_path, .{});
    results_dir.close(test_io.io());

    var artifacts_dir = try test_io.openDir(env.artifacts_path, .{});
    artifacts_dir.close(test_io.io());

    // The default group services directory must also exist.
    const services = try std.fmt.allocPrint(allocator, "{s}/ringloom-test/services", .{env.storage_path});
    var services_dir = try test_io.openDir(services, .{});
    services_dir.close(test_io.io());
}

test "preserve prevents deletion on deinit" {
    // Given
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var env = try TempEnv.init(allocator, "preserve_test");
    const base_copy = try allocator.dupe(u8, env.base_path);

    // When
    env.preserve();
    env.deinit();

    // Then — directory should still exist on disk.
    var dir = try test_io.openDir(base_copy, .{});
    dir.close(test_io.io());

    // Cleanup: remove the preserved directory manually.
    try test_io.deleteTree(base_copy);
}

test "groupStoragePath creates group directory on demand" {
    // Given
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var env = try TempEnv.init(allocator, "group_path_test");
    defer env.deinit();

    // When
    const path = try env.groupStoragePath("my-custom-group");

    // Then
    const services_path = try std.fmt.allocPrint(allocator, "{s}/services", .{path});
    var dir = try test_io.openDir(services_path, .{});
    dir.close(test_io.io());
}

test "deinit removes directory tree" {
    // Given
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    var env = try TempEnv.init(allocator, "cleanup_test");
    const base_copy = try allocator.dupe(u8, env.base_path);

    // When
    env.deinit();

    // Then — the base directory should no longer exist.
    const result = test_io.openDir(base_copy, .{});
    try std.testing.expectError(error.FileNotFound, result);
}

test "test name with slashes is sanitised" {
    // Given
    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    // When
    var env = try TempEnv.init(allocator, "some/nested/test name");
    defer env.deinit();

    // Then — base_path should not contain slashes within the test name part.
    // (The only slashes should be the path separators for /tmp/ringloom-e2e-…)
    try std.testing.expect(std.mem.indexOf(u8, env.base_path, "some_nested_test_name") != null);
}
