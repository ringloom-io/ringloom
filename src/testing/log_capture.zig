//! Log capture and failure diagnostics for BRZ end-to-end tests.
//!
//! `LogCapture` accumulates raw bytes from a child process's stdout or
//! stderr pipe into a growable buffer and provides search, tail, and
//! file-dump operations used by the readiness module and the failure
//! diagnostics reporter.
//!
//! `dumpFailureDiagnostics` prints a human-readable summary to stderr
//! when a test scenario fails, including the last N lines of every
//! managed process and a listing of the storage directory.

const std = @import("std");
const fs = std.fs;
const mem = std.mem;
const Allocator = std.mem.Allocator;

const process_runner = @import("process_runner.zig");
const ProcessHandle = process_runner.ProcessHandle;
const ProcessState = process_runner.ProcessState;
const temp_env = @import("temp_env.zig");
const TempEnv = temp_env.TempEnv;

/// Accumulates raw byte output and provides search / tail operations.
pub const LogCapture = struct {
    allocator: Allocator,
    accumulated: std.ArrayList(u8),

    /// Creates an empty log capture buffer.
    pub fn init(allocator: Allocator) LogCapture {
        return .{
            .allocator = allocator,
            .accumulated = .empty,
        };
    }

    /// Creates an empty log capture buffer with pre-allocated capacity.
    pub fn initCapacity(allocator: Allocator, capacity: usize) !LogCapture {
        var list: std.ArrayList(u8) = .empty;
        try list.ensureTotalCapacity(allocator, capacity);
        return .{
            .allocator = allocator,
            .accumulated = list,
        };
    }

    /// Releases all memory owned by this capture buffer.
    pub fn deinit(self: *LogCapture) void {
        self.accumulated.deinit(self.allocator);
    }

    /// Appends raw bytes to the accumulated buffer.
    pub fn append(self: *LogCapture, data: []const u8) !void {
        try self.accumulated.appendSlice(self.allocator, data);
    }

    /// Returns `true` if the accumulated output contains `needle`.
    pub fn contains(self: *const LogCapture, needle: []const u8) bool {
        return mem.indexOf(u8, self.accumulated.items, needle) != null;
    }

    /// Returns the number of occurrences of `needle` in the accumulated output.
    pub fn countOccurrences(self: *const LogCapture, needle: []const u8) usize {
        if (needle.len == 0) return 0;

        var count: usize = 0;
        var offset: usize = 0;
        while (offset < self.accumulated.items.len) {
            if (mem.indexOfPos(u8, self.accumulated.items, offset, needle)) |pos| {
                count += 1;
                offset = pos + needle.len;
            } else {
                break;
            }
        }
        return count;
    }

    /// Returns a slice over the entire accumulated output (not owned by caller).
    pub fn getOutput(self: *const LogCapture) []const u8 {
        return self.accumulated.items;
    }

    /// Returns the total number of accumulated bytes.
    pub fn len(self: *const LogCapture) usize {
        return self.accumulated.items.len;
    }

    /// Returns the last `n` lines of the accumulated output as a slice
    /// into the internal buffer.  If fewer than `n` lines exist the
    /// entire buffer is returned.
    pub fn lastNLines(self: *const LogCapture, n: usize) []const u8 {
        if (n == 0 or self.accumulated.items.len == 0) return "";

        const data = self.accumulated.items;

        // Walk backwards counting newline characters.
        var lines_found: usize = 0;
        var pos: usize = data.len;

        // Skip a trailing newline so that it does not count as an empty
        // last line.
        if (pos > 0 and data[pos - 1] == '\n') {
            pos -= 1;
        }

        while (pos > 0) : (pos -= 1) {
            if (data[pos - 1] == '\n') {
                lines_found += 1;
                if (lines_found == n) {
                    return data[pos..];
                }
            }
        }

        // Fewer than n lines — return everything.
        return data;
    }

    /// Writes the accumulated output to a file at `path`, creating or
    /// truncating the file as necessary.
    pub fn writeToFile(self: *const LogCapture, path: []const u8) !void {
        const file = try fs.cwd().createFile(path, .{ .truncate = true });
        defer file.close();
        try file.writeAll(self.accumulated.items);
    }

    /// Clears all accumulated data, keeping allocated memory for reuse.
    pub fn reset(self: *LogCapture) void {
        self.accumulated.clearRetainingCapacity();
    }
};

// ── Failure diagnostics ──────────────────────────────────────────────

/// Prints a failure summary to stderr for operator inspection.
///
/// For each process the report includes its name, current state, exit
/// code (if available), and the last `tail_lines` lines of captured
/// stdout.  A listing of the storage directory is appended so that the
/// operator can see which metadata files were created.
pub fn dumpFailureDiagnostics(
    allocator: Allocator,
    scenario_name: []const u8,
    processes: []const *ProcessHandle,
    env: *const TempEnv,
) void {
    var stderr_buf: [4096]u8 = undefined;
    var stderr_w = std.fs.File.stderr().writer(&stderr_buf);
    const stderr = &stderr_w.interface;

    stderr.print(
        "\n╔══════════════════════════════════════════════════════════╗\n" ++
            "║  FAILURE DIAGNOSTICS: {s}\n" ++
            "╚══════════════════════════════════════════════════════════╝\n\n",
        .{scenario_name},
    ) catch return;

    stderr.print("  Base path : {s}\n", .{env.base_path}) catch return;
    stderr.print("  Storage   : {s}\n", .{env.storage_path}) catch return;
    stderr.print("  Logs      : {s}\n\n", .{env.logs_path}) catch return;
    stderr.flush() catch {};

    // ── Per-process diagnostics ──────────────────────────────────
    for (processes) |handle| {
        stderr.print("── Process: {s} ──\n", .{handle.name}) catch return;
        stderr.print("   State     : {s}\n", .{@tagName(handle.state)}) catch return;

        if (handle.exit_code) |code| {
            stderr.print("   Exit code : {d}\n", .{code}) catch return;
        } else {
            stderr.print("   Exit code : (none)\n", .{}) catch return;
        }

        stderr.print("   Stdout    : {s}\n", .{handle.stdout_path}) catch return;
        stderr.print("   Stderr    : {s}\n", .{handle.stderr_path}) catch return;
        stderr.flush() catch {};

        // Try to read the last 50 lines of the stdout log file.
        dumpTailOfFile(allocator, stderr, handle.stdout_path, 50, "stdout");
        dumpTailOfFile(allocator, stderr, handle.stderr_path, 20, "stderr");

        stderr.print("\n", .{}) catch return;
        stderr.flush() catch {};
    }

    // ── Storage directory listing ────────────────────────────────
    stderr.print("── Storage directory listing ──\n", .{}) catch return;
    stderr.flush() catch {};
    listDirectoryRecursive(stderr, env.storage_path, 0) catch {
        stderr.print("   (unable to list storage directory)\n", .{}) catch return;
    };
    stderr.flush() catch {};

    stderr.print("\n── End of failure diagnostics ──\n\n", .{}) catch return;
    stderr.flush() catch {};
}

// ── Internal helpers ─────────────────────────────────────────────────

fn dumpTailOfFile(
    allocator: Allocator,
    writer: anytype,
    path: []const u8,
    max_lines: usize,
    label: []const u8,
) void {
    const file = fs.cwd().openFile(path, .{}) catch {
        writer.print("   ({s} file not found)\n", .{label}) catch return;
        return;
    };
    defer file.close();

    const content = file.readToEndAlloc(allocator, 4 * 1024 * 1024) catch {
        writer.print("   ({s} file too large or unreadable)\n", .{label}) catch return;
        return;
    };
    defer allocator.free(content);

    if (content.len == 0) {
        writer.print("   ({s}: empty)\n", .{label}) catch return;
        return;
    }

    writer.print("   Last {d} lines of {s}:\n", .{ max_lines, label }) catch return;

    // Find last N lines.
    var lines_found: usize = 0;
    var pos: usize = content.len;
    if (pos > 0 and content[pos - 1] == '\n') pos -= 1;

    while (pos > 0) : (pos -= 1) {
        if (content[pos - 1] == '\n') {
            lines_found += 1;
            if (lines_found == max_lines) break;
        }
    }

    const tail = content[pos..];
    var line_iter = mem.splitScalar(u8, tail, '\n');
    while (line_iter.next()) |line| {
        if (line.len > 0) {
            writer.print("   │ {s}\n", .{line}) catch return;
        }
    }
}

fn listDirectoryRecursive(writer: anytype, path: []const u8, depth: usize) !void {
    var dir = try fs.cwd().openDir(path, .{ .iterate = true });
    defer dir.close();

    var iter = dir.iterate();
    while (try iter.next()) |entry| {
        // Indent by depth.
        var i: usize = 0;
        while (i < depth) : (i += 1) {
            writer.print("  ", .{}) catch return;
        }

        switch (entry.kind) {
            .directory => {
                writer.print("   📁 {s}/\n", .{entry.name}) catch return;
                const sub_path = try std.fmt.allocPrint(
                    std.heap.page_allocator,
                    "{s}/{s}",
                    .{ path, entry.name },
                );
                defer std.heap.page_allocator.free(sub_path);
                listDirectoryRecursive(writer, sub_path, depth + 1) catch {};
            },
            else => {
                writer.print("   📄 {s}\n", .{entry.name}) catch return;
            },
        }
    }
}

// ── Tests ────────────────────────────────────────────────────────────

test "LogCapture starts empty" {
    // Given
    var lc = LogCapture.init(std.testing.allocator);
    defer lc.deinit();

    // Then
    try std.testing.expectEqual(@as(usize, 0), lc.len());
    try std.testing.expectEqualStrings("", lc.getOutput());
}

test "append accumulates data" {
    // Given
    var lc = LogCapture.init(std.testing.allocator);
    defer lc.deinit();

    // When
    try lc.append("hello ");
    try lc.append("world");

    // Then
    try std.testing.expectEqualStrings("hello world", lc.getOutput());
    try std.testing.expectEqual(@as(usize, 11), lc.len());
}

test "contains finds needle in accumulated data" {
    // Given
    var lc = LogCapture.init(std.testing.allocator);
    defer lc.deinit();

    try lc.append("broker started on port 9000\n");
    try lc.append("service registered: my-service\n");

    // When / Then
    try std.testing.expect(lc.contains("broker started"));
    try std.testing.expect(lc.contains("service registered"));
    try std.testing.expect(!lc.contains("fatal error"));
}

test "countOccurrences counts multiple matches" {
    // Given
    var lc = LogCapture.init(std.testing.allocator);
    defer lc.deinit();

    try lc.append("foo bar foo baz foo\n");

    // When / Then
    try std.testing.expectEqual(@as(usize, 3), lc.countOccurrences("foo"));
    try std.testing.expectEqual(@as(usize, 1), lc.countOccurrences("bar"));
    try std.testing.expectEqual(@as(usize, 0), lc.countOccurrences("qux"));
}

test "countOccurrences returns zero for empty needle" {
    // Given
    var lc = LogCapture.init(std.testing.allocator);
    defer lc.deinit();

    try lc.append("something");

    // When / Then
    try std.testing.expectEqual(@as(usize, 0), lc.countOccurrences(""));
}

test "lastNLines returns last N lines" {
    // Given
    var lc = LogCapture.init(std.testing.allocator);
    defer lc.deinit();

    try lc.append("line 1\nline 2\nline 3\nline 4\nline 5\n");

    // When
    const last2 = lc.lastNLines(2);

    // Then
    try std.testing.expectEqualStrings("line 4\nline 5\n", last2);
}

test "lastNLines returns all when fewer lines exist" {
    // Given
    var lc = LogCapture.init(std.testing.allocator);
    defer lc.deinit();

    try lc.append("only one line\n");

    // When
    const last10 = lc.lastNLines(10);

    // Then
    try std.testing.expectEqualStrings("only one line\n", last10);
}

test "lastNLines returns empty for zero lines requested" {
    // Given
    var lc = LogCapture.init(std.testing.allocator);
    defer lc.deinit();

    try lc.append("some data\n");

    // When / Then
    try std.testing.expectEqualStrings("", lc.lastNLines(0));
}

test "lastNLines handles data without trailing newline" {
    // Given
    var lc = LogCapture.init(std.testing.allocator);
    defer lc.deinit();

    try lc.append("line 1\nline 2\nline 3");

    // When
    const last2 = lc.lastNLines(2);

    // Then
    try std.testing.expectEqualStrings("line 2\nline 3", last2);
}

test "lastNLines returns empty for empty buffer" {
    // Given
    var lc = LogCapture.init(std.testing.allocator);
    defer lc.deinit();

    // When / Then
    try std.testing.expectEqualStrings("", lc.lastNLines(5));
}

test "writeToFile persists accumulated data" {
    // Given
    var lc = LogCapture.init(std.testing.allocator);
    defer lc.deinit();

    try lc.append("persisted content\n");

    const tmp_path = "/tmp/brz-test-log-capture-write.txt";

    // When
    try lc.writeToFile(tmp_path);

    // Then
    const file = try fs.cwd().openFile(tmp_path, .{});
    defer file.close();
    defer fs.cwd().deleteFile(tmp_path) catch {};

    var buf: [256]u8 = undefined;
    const n = try file.readAll(&buf);
    try std.testing.expectEqualStrings("persisted content\n", buf[0..n]);
}

test "reset clears accumulated data" {
    // Given
    var lc = LogCapture.init(std.testing.allocator);
    defer lc.deinit();

    try lc.append("some data");
    try std.testing.expectEqual(@as(usize, 9), lc.len());

    // When
    lc.reset();

    // Then
    try std.testing.expectEqual(@as(usize, 0), lc.len());
    try std.testing.expectEqualStrings("", lc.getOutput());
    try std.testing.expect(!lc.contains("some data"));
}

test "initCapacity pre-allocates without affecting content" {
    // Given
    var lc = try LogCapture.initCapacity(std.testing.allocator, 4096);
    defer lc.deinit();

    // Then
    try std.testing.expectEqual(@as(usize, 0), lc.len());

    // When
    try lc.append("test");
    try std.testing.expectEqual(@as(usize, 4), lc.len());
}
