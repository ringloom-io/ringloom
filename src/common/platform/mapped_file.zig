//! Memory-mapped file abstraction for the BRZ broker.
//!
//! Provides POSIX mmap-based memory mapping for shared memory IPC.
//! The metadata files on /dev/shm (tmpfs) are the backbone of BRZ's
//! same-host communication.

const std = @import("std");
const posix = std.posix;
const constants = @import("constants.zig");
const platform_io = @import("io.zig");
const defaultIo = platform_io.default;

fn closeFd(fd: posix.fd_t) void {
    switch (posix.errno(posix.system.close(fd))) {
        .SUCCESS, .INTR => {},
        else => {},
    }
}

pub const MappedFile = struct {
    /// Pointer to the mapped memory region, page-aligned.
    data: [*]align(constants.page_size) u8,

    /// Total size of the mapped region in bytes.
    len: usize,

    /// File descriptor (POSIX). Kept open for msync/munmap.
    fd: posix.fd_t,

    /// The filesystem path of the backing file (owned, must be freed).
    path: []const u8,

    /// Allocator used for the path string — needed for cleanup.
    allocator: std.mem.Allocator,

    const Self = @This();

    pub const CreateError = error{
        FileAlreadyOwnedByLiveProcess,
        MappingFailed,
        FileCreateFailed,
        TruncateFailed,
        DirectoryCreateFailed,
        InvalidSize,
        PathTooLong,
    } || posix.MMapError || posix.OpenError || std.Io.File.OpenError || std.mem.Allocator.Error;

    pub const OpenError = error{
        FileNotFound,
        MappingFailed,
        InvalidFileSize,
    } || posix.MMapError || posix.OpenError || std.Io.File.OpenError || std.mem.Allocator.Error;

    /// Create a new metadata file at the given path with the given size.
    ///
    /// If the file already exists:
    ///   1. Read the PID from offset 16 (i64, little-endian).
    ///   2. Check if that process is alive (see `isProcessAlive`).
    ///   3. If alive → return error.FileAlreadyOwnedByLiveProcess.
    ///   4. If dead → truncate and reuse.
    ///
    /// The size is rounded up to the nearest page boundary.
    /// Parent directories are created as needed.
    pub fn create(
        allocator: std.mem.Allocator,
        dir_path: []const u8,
        file_name: []const u8,
        size: usize,
    ) CreateError!Self {
        if (size == 0) return error.InvalidSize;

        const aligned_size = constants.alignUp(size, constants.page_size);

        // Build full path: "{dir_path}/{file_name}"
        const full_path = try std.fs.path.join(allocator, &.{ dir_path, file_name });
        errdefer allocator.free(full_path);

        // Create parent directories recursively.
        std.Io.Dir.cwd().createDirPath(defaultIo(), dir_path) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return error.DirectoryCreateFailed,
        };

        // Check if file exists and handle PID check.
        const file_exists = blk: {
            std.Io.Dir.accessAbsolute(defaultIo(), full_path, .{}) catch break :blk false;
            break :blk true;
        };

        if (file_exists) {
            check_pid: {
                // Temporarily open read-only to check the PID.
                const check_file = std.Io.Dir.openFileAbsolute(defaultIo(), full_path, .{ .mode = .read_only }) catch break :check_pid;
                defer check_file.close(defaultIo());

                // Read the PID at the fixed metadata header offset.
                var pid_buf: [8]u8 = undefined;
                const bytes_read = check_file.readPositionalAll(defaultIo(), &pid_buf, pid_field_offset) catch break :check_pid;
                if (bytes_read == 8) {
                    const pid = std.mem.readInt(i64, &pid_buf, .little);
                    if (pid > 0 and isProcessAlive(pid)) {
                        return error.FileAlreadyOwnedByLiveProcess;
                    }
                }
            }
        }

        // Open/create the file with read-write permissions, truncating.
        const file = std.Io.Dir.createFileAbsolute(defaultIo(), full_path, .{
            .read = true,
            .truncate = true,
        }) catch return error.FileCreateFailed;
        const fd = file.handle;
        errdefer closeFd(fd);

        // Set file size.
        const trunc_file: std.Io.File = .{
            .handle = fd,
            .flags = .{ .nonblocking = false },
        };
        trunc_file.setLength(defaultIo(), @intCast(aligned_size)) catch return error.TruncateFailed;

        // Memory-map the file.
        const mapped = posix.mmap(
            null,
            aligned_size,
            .{ .READ = true, .WRITE = true },
            .{ .TYPE = .SHARED },
            fd,
            0,
        ) catch return error.MappingFailed;

        return Self{
            .data = @alignCast(mapped.ptr),
            .len = aligned_size,
            .fd = fd,
            .path = full_path,
            .allocator = allocator,
        };
    }

    /// Open an existing metadata file for read/write access.
    ///
    /// The entire file is mapped. Returns error.FileNotFound if it doesn't exist.
    pub fn open(
        allocator: std.mem.Allocator,
        path: []const u8,
    ) OpenError!Self {
        const path_copy = try allocator.dupe(u8, path);
        errdefer allocator.free(path_copy);

        const file = std.Io.Dir.openFileAbsolute(defaultIo(), path, .{ .mode = .read_write }) catch
            return error.FileNotFound;
        const fd = file.handle;
        errdefer closeFd(fd);

        // Get file size via fstat.
        const stat = posix.fstat(fd);
        const file_size: usize = @intCast(stat.size);
        if (file_size == 0) return error.InvalidFileSize;

        const mapped = posix.mmap(
            null,
            file_size,
            .{ .READ = true, .WRITE = true },
            .{ .TYPE = .SHARED },
            fd,
            0,
        ) catch return error.MappingFailed;

        return Self{
            .data = @alignCast(mapped.ptr),
            .len = file_size,
            .fd = fd,
            .path = path_copy,
            .allocator = allocator,
        };
    }

    /// Flush dirty pages to the backing file (or no-op on tmpfs).
    /// Uses MS_SYNC for guaranteed durability.
    pub fn sync(self: *Self) void {
        posix.msync(
            @alignCast(self.data[0..self.len]),
            .{ .SYNC = true },
        ) catch {};
    }

    /// Unmap the memory region and close the file descriptor.
    /// After this call, the MappedFile is invalidated — do not use.
    pub fn close(self: *Self) void {
        // Step 1: Unmap.
        posix.munmap(@alignCast(self.data[0..self.len]));

        // Step 2: Close file descriptor.
        closeFd(self.fd);

        // Step 3: Free the path string.
        self.allocator.free(self.path);

        // Invalidate.
        self.data = undefined;
        self.len = 0;
        self.fd = -1;
    }

    /// Get a typed pointer into the mapped region at a given byte offset.
    /// Useful for overlaying the metadata header struct.
    pub fn ptrAt(self: *Self, comptime T: type, offset: usize) *T {
        std.debug.assert(offset + @sizeOf(T) <= self.len);
        return @ptrCast(@alignCast(self.data + offset));
    }

    /// Get the mapped memory as a byte slice.
    pub fn asSlice(self: *Self) []align(constants.page_size) u8 {
        return self.data[0..self.len];
    }
};

/// PID field offset in the metadata header (matches Java's PID_FIELD_OFFSET).
const pid_field_offset: u64 = 16;

/// Check if a process with the given PID is currently alive.
///
/// Linux/macOS: `kill(pid, 0)` — sends signal 0 (no actual signal) to test existence.
///   - Returns true if the process exists (even if we can't signal it — EPERM).
///   - Returns false if ESRCH (no such process).
pub fn isProcessAlive(pid: i64) bool {
    if (pid <= 0) return false;

    const pid_int: std.posix.pid_t = @intCast(pid);

    const result = std.posix.kill(pid_int, @enumFromInt(0)) catch |err| {
        return switch (err) {
            error.PermissionDenied => true, // process exists but we can't signal it
            error.ProcessNotFound => false,
            else => false,
        };
    };
    _ = result;
    return true; // kill succeeded — process exists
}

/// Build the full path to a service metadata file.
///
/// Format: `{storage_path}/{group}/services/{name}_{id}.dat`
pub fn serviceMetadataPath(
    allocator: std.mem.Allocator,
    storage_path: []const u8,
    group: []const u8,
    service_name: []const u8,
    service_id: i32,
) ![]const u8 {
    const file_name = try std.fmt.allocPrint(allocator, "{s}_{d}.dat", .{ service_name, service_id });
    defer allocator.free(file_name);

    return std.fs.path.join(allocator, &.{
        storage_path,
        group,
        constants.services_directory,
        file_name,
    });
}

/// Build the path to the services directory.
///
/// Format: `{storage_path}/{group}/services/`
pub fn servicesDirectoryPath(
    allocator: std.mem.Allocator,
    storage_path: []const u8,
    group: []const u8,
) ![]const u8 {
    return std.fs.path.join(allocator, &.{
        storage_path,
        group,
        constants.services_directory,
    });
}

/// Ensure the services directory exists, creating it and any parent directories.
pub fn ensureServicesDirectory(
    storage_path: []const u8,
    group: []const u8,
) !void {
    const allocator = std.heap.page_allocator;
    const dir_path = try servicesDirectoryPath(allocator, storage_path, group);
    defer allocator.free(dir_path);

    std.Io.Dir.cwd().createDirPath(defaultIo(), dir_path) catch |err| switch (err) {
        error.PathAlreadyExists => {},
        else => return err,
    };
}

// ── Tests ─────────────────────────────────────────────────────────────

test "MappedFile create and close on tmpfs" {
    const testing = std.testing;
    const allocator = testing.allocator;
    const dir = "/tmp/brz-test-mapped-file";

    // Cleanup from previous runs.
    std.Io.Dir.cwd().deleteTree(defaultIo(), dir) catch {};
    defer std.Io.Dir.cwd().deleteTree(defaultIo(), dir) catch {};

    var mf = try MappedFile.create(allocator, dir, "test_1.dat", 4096);
    defer mf.close();

    // Verify size is page-aligned.
    try testing.expect(mf.len == 4096);

    // Write and read back.
    mf.data[0] = 0xAB;
    mf.data[4095] = 0xCD;
    try testing.expectEqual(@as(u8, 0xAB), mf.data[0]);
    try testing.expectEqual(@as(u8, 0xCD), mf.data[4095]);
}

test "MappedFile create with sub-page size rounds up" {
    const testing = std.testing;
    const allocator = testing.allocator;
    const dir = "/tmp/brz-test-mapped-file-roundup";
    std.Io.Dir.cwd().deleteTree(defaultIo(), dir) catch {};
    defer std.Io.Dir.cwd().deleteTree(defaultIo(), dir) catch {};

    var mf = try MappedFile.create(allocator, dir, "small.dat", 100);
    defer mf.close();

    try testing.expect(mf.len == 4096); // Rounded up to page size.
}

test "MappedFile ptrAt" {
    const testing = std.testing;
    const allocator = testing.allocator;
    const dir = "/tmp/brz-test-mapped-file-ptrat";
    std.Io.Dir.cwd().deleteTree(defaultIo(), dir) catch {};
    defer std.Io.Dir.cwd().deleteTree(defaultIo(), dir) catch {};

    var mf = try MappedFile.create(allocator, dir, "ptrat.dat", 4096);
    defer mf.close();

    // Write an i32 at offset 8.
    const id_ptr = mf.ptrAt(i32, 8);
    id_ptr.* = 42;
    try testing.expectEqual(@as(i32, 42), mf.ptrAt(i32, 8).*);
}

test "isProcessAlive" {
    const testing = std.testing;

    // Our own PID should be alive.
    const our_pid: i64 = @intCast(std.os.linux.getpid());
    try testing.expect(isProcessAlive(our_pid));

    // PID 0 is special, PID -1 is invalid.
    try testing.expect(!isProcessAlive(0));
    try testing.expect(!isProcessAlive(-1));

    // Very high PID is almost certainly dead.
    try testing.expect(!isProcessAlive(999999999));
}

test "MappedFile rejects live PID" {
    const testing = std.testing;
    const allocator = testing.allocator;
    const dir = "/tmp/brz-test-mapped-file-pid";
    std.Io.Dir.cwd().deleteTree(defaultIo(), dir) catch {};
    defer std.Io.Dir.cwd().deleteTree(defaultIo(), dir) catch {};

    // Create a file and write our own PID at offset 16.
    var mf = try MappedFile.create(allocator, dir, "live.dat", 4096);
    const our_pid: i64 = @intCast(std.os.linux.getpid());
    std.mem.writeInt(i64, mf.data[16..24], our_pid, .little);
    mf.close();

    // Attempting to create again should fail because we're alive.
    const result = MappedFile.create(allocator, dir, "live.dat", 4096);
    try testing.expectError(error.FileAlreadyOwnedByLiveProcess, result);
}

test "MappedFile allows reuse of dead PID" {
    const testing = std.testing;
    const allocator = testing.allocator;
    const dir = "/tmp/brz-test-mapped-file-dead";
    std.Io.Dir.cwd().deleteTree(defaultIo(), dir) catch {};
    defer std.Io.Dir.cwd().deleteTree(defaultIo(), dir) catch {};

    // Create a file and write a very high PID that doesn't exist.
    var mf = try MappedFile.create(allocator, dir, "dead.dat", 4096);
    std.mem.writeInt(i64, mf.data[16..24], 999999999, .little);
    mf.close();

    // Should succeed — PID 999999999 is (almost certainly) dead.
    var mf2 = try MappedFile.create(allocator, dir, "dead.dat", 4096);
    defer mf2.close();
    try testing.expect(mf2.len == 4096);
}
