//! Platform abstraction layer for the RingLoom broker.
//!
//! This is the single import point for all platform-specific functionality.
//! The rest of the codebase imports this module instead of individual platform files.

pub const constants = @import("platform/constants.zig");
pub const io = @import("platform/io.zig");
pub const atomic = @import("platform/atomic.zig");
pub const mapped_file = @import("platform/mapped_file.zig");
pub const MappedFile = mapped_file.MappedFile;
pub const clock = @import("platform/clock.zig");
pub const Clock = clock.Clock;
pub const thread = @import("platform/thread.zig");
pub const ThreadRunner = thread.ThreadRunner;
pub const EventLoop = thread.EventLoop;
pub const IdleStrategy = thread.IdleStrategy;
pub const composite_event_loop = @import("platform/composite_event_loop.zig");
pub const CompositeEventLoop = composite_event_loop.CompositeEventLoop;
pub const process_sync = @import("platform/process_sync.zig");
pub const ProcessSynchronizer = process_sync.ProcessSynchronizer;
pub const WaitResult = process_sync.WaitResult;
pub const socket = @import("platform/socket.zig");

// Re-export commonly used atomic types for convenience.
pub const AtomicI32 = atomic.AtomicI32;
pub const AtomicI64 = atomic.AtomicI64;
pub const AtomicBool = atomic.AtomicBool;
pub const CacheLinePaddedAtomicI64 = atomic.CacheLinePaddedAtomicI64;
pub const CacheLinePaddedAtomicI32 = atomic.CacheLinePaddedAtomicI32;

// Re-export commonly used mapped_file helpers.
pub const isProcessAlive = mapped_file.isProcessAlive;
pub const serviceMetadataPath = mapped_file.serviceMetadataPath;
pub const servicesDirectoryPath = mapped_file.servicesDirectoryPath;
pub const ensureServicesDirectory = mapped_file.ensureServicesDirectory;

pub fn sleepNanos(duration_ns: u64) void {
    return thread.sleepNanos(duration_ns);
}

// Platform helpers that wrap OS-specific calls used by the memory subsystem.

const builtin = @import("builtin");
const posix = std.posix;

const std = @import("std");
pub const defaultIo = io.default;

/// Get the current process ID.
pub fn getPid() i64 {
    if (comptime builtin.os.tag == .linux) {
        return @intCast(std.os.linux.getpid());
    } else {
        // Portable fallback — std.process doesn't expose getpid directly,
        // but on POSIX we can use the C library.
        const c = struct {
            extern "c" fn getpid() c_int;
        };
        return @intCast(c.getpid());
    }
}

/// Create a new file, truncating if it exists. Returns the raw fd.
pub fn createFile(path: []const u8) !posix.fd_t {
    const file = try std.Io.Dir.createFileAbsolute(defaultIo(), path, .{
        .read = true,
        .truncate = true,
    });
    return file.handle;
}

/// Open an existing file for read-write access. Returns the raw fd.
pub fn openFile(path: []const u8) !posix.fd_t {
    const file = std.Io.Dir.openFileAbsolute(defaultIo(), path, .{ .mode = .read_write }) catch
        return error.FileNotFound;
    return file.handle;
}

pub fn closeFd(fd: posix.fd_t) void {
    switch (posix.errno(posix.system.close(fd))) {
        .SUCCESS, .INTR => {},
        else => {},
    }
}

/// Set file size via ftruncate.
pub fn ftruncate(fd: posix.fd_t, size: usize) !void {
    const file: std.Io.File = .{
        .handle = fd,
        .flags = .{ .nonblocking = false },
    };
    try file.setLength(defaultIo(), @intCast(size));
}

/// Get file size via fstat.
pub fn fileSize(fd: posix.fd_t) !usize {
    const file: std.Io.File = .{
        .handle = fd,
        .flags = .{ .nonblocking = false },
    };
    return @intCast(try file.length(defaultIo()));
}

/// Memory-map a file descriptor as shared read-write.
pub fn mmap(fd: posix.fd_t, size: usize) ![]align(constants.page_size) u8 {
    const mapped = try posix.mmap(
        null,
        size,
        .{ .READ = true, .WRITE = true },
        .{ .TYPE = .SHARED },
        fd,
        0,
    );
    return @alignCast(mapped);
}

/// Memory-map anonymous memory (not file-backed).
pub fn mmapAnonymous(size: usize) ![]align(constants.page_size) u8 {
    const mapped = try posix.mmap(
        null,
        size,
        .{ .READ = true, .WRITE = true },
        .{ .TYPE = .PRIVATE, .ANONYMOUS = true },
        @as(posix.fd_t, -1),
        0,
    );
    return @alignCast(mapped);
}

/// Unmap a previously mapped region.
pub fn munmap(mapped: []align(constants.page_size) u8) void {
    posix.munmap(@alignCast(mapped));
}

// Ensure all platform module tests are discovered by `zig build test`.
comptime {
    _ = @import("platform/constants.zig");
    _ = @import("platform/io.zig");
    _ = @import("platform/atomic.zig");
    _ = @import("platform/mapped_file.zig");
    _ = @import("platform/clock.zig");
    _ = @import("platform/thread.zig");
    _ = @import("platform/composite_event_loop.zig");
    _ = @import("platform/process_sync.zig");
    _ = @import("platform/socket.zig");
}
