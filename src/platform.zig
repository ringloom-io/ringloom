//! Platform abstraction layer for the BRZ broker.
//!
//! This is the single import point for all platform-specific functionality.
//! The rest of the codebase imports this module instead of individual platform files.

pub const constants = @import("platform/constants.zig");
pub const atomic = @import("platform/atomic.zig");
pub const mapped_file = @import("platform/mapped_file.zig");
pub const MappedFile = mapped_file.MappedFile;
pub const clock = @import("platform/clock.zig");
pub const Clock = clock.Clock;
pub const thread = @import("platform/thread.zig");
pub const ThreadRunner = thread.ThreadRunner;
pub const EventLoop = thread.EventLoop;
pub const IdleStrategy = thread.IdleStrategy;
pub const process_sync = @import("platform/process_sync.zig");
pub const ProcessSynchronizer = process_sync.ProcessSynchronizer;
pub const WaitResult = process_sync.WaitResult;

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

// Re-export clock helpers as free functions for convenience.
pub fn epochMillis() i64 {
    return Clock.epochMillis();
}

pub fn monotonicNanos() i64 {
    return Clock.monotonicNanos();
}

// Platform helpers that wrap OS-specific calls used by the memory subsystem.

const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;

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
    const file = try std.fs.createFileAbsolute(path, .{
        .read = true,
        .truncate = true,
        .mode = 0o666,
    });
    return file.handle;
}

/// Open an existing file for read-write access. Returns the raw fd.
pub fn openFile(path: []const u8) !posix.fd_t {
    const file = std.fs.openFileAbsolute(path, .{ .mode = .read_write }) catch
        return error.FileNotFound;
    return file.handle;
}

/// Set file size via ftruncate.
pub fn ftruncate(fd: posix.fd_t, size: usize) !void {
    try posix.ftruncate(fd, @intCast(size));
}

/// Get file size via fstat.
pub fn fileSize(fd: posix.fd_t) !usize {
    const stat = try posix.fstat(fd);
    return @intCast(stat.size);
}

/// Memory-map a file descriptor as shared read-write.
pub fn mmap(fd: posix.fd_t, size: usize) ![]align(constants.page_size) u8 {
    const mapped = try posix.mmap(
        null,
        size,
        posix.PROT.READ | posix.PROT.WRITE,
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
        posix.PROT.READ | posix.PROT.WRITE,
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
    _ = @import("platform/atomic.zig");
    _ = @import("platform/mapped_file.zig");
    _ = @import("platform/clock.zig");
    _ = @import("platform/thread.zig");
    _ = @import("platform/process_sync.zig");
}
