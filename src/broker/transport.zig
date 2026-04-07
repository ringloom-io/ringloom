//! Transport layer — TCP transport, buffer management, and platform I/O backends.
//!
//! This is the single import point for all transport-related functionality.
//! The rest of the codebase imports this module instead of individual files.

const builtin = @import("builtin");

// ── Submodules ────────────────────────────────────────────────────────

pub const buffer_pool = @import("transport/buffer_pool.zig");
pub const network_io = @import("transport/network_io.zig");
pub const kqueue = @import("transport/kqueue.zig");

/// io_uring backend — only available on Linux.
pub const io_uring = if (builtin.os.tag == .linux)
    @import("transport/io_uring.zig")
else
    struct {};

// ── Re-exports: Buffer Pool ──────────────────────────────────────────

pub const BufferPool = buffer_pool.BufferPool;
pub const BufferSlot = buffer_pool.BufferSlot;

// ── Re-exports: Network I/O ──────────────────────────────────────────

pub const NetworkIo = network_io.NetworkIo;
pub const IoUringNetworkIo = network_io.IoUringNetworkIo;
pub const KqueueNetworkIo = network_io.KqueueNetworkIo;

/// IoUring wrapper type — only available on Linux.
pub const IoUring = if (builtin.os.tag == .linux)
    io_uring.IoUring
else
    void;

/// IoUring configuration — only available on Linux.
pub const IoUringConfig = if (builtin.os.tag == .linux)
    io_uring.IoUringConfig
else
    void;

// ── Test Discovery ───────────────────────────────────────────────────

// Ensure all transport module tests are discovered by `zig build test`.
comptime {
    _ = @import("transport/buffer_pool.zig");
    _ = @import("transport/network_io.zig");
    _ = @import("transport/kqueue.zig");
    if (builtin.os.tag == .linux) {
        _ = @import("transport/io_uring.zig");
    }
}
