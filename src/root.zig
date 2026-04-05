//! BRZ Broker — high-performance IPC framework using shared-memory ring buffers.
//!
//! This is the root source file for the brz_broker library module.
//! It re-exports all public APIs and ensures all tests are discovered.

pub const platform = @import("platform.zig");
pub const memory = @import("memory.zig");
pub const concurrent = @import("concurrent.zig");

// Re-export commonly used platform types at the top level for convenience.
pub const AtomicI32 = platform.AtomicI32;
pub const AtomicI64 = platform.AtomicI64;
pub const AtomicBool = platform.AtomicBool;
pub const CacheLinePaddedAtomicI64 = platform.CacheLinePaddedAtomicI64;
pub const CacheLinePaddedAtomicI32 = platform.CacheLinePaddedAtomicI32;
pub const Clock = platform.Clock;
pub const MappedFile = platform.MappedFile;
pub const ThreadRunner = platform.ThreadRunner;
pub const EventLoop = platform.EventLoop;
pub const IdleStrategy = platform.IdleStrategy;
pub const ProcessSynchronizer = platform.ProcessSynchronizer;
pub const WaitResult = platform.WaitResult;

// Ensure all tests in all submodules are discovered by `zig build test`.
comptime {
    // Platform layer (task 01)
    _ = @import("platform/constants.zig");
    _ = @import("platform/atomic.zig");
    _ = @import("platform/mapped_file.zig");
    _ = @import("platform/clock.zig");
    _ = @import("platform/thread.zig");
    _ = @import("platform/process_sync.zig");

    // Memory layout layer (task 02)
    _ = @import("memory/constants.zig");
    _ = @import("memory/broker_metadata.zig");
    _ = @import("memory/service_metadata.zig");
    _ = @import("memory/receive_log.zig");
    _ = @import("memory/service_scanner.zig");
    _ = @import("memory/metadata_descriptor_provider.zig");
    _ = @import("memory/buffers_provider.zig");

    // Concurrent utilities (task 03)
    _ = @import("concurrent/error_log.zig");
    _ = @import("concurrent/error_state.zig");
    _ = @import("concurrent/counters.zig");
    _ = @import("concurrent/ring_buffer.zig");
}

test "root module compiles" {
    // Smoke test: verify the module graph is valid.
    _ = platform;
    _ = memory;
    _ = concurrent;
}
