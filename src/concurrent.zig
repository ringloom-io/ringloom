//! Concurrent utilities for the BRZ broker.
//!
//! This is the single import point for all concurrency-related functionality.
//! It re-exports the counters manager, deduplicating error log, and
//! thread-local error state.

pub const counters = @import("concurrent/counters.zig");
pub const CountersManager = counters.CountersManager;

pub const error_log = @import("concurrent/error_log.zig");
pub const ErrorLog = error_log.ErrorLog;

pub const error_state = @import("concurrent/error_state.zig");
pub const ErrorState = error_state.ErrorState;

pub const ring_buffer = @import("concurrent/ring_buffer.zig");
pub const RingBuffer = ring_buffer.RingBuffer;

pub const command_queue = @import("concurrent/command_queue.zig");
pub const CommandQueue = command_queue.CommandQueue;
pub const Command = command_queue.Command;

// Ensure all concurrent module tests are discovered by `zig build test`.
comptime {
    _ = @import("concurrent/counters.zig");
    _ = @import("concurrent/error_log.zig");
    _ = @import("concurrent/error_state.zig");
    _ = @import("concurrent/ring_buffer.zig");
    _ = @import("concurrent/command_queue.zig");
}
