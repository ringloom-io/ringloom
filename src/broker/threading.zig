//! Threading model for the BRZ broker.
//!
//! This is the single import point for all threading-related functionality.
//! It re-exports the command queue, composite event loop, threading mode,
//! and broker threads modules.

pub const command = @import("threading/command.zig");
pub const Command = command.Command;

pub const command_queue = @import("threading/command_queue.zig");
pub const CommandQueue = command_queue.CommandQueue;
pub const command_queue_buffer_length = command_queue.command_queue_buffer_length;

pub const composite_event_loop = @import("threading/composite_event_loop.zig");
pub const CompositeEventLoop = composite_event_loop.CompositeEventLoop;

pub const threading_mode = @import("threading/threading_mode.zig");
pub const ThreadingMode = threading_mode.ThreadingMode;

pub const broker_threads = @import("threading/broker_threads.zig");
pub const BrokerThreads = broker_threads.BrokerThreads;

// Ensure all threading module tests are discovered by `zig build test`.
comptime {
    _ = @import("threading/command.zig");
    _ = @import("threading/command_queue.zig");
    _ = @import("threading/composite_event_loop.zig");
    _ = @import("threading/threading_mode.zig");
    _ = @import("threading/broker_threads.zig");
}
