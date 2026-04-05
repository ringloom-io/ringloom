//! IPC (Inter-Process Communication) module for same-host shared-memory transport.
//!
//! This is the single import point for all IPC-related functionality.
//! It re-exports the producer and consumer types that wrap ring buffers
//! for zero-copy message passing between co-located services.

pub const ipc_producer = @import("ipc/ipc_producer.zig");
pub const IpcProducer = ipc_producer.IpcProducer;

pub const ipc_consumer = @import("ipc/ipc_consumer.zig");
pub const IpcConsumer = ipc_consumer.IpcConsumer;
pub const MessageHandler = ipc_consumer.MessageHandler;

// Ensure all IPC module tests are discovered by `zig build test`.
comptime {
    _ = @import("ipc/ipc_producer.zig");
    _ = @import("ipc/ipc_consumer.zig");
    _ = @import("ipc/ipc_test.zig");
}
