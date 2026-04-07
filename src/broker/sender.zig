//! Sender subsystem — outbound message path for cross-host communication.
//!
//! This is the single import point for all sender-related functionality.
//! The rest of the codebase imports this module instead of individual files.

pub const sender_event_loop = @import("sender/sender_event_loop.zig");
pub const peer_sender = @import("sender/peer_sender.zig");
pub const send_buffer_pool = @import("sender/send_buffer_pool.zig");
pub const sender_command = @import("sender/sender_command.zig");
pub const write_queue = @import("sender/write_queue.zig");

// ── Re-exports: primary types ────────────────────────────────────────

pub const SenderEventLoop = sender_event_loop.SenderEventLoop;
pub const PeerSender = peer_sender.PeerSender;
pub const SendBufferPool = send_buffer_pool.SendBufferPool;
pub const SenderCommand = sender_command.SenderCommand;
pub const WriteQueue = write_queue.WriteQueue;

// ── Test Discovery ───────────────────────────────────────────────────

// Ensure all sender module tests are discovered by `zig build test`.
comptime {
    _ = @import("sender/sender_event_loop.zig");
    _ = @import("sender/peer_sender.zig");
    _ = @import("sender/send_buffer_pool.zig");
    _ = @import("sender/sender_command.zig");
    _ = @import("sender/write_queue.zig");
}
