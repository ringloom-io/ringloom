//! Sender subsystem — outbound message path for cross-host communication.
//!
//! This is the single import point for all sender-related functionality.
//! The rest of the codebase imports this module instead of individual files.

pub const sender_event_loop = @import("sender/sender_event_loop.zig");
pub const peer_sender = @import("sender/peer_sender.zig");
pub const sender_command = @import("sender/sender_command.zig");
pub const udp_scheduler = @import("sender/udp_scheduler.zig");

// ── Re-exports: primary types ────────────────────────────────────────

pub const SenderEventLoop = sender_event_loop.SenderEventLoop;
pub const PeerSender = peer_sender.PeerSender;
pub const SenderCommand = sender_command.SenderCommand;
pub const StreamSender = udp_scheduler.StreamSender;
pub const DestinationScheduler = udp_scheduler.DestinationScheduler;

// ── Test Discovery ───────────────────────────────────────────────────

// Ensure all sender module tests are discovered by `zig build test`.
comptime {
    _ = @import("sender/sender_event_loop.zig");
    _ = @import("sender/peer_sender.zig");
    _ = @import("sender/sender_command.zig");
    _ = @import("sender/udp_scheduler.zig");
}
