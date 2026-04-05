//! Receiver subsystem — inbound message path for cross-host communication.
//!
//! This is the single import point for all receiver-related functionality.
//! The rest of the codebase imports this module instead of individual files.

pub const receiver_event_loop = @import("receiver/receiver_event_loop.zig");
pub const peer_receiver = @import("receiver/peer_receiver.zig");
pub const loss_detector = @import("receiver/loss_detector.zig");
pub const fragment_assembler = @import("receiver/fragment_assembler.zig");
pub const message_router_mod = @import("receiver/message_router.zig");
pub const receive_log_buffer = @import("receiver/receive_log_buffer.zig");

// ── Re-exports: primary types ────────────────────────────────────────

pub const ReceiverEventLoop = receiver_event_loop.ReceiverEventLoop;
pub const ReceiverCounterId = receiver_event_loop.ReceiverCounterId;
pub const ReceiverCounters = receiver_event_loop.ReceiverCounters;
pub const PeerReceiver = peer_receiver.PeerReceiver;
pub const LossDetector = loss_detector.LossDetector;
pub const FragmentAssembler = fragment_assembler.FragmentAssembler;
pub const ServiceRegistry = message_router_mod.ServiceRegistry;
pub const RouteResult = message_router_mod.RouteResult;

pub const insertPacket = receive_log_buffer.insertPacket;
pub const readFrame = receive_log_buffer.readFrame;
pub const routeToService = message_router_mod.routeToService;
pub const markFrameConsumed = message_router_mod.markFrameConsumed;

// ── Test Discovery ───────────────────────────────────────────────────

// Ensure all receiver module tests are discovered by `zig build test`.
comptime {
    _ = @import("receiver/receiver_event_loop.zig");
    _ = @import("receiver/peer_receiver.zig");
    _ = @import("receiver/loss_detector.zig");
    _ = @import("receiver/fragment_assembler.zig");
    _ = @import("receiver/message_router.zig");
    _ = @import("receiver/receive_log_buffer.zig");
}
