//! Receiver subsystem — inbound message path for cross-host communication.
//!
//! This is the single import point for all receiver-related functionality.
//! The rest of the codebase imports this module instead of individual files.

pub const receiver_event_loop = @import("receiver/receiver_event_loop.zig");
pub const peer_receiver = @import("receiver/peer_receiver.zig");
pub const message_router_mod = @import("receiver/message_router.zig");
pub const udp_reassembly = @import("receiver/udp_reassembly.zig");

// ── Re-exports: primary types ────────────────────────────────────────

pub const ReceiverEventLoop = receiver_event_loop.ReceiverEventLoop;
pub const ReceiverCounters = receiver_event_loop.ReceiverCounters;
pub const PeerReceiver = peer_receiver.PeerReceiver;
pub const LivenessState = peer_receiver.LivenessState;
pub const ReadState = peer_receiver.ReadState;
pub const ServiceRegistry = message_router_mod.ServiceRegistry;
pub const RouteResult = message_router_mod.RouteResult;
pub const StreamReceiver = udp_reassembly.StreamReceiver;

pub const routeUdpDataToService = message_router_mod.routeUdpDataToService;

// ── Test Discovery ───────────────────────────────────────────────────

// Ensure all receiver module tests are discovered by `zig build test`.
comptime {
    _ = @import("receiver/receiver_event_loop.zig");
    _ = @import("receiver/peer_receiver.zig");
    _ = @import("receiver/message_router.zig");
    _ = @import("receiver/udp_reassembly.zig");
}
