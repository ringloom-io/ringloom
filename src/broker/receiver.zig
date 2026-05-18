//! Receiver subsystem — Aeron UDP receive and final service delivery.
//!
//! This is the single import point for all receiver-related functionality.
//! The rest of the codebase imports this module instead of individual files.

pub const receiver_event_loop = @import("receiver/receiver_event_loop.zig");
pub const message_router_mod = @import("receiver/message_router.zig");

// ── Re-exports: primary types ────────────────────────────────────────

pub const ReceiverEventLoop = receiver_event_loop.ReceiverEventLoop;
pub const ReceiverCounters = receiver_event_loop.ReceiverCounters;
pub const PeerLiveness = receiver_event_loop.PeerLiveness;
pub const LivenessState = receiver_event_loop.LivenessState;
pub const ServiceRegistry = message_router_mod.ServiceRegistry;
pub const RouteResult = message_router_mod.RouteResult;

pub const routeDataToService = message_router_mod.routeDataToService;

// ── Test Discovery ───────────────────────────────────────────────────

// Ensure all receiver module tests are discovered by `zig build test`.
comptime {
    _ = @import("receiver/receiver_event_loop.zig");
    _ = @import("receiver/message_router.zig");
}
