//! Control plane module for the BRZ broker.
//!
//! This is the single import point for control-plane functionality.
//! It re-exports the ControlLoop, ServiceRegistry, control messages,
//! heartbeat checker, and leader election modules.

pub const control_loop = @import("control/control_loop.zig");
pub const ControlLoop = control_loop.ControlLoop;

pub const control_messages = @import("control/control_messages.zig");

pub const service_registry = @import("control/service_registry.zig");
pub const ServiceRegistry = service_registry.ServiceRegistry;
pub const ServiceInstance = service_registry.ServiceInstance;
pub const InstanceKey = service_registry.InstanceKey;

pub const service_heartbeat_checker = @import("control/service_heartbeat_checker.zig");
pub const ServiceHeartbeatChecker = service_heartbeat_checker.ServiceHeartbeatChecker;

pub const service_leader_election = @import("control/service_leader_election.zig");
pub const ServiceLeaderElection = service_leader_election.ServiceLeaderElection;

// Ensure all control module tests are discovered by `zig build test`.
comptime {
    _ = @import("control/control_messages.zig");
    _ = @import("control/service_registry.zig");
    _ = @import("control/service_heartbeat_checker.zig");
    _ = @import("control/service_leader_election.zig");
    _ = @import("control/control_loop.zig");
}
