//! Cluster management module for the BRZ broker.
//!
//! This is the single import point for cluster-related functionality.
//! It re-exports all cluster subsystem modules: leader election, node
//! membership, cluster state synchronization, service leader election,
//! admin message protocol, admin dispatch, broker heartbeat, the
//! ClusterEventHandler coordinator, and the ClusterManager facade.

pub const admin_messages = @import("cluster/admin_messages.zig");
pub const AdminMessageHeader = admin_messages.AdminMessageHeader;

pub const leader_election = @import("cluster/leader_election.zig");
pub const LeaderElection = leader_election.LeaderElection;

pub const node_membership = @import("cluster/node_membership.zig");
pub const NodeMembership = node_membership.NodeMembership;
pub const Node = node_membership.Node;
pub const ConnectionState = node_membership.ConnectionState;

pub const cluster_state = @import("cluster/cluster_state.zig");
pub const ClusterState = cluster_state.ClusterState;
pub const RemoteServiceInstance = cluster_state.RemoteServiceInstance;

pub const service_leader_election = @import("cluster/service_leader_election.zig");
pub const ServiceLeaderElectionManager = service_leader_election.ServiceLeaderElectionManager;

pub const admin_dispatch = @import("cluster/admin_dispatch.zig");
pub const AdminCommand = admin_dispatch.AdminCommand;

pub const broker_heartbeat = @import("cluster/broker_heartbeat.zig");
pub const BrokerHeartbeatSender = broker_heartbeat.BrokerHeartbeatSender;

pub const cluster_event_handler = @import("cluster/cluster_event_handler.zig");
pub const ClusterEventHandler = cluster_event_handler.ClusterEventHandler;

pub const cluster_manager = @import("cluster/cluster_manager.zig");
pub const ClusterManager = cluster_manager.ClusterManager;

// Ensure all cluster module tests are discovered by `zig build test`.
comptime {
    _ = @import("cluster/admin_messages.zig");
    _ = @import("cluster/leader_election.zig");
    _ = @import("cluster/node_membership.zig");
    _ = @import("cluster/cluster_state.zig");
    _ = @import("cluster/service_leader_election.zig");
    _ = @import("cluster/admin_dispatch.zig");
    _ = @import("cluster/broker_heartbeat.zig");
    _ = @import("cluster/cluster_event_handler.zig");
    _ = @import("cluster/cluster_manager.zig");
}
