//! Broker module root for BRZ.
//!
//! This is the root source file for the `brz_broker` library module.
//! It re-exports broker-specific APIs: configuration, control loop,
//! cluster management, sender/receiver event loops, transport,
//! flow control, threading, and the application bootstrap layer.
//!
//! Consumers should import this root rather than deep internal files.

const brz_common = @import("brz_common");

// ── Broker configuration ─────────────────────────────────────────────

pub const config = struct {
    pub const broker_config = @import("brz_common").config.broker_config;
    pub const BrokerConfig = broker_config.BrokerConfig;
    pub const ThreadingMode = broker_config.ThreadingMode;
    pub const IdleStrategyName = broker_config.IdleStrategyName;
    pub const PeerEndpoint = broker_config.PeerEndpoint;
};

// ── Broker subsystem namespaces ──────────────────────────────────────

pub const control = @import("control.zig");
pub const cluster = @import("cluster.zig");
pub const sender = @import("sender.zig");
pub const receiver = @import("receiver.zig");
pub const transport = @import("transport.zig");
pub const threading = @import("threading.zig");

// ── Monitoring (re-exported from common for broker convenience) ──────

pub const monitoring = brz_common.monitoring;

// ── Application bootstrap layer ──────────────────────────────────────

pub const app = struct {
    pub const broker_runtime = @import("app/broker_runtime.zig");
    pub const BrokerRuntime = broker_runtime.BrokerRuntime;

    pub const broker_application = @import("app/broker_application.zig");
    pub const BrokerApplication = broker_application.BrokerApplication;
    pub const ExitCode = broker_application.ExitCode;

    pub const broker_application_factory = @import("app/broker_application_factory.zig");
    pub const BrokerApplicationFactory = broker_application_factory.BrokerApplicationFactory;
    pub const BrokerApplicationBootstrap = broker_application_factory.BrokerApplicationBootstrap;
    pub const freeBrokerConfig = broker_application_factory.freeBrokerConfig;
};

// ── Top-level convenience re-exports ─────────────────────────────────

pub const BrokerRuntime = app.BrokerRuntime;
pub const BrokerApplication = app.BrokerApplication;
pub const BrokerApplicationFactory = app.BrokerApplicationFactory;
pub const BrokerApplicationBootstrap = app.BrokerApplicationBootstrap;
pub const ExitCode = app.ExitCode;
pub const freeBrokerConfig = app.freeBrokerConfig;

pub const BrokerConfig = config.BrokerConfig;
pub const ConfigLoader = brz_common.config.ConfigLoader;

pub const ControlLoop = control.ControlLoop;
pub const ServiceRegistry = control.ServiceRegistry;
pub const ClusterManager = cluster.ClusterManager;
pub const SenderEventLoop = sender.SenderEventLoop;
pub const ReceiverEventLoop = receiver.ReceiverEventLoop;
pub const BrokerThreads = threading.BrokerThreads;

// ── Test discovery ───────────────────────────────────────────────────
// Ensure broker module tests and submodule tests are discovered by `zig build test`.

comptime {
    // Application layer
    _ = @import("app/broker_runtime.zig");
    _ = @import("app/broker_application.zig");
    _ = @import("app/broker_application_factory.zig");

    // Control plane
    _ = @import("control/control_messages.zig");
    _ = @import("control/service_registry.zig");
    _ = @import("control/service_heartbeat_checker.zig");
    _ = @import("control/service_leader_election.zig");
    _ = @import("control/control_loop.zig");

    // Cluster management
    _ = @import("cluster/admin_messages.zig");
    _ = @import("cluster/leader_election.zig");
    _ = @import("cluster/node_membership.zig");
    _ = @import("cluster/cluster_state.zig");
    _ = @import("cluster/service_leader_election.zig");
    _ = @import("cluster/admin_dispatch.zig");
    _ = @import("cluster/broker_heartbeat.zig");
    _ = @import("cluster/cluster_event_handler.zig");
    _ = @import("cluster/cluster_manager.zig");

    // Sender
    _ = @import("sender/sender_event_loop.zig");
    _ = @import("sender/peer_sender.zig");
    _ = @import("sender/send_buffer_pool.zig");
    _ = @import("sender/sender_command.zig");
    _ = @import("sender/write_queue.zig");

    // Receiver
    _ = @import("receiver/peer_receiver.zig");
    _ = @import("receiver/message_router.zig");
    _ = @import("receiver/receiver_event_loop.zig");

    // Transport
    _ = @import("transport/buffer_pool.zig");
    _ = @import("transport/network_io.zig");
    _ = @import("transport/kqueue.zig");
    if (@import("builtin").os.tag == .linux) {
        _ = @import("transport/io_uring.zig");
    }

    // Threading
    _ = @import("threading/command.zig");
    _ = @import("threading/command_queue.zig");
    _ = @import("threading/composite_event_loop.zig");
    _ = @import("threading/threading_mode.zig");
    _ = @import("threading/broker_threads.zig");

    // Monitoring (via common)
    _ = brz_common.monitoring;
}

test "broker module exports canonical broker APIs" {
    _ = BrokerRuntime;
    _ = BrokerApplication;
    _ = BrokerApplicationFactory;
    _ = BrokerApplicationBootstrap;
    _ = ExitCode;
    _ = config.BrokerConfig;
    _ = ConfigLoader;
    _ = ControlLoop;
    _ = ServiceRegistry;
    _ = ClusterManager;
    _ = SenderEventLoop;
    _ = ReceiverEventLoop;
    _ = BrokerThreads;
}
