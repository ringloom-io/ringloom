//! BRZ Service — service-side runtime library.
//!
//! This is the root source file for the `brz_service` library module.
//! It re-exports the service engine, client types, control agent,
//! message consumer, load balancer, IPC wrappers, and service configuration.
//!
//! Consumers should import this root rather than deep internal files.

// ── Service engine and lifecycle ─────────────────────────────────────

pub const brz_engine = @import("brz_engine.zig");
pub const BrzEngine = brz_engine.BrzEngine;
pub const ServiceConfig = brz_engine.ServiceConfig;

// ── Message consumption ──────────────────────────────────────────────

pub const message_consumer = @import("message_consumer.zig");
pub const MessageConsumer = message_consumer.MessageConsumer;

// ── Control agent ────────────────────────────────────────────────────

pub const control_agent = @import("control_agent.zig");
pub const ControlAgent = control_agent.ControlAgent;

// ── Service clients and discovery ────────────────────────────────────

pub const service_client = @import("service_client.zig");
pub const ServiceClient = service_client.ServiceClient;

pub const service_client_registry = @import("service_client_registry.zig");
pub const ServiceClientRegistry = service_client_registry.ServiceClientRegistry;

pub const service_instance = @import("service_instance.zig");
pub const ServiceInstance = service_instance.ServiceInstance;

pub const load_balancer = @import("load_balancer.zig");
pub const ClientLoadBalancer = load_balancer.ClientLoadBalancer;

pub const flow_control_config = @import("flow_control_config.zig");
pub const FlowControlConfig = flow_control_config.FlowControlConfig;
pub const BackpressureStrategy = flow_control_config.BackpressureStrategy;

// ── IPC (same-host shared-memory transport) ──────────────────────────

pub const ipc = struct {
    pub const ipc_producer = @import("ipc/ipc_producer.zig");
    pub const IpcProducer = ipc_producer.IpcProducer;

    pub const ipc_consumer = @import("ipc/ipc_consumer.zig");
    pub const IpcConsumer = ipc_consumer.IpcConsumer;
    pub const MessageHandler = ipc_consumer.MessageHandler;
};

pub const IpcProducer = ipc.IpcProducer;
pub const IpcConsumer = ipc.IpcConsumer;

// ── Service configuration ────────────────────────────────────────────

pub const config = struct {
    pub const service_config = @import("config/service_config.zig");
    pub const ServiceConfigType = service_config.ServiceConfig;
};

// ── Test discovery ───────────────────────────────────────────────────
// Ensure all service module tests are discovered by `zig build test`.

comptime {
    // Service engine and agents
    _ = @import("brz_engine.zig");
    _ = @import("message_consumer.zig");
    _ = @import("control_agent.zig");

    // Client types
    _ = @import("service_client.zig");
    _ = @import("service_client_registry.zig");
    _ = @import("service_instance.zig");
    _ = @import("load_balancer.zig");
    _ = @import("service_client_test.zig");
    _ = @import("flow_control_config.zig");

    // IPC
    _ = @import("ipc/ipc_producer.zig");
    _ = @import("ipc/ipc_consumer.zig");
    _ = @import("ipc/ipc_test.zig");

    // Configuration
    _ = @import("config/service_config.zig");
}

test "service module exports canonical service APIs" {
    _ = BrzEngine;
    _ = ServiceConfig;
    _ = MessageConsumer;
    _ = ControlAgent;
    _ = ServiceClient;
    _ = ServiceClientRegistry;
    _ = ServiceInstance;
    _ = ClientLoadBalancer;
    _ = IpcProducer;
    _ = IpcConsumer;
}
