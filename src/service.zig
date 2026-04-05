//! Service layer module for the BRZ broker.
//!
//! This is the single import point for service-related functionality.
//! It re-exports the BrzEngine, ServiceClient, ServiceClientRegistry,
//! and related types.

pub const brz_engine = @import("service/brz_engine.zig");
pub const BrzEngine = brz_engine.BrzEngine;
pub const ServiceConfig = brz_engine.ServiceConfig;

pub const message_consumer = @import("service/message_consumer.zig");
pub const MessageConsumer = message_consumer.MessageConsumer;

pub const control_agent = @import("service/control_agent.zig");
pub const ControlAgent = control_agent.ControlAgent;

pub const service_client = @import("service/service_client.zig");
pub const ServiceClient = service_client.ServiceClient;

pub const service_client_registry = @import("service/service_client_registry.zig");
pub const ServiceClientRegistry = service_client_registry.ServiceClientRegistry;

pub const service_instance = @import("service/service_instance.zig");
pub const ServiceInstance = service_instance.ServiceInstance;

pub const load_balancer = @import("service/load_balancer.zig");
pub const ClientLoadBalancer = load_balancer.ClientLoadBalancer;

// Ensure all service module tests are discovered by `zig build test`.
comptime {
    _ = @import("service/brz_engine.zig");
    _ = @import("service/message_consumer.zig");
    _ = @import("service/control_agent.zig");
    _ = @import("service/service_client.zig");
    _ = @import("service/service_client_registry.zig");
    _ = @import("service/service_instance.zig");
    _ = @import("service/load_balancer.zig");
    _ = @import("service/service_client_test.zig");
}
