//! Load balancing strategies for ServiceClient.
//!
//! Currently implements round-robin; the union(enum) is extensible
//! for future strategies (random, leader-only, weighted).

const std = @import("std");
const ServiceInstance = @import("service_instance.zig").ServiceInstance;

pub const ClientLoadBalancer = union(enum) {
    round_robin: RoundRobinBalancer,

    pub fn next(self: *ClientLoadBalancer, instances: []ServiceInstance) ?*ServiceInstance {
        return switch (self.*) {
            .round_robin => |*rb| rb.next(instances),
        };
    }
};

pub const RoundRobinBalancer = struct {
    index: usize = 0,

    /// Returns the next instance in round-robin order.
    /// Uses wrapping arithmetic to avoid overflow.
    pub fn next(self: *RoundRobinBalancer, instances: []ServiceInstance) ?*ServiceInstance {
        if (instances.len == 0) return null;
        const instance = &instances[self.index % instances.len];
        self.index +%= 1;
        return instance;
    }

    /// Reset the round-robin counter.
    pub fn reset(self: *RoundRobinBalancer) void {
        self.index = 0;
    }
};
