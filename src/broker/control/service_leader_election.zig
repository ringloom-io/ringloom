//! Service Leader Election — determines which instance of a service is the
//! leader across the cluster.
//!
//! Election rule: lowest serviceId wins (effectively first-registered wins,
//! since service IDs are monotonically incremented).
//!
//! Only the broker cluster leader performs leader designations. Non-leader
//! brokers track the leader state they receive from the cluster leader but
//! never initiate leader changes.

const std = @import("std");
const ServiceRegistry = @import("service_registry.zig").ServiceRegistry;
const ServiceInstance = @import("service_registry.zig").ServiceInstance;
const msg = @import("control_messages.zig");
const constants = @import("brz_common").platform.constants;
const log = std.log.scoped(.leader_election);

pub const ServiceLeaderElection = struct {
    const Self = @This();

    pub fn init() Self {
        return .{};
    }

    /// Evaluate the leader for a service name. Returns true if the
    /// designated leader is on `local_node_id`.
    ///
    /// Side effects:
    ///   - Updates `registry.service_leaders` if the leader changed.
    ///
    /// The caller is responsible for broadcasting LeaderChanged and
    /// ServiceLeaderDesignated messages after this returns.
    pub fn evaluate(
        self: *Self,
        registry: *ServiceRegistry,
        service_name: []const u8,
        local_node_id: u8,
        local_candidate_id: i32,
    ) bool {
        _ = self;
        _ = local_candidate_id;

        // Collect all instances of this service name.
        var instance_buf: [constants.default_max_services]ServiceInstance = undefined;
        const count = registry.getInstancesByNameBuf(service_name, &instance_buf);

        if (count == 0) {
            // No instances — clear the leader.
            registry.removeLeader(service_name);
            return false;
        }

        // Find the instance with the lowest serviceId.
        var leader = instance_buf[0];
        for (instance_buf[1..count]) |inst| {
            if (inst.service_id < leader.service_id) {
                leader = inst;
            }
        }

        // Check if the leader changed.
        const prev_leader = registry.getLeader(service_name);
        const leader_changed = (prev_leader == null or prev_leader.? != leader.service_id);

        if (leader_changed) {
            log.info("leader elected: service_name={s}, leader_id={}, leader_node={}", .{
                service_name,
                leader.service_id,
                leader.node_id,
            });
            registry.setLeader(service_name, leader.service_id);
        }

        return leader.node_id == local_node_id;
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "leader election selects lowest serviceId" {
    // Given
    var registry = ServiceRegistry.init(testing.allocator);
    defer registry.deinit();

    try registry.register(.{ .service_id = 10, .node_id = 1, .service_name = "svc", .leader_election_enabled = true, .is_local = true });
    try registry.register(.{ .service_id = 3, .node_id = 2, .service_name = "svc", .leader_election_enabled = true, .is_local = false });
    try registry.register(.{ .service_id = 7, .node_id = 1, .service_name = "svc", .leader_election_enabled = true, .is_local = true });

    var election = ServiceLeaderElection.init();

    // When
    _ = election.evaluate(&registry, "svc", 1, 10);

    // Then
    const leader = registry.getLeader("svc");
    try testing.expect(leader != null);
    try testing.expectEqual(@as(i32, 3), leader.?);
}

test "leader election with single instance" {
    // Given
    var registry = ServiceRegistry.init(testing.allocator);
    defer registry.deinit();

    try registry.register(.{ .service_id = 5, .node_id = 1, .service_name = "svc", .leader_election_enabled = true, .is_local = true });

    var election = ServiceLeaderElection.init();

    // When
    const is_local_leader = election.evaluate(&registry, "svc", 1, 5);

    // Then
    try testing.expect(is_local_leader);
    try testing.expectEqual(@as(i32, 5), registry.getLeader("svc").?);
}

test "leader election with no instances clears leader" {
    // Given
    var registry = ServiceRegistry.init(testing.allocator);
    defer registry.deinit();

    registry.setLeader("svc", 5); // pre-existing leader

    var election = ServiceLeaderElection.init();

    // When
    const is_local_leader = election.evaluate(&registry, "svc", 1, -1);

    // Then
    try testing.expect(!is_local_leader);
    try testing.expect(registry.getLeader("svc") == null);
}
