//! Unit tests for ServiceClient and load balancing.

const std = @import("std");
const testing = std.testing;
const ServiceClient = @import("service_client.zig").ServiceClient;
const ServiceInstance = @import("service_instance.zig").ServiceInstance;

test "round-robin load balancer cycles through instances" {
    // Given: a ServiceClient with 3 instances.
    var client = ServiceClient.init(
        testing.allocator,
        "test-service",
        null, // broker_meta not needed for this test
        1,
        100,
    );
    defer client.deinit();

    try client.addInstance(.{ .service_id = 1, .service_name = "test-service", .node_id = 1 });
    try client.addInstance(.{ .service_id = 2, .service_name = "test-service", .node_id = 1 });
    try client.addInstance(.{ .service_id = 3, .service_name = "test-service", .node_id = 1 });

    // When/Then: next() cycles through instances in order.
    const first = client.balancer.next(client.instances.items);
    try testing.expect(first != null);
    try testing.expectEqual(@as(i32, 1), first.?.service_id);

    const second = client.balancer.next(client.instances.items);
    try testing.expectEqual(@as(i32, 2), second.?.service_id);

    const third = client.balancer.next(client.instances.items);
    try testing.expectEqual(@as(i32, 3), third.?.service_id);

    // Wraps around.
    const fourth = client.balancer.next(client.instances.items);
    try testing.expectEqual(@as(i32, 1), fourth.?.service_id);
}

test "round-robin returns null for empty instance list" {
    // Given: a ServiceClient with no instances.
    var client = ServiceClient.init(
        testing.allocator,
        "empty-service",
        null,
        1,
        100,
    );
    defer client.deinit();

    // When/Then: next() returns null.
    const result = client.balancer.next(client.instances.items);
    try testing.expect(result == null);
}

test "ServiceClient updateLeader sets correct instance" {
    // Given: a ServiceClient with 3 instances, none are leader.
    var client = ServiceClient.init(
        testing.allocator,
        "leader-test",
        null,
        1,
        100,
    );
    defer client.deinit();

    try client.addInstance(.{ .service_id = 1, .service_name = "leader-test", .node_id = 1 });
    try client.addInstance(.{ .service_id = 2, .service_name = "leader-test", .node_id = 1 });
    try client.addInstance(.{ .service_id = 3, .service_name = "leader-test", .node_id = 2 });

    // When: leader is updated to instance 2.
    client.updateLeader(2);

    // Then: only instance 2 is leader.
    try testing.expect(!client.instances.items[0].is_leader);
    try testing.expect(client.instances.items[1].is_leader);
    try testing.expect(!client.instances.items[2].is_leader);
}

test "ServiceClient removeInstance removes correct instance" {
    // Given: a ServiceClient with 2 instances.
    var client = ServiceClient.init(
        testing.allocator,
        "remove-test",
        null,
        1,
        100,
    );
    defer client.deinit();

    try client.addInstance(.{ .service_id = 1, .service_name = "remove-test", .node_id = 1 });
    try client.addInstance(.{ .service_id = 2, .service_name = "remove-test", .node_id = 1 });

    try testing.expectEqual(@as(usize, 2), client.instanceCount());

    // When: remove instance 1.
    client.removeInstance(1);

    // Then: only instance 2 remains.
    try testing.expectEqual(@as(usize, 1), client.instanceCount());
    try testing.expectEqual(@as(i32, 2), client.instances.items[0].service_id);
}
