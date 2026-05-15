//! Unit tests for ServiceClient and load balancing.

const std = @import("std");
const testing = std.testing;
const ServiceClient = @import("service_client.zig").ServiceClient;
const ServiceInstance = @import("service_instance.zig").ServiceInstance;
const ringloom_common = @import("ringloom_common");
const BrokerMetadataFile = ringloom_common.memory.BrokerMetadataFile;
const RingBuffer = ringloom_common.concurrent.RingBuffer;
const FlowControlConfig = @import("flow_control_config.zig").FlowControlConfig;

test "round-robin load balancer cycles through instances" {
    // Given: a ServiceClient with 3 instances.
    var client = ServiceClient.init(
        testing.allocator,
        "test-service",
        null, // broker_meta not needed for this test
        1,
        100,
        null,
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
        null,
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
        null,
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
        null,
    );
    defer client.deinit();

    try client.addInstance(.{ .service_id = 1, .service_name = "remove-test", .node_id = 1 });
    try client.addInstance(.{ .service_id = 2, .service_name = "remove-test", .node_id = 1 });

    try testing.expectEqual(@as(usize, 2), client.instanceCount());

    // When: remove instance 1.
    client.removeInstance(1, 1);

    // Then: only instance 2 remains.
    try testing.expectEqual(@as(usize, 1), client.instanceCount());
    try testing.expectEqual(@as(i32, 2), client.instances.items[0].service_id);
}

test "ServiceClient copyTargetInstances returns node and service ids with leader flags" {
    var client = ServiceClient.init(
        testing.allocator,
        "target-list-test",
        null,
        1,
        100,
        null,
    );
    defer client.deinit();

    try client.addInstance(.{
        .service_id = 11,
        .service_name = "target-list-test",
        .node_id = 1,
        .is_leader = false,
    });
    try client.addInstance(.{
        .service_id = 12,
        .service_name = "target-list-test",
        .node_id = 2,
        .is_leader = true,
    });

    var targets: [1]ServiceClient.TargetInstanceInfo = undefined;
    const total = client.copyTargetInstances(targets[0..]);

    try testing.expectEqual(@as(usize, 2), total);
    try testing.expectEqual(@as(i32, 11), targets[0].target_service_id);
    try testing.expectEqual(@as(i16, 1), targets[0].target_node_id);
    try testing.expect(!targets[0].is_leader);

    var all_targets: [2]ServiceClient.TargetInstanceInfo = undefined;
    const all_total = client.copyTargetInstances(all_targets[0..]);

    try testing.expectEqual(@as(usize, 2), all_total);
    try testing.expectEqual(@as(i32, 11), all_targets[0].target_service_id);
    try testing.expectEqual(@as(i16, 1), all_targets[0].target_node_id);
    try testing.expect(!all_targets[0].is_leader);
    try testing.expectEqual(@as(i32, 12), all_targets[1].target_service_id);
    try testing.expectEqual(@as(i16, 2), all_targets[1].target_node_id);
    try testing.expect(all_targets[1].is_leader);
}

test "ServiceClient tracks duplicate service ids on different nodes" {
    var client = ServiceClient.init(
        testing.allocator,
        "duplicate-id-test",
        null,
        1,
        100,
        null,
    );
    defer client.deinit();

    try client.addInstance(.{ .service_id = 11, .service_name = "duplicate-id-test", .node_id = 1 });
    try client.addInstance(.{ .service_id = 11, .service_name = "duplicate-id-test", .node_id = 2 });

    try testing.expect(client.findInstance(1, 11) != null);
    try testing.expect(client.findInstance(2, 11) != null);

    client.removeInstance(1, 11);

    try testing.expect(client.findInstance(1, 11) == null);
    try testing.expect(client.findInstance(2, 11) != null);
}

test "ServiceClient remote sends use distinct destination buffers" {
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(storage_path);
    try tmp_dir.dir.createDirPath(testing.io, "test-group/services");

    var broker = try BrokerMetadataFile.create(storage_path, "test-group", 1, 4096, 4096);
    defer broker.close();

    var client = ServiceClient.init(testing.allocator, "remote-test", &broker, 1, 100, null);
    defer client.deinit();
    try client.addInstance(.{ .service_id = 7, .service_name = "remote-test", .node_id = 2 });
    try client.addInstance(.{ .service_id = 8, .service_name = "remote-test", .node_id = 2 });

    try client.sendToMessage(2, 7, 42, "alpha");
    try client.sendToMessage(2, 8, 43, "bravo");

    const directory = broker.getSendBufferDirectory();
    const handle_a = directory.findByDestination(2, 7).?;
    const handle_b = directory.findByDestination(2, 8).?;
    try testing.expect(handle_a.index != handle_b.index);

    var ring_a = try RingBuffer.init(try directory.ringSliceForHandle(broker.mapped_bytes, handle_a), false, null, null);
    var ring_b = try RingBuffer.init(try directory.ringSliceForHandle(broker.mapped_bytes, handle_b), false, null, null);
    try testing.expect(ring_a.size() > 0);
    try testing.expect(ring_b.size() > 0);
}

test "ServiceClient full destination buffer does not block another destination" {
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(storage_path);
    try tmp_dir.dir.createDirPath(testing.io, "test-group/services");

    var broker = try BrokerMetadataFile.create(storage_path, "test-group", 1, 4096, 1024);
    defer broker.close();

    var client = ServiceClient.init(testing.allocator, "remote-test", &broker, 1, 100, null);
    defer client.deinit();
    try client.addInstance(.{ .service_id = 7, .service_name = "remote-test", .node_id = 2 });
    try client.addInstance(.{ .service_id = 8, .service_name = "remote-test", .node_id = 2 });

    const payload = [_]u8{0xaa} ** 64;
    var filled_a = false;
    for (0..64) |_| {
        client.sendToMessage(2, 7, 42, &payload) catch |err| switch (err) {
            error.SendBufferFull => {
                filled_a = true;
                break;
            },
            else => return err,
        };
    }

    try testing.expect(filled_a);

    try client.sendToMessage(2, 8, 43, "still-progresses");
    const handle_b = broker.getSendBufferDirectory().findByDestination(2, 8).?;
    var ring_b = try RingBuffer.init(
        try broker.getSendBufferDirectory().ringSliceForHandle(broker.mapped_bytes, handle_b),
        false,
        null,
        null,
    );
    try testing.expect(ring_b.size() > 0);
}

test "ServiceClient maps destination pressure states to send errors" {
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(storage_path);
    try tmp_dir.dir.createDirPath(testing.io, "test-group/services");

    var broker = try BrokerMetadataFile.create(storage_path, "test-group", 1, 4096, 4096);
    defer broker.close();

    const fc_config = FlowControlConfig{
        .enabled = true,
        .strategy = .drop,
        .check_peer_connectivity = false,
    };
    var client = ServiceClient.initWithFlowControl(
        testing.allocator,
        "remote-test",
        &broker,
        1,
        100,
        fc_config,
        null,
        null,
        null,
    );
    defer client.deinit();
    try client.addInstance(.{ .service_id = 7, .service_name = "remote-test", .node_id = 2 });

    const handle = try broker.findOrAllocateSendBuffer(2, 7);
    const entry = &broker.getMutableSendBufferDirectory().entries[handle.index];

    entry.storePressureState(.flow_blocked);
    try testing.expectError(error.BackPressure, client.sendToMessage(2, 7, 42, "blocked"));

    entry.storePressureState(.congested);
    try testing.expectError(error.PeerCongested, client.sendToMessage(2, 7, 42, "congested"));

    entry.storePressureState(.term_blocked);
    try testing.expectError(error.BackPressure, client.sendToMessage(2, 7, 42, "term"));

    entry.storePressureState(.peer_down);
    try testing.expectError(error.PeerDisconnected, client.sendToMessage(2, 7, 42, "down"));

    entry.storePressureState(.draining);
    try testing.expectError(error.SendBufferFull, client.sendToMessage(2, 7, 42, "draining"));
}
