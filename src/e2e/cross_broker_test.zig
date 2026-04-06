const std = @import("std");
const testing_mod = @import("brz_testing");
const TestHarness = testing_mod.TestHarness;

test "cross-broker routing works" {
    // Given — two brokers peered together
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "cross-broker");
    errdefer harness.markFailed();
    defer harness.deinit();

    const broker_a = try harness.startBroker(.{
        .node_id = 1,
        .port = 19001,
        .peers = &.{.{ .node_id = 2, .host = "127.0.0.1", .port = 19002 }},
    });
    try harness.waitForBrokerReady(broker_a, 5000);

    const broker_b = try harness.startBroker(.{
        .node_id = 2,
        .port = 19002,
        .peers = &.{.{ .node_id = 1, .host = "127.0.0.1", .port = 19001 }},
    });
    try harness.waitForBrokerReady(broker_b, 5000);

    // Wait for cluster convergence between the two brokers
    std.Thread.sleep(2 * std.time.ns_per_s);

    // When — echo on broker B, ping on broker A (forces cross-broker routing)
    const echo = try harness.startService(.{
        .executable_name = "brz-test-echo-service",
        .service_name = "echo",
        .broker_node_id = 2,
    });
    try harness.waitForServiceReady(echo, 5000);

    const ping = try harness.startService(.{
        .executable_name = "brz-test-ping-service",
        .service_name = "ping",
        .broker_node_id = 1,
        .extra_args = &.{ "--target-service", "echo", "--message-count", "5" },
    });
    try harness.waitForServiceReady(ping, 5000);

    // Then — ping completes successfully, meaning messages were routed across brokers
    const exit_code = try ping.waitForExit(15000);
    try std.testing.expectEqual(@as(u32, 0), exit_code);

    // Cleanup — services first, then brokers in reverse order
    try harness.stopProcess(echo);
    try harness.stopProcess(broker_b);
    try harness.stopProcess(broker_a);
}

test "cross-broker routing with late broker join" {
    // Given — broker A starts alone
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "cross-broker-late-join");
    errdefer harness.markFailed();
    defer harness.deinit();

    const broker_a = try harness.startBroker(.{
        .node_id = 1,
        .port = 19011,
        .peers = &.{.{ .node_id = 2, .host = "127.0.0.1", .port = 19012 }},
    });
    try harness.waitForBrokerReady(broker_a, 5000);

    // Echo registers on broker A
    const echo = try harness.startService(.{
        .executable_name = "brz-test-echo-service",
        .service_name = "echo",
        .broker_node_id = 1,
    });
    try harness.waitForServiceReady(echo, 5000);

    // When — broker B joins late
    const broker_b = try harness.startBroker(.{
        .node_id = 2,
        .port = 19012,
        .peers = &.{.{ .node_id = 1, .host = "127.0.0.1", .port = 19011 }},
    });
    try harness.waitForBrokerReady(broker_b, 5000);

    // Wait for cluster convergence and service discovery propagation
    std.Thread.sleep(3 * std.time.ns_per_s);

    // Ping on broker B should discover echo on broker A
    const ping = try harness.startService(.{
        .executable_name = "brz-test-ping-service",
        .service_name = "ping",
        .broker_node_id = 2,
        .extra_args = &.{ "--target-service", "echo", "--message-count", "5" },
    });
    try harness.waitForServiceReady(ping, 5000);

    // Then — messages route from broker B → broker A successfully
    const exit_code = try ping.waitForExit(15000);
    try std.testing.expectEqual(@as(u32, 0), exit_code);

    // Cleanup
    try harness.stopProcess(echo);
    try harness.stopProcess(broker_b);
    try harness.stopProcess(broker_a);
}

test "cross-broker routing with three brokers" {
    // Given — three brokers in a fully-meshed cluster
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "cross-broker-three");
    errdefer harness.markFailed();
    defer harness.deinit();

    const broker_a = try harness.startBroker(.{
        .node_id = 1,
        .port = 19021,
        .peers = &.{
            .{ .node_id = 2, .host = "127.0.0.1", .port = 19022 },
            .{ .node_id = 3, .host = "127.0.0.1", .port = 19023 },
        },
    });
    try harness.waitForBrokerReady(broker_a, 5000);

    const broker_b = try harness.startBroker(.{
        .node_id = 2,
        .port = 19022,
        .peers = &.{
            .{ .node_id = 1, .host = "127.0.0.1", .port = 19021 },
            .{ .node_id = 3, .host = "127.0.0.1", .port = 19023 },
        },
    });
    try harness.waitForBrokerReady(broker_b, 5000);

    const broker_c = try harness.startBroker(.{
        .node_id = 3,
        .port = 19023,
        .peers = &.{
            .{ .node_id = 1, .host = "127.0.0.1", .port = 19021 },
            .{ .node_id = 2, .host = "127.0.0.1", .port = 19022 },
        },
    });
    try harness.waitForBrokerReady(broker_c, 5000);

    // Wait for full cluster convergence
    std.Thread.sleep(3 * std.time.ns_per_s);

    // When — echo on broker C, ping on broker A (two hops possible)
    const echo = try harness.startService(.{
        .executable_name = "brz-test-echo-service",
        .service_name = "echo",
        .broker_node_id = 3,
    });
    try harness.waitForServiceReady(echo, 5000);

    const ping = try harness.startService(.{
        .executable_name = "brz-test-ping-service",
        .service_name = "ping",
        .broker_node_id = 1,
        .extra_args = &.{ "--target-service", "echo", "--message-count", "5" },
    });
    try harness.waitForServiceReady(ping, 5000);

    // Then — routing across three-node cluster succeeds
    const exit_code = try ping.waitForExit(20000);
    try std.testing.expectEqual(@as(u32, 0), exit_code);

    // Cleanup — services, then brokers in reverse
    try harness.stopProcess(echo);
    try harness.stopProcess(broker_c);
    try harness.stopProcess(broker_b);
    try harness.stopProcess(broker_a);
}
