const std = @import("std");
const testing_mod = @import("ringloom_testing");
const TestHarness = testing_mod.TestHarness;
const readiness = testing_mod.readiness;

test "leader election elects one leader among two instances" {
    // Given
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "leader-election");
    errdefer harness.markFailed();
    defer harness.deinit();

    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    // When — start two leader-aware service instances with the same service name
    const leader_1 = try harness.startService(.{
        .executable_name = "ringloom-test-leader-service",
        .service_name = "leader-svc",
        .leader_election_enabled = true,
        .extra_args = &.{"--instance-tag=A"},
    });
    try harness.waitForServiceReady(leader_1, 5000);

    const leader_2 = try harness.startService(.{
        .executable_name = "ringloom-test-leader-service",
        .service_name = "leader-svc",
        .leader_election_enabled = true,
        .extra_args = &.{"--instance-tag=B"},
    });
    try harness.waitForServiceReady(leader_2, 5000);

    // Then — broker logs that exactly one instance was elected leader
    // The first registered instance wins (first-registered-wins policy)
    try readiness.waitForLogLine(broker, "leader elected", 10000);

    // Verify both services remain alive and stable
    try std.testing.expect(leader_1.isAlive());
    try std.testing.expect(leader_2.isAlive());

    // Cleanup
    try harness.stopProcess(leader_2);
    try harness.stopProcess(leader_1);
    try harness.stopProcess(broker);
}

test "leader election fails over when leader stops" {
    // Given
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "leader-failover");
    errdefer harness.markFailed();
    defer harness.deinit();

    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    // Start two leader-aware instances
    const leader_1 = try harness.startService(.{
        .executable_name = "ringloom-test-leader-service",
        .service_name = "leader-svc",
        .leader_election_enabled = true,
        .extra_args = &.{"--instance-tag=primary"},
    });
    try harness.waitForServiceReady(leader_1, 5000);

    const leader_2 = try harness.startService(.{
        .executable_name = "ringloom-test-leader-service",
        .service_name = "leader-svc",
        .leader_election_enabled = true,
        .extra_args = &.{"--instance-tag=standby"},
    });
    try harness.waitForServiceReady(leader_2, 5000);

    // Wait for initial leader election to settle
    try readiness.waitForLogLine(broker, "leader elected", 10000);

    // When — stop the first instance (the current leader)
    try harness.stopProcess(leader_1);
    _ = try leader_1.waitForExit(5000);

    // Then — broker detects removal and elects the remaining instance as new leader
    // Wait for heartbeat timeout + margin for cleanup and re-election
    try readiness.waitForLogLine(broker, "leader elected", 20000);

    // The surviving instance should still be alive
    try std.testing.expect(leader_2.isAlive());

    // Cleanup
    try harness.stopProcess(leader_2);
    try harness.stopProcess(broker);
}

test "leader election with three instances survives two failures" {
    // Given
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "leader-triple-failover");
    errdefer harness.markFailed();
    defer harness.deinit();

    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    // Start three leader-aware instances
    const instance_a = try harness.startService(.{
        .executable_name = "ringloom-test-leader-service",
        .service_name = "leader-svc",
        .leader_election_enabled = true,
        .extra_args = &.{"--instance-tag=A"},
    });
    try harness.waitForServiceReady(instance_a, 5000);

    const instance_b = try harness.startService(.{
        .executable_name = "ringloom-test-leader-service",
        .service_name = "leader-svc",
        .leader_election_enabled = true,
        .extra_args = &.{"--instance-tag=B"},
    });
    try harness.waitForServiceReady(instance_b, 5000);

    const instance_c = try harness.startService(.{
        .executable_name = "ringloom-test-leader-service",
        .service_name = "leader-svc",
        .leader_election_enabled = true,
        .extra_args = &.{"--instance-tag=C"},
    });
    try harness.waitForServiceReady(instance_c, 5000);

    // Wait for initial election
    try readiness.waitForLogLine(broker, "leader elected", 10000);

    // When — kill the first instance (current leader)
    harness.killProcess(instance_a);

    // Then — second instance becomes leader after heartbeat timeout
    try readiness.waitForLogLine(broker, "leader elected", 20000);

    // When — kill the second instance too
    harness.killProcess(instance_b);

    // Then — third and final instance becomes leader
    try readiness.waitForLogLine(broker, "leader elected", 20000);
    try std.testing.expect(instance_c.isAlive());

    // Cleanup
    try harness.stopProcess(instance_c);
    try harness.stopProcess(broker);
}
