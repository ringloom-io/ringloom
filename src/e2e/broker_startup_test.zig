const std = @import("std");
const testing_mod = @import("brz_testing");
const TestHarness = testing_mod.TestHarness;

test "broker starts and shuts down cleanly" {
    // Given
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "broker-startup");
    errdefer harness.markFailed();
    defer harness.deinit();

    // When — start broker with default spec
    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    // Then — broker process is alive
    try std.testing.expect(broker.isAlive());

    // When — stop broker gracefully
    try harness.stopProcess(broker);
    const exit_code = try broker.waitForExit(5000);

    // Then — clean exit with code 0
    try std.testing.expectEqual(@as(u32, 0), exit_code);
}

test "broker rejects invalid node id zero" {
    // Given
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "broker-invalid-node-id");
    errdefer harness.markFailed();
    defer harness.deinit();

    // When — start broker with node_id = 0 (invalid)
    const broker = try harness.startBroker(.{
        .node_id = 0,
    });

    // Then — broker should exit with non-zero code
    const exit_code = try broker.waitForExit(5000);
    try std.testing.expect(exit_code != 0);
}

test "broker creates metadata directory on startup" {
    // Given
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "broker-metadata-dir");
    errdefer harness.markFailed();
    defer harness.deinit();

    // When — start broker
    const broker = try harness.startBroker(.{
        .group_name = "broker-meta-test",
    });
    try harness.waitForBrokerReady(broker, 5000);

    // Then — metadata directory exists on the filesystem
    try testing_mod.readiness.waitForFileExists("/dev/shm/broker-meta-test/services", 5000);

    // Cleanup
    try harness.stopProcess(broker);
    const exit_code = try broker.waitForExit(5000);
    try std.testing.expectEqual(@as(u32, 0), exit_code);
}

test "broker handles SIGTERM gracefully" {
    // Given
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "broker-sigterm");
    errdefer harness.markFailed();
    defer harness.deinit();

    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);
    try std.testing.expect(broker.isAlive());

    // When — kill broker with SIGTERM (stopProcess sends SIGTERM)
    try harness.stopProcess(broker);

    // Then — broker exits cleanly within timeout
    const exit_code = try broker.waitForExit(5000);
    try std.testing.expectEqual(@as(u32, 0), exit_code);
    try std.testing.expect(!broker.isAlive());
}
