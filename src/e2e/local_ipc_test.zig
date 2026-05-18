const std = @import("std");
const testing_mod = @import("ringloom_testing");
const TestHarness = testing_mod.TestHarness;

test "two local services communicate via direct IPC" {
    // Given
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "local-ipc");
    defer harness.deinit();
    errdefer harness.markFailed();

    // Given — broker is running
    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    // Given — echo service is running and registered
    const echo = try harness.startService(.{
        .executable_name = "ringloom-test-echo-service",
        .service_name = "echo",
    });
    try harness.waitForServiceReady(echo, 5000);

    // When — start ping service that sends 10 messages to echo
    const ping = try harness.startService(.{
        .executable_name = "ringloom-test-ping-service",
        .service_name = "ping",
        .extra_args = &.{ "--target-service", "echo", "--message-count", "10" },
    });
    try harness.waitForServiceReady(ping, 5000);

    // Then — ping completes successfully after sending all messages
    const exit_code = try ping.waitForExit(15000);
    try std.testing.expectEqual(@as(u32, 0), exit_code);

    // Then — echo service is still alive after handling all messages
    try std.testing.expect(echo.isAlive());

    // Cleanup — stop services first, then broker
    try harness.stopProcess(echo);
    try harness.stopProcess(broker);
}

test "multiple services communicate via local IPC concurrently" {
    // Given
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "local-ipc-concurrent");
    defer harness.deinit();
    errdefer harness.markFailed();

    // Given — broker is running
    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    // Given — echo service is running
    const echo = try harness.startService(.{
        .executable_name = "ringloom-test-echo-service",
        .service_name = "echo",
    });
    try harness.waitForServiceReady(echo, 5000);

    // When — start two ping services concurrently, both targeting echo
    const ping_a = try harness.startService(.{
        .executable_name = "ringloom-test-ping-service",
        .service_name = "ping-a",
        .extra_args = &.{ "--target-service", "echo", "--message-count", "5" },
    });
    try harness.waitForServiceReady(ping_a, 5000);

    const ping_b = try harness.startService(.{
        .executable_name = "ringloom-test-ping-service",
        .service_name = "ping-b",
        .extra_args = &.{ "--target-service", "echo", "--message-count", "5" },
    });
    try harness.waitForServiceReady(ping_b, 5000);

    // Then — both ping services complete successfully
    const exit_a = try ping_a.waitForExit(15000);
    const exit_b = try ping_b.waitForExit(15000);
    try std.testing.expectEqual(@as(u32, 0), exit_a);
    try std.testing.expectEqual(@as(u32, 0), exit_b);

    // Then — echo service survived concurrent load
    try std.testing.expect(echo.isAlive());

    // Cleanup
    try harness.stopProcess(echo);
    try harness.stopProcess(broker);
}

test "local IPC with forwarder chain" {
    // Given
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "local-ipc-chain");
    defer harness.deinit();
    errdefer harness.markFailed();

    // Given — broker is running
    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    // Given — echo service is running at the end of the chain
    const echo = try harness.startService(.{
        .executable_name = "ringloom-test-echo-service",
        .service_name = "echo",
    });
    try harness.waitForServiceReady(echo, 5000);

    // Given — forwarder sits between ping and echo
    const forwarder = try harness.startService(.{
        .executable_name = "ringloom-test-forwarder-service",
        .service_name = "forwarder",
        .extra_args = &.{ "--forward-to", "echo" },
    });
    try harness.waitForServiceReady(forwarder, 5000);

    // When — ping sends messages through forwarder to echo
    const ping = try harness.startService(.{
        .executable_name = "ringloom-test-ping-service",
        .service_name = "ping",
        .extra_args = &.{ "--target-service", "forwarder", "--message-count", "5" },
    });
    try harness.waitForServiceReady(ping, 5000);

    // Then — ping completes end-to-end through the chain
    const exit_code = try ping.waitForExit(20000);
    try std.testing.expectEqual(@as(u32, 0), exit_code);

    // Then — all intermediate services are still alive
    try std.testing.expect(forwarder.isAlive());
    try std.testing.expect(echo.isAlive());

    // Cleanup — reverse order of the chain
    try harness.stopProcess(forwarder);
    try harness.stopProcess(echo);
    try harness.stopProcess(broker);
}
