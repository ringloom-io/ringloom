const std = @import("std");
const testing_mod = @import("ringloom_testing");
const TestHarness = testing_mod.TestHarness;

test "single service registers with broker" {
    // Given
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "registration");
    defer harness.deinit();
    errdefer harness.markFailed();

    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    // When — start echo service
    const echo = try harness.startService(.{
        .executable_name = "ringloom-test-echo-service",
        .service_name = "echo",
    });
    try harness.waitForServiceReady(echo, 5000);

    // Then — service is alive and registered
    try std.testing.expect(echo.isAlive());

    // Cleanup
    try harness.stopProcess(echo);
    try harness.stopProcess(broker);
}

test "multiple services register with broker" {
    // Given
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "registration-multi");
    defer harness.deinit();
    errdefer harness.markFailed();

    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    // When — start multiple services
    const echo = try harness.startService(.{
        .executable_name = "ringloom-test-echo-service",
        .service_name = "echo",
    });
    try harness.waitForServiceReady(echo, 5000);

    const ping = try harness.startService(.{
        .executable_name = "ringloom-test-ping-service",
        .service_name = "ping",
        .extra_args = &.{ "--target-service", "echo", "--message-count", "0" },
    });
    try harness.waitForServiceReady(ping, 5000);

    const forwarder = try harness.startService(.{
        .executable_name = "ringloom-test-forwarder-service",
        .service_name = "forwarder",
    });
    try harness.waitForServiceReady(forwarder, 5000);

    // Then — all services are alive
    try std.testing.expect(echo.isAlive());
    try std.testing.expect(ping.isAlive());
    try std.testing.expect(forwarder.isAlive());

    // Cleanup — stop services before broker
    try harness.stopProcess(forwarder);
    try harness.stopProcess(ping);
    try harness.stopProcess(echo);
    try harness.stopProcess(broker);
}

test "service registers and gets unique service id" {
    // Given
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "registration-unique-id");
    defer harness.deinit();
    errdefer harness.markFailed();

    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    // When — start two instances of the same service name
    const echo1 = try harness.startService(.{
        .executable_name = "ringloom-test-echo-service",
        .service_name = "echo",
    });
    try harness.waitForServiceReady(echo1, 5000);

    const echo2 = try harness.startService(.{
        .executable_name = "ringloom-test-echo-service",
        .service_name = "echo",
    });
    try harness.waitForServiceReady(echo2, 5000);

    // Then — both instances are alive (broker assigned distinct IDs)
    try std.testing.expect(echo1.isAlive());
    try std.testing.expect(echo2.isAlive());

    // Cleanup
    try harness.stopProcess(echo2);
    try harness.stopProcess(echo1);
    try harness.stopProcess(broker);
}
