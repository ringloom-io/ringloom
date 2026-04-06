const std = @import("std");
const testing_mod = @import("brz_testing");
const TestHarness = testing_mod.TestHarness;
const readiness = testing_mod.readiness;

test "heartbeat timeout triggers service cleanup" {
    // Given
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "heartbeat-timeout");
    errdefer harness.markFailed();
    defer harness.deinit();

    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    // — start crashy service that will exit abruptly after a delay
    const crashy = try harness.startService(.{
        .executable_name = "brz-test-crashy-service",
        .service_name = "crashy",
    });
    try harness.waitForServiceReady(crashy, 5000);

    // Then — service is alive and registered
    try std.testing.expect(crashy.isAlive());

    // When — wait for the crashy service to exit on its own
    const crashy_exit = try crashy.waitForExit(10000);
    // crashy-service exits with non-zero (simulated crash)
    try std.testing.expect(crashy_exit != 0);

    // Then — broker should detect the dead service via heartbeat timeout
    // Default heartbeat timeout is ~10s; wait heartbeat_timeout + 5s margin
    const heartbeat_cleanup_timeout_ms: u64 = 15000;
    try readiness.waitForLogLine(broker, "service removed", heartbeat_cleanup_timeout_ms);

    // Then — broker is still alive and healthy after cleanup
    try std.testing.expect(broker.isAlive());

    // Cleanup
    try harness.stopProcess(broker);
    const broker_exit = try broker.waitForExit(5000);
    try std.testing.expectEqual(@as(u32, 0), broker_exit);
}

test "heartbeat timeout does not affect healthy services" {
    // Given
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "heartbeat-healthy");
    errdefer harness.markFailed();
    defer harness.deinit();

    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    // — start a healthy echo service alongside a crashy one
    const echo = try harness.startService(.{
        .executable_name = "brz-test-echo-service",
        .service_name = "echo",
    });
    try harness.waitForServiceReady(echo, 5000);

    const crashy = try harness.startService(.{
        .executable_name = "brz-test-crashy-service",
        .service_name = "crashy",
    });
    try harness.waitForServiceReady(crashy, 5000);

    // When — crashy exits abruptly
    _ = try crashy.waitForExit(10000);

    // Then — wait for broker to clean up the crashed service
    const heartbeat_cleanup_timeout_ms: u64 = 15000;
    try readiness.waitForLogLine(broker, "service removed", heartbeat_cleanup_timeout_ms);

    // Then — echo service is still alive and unaffected
    try std.testing.expect(echo.isAlive());
    try std.testing.expect(broker.isAlive());

    // Cleanup
    try harness.stopProcess(echo);
    try harness.stopProcess(broker);
}
