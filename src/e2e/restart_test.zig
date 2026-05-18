const std = @import("std");
const testing_mod = @import("ringloom_testing");
const TestHarness = testing_mod.TestHarness;
const readiness = testing_mod.readiness;

test "service restart reuses metadata after cleanup" {
    // Given
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "restart");
    defer harness.deinit();
    errdefer harness.markFailed();

    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    // Start a crashy service that will exit abruptly after a delay
    const crashy = try harness.startService(.{
        .executable_name = "ringloom-test-crashy-service",
        .service_name = "ephemeral",
    });
    try harness.waitForServiceReady(crashy, 5000);

    // Then — crashy is initially alive and registered
    try std.testing.expect(crashy.isAlive());

    // When — kill the crashy service (simulating an unexpected crash)
    harness.killProcess(crashy);

    // Then — wait for the broker to detect the dead service via heartbeat timeout
    // Default heartbeat timeout is 10s, plus margin for cleanup processing
    try readiness.waitForLogLine(broker, "service removed", 20000);

    // When — start a new echo service with the same service name
    // The broker should allow re-registration; metadata file should be reused
    // since the original process is dead
    const echo = try harness.startService(.{
        .executable_name = "ringloom-test-echo-service",
        .service_name = "ephemeral",
    });
    try harness.waitForServiceReady(echo, 5000);

    // Then — the replacement service is alive and successfully registered
    try std.testing.expect(echo.isAlive());

    // Verify broker acknowledged the new registration
    try readiness.waitForLogLine(broker, "service registered", 5000);

    // Cleanup
    try harness.stopProcess(echo);
    try harness.stopProcess(broker);
}

test "service restart without prior cleanup still succeeds" {
    // Given — tests that a service can restart quickly even if the broker
    // hasn't fully processed the heartbeat timeout yet, because the metadata
    // file's stored PID belongs to a dead process
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "restart-fast");
    defer harness.deinit();
    errdefer harness.markFailed();

    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    const crashy = try harness.startService(.{
        .executable_name = "ringloom-test-crashy-service",
        .service_name = "fast-restart",
    });
    try harness.waitForServiceReady(crashy, 5000);

    // When — kill and immediately restart with a new service (don't wait for
    // heartbeat timeout; the new process should detect the dead PID in the
    // metadata file and reclaim it)
    harness.killProcess(crashy);

    // Brief pause to ensure the OS has reaped the process
    std.Io.sleep(std.testing.io, .fromNanoseconds(500 * std.time.ns_per_ms), .awake) catch unreachable;

    const replacement = try harness.startService(.{
        .executable_name = "ringloom-test-echo-service",
        .service_name = "fast-restart",
    });
    try harness.waitForServiceReady(replacement, 10000);

    // Then — replacement is alive and functioning
    try std.testing.expect(replacement.isAlive());

    // Cleanup
    try harness.stopProcess(replacement);
    try harness.stopProcess(broker);
}
