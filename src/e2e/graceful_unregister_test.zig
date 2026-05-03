const std = @import("std");
const Clock = @import("brz_common").platform.Clock;
const testing_mod = @import("brz_testing");
const TestHarness = testing_mod.TestHarness;
const readiness = testing_mod.readiness;

test "graceful unregister is processed faster than heartbeat timeout" {
    // Given
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "graceful-unregister");
    errdefer harness.markFailed();
    defer harness.deinit();

    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    const echo = try harness.startService(.{
        .executable_name = "brz-test-echo-service",
        .service_name = "echo",
    });
    try harness.waitForServiceReady(echo, 5000);

    // When — stop echo gracefully (sends unregister control message)
    const before_stop = Clock.monotonicNanosStable();
    try harness.stopProcess(echo);
    const echo_exit = try echo.waitForExit(5000);
    try std.testing.expectEqual(@as(u32, 0), echo_exit);

    // Then — broker logs removal well before heartbeat timeout (10s default)
    try readiness.waitForLogLine(broker, "service unregistered", 5000);
    const elapsed_ms = @divTrunc(Clock.monotonicNanosStable() - before_stop, std.time.ns_per_ms);

    // Graceful unregister should be near-instant, certainly under 3 seconds.
    // Heartbeat timeout is 10s — if it took that long we fell back to timeout cleanup.
    try std.testing.expect(elapsed_ms < 3000);

    // Cleanup
    try harness.stopProcess(broker);
}

test "graceful unregister triggers discovery removal for subscribers" {
    // Given
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "graceful-unregister-discovery");
    errdefer harness.markFailed();
    defer harness.deinit();

    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    // Start echo first, then ping which subscribes to echo
    const echo = try harness.startService(.{
        .executable_name = "brz-test-echo-service",
        .service_name = "echo",
    });
    try harness.waitForServiceReady(echo, 5000);

    const ping = try harness.startService(.{
        .executable_name = "brz-test-ping-service",
        .service_name = "ping",
        .extra_args = &.{ "--target-service", "echo", "--message-count", "0", "--warmup-count", "0" },
    });
    try harness.waitForServiceReady(ping, 5000);

    // Allow discovery propagation
    std.Io.sleep(std.testing.io, .fromNanoseconds(1 * std.time.ns_per_s), .awake) catch unreachable;

    // When — gracefully stop echo
    try harness.stopProcess(echo);
    _ = try echo.waitForExit(5000);

    // Then — broker should process the unregistration
    try readiness.waitForLogLine(broker, "service unregistered", 5000);

    // Cleanup
    try harness.stopProcess(ping);
    try harness.stopProcess(broker);
}

test "graceful unregister of multiple services in sequence" {
    // Given
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "graceful-unregister-multi");
    errdefer harness.markFailed();
    defer harness.deinit();

    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    const echo1 = try harness.startService(.{
        .executable_name = "brz-test-echo-service",
        .service_name = "echo-1",
    });
    try harness.waitForServiceReady(echo1, 5000);

    const echo2 = try harness.startService(.{
        .executable_name = "brz-test-echo-service",
        .service_name = "echo-2",
    });
    try harness.waitForServiceReady(echo2, 5000);

    const echo3 = try harness.startService(.{
        .executable_name = "brz-test-echo-service",
        .service_name = "echo-3",
    });
    try harness.waitForServiceReady(echo3, 5000);

    // When — stop all three services gracefully in sequence
    try harness.stopProcess(echo1);
    _ = try echo1.waitForExit(5000);

    try harness.stopProcess(echo2);
    _ = try echo2.waitForExit(5000);

    try harness.stopProcess(echo3);
    _ = try echo3.waitForExit(5000);

    // Then — broker should have processed all three unregistrations
    // Allow a moment for broker to finish processing
    std.Io.sleep(std.testing.io, .fromNanoseconds(500 * std.time.ns_per_ms), .awake) catch unreachable;

    // Broker should still be healthy after all unregistrations
    try std.testing.expect(broker.isAlive());

    // Cleanup
    try harness.stopProcess(broker);
    const broker_exit = try broker.waitForExit(5000);
    try std.testing.expectEqual(@as(u32, 0), broker_exit);
}
