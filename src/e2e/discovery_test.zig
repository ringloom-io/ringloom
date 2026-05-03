const std = @import("std");
const testing_mod = @import("ringloom_testing");
const TestHarness = testing_mod.TestHarness;

test "service discovery updates are delivered" {
    // Given
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "discovery");
    errdefer harness.markFailed();
    defer harness.deinit();

    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    // When — start ping first; it subscribes to echo but echo isn't running yet
    const ping = try harness.startService(.{
        .executable_name = "ringloom-test-ping-service",
        .service_name = "ping",
        .extra_args = &.{ "--target-service", "echo", "--message-count", "5", "--warmup-count", "0" },
    });
    try harness.waitForServiceReady(ping, 5000);

    // Then — ping is alive but blocked waiting for echo to appear
    try std.testing.expect(ping.isAlive());

    // When — now start echo; discovery should propagate to ping
    const echo = try harness.startService(.{
        .executable_name = "ringloom-test-echo-service",
        .service_name = "echo",
    });
    try harness.waitForServiceReady(echo, 5000);

    // Allow time for discovery propagation and message exchange
    std.Io.sleep(std.testing.io, .fromNanoseconds(2 * std.time.ns_per_s), .awake) catch unreachable;

    // Then — ping should eventually complete after discovering echo
    const exit_code = try ping.waitForExit(15000);
    try std.testing.expectEqual(@as(u32, 0), exit_code);

    // Cleanup
    try harness.stopProcess(echo);
    try harness.stopProcess(broker);
}

test "late service registration triggers discovery notification" {
    // Given
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "discovery-late-reg");
    errdefer harness.markFailed();
    defer harness.deinit();

    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    // When — start two ping services both targeting echo, before echo exists
    const ping_a = try harness.startService(.{
        .executable_name = "ringloom-test-ping-service",
        .service_name = "ping-a",
        .extra_args = &.{ "--target-service", "echo", "--message-count", "3", "--warmup-count", "0" },
    });
    try harness.waitForServiceReady(ping_a, 5000);

    const ping_b = try harness.startService(.{
        .executable_name = "ringloom-test-ping-service",
        .service_name = "ping-b",
        .extra_args = &.{ "--target-service", "echo", "--message-count", "3", "--warmup-count", "0" },
    });
    try harness.waitForServiceReady(ping_b, 5000);

    // Then — both pings are alive, waiting for discovery
    try std.testing.expect(ping_a.isAlive());
    try std.testing.expect(ping_b.isAlive());

    // When — start echo; both pings should discover it
    const echo = try harness.startService(.{
        .executable_name = "ringloom-test-echo-service",
        .service_name = "echo",
    });
    try harness.waitForServiceReady(echo, 5000);

    // Allow time for discovery propagation
    std.Io.sleep(std.testing.io, .fromNanoseconds(2 * std.time.ns_per_s), .awake) catch unreachable;

    // Then — both pings complete successfully
    const exit_a = try ping_a.waitForExit(15000);
    try std.testing.expectEqual(@as(u32, 0), exit_a);

    const exit_b = try ping_b.waitForExit(15000);
    try std.testing.expectEqual(@as(u32, 0), exit_b);

    // Cleanup
    try harness.stopProcess(echo);
    try harness.stopProcess(broker);
}

test "service removal triggers discovery removal notification" {
    // Given — broker, echo, and forwarder are all running
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "discovery-removal");
    errdefer harness.markFailed();
    defer harness.deinit();

    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    const echo = try harness.startService(.{
        .executable_name = "ringloom-test-echo-service",
        .service_name = "echo",
    });
    try harness.waitForServiceReady(echo, 5000);

    const forwarder = try harness.startService(.{
        .executable_name = "ringloom-test-forwarder-service",
        .service_name = "forwarder",
        .extra_args = &.{"--target-service", "echo"},
    });
    try harness.waitForServiceReady(forwarder, 5000);

    // Allow discovery to fully propagate
    std.Io.sleep(std.testing.io, .fromNanoseconds(1 * std.time.ns_per_s), .awake) catch unreachable;

    // When — stop echo gracefully
    try harness.stopProcess(echo);

    // Then — broker logs removal; wait for removal log line
    const readiness = testing_mod.readiness;
    try readiness.waitForLogLine(broker, "service removed", 10000);

    // Cleanup
    try harness.stopProcess(forwarder);
    try harness.stopProcess(broker);
}
