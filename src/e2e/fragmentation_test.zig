const std = @import("std");
const testing_mod = @import("ringloom_testing");
const TestHarness = testing_mod.TestHarness;

test "large messages are fragmented and reassembled correctly" {
    // Given
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "fragmentation");
    errdefer harness.markFailed();
    defer harness.deinit();

    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    const echo = try harness.startService(.{
        .executable_name = "ringloom-test-echo-service",
        .service_name = "echo",
    });
    try harness.waitForServiceReady(echo, 5000);

    // When — ping sends messages larger than MTU (4096 bytes each)
    const ping = try harness.startService(.{
        .executable_name = "ringloom-test-ping-service",
        .service_name = "ping",
        .extra_args = &.{
            "--target-service",  "echo",
            "--message-count",   "10",
            "--message-size",    "4096",
        },
    });
    try harness.waitForServiceReady(ping, 5000);

    // Then — all large messages round-trip successfully
    const exit_code = try ping.waitForExit(20000);
    try std.testing.expectEqual(@as(u32, 0), exit_code);

    // Cleanup
    try harness.stopProcess(echo);
    try harness.stopProcess(broker);
}

test "very large messages beyond multiple fragment boundaries" {
    // Given
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "fragmentation-large");
    errdefer harness.markFailed();
    defer harness.deinit();

    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    const echo = try harness.startService(.{
        .executable_name = "ringloom-test-echo-service",
        .service_name = "echo",
    });
    try harness.waitForServiceReady(echo, 5000);

    // When — ping sends messages at 64KB (well beyond single-fragment size)
    const ping = try harness.startService(.{
        .executable_name = "ringloom-test-ping-service",
        .service_name = "ping",
        .extra_args = &.{
            "--target-service",  "echo",
            "--message-count",   "3",
            "--message-size",    "65536",
        },
    });
    try harness.waitForServiceReady(ping, 5000);

    // Then — all messages arrive intact despite spanning many fragments
    const exit_code = try ping.waitForExit(30000);
    try std.testing.expectEqual(@as(u32, 0), exit_code);

    // Cleanup
    try harness.stopProcess(echo);
    try harness.stopProcess(broker);
}

test "mixed small and large messages are handled correctly" {
    // Given
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "fragmentation-mixed");
    errdefer harness.markFailed();
    defer harness.deinit();

    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    const echo = try harness.startService(.{
        .executable_name = "ringloom-test-echo-service",
        .service_name = "echo",
    });
    try harness.waitForServiceReady(echo, 5000);

    // When — ping sends a mix of small (64 byte) and large (8192 byte) messages
    const ping = try harness.startService(.{
        .executable_name = "ringloom-test-ping-service",
        .service_name = "ping",
        .extra_args = &.{
            "--target-service",    "echo",
            "--message-count",     "20",
            "--mixed-sizes",
            "--small-message-size", "64",
            "--large-message-size", "8192",
        },
    });
    try harness.waitForServiceReady(ping, 5000);

    // Then — all messages arrive and responses match
    const exit_code = try ping.waitForExit(20000);
    try std.testing.expectEqual(@as(u32, 0), exit_code);

    // Cleanup
    try harness.stopProcess(echo);
    try harness.stopProcess(broker);
}
