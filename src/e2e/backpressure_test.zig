const std = @import("std");
const testing_mod = @import("brz_testing");

const TestHarness = testing_mod.TestHarness;
const readiness = testing_mod.readiness;

// ── Backpressure Behavior ────────────────────────────────────────────
//
// Validates that the system remains stable under backpressure conditions.
// A slow consumer is paired with a high-rate producer. The producer may
// experience send failures (ring buffer full / claim failures), but the
// overall system must not crash, deadlock, or corrupt data.

test "system remains stable under backpressure with slow consumer" {
    // Given
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "backpressure");
    errdefer harness.markFailed();
    defer harness.deinit();

    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    // Start a slow consumer service — it intentionally delays processing
    // each message, causing the ring buffer to fill up.
    const slow = try harness.startService(.{
        .executable_name = "brz-test-slow-consumer-service",
        .service_name = "slow-echo",
    });
    try harness.waitForServiceReady(slow, 5000);

    // When — start ping service sending at a high rate to overwhelm the
    // slow consumer. We use a large message count with zero delay between
    // sends so the ring buffer will back up.
    const ping = try harness.startService(.{
        .executable_name = "brz-test-ping-service",
        .service_name = "ping",
        .extra_args = &.{
            "--target-service",  "slow-echo",
            "--message-count",   "500",
            "--send-delay-us",   "0",
            "--allow-failures",  "true",
        },
    });
    try harness.waitForServiceReady(ping, 5000);

    // Then — ping completes (may have partial send failures, but exits 0)
    const exit_code = try ping.waitForExit(30000);
    try std.testing.expectEqual(@as(u32, 0), exit_code);

    // Then — slow consumer is still alive and hasn't crashed
    try std.testing.expect(slow.isAlive());

    // Then — broker is still alive and hasn't crashed
    try std.testing.expect(broker.isAlive());

    // Cleanup
    try harness.stopProcess(slow);
    try harness.stopProcess(broker);
}

test "broker stays healthy when multiple producers overwhelm a single consumer" {
    // Given
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "backpressure-multi-producer");
    errdefer harness.markFailed();
    defer harness.deinit();

    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    const slow = try harness.startService(.{
        .executable_name = "brz-test-slow-consumer-service",
        .service_name = "slow-echo",
    });
    try harness.waitForServiceReady(slow, 5000);

    // When — start three ping services all targeting the same slow consumer
    const ping_a = try harness.startService(.{
        .executable_name = "brz-test-ping-service",
        .service_name = "ping-a",
        .extra_args = &.{
            "--target-service",  "slow-echo",
            "--message-count",   "200",
            "--send-delay-us",   "0",
            "--allow-failures",  "true",
        },
    });
    try harness.waitForServiceReady(ping_a, 5000);

    const ping_b = try harness.startService(.{
        .executable_name = "brz-test-ping-service",
        .service_name = "ping-b",
        .extra_args = &.{
            "--target-service",  "slow-echo",
            "--message-count",   "200",
            "--send-delay-us",   "0",
            "--allow-failures",  "true",
        },
    });
    try harness.waitForServiceReady(ping_b, 5000);

    const ping_c = try harness.startService(.{
        .executable_name = "brz-test-ping-service",
        .service_name = "ping-c",
        .extra_args = &.{
            "--target-service",  "slow-echo",
            "--message-count",   "200",
            "--send-delay-us",   "0",
            "--allow-failures",  "true",
        },
    });
    try harness.waitForServiceReady(ping_c, 5000);

    // Then — all producers complete without crashing
    const exit_a = try ping_a.waitForExit(30000);
    const exit_b = try ping_b.waitForExit(30000);
    const exit_c = try ping_c.waitForExit(30000);

    try std.testing.expectEqual(@as(u32, 0), exit_a);
    try std.testing.expectEqual(@as(u32, 0), exit_b);
    try std.testing.expectEqual(@as(u32, 0), exit_c);

    // Then — slow consumer is still alive
    try std.testing.expect(slow.isAlive());

    // Then — broker is still alive
    try std.testing.expect(broker.isAlive());

    // Cleanup
    try harness.stopProcess(slow);
    try harness.stopProcess(broker);
}
