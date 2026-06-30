// SPDX-License-Identifier: Apache-2.0

//! Persistent topics end-to-end tests.
//!
//! Validates: broker startup with topics enabled, config loading,
//! disabled broker behavior, and publisher ack flow.

const std = @import("std");
const testing_mod = @import("ringloom_testing");
const TestHarness = testing_mod.TestHarness;

// ── Broker startup with topics enabled ───────────────────────────────

test "topic broker starts with topics enabled" {
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "topic-broker-startup");
    defer harness.deinit();
    errdefer harness.markFailed();

    const broker = try harness.startBroker(.{ .topics_enabled = true });
    try harness.waitForBrokerReady(broker, 5000);

    try std.testing.expect(broker.isAlive());
    try harness.stopProcess(broker);
}

// ── Topics disabled broker ────────────────────────────────────────────

test "topic disabled broker starts normally" {
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "topic-disabled");
    defer harness.deinit();
    errdefer harness.markFailed();

    const broker = try harness.startBroker(.{ .topics_enabled = false });
    try harness.waitForBrokerReady(broker, 5000);

    try std.testing.expect(broker.isAlive());
    try harness.stopProcess(broker);
}

// ── Topic publisher with replicate_once ack (single-node) ────────────

test "topic replicate_once ack completes on single node broker" {
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "topic-ack-single");
    defer harness.deinit();
    errdefer harness.markFailed();

    const broker = try harness.startBroker(.{ .topics_enabled = true });
    try harness.waitForBrokerReady(broker, 5000);

    // Publisher sends 5 messages with replicate_once (single-node acks on append).
    // This validates the broker's ack infrastructure starts correctly.
    const publisher = try harness.startService(.{
        .executable_name = "ringloom-test-topic-publisher-service",
        .service_name = "topic-pub-ack",
        .extra_args = &.{ "trades", "5", "1" },
    });
    try harness.waitForServiceReady(publisher, 5000);

    const pub_exit = try publisher.waitForExit(15000);
    try std.testing.expectEqual(@as(u32, 0), pub_exit);

    try harness.stopProcess(publisher);
    try harness.stopProcess(broker);
}

// ── Broker survives rapid start/stop with topics ─────────────────────

test "topic broker survives stop and restart" {
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "topic-restart");
    defer harness.deinit();
    errdefer harness.markFailed();

    const broker = try harness.startBroker(.{ .topics_enabled = true });
    try harness.waitForBrokerReady(broker, 5000);
    try harness.stopProcess(broker);

    // Restart with the same config (queue dirs persist).
    const broker2 = try harness.startBroker(.{ .topics_enabled = true });
    try harness.waitForBrokerReady(broker2, 5000);
    try std.testing.expect(broker2.isAlive());
    try harness.stopProcess(broker2);
}
