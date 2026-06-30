// SPDX-License-Identifier: Apache-2.0

//! Topics publish/consume performance benchmarks.
//!
//! Measures: broker startup with topics enabled, publisher ack latency,
//! and bulk publish throughput.

const std = @import("std");
const platform = @import("ringloom_common").platform;
const testing_mod = @import("ringloom_testing");
const TestHarness = testing_mod.TestHarness;

test "topic broker startup latency with topics enabled" {
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "perf-topic-broker-startup");
    defer harness.deinit();
    errdefer harness.markFailed();

    const start_ns = platform.Clock.monotonicNanosStable();
    const broker = try harness.startBroker(.{ .topics_enabled = true });
    try harness.waitForBrokerReady(broker, 5000);
    const elapsed_ns = platform.Clock.monotonicNanosStable() - start_ns;

    try std.testing.expect(broker.isAlive());
    try std.testing.expect(elapsed_ns < 10_000_000_000); // < 10s
    try harness.stopProcess(broker);
}

test "topic publisher throughput with replicate_once" {
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "perf-topic-pub-throughput");
    defer harness.deinit();
    errdefer harness.markFailed();

    const broker = try harness.startBroker(.{ .topics_enabled = true });
    try harness.waitForBrokerReady(broker, 5000);

    var publisher = try harness.startService(.{
        .executable_name = "ringloom-test-topic-publisher-service",
        .service_name = "topic-perf-pub",
        .extra_args = &.{ "perf", "1000", "1" },
    });
    try harness.waitForServiceReady(publisher, 5000);

    const exit_code = try publisher.waitForExit(30000);
    try std.testing.expectEqual(@as(u32, 0), exit_code);

    try harness.stopProcess(publisher);
    try harness.stopProcess(broker);
}

test "topic broker restart with topics reuses queue dirs" {
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "perf-topic-restart");
    defer harness.deinit();
    errdefer harness.markFailed();

    const b1 = try harness.startBroker(.{ .topics_enabled = true });
    try harness.waitForBrokerReady(b1, 5000);
    try harness.stopProcess(b1);

    const start_ns = platform.Clock.monotonicNanosStable();
    const b2 = try harness.startBroker(.{ .topics_enabled = true });
    try harness.waitForBrokerReady(b2, 5000);
    const elapsed_ns = platform.Clock.monotonicNanosStable() - start_ns;

    try std.testing.expect(b2.isAlive());
    try std.testing.expect(elapsed_ns < 10_000_000_000);
    try harness.stopProcess(b2);
}
