// SPDX-License-Identifier: Apache-2.0

//! Topics publish/consume performance benchmarks.
//!
//! Measures: broker startup with topics enabled, and registered-publisher
//! publish throughput plus replicate_once ack wait (results persisted under
//! /tmp/ringloom-perf-results/topic/).

const std = @import("std");
const platform = @import("ringloom_common").platform;
const testing_mod = @import("ringloom_testing");
const TestHarness = testing_mod.TestHarness;
const persistent_results = @import("persistent_results.zig");

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

fn runTopicPublishBench(comptime ack_mode: []const u8, comptime size: []const u8) !void {
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "perf-topic-publish-" ++ ack_mode ++ "-" ++ size);
    defer harness.deinit();
    errdefer harness.markFailed();

    const broker = try harness.startBroker(.{ .topics_enabled = true });
    try harness.waitForBrokerReady(broker, 5000);

    const mode_name = comptime if (std.mem.eql(u8, ack_mode, "1")) "replicate_once" else "fire_and_forget";
    const result_path = try persistent_results.scenarioPath(
        allocator,
        "topic",
        "topic-publish-" ++ mode_name ++ "-" ++ size ++ "B.json",
    );
    defer allocator.free(result_path);

    // The publisher registers the topic, publishes through the leader's IPC
    // publication and, for replicate_once, exits non-zero unless the last
    // publish is acked.
    var publisher = try harness.startService(.{
        .executable_name = "ringloom-test-topic-publisher-service",
        .service_name = "topic-perf-pub",
        .extra_args = &.{
            "--topic",       "perf",
            "--count",       "100000",
            "--warmup",      "1000",
            "--size",        size,
            "--ack-mode",    ack_mode,
            "--result-file", result_path,
        },
    });
    try harness.waitForServiceReady(publisher, 5000);

    const exit_code = try publisher.waitForExit(60_000);
    try std.testing.expectEqual(@as(u32, 0), exit_code);

    try harness.stopProcess(publisher);
    try harness.stopProcess(broker);
}

test "topic publish throughput - fire_and_forget 128B" {
    try runTopicPublishBench("0", "128");
}

test "topic publish throughput and ack latency - replicate_once 128B" {
    try runTopicPublishBench("1", "128");
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
