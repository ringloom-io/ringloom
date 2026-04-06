//! Backpressure Onset Detection Benchmark
//!
//! Measures the point at which backpressure manifests when a fast producer
//! overwhelms a slow consumer. Incrementally increases the offered load and
//! records achieved throughput, p99 latency, and send failures at each step.
//!
//! Topology: 1 broker, 1 slow-consumer service, 1 ping service (producer).

const std = @import("std");
const testing_mod = @import("brz_testing");
const TestHarness = testing_mod.TestHarness;
const BrokerSpec = testing_mod.BrokerSpec;
const ServiceSpec = testing_mod.ServiceSpec;
const result_writer = testing_mod.result_writer;
const PerfResult = result_writer.PerfResult;

// ── Helpers ──────────────────────────────────────────────────────────

const BackpressureStep = struct {
    message_count: u32,
    message_size: u32,
    label: []const u8,
};

const backpressure_steps = [_]BackpressureStep{
    .{ .message_count = 1_000, .message_size = 128, .label = "1k-msgs" },
    .{ .message_count = 5_000, .message_size = 128, .label = "5k-msgs" },
    .{ .message_count = 10_000, .message_size = 128, .label = "10k-msgs" },
    .{ .message_count = 25_000, .message_size = 128, .label = "25k-msgs" },
    .{ .message_count = 50_000, .message_size = 128, .label = "50k-msgs" },
    .{ .message_count = 100_000, .message_size = 128, .label = "100k-msgs" },
};

fn fmtU32(buf: []u8, val: u32) []const u8 {
    return std.fmt.bufPrint(buf, "{d}", .{val}) catch "0";
}

// ── Backpressure onset — 128B, escalating message counts ─────────────

test "backpressure onset detection - 128B escalating load" {
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "perf-backpressure-onset-128");
    defer harness.deinit();
    errdefer harness.markFailed();

    // Given — a broker and a deliberately slow consumer.
    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    const slow = try harness.startService(.{
        .executable_name = "brz-test-slow-consumer-service",
        .service_name = "slow-consumer",
        .extra_args = &.{ "--delay-per-message-ms", "1" },
    });
    try harness.waitForServiceReady(slow, 5000);

    // When — send escalating batch sizes through the slow consumer.
    for (backpressure_steps) |step| {
        var count_buf: [16]u8 = undefined;
        var size_buf: [16]u8 = undefined;
        const count_str = fmtU32(&count_buf, step.message_count);
        const size_str = fmtU32(&size_buf, step.message_size);

        const result_path = try std.fmt.allocPrint(
            allocator,
            "{s}/backpressure-onset-{s}.json",
            .{ harness.env.results_path, step.label },
        );
        defer allocator.free(result_path);

        const ping = try harness.startService(.{
            .executable_name = "brz-test-ping-service",
            .service_name = "ping",
            .extra_args = &.{
                "--target-service",  "slow-consumer",
                "--message-count",   count_str,
                "--message-size",    size_str,
                "--warmup-count",    "0",
                "--result-file",     result_path,
            },
        });
        try harness.waitForServiceReady(ping, 5000);

        // Then — the ping service completes (some failures expected at high load).
        const exit_code = try ping.waitForExit(120_000);
        _ = exit_code; // Don't assert zero — send failures are expected.
    }

    // Cleanup.
    try harness.stopProcess(slow);
    try harness.stopProcess(broker);
}

// ── Backpressure onset — 1024B, escalating message counts ────────────

test "backpressure onset detection - 1024B escalating load" {
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "perf-backpressure-onset-1024");
    defer harness.deinit();
    errdefer harness.markFailed();

    // Given
    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    const slow = try harness.startService(.{
        .executable_name = "brz-test-slow-consumer-service",
        .service_name = "slow-consumer",
        .extra_args = &.{ "--delay-per-message-ms", "1" },
    });
    try harness.waitForServiceReady(slow, 5000);

    const large_steps = [_]BackpressureStep{
        .{ .message_count = 1_000, .message_size = 1024, .label = "1k-msgs-1024B" },
        .{ .message_count = 5_000, .message_size = 1024, .label = "5k-msgs-1024B" },
        .{ .message_count = 10_000, .message_size = 1024, .label = "10k-msgs-1024B" },
        .{ .message_count = 25_000, .message_size = 1024, .label = "25k-msgs-1024B" },
        .{ .message_count = 50_000, .message_size = 1024, .label = "50k-msgs-1024B" },
    };

    // When — send escalating batch sizes with larger payloads.
    for (large_steps) |step| {
        var count_buf: [16]u8 = undefined;
        var size_buf: [16]u8 = undefined;
        const count_str = fmtU32(&count_buf, step.message_count);
        const size_str = fmtU32(&size_buf, step.message_size);

        const result_path = try std.fmt.allocPrint(
            allocator,
            "{s}/backpressure-onset-{s}.json",
            .{ harness.env.results_path, step.label },
        );
        defer allocator.free(result_path);

        const ping = try harness.startService(.{
            .executable_name = "brz-test-ping-service",
            .service_name = "ping",
            .extra_args = &.{
                "--target-service",  "slow-consumer",
                "--message-count",   count_str,
                "--message-size",    size_str,
                "--warmup-count",    "0",
                "--result-file",     result_path,
            },
        });
        try harness.waitForServiceReady(ping, 5000);

        const exit_code = try ping.waitForExit(120_000);
        _ = exit_code;
    }

    // Cleanup.
    try harness.stopProcess(slow);
    try harness.stopProcess(broker);
}

// ── Backpressure with varying consumer delay ─────────────────────────

test "backpressure varying consumer delay - 128B" {
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "perf-backpressure-delay-sweep");
    defer harness.deinit();
    errdefer harness.markFailed();

    // Given — broker only; consumer varies per iteration.
    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    const delays_ms = [_][]const u8{ "0", "1", "2", "5", "10" };

    // When — for each delay value, start a slow consumer and blast messages.
    for (delays_ms) |delay_str| {
        const slow = try harness.startService(.{
            .executable_name = "brz-test-slow-consumer-service",
            .service_name = "slow-consumer",
            .extra_args = &.{ "--delay-per-message-ms", delay_str },
        });
        try harness.waitForServiceReady(slow, 5000);

        const result_path = try std.fmt.allocPrint(
            allocator,
            "{s}/backpressure-delay-{s}ms.json",
            .{ harness.env.results_path, delay_str },
        );
        defer allocator.free(result_path);

        const ping = try harness.startService(.{
            .executable_name = "brz-test-ping-service",
            .service_name = "ping",
            .extra_args = &.{
                "--target-service",  "slow-consumer",
                "--message-count",   "10000",
                "--message-size",    "128",
                "--warmup-count",    "0",
                "--result-file",     result_path,
            },
        });
        try harness.waitForServiceReady(ping, 5000);

        // Then — wait for ping to finish.
        const exit_code = try ping.waitForExit(120_000);
        _ = exit_code;

        // Tear down consumer for next iteration.
        try harness.stopProcess(slow);
    }

    // Cleanup.
    try harness.stopProcess(broker);
}

// ── Backpressure with large messages ─────────────────────────────────

test "backpressure onset detection - 4096B large payload" {
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "perf-backpressure-4096");
    defer harness.deinit();
    errdefer harness.markFailed();

    // Given
    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    const slow = try harness.startService(.{
        .executable_name = "brz-test-slow-consumer-service",
        .service_name = "slow-consumer",
        .extra_args = &.{ "--delay-per-message-ms", "2" },
    });
    try harness.waitForServiceReady(slow, 5000);

    // When — 4096B messages at volume to stress ring buffer capacity.
    const large_msg_steps = [_]BackpressureStep{
        .{ .message_count = 1_000, .message_size = 4096, .label = "1k-msgs-4096B" },
        .{ .message_count = 5_000, .message_size = 4096, .label = "5k-msgs-4096B" },
        .{ .message_count = 10_000, .message_size = 4096, .label = "10k-msgs-4096B" },
        .{ .message_count = 25_000, .message_size = 4096, .label = "25k-msgs-4096B" },
    };

    for (large_msg_steps) |step| {
        var count_buf: [16]u8 = undefined;
        var size_buf: [16]u8 = undefined;
        const count_str = fmtU32(&count_buf, step.message_count);
        const size_str = fmtU32(&size_buf, step.message_size);

        const result_path = try std.fmt.allocPrint(
            allocator,
            "{s}/backpressure-onset-{s}.json",
            .{ harness.env.results_path, step.label },
        );
        defer allocator.free(result_path);

        const ping = try harness.startService(.{
            .executable_name = "brz-test-ping-service",
            .service_name = "ping",
            .extra_args = &.{
                "--target-service",  "slow-consumer",
                "--message-count",   count_str,
                "--message-size",    size_str,
                "--warmup-count",    "0",
                "--result-file",     result_path,
            },
        });
        try harness.waitForServiceReady(ping, 5000);

        // Then — allow completion (failures expected).
        const exit_code = try ping.waitForExit(120_000);
        _ = exit_code;
    }

    // Cleanup.
    try harness.stopProcess(slow);
    try harness.stopProcess(broker);
}

// ── Sustained backpressure — steady-state under overload ─────────────

test "sustained backpressure - steady state under overload" {
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "perf-backpressure-sustained");
    defer harness.deinit();
    errdefer harness.markFailed();

    // Given — broker and a very slow consumer (5ms per message).
    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    const slow = try harness.startService(.{
        .executable_name = "brz-test-slow-consumer-service",
        .service_name = "slow-consumer",
        .extra_args = &.{ "--delay-per-message-ms", "5" },
    });
    try harness.waitForServiceReady(slow, 5000);

    const result_path = try std.fmt.allocPrint(
        allocator,
        "{s}/backpressure-sustained.json",
        .{harness.env.results_path},
    );
    defer allocator.free(result_path);

    // When — send a very large batch to force sustained backpressure.
    const ping = try harness.startService(.{
        .executable_name = "brz-test-ping-service",
        .service_name = "ping",
        .extra_args = &.{
            "--target-service",  "slow-consumer",
            "--message-count",   "50000",
            "--message-size",    "256",
            "--warmup-count",    "0",
            "--result-file",     result_path,
        },
    });
    try harness.waitForServiceReady(ping, 5000);

    // Then — give ample time for the overloaded path to complete.
    const exit_code = try ping.waitForExit(300_000);
    _ = exit_code;

    // Cleanup.
    try harness.stopProcess(slow);
    try harness.stopProcess(broker);
}
