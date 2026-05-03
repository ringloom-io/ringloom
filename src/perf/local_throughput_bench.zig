//! Local Throughput Benchmark
//!
//! Measures single-host message throughput (messages/sec and bytes/sec) with
//! one producer (ping service) and one consumer (echo service) connected
//! through a single local broker.
//!
//! Topology:  1 broker  ·  ping-service → echo-service  (same host)
//! Sizes:     32 B · 256 B · 1024 B · 4096 B

const std = @import("std");
const testing_mod = @import("ringloom_testing");

const TestHarness = testing_mod.TestHarness;
const BrokerSpec = testing_mod.BrokerSpec;
const ServiceSpec = testing_mod.ServiceSpec;

// ── Helpers ──────────────────────────────────────────────────────────

const throughput_message_count = "500000";
const throughput_warmup_count = "50000";

/// Shared implementation for every message-size variant.
fn runLocalThroughputBench(
    comptime tag: []const u8,
    comptime size_str: []const u8,
) !void {
    const allocator = std.testing.allocator;

    var harness = try TestHarness.init(allocator, "perf-local-throughput-" ++ tag);
    defer harness.deinit();
    errdefer harness.markFailed();

    // Given — a single broker with an echo service ready to consume messages.
    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    const echo = try harness.startService(.{
        .executable_name = "ringloom-test-echo-service",
        .service_name = "echo",
        .extra_args = &.{"--quiet"},
    });
    try harness.waitForServiceReady(echo, 5000);

    const result_path = try std.fmt.allocPrint(
        allocator,
        "{s}/local-throughput-" ++ tag ++ ".json",
        .{harness.env.results_path},
    );
    defer allocator.free(result_path);

    // When — the ping service fires a sustained burst and records throughput.
    const ping = try harness.startService(.{
        .executable_name = "ringloom-test-ping-service",
        .service_name = "ping",
        .extra_args = &.{
            "--target-service",  "echo",
            "--message-count",   throughput_message_count,
            "--message-size",    size_str,
            "--warmup-count",    throughput_warmup_count,
            "--result-file",     result_path,
        },
    });
    try harness.waitForServiceReady(ping, 5000);

    // Then — the ping service completes successfully within the timeout.
    const exit_code = try ping.waitForExit(120_000);
    try std.testing.expectEqual(@as(u32, 0), exit_code);

    // Cleanup — stop services before the broker.
    try harness.stopProcess(echo);
    try harness.stopProcess(broker);
}

// ── 32 B ─────────────────────────────────────────────────────────────

test "local throughput - 32B" {
    try runLocalThroughputBench("32", "32");
}

// ── 256 B ────────────────────────────────────────────────────────────

test "local throughput - 256B" {
    try runLocalThroughputBench("256", "256");
}

// ── 1024 B ───────────────────────────────────────────────────────────

test "local throughput - 1024B" {
    try runLocalThroughputBench("1024", "1024");
}

// ── 4096 B ───────────────────────────────────────────────────────────

test "local throughput - 4096B" {
    try runLocalThroughputBench("4096", "4096");
}
