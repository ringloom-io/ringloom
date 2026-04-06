//! Local Round-Trip Latency Benchmark
//!
//! Measures round-trip latency for messages routed through a single local broker.
//! Topology: 1 broker + ping service + echo service on the same host.
//!
//! Each test varies the message size while keeping the topology fixed.
//! The ping service sends a configurable number of messages (with warmup),
//! records per-message round-trip times, and writes structured JSON results.

const std = @import("std");
const testing_mod = @import("brz_testing");
const TestHarness = testing_mod.TestHarness;
const result_writer = testing_mod.result_writer;

// ── Helpers ──────────────────────────────────────────────────────────

const MessageSizeConfig = struct {
    size: u32,
    label: []const u8,
    warmup_count: []const u8,
    message_count: []const u8,
};

const message_configs = [_]MessageSizeConfig{
    .{ .size = 32, .label = "32", .warmup_count = "10000", .message_count = "100000" },
    .{ .size = 128, .label = "128", .warmup_count = "10000", .message_count = "100000" },
    .{ .size = 512, .label = "512", .warmup_count = "10000", .message_count = "100000" },
    .{ .size = 1024, .label = "1024", .warmup_count = "10000", .message_count = "100000" },
    .{ .size = 4096, .label = "4096", .warmup_count = "5000", .message_count = "50000" },
};

fn runLocalLatencyBench(
    allocator: std.mem.Allocator,
    harness_name: []const u8,
    cfg: MessageSizeConfig,
) !void {
    // Given — single broker with echo service
    var harness = try TestHarness.init(allocator, harness_name);
    defer harness.deinit();
    errdefer harness.markFailed();

    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    const echo = try harness.startService(.{
        .executable_name = "brz-test-echo-service",
        .service_name = "echo",
    });
    try harness.waitForServiceReady(echo, 5000);

    const result_filename = try std.fmt.allocPrint(
        allocator,
        "{s}/local-latency-{s}B.json",
        .{ harness.env.results_path, cfg.label },
    );
    defer allocator.free(result_filename);

    // When — run ping service with warmup and measurement phases
    const ping = try harness.startService(.{
        .executable_name = "brz-test-ping-service",
        .service_name = "ping",
        .extra_args = &.{
            "--target-service",  "echo",
            "--message-count",   cfg.message_count,
            "--message-size",    cfg.label,
            "--warmup-count",    cfg.warmup_count,
            "--result-file",     result_filename,
        },
    });
    try harness.waitForServiceReady(ping, 5000);

    // Then — ping service completes successfully within the timeout
    const exit_code = try ping.waitForExit(60_000);
    try std.testing.expectEqual(@as(u32, 0), exit_code);

    // Cleanup — stop services then broker (reverse start order)
    try harness.stopProcess(echo);
    try harness.stopProcess(broker);
}

// ── 32 B ─────────────────────────────────────────────────────────────

test "local round-trip latency - 32B" {
    const allocator = std.testing.allocator;

    // Given / When / Then
    try runLocalLatencyBench(allocator, "perf-local-latency-32", message_configs[0]);
}

// ── 128 B ────────────────────────────────────────────────────────────

test "local round-trip latency - 128B" {
    const allocator = std.testing.allocator;

    // Given / When / Then
    try runLocalLatencyBench(allocator, "perf-local-latency-128", message_configs[1]);
}

// ── 512 B ────────────────────────────────────────────────────────────

test "local round-trip latency - 512B" {
    const allocator = std.testing.allocator;

    // Given / When / Then
    try runLocalLatencyBench(allocator, "perf-local-latency-512", message_configs[2]);
}

// ── 1024 B ───────────────────────────────────────────────────────────

test "local round-trip latency - 1024B" {
    const allocator = std.testing.allocator;

    // Given / When / Then
    try runLocalLatencyBench(allocator, "perf-local-latency-1024", message_configs[3]);
}

// ── 4096 B ───────────────────────────────────────────────────────────

test "local round-trip latency - 4096B" {
    const allocator = std.testing.allocator;

    // Given / When / Then
    try runLocalLatencyBench(allocator, "perf-local-latency-4096", message_configs[4]);
}
