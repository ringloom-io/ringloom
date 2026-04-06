//! Cross-Broker Throughput Benchmark
//!
//! Measures sustained message throughput (messages/sec and bytes/sec) across two
//! brokers connected via loopback UDP. A ping service on broker A sends a high
//! volume of messages to an echo service on broker B, and the result file
//! captures achieved throughput for each message size.
//!
//! Topology:
//!   broker-A (node 1, port 19001)  ←→  broker-B (node 2, port 19002)
//!   ping service → broker-A → UDP → broker-B → echo service → broker-B → UDP → broker-A → ping service
//!
//! Message sizes tested: 32, 256, 1024, 4096, 16384 bytes.

const std = @import("std");
const testing_mod = @import("brz_testing");
const TestHarness = testing_mod.TestHarness;

const ProcessHandle = testing_mod.ProcessHandle;

// ─── Helpers ─────────────────────────────────────────────────────────

const TwoBrokerTopology = struct {
    broker_a: *ProcessHandle,
    broker_b: *ProcessHandle,
    echo: *ProcessHandle,
};

fn setupTwoBrokerTopology(harness: *TestHarness) !TwoBrokerTopology {
    // Start broker A (node 1).
    const broker_a = try harness.startBroker(.{
        .node_id = 1,
        .port = 19001,
        .peers = &.{.{ .node_id = 2, .host = "127.0.0.1", .port = 19002 }},
    });
    try harness.waitForBrokerReady(broker_a, 5000);

    // Start broker B (node 2).
    const broker_b = try harness.startBroker(.{
        .node_id = 2,
        .port = 19002,
        .peers = &.{.{ .node_id = 1, .host = "127.0.0.1", .port = 19001 }},
    });
    try harness.waitForBrokerReady(broker_b, 5000);

    // Allow cluster link to stabilise.
    std.Thread.sleep(2 * std.time.ns_per_s);

    // Echo service on broker B.
    const echo = try harness.startService(.{
        .executable_name = "brz-test-echo-service",
        .service_name = "echo",
        .broker_node_id = 2,
    });
    try harness.waitForServiceReady(echo, 5000);

    return .{
        .broker_a = broker_a,
        .broker_b = broker_b,
        .echo = echo,
    };
}

fn teardownTopology(harness: *TestHarness, topo: TwoBrokerTopology) void {
    harness.stopProcess(topo.echo) catch {};
    harness.stopProcess(topo.broker_b) catch {};
    harness.stopProcess(topo.broker_a) catch {};
}

fn runRemoteThroughputScenario(
    harness: *TestHarness,
    allocator: std.mem.Allocator,
    topo: TwoBrokerTopology,
    comptime size_str: []const u8,
    comptime label: []const u8,
) !void {
    _ = topo; // topology already running; ping targets echo by name

    const result_path = try std.fmt.allocPrint(
        allocator,
        "{s}/remote-throughput-{s}.json",
        .{ harness.env.results_path, label },
    );
    defer allocator.free(result_path);

    // When — launch ping service on broker A with high message count.
    const ping = try harness.startService(.{
        .executable_name = "brz-test-ping-service",
        .service_name = "ping",
        .broker_node_id = 1,
        .extra_args = &.{
            "--target-service",  "echo",
            "--message-count",   "200000",
            "--message-size",    size_str,
            "--warmup-count",    "20000",
            "--result-file",     result_path,
        },
    });
    try harness.waitForServiceReady(ping, 5000);

    // Then — ping completes within the generous cross-broker timeout.
    const exit_code = try ping.waitForExit(180_000);
    try std.testing.expectEqual(@as(u32, 0), exit_code);
}

// ─── 32 B ────────────────────────────────────────────────────────────

test "cross-broker throughput - 32B" {
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "perf-remote-throughput-32");
    defer harness.deinit();
    errdefer harness.markFailed();

    // Given — two brokers with a cluster link and an echo service on broker B.
    const topo = try setupTwoBrokerTopology(&harness);
    defer teardownTopology(&harness, topo);

    // When / Then
    try runRemoteThroughputScenario(&harness, allocator, topo, "32", "32B");
}

// ─── 256 B ───────────────────────────────────────────────────────────

test "cross-broker throughput - 256B" {
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "perf-remote-throughput-256");
    defer harness.deinit();
    errdefer harness.markFailed();

    // Given
    const topo = try setupTwoBrokerTopology(&harness);
    defer teardownTopology(&harness, topo);

    // When / Then
    try runRemoteThroughputScenario(&harness, allocator, topo, "256", "256B");
}

// ─── 1024 B ──────────────────────────────────────────────────────────

test "cross-broker throughput - 1024B" {
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "perf-remote-throughput-1024");
    defer harness.deinit();
    errdefer harness.markFailed();

    // Given
    const topo = try setupTwoBrokerTopology(&harness);
    defer teardownTopology(&harness, topo);

    // When / Then
    try runRemoteThroughputScenario(&harness, allocator, topo, "1024", "1024B");
}

// ─── 4096 B ──────────────────────────────────────────────────────────

test "cross-broker throughput - 4096B" {
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "perf-remote-throughput-4096");
    defer harness.deinit();
    errdefer harness.markFailed();

    // Given
    const topo = try setupTwoBrokerTopology(&harness);
    defer teardownTopology(&harness, topo);

    // When / Then
    try runRemoteThroughputScenario(&harness, allocator, topo, "4096", "4096B");
}

// ─── 16384 B ─────────────────────────────────────────────────────────

test "cross-broker throughput - 16384B" {
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "perf-remote-throughput-16384");
    defer harness.deinit();
    errdefer harness.markFailed();

    // Given
    const topo = try setupTwoBrokerTopology(&harness);
    defer teardownTopology(&harness, topo);

    // When / Then
    try runRemoteThroughputScenario(&harness, allocator, topo, "16384", "16384B");
}
