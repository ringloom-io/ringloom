//! Cross-Broker Round-Trip Latency Benchmark
//!
//! Measures round-trip latency for messages routed between two brokers
//! connected via loopback UDP. Topology: broker A (node 1) ↔ broker B (node 2),
//! ping service on broker A, echo service on broker B.
//!
//! Message sizes tested: 32, 128, 512, 1024, 4096 bytes.

const std = @import("std");
const testing_mod = @import("brz_testing");
const TestHarness = testing_mod.TestHarness;
const BrokerSpec = testing_mod.BrokerSpec;
const ServiceSpec = testing_mod.ServiceSpec;
const PeerSpec = testing_mod.PeerSpec;
const result_writer = testing_mod.result_writer;
const PerfResult = result_writer.PerfResult;

// ── Shared constants ─────────────────────────────────────────────────

const warmup_count = "10000";
const message_count = "100000";
const ping_completion_timeout_ms = 120_000;
const broker_ready_timeout_ms = 5_000;
const service_ready_timeout_ms = 5_000;
const cluster_settle_ns = 2 * std.time.ns_per_s;

const broker_a_node_id = 1;
const broker_b_node_id = 2;
const broker_a_port = 19001;
const broker_b_port = 19002;

// ── Helpers ──────────────────────────────────────────────────────────

fn brokerASpec() BrokerSpec {
    return .{
        .node_id = broker_a_node_id,
        .port = broker_a_port,
        .peers = &.{.{ .node_id = broker_b_node_id, .host = "127.0.0.1", .port = broker_b_port }},
    };
}

fn brokerBSpec() BrokerSpec {
    return .{
        .node_id = broker_b_node_id,
        .port = broker_b_port,
        .peers = &.{.{ .node_id = broker_a_node_id, .host = "127.0.0.1", .port = broker_a_port }},
    };
}

fn runRemoteLatencyBench(
    allocator: std.mem.Allocator,
    comptime tag: []const u8,
    comptime size_str: []const u8,
) !void {
    var harness = try TestHarness.init(allocator, "perf-remote-latency-" ++ tag);
    defer harness.deinit();
    errdefer harness.markFailed();

    // Given — two brokers forming a cluster on loopback.
    const broker_a = try harness.startBroker(brokerASpec());
    try harness.waitForBrokerReady(broker_a, broker_ready_timeout_ms);

    const broker_b = try harness.startBroker(brokerBSpec());
    try harness.waitForBrokerReady(broker_b, broker_ready_timeout_ms);

    // Allow the cluster to discover peers and stabilise.
    std.Thread.sleep(cluster_settle_ns);

    // Given — echo service registered on broker B.
    const echo = try harness.startService(.{
        .executable_name = "brz-test-echo-service",
        .service_name = "echo",
        .broker_node_id = broker_b_node_id,
        .extra_args = &.{"--quiet"},
    });
    try harness.waitForServiceReady(echo, service_ready_timeout_ms);

    const result_path = try std.fmt.allocPrint(
        allocator,
        "{s}/remote-latency-{s}.json",
        .{ harness.env.results_path, tag },
    );
    defer allocator.free(result_path);

    // When — ping service on broker A sends round-trip messages across brokers.
    const ping = try harness.startService(.{
        .executable_name = "brz-test-ping-service",
        .service_name = "ping",
        .broker_node_id = broker_a_node_id,
        .extra_args = &.{
            "--target-service",  "echo",
            "--message-count",   message_count,
            "--message-size",    size_str,
            "--warmup-count",    warmup_count,
            "--result-file",     result_path,
        },
    });
    try harness.waitForServiceReady(ping, service_ready_timeout_ms);

    // Then — ping completes successfully.
    const exit_code = try ping.waitForExit(ping_completion_timeout_ms);
    try std.testing.expectEqual(@as(u32, 0), exit_code);

    // Cleanup — stop services first, then brokers in reverse order.
    try harness.stopProcess(echo);
    try harness.stopProcess(broker_b);
    try harness.stopProcess(broker_a);
}

// ── Benchmark tests ──────────────────────────────────────────────────

test "cross-broker round-trip latency - 32B" {
    // Given — two brokers on loopback, echo on B, ping on A (32-byte messages).
    // When  — ping sends 100 000 round-trip messages across brokers.
    // Then  — ping exits successfully and writes latency results.
    try runRemoteLatencyBench(std.testing.allocator, "32", "32");
}

test "cross-broker round-trip latency - 128B" {
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "perf-remote-latency-128");
    defer harness.deinit();
    errdefer harness.markFailed();

    // Given — two brokers forming a cluster on loopback.
    const broker_a = try harness.startBroker(.{
        .node_id = broker_a_node_id,
        .port = broker_a_port,
        .peers = &.{.{ .node_id = broker_b_node_id, .host = "127.0.0.1", .port = broker_b_port }},
    });
    try harness.waitForBrokerReady(broker_a, broker_ready_timeout_ms);

    const broker_b = try harness.startBroker(.{
        .node_id = broker_b_node_id,
        .port = broker_b_port,
        .peers = &.{.{ .node_id = broker_a_node_id, .host = "127.0.0.1", .port = broker_a_port }},
    });
    try harness.waitForBrokerReady(broker_b, broker_ready_timeout_ms);

    // Allow the cluster to discover peers and stabilise.
    std.Thread.sleep(cluster_settle_ns);

    // Given — echo service registered on broker B.
    const echo = try harness.startService(.{
        .executable_name = "brz-test-echo-service",
        .service_name = "echo",
        .broker_node_id = broker_b_node_id,
        .extra_args = &.{"--quiet"},
    });
    try harness.waitForServiceReady(echo, service_ready_timeout_ms);

    const result_path = try std.fmt.allocPrint(
        allocator,
        "{s}/remote-latency-128.json",
        .{harness.env.results_path},
    );
    defer allocator.free(result_path);

    // When — ping service on broker A sends round-trip messages to echo on broker B.
    const ping = try harness.startService(.{
        .executable_name = "brz-test-ping-service",
        .service_name = "ping",
        .broker_node_id = broker_a_node_id,
        .extra_args = &.{
            "--target-service",  "echo",
            "--message-count",   message_count,
            "--message-size",    "128",
            "--warmup-count",    warmup_count,
            "--result-file",     result_path,
        },
    });
    try harness.waitForServiceReady(ping, service_ready_timeout_ms);

    // Then — ping completes with exit code 0.
    const exit_code = try ping.waitForExit(ping_completion_timeout_ms);
    try std.testing.expectEqual(@as(u32, 0), exit_code);

    // Cleanup — services first, then brokers in reverse start order.
    try harness.stopProcess(echo);
    try harness.stopProcess(broker_b);
    try harness.stopProcess(broker_a);
}

test "cross-broker round-trip latency - 512B" {
    // Given — two brokers on loopback, echo on B, ping on A (512-byte messages).
    // When  — ping sends 100 000 round-trip messages across brokers.
    // Then  — ping exits successfully and writes latency results.
    try runRemoteLatencyBench(std.testing.allocator, "512", "512");
}

test "cross-broker round-trip latency - 1024B" {
    // Given — two brokers on loopback, echo on B, ping on A (1024-byte messages).
    // When  — ping sends 100 000 round-trip messages across brokers.
    // Then  — ping exits successfully and writes latency results.
    try runRemoteLatencyBench(std.testing.allocator, "1024", "1024");
}

test "cross-broker round-trip latency - 4096B" {
    // Given — two brokers on loopback, echo on B, ping on A (4096-byte messages).
    // When  — ping sends 100 000 round-trip messages across brokers.
    // Then  — ping exits successfully and writes latency results.
    try runRemoteLatencyBench(std.testing.allocator, "4096", "4096");
}
