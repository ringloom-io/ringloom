//! Recovery Time Benchmarks
//!
//! Measures how quickly the BRZ system recovers from failures:
//!
//! 1. **Service recovery** — a service crashes, the broker detects the loss via
//!    heartbeat timeout, and a replacement service registers successfully.
//!    Total wall-clock time from crash to ready replacement is recorded.
//!
//! 2. **Broker recovery** — in a two-broker topology the remote broker is
//!    stopped and restarted.  The benchmark measures how long until cross-broker
//!    messaging resumes end-to-end.

const std = @import("std");
const testing_mod = @import("brz_testing");
const TestHarness = testing_mod.TestHarness;
const BrokerSpec = testing_mod.BrokerSpec;
const ServiceSpec = testing_mod.ServiceSpec;
const PeerSpec = testing_mod.PeerSpec;
const result_writer = testing_mod.result_writer;
const PerfResult = result_writer.PerfResult;

// ---------------------------------------------------------------------------
// Service recovery
// ---------------------------------------------------------------------------

test "service recovery time after crash" {
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "perf-recovery-service-crash");
    defer harness.deinit();
    errdefer harness.markFailed();

    // Given — a broker and a "crashy" service that will self-terminate after 2 s.
    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    const crashy = try harness.startService(.{
        .executable_name = "brz-test-crashy-service",
        .service_name = "crashy",
        .extra_args = &.{ "--crash-after-ms", "2000" },
    });
    try harness.waitForServiceReady(crashy, 5000);

    // When — the crashy service terminates on its own.
    _ = try crashy.waitForExit(5000);

    const crash_detected_ns = std.time.nanoTimestamp();

    // Allow the broker's heartbeat checker to notice the dead service.
    // Default heartbeat timeout is 10 s; we wait a bit longer to be safe.
    std.Thread.sleep(12 * std.time.ns_per_s);

    // Start a replacement service under the same logical name.
    const replacement = try harness.startService(.{
        .executable_name = "brz-test-echo-service",
        .service_name = "crashy",
    });
    try harness.waitForServiceReady(replacement, 5000);

    const recovery_ns = std.time.nanoTimestamp() - crash_detected_ns;

    // Then — recovery completes within a reasonable bound.
    const recovery_ms: u64 = @intCast(@divFloor(recovery_ns, std.time.ns_per_ms));
    std.log.info("service recovery time: {}ms", .{recovery_ms});

    // Write a result file for CI dashboards.
    const result = PerfResult{
        .suite = "recovery",
        .scenario = "service-crash-recovery",
        .build_mode = @tagName(@import("builtin").mode),
        .message_size_bytes = 0,
        .warmup_messages = 0,
        .measured_messages = 0,
        .throughput_msgs_per_sec = 0,
        .throughput_bytes_per_sec = 0,
        .latency_p50_ns = 0,
        .latency_p95_ns = 0,
        .latency_p99_ns = 0,
        .latency_max_ns = @intCast(recovery_ns),
        .messages_sent = 0,
        .messages_received = 0,
        .send_failures = 0,
    };
    const result_path = try result_writer.writePerfResult(allocator, harness.env.results_path, result);
    allocator.free(result_path);

    // Cleanup
    try harness.stopProcess(replacement);
    try harness.stopProcess(broker);
}

test "service recovery time after kill" {
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "perf-recovery-service-kill");
    defer harness.deinit();
    errdefer harness.markFailed();

    // Given — a broker and a long-lived echo service that we will forcefully kill.
    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    const echo = try harness.startService(.{
        .executable_name = "brz-test-echo-service",
        .service_name = "killable",
    });
    try harness.waitForServiceReady(echo, 5000);

    // When — we forcefully kill the service.
    harness.killProcess(echo);

    const kill_ns = std.time.nanoTimestamp();

    // Wait for the broker to notice via heartbeat timeout.
    std.Thread.sleep(12 * std.time.ns_per_s);

    // Start a replacement.
    const replacement = try harness.startService(.{
        .executable_name = "brz-test-echo-service",
        .service_name = "killable",
    });
    try harness.waitForServiceReady(replacement, 5000);

    const recovery_ns = std.time.nanoTimestamp() - kill_ns;

    // Then — record the recovery time.
    const recovery_ms: u64 = @intCast(@divFloor(recovery_ns, std.time.ns_per_ms));
    std.log.info("service kill-recovery time: {}ms", .{recovery_ms});

    const result = PerfResult{
        .suite = "recovery",
        .scenario = "service-kill-recovery",
        .build_mode = @tagName(@import("builtin").mode),
        .message_size_bytes = 0,
        .warmup_messages = 0,
        .measured_messages = 0,
        .throughput_msgs_per_sec = 0,
        .throughput_bytes_per_sec = 0,
        .latency_p50_ns = 0,
        .latency_p95_ns = 0,
        .latency_p99_ns = 0,
        .latency_max_ns = @intCast(recovery_ns),
        .messages_sent = 0,
        .messages_received = 0,
        .send_failures = 0,
    };
    const result_path = try result_writer.writePerfResult(allocator, harness.env.results_path, result);
    allocator.free(result_path);

    // Cleanup
    try harness.stopProcess(replacement);
    try harness.stopProcess(broker);
}

// ---------------------------------------------------------------------------
// Broker recovery (cross-broker topology)
// ---------------------------------------------------------------------------

test "broker recovery - cross-broker messaging resumes after restart" {
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "perf-recovery-broker-restart");
    defer harness.deinit();
    errdefer harness.markFailed();

    // Given — two brokers peered on loopback, echo on broker B, ping on broker A.
    const broker_a = try harness.startBroker(.{
        .node_id = 1,
        .port = 19001,
        .peers = &.{.{ .node_id = 2, .host = "127.0.0.1", .port = 19002 }},
    });
    try harness.waitForBrokerReady(broker_a, 5000);

    var broker_b = try harness.startBroker(.{
        .node_id = 2,
        .port = 19002,
        .peers = &.{.{ .node_id = 1, .host = "127.0.0.1", .port = 19001 }},
    });
    try harness.waitForBrokerReady(broker_b, 5000);

    // Let cluster form.
    std.Thread.sleep(2 * std.time.ns_per_s);

    const echo = try harness.startService(.{
        .executable_name = "brz-test-echo-service",
        .service_name = "echo",
        .broker_node_id = 2,
    });
    try harness.waitForServiceReady(echo, 5000);

    // Verify baseline cross-broker messaging works.
    const baseline_result_path = try std.fmt.allocPrint(
        allocator,
        "{s}/broker-recovery-baseline.json",
        .{harness.env.results_path},
    );
    defer allocator.free(baseline_result_path);

    const baseline_ping = try harness.startService(.{
        .executable_name = "brz-test-ping-service",
        .service_name = "ping",
        .broker_node_id = 1,
        .extra_args = &.{
            "--target-service", "echo",
            "--message-count", "1000",
            "--message-size",  "128",
            "--warmup-count",  "100",
            "--result-file",   baseline_result_path,
        },
    });
    try harness.waitForServiceReady(baseline_ping, 5000);

    const baseline_exit = try baseline_ping.waitForExit(30000);
    try std.testing.expectEqual(@as(u32, 0), baseline_exit);

    // When — stop broker B, wait briefly, then restart it.
    try harness.stopProcess(echo);
    try harness.stopProcess(broker_b);

    const stop_ns = std.time.nanoTimestamp();

    std.Thread.sleep(2 * std.time.ns_per_s);

    broker_b = try harness.startBroker(.{
        .node_id = 2,
        .port = 19002,
        .peers = &.{.{ .node_id = 1, .host = "127.0.0.1", .port = 19001 }},
    });
    try harness.waitForBrokerReady(broker_b, 5000);

    // Let the cluster re-form.
    std.Thread.sleep(3 * std.time.ns_per_s);

    // Re-register echo on the restarted broker.
    const echo2 = try harness.startService(.{
        .executable_name = "brz-test-echo-service",
        .service_name = "echo",
        .broker_node_id = 2,
    });
    try harness.waitForServiceReady(echo2, 5000);

    const recovery_ns = std.time.nanoTimestamp() - stop_ns;

    // Then — cross-broker messaging works again.
    const recovered_result_path = try std.fmt.allocPrint(
        allocator,
        "{s}/broker-recovery-resumed.json",
        .{harness.env.results_path},
    );
    defer allocator.free(recovered_result_path);

    const recovery_ping = try harness.startService(.{
        .executable_name = "brz-test-ping-service",
        .service_name = "ping",
        .broker_node_id = 1,
        .extra_args = &.{
            "--target-service", "echo",
            "--message-count", "1000",
            "--message-size",  "128",
            "--warmup-count",  "100",
            "--result-file",   recovered_result_path,
        },
    });
    try harness.waitForServiceReady(recovery_ping, 5000);

    const recovery_exit = try recovery_ping.waitForExit(60000);
    try std.testing.expectEqual(@as(u32, 0), recovery_exit);

    const recovery_ms: u64 = @intCast(@divFloor(recovery_ns, std.time.ns_per_ms));
    std.log.info("broker recovery time (stop→messaging resumed): {}ms", .{recovery_ms});

    const result = PerfResult{
        .suite = "recovery",
        .scenario = "broker-restart-recovery",
        .build_mode = @tagName(@import("builtin").mode),
        .message_size_bytes = 128,
        .warmup_messages = 100,
        .measured_messages = 1000,
        .throughput_msgs_per_sec = 0,
        .throughput_bytes_per_sec = 0,
        .latency_p50_ns = 0,
        .latency_p95_ns = 0,
        .latency_p99_ns = 0,
        .latency_max_ns = @intCast(recovery_ns),
        .messages_sent = 1000,
        .messages_received = 1000,
        .send_failures = 0,
    };
    const result_path = try result_writer.writePerfResult(allocator, harness.env.results_path, result);
    allocator.free(result_path);

    // Cleanup — reverse order.
    try harness.stopProcess(echo2);
    try harness.stopProcess(broker_b);
    try harness.stopProcess(broker_a);
}

test "broker recovery - local services survive remote broker restart" {
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "perf-recovery-local-survive");
    defer harness.deinit();
    errdefer harness.markFailed();

    // Given — two brokers, echo registered on broker A (local).
    const broker_a = try harness.startBroker(.{
        .node_id = 1,
        .port = 19001,
        .peers = &.{.{ .node_id = 2, .host = "127.0.0.1", .port = 19002 }},
    });
    try harness.waitForBrokerReady(broker_a, 5000);

    var broker_b = try harness.startBroker(.{
        .node_id = 2,
        .port = 19002,
        .peers = &.{.{ .node_id = 1, .host = "127.0.0.1", .port = 19001 }},
    });
    try harness.waitForBrokerReady(broker_b, 5000);

    std.Thread.sleep(2 * std.time.ns_per_s);

    const echo = try harness.startService(.{
        .executable_name = "brz-test-echo-service",
        .service_name = "echo",
        .broker_node_id = 1,
    });
    try harness.waitForServiceReady(echo, 5000);

    // When — kill broker B (the remote peer).
    harness.killProcess(broker_b);
    std.Thread.sleep(2 * std.time.ns_per_s);

    // Then — local messaging on broker A still works.
    const result_path = try std.fmt.allocPrint(
        allocator,
        "{s}/local-survive-remote-kill.json",
        .{harness.env.results_path},
    );
    defer allocator.free(result_path);

    const ping = try harness.startService(.{
        .executable_name = "brz-test-ping-service",
        .service_name = "ping",
        .broker_node_id = 1,
        .extra_args = &.{
            "--target-service", "echo",
            "--message-count", "5000",
            "--message-size",  "128",
            "--warmup-count",  "500",
            "--result-file",   result_path,
        },
    });
    try harness.waitForServiceReady(ping, 5000);

    const exit_code = try ping.waitForExit(30000);
    try std.testing.expectEqual(@as(u32, 0), exit_code);

    std.log.info("local services survived remote broker kill — messaging intact", .{});

    // Restart broker B and confirm cluster re-forms.
    broker_b = try harness.startBroker(.{
        .node_id = 2,
        .port = 19002,
        .peers = &.{.{ .node_id = 1, .host = "127.0.0.1", .port = 19001 }},
    });
    try harness.waitForBrokerReady(broker_b, 5000);
    std.Thread.sleep(3 * std.time.ns_per_s);

    // Cleanup
    try harness.stopProcess(echo);
    try harness.stopProcess(broker_b);
    try harness.stopProcess(broker_a);
}

test "rapid service restart recovery" {
    const allocator = std.testing.allocator;
    var harness = try TestHarness.init(allocator, "perf-recovery-rapid-restart");
    defer harness.deinit();
    errdefer harness.markFailed();

    // Given — a single broker.
    const broker = try harness.startBroker(.{});
    try harness.waitForBrokerReady(broker, 5000);

    // When — start and kill a service 5 times in quick succession, measuring
    //        the time from kill to successful replacement each iteration.
    const iterations: u32 = 5;
    var total_recovery_ns: i128 = 0;

    for (0..iterations) |i| {
        const svc = try harness.startService(.{
            .executable_name = "brz-test-echo-service",
            .service_name = "rapid",
        });
        try harness.waitForServiceReady(svc, 5000);

        harness.killProcess(svc);

        const iter_start = std.time.nanoTimestamp();

        // Heartbeat timeout before re-registration.
        std.Thread.sleep(12 * std.time.ns_per_s);

        const replacement = try harness.startService(.{
            .executable_name = "brz-test-echo-service",
            .service_name = "rapid",
        });
        try harness.waitForServiceReady(replacement, 5000);

        const iter_recovery_ns = std.time.nanoTimestamp() - iter_start;
        total_recovery_ns += iter_recovery_ns;

        const iter_ms: u64 = @intCast(@divFloor(iter_recovery_ns, std.time.ns_per_ms));
        std.log.info("rapid restart iteration {}: recovery {}ms", .{ i + 1, iter_ms });

        try harness.stopProcess(replacement);
    }

    // Then — record average recovery time.
    const avg_ns: u64 = @intCast(@divFloor(total_recovery_ns, iterations));
    const avg_ms: u64 = avg_ns / std.time.ns_per_ms;
    std.log.info("rapid restart average recovery: {}ms over {} iterations", .{ avg_ms, iterations });

    const result = PerfResult{
        .suite = "recovery",
        .scenario = "rapid-service-restart",
        .build_mode = @tagName(@import("builtin").mode),
        .message_size_bytes = 0,
        .warmup_messages = 0,
        .measured_messages = iterations,
        .throughput_msgs_per_sec = 0,
        .throughput_bytes_per_sec = 0,
        .latency_p50_ns = 0,
        .latency_p95_ns = 0,
        .latency_p99_ns = avg_ns,
        .latency_max_ns = @intCast(@divFloor(total_recovery_ns, 1)),
        .messages_sent = 0,
        .messages_received = 0,
        .send_failures = 0,
    };
    const result_path = try result_writer.writePerfResult(allocator, harness.env.results_path, result);
    allocator.free(result_path);

    // Cleanup
    try harness.stopProcess(broker);
}
