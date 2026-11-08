//! Recovery Time Benchmarks
//!
//! Measures how quickly the RingLoom system recovers from failures:
//!
//! 1. **Service recovery** — a service crashes or is killed and the broker
//!    detects the loss via heartbeat timeout. Records the measured detection
//!    time (death → broker logs the heartbeat timeout) and the time until a
//!    replacement service is registered. Detection is bounded below by the
//!    service heartbeat timeout (`service_heartbeat_timeout_ms`, 10 s, in
//!    common/platform/constants.zig), so changing it moves these numbers.
//!
//! 2. **Broker recovery** — in a two-broker topology the remote broker is
//!    stopped and restarted.  The benchmark measures how long until cross-broker
//!    messaging resumes end-to-end.

const std = @import("std");
const platform = @import("ringloom_common").platform;
const Clock = platform.Clock;
const testing_mod = @import("ringloom_testing");
const TestHarness = testing_mod.TestHarness;
const BrokerSpec = testing_mod.BrokerSpec;
const ServiceSpec = testing_mod.ServiceSpec;
const PeerSpec = testing_mod.PeerSpec;
const persistent_results = @import("persistent_results.zig");

const RecoverySummary = struct {
    scenario: []const u8,
    build_mode: []const u8,
    recovery_time_ms: u64,
    iterations: u32 = 1,
    average_recovery_time_ms: u64,
    max_recovery_time_ms: u64,
    total_recovery_time_ms: u64,
    /// Death → broker logs the heartbeat timeout (average over iterations).
    /// 0 for scenarios that do not measure detection.
    detection_time_ms: u64 = 0,
};

/// Broker log line written when a local service misses its heartbeat
/// deadline (service_heartbeat_checker.zig).
const heartbeat_timeout_log = "heartbeat timeout: service";

/// Waits until the broker has logged `heartbeat_timeout_log` at least
/// `occurrences` times and returns the elapsed time since `since_ns`.
/// Polls every millisecond so the measurement resolution is ~1 ms.
fn waitForHeartbeatTimeout(broker: *testing_mod.ProcessHandle, occurrences: usize, since_ns: i64, timeout_ms: u64) !u64 {
    const deadline_ns = Clock.monotonicNanosStable() + @as(i64, @intCast(timeout_ms)) * std.time.ns_per_ms;
    while (Clock.monotonicNanosStable() < deadline_ns) {
        _ = broker.drainAvailableOutput() catch {};
        if (broker.stdout_capture.countOccurrences(heartbeat_timeout_log) >= occurrences) {
            return @intCast(Clock.monotonicNanosStable() - since_ns);
        }
        platform.sleepNanos(std.time.ns_per_ms);
    }
    return error.Timeout;
}

fn writeRecoverySummary(
    allocator: std.mem.Allocator,
    file_path: []const u8,
    summary: RecoverySummary,
) !void {
    const file = if (std.fs.path.isAbsolute(file_path))
        try std.Io.Dir.createFileAbsolute(std.testing.io, file_path, .{})
    else
        try std.Io.Dir.cwd().createFile(std.testing.io, file_path, .{});
    defer file.close(std.testing.io);

    const json = try allocator.print(
        \\{{
        \\  "scenario": "{s}",
        \\  "build_mode": "{s}",
        \\  "recovery_time_ms": {d},
        \\  "iterations": {d},
        \\  "average_recovery_time_ms": {d},
        \\  "max_recovery_time_ms": {d},
        \\  "total_recovery_time_ms": {d},
        \\  "detection_time_ms": {d}
        \\}}
        \\
    , .{
        summary.scenario,
        summary.build_mode,
        summary.recovery_time_ms,
        summary.iterations,
        summary.average_recovery_time_ms,
        summary.max_recovery_time_ms,
        summary.total_recovery_time_ms,
        summary.detection_time_ms,
    });
    defer allocator.free(json);

    try file.writeStreamingAll(std.testing.io, json);
}

fn pingResultShowsFullSuccess(
    allocator: std.mem.Allocator,
    file_path: []const u8,
    expected_sent: u64,
) !bool {
    const content = try std.Io.Dir.cwd().readFileAlloc(
        std.testing.io,
        file_path,
        allocator,
        .limited(16 * 1024),
    );
    defer allocator.free(content);

    const sent_field = try allocator.print("\"sent\": {d}", .{expected_sent});
    defer allocator.free(sent_field);

    return std.mem.indexOf(u8, content, sent_field) != null and
        std.mem.indexOf(u8, content, "\"failed\": 0") != null;
}

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
        .executable_name = "ringloom-test-crashy-service",
        .service_name = "crashy",
        .extra_args = &.{ "--crash-after-ms", "2000" },
    });
    try harness.waitForServiceReady(crashy, 5000);

    // When — the crashy service terminates on its own.
    _ = try crashy.waitForExit(5000);

    const crash_detected_ns = Clock.monotonicNanosStable();

    // Measure how long the broker takes to notice the dead service.
    const detection_ns = try waitForHeartbeatTimeout(broker, 1, crash_detected_ns, 30_000);

    // Start a replacement service under the same logical name.
    const replacement = try harness.startService(.{
        .executable_name = "ringloom-test-echo-service",
        .service_name = "crashy",
        .extra_args = &.{"--quiet"},
    });
    try harness.waitForServiceReady(replacement, 5000);

    const recovery_ns = Clock.monotonicNanosStable() - crash_detected_ns;

    // Then — recovery completes within a reasonable bound.
    const recovery_ms: u64 = @intCast(@divFloor(recovery_ns, std.time.ns_per_ms));
    std.log.info("service recovery time: {}ms", .{recovery_ms});

    // Write a result file for CI dashboards.
    const result_path = try persistent_results.scenarioPath(
        allocator,
        "recovery",
        "service-crash-recovery.json",
    );
    defer allocator.free(result_path);
    try writeRecoverySummary(allocator, result_path, .{
        .scenario = "service-crash-recovery",
        .build_mode = @tagName(@import("builtin").mode),
        .recovery_time_ms = recovery_ms,
        .average_recovery_time_ms = recovery_ms,
        .max_recovery_time_ms = recovery_ms,
        .total_recovery_time_ms = recovery_ms,
        .detection_time_ms = detection_ns / std.time.ns_per_ms,
    });

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
        .executable_name = "ringloom-test-echo-service",
        .service_name = "killable",
        .extra_args = &.{"--quiet"},
    });
    try harness.waitForServiceReady(echo, 5000);

    // When — we forcefully kill the service.
    const kill_ns = Clock.monotonicNanosStable();
    harness.killProcess(echo);

    // Measure how long the broker takes to notice via heartbeat timeout.
    const detection_ns = try waitForHeartbeatTimeout(broker, 1, kill_ns, 30_000);

    // Start a replacement.
    const replacement = try harness.startService(.{
        .executable_name = "ringloom-test-echo-service",
        .service_name = "killable",
        .extra_args = &.{"--quiet"},
    });
    try harness.waitForServiceReady(replacement, 5000);

    const recovery_ns = Clock.monotonicNanosStable() - kill_ns;

    // Then — record the recovery time.
    const recovery_ms: u64 = @intCast(@divFloor(recovery_ns, std.time.ns_per_ms));
    std.log.info("service kill-recovery time: {}ms", .{recovery_ms});

    const result_path = try persistent_results.scenarioPath(
        allocator,
        "recovery",
        "service-kill-recovery.json",
    );
    defer allocator.free(result_path);
    try writeRecoverySummary(allocator, result_path, .{
        .scenario = "service-kill-recovery",
        .build_mode = @tagName(@import("builtin").mode),
        .recovery_time_ms = recovery_ms,
        .average_recovery_time_ms = recovery_ms,
        .max_recovery_time_ms = recovery_ms,
        .total_recovery_time_ms = recovery_ms,
        .detection_time_ms = detection_ns / std.time.ns_per_ms,
    });

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
    platform.sleepNanos(2 * std.time.ns_per_s);

    const echo = try harness.startService(.{
        .executable_name = "ringloom-test-echo-service",
        .service_name = "echo",
        .broker_node_id = 2,
        .extra_args = &.{"--quiet"},
    });
    try harness.waitForServiceReady(echo, 5000);

    // Verify baseline cross-broker messaging works.
    const baseline_result_path = try persistent_results.scenarioPath(
        allocator,
        "recovery",
        "broker-recovery-baseline.json",
    );
    defer allocator.free(baseline_result_path);

    const baseline_ping = try harness.startService(.{
        .executable_name = "ringloom-test-ping-service",
        .service_name = "ping",
        .broker_node_id = 1,
        .extra_args = &.{
            "--target-service", "echo",
            "--message-count",  "1000",
            "--message-size",   "128",
            "--warmup-count",   "100",
            "--result-file",    baseline_result_path,
        },
    });
    try harness.waitForServiceReady(baseline_ping, 5000);

    const baseline_exit = try baseline_ping.waitForExit(30000);
    try std.testing.expectEqual(@as(u32, 0), baseline_exit);
    try std.testing.expect(try pingResultShowsFullSuccess(allocator, baseline_result_path, 1000));

    // When — stop broker B, wait briefly, then restart it.
    try harness.stopProcess(echo);
    try harness.stopProcess(broker_b);

    const stop_ns = Clock.monotonicNanosStable();

    platform.sleepNanos(2 * std.time.ns_per_s);

    broker_b = try harness.startBroker(.{
        .node_id = 2,
        .port = 19002,
        .peers = &.{.{ .node_id = 1, .host = "127.0.0.1", .port = 19001 }},
    });
    try harness.waitForBrokerReady(broker_b, 5000);

    // Let the cluster re-form.
    platform.sleepNanos(3 * std.time.ns_per_s);

    // Re-register echo on the restarted broker.
    const echo2 = try harness.startService(.{
        .executable_name = "ringloom-test-echo-service",
        .service_name = "echo",
        .broker_node_id = 2,
        .extra_args = &.{"--quiet"},
    });
    try harness.waitForServiceReady(echo2, 5000);

    // Then — cross-broker messaging works again.
    const recovered_result_path = try persistent_results.scenarioPath(
        allocator,
        "recovery",
        "broker-recovery-resumed.json",
    );
    defer allocator.free(recovered_result_path);

    const probe_message_count = "100";
    const recovery_deadline_ns = stop_ns + (20 * std.time.ns_per_s);
    var messaging_resumed = false;

    while (Clock.monotonicNanosStable() < recovery_deadline_ns) {
        std.Io.Dir.deleteFileAbsolute(std.testing.io, recovered_result_path) catch |err| switch (err) {
            error.FileNotFound => {},
            else => return err,
        };

        const recovery_ping = try harness.startService(.{
            .executable_name = "ringloom-test-ping-service",
            .service_name = "ping-probe",
            .broker_node_id = 1,
            .extra_args = &.{
                "--target-service", "echo",
                "--message-count",  probe_message_count,
                "--message-size",   "128",
                "--warmup-count",   "0",
                "--result-file",    recovered_result_path,
            },
        });
        try harness.waitForServiceReady(recovery_ping, 5000);

        const recovery_exit = try recovery_ping.waitForExit(30000);
        try std.testing.expectEqual(@as(u32, 0), recovery_exit);

        if (try pingResultShowsFullSuccess(allocator, recovered_result_path, 100)) {
            messaging_resumed = true;
            break;
        }

        platform.sleepNanos(250 * std.time.ns_per_ms);
    }

    try std.testing.expect(messaging_resumed);

    const recovery_ns = Clock.monotonicNanosStable() - stop_ns;
    const recovery_ms: u64 = @intCast(@divFloor(recovery_ns, std.time.ns_per_ms));
    std.log.info("broker recovery time (stop→messaging resumed): {}ms", .{recovery_ms});

    const result_path = try persistent_results.scenarioPath(
        allocator,
        "recovery",
        "broker-restart-recovery.json",
    );
    defer allocator.free(result_path);
    try writeRecoverySummary(allocator, result_path, .{
        .scenario = "broker-restart-recovery",
        .build_mode = @tagName(@import("builtin").mode),
        .recovery_time_ms = recovery_ms,
        .average_recovery_time_ms = recovery_ms,
        .max_recovery_time_ms = recovery_ms,
        .total_recovery_time_ms = recovery_ms,
    });

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

    platform.sleepNanos(2 * std.time.ns_per_s);

    const echo = try harness.startService(.{
        .executable_name = "ringloom-test-echo-service",
        .service_name = "echo",
        .broker_node_id = 1,
        .extra_args = &.{"--quiet"},
    });
    try harness.waitForServiceReady(echo, 5000);

    // When — kill broker B (the remote peer).
    harness.killProcess(broker_b);
    platform.sleepNanos(2 * std.time.ns_per_s);

    // Then — local messaging on broker A still works.
    const result_path = try persistent_results.scenarioPath(
        allocator,
        "recovery",
        "local-survive-remote-kill.json",
    );
    defer allocator.free(result_path);

    const ping = try harness.startService(.{
        .executable_name = "ringloom-test-ping-service",
        .service_name = "ping",
        .broker_node_id = 1,
        .extra_args = &.{
            "--target-service", "echo",
            "--message-count",  "5000",
            "--message-size",   "128",
            "--warmup-count",   "500",
            "--result-file",    result_path,
        },
    });
    try harness.waitForServiceReady(ping, 5000);

    const exit_code = try ping.waitForExit(30000);
    try std.testing.expectEqual(@as(u32, 0), exit_code);
    try std.testing.expect(try pingResultShowsFullSuccess(allocator, result_path, 5000));

    std.log.info("local services survived remote broker kill — messaging intact", .{});

    // Restart broker B and confirm cluster re-forms.
    broker_b = try harness.startBroker(.{
        .node_id = 2,
        .port = 19002,
        .peers = &.{.{ .node_id = 1, .host = "127.0.0.1", .port = 19001 }},
    });
    try harness.waitForBrokerReady(broker_b, 5000);
    platform.sleepNanos(3 * std.time.ns_per_s);

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
    var max_recovery_ns: u64 = 0;
    var total_detection_ns: u64 = 0;

    for (0..iterations) |i| {
        const svc = try harness.startService(.{
            .executable_name = "ringloom-test-echo-service",
            .service_name = "rapid",
            .extra_args = &.{"--quiet"},
        });
        try harness.waitForServiceReady(svc, 5000);

        const iter_start = Clock.monotonicNanosStable();
        harness.killProcess(svc);

        // Wait for this iteration's heartbeat timeout before re-registering.
        total_detection_ns += try waitForHeartbeatTimeout(broker, i + 1, iter_start, 30_000);

        const replacement = try harness.startService(.{
            .executable_name = "ringloom-test-echo-service",
            .service_name = "rapid",
            .extra_args = &.{"--quiet"},
        });
        try harness.waitForServiceReady(replacement, 5000);

        const iter_recovery_ns = Clock.monotonicNanosStable() - iter_start;
        total_recovery_ns += iter_recovery_ns;
        max_recovery_ns = @max(max_recovery_ns, @as(u64, @intCast(iter_recovery_ns)));

        const iter_ms: u64 = @intCast(@divFloor(iter_recovery_ns, std.time.ns_per_ms));
        std.log.info("rapid restart iteration {}: recovery {}ms", .{ i + 1, iter_ms });

        try harness.stopProcess(replacement);
    }

    // Then — record average recovery time.
    const avg_ns: u64 = @intCast(@divFloor(total_recovery_ns, iterations));
    const avg_ms: u64 = avg_ns / std.time.ns_per_ms;
    std.log.info("rapid restart average recovery: {}ms over {} iterations", .{ avg_ms, iterations });

    const result_path = try persistent_results.scenarioPath(
        allocator,
        "recovery",
        "rapid-service-restart.json",
    );
    defer allocator.free(result_path);
    try writeRecoverySummary(allocator, result_path, .{
        .scenario = "rapid-service-restart",
        .build_mode = @tagName(@import("builtin").mode),
        .recovery_time_ms = avg_ms,
        .iterations = iterations,
        .average_recovery_time_ms = avg_ms,
        .max_recovery_time_ms = @intCast(@divFloor(max_recovery_ns, std.time.ns_per_ms)),
        .total_recovery_time_ms = @intCast(@divFloor(total_recovery_ns, std.time.ns_per_ms)),
        .detection_time_ms = total_detection_ns / iterations / std.time.ns_per_ms,
    });

    // Cleanup
    try harness.stopProcess(broker);
}
