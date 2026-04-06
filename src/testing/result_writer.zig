//! Result writer for BRZ end-to-end tests and performance benchmarks.
//!
//! Provides structured result types (`PerfResult`, `CorrectnessResult`)
//! and functions to serialise them as JSON files into a results directory.
//! The JSON output is human-readable (pretty-printed with 4-space indent)
//! so that CI tooling and operators can inspect results easily.
//!
//! File naming convention:
//!   - Performance:  `<dir>/<suite>_<scenario>.json`
//!   - Correctness:  `<dir>/<scenario>.json`

const std = @import("std");
const fs = std.fs;
const mem = std.mem;
const Allocator = std.mem.Allocator;

/// Captures the outcome of a single performance benchmark scenario.
pub const PerfResult = struct {
    suite: []const u8,
    scenario: []const u8,
    build_mode: []const u8,
    message_size_bytes: u32,
    warmup_messages: u32,
    measured_messages: u32,
    throughput_msgs_per_sec: u64,
    throughput_bytes_per_sec: u64,
    latency_p50_ns: u64,
    latency_p95_ns: u64,
    latency_p99_ns: u64,
    latency_max_ns: u64,
    messages_sent: u64,
    messages_received: u64,
    send_failures: u64,
};

/// Captures the outcome of a single correctness / integration test scenario.
pub const CorrectnessResult = struct {
    scenario: []const u8,
    passed: bool,
    elapsed_ms: u64,
    failure_reason: ?[]const u8,
};

/// Writes a `PerfResult` as a pretty-printed JSON file into `dir_path`.
///
/// The file is named `<suite>_<scenario>.json`.  Any existing file with
/// the same name is overwritten.  The returned slice is the full path to
/// the written file (heap-allocated, caller-owned).
pub fn writePerfResult(allocator: Allocator, dir_path: []const u8, result: PerfResult) ![]const u8 {
    const file_name = try std.fmt.allocPrint(allocator, "{s}/{s}_{s}.json", .{
        dir_path,
        result.suite,
        result.scenario,
    });
    errdefer allocator.free(file_name);

    const json = try formatPerfResultJson(allocator, result);
    defer allocator.free(json);

    const file = try fs.cwd().createFile(file_name, .{ .truncate = true });
    defer file.close();
    try file.writeAll(json);

    return file_name;
}

/// Writes a `CorrectnessResult` as a pretty-printed JSON file into `dir_path`.
///
/// The file is named `<scenario>.json`.  Any existing file with the same
/// name is overwritten.  The returned slice is the full path to the
/// written file (heap-allocated, caller-owned).
pub fn writeCorrectnessResult(allocator: Allocator, dir_path: []const u8, result: CorrectnessResult) ![]const u8 {
    const file_name = try std.fmt.allocPrint(allocator, "{s}/{s}.json", .{
        dir_path,
        result.scenario,
    });
    errdefer allocator.free(file_name);

    const json = try formatCorrectnessResultJson(allocator, result);
    defer allocator.free(json);

    const file = try fs.cwd().createFile(file_name, .{ .truncate = true });
    defer file.close();
    try file.writeAll(json);

    return file_name;
}

// ── JSON formatting (manual — avoids std.json limitations) ───────────

fn formatPerfResultJson(allocator: Allocator, r: PerfResult) ![]const u8 {
    return std.fmt.allocPrint(allocator,
        \\{{
        \\    "suite": "{s}",
        \\    "scenario": "{s}",
        \\    "build_mode": "{s}",
        \\    "message_size_bytes": {d},
        \\    "warmup_messages": {d},
        \\    "measured_messages": {d},
        \\    "throughput_msgs_per_sec": {d},
        \\    "throughput_bytes_per_sec": {d},
        \\    "latency_p50_ns": {d},
        \\    "latency_p95_ns": {d},
        \\    "latency_p99_ns": {d},
        \\    "latency_max_ns": {d},
        \\    "messages_sent": {d},
        \\    "messages_received": {d},
        \\    "send_failures": {d}
        \\}}
        \\
    , .{
        r.suite,
        r.scenario,
        r.build_mode,
        r.message_size_bytes,
        r.warmup_messages,
        r.measured_messages,
        r.throughput_msgs_per_sec,
        r.throughput_bytes_per_sec,
        r.latency_p50_ns,
        r.latency_p95_ns,
        r.latency_p99_ns,
        r.latency_max_ns,
        r.messages_sent,
        r.messages_received,
        r.send_failures,
    });
}

fn formatCorrectnessResultJson(allocator: Allocator, r: CorrectnessResult) ![]const u8 {
    const passed_str = if (r.passed) "true" else "false";

    if (r.failure_reason) |reason| {
        return std.fmt.allocPrint(allocator,
            \\{{
            \\    "scenario": "{s}",
            \\    "passed": {s},
            \\    "elapsed_ms": {d},
            \\    "failure_reason": "{s}"
            \\}}
            \\
        , .{
            r.scenario,
            passed_str,
            r.elapsed_ms,
            reason,
        });
    } else {
        return std.fmt.allocPrint(allocator,
            \\{{
            \\    "scenario": "{s}",
            \\    "passed": {s},
            \\    "elapsed_ms": {d},
            \\    "failure_reason": null
            \\}}
            \\
        , .{
            r.scenario,
            passed_str,
            r.elapsed_ms,
        });
    }
}

/// Reads a JSON result file and returns the raw content.  Utility for
/// tests that need to verify written output.
fn readFileContent(allocator: Allocator, path: []const u8) ![]const u8 {
    const file = try fs.cwd().openFile(path, .{});
    defer file.close();
    return file.readToEndAlloc(allocator, 1024 * 1024);
}

// ── Tests ────────────────────────────────────────────────────────────

test "writePerfResult creates valid JSON file" {
    // Given
    const allocator = std.testing.allocator;

    const tmp_dir = "/tmp/brz-test-result-writer-perf";
    try fs.cwd().makePath(tmp_dir);
    defer fs.cwd().deleteTree(tmp_dir) catch {};

    const result = PerfResult{
        .suite = "ipc",
        .scenario = "ping_pong",
        .build_mode = "ReleaseFast",
        .message_size_bytes = 64,
        .warmup_messages = 1000,
        .measured_messages = 100_000,
        .throughput_msgs_per_sec = 2_500_000,
        .throughput_bytes_per_sec = 160_000_000,
        .latency_p50_ns = 450,
        .latency_p95_ns = 1200,
        .latency_p99_ns = 3500,
        .latency_max_ns = 15000,
        .messages_sent = 101_000,
        .messages_received = 101_000,
        .send_failures = 0,
    };

    // When
    const path = try writePerfResult(allocator, tmp_dir, result);
    defer allocator.free(path);

    // Then — file must exist and contain expected fields.
    const content = try readFileContent(allocator, path);
    defer allocator.free(content);

    try std.testing.expect(mem.indexOf(u8, content, "\"suite\": \"ipc\"") != null);
    try std.testing.expect(mem.indexOf(u8, content, "\"scenario\": \"ping_pong\"") != null);
    try std.testing.expect(mem.indexOf(u8, content, "\"build_mode\": \"ReleaseFast\"") != null);
    try std.testing.expect(mem.indexOf(u8, content, "\"message_size_bytes\": 64") != null);
    try std.testing.expect(mem.indexOf(u8, content, "\"throughput_msgs_per_sec\": 2500000") != null);
    try std.testing.expect(mem.indexOf(u8, content, "\"latency_p50_ns\": 450") != null);
    try std.testing.expect(mem.indexOf(u8, content, "\"latency_p99_ns\": 3500") != null);
    try std.testing.expect(mem.indexOf(u8, content, "\"send_failures\": 0") != null);
}

test "writePerfResult file is named suite_scenario.json" {
    // Given
    const allocator = std.testing.allocator;

    const tmp_dir = "/tmp/brz-test-result-writer-name";
    try fs.cwd().makePath(tmp_dir);
    defer fs.cwd().deleteTree(tmp_dir) catch {};

    const result = PerfResult{
        .suite = "throughput",
        .scenario = "broadcast",
        .build_mode = "Debug",
        .message_size_bytes = 128,
        .warmup_messages = 0,
        .measured_messages = 1000,
        .throughput_msgs_per_sec = 500_000,
        .throughput_bytes_per_sec = 64_000_000,
        .latency_p50_ns = 1000,
        .latency_p95_ns = 2000,
        .latency_p99_ns = 5000,
        .latency_max_ns = 20000,
        .messages_sent = 1000,
        .messages_received = 1000,
        .send_failures = 0,
    };

    // When
    const path = try writePerfResult(allocator, tmp_dir, result);
    defer allocator.free(path);

    // Then
    try std.testing.expect(mem.endsWith(u8, path, "/throughput_broadcast.json"));
}

test "writeCorrectnessResult creates JSON for passing test" {
    // Given
    const allocator = std.testing.allocator;

    const tmp_dir = "/tmp/brz-test-result-writer-pass";
    try fs.cwd().makePath(tmp_dir);
    defer fs.cwd().deleteTree(tmp_dir) catch {};

    const result = CorrectnessResult{
        .scenario = "single_broker_two_services",
        .passed = true,
        .elapsed_ms = 1250,
        .failure_reason = null,
    };

    // When
    const path = try writeCorrectnessResult(allocator, tmp_dir, result);
    defer allocator.free(path);

    // Then
    const content = try readFileContent(allocator, path);
    defer allocator.free(content);

    try std.testing.expect(mem.indexOf(u8, content, "\"passed\": true") != null);
    try std.testing.expect(mem.indexOf(u8, content, "\"elapsed_ms\": 1250") != null);
    try std.testing.expect(mem.indexOf(u8, content, "\"failure_reason\": null") != null);
    try std.testing.expect(mem.indexOf(u8, content, "\"scenario\": \"single_broker_two_services\"") != null);
}

test "writeCorrectnessResult creates JSON for failing test" {
    // Given
    const allocator = std.testing.allocator;

    const tmp_dir = "/tmp/brz-test-result-writer-fail";
    try fs.cwd().makePath(tmp_dir);
    defer fs.cwd().deleteTree(tmp_dir) catch {};

    const result = CorrectnessResult{
        .scenario = "broker_failover",
        .passed = false,
        .elapsed_ms = 5000,
        .failure_reason = "service did not reconnect within timeout",
    };

    // When
    const path = try writeCorrectnessResult(allocator, tmp_dir, result);
    defer allocator.free(path);

    // Then
    const content = try readFileContent(allocator, path);
    defer allocator.free(content);

    try std.testing.expect(mem.indexOf(u8, content, "\"passed\": false") != null);
    try std.testing.expect(mem.indexOf(u8, content, "\"failure_reason\": \"service did not reconnect within timeout\"") != null);
}

test "writeCorrectnessResult file is named scenario.json" {
    // Given
    const allocator = std.testing.allocator;

    const tmp_dir = "/tmp/brz-test-result-writer-cname";
    try fs.cwd().makePath(tmp_dir);
    defer fs.cwd().deleteTree(tmp_dir) catch {};

    const result = CorrectnessResult{
        .scenario = "heartbeat_recovery",
        .passed = true,
        .elapsed_ms = 300,
        .failure_reason = null,
    };

    // When
    const path = try writeCorrectnessResult(allocator, tmp_dir, result);
    defer allocator.free(path);

    // Then
    try std.testing.expect(mem.endsWith(u8, path, "/heartbeat_recovery.json"));
}

test "formatPerfResultJson produces valid JSON structure" {
    // Given
    const allocator = std.testing.allocator;

    const result = PerfResult{
        .suite = "test",
        .scenario = "basic",
        .build_mode = "Debug",
        .message_size_bytes = 32,
        .warmup_messages = 10,
        .measured_messages = 100,
        .throughput_msgs_per_sec = 1000,
        .throughput_bytes_per_sec = 32000,
        .latency_p50_ns = 100,
        .latency_p95_ns = 200,
        .latency_p99_ns = 300,
        .latency_max_ns = 500,
        .messages_sent = 110,
        .messages_received = 110,
        .send_failures = 0,
    };

    // When
    const json = try formatPerfResultJson(allocator, result);
    defer allocator.free(json);

    // Then — must start with '{' and end with '}\n'.
    try std.testing.expect(json.len > 2);
    try std.testing.expectEqual(@as(u8, '{'), json[0]);
    try std.testing.expect(mem.endsWith(u8, json, "}\n"));
}

test "formatCorrectnessResultJson with null failure_reason" {
    // Given
    const allocator = std.testing.allocator;

    const result = CorrectnessResult{
        .scenario = "null_check",
        .passed = true,
        .elapsed_ms = 42,
        .failure_reason = null,
    };

    // When
    const json = try formatCorrectnessResultJson(allocator, result);
    defer allocator.free(json);

    // Then
    try std.testing.expect(mem.indexOf(u8, json, "\"failure_reason\": null") != null);
    // Must NOT contain the string literal "null" in quotes — it should be
    // the JSON keyword null.
    try std.testing.expect(mem.indexOf(u8, json, "\"failure_reason\": \"null\"") == null);
}

test "formatCorrectnessResultJson with failure_reason present" {
    // Given
    const allocator = std.testing.allocator;

    const result = CorrectnessResult{
        .scenario = "reason_check",
        .passed = false,
        .elapsed_ms = 99,
        .failure_reason = "timeout waiting for broker",
    };

    // When
    const json = try formatCorrectnessResultJson(allocator, result);
    defer allocator.free(json);

    // Then
    try std.testing.expect(mem.indexOf(u8, json, "\"failure_reason\": \"timeout waiting for broker\"") != null);
}
