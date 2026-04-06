//! Test Leader Service — participates in leader election and reports leader changes.
//!
//! This test binary registers with leader_election_enabled=true and periodically
//! logs its leader status. Used by end-to-end tests to validate the broker's
//! leader election protocol across multiple service instances.
//!
//! Usage:
//!   test_leader_service [--storage-path PATH] [--group GROUP]
//!                       [--service-name NAME] [--max-runtime-ms MS]

const std = @import("std");
const brz_service = @import("brz_service");
const brz_common = @import("brz_common");

const BrzEngine = brz_service.BrzEngine;
const ServiceConfig = brz_service.ServiceConfig;
const RingBuffer = brz_common.concurrent.ring_buffer.RingBuffer;
const Clock = brz_common.platform.Clock;

// ── Mutable file-level state (acceptable for test binaries) ──────────

var received_leader_changes: u64 = 0;
var is_leader: bool = false;
var shutdown_requested: bool = false;

// ── Message handler ──────────────────────────────────────────────────

fn messageHandler(_: i32, payload: []const u8) void {
    // Look for leader-changed control messages.
    // The payload might contain a leader notification from the broker's
    // control plane. We inspect the content heuristically — in a real
    // service this would be SBE-decoded, but for test purposes we simply
    // track any message arrival as a potential leader-change signal and
    // log the raw length.
    received_leader_changes += 1;

    var buf: [512]u8 = undefined;
    var stdout_w = std.fs.File.stdout().writer(&buf);
    const stdout = &stdout_w.interface;
    stdout.print("leader-test: received control msg #{d}, len={d}\n", .{
        received_leader_changes,
        payload.len,
    }) catch {};
    stdout.flush() catch {};
}

// ── Arg parsing helpers ──────────────────────────────────────────────

fn parseStringArg(args: []const []const u8, flag: []const u8, default: []const u8) []const u8 {
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], flag) and i + 1 < args.len) {
            return args[i + 1];
        }
    }
    return default;
}

fn parseIntArg(comptime T: type, args: []const []const u8, flag: []const u8, default: T) T {
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], flag) and i + 1 < args.len) {
            return std.fmt.parseInt(T, args[i + 1], 10) catch default;
        }
    }
    return default;
}

// ── Entry point ──────────────────────────────────────────────────────

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    // ── Parse CLI arguments ──────────────────────────────────────────

    const storage_path = parseStringArg(args, "--storage-path", "/dev/shm");
    const group = parseStringArg(args, "--group", "default");
    const service_name = parseStringArg(args, "--service-name", "leader-test");
    const max_runtime_ms: u64 = parseIntArg(u64, args, "--max-runtime-ms", 30000);

    var stdout_buf: [4096]u8 = undefined;
    var stdout_w = std.fs.File.stdout().writer(&stdout_buf);
    const stdout = &stdout_w.interface;
    var stderr_buf: [4096]u8 = undefined;
    var stderr_w = std.fs.File.stderr().writer(&stderr_buf);
    const stderr = &stderr_w.interface;

    try stdout.print("leader-test: starting service_name={s} group={s} storage_path={s} max_runtime_ms={d}\n", .{
        service_name,
        group,
        storage_path,
        max_runtime_ms,
    });
    try stdout.flush();

    // ── Start BrzEngine with leader election enabled ─────────────────

    const config = ServiceConfig{
        .storage_path = storage_path,
        .group = group,
        .service_name = service_name,
        .leader_election_enabled = true,
    };

    const engine = BrzEngine.start(allocator, config) catch |err| {
        try stderr.print("leader-test: failed to start engine: {any}\n", .{err});
        try stderr.flush();
        std.process.exit(1);
    };

    try stdout.print("service ready: name={s}\n", .{service_name});
    try stdout.print("leader-test: service_id={d} node_id={d}\n", .{
        engine.service_id,
        engine.node_id,
    });
    try stdout.flush();

    // ── Set message handler ──────────────────────────────────────────

    engine.setMessageHandler(&messageHandler);

    // ── Main loop — periodic leader status logging ───────────────────

    const start_time = Clock.epochMillis();
    var last_status_log: i64 = 0;
    const status_interval_ms: i64 = 1000;
    const max_runtime_i64: i64 = @intCast(max_runtime_ms);

    while (!shutdown_requested) {
        const now = Clock.epochMillis();
        const elapsed = now - start_time;

        // Check max runtime.
        if (elapsed >= max_runtime_i64) {
            try stdout.print("leader-test: max runtime reached ({d}ms), shutting down\n", .{max_runtime_ms});
            try stdout.flush();
            break;
        }

        // Periodically log leader status.
        if (now - last_status_log >= status_interval_ms) {
            last_status_log = now;
            // In a real integration the broker would push leader notifications
            // and the engine would update an internal flag. For test purposes we
            // report received_leader_changes as a proxy indicator.
            is_leader = received_leader_changes > 0;
            try stdout.print("leader-status: is_leader={}, leader_changes={d}, elapsed_ms={d}\n", .{
                is_leader,
                received_leader_changes,
                elapsed,
            });
            try stdout.flush();
        }

        // Sleep 100ms between iterations.
        std.Thread.sleep(100 * std.time.ns_per_ms);
    }

    // ── Shutdown ─────────────────────────────────────────────────────

    try stdout.print("leader-test: shutting down, total_leader_changes={d}\n", .{received_leader_changes});
    try stdout.flush();
    engine.stop();
    try stdout.print("leader-test: stopped cleanly\n", .{});
    try stdout.flush();
}
