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
const builtin = @import("builtin");
const ringloom_service = @import("ringloom_service");
const ringloom_common = @import("ringloom_common");

const RingLoomEngine = ringloom_service.RingLoomEngine;
const ServiceConfig = ringloom_service.ServiceConfig;
const RingBuffer = ringloom_common.concurrent.ring_buffer.RingBuffer;
const Clock = ringloom_common.platform.Clock;

// ── Mutable file-level state (acceptable for test binaries) ──────────

var received_leader_changes: u64 = 0;
var is_leader: bool = false;
var shutdown_requested: std.atomic.Value(bool) = std.atomic.Value(bool).init(false);
var runtime_io: std.Io = undefined;

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
    var stdout_w = std.Io.File.stdout().writer(runtime_io, &buf);
    const stdout = &stdout_w.interface;
    stdout.print("leader-test: received control msg #{d}, len={d}\n", .{
        received_leader_changes,
        payload.len,
    }) catch {};
    stdout.flush() catch {};
}

// ── Arg parsing helpers ──────────────────────────────────────────────

fn parseStringArg(args: []const [:0]const u8, flag: []const u8, default: []const u8) []const u8 {
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], flag) and i + 1 < args.len) {
            return args[i + 1];
        }
    }
    return default;
}

fn parseIntArg(comptime T: type, args: []const [:0]const u8, flag: []const u8, default: T) T {
    var i: usize = 0;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], flag) and i + 1 < args.len) {
            return std.fmt.parseInt(T, args[i + 1], 10) catch default;
        }
    }
    return default;
}

// ── Signal handling ──────────────────────────────────────────────────

fn installSignalHandler() void {
    const handler: std.posix.Sigaction = .{
        .handler = .{ .handler = signalHandler },
        .mask = std.posix.sigemptyset(),
        .flags = 0,
    };
    std.posix.sigaction(std.posix.SIG.TERM, &handler, null);
    std.posix.sigaction(std.posix.SIG.INT, &handler, null);
}

fn signalHandler(_: std.posix.SIG) callconv(.c) void {
    shutdown_requested.store(true, .release);
}

// ── Entry point ──────────────────────────────────────────────────────

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    runtime_io = io;
    var debug_alloc: std.heap.DebugAllocator(.{}) = .init;
    const allocator = switch (builtin.mode) {
        .Debug, .ReleaseSafe => debug_alloc.allocator(),
        .ReleaseFast, .ReleaseSmall => std.heap.smp_allocator,
    };
    defer if (builtin.mode == .Debug or builtin.mode == .ReleaseSafe) {
        _ = debug_alloc.deinit();
    };

    const args = try init.minimal.args.toSlice(init.arena.allocator());

    // ── Parse CLI arguments ──────────────────────────────────────────

    const storage_path = parseStringArg(args, "--storage-path", "/dev/shm");
    const group = parseStringArg(args, "--group", "default");
    const service_name = parseStringArg(args, "--service-name", "leader-test");
    const broker_node_id: i16 = @intCast(parseIntArg(u64, args, "--broker-node-id", 1));
    const max_runtime_ms: u64 = parseIntArg(u64, args, "--max-runtime-ms", 30000);

    var stdout_buf: [4096]u8 = undefined;
    var stdout_w = std.Io.File.stdout().writer(io, &stdout_buf);
    const stdout = &stdout_w.interface;
    var stderr_buf: [4096]u8 = undefined;
    var stderr_w = std.Io.File.stderr().writer(io, &stderr_buf);
    const stderr = &stderr_w.interface;

    try stdout.print("leader-test: starting service_name={s} group={s} storage_path={s} max_runtime_ms={d}\n", .{
        service_name,
        group,
        storage_path,
        max_runtime_ms,
    });
    try stdout.flush();

    // ── Start RingLoomEngine with leader election enabled ─────────────────

    const config = ServiceConfig{
        .storage_path = storage_path,
        .group = group,
        .service_name = service_name,
        .broker_node_id = broker_node_id,
        .leader_election_enabled = true,
    };

    const engine = RingLoomEngine.start(allocator, config) catch |err| {
        try stderr.print("leader-test: failed to start engine: {any}\n", .{err});
        try stderr.flush();
        std.process.exit(1);
    };
    defer engine.deinit();

    try stdout.print("service ready: name={s}\n", .{service_name});
    try stdout.print("leader-test: service_id={d} node_id={d}\n", .{
        engine.service_id,
        engine.node_id,
    });
    try stdout.flush();

    // ── Set message handler ──────────────────────────────────────────

    engine.setMessageHandler(&messageHandler);
    installSignalHandler();

    // ── Main loop — periodic leader status logging ───────────────────

    const start_time = Clock.epochMillis();
    var last_status_log: i64 = 0;
    const status_interval_ms: i64 = 1000;
    const max_runtime_i64: i64 = @intCast(max_runtime_ms);

    while (!shutdown_requested.load(.acquire)) {
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
        ringloom_common.platform.sleepNanos(100 * std.time.ns_per_ms);
    }

    // ── Shutdown ─────────────────────────────────────────────────────

    try stdout.print("leader-test: shutting down, total_leader_changes={d}\n", .{received_leader_changes});
    try stdout.flush();
    engine.stop();
    try stdout.print("leader-test: stopped cleanly\n", .{});
    try stdout.flush();
}
