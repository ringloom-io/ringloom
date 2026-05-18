// SPDX-License-Identifier: Apache-2.0
//! Plain Aeron remote transit benchmark.
//!
//! This benchmark bypasses RingLoom entirely and sends timestamped messages over
//! Aeron UDP loopback between two embedded media drivers. Each driver runs in
//! DEDICATED mode with separate conductor, sender, and receiver agent threads.

const std = @import("std");
const ringloom_aeron = @import("ringloom_aeron");
const ringloom_common = @import("ringloom_common");
const ringloom_testing = @import("ringloom_testing");

const Clock = ringloom_common.Clock;
const Histogram = ringloom_testing.Histogram;
const latency_trace = ringloom_common.message.latency_trace;

const output_dir = "/tmp/ringloom-plain-aeron-bench-results";
const send_interval_ns: u64 = 10_000;
const poll_fragment_limit: usize = 256;
const spin_timeout_ns: i64 = 100 * std.time.ns_per_ms;
const connection_timeout_ns: i64 = 10 * std.time.ns_per_s;
const completion_timeout_ns: i64 = 120 * std.time.ns_per_s;
const mtu_length: usize = 8192;
const ipc_mtu_length: usize = 8192;
const network_publication_max_messages_per_send: u32 = 16;

const MessageSizeConfig = struct {
    size: usize,
    label: []const u8,
    port: u16,
    warmup_count: u64 = 10_000,
    measured_count: u64 = 100_000,
};

const message_configs = [_]MessageSizeConfig{
    .{ .size = 32, .label = "32", .port = 22132 },
    .{ .size = 128, .label = "128", .port = 22228 },
    .{ .size = 512, .label = "512", .port = 22612 },
    .{ .size = 1024, .label = "1024", .port = 23124 },
    .{ .size = 4096, .label = "4096", .port = 26196, .warmup_count = 5_000, .measured_count = 50_000 },
};

const AgentLoop = struct {
    invoker: *ringloom_aeron.AgentInvoker,
    running: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    failed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    fn run(self: *AgentLoop) void {
        while (self.running.load(.acquire)) {
            const work = self.invoker.invoke() catch {
                self.failed.store(true, .release);
                return;
            };
            if (work == 0) {
                std.Thread.yield() catch {};
            }
        }
    }
};

const DedicatedDriverThreads = struct {
    conductor_loop: AgentLoop,
    sender_loop: AgentLoop,
    receiver_loop: AgentLoop,
    conductor_thread: ?std.Thread = null,
    sender_thread: ?std.Thread = null,
    receiver_thread: ?std.Thread = null,

    fn init(prefix: []const u8, agents: *ringloom_aeron.DriverAgents) DedicatedDriverThreads {
        _ = prefix;
        const dedicated = switch (agents.*) {
            .dedicated => |*value| value,
            else => unreachable,
        };

        return .{
            .conductor_loop = .{ .invoker = &dedicated.conductor },
            .sender_loop = .{ .invoker = &dedicated.sender },
            .receiver_loop = .{ .invoker = &dedicated.receiver },
        };
    }

    fn start(self: *DedicatedDriverThreads) !void {
        self.conductor_loop.running.store(true, .release);
        self.sender_loop.running.store(true, .release);
        self.receiver_loop.running.store(true, .release);
        self.conductor_thread = try std.Thread.spawn(.{}, AgentLoop.run, .{&self.conductor_loop});
        errdefer self.stopAndJoin();
        self.sender_thread = try std.Thread.spawn(.{}, AgentLoop.run, .{&self.sender_loop});
        errdefer self.stopAndJoin();
        self.receiver_thread = try std.Thread.spawn(.{}, AgentLoop.run, .{&self.receiver_loop});
    }

    fn stopAndJoin(self: *DedicatedDriverThreads) void {
        self.sender_loop.running.store(false, .release);
        self.receiver_loop.running.store(false, .release);
        self.conductor_loop.running.store(false, .release);
        if (self.sender_thread) |thread| {
            thread.join();
            self.sender_thread = null;
        }
        if (self.receiver_thread) |thread| {
            thread.join();
            self.receiver_thread = null;
        }
        if (self.conductor_thread) |thread| {
            thread.join();
            self.conductor_thread = null;
        }
    }

    fn failed(self: *const DedicatedDriverThreads) bool {
        return self.conductor_loop.failed.load(.acquire) or
            self.sender_loop.failed.load(.acquire) or
            self.receiver_loop.failed.load(.acquire);
    }
};

const EchoState = struct {
    client: *ringloom_aeron.Client,
    subscription: *ringloom_aeron.Subscription,
    histogram: *Histogram,
    expected_total: u64,
    total_received: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    total_measured: std.atomic.Value(u64) = std.atomic.Value(u64).init(0),
    stop: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),
    failed: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    fn onFragment(context: ?*anyopaque, bytes: []const u8) void {
        const self: *@This() = @ptrCast(@alignCast(context.?));
        _ = self.total_received.fetchAdd(1, .monotonic);
        if (!latency_trace.isMeasured(bytes)) return;

        const send_ts = latency_trace.readSendTimestamp(bytes) orelse return;
        const recv_ts: u64 = @intCast(Clock.monotonicNanosStable());
        if (recv_ts <= send_ts) return;

        self.histogram.record(recv_ts - send_ts) catch {
            self.failed.store(true, .release);
            return;
        };
        _ = self.total_measured.fetchAdd(1, .monotonic);
    }

    fn run(self: *@This()) void {
        const handler = ringloom_aeron.FragmentHandler{
            .context = self,
            .callback = onFragment,
        };

        while (!self.stop.load(.acquire)) {
            _ = self.client.invokeConductor() catch {
                self.failed.store(true, .release);
                return;
            };
            const fragments = self.subscription.poll(handler, poll_fragment_limit) catch {
                self.failed.store(true, .release);
                return;
            };
            if (self.total_received.load(.acquire) >= self.expected_total) {
                return;
            }
            if (fragments == 0) {
                std.Thread.yield() catch {};
            }
        }
    }
};

fn runPlainAeronRemoteBench(allocator: std.mem.Allocator, cfg: MessageSizeConfig) !void {
    try std.Io.Dir.cwd().createDirPath(std.testing.io, output_dir);

    var dir_a_buf: [128]u8 = undefined;
    const dir_a = try std.fmt.bufPrintSentinel(
        &dir_a_buf,
        "/tmp/ringloom-plain-aeron-{s}-{d}-a",
        .{ cfg.label, Clock.monotonicNanosStable() },
        0,
    );
    var dir_b_buf: [128]u8 = undefined;
    const dir_b = try std.fmt.bufPrintSentinel(
        &dir_b_buf,
        "/tmp/ringloom-plain-aeron-{s}-{d}-b",
        .{ cfg.label, Clock.monotonicNanosStable() },
        0,
    );

    var driver_a = try startDriver(dir_a);
    defer driver_a.deinit();
    var agents_a = try driver_a.agents(.dedicated);

    var driver_b = try startDriver(dir_b);
    defer driver_b.deinit();
    var agents_b = try driver_b.agents(.dedicated);

    var ping_client = try ringloom_aeron.Client.connect(.{
        .directory = dir_a,
        .use_conductor_agent_invoker = true,
        .driver_timeout_ms = 5000,
    });
    var cleanup_owns_aeron_resources = false;
    errdefer if (!cleanup_owns_aeron_resources) ping_client.deinit();

    var echo_client = try ringloom_aeron.Client.connect(.{
        .directory = dir_b,
        .use_conductor_agent_invoker = true,
        .driver_timeout_ms = 5000,
    });
    errdefer if (!cleanup_owns_aeron_resources) echo_client.deinit();

    var channel_buf: [128]u8 = undefined;
    const channel = try ringloom_aeron.ChannelUri.udpEndpoint(&channel_buf, "127.0.0.1", cfg.port, null);
    const stream_id: i32 = 10_001;

    var subscription = try echo_client.addSubscription(channel, stream_id, &agents_b);
    errdefer if (!cleanup_owns_aeron_resources) subscription.close() catch {};

    var publication = try ping_client.addExclusivePublication(channel, stream_id, &agents_a);
    errdefer if (!cleanup_owns_aeron_resources) publication.close() catch {};

    var threads_a = DedicatedDriverThreads.init("aeron-a", &agents_a);
    var threads_b = DedicatedDriverThreads.init("aeron-b", &agents_b);
    try threads_a.start();
    errdefer threads_a.stopAndJoin();
    try threads_b.start();
    errdefer threads_b.stopAndJoin();
    cleanup_owns_aeron_resources = true;
    defer {
        publication.close() catch {};
        subscription.close() catch {};
        ping_client.deinit();
        echo_client.deinit();
        ringloom_common.platform.sleepNanos(100 * std.time.ns_per_ms);
        threads_b.stopAndJoin();
        threads_a.stopAndJoin();
    }

    try waitForConnected(&publication, &ping_client, &echo_client);

    var histogram = try Histogram.initCapacity(allocator, @intCast(cfg.measured_count));
    defer histogram.deinit();

    var echo_state = EchoState{
        .client = &echo_client,
        .subscription = &subscription,
        .histogram = &histogram,
        .expected_total = cfg.warmup_count + cfg.measured_count,
    };
    const echo_thread = try std.Thread.spawn(.{}, EchoState.run, .{&echo_state});
    var echo_joined = false;
    errdefer if (!echo_joined) {
        echo_state.stop.store(true, .release);
        echo_thread.join();
    };

    const payload = try allocator.alloc(u8, cfg.size);
    defer allocator.free(payload);
    @memset(payload, 0xAB);

    const send_start_ns = Clock.monotonicNanosStable();
    var next_deadline_ns: i64 = 0;
    var sent: u64 = 0;
    var send_failures: u64 = 0;

    for (0..cfg.warmup_count) |_| {
        paceNextSend(&next_deadline_ns, send_interval_ns);
        latency_trace.embedSend(payload, latency_trace.warmup_phase, @intCast(Clock.monotonicNanosStable()));
        sendPlainAeron(&publication, &ping_client, &echo_client, payload) catch {
            send_failures += 1;
            continue;
        };
        sent += 1;
    }

    next_deadline_ns = 0;
    for (0..cfg.measured_count) |_| {
        paceNextSend(&next_deadline_ns, send_interval_ns);
        latency_trace.embedSend(payload, latency_trace.measured_phase, @intCast(Clock.monotonicNanosStable()));
        sendPlainAeron(&publication, &ping_client, &echo_client, payload) catch {
            send_failures += 1;
            continue;
        };
        sent += 1;
    }
    const send_elapsed_ns: u64 = @intCast(@max(Clock.monotonicNanosStable() - send_start_ns, 1));

    waitForEchoCompletion(&echo_state) catch |err| {
        echo_state.stop.store(true, .release);
        echo_thread.join();
        return err;
    };
    echo_state.stop.store(true, .release);
    echo_thread.join();
    echo_joined = true;

    if (echo_state.failed.load(.acquire) or threads_a.failed() or threads_b.failed()) {
        return error.AeronCallFailed;
    }
    try std.testing.expectEqual(cfg.measured_count, echo_state.total_measured.load(.acquire));

    const result_path = try std.fmt.allocPrint(
        allocator,
        "{s}/plain-aeron-remote-transit-{s}B.json",
        .{ output_dir, cfg.label },
    );
    defer allocator.free(result_path);

    try writePlainAeronResults(
        result_path,
        cfg,
        &histogram,
        echo_state.total_received.load(.acquire),
        echo_state.total_measured.load(.acquire),
        sent,
        send_failures,
        send_elapsed_ns,
    );
    std.debug.print("plain Aeron result: {s}\n", .{result_path});
}

fn startDriver(directory: [:0]const u8) !ringloom_aeron.Driver {
    return ringloom_aeron.Driver.initEmbedded(.{
        .directory = directory,
        .delete_dir_on_start = true,
        .delete_dir_on_shutdown = true,
        .mtu_length = mtu_length,
        .ipc_mtu_length = ipc_mtu_length,
        .term_buffer_sparse_file = false,
        .network_publication_max_messages_per_send = network_publication_max_messages_per_send,
    }, .dedicated);
}

fn waitForConnected(
    publication: *ringloom_aeron.ExclusivePublication,
    ping_client: *ringloom_aeron.Client,
    echo_client: *ringloom_aeron.Client,
) !void {
    const start = Clock.monotonicNanosStable();
    while (Clock.monotonicNanosStable() - start < connection_timeout_ns) {
        _ = try ping_client.invokeConductor();
        _ = try echo_client.invokeConductor();
        if (publication.isConnected()) return;
        std.Thread.yield() catch {};
    }
    return error.Timeout;
}

fn sendPlainAeron(
    publication: *ringloom_aeron.ExclusivePublication,
    ping_client: *ringloom_aeron.Client,
    echo_client: *ringloom_aeron.Client,
    payload: []u8,
) !void {
    const start = Clock.monotonicNanosStable();
    while (Clock.monotonicNanosStable() - start < spin_timeout_ns) {
        _ = try ping_client.invokeConductor();
        _ = try echo_client.invokeConductor();

        if (payload.len <= publication.maxPayloadLength()) {
            switch (publication.tryClaim(payload.len)) {
                .claim => |claim_value| {
                    var claim = claim_value;
                    @memcpy(claim.bytes(), payload);
                    try claim.commit();
                    return;
                },
                .not_connected, .back_pressured, .admin_action => {},
                .closed, .max_position_exceeded, .failed => return error.AeronCallFailed,
            }
        } else {
            switch (publication.offer(payload)) {
                .position => return,
                .not_connected, .back_pressured, .admin_action => {},
                .closed, .max_position_exceeded, .failed => return error.AeronCallFailed,
            }
        }

        if (latency_trace.isMeasured(payload)) {
            latency_trace.embedSend(payload, latency_trace.measured_phase, @intCast(Clock.monotonicNanosStable()));
        }
        std.atomic.spinLoopHint();
    }
    return error.Timeout;
}

fn waitForEchoCompletion(state: *EchoState) !void {
    const start = Clock.monotonicNanosStable();
    while (Clock.monotonicNanosStable() - start < completion_timeout_ns) {
        if (state.failed.load(.acquire)) return error.AeronCallFailed;
        if (state.total_received.load(.acquire) >= state.expected_total) return;
        std.Thread.yield() catch {};
    }
    return error.Timeout;
}

fn paceNextSend(next_deadline_ns: *i64, interval_ns: u64) void {
    if (interval_ns == 0) return;

    const now = Clock.monotonicNanosStable();
    if (next_deadline_ns.* == 0) {
        next_deadline_ns.* = now;
    }
    next_deadline_ns.* += @intCast(interval_ns);

    while (Clock.monotonicNanosStable() < next_deadline_ns.*) {
        std.atomic.spinLoopHint();
    }
}

fn writePlainAeronResults(
    path: []const u8,
    cfg: MessageSizeConfig,
    histogram: *Histogram,
    total_received: u64,
    total_measured: u64,
    sent: u64,
    send_failures: u64,
    send_elapsed_ns: u64,
) !void {
    const latency = histogram.summaryPercentiles();
    const throughput_msgs_per_sec = sent * std.time.ns_per_s / send_elapsed_ns;
    const throughput_bytes_per_sec = throughput_msgs_per_sec * cfg.size;

    const file = try std.Io.Dir.createFileAbsolute(std.testing.io, path, .{});
    defer file.close(std.testing.io);

    var write_buf: [4096]u8 = undefined;
    var writer = file.writer(std.testing.io, &write_buf);
    try writer.interface.print(
        \\{{
        \\  "suite": "plain-aeron",
        \\  "scenario": "remote-transit-{s}B",
        \\  "driver_threading_mode": "dedicated",
        \\  "topology": "two embedded Aeron media drivers over UDP loopback",
        \\  "message_size": {d},
        \\  "warmup_count": {d},
        \\  "send_interval_ns": {d},
        \\  "mtu_length": {d},
        \\  "ipc_mtu_length": {d},
        \\  "network_publication_max_messages_per_send": {d},
        \\  "sent": {d},
        \\  "send_failures": {d},
        \\  "throughput_msgs_per_sec": {d},
        \\  "throughput_bytes_per_sec": {d},
        \\  "total_received": {d},
        \\  "total_measured": {d},
        \\  "stage_breakdown_measured": 0,
        \\  "latency_p50_ns": {d},
        \\  "latency_p95_ns": {d},
        \\  "latency_p99_ns": {d},
        \\  "latency_p99_9_ns": {d},
        \\  "latency_max_ns": {d}
        \\}}
        \\
    , .{
        cfg.label,
        cfg.size,
        cfg.warmup_count,
        send_interval_ns,
        mtu_length,
        ipc_mtu_length,
        network_publication_max_messages_per_send,
        sent,
        send_failures,
        throughput_msgs_per_sec,
        throughput_bytes_per_sec,
        total_received,
        total_measured,
        latency.p50,
        latency.p95,
        latency.p99,
        latency.p99_9,
        latency.max_val,
    });
    try writer.interface.flush();
}

test "plain Aeron remote transit - 32B" {
    try runPlainAeronRemoteBench(std.testing.allocator, message_configs[0]);
}

test "plain Aeron remote transit - 128B" {
    try runPlainAeronRemoteBench(std.testing.allocator, message_configs[1]);
}

test "plain Aeron remote transit - 512B" {
    try runPlainAeronRemoteBench(std.testing.allocator, message_configs[2]);
}

test "plain Aeron remote transit - 1024B" {
    try runPlainAeronRemoteBench(std.testing.allocator, message_configs[3]);
}

test "plain Aeron remote transit - 4096B" {
    try runPlainAeronRemoteBench(std.testing.allocator, message_configs[4]);
}
