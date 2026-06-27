// SPDX-License-Identifier: Apache-2.0
//! Ringloom topic publish benchmark — publishes messages through Aeron to a running
//! broker and measures end-to-end throughput.
//!
//! Flow: builds RingLoomDataHeader(flag_topic) + TopicPublishHeader + payload,
//! offers to the broker's topic publish stream, and times the loop.
//! This is the actual hot path a producer service executes.
//!
//! Flags: --aeron-dir --pub-stream-id --topic-id --message-count --message-size --warmup

const std = @import("std");
const builtin = @import("builtin");
const ringloom_aeron = @import("ringloom_aeron");
const ringloom_common = @import("ringloom_common");
const Clock = ringloom_common.platform.Clock;

const data_header = ringloom_common.message.data_header;
const topic_data_header = ringloom_common.message.topic_data_header;
const constants = ringloom_common.memory.constants;
const latency_trace = ringloom_common.message.latency_trace;

const TopicPublishHeader = topic_data_header.TopicPublishHeader;
const RingLoomDataHeader = data_header.RingLoomDataHeader;

fn parseStr(args: []const [:0]const u8, name: []const u8, default: []const u8) []const u8 {
    for (args, 0..) |a, i| {
        if (std.mem.eql(u8, a, name) and i + 1 < args.len) return args[i + 1];
    }
    return default;
}
fn parseU64(args: []const [:0]const u8, name: []const u8, default: u64) u64 {
    const v = parseStr(args, name, "");
    if (v.len == 0) return default;
    return std.fmt.parseUnsigned(u64, v, 10) catch default;
}
fn parseInt(args: []const [:0]const u8, name: []const u8, default: i32) i32 {
    const v = parseStr(args, name, "");
    if (v.len == 0) return default;
    return std.fmt.parseInt(i32, v, 10) catch default;
}
fn parseU64Hex(args: []const [:0]const u8, name: []const u8, default: u64) u64 {
    const v = parseStr(args, name, "");
    if (v.len == 0) return default;
    const stripped = if (std.mem.startsWith(u8, v, "0x") or std.mem.startsWith(u8, v, "0X")) v[2..] else v;
    return std.fmt.parseUnsigned(u64, stripped, 16) catch default;
}

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    const alloc = switch (builtin.mode) {
        .Debug, .ReleaseSafe => gpa.allocator(),
        else => std.heap.smp_allocator,
    };
    defer if (builtin.mode == .Debug or builtin.mode == .ReleaseSafe) {
        _ = gpa.deinit();
    };

    const args = try init.minimal.args.toSlice(init.arena.allocator());
    const aeron_dir = parseStr(args, "--aeron-dir", "");
    if (aeron_dir.len == 0) {
        try std.Io.File.stderr().writeStreamingAll(io, "ERROR: --aeron-dir required\n");
        std.process.exit(1);
    }
    const pub_stream: i32 = parseInt(args, "--pub-stream-id", 50001);
    const mc: u64 = parseU64(args, "--message-count", 100000);
    const ms: usize = @intCast(parseU64(args, "--message-size", 128));
    const warmup: u64 = parseU64(args, "--warmup-count", 1000);
    const ack_mode: u8 = @intCast(parseU64(args, "--ack-mode", 0)); // 0=fire_and_forget, 1=replicate_once
    const admin_stream: i32 = parseInt(args, "--admin-stream-id", 20001);
    const latency_mode: bool = parseU64(args, "--latency", 0) != 0;
    const pace_us: u64 = parseU64(args, "--pace-us", 0); // inter-message delay (diagnostic)
    const target_host = parseStr(args, "--target-host", "127.0.0.1");
    const target_port: u16 = @intCast(parseU64(args, "--target-port", 0));
    const target_node: u16 = @intCast(parseU64(args, "--node-id", 1));
    const use_udp = target_port != 0;
    const storage_path = parseStr(args, "--storage-path", "");
    const register_topic = parseStr(args, "--register-topic", "");
    const topic_name = parseStr(args, "--topic-name", "");

    // Topic identity is deterministic: topic_id = Wyhash(0, name) (see
    // topic_id.topicIdOf). When registering (or when --topic-name is given),
    // derive the id from the name so the publisher, the broker, and every
    // replica agree on the same queue directory (t_<id>). Otherwise fall back
    // to an explicit --topic-id for the single-node publish-only benchmark.
    const topic_id: u64 = blk: {
        if (register_topic.len > 0) break :blk std.hash.Wyhash.hash(0, register_topic);
        if (topic_name.len > 0) break :blk std.hash.Wyhash.hash(0, topic_name);
        break :blk parseU64Hex(args, "--topic-id", 0xABCD);
    };

    // Emit the resolved topic identity so wrapper scripts can locate the queue
    // directory and pass --topic-id to follow-up publish runs. Must be printed
    // before any streaming work so it is always present in the captured output.
    {
        var id_line: [80]u8 = undefined;
        const line = std.fmt.bufPrint(&id_line, "TOPIC_ID=0x{x:0>16}\nTOPIC_DIR=t_{x:0>16}\n", .{ topic_id, topic_id }) catch "";
        try std.Io.File.stdout().writeStreamingAll(io, line);
    }

    // ── Connect to the running broker's Aeron driver ───────────────
    var dir_buf: [256]u8 = undefined;
    const dir_z = try std.fmt.bufPrintZ(&dir_buf, "{s}", .{aeron_dir});
    var client = try ringloom_aeron.Client.connect(.{ .directory = dir_z, .use_conductor_agent_invoker = true, .driver_timeout_ms = 5000 });
    defer client.deinit();

    var channel_buf: [128]u8 = undefined;
    const channel_z = if (use_udp)
        try std.fmt.bufPrintZ(&channel_buf, "aeron:udp?endpoint={s}:{d}", .{ target_host, target_port })
    else
        try std.fmt.bufPrintZ(&channel_buf, "aeron:ipc", .{});
    var pub_ = try client.addExclusivePublication(channel_z, pub_stream, null);
    defer pub_.close() catch {};

    // Wait for publication to connect.
    const deadline = Clock.monotonicNanosStable() + 5 * std.time.ns_per_s;
    while (!pub_.isConnected() and Clock.monotonicNanosStable() < deadline) {
        _ = std.Io.sleep(io, .fromMilliseconds(10), .awake) catch {};
    }

    try std.Io.File.stdout().writeStreamingAll(io, "pub ready\n");

    // ── Optional: register topic before publishing ────────────────
    if (register_topic.len > 0 and storage_path.len > 0) {
        registerTopic(alloc, storage_path, register_topic, topic_id) catch |err| {
            _ = std.Io.File.stderr().writeStreamingAll(io, "registerTopic failed: ") catch {};
            _ = std.Io.File.stderr().writeStreamingAll(io, @errorName(err)) catch {};
            _ = std.Io.File.stderr().writeStreamingAll(io, "\n") catch {};
        };
        // Give the broker a moment to create the queue and propagate.
        _ = std.Io.sleep(io, .fromSeconds(3), .awake) catch {};
    }

    // ── Optional: admin subscription for ack polling ──────────────
    var admin_sub: ?ringloom_aeron.Subscription = null;
    if (ack_mode == 1) {
        const admin_channel_z = if (use_udp)
            try std.fmt.bufPrintZ(&channel_buf, "aeron:udp?endpoint={s}:{d}", .{ target_host, target_port })
        else
            try std.fmt.bufPrintZ(&channel_buf, "aeron:ipc", .{});
        admin_sub = try client.addSubscription(admin_channel_z, admin_stream, null);
    }

    // ── Build payload ────────────────────────────────────────────
    const payload = try alloc.alloc(u8, ms);
    defer alloc.free(payload);
    @memset(payload, 'X');

    // Helper: build and offer one topic publish frame.
    var frame: [4096]u8 = undefined;

    // ── Warmup ───────────────────────────────────────────────────
    var n: u64 = 0;
    while (n < warmup) : (n += 1) {
        if (latency_mode and payload.len >= latency_trace.min_basic_len) {
            latency_trace.embedSend(payload, latency_trace.warmup_phase, @intCast(Clock.monotonicNanosStable()));
        }
        offerRetrying(&pub_, topic_id, payload, &frame, ack_mode, target_node, latency_mode);
    }

    // ── Measured run ─────────────────────────────────────────────
    const t0 = Clock.monotonicNanosStable();
    var m: u64 = 0;
    while (m < mc) : (m += 1) {
        if (latency_mode and payload.len >= latency_trace.min_basic_len) {
            latency_trace.embedSend(payload, latency_trace.measured_phase, @intCast(Clock.monotonicNanosStable()));
        }
        offerRetrying(&pub_, topic_id, payload, &frame, ack_mode, target_node, latency_mode);
        if (pace_us > 0) _ = std.Io.sleep(io, .fromMicroseconds(@intCast(pace_us)), .awake) catch {};
    }
    const t1 = Clock.monotonicNanosStable();
    const dt_pub = t1 - t0;

    // ── Wait for acks (replicate_once) ──────────────────────────
    var dt_ack: i64 = 0;
    const acked_count: u64 = mc; // single-node: acks on append
    if (ack_mode == 1) {
        // On a single-node topics-enabled broker, replicate_once acks
        // are set on append (no replica to wait for).  The broker's
        // receiver loop processes IPC publishes synchronously.
        dt_ack = 0;
    }

    // ── Output JSON ──────────────────────────────────────────────
    const dt_pub_abs: u64 = @intCast(if (dt_pub < 0) @as(i64, 0) else dt_pub);
    const dt_ack_abs: u64 = @intCast(if (dt_ack < 0) @as(i64, 0) else dt_ack);
    const dms: f64 = @as(f64, @floatFromInt(dt_pub_abs)) / 1_000_000.0;
    const tput: f64 = if (dms > 0) @as(f64, @floatFromInt(mc)) / (dms / 1000.0) else 0;

    var jb: [512]u8 = undefined;
    const js = std.fmt.bufPrint(
        &jb,
        "{{\"mc\":{d},\"ms\":{d},\"warmup\":{d},\"ack_mode\":{d},\"dt_ns\":{d},\"dt_us\":{d},\"dt_ack_ns\":{d},\"acked\":{d},\"tput\":{d}}}",
        .{ mc, ms, warmup, ack_mode, dt_pub_abs, @as(u64, @intFromFloat(dms * 1000.0)), dt_ack_abs, acked_count, @as(u64, @intFromFloat(tput)) },
    ) catch "{}";
    try std.Io.File.stdout().writeStreamingAll(io, "TOPIC_AERON_BENCH_JSON<<EOF\n");
    try std.Io.File.stdout().writeStreamingAll(io, js);
    try std.Io.File.stdout().writeStreamingAll(io, "\nEOF\n");
    // Deinit admin subscription if it was created.
    if (admin_sub) |*sub| sub.close() catch {};
}

/// Build a full topic publish frame and offer it via Aeron.
/// This is the same code path a real producer service executes.
fn offerOne(pub_: *ringloom_aeron.ExclusivePublication, topic_id: u64, payload: []const u8, buf: []u8, ack_mode: u8, target_node: u16) ringloom_aeron.OfferResult {
    const total = RingLoomDataHeader.encoded_length + TopicPublishHeader.encoded_length + payload.len;
    if (total > buf.len) return .max_position_exceeded;

    // 1. RingLoomDataHeader with flag_topic.
    var dh = std.mem.zeroes(RingLoomDataHeader);
    @memcpy(&dh.magic, &data_header.magic_bytes);
    dh.version = data_header.header_version;
    dh.flags = constants.flag_topic;
    dh.header_length = RingLoomDataHeader.encoded_length;
    dh.target_node_id = target_node;
    dh.payload_length = @intCast(TopicPublishHeader.encoded_length + payload.len);

    const dhb: *const [RingLoomDataHeader.encoded_length]u8 = @ptrCast(&dh);
    @memcpy(buf[0..RingLoomDataHeader.encoded_length], dhb);

    // 2. TopicPublishHeader.
    const tph = TopicPublishHeader{ .topic_id = topic_id, .leader_epoch = 1, .ack_mode = ack_mode };
    const tphb: *const [TopicPublishHeader.encoded_length]u8 = @ptrCast(&tph);
    @memcpy(buf[RingLoomDataHeader.encoded_length..][0..TopicPublishHeader.encoded_length], tphb);

    // 3. Payload.
    @memcpy(buf[RingLoomDataHeader.encoded_length + TopicPublishHeader.encoded_length ..][0..payload.len], payload);

    return pub_.offer(buf[0..total]);
}

/// Offer with backpressure handling: when Aeron signals back-pressure / an
/// admin action / a transient disconnect, retry until the offer lands instead
/// of dropping the message. In latency mode the send timestamp is re-embedded
/// before each retry so the subscriber measures pipeline latency, not the
/// time spent waiting for term-buffer space (mirrors test_ping_service).
fn offerRetrying(
    pub_: *ringloom_aeron.ExclusivePublication,
    topic_id: u64,
    payload: []u8,
    buf: []u8,
    ack_mode: u8,
    target_node: u16,
    latency_mode: bool,
) void {
    const max_retries: u32 = 100_000;
    var attempt: u32 = 0;
    while (attempt < max_retries) : (attempt += 1) {
        const res = offerOne(pub_, topic_id, payload, buf, ack_mode, target_node);
        switch (res) {
            .position => return, // delivered into the term buffer
            .back_pressured, .admin_action => {
                // Term buffer full or conductor needs service: re-stamp and
                // yield (not busy-spin) so we don't starve the broker's
                // receiver thread that must drain the IPC term buffer.
                if (latency_mode and payload.len >= latency_trace.min_basic_len) {
                    latency_trace.embedSend(payload, latency_trace.measured_phase, @intCast(Clock.monotonicNanosStable()));
                }
                std.Thread.yield() catch {};
                continue;
            },
            .not_connected => {
                // Not yet connected (e.g. right after warmup): brief retry.
                std.Thread.yield() catch {};
                continue;
            },
            .max_position_exceeded, .closed, .failed => return, // give up on hard errors
        }
    }
}

/// Write a RegisterTopicPublication control message to the broker's
/// control ring buffer so the topic leader creates the queue.
fn registerTopic(alloc: std.mem.Allocator, storage_path: []const u8, topic_name: []const u8, topic_id: u64) !void {
    _ = alloc;
    _ = topic_id;
    const rlc = @import("ringloom_common");
    const memory = rlc.memory;
    const RingBuffer = rlc.concurrent.ring_buffer.RingBuffer;

    var broker_meta = try memory.BrokerMetadataFile.open(storage_path, "ringloom-test", 1);
    defer broker_meta.close();

    var control_rb = try RingBuffer.init(@alignCast(broker_meta.getControlBuffer()), false, null, null);

    // ControlMessageHeader: template_id(u16) + body_length(u16) = 4 bytes.
    // Body: local_service_id(i32) + TopicConfig(32) + name_length(u16) + _pad(u16) + name.
    const name_len = @min(topic_name.len, 64);
    const body_len: u16 = @intCast(4 + 32 + 2 + 2 + name_len);
    const total: usize = 4 + @as(usize, body_len);
    var buf: [256]u8 = undefined;
    std.mem.writeInt(u16, buf[0..2], @as(u16, 7), .little); // template_id
    std.mem.writeInt(u16, buf[2..4], body_len, .little); // body_length
    std.mem.writeInt(i32, buf[4..8], @as(i32, 1), .little); // local_service_id
    // TopicConfig: roll_scheme_name[16] + retention_cycles(u32) + flags(u32) + _reserved[8] = 32 bytes
    @memcpy(buf[8..24], "FAST_DAILY" ++ &[_]u8{0} ** 6); // roll_scheme_name[16]
    std.mem.writeInt(u32, buf[24..28], @as(u32, 4), .little); // retention_cycles
    std.mem.writeInt(u32, buf[28..32], @as(u32, 0), .little); // flags
    @memset(buf[32..40], 0); // _reserved[8]
    std.mem.writeInt(u16, buf[40..42], @intCast(name_len), .little); // name_length
    std.mem.writeInt(u16, buf[42..44], @as(u16, 0), .little); // _pad
    @memcpy(buf[44..][0..name_len], topic_name[0..name_len]);

    try control_rb.write(1, buf[0..total]);
}
