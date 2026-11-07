// SPDX-License-Identifier: Apache-2.0
//! test_topic_publisher_service — registers a topic publication and publishes
//! through the same path a real producer uses (engine registration, Aeron IPC
//! to the topic leader, per-publisher replicate_once acks).
//!
//! Flags (in addition to the harness flags --storage-path, --group,
//! --service-name, --broker-node-id):
//!   --topic NAME            topic name (default "bench")
//!   --count N               messages to publish (default 10)
//!   --size BYTES            payload size (default 32)
//!   --ack-mode 0|1          0 = fire_and_forget, 1 = replicate_once (default 0)
//!   --warmup N              untimed publishes first, waited on until acked
//!                           (default 0; absorbs first-append cycle-file setup)
//!   --ack-timeout-ms MS     how long to wait for the last ack (default 10000)
//!   --result-file PATH      write a JSON summary here
//!
//! The timed phase ends when the broker has appended every message: in
//! replicate_once mode when the last publish is acked, in fire_and_forget mode
//! when one extra replicate_once "sentinel" publish is acked (the leader
//! appends in arrival order, so its ack implies everything before it landed).
//! So `throughput_msgs_per_sec` is what the broker absorbed, while
//! `offer_msgs_per_sec` is only how fast frames entered the Aeron publication.
//!
//! Exits non-zero if registration fails, a publish cannot be delivered, or the
//! final ack does not arrive in time.

const std = @import("std");
const builtin = @import("builtin");
const ringloom_service = @import("ringloom_service");
const ringloom_common = @import("ringloom_common");

const Clock = ringloom_common.platform.Clock;
const AckMode = ringloom_common.topics.AckMode;
const TopicConfig = ringloom_common.topics.TopicConfig;

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

fn fail(io: std.Io, comptime fmt: []const u8, args: anytype) noreturn {
    var buf: [256]u8 = undefined;
    const msg = std.fmt.bufPrint(&buf, "topic publisher: " ++ fmt ++ "\n", args) catch "topic publisher: error\n";
    std.Io.File.stderr().writeStreamingAll(io, msg) catch {};
    std.process.exit(1);
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
    const storage_path = parseStr(args, "--storage-path", "/dev/shm");
    const group = parseStr(args, "--group", "default");
    const service_name = parseStr(args, "--service-name", "pub");
    const broker_node_id: i16 = @intCast(parseU64(args, "--broker-node-id", 1));
    const topic = parseStr(args, "--topic", "bench");
    const count = parseU64(args, "--count", 10);
    const size: usize = @intCast(parseU64(args, "--size", 32));
    const ack_mode: AckMode = if (parseU64(args, "--ack-mode", 0) == 1) .replicate_once else .fire_and_forget;
    const ack_timeout_ms = parseU64(args, "--ack-timeout-ms", 10_000);
    const result_file = parseStr(args, "--result-file", "");

    // Manual control agent: registration reads the broker's response from the
    // control ring buffer on this thread, and acks arrive via poll() below.
    var engine = try ringloom_service.RingLoomEngine.startWithOptions(alloc, .{
        .service_name = service_name,
        .storage_path = storage_path,
        .group = group,
        .broker_node_id = broker_node_id,
        .heartbeat_timeout_ms = 5000,
    }, .{ .control_agent_mode = .manual });
    defer engine.deinit(); // stops the engine first

    var publisher = engine.registerTopicPublication(TopicConfig.fromName("FAST_DAILY", 0, false), topic) catch |err|
        fail(io, "registration of '{s}' failed: {s}", .{ topic, @errorName(err) });
    try engine.topic_publishers.register(&publisher);
    defer engine.topic_publishers.unregister(&publisher);

    try std.Io.File.stdout().writeStreamingAll(io, "service ready\n");

    const payload = try alloc.alloc(u8, size);
    defer alloc.free(payload);
    @memset(payload, 'X');

    const ctx = Ctx{ .io = io, .engine = engine, .publisher = &publisher, .payload = payload, .ack_timeout_ms = ack_timeout_ms };

    const warmup = parseU64(args, "--warmup", 0);
    if (warmup > 0) ctx.waitAcked(ctx.publishBatch(warmup, .replicate_once));

    const start_ns = Clock.monotonicNanos();
    var last_token = ctx.publishBatch(count, ack_mode);
    const published_ns = Clock.monotonicNanos();
    if (ack_mode == .fire_and_forget) last_token = ctx.publishBatch(1, .replicate_once); // sentinel
    ctx.waitAcked(last_token);
    const done_ns = Clock.monotonicNanos();

    if (result_file.len > 0) {
        const publish_ns: u64 = @intCast(published_ns - start_ns);
        const total_ns: u64 = @intCast(done_ns - start_ns);
        const ack_wait_ns: u64 = @intCast(done_ns - published_ns);
        const offer_rate: u64 = if (publish_ns > 0) count * std.time.ns_per_s / publish_ns else 0;
        const tput: u64 = if (total_ns > 0) count * std.time.ns_per_s / total_ns else 0;
        var buf: [640]u8 = undefined;
        const json = try std.fmt.bufPrint(&buf,
            \\{{"suite":"topic","scenario":"topic-publish-{s}-{d}B","topic":"{s}","ack_mode":{d},"message_size":{d},"warmup":{d},"published":{d},"acked_through":{d},"publish_ns":{d},"ack_wait_ns":{d},"total_ns":{d},"offer_msgs_per_sec":{d},"throughput_msgs_per_sec":{d}}}
            \\
        , .{ @tagName(ack_mode), size, topic, @intFromEnum(ack_mode), size, warmup, count, publisher.ackedSeq(), publish_ns, ack_wait_ns, total_ns, offer_rate, tput });
        try std.Io.Dir.cwd().writeFile(io, .{ .sub_path = result_file, .data = json });
    }
}

const Ctx = struct {
    io: std.Io,
    engine: *ringloom_service.RingLoomEngine,
    publisher: *ringloom_service.topics.TopicPublisher,
    payload: []const u8,
    ack_timeout_ms: u64,

    /// Publishes `n` messages, retrying transient back-pressure (giving up on
    /// one that cannot be delivered within a second). Returns the last token.
    fn publishBatch(self: Ctx, n: u64, mode: AckMode) u64 {
        var i: u64 = 0;
        while (i < n) : (i += 1) {
            const deadline = Clock.monotonicNanos() + std.time.ns_per_s;
            while (true) {
                switch (self.publisher.publish(self.payload, @intCast(i), mode)) {
                    .ok => break,
                    .back_pressured, .not_connected => {
                        if (Clock.monotonicNanos() > deadline) fail(self.io, "publish {d} not delivered within 1s", .{i});
                        _ = self.engine.aeron_runtime.doWork() catch {};
                        std.atomic.spinLoopHint();
                    },
                    else => |r| fail(self.io, "publish {d} failed: {s}", .{ i, @tagName(r) }),
                }
            }
        }
        return self.publisher.lastAssignedToken();
    }

    /// Polls the control plane until `token` is acked. Tokens are acked in
    /// order, so this covers every earlier publish too.
    fn waitAcked(self: Ctx, token: u64) void {
        if (token == 0) return;
        const deadline = Clock.monotonicNanos() + @as(i64, @intCast(self.ack_timeout_ms)) * std.time.ns_per_ms;
        while (!self.publisher.isAcked(token)) {
            if (Clock.monotonicNanos() > deadline) {
                fail(self.io, "token {d} not acked within {d} ms; acked through {d}", .{ token, self.ack_timeout_ms, self.publisher.ackedSeq() });
            }
            _ = self.engine.control_agent.poll(64);
            std.atomic.spinLoopHint();
        }
    }
};
