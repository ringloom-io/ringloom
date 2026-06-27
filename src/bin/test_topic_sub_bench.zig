// SPDX-License-Identifier: Apache-2.0
//! test_topic_sub_bench — subscriber benchmark that reads from a local
//! ringloom-queue replica and measures consume throughput + latency.
//!
//! Expects messages with an 8-byte nanosecond send timestamp prepended
//! (inserted by test_topic_aeron_bench in latency mode). When the payload
//! also carries the benchmark stage-trace marker (see latency_trace.zig),
//! end-to-end latency is split into two hops:
//!   pub_to_a : send → broker-A receiver ingress
//!   a_to_sub : broker-A ingress → subscriber receive
//!
//! Flags: --queue-dir --expected-count --timeout-sec

const std = @import("std");
const builtin = @import("builtin");
const rq = @import("ringloom_queue");
const ringloom_common = @import("ringloom_common");
const Clock = ringloom_common.platform.Clock;
const latency_trace = ringloom_common.message.latency_trace;

const RawQueue = rq.Queue([]const u8);

/// Rolling min/max/sum accumulator for a latency hop (ns).
const HopStats = struct {
    sum: u64 = 0,
    min: u64 = std.math.maxInt(u64),
    max: u64 = 0,
    count: u64 = 0,

    fn record(self: *HopStats, lat: u64) void {
        self.sum += lat;
        if (lat < self.min) self.min = lat;
        if (lat > self.max) self.max = lat;
        self.count += 1;
    }
    fn avg(self: HopStats) u64 {
        if (self.count == 0) return 0;
        return self.sum / self.count;
    }
    fn minOrZero(self: HopStats) u64 {
        return if (self.count == 0) 0 else self.min;
    }
};

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
    const dir = parseStr(args, "--queue-dir", "");
    if (dir.len == 0) {
        try std.Io.File.stderr().writeStreamingAll(io, "ERROR: --queue-dir required\n");
        std.process.exit(1);
    }
    const expected: u64 = parseU64(args, "--expected-count", 1000);
    const timeout_s: u64 = parseU64(args, "--timeout-sec", 30);

    // Open the local replica queue as a tailer.
    const scheme = rq.roll.findSchemeByName("FAST_DAILY") orelse return error.UnknownScheme;
    var queue = try RawQueue.open(.{
        .dir = dir,
        .roll_scheme = scheme,
        .create = false,
        .use_huge_pages = false,
        .enable_prefetcher = false,
        .enable_cleaner = false,
        .spawn_helper_threads = false,
        .retention_cycles = 4,
        .allocator = alloc,
    }, rq.codec.RawCodec);
    defer queue.deinit();

    var tailer = try queue.tailer(0);
    defer tailer.deinit();
    try std.Io.File.stdout().writeStreamingAll(io, "sub ready\n");

    // ── Read loop with latency measurement ────────────────────
    // end-to-end (send → receive) and the per-hop split when the stage-trace
    // marker is present in the payload.
    var received: u64 = 0;
    var e2e: HopStats = .{};
    var pub_to_a: HopStats = .{}; // send → broker-A receiver ingress
    var a_to_sub: HopStats = .{}; // broker-A ingress → subscriber receive

    // Spin-then-sleep idle: spin briefly to catch the next message with low
    // latency, then back off to a short sleep so a slow producer doesn't burn
    // a core. Reset to spin whenever a message arrives.
    var idle_spins: u32 = 0;
    const idle_spin_budget: u32 = 4096;

    const deadline = Clock.monotonicNanosStable() + @as(i64, @intCast(timeout_s)) * std.time.ns_per_s;

    while (received < expected and Clock.monotonicNanosStable() < deadline) {
        if (try tailer.poll()) |entry| {
            const payload = entry.message;
            idle_spins = 0; // got a message: reset the spin budget

            // Skip warmup-phase messages — they prime the pipeline but must not
            // pollute the latency measurement. Only measured-phase messages are
            // counted toward `expected` and recorded.
            if (!latency_trace.isMeasured(payload)) continue;

            // Payload format: [8 bytes send_timestamp_ns][phase][magic+stages][data]
            if (latency_trace.readSendTimestamp(payload)) |send_ns| {
                const recv_ns: u64 = @intCast(Clock.monotonicNanosStable());
                if (recv_ns > send_ns) {
                    e2e.record(recv_ns - send_ns);
                }
                // Per-hop split requires the broker-A ingress stamp (magic marker).
                // The topic leader stamps only receiver ingress (no dequeue stage),
                // so use the topic-specific reader.
                if (latency_trace.readTopicStageTrace(payload)) |trace| {
                    pub_to_a.record(trace.receiver_ingress_ns - send_ns);
                    if (recv_ns >= trace.receiver_ingress_ns) {
                        a_to_sub.record(recv_ns - trace.receiver_ingress_ns);
                    }
                }
            }
            received += 1;
        } else {
            // Spin briefly to catch the next message promptly (low latency),
            // then fall back to a short sleep to avoid pegging a core when
            // the producer is slow or silent.
            if (idle_spins < idle_spin_budget) {
                idle_spins += 1;
                std.atomic.spinLoopHint();
            } else {
                _ = std.Io.sleep(io, .fromMicroseconds(10), .awake) catch {};
            }
        }
    }

    // ── Output JSON ──────────────────────────────────────────
    var jb: [768]u8 = undefined;
    const js = std.fmt.bufPrint(
        &jb,
        "{{\"received\":{d},\"expected\":{d}," ++
            "\"lat_avg_ns\":{d},\"lat_min_ns\":{d},\"lat_max_ns\":{d}," ++
            "\"pub_to_a_avg_ns\":{d},\"pub_to_a_min_ns\":{d},\"pub_to_a_max_ns\":{d}," ++
            "\"a_to_sub_avg_ns\":{d},\"a_to_sub_min_ns\":{d},\"a_to_sub_max_ns\":{d}}}",
        .{
            received,
            expected,
            e2e.avg(),          e2e.minOrZero(),          e2e.max,
            pub_to_a.avg(),     pub_to_a.minOrZero(),     pub_to_a.max,
            a_to_sub.avg(),     a_to_sub.minOrZero(),     a_to_sub.max,
        },
    ) catch "{}";
    try std.Io.File.stdout().writeStreamingAll(io, "TOPIC_SUB_JSON<<EOF\n");
    try std.Io.File.stdout().writeStreamingAll(io, js);
    try std.Io.File.stdout().writeStreamingAll(io, "\nEOF\n");
}
