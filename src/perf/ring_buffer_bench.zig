// SPDX-License-Identifier: Apache-2.0

//! Raw ring-buffer microbenchmarks.
//!
//! These tests isolate the shared-memory ring buffer from broker/service
//! orchestration so performance work can start from a direct baseline:
//!
//! 1. Single-threaded uncontended write lower bound
//! 2. Single-threaded read lower bound from a prefilled ring
//! 3. SPSC saturated producer write cost across two threads
//! 4. SPSC one-message-at-a-time handoff latency across two threads

const builtin = @import("builtin");
const std = @import("std");
const ringloom_common = @import("ringloom_common");
const RingBuffer = ringloom_common.RingBuffer;
const Clock = ringloom_common.Clock;
const platform = ringloom_common.platform;
const ring_buffer_mod = ringloom_common.concurrent.ring_buffer;
const persistent_results = @import("persistent_results.zig");

const suite_name = "ring-buffer";
const benchmark_msg_type_id: i32 = 1;
const payload_size_bytes: usize = 64;
const mapped_storage_dir = "/dev/shm/ringloom-perf-ring-buffer";
const lower_bound_capacity_bytes: usize = 16 * 1024 * 1024;
const spsc_capacity_bytes: usize = 8 * 1024 * 1024;
const lower_bound_batch_size: u32 = 256;
const lower_bound_warmup_batches: usize = 128;
const lower_bound_measure_batches: usize = 512;
const spsc_batch_size: u32 = 256;
const spsc_warmup_batches: usize = 128;
const spsc_measure_batches: usize = 256;
const handoff_warmup_messages: usize = 10_000;
const handoff_measured_messages: usize = 100_000;

const SampleSummary = struct {
    min_ns: u64,
    p50_ns: u64,
    p95_ns: u64,
    p99_ns: u64,
    avg_ns: u64,
    max_ns: u64,
};

const ScenarioResult = struct {
    scenario: []const u8,
    build_mode: []const u8,
    message_size_bytes: u32,
    ring_capacity_bytes: usize,
    batch_size: u32,
    warmup_messages: u32,
    measured_messages: u32,
    throughput_msgs_per_sec: u64,
    throughput_payload_bytes_per_sec: u64,
    latency_min_ns: u64,
    latency_p50_ns: u64,
    latency_p95_ns: u64,
    latency_p99_ns: u64,
    latency_avg_ns: u64,
    latency_max_ns: u64,
    messages_sent: u64,
    messages_received: u64,
    notes: []const u8,
};

const DrainContext = struct {
    messages: usize = 0,
    checksum: u64 = 0,

    fn onMessage(context: *anyopaque, msg_type_id: i32, payload: []const u8) void {
        const self: *@This() = @ptrCast(@alignCast(context));
        self.messages += 1;
        self.checksum +%= @as(u64, @intCast(msg_type_id));
        self.checksum +%= payload[0];
        self.checksum +%= payload[payload.len - 1];
    }
};

const ReadContext = struct {
    messages: usize = 0,
    checksum: u64 = 0,

    fn onMessage(context: *anyopaque, msg_type_id: i32, payload: []const u8) void {
        const self: *@This() = @ptrCast(@alignCast(context));
        self.messages += 1;
        self.checksum +%= @as(u64, @intCast(msg_type_id));
        self.checksum +%= payload[0];
        self.checksum +%= payload[payload.len - 1];
    }
};

const SpscSaturatedConsumer = struct {
    ring_buffer: *RingBuffer,
    target_messages: usize,
    messages: usize = 0,
    checksum: u64 = 0,

    fn run(self: *@This()) void {
        platform.thread.setThreadName("rb-spsc-cons");
        platform.thread.setThreadAffinity(1) catch {};

        while (self.messages < self.target_messages) {
            const read = self.ring_buffer.readWithContext(self, onMessage, spsc_batch_size);
            if (read == 0) {
                std.atomic.spinLoopHint();
            }
        }
    }

    fn onMessage(context: *anyopaque, msg_type_id: i32, payload: []const u8) void {
        const self: *@This() = @ptrCast(@alignCast(context));
        self.messages += 1;
        self.checksum +%= @as(u64, @intCast(msg_type_id));
        self.checksum +%= payload[0];
        self.checksum +%= payload[payload.len - 1];
    }
};

const SpscSaturatedProducer = struct {
    ring_buffer: *RingBuffer,
    write_samples_ns: []u64,
    total_elapsed_ns: u64 = 0,
    err: ?anyerror = null,

    fn run(self: *@This()) void {
        platform.thread.setThreadName("rb-spsc-prod");
        platform.thread.setThreadAffinity(0) catch {};
        self.runInner() catch |err| {
            self.err = err;
        };
    }

    fn runInner(self: *@This()) !void {
        var payload = [_]u8{0x6D} ** payload_size_bytes;
        payload[payload.len - 1] = 0x92;

        for (0..spsc_warmup_batches) |_| {
            for (0..spsc_batch_size) |_| {
                try writeSpinning(self.ring_buffer, &payload);
            }
        }

        const total_start_ns = Clock.monotonicNanos();
        for (0..spsc_measure_batches) |batch_index| {
            const batch_start_ns = Clock.monotonicNanos();
            for (0..spsc_batch_size) |_| {
                try writeSpinning(self.ring_buffer, &payload);
            }
            const batch_elapsed_ns: u64 = @intCast(Clock.monotonicNanos() - batch_start_ns);
            self.write_samples_ns[batch_index] = batch_elapsed_ns / spsc_batch_size;
        }
        self.total_elapsed_ns = @intCast(Clock.monotonicNanos() - total_start_ns);
    }
};

const SpscHandoffConsumer = struct {
    ring_buffer: *RingBuffer,
    measured_messages: usize,
    handoff_samples_ns: []u64,
    acknowledged_messages: std.atomic.Value(usize) = std.atomic.Value(usize).init(0),
    seen_messages: usize = 0,
    recorded_messages: usize = 0,
    checksum: u64 = 0,

    fn run(self: *@This()) void {
        platform.thread.setThreadName("rb-handoff");
        platform.thread.setThreadAffinity(1) catch {};

        while (self.recorded_messages < self.measured_messages) {
            const read = self.ring_buffer.readWithContext(self, onMessage, spsc_batch_size);
            if (read == 0) {
                std.atomic.spinLoopHint();
            }
        }
    }

    fn onMessage(context: *anyopaque, msg_type_id: i32, payload: []const u8) void {
        const self: *@This() = @ptrCast(@alignCast(context));
        const sent_ns = std.mem.readInt(u64, payload[0..8], .little);
        self.checksum +%= @as(u64, @intCast(msg_type_id));
        self.checksum +%= payload[payload.len - 1];

        const seen = self.seen_messages;
        self.seen_messages += 1;
        if (seen >= handoff_warmup_messages) {
            const sample_index = seen - handoff_warmup_messages;
            if (sample_index < self.measured_messages) {
                const now_ns: u64 = @intCast(Clock.monotonicNanosStable());
                self.handoff_samples_ns[sample_index] = now_ns - sent_ns;
                self.recorded_messages = sample_index + 1;
            }
        }

        self.acknowledged_messages.store(seen + 1, .release);
    }
};

const SpscHandoffProducer = struct {
    ring_buffer: *RingBuffer,
    consumer: *SpscHandoffConsumer,
    total_messages: usize,
    total_elapsed_ns: u64 = 0,
    err: ?anyerror = null,

    fn run(self: *@This()) void {
        platform.thread.setThreadName("rb-hand-prod");
        platform.thread.setThreadAffinity(0) catch {};
        self.runInner() catch |err| {
            self.err = err;
        };
    }

    fn runInner(self: *@This()) !void {
        var payload = [_]u8{0xA5} ** payload_size_bytes;
        payload[payload.len - 1] = 0x5A;

        const total_start_ns = Clock.monotonicNanos();
        for (0..self.total_messages) |message_index| {
            std.mem.writeInt(u64, payload[0..8], @intCast(Clock.monotonicNanosStable()), .little);
            try writeSpinning(self.ring_buffer, &payload);
            while (self.consumer.acknowledged_messages.load(.acquire) <= message_index) {
                std.atomic.spinLoopHint();
            }
        }
        self.total_elapsed_ns = @intCast(Clock.monotonicNanos() - total_start_ns);
    }
};

const RingBufferStorage = struct {
    mapped_file: platform.MappedFile,
    bytes: []align(ring_buffer_mod.record_alignment) u8,

    fn create(
        allocator: std.mem.Allocator,
        data_capacity: usize,
        label: []const u8,
    ) !RingBufferStorage {
        const total_size = RingBuffer.calculateRequiredSize(data_capacity, false);
        const file_name = try std.fmt.allocPrint(
            allocator,
            "ring-buffer-{d}-{s}.bin",
            .{ platform.getPid(), label },
        );
        defer allocator.free(file_name);

        var mapped_file = try platform.MappedFile.create(
            allocator,
            mapped_storage_dir,
            file_name,
            total_size,
        );
        errdefer mapped_file.close();

        std.Io.Dir.deleteFileAbsolute(std.testing.io, mapped_file.path) catch {};

        const bytes: []align(ring_buffer_mod.record_alignment) u8 = @alignCast(mapped_file.data[0..total_size]);
        @memset(bytes, 0);
        return .{
            .mapped_file = mapped_file,
            .bytes = bytes,
        };
    }

    fn deinit(self: *RingBufferStorage) void {
        self.mapped_file.close();
        self.bytes = undefined;
    }
};

fn writeScenarioResult(
    allocator: std.mem.Allocator,
    file_name: []const u8,
    result: ScenarioResult,
) !void {
    const file_path = try persistent_results.scenarioPath(allocator, suite_name, file_name);
    defer allocator.free(file_path);

    const file = if (std.fs.path.isAbsolute(file_path))
        try std.Io.Dir.createFileAbsolute(std.testing.io, file_path, .{})
    else
        try std.Io.Dir.cwd().createFile(std.testing.io, file_path, .{});
    defer file.close(std.testing.io);

    const json = try std.fmt.allocPrint(allocator,
        \\{{
        \\  "suite": "{s}",
        \\  "scenario": "{s}",
        \\  "build_mode": "{s}",
        \\  "message_size_bytes": {d},
        \\  "ring_capacity_bytes": {d},
        \\  "batch_size": {d},
        \\  "warmup_messages": {d},
        \\  "measured_messages": {d},
        \\  "throughput_msgs_per_sec": {d},
        \\  "throughput_payload_bytes_per_sec": {d},
        \\  "latency_min_ns": {d},
        \\  "latency_p50_ns": {d},
        \\  "latency_p95_ns": {d},
        \\  "latency_p99_ns": {d},
        \\  "latency_avg_ns": {d},
        \\  "latency_max_ns": {d},
        \\  "messages_sent": {d},
        \\  "messages_received": {d},
        \\  "notes": "{s}"
        \\}}
        \\
    , .{
        suite_name,
        result.scenario,
        result.build_mode,
        result.message_size_bytes,
        result.ring_capacity_bytes,
        result.batch_size,
        result.warmup_messages,
        result.measured_messages,
        result.throughput_msgs_per_sec,
        result.throughput_payload_bytes_per_sec,
        result.latency_min_ns,
        result.latency_p50_ns,
        result.latency_p95_ns,
        result.latency_p99_ns,
        result.latency_avg_ns,
        result.latency_max_ns,
        result.messages_sent,
        result.messages_received,
        result.notes,
    });
    defer allocator.free(json);

    try file.writeStreamingAll(std.testing.io, json);
}

fn resetRingBuffer(storage: []align(ring_buffer_mod.record_alignment) u8) !RingBuffer {
    @memset(storage, 0);
    return try RingBuffer.init(storage, false, null, null);
}

fn throughputPerSecond(total_units: usize, elapsed_ns: u64) u64 {
    return @intCast((@as(u128, total_units) * std.time.ns_per_s) / @max(elapsed_ns, 1));
}

fn summarizeSamples(samples: []u64) SampleSummary {
    std.debug.assert(samples.len > 0);

    var sum: u128 = 0;
    for (samples) |sample| {
        sum += sample;
    }

    std.mem.sort(u64, samples, {}, std.sort.asc(u64));

    return .{
        .min_ns = samples[0],
        .p50_ns = percentile(samples, 50),
        .p95_ns = percentile(samples, 95),
        .p99_ns = percentile(samples, 99),
        .avg_ns = @intCast(sum / samples.len),
        .max_ns = samples[samples.len - 1],
    };
}

fn percentile(sorted: []const u64, pct: usize) u64 {
    std.debug.assert(sorted.len > 0);
    const rank = (sorted.len * pct + 99) / 100;
    const index = if (rank == 0) 0 else @min(rank - 1, sorted.len - 1);
    return sorted[index];
}

fn initPayload(fill_byte: u8) [payload_size_bytes]u8 {
    var payload = [_]u8{fill_byte} ** payload_size_bytes;
    payload[0] = fill_byte;
    payload[payload.len - 1] = fill_byte ^ 0xFF;
    return payload;
}

fn fillRingBuffer(
    ring_buffer: *RingBuffer,
    payload: []const u8,
    batches: usize,
    batch_size: u32,
) !void {
    for (0..batches) |_| {
        for (0..batch_size) |_| {
            try ring_buffer.write(benchmark_msg_type_id, payload);
        }
    }
}

fn drainRingBuffer(ring_buffer: *RingBuffer, limit: u32) DrainContext {
    var context = DrainContext{};
    while (true) {
        const read = ring_buffer.readWithContext(&context, DrainContext.onMessage, limit);
        if (read == 0) break;
    }
    return context;
}

fn runSingleThreadedWriteBench(allocator: std.mem.Allocator) !void {
    const warmup_messages = lower_bound_warmup_batches * lower_bound_batch_size;
    const measured_messages = lower_bound_measure_batches * lower_bound_batch_size;
    const payload = initPayload(0x3C);

    var storage = try RingBufferStorage.create(allocator, lower_bound_capacity_bytes, "single-write");
    defer storage.deinit();

    var ring_buffer = try resetRingBuffer(storage.bytes);
    try fillRingBuffer(&ring_buffer, &payload, lower_bound_warmup_batches, lower_bound_batch_size);

    ring_buffer = try resetRingBuffer(storage.bytes);

    var batch_samples = try allocator.alloc(u64, lower_bound_measure_batches);
    defer allocator.free(batch_samples);

    const total_start_ns = Clock.monotonicNanos();
    for (0..lower_bound_measure_batches) |batch_index| {
        const batch_start_ns = Clock.monotonicNanos();
        for (0..lower_bound_batch_size) |_| {
            try ring_buffer.write(benchmark_msg_type_id, &payload);
        }
        const batch_elapsed_ns: u64 = @intCast(Clock.monotonicNanos() - batch_start_ns);
        batch_samples[batch_index] = batch_elapsed_ns / lower_bound_batch_size;
    }
    const total_elapsed_ns: u64 = @intCast(Clock.monotonicNanos() - total_start_ns);

    const drain = drainRingBuffer(&ring_buffer, 1024);
    try std.testing.expectEqual(@as(usize, measured_messages), drain.messages);
    try std.testing.expect(drain.checksum != 0);

    const summary = summarizeSamples(batch_samples);
    try writeScenarioResult(allocator, "single-threaded-write-64B.json", .{
        .scenario = "single-threaded-write-64B",
        .build_mode = @tagName(builtin.mode),
        .message_size_bytes = payload_size_bytes,
        .ring_capacity_bytes = lower_bound_capacity_bytes,
        .batch_size = lower_bound_batch_size,
        .warmup_messages = warmup_messages,
        .measured_messages = measured_messages,
        .throughput_msgs_per_sec = throughputPerSecond(measured_messages, total_elapsed_ns),
        .throughput_payload_bytes_per_sec = throughputPerSecond(measured_messages * payload_size_bytes, total_elapsed_ns),
        .latency_min_ns = summary.min_ns,
        .latency_p50_ns = summary.p50_ns,
        .latency_p95_ns = summary.p95_ns,
        .latency_p99_ns = summary.p99_ns,
        .latency_avg_ns = summary.avg_ns,
        .latency_max_ns = summary.max_ns,
        .messages_sent = measured_messages,
        .messages_received = drain.messages,
        .notes = "Uncontended lower-bound write cost on a single thread with no concurrent drain, using a /dev/shm shared mmap backing file.",
    });
}

fn runSingleThreadedReadBench(allocator: std.mem.Allocator) !void {
    const warmup_messages = lower_bound_warmup_batches * lower_bound_batch_size;
    const measured_messages = lower_bound_measure_batches * lower_bound_batch_size;
    const payload = initPayload(0x55);

    var storage = try RingBufferStorage.create(allocator, lower_bound_capacity_bytes, "single-read");
    defer storage.deinit();

    var ring_buffer = try resetRingBuffer(storage.bytes);
    try fillRingBuffer(&ring_buffer, &payload, lower_bound_warmup_batches, lower_bound_batch_size);
    _ = drainRingBuffer(&ring_buffer, lower_bound_batch_size);

    ring_buffer = try resetRingBuffer(storage.bytes);
    try fillRingBuffer(&ring_buffer, &payload, lower_bound_measure_batches, lower_bound_batch_size);

    var batch_samples = try allocator.alloc(u64, lower_bound_measure_batches);
    defer allocator.free(batch_samples);

    var read_context = ReadContext{};
    const total_start_ns = Clock.monotonicNanos();
    for (0..lower_bound_measure_batches) |batch_index| {
        const batch_start_ns = Clock.monotonicNanos();
        const read = ring_buffer.readWithContext(&read_context, ReadContext.onMessage, lower_bound_batch_size);
        try std.testing.expectEqual(lower_bound_batch_size, read);
        const batch_elapsed_ns: u64 = @intCast(Clock.monotonicNanos() - batch_start_ns);
        batch_samples[batch_index] = batch_elapsed_ns / read;
    }
    const total_elapsed_ns: u64 = @intCast(Clock.monotonicNanos() - total_start_ns);

    try std.testing.expectEqual(@as(usize, measured_messages), read_context.messages);
    try std.testing.expect(read_context.checksum != 0);

    const summary = summarizeSamples(batch_samples);
    try writeScenarioResult(allocator, "single-threaded-read-64B.json", .{
        .scenario = "single-threaded-read-64B",
        .build_mode = @tagName(builtin.mode),
        .message_size_bytes = payload_size_bytes,
        .ring_capacity_bytes = lower_bound_capacity_bytes,
        .batch_size = lower_bound_batch_size,
        .warmup_messages = warmup_messages,
        .measured_messages = measured_messages,
        .throughput_msgs_per_sec = throughputPerSecond(measured_messages, total_elapsed_ns),
        .throughput_payload_bytes_per_sec = throughputPerSecond(measured_messages * payload_size_bytes, total_elapsed_ns),
        .latency_min_ns = summary.min_ns,
        .latency_p50_ns = summary.p50_ns,
        .latency_p95_ns = summary.p95_ns,
        .latency_p99_ns = summary.p99_ns,
        .latency_avg_ns = summary.avg_ns,
        .latency_max_ns = summary.max_ns,
        .messages_sent = measured_messages,
        .messages_received = read_context.messages,
        .notes = "Single-threaded read lower bound from a prefilled /dev/shm shared mmap ring buffer using 256-message batches.",
    });
}

fn writeSpinning(ring_buffer: *RingBuffer, payload: []const u8) !void {
    while (true) {
        ring_buffer.write(benchmark_msg_type_id, payload) catch |err| switch (err) {
            error.BufferFull => {
                std.atomic.spinLoopHint();
                continue;
            },
            else => return err,
        };
        break;
    }
}

fn runSpscSaturatedWriteBench(allocator: std.mem.Allocator) !void {
    const warmup_messages = spsc_warmup_batches * spsc_batch_size;
    const measured_messages = spsc_measure_batches * spsc_batch_size;
    const total_messages = warmup_messages + measured_messages;

    var storage = try RingBufferStorage.create(allocator, spsc_capacity_bytes, "spsc-saturated");
    defer storage.deinit();

    var ring_buffer = try resetRingBuffer(storage.bytes);

    const write_samples = try allocator.alloc(u64, spsc_measure_batches);
    defer allocator.free(write_samples);

    var consumer = SpscSaturatedConsumer{
        .ring_buffer = &ring_buffer,
        .target_messages = total_messages,
    };
    var producer = SpscSaturatedProducer{
        .ring_buffer = &ring_buffer,
        .write_samples_ns = write_samples,
    };

    const consumer_thread = try std.Thread.spawn(.{}, SpscSaturatedConsumer.run, .{&consumer});
    const producer_thread = try std.Thread.spawn(.{}, SpscSaturatedProducer.run, .{&producer});

    producer_thread.join();
    consumer_thread.join();
    if (producer.err) |err| return err;

    try std.testing.expectEqual(@as(usize, total_messages), consumer.messages);
    try std.testing.expect(consumer.checksum != 0);

    const write_summary = summarizeSamples(write_samples);

    try writeScenarioResult(allocator, "spsc-write-64B.json", .{
        .scenario = "spsc-write-64B",
        .build_mode = @tagName(builtin.mode),
        .message_size_bytes = payload_size_bytes,
        .ring_capacity_bytes = spsc_capacity_bytes,
        .batch_size = spsc_batch_size,
        .warmup_messages = warmup_messages,
        .measured_messages = measured_messages,
        .throughput_msgs_per_sec = throughputPerSecond(measured_messages, producer.total_elapsed_ns),
        .throughput_payload_bytes_per_sec = throughputPerSecond(measured_messages * payload_size_bytes, producer.total_elapsed_ns),
        .latency_min_ns = write_summary.min_ns,
        .latency_p50_ns = write_summary.p50_ns,
        .latency_p95_ns = write_summary.p95_ns,
        .latency_p99_ns = write_summary.p99_ns,
        .latency_avg_ns = write_summary.avg_ns,
        .latency_max_ns = write_summary.max_ns,
        .messages_sent = measured_messages,
        .messages_received = measured_messages,
        .notes = "Producer write cost while a dedicated consumer thread busy-polls the same /dev/shm shared mmap ring buffer; this is saturated throughput, not one-way latency.",
    });
}

fn runSpscHandoffBench(allocator: std.mem.Allocator) !void {
    const measured_messages = handoff_measured_messages;
    const total_messages = handoff_warmup_messages + measured_messages;

    var storage = try RingBufferStorage.create(allocator, spsc_capacity_bytes, "spsc-handoff");
    defer storage.deinit();

    var ring_buffer = try resetRingBuffer(storage.bytes);

    const handoff_samples = try allocator.alloc(u64, measured_messages);
    defer allocator.free(handoff_samples);

    var consumer = SpscHandoffConsumer{
        .ring_buffer = &ring_buffer,
        .measured_messages = measured_messages,
        .handoff_samples_ns = handoff_samples,
    };
    var producer = SpscHandoffProducer{
        .ring_buffer = &ring_buffer,
        .consumer = &consumer,
        .total_messages = total_messages,
    };

    const consumer_thread = try std.Thread.spawn(.{}, SpscHandoffConsumer.run, .{&consumer});
    const producer_thread = try std.Thread.spawn(.{}, SpscHandoffProducer.run, .{&producer});

    producer_thread.join();
    consumer_thread.join();
    if (producer.err) |err| return err;

    try std.testing.expectEqual(@as(usize, total_messages), consumer.seen_messages);
    try std.testing.expectEqual(@as(usize, measured_messages), consumer.recorded_messages);
    try std.testing.expect(consumer.checksum != 0);

    const handoff_summary = summarizeSamples(handoff_samples);

    try writeScenarioResult(allocator, "spsc-handoff-64B.json", .{
        .scenario = "spsc-handoff-64B",
        .build_mode = @tagName(builtin.mode),
        .message_size_bytes = payload_size_bytes,
        .ring_capacity_bytes = spsc_capacity_bytes,
        .batch_size = 1,
        .warmup_messages = handoff_warmup_messages,
        .measured_messages = measured_messages,
        .throughput_msgs_per_sec = throughputPerSecond(measured_messages, producer.total_elapsed_ns),
        .throughput_payload_bytes_per_sec = throughputPerSecond(measured_messages * payload_size_bytes, producer.total_elapsed_ns),
        .latency_min_ns = handoff_summary.min_ns,
        .latency_p50_ns = handoff_summary.p50_ns,
        .latency_p95_ns = handoff_summary.p95_ns,
        .latency_p99_ns = handoff_summary.p99_ns,
        .latency_avg_ns = handoff_summary.avg_ns,
        .latency_max_ns = handoff_summary.max_ns,
        .messages_sent = measured_messages,
        .messages_received = consumer.recorded_messages,
        .notes = "One-message-at-a-time cross-thread send-to-read latency using a /dev/shm shared mmap ring buffer; producer waits for consumer acknowledgement to avoid queueing.",
    });
}

test "ring buffer lower-bound write - 64B" {
    try runSingleThreadedWriteBench(std.testing.allocator);
}

test "ring buffer lower-bound read - 64B" {
    try runSingleThreadedReadBench(std.testing.allocator);
}

test "ring buffer spsc handoff - 64B" {
    try runSpscSaturatedWriteBench(std.testing.allocator);
    try runSpscHandoffBench(std.testing.allocator);
}
