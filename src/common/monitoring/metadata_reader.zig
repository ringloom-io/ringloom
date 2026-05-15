//! Shared helpers for reading monitoring data from metadata buffers.

const std = @import("std");
const memory = @import("../memory.zig");
const counters = @import("../concurrent/counters.zig");
const ring_buffer = @import("../concurrent/ring_buffer.zig");

pub const MetadataKind = enum {
    broker,
    service,
};

pub const RingStats = struct {
    capacity: usize,
    producer_position: i64,
    consumer_position: i64,
    consumer_heartbeat_ms: i64,
    used_bytes: usize,
    free_bytes: usize,

    pub fn usageRatio(self: RingStats) f64 {
        if (self.capacity == 0) return 0.0;
        return @as(f64, @floatFromInt(self.used_bytes)) / @as(f64, @floatFromInt(self.capacity));
    }
};

pub const CounterSample = struct {
    id: usize,
    type_id: i32,
    label: []const u8,
    value: i64,
};

pub const DestinationBufferSample = struct {
    target_node_id: i16,
    target_service_id: i32,
    stream_id: u32,
    pressure_state: memory.SendBufferPressureState,
    bytes_pending: u64,
    messages_pending: u64,
    lifetime_messages_dropped: u64,
};

pub const ErrorEntry = struct {
    observation_count: i32,
    first_observation_timestamp: i64,
    last_observation_timestamp: i64,
    description: []const u8,
};

pub fn deriveRingStats(buffer: []u8, capacity: usize) RingStats {
    const trailer_offset = capacity;
    const producer = loadAt(i64, buffer, trailer_offset + ring_buffer.tail_position_offset);
    const consumer = loadAt(i64, buffer, trailer_offset + ring_buffer.head_position_offset);
    const heartbeat = loadAt(i64, buffer, trailer_offset + ring_buffer.consumer_heartbeat_offset);
    const raw_used: usize = if (producer > consumer) @intCast(producer - consumer) else 0;
    const used = @min(raw_used, capacity);
    return .{
        .capacity = capacity,
        .producer_position = producer,
        .consumer_position = consumer,
        .consumer_heartbeat_ms = heartbeat,
        .used_bytes = used,
        .free_bytes = capacity - used,
    };
}

pub fn counterCapacity(values_buffer: []const u8, metadata_buffer: []const u8) usize {
    return @min(
        values_buffer.len / counters.counter_value_length,
        metadata_buffer.len / counters.counter_metadata_length,
    );
}

pub fn readCounter(
    values_buffer: []align(memory.constants.cache_line_pad) u8,
    metadata_buffer: []u8,
    id: usize,
) ?CounterSample {
    if (id >= counterCapacity(values_buffer, metadata_buffer)) return null;

    const meta_base = id * counters.counter_metadata_length;
    const state = loadAt(i32, metadata_buffer, meta_base);
    if (state != @intFromEnum(counters.CounterState.allocated)) return null;

    const type_id = loadPlain(i32, metadata_buffer, meta_base + 4);
    const label_len_raw = loadPlain(i32, metadata_buffer, meta_base + 8);
    if (label_len_raw < 0) return null;
    const label_len: usize = @intCast(label_len_raw);
    const max_label_len = counters.counter_metadata_length - 12;
    if (label_len > max_label_len) return null;

    const value = loadAt(i64, values_buffer, id * counters.counter_value_length);
    return .{
        .id = id,
        .type_id = type_id,
        .label = metadata_buffer[meta_base + 12 .. meta_base + 12 + label_len],
        .value = value,
    };
}

pub fn readDestinationBufferEntry(entry: *const memory.SendBufferEntry) ?DestinationBufferSample {
    const state = entry.loadState();
    if (state != .active and state != .draining) return null;
    return .{
        .target_node_id = entry.target_node_id,
        .target_service_id = entry.target_service_id,
        .stream_id = entry.stream_id,
        .pressure_state = entry.loadPressureState(),
        .bytes_pending = @atomicLoad(u64, @as(*const u64, @ptrCast(@alignCast(&entry.bytes_pending))), .acquire),
        .messages_pending = @atomicLoad(u64, @as(*const u64, @ptrCast(@alignCast(&entry.messages_pending))), .acquire),
        .lifetime_messages_dropped = @atomicLoad(u64, @as(*const u64, @ptrCast(@alignCast(&entry.lifetime_messages_dropped))), .acquire),
    };
}

pub fn pressureStateLabel(state: memory.SendBufferPressureState) []const u8 {
    return switch (state) {
        .unknown => "unknown",
        .normal => "normal",
        .flow_blocked => "flow_blocked",
        .congested => "congested",
        .term_blocked => "term_blocked",
        .peer_down => "peer_down",
        .draining => "draining",
        .closed => "closed",
    };
}

pub fn readErrorEntry(error_log_buffer: []u8, offset: usize) ?struct { entry: ErrorEntry, next_offset: usize } {
    if (offset + @import("../concurrent/error_log.zig").entry_header_length > error_log_buffer.len) return null;
    const entry_header_length = @import("../concurrent/error_log.zig").entry_header_length;
    const entry_alignment = @import("../concurrent/error_log.zig").entry_alignment;
    const entry_length = loadAt(i32, error_log_buffer, offset);
    if (entry_length <= 0) return null;
    const entry_len: usize = @intCast(entry_length);
    if (entry_len < entry_header_length) return null;
    if (offset + entry_len > error_log_buffer.len) return null;
    const desc_len = entry_len - entry_header_length;
    return .{
        .entry = .{
            .observation_count = loadAt(i32, error_log_buffer, offset + 4),
            .last_observation_timestamp = loadAt(i64, error_log_buffer, offset + 8),
            .first_observation_timestamp = loadPlain(i64, error_log_buffer, offset + 16),
            .description = error_log_buffer[offset + entry_header_length .. offset + entry_header_length + desc_len],
        },
        .next_offset = std.mem.alignForward(usize, offset + entry_len, entry_alignment),
    };
}

pub fn heartbeatAgeSeconds(now_ms: i64, heartbeat_ms: i64) f64 {
    if (heartbeat_ms <= 0 or now_ms <= heartbeat_ms) return 0.0;
    return @as(f64, @floatFromInt(now_ms - heartbeat_ms)) / 1000.0;
}

fn loadAt(comptime T: type, buffer: []u8, offset: usize) T {
    const ptr: *const T = @ptrCast(@alignCast(buffer.ptr + offset));
    return @atomicLoad(T, ptr, .acquire);
}

fn loadPlain(comptime T: type, buffer: []u8, offset: usize) T {
    const ptr: *const T = @ptrCast(@alignCast(buffer.ptr + offset));
    return ptr.*;
}

test "deriveRingStats clamps used bytes to capacity" {
    var buf: [1024 + ring_buffer.trailer_length]u8 align(8) = [_]u8{0} ** (1024 + ring_buffer.trailer_length);
    storeAt(i64, &buf, 1024 + ring_buffer.tail_position_offset, 2048);
    storeAt(i64, &buf, 1024 + ring_buffer.head_position_offset, 0);

    const stats = deriveRingStats(&buf, 1024);
    try std.testing.expectEqual(@as(usize, 1024), stats.used_bytes);
    try std.testing.expectEqual(@as(usize, 0), stats.free_bytes);
}

test "readDestinationBufferEntry exposes v2 pressure counters" {
    var entry = memory.SendBufferEntry{
        .target_node_id = 2,
        .target_service_id = 7,
        .stream_id = 123,
    };
    entry.storeState(.active);
    entry.storePressureState(.flow_blocked);
    entry.bytes_pending = 64;
    entry.messages_pending = 2;

    const sample = readDestinationBufferEntry(&entry).?;
    try std.testing.expectEqual(@as(i16, 2), sample.target_node_id);
    try std.testing.expectEqual(@as(i32, 7), sample.target_service_id);
    try std.testing.expectEqual(memory.SendBufferPressureState.flow_blocked, sample.pressure_state);
    try std.testing.expectEqualStrings("flow_blocked", pressureStateLabel(sample.pressure_state));
}

fn storeAt(comptime T: type, buffer: []u8, offset: usize, value: T) void {
    const ptr: *T = @ptrCast(@alignCast(buffer.ptr + offset));
    @atomicStore(T, ptr, value, .release);
}
