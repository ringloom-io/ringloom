// SPDX-License-Identifier: Apache-2.0
//! Read-only Aeron CnC counter reader for out-of-process observability.

const std = @import("std");
const platform = @import("../platform.zig");

pub const Error = error{
    MissingCncFile,
    InvalidCncFile,
    PathTooLong,
};

pub const cnc_file_name = "cnc.dat";
pub const cnc_metadata_length: usize = 128;
pub const counter_value_length: usize = 128;
pub const counter_metadata_length: usize = 512;
pub const counter_key_offset: usize = 16;
pub const counter_key_length: usize = 112;
pub const counter_label_length_offset: usize = 128;
pub const counter_label_offset: usize = 132;
pub const counter_label_length: usize = 380;

const counter_record_allocated: i32 = 1;

pub const system_counter_type_id: i32 = 0;
pub const publisher_limit_type_id: i32 = 1;
pub const sender_position_type_id: i32 = 2;
pub const receiver_hwm_type_id: i32 = 3;
pub const subscription_position_type_id: i32 = 4;
pub const receiver_position_type_id: i32 = 5;
pub const send_channel_status_type_id: i32 = 6;
pub const receive_channel_status_type_id: i32 = 7;
pub const sender_limit_type_id: i32 = 9;
pub const publisher_position_type_id: i32 = 12;
pub const sender_back_pressure_events_type_id: i32 = 13;
pub const flow_control_receiver_count_type_id: i32 = 17;

pub const ChannelKind = enum {
    ipc,
    udp_data,
    udp_admin,
    unknown,

    pub fn label(self: ChannelKind) []const u8 {
        return switch (self) {
            .ipc => "ipc",
            .udp_data => "udp_data",
            .udp_admin => "udp_admin",
            .unknown => "unknown",
        };
    }
};

pub const SystemCounter = enum(i32) {
    bytes_sent = 0,
    bytes_received = 1,
    receiver_proxy_fails = 2,
    sender_proxy_fails = 3,
    conductor_proxy_fails = 4,
    naks_sent = 5,
    naks_received = 6,
    status_messages_sent = 7,
    status_messages_received = 8,
    heartbeats_sent = 9,
    heartbeats_received = 10,
    retransmits_sent = 11,
    flow_control_under_runs = 12,
    flow_control_over_runs = 13,
    invalid_packets = 14,
    errors = 15,
    short_sends = 16,
    free_fails = 17,
    sender_flow_control_limits = 18,
    unblocked_publications = 19,
    unblocked_commands = 20,
    possible_ttl_asymmetry = 21,
    controllable_idle_strategy = 22,
    loss_gap_fills = 23,
    client_timeouts = 24,
    resolution_changes = 25,
    conductor_max_cycle_time = 26,
    conductor_cycle_time_threshold_exceeded = 27,
    sender_max_cycle_time = 28,
    sender_cycle_time_threshold_exceeded = 29,
    receiver_max_cycle_time = 30,
    receiver_cycle_time_threshold_exceeded = 31,
    name_resolver_max_time = 32,
    name_resolver_time_threshold_exceeded = 33,
    aeron_version = 34,
    bytes_currently_mapped = 35,
    retransmitted_bytes = 36,
    retransmit_overflow = 37,
    error_frames_received = 38,
    error_frames_sent = 39,
};

pub const StreamKey = struct {
    registration_id: i64,
    session_id: i32,
    stream_id: i32,
    channel: []const u8,
};

pub const CounterSample = struct {
    id: usize,
    type_id: i32,
    key: []const u8,
    label: []const u8,
    value: i64,
    registration_id: i64,
    owner_id: i64,
    reference_id: i64,

    pub fn systemCounter(self: CounterSample) ?SystemCounter {
        if (self.type_id != system_counter_type_id or self.key.len < 4) return null;
        const raw = std.mem.readInt(i32, self.key[0..4], .little);
        if (raw < 0 or raw > @intFromEnum(SystemCounter.error_frames_sent)) return null;
        return @enumFromInt(raw);
    }

    pub fn streamKey(self: CounterSample) ?StreamKey {
        if (!isStreamPositionType(self.type_id)) return null;
        if (self.key.len < 20) return null;

        const channel_len_raw = std.mem.readInt(i32, self.key[16..20], .little);
        if (channel_len_raw < 0) return null;
        const channel_len: usize = @intCast(channel_len_raw);
        if (20 + channel_len > self.key.len) return null;

        return .{
            .registration_id = std.mem.readInt(i64, self.key[0..8], .little),
            .session_id = std.mem.readInt(i32, self.key[8..12], .little),
            .stream_id = std.mem.readInt(i32, self.key[12..16], .little),
            .channel = self.key[20 .. 20 + channel_len],
        };
    }
};

pub const Metadata = struct {
    cnc_version: i32,
    to_driver_buffer_length: i32,
    to_clients_buffer_length: i32,
    counter_metadata_buffer_length: i32,
    counter_values_buffer_length: i32,
    error_log_buffer_length: i32,
    client_liveness_timeout_ns: i64,
    start_timestamp_ms: i64,
    pid: i64,

    pub fn driverAlive(self: Metadata) bool {
        if (self.pid <= 0) return false;
        return platform.isProcessAlive(@intCast(self.pid));
    }

    pub fn startTimeSeconds(self: Metadata) f64 {
        if (self.start_timestamp_ms <= 0) return 0.0;
        return @as(f64, @floatFromInt(self.start_timestamp_ms)) / 1000.0;
    }

    pub fn clientLivenessTimeoutSeconds(self: Metadata) f64 {
        if (self.client_liveness_timeout_ns <= 0) return 0.0;
        return @as(f64, @floatFromInt(self.client_liveness_timeout_ns)) /
            @as(f64, @floatFromInt(std.time.ns_per_s));
    }
};

pub const CncFile = struct {
    file: std.Io.File,
    mapped: []align(std.heap.page_size_min) u8,
    metadata: Metadata,
    counters_metadata: []u8,
    counters_values: []u8,

    pub fn open(io: std.Io, directory: []const u8) !CncFile {
        if (directory.len == 0) return error.MissingCncFile;

        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const path = std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ directory, cnc_file_name }) catch
            return error.PathTooLong;

        const file = (if (std.fs.path.isAbsolute(path))
            std.Io.Dir.openFileAbsolute(io, path, .{ .mode = .read_only })
        else
            std.Io.Dir.cwd().openFile(io, path, .{ .mode = .read_only })) catch |err| switch (err) {
            error.FileNotFound => return error.MissingCncFile,
            else => return err,
        };
        errdefer file.close(io);

        const stat = try file.stat(io);
        if (stat.size < cnc_metadata_length) return error.InvalidCncFile;

        const mapped = try std.posix.mmap(
            null,
            stat.size,
            .{ .READ = true },
            .{ .TYPE = .SHARED },
            file.handle,
            0,
        );
        errdefer std.posix.munmap(mapped);

        const metadata = try readMetadata(mapped);
        const offsets = try counterOffsets(mapped.len, metadata);

        return .{
            .file = file,
            .mapped = mapped,
            .metadata = metadata,
            .counters_metadata = mapped[offsets.metadata_start..offsets.metadata_end],
            .counters_values = mapped[offsets.values_start..offsets.values_end],
        };
    }

    pub fn close(self: *CncFile, io: std.Io) void {
        std.posix.munmap(self.mapped);
        self.file.close(io);
        self.* = undefined;
    }

    pub fn counterCapacity(self: *const CncFile) usize {
        return @min(
            self.counters_values.len / counter_value_length,
            self.counters_metadata.len / counter_metadata_length,
        );
    }

    pub fn readCounter(self: *const CncFile, id: usize) ?CounterSample {
        if (id >= self.counterCapacity()) return null;
        const meta_base = id * counter_metadata_length;
        const value_base = id * counter_value_length;

        const state = loadAt(i32, self.counters_metadata, meta_base);
        if (state != counter_record_allocated) return null;

        const label_len_raw = loadAt(i32, self.counters_metadata, meta_base + counter_label_length_offset);
        if (label_len_raw < 0) return null;
        const label_len: usize = @intCast(label_len_raw);
        if (label_len > counter_label_length) return null;

        return .{
            .id = id,
            .type_id = loadAt(i32, self.counters_metadata, meta_base + 4),
            .key = self.counters_metadata[meta_base + counter_key_offset .. meta_base + counter_key_offset + counter_key_length],
            .label = self.counters_metadata[meta_base + counter_label_offset .. meta_base + counter_label_offset + label_len],
            .value = loadAt(i64, self.counters_values, value_base),
            .registration_id = loadAt(i64, self.counters_values, value_base + 8),
            .owner_id = loadAt(i64, self.counters_values, value_base + 16),
            .reference_id = loadAt(i64, self.counters_values, value_base + 24),
        };
    }
};

pub fn isStreamPositionType(type_id: i32) bool {
    return switch (type_id) {
        publisher_limit_type_id,
        sender_position_type_id,
        receiver_hwm_type_id,
        subscription_position_type_id,
        receiver_position_type_id,
        sender_limit_type_id,
        publisher_position_type_id,
        sender_back_pressure_events_type_id,
        flow_control_receiver_count_type_id,
        => true,
        else => false,
    };
}

pub fn channelKind(stream_key: StreamKey, broker_ingress_stream_id: i32, admin_stream_base: i32, data_stream_base: i32) ChannelKind {
    if (std.mem.startsWith(u8, stream_key.channel, "aeron:ipc") or
        (broker_ingress_stream_id != 0 and stream_key.stream_id == broker_ingress_stream_id))
    {
        return .ipc;
    }
    if (admin_stream_base > 0 and stream_key.stream_id >= admin_stream_base and stream_key.stream_id < admin_stream_base + 256) {
        return .udp_admin;
    }
    if (data_stream_base > 0 and stream_key.stream_id >= data_stream_base and stream_key.stream_id < data_stream_base + 256) {
        return .udp_data;
    }
    return .unknown;
}

pub fn streamMetricName(type_id: i32) ?[]const u8 {
    return switch (type_id) {
        publisher_limit_type_id => "ringloom_aeron_publication_limit_position",
        publisher_position_type_id => "ringloom_aeron_publication_position",
        sender_position_type_id => "ringloom_aeron_sender_position",
        sender_limit_type_id => "ringloom_aeron_sender_limit_position",
        sender_back_pressure_events_type_id => "ringloom_aeron_sender_back_pressure_events_total",
        subscription_position_type_id => "ringloom_aeron_subscription_position",
        receiver_position_type_id => "ringloom_aeron_receiver_position",
        receiver_hwm_type_id => "ringloom_aeron_receiver_high_water_mark_position",
        flow_control_receiver_count_type_id => "ringloom_aeron_flow_control_receiver_count",
        else => null,
    };
}

pub fn channelStatusMetricName(type_id: i32) ?[]const u8 {
    return switch (type_id) {
        send_channel_status_type_id => "ringloom_aeron_send_channel_status",
        receive_channel_status_type_id => "ringloom_aeron_receive_channel_status",
        else => null,
    };
}

pub fn systemMetricName(counter: SystemCounter) ?[]const u8 {
    return switch (counter) {
        .bytes_sent => "ringloom_aeron_bytes_sent_total",
        .bytes_received => "ringloom_aeron_bytes_received_total",
        .naks_sent => "ringloom_aeron_naks_sent_total",
        .naks_received => "ringloom_aeron_naks_received_total",
        .status_messages_sent => "ringloom_aeron_status_messages_sent_total",
        .status_messages_received => "ringloom_aeron_status_messages_received_total",
        .heartbeats_sent => "ringloom_aeron_heartbeats_sent_total",
        .heartbeats_received => "ringloom_aeron_heartbeats_received_total",
        .retransmits_sent => "ringloom_aeron_retransmits_sent_total",
        .invalid_packets => "ringloom_aeron_invalid_packets_total",
        .errors => "ringloom_aeron_errors_total",
        .short_sends => "ringloom_aeron_short_sends_total",
        .sender_flow_control_limits => "ringloom_aeron_sender_flow_control_limits_total",
        .unblocked_publications => "ringloom_aeron_unblocked_publications_total",
        .client_timeouts => "ringloom_aeron_client_timeouts_total",
        .conductor_max_cycle_time => "ringloom_aeron_conductor_max_cycle_time_ns",
        .sender_max_cycle_time => "ringloom_aeron_sender_max_cycle_time_ns",
        .receiver_max_cycle_time => "ringloom_aeron_receiver_max_cycle_time_ns",
        .bytes_currently_mapped => "ringloom_aeron_bytes_currently_mapped",
        .retransmitted_bytes => "ringloom_aeron_retransmitted_bytes_total",
        .retransmit_overflow => "ringloom_aeron_retransmit_overflow_total",
        .error_frames_received => "ringloom_aeron_error_frames_received_total",
        .error_frames_sent => "ringloom_aeron_error_frames_sent_total",
        else => null,
    };
}

const CounterOffsets = struct {
    metadata_start: usize,
    metadata_end: usize,
    values_start: usize,
    values_end: usize,
};

fn readMetadata(mapped: []u8) Error!Metadata {
    const metadata = Metadata{
        .cnc_version = loadAt(i32, mapped, 0),
        .to_driver_buffer_length = loadAt(i32, mapped, 4),
        .to_clients_buffer_length = loadAt(i32, mapped, 8),
        .counter_metadata_buffer_length = loadAt(i32, mapped, 12),
        .counter_values_buffer_length = loadAt(i32, mapped, 16),
        .error_log_buffer_length = loadAt(i32, mapped, 20),
        .client_liveness_timeout_ns = loadAt(i64, mapped, 24),
        .start_timestamp_ms = loadAt(i64, mapped, 32),
        .pid = loadAt(i64, mapped, 40),
    };
    if (metadata.cnc_version <= 0 or
        metadata.to_driver_buffer_length < 0 or
        metadata.to_clients_buffer_length < 0 or
        metadata.counter_metadata_buffer_length < 0 or
        metadata.counter_values_buffer_length < 0 or
        metadata.error_log_buffer_length < 0)
    {
        return error.InvalidCncFile;
    }
    return metadata;
}

fn counterOffsets(file_len: usize, metadata: Metadata) Error!CounterOffsets {
    var offset: usize = cnc_metadata_length;
    offset = try addLength(offset, metadata.to_driver_buffer_length, file_len);
    offset = try addLength(offset, metadata.to_clients_buffer_length, file_len);
    const metadata_start = offset;
    offset = try addLength(offset, metadata.counter_metadata_buffer_length, file_len);
    const metadata_end = offset;
    const values_start = offset;
    offset = try addLength(offset, metadata.counter_values_buffer_length, file_len);
    const values_end = offset;
    _ = try addLength(offset, metadata.error_log_buffer_length, file_len);

    if (@as(usize, @intCast(metadata.counter_values_buffer_length)) % counter_value_length != 0 or
        @as(usize, @intCast(metadata.counter_metadata_buffer_length)) % counter_metadata_length != 0)
    {
        return error.InvalidCncFile;
    }

    return .{
        .metadata_start = metadata_start,
        .metadata_end = metadata_end,
        .values_start = values_start,
        .values_end = values_end,
    };
}

fn addLength(offset: usize, length_raw: i32, file_len: usize) Error!usize {
    if (length_raw < 0) return error.InvalidCncFile;
    const length: usize = @intCast(length_raw);
    if (length > file_len or offset > file_len - length) return error.InvalidCncFile;
    return offset + length;
}

fn loadAt(comptime T: type, buffer: []u8, offset: usize) T {
    const ptr: *const T = @ptrCast(@alignCast(buffer.ptr + offset));
    return @atomicLoad(T, ptr, .acquire);
}

test "missing CnC file returns a clear error" {
    try std.testing.expectError(
        error.MissingCncFile,
        CncFile.open(std.testing.io, "/tmp/ringloom-cnc-missing-for-test"),
    );
}

test "stream key parsing and channel kind mapping are stable" {
    var key: [counter_key_length]u8 = [_]u8{0} ** counter_key_length;
    std.mem.writeInt(i64, key[0..8], 123, .little);
    std.mem.writeInt(i32, key[8..12], 7, .little);
    std.mem.writeInt(i32, key[12..16], 30_003, .little);
    std.mem.writeInt(i32, key[16..20], 34, .little);
    @memcpy(key[20..54], "aeron:udp?endpoint=127.0.0.1:40455");

    const sample = CounterSample{
        .id = 1,
        .type_id = sender_position_type_id,
        .key = &key,
        .label = "snd-pos",
        .value = 42,
        .registration_id = 0,
        .owner_id = 0,
        .reference_id = 0,
    };

    const stream_key = sample.streamKey().?;
    try std.testing.expectEqual(@as(i64, 123), stream_key.registration_id);
    try std.testing.expectEqual(@as(i32, 7), stream_key.session_id);
    try std.testing.expectEqual(@as(i32, 30_003), stream_key.stream_id);
    try std.testing.expectEqualStrings("aeron:udp?endpoint=127.0.0.1:40455", stream_key.channel);
    try std.testing.expectEqual(ChannelKind.udp_data, channelKind(stream_key, 10_003, 20_000, 30_000));
}

test "system counter metric mapping covers Aeron pressure and loss counters" {
    try std.testing.expectEqualStrings(
        "ringloom_aeron_sender_flow_control_limits_total",
        systemMetricName(.sender_flow_control_limits).?,
    );
    try std.testing.expectEqualStrings(
        "ringloom_aeron_retransmits_sent_total",
        systemMetricName(.retransmits_sent).?,
    );
    try std.testing.expectEqualStrings(
        "ringloom_aeron_retransmitted_bytes_total",
        systemMetricName(.retransmitted_bytes).?,
    );
}
