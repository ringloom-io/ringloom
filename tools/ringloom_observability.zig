// SPDX-License-Identifier: Apache-2.0
//! ringloom-observability — Prometheus exporter for RingLoom metadata files.

const std = @import("std");
const net = @import("ringloom_tcp").socket;
const ringloom_common = @import("ringloom_common");

const memory = ringloom_common.memory;
const platform = ringloom_common.platform;
const metadata_reader = ringloom_common.monitoring.metadata_reader;
const prometheus = ringloom_common.monitoring.prometheus;
const ServiceCounter = ringloom_common.monitoring.ServiceCounter;
const SystemCounter = ringloom_common.monitoring.SystemCounter;

const BrokerMetadataFile = memory.BrokerMetadataFile;
const ServiceMetadataFile = memory.ServiceMetadataFile;
const ServiceScanner = memory.ServiceScanner;
const constants = memory.constants;

const Args = struct {
    storage_path: []const u8 = constants.default_storage_path,
    group: []const u8 = "default",
    listen: []const u8 = "127.0.0.1:9464",
    refresh_ms: u64 = 1000,
    broker_node_id: ?i16 = null,
    max_files: usize = 4096,
    max_response_bytes: usize = 8 * 1024 * 1024,
};

const SelfMetrics = struct {
    scrapes_total: u64 = 0,
    scrape_errors_total: u64 = 0,
    scan_errors_total: u64 = 0,
    invalid_metadata_files_total: u64 = 0,
    invalid_regions_total: u64 = 0,
    render_truncated_total: u64 = 0,
    metadata_files: usize = 0,
    last_scan_timestamp_seconds: i64 = 0,
    ready: bool = false,
};

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const allocator = init.arena.allocator();
    const argv = try init.minimal.args.toSlice(allocator);

    var stderr_buf: [4096]u8 = undefined;
    var stderr_w = std.Io.File.stderr().writer(io, &stderr_buf);
    const stderr = &stderr_w.interface;
    defer stderr.flush() catch {};

    const args = parseArgs(argv, stderr);
    var self_metrics: SelfMetrics = .{};

    const listen_addr = parseListen(args.listen) catch |err| {
        try stderr.print("error: invalid --listen '{s}': {s}\n", .{ args.listen, @errorName(err) });
        std.process.exit(2);
    };

    const fd = try net.socket(
        listen_addr.any.family,
        std.posix.SOCK.STREAM | std.posix.SOCK.CLOEXEC,
        0,
    );
    defer platform.closeFd(fd);
    try net.bind(fd, &listen_addr.any, listen_addr.getOsSockLen());
    try net.listen(fd, 128);

    while (true) {
        var peer_addr: net.Address = undefined;
        var peer_len: std.posix.socklen_t = @sizeOf(std.posix.sockaddr);
        const client_fd = net.accept(fd, &peer_addr.any, &peer_len, std.posix.SOCK.CLOEXEC) catch |err| switch (err) {
            error.WouldBlock => continue,
            else => return err,
        };
        {
            defer platform.closeFd(client_fd);
            handleClient(client_fd, args, &self_metrics) catch {
                self_metrics.scrape_errors_total += 1;
            };
        }
    }
}

fn handleClient(fd: std.posix.fd_t, args: Args, self_metrics: *SelfMetrics) !void {
    var req_buf: [4096]u8 = undefined;
    const n = std.posix.read(fd, &req_buf) catch 0;
    const request = req_buf[0..n];
    const path = parsePath(request);

    if (std.mem.eql(u8, path, "/healthz")) {
        try writeResponse(fd, "200 OK", "text/plain", "ok\n");
        return;
    }
    if (std.mem.eql(u8, path, "/readyz")) {
        if (self_metrics.ready) {
            try writeResponse(fd, "200 OK", "text/plain", "ready\n");
        } else {
            try writeResponse(fd, "503 Service Unavailable", "text/plain", "not ready\n");
        }
        return;
    }
    if (!std.mem.eql(u8, path, "/metrics")) {
        try writeResponse(fd, "404 Not Found", "text/plain", "not found\n");
        return;
    }

    self_metrics.scrapes_total += 1;
    const response_buf = try std.heap.page_allocator.alloc(u8, args.max_response_bytes);
    defer std.heap.page_allocator.free(response_buf);

    var writer: std.Io.Writer = .fixed(response_buf);
    renderMetrics(&writer, args, self_metrics) catch |err| switch (err) {
        error.WriteFailed => {
            self_metrics.render_truncated_total += 1;
            try writeResponse(fd, "500 Internal Server Error", "text/plain", "metrics response exceeded --max-response-bytes\n");
            return;
        },
        else => return err,
    };
    try writeResponse(fd, "200 OK", "text/plain; version=0.0.4; charset=utf-8", writer.buffered());
}

fn renderMetrics(writer: *std.Io.Writer, args: Args, self_metrics: *SelfMetrics) !void {
    try prometheus.writeHelpType(writer, "ringloom_metadata_file_up", "Metadata file parse status.", "gauge");
    try prometheus.writeHelpType(writer, "ringloom_process_alive", "Metadata owner process liveness.", "gauge");
    try prometheus.writeHelpType(writer, "ringloom_heartbeat_age_seconds", "Age of metadata heartbeat.", "gauge");
    try prometheus.writeHelpType(writer, "ringloom_process_start_time_seconds", "Metadata owner process start timestamp.", "gauge");
    try prometheus.writeHelpType(writer, "ringloom_metadata_version", "Metadata monitoring version.", "gauge");
    try prometheus.writeHelpType(writer, "ringloom_ring_capacity_bytes", "Ring data capacity.", "gauge");
    try prometheus.writeHelpType(writer, "ringloom_ring_used_bytes", "Ring bytes currently used.", "gauge");
    try prometheus.writeHelpType(writer, "ringloom_ring_free_bytes", "Ring bytes currently free.", "gauge");
    try prometheus.writeHelpType(writer, "ringloom_ring_usage_ratio", "Ring occupancy ratio.", "gauge");
    try prometheus.writeHelpType(writer, "ringloom_ring_producer_position", "Ring producer position.", "gauge");
    try prometheus.writeHelpType(writer, "ringloom_ring_consumer_position", "Ring consumer position.", "gauge");
    try prometheus.writeHelpType(writer, "ringloom_ring_consumer_heartbeat_age_seconds", "Age of ring consumer heartbeat.", "gauge");
    try prometheus.writeHelpType(writer, "ringloom_counter", "Generic metadata counter by stored counter label.", "untyped");

    self_metrics.metadata_files = 0;
    const now_ms = platform.Clock.epochMillis();
    const now_ns = platform.Clock.monotonicNanosStable();
    var dir_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_path = std.fmt.bufPrint(&dir_path_buf, "{s}/{s}/services", .{
        args.storage_path,
        args.group,
    }) catch return error.PathTooLong;

    const io = platform.defaultIo();
    var dir = std.Io.Dir.openDirAbsolute(io, dir_path, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => {
            self_metrics.ready = true;
            self_metrics.last_scan_timestamp_seconds = @divTrunc(now_ms, 1000);
            try renderSelfMetrics(writer, args.group, self_metrics);
            return;
        },
        else => {
            self_metrics.scan_errors_total += 1;
            return err;
        },
    };
    defer dir.close(io);

    var files_seen: usize = 0;
    var iter = dir.iterate();
    while (try iter.next(io)) |entry| {
        if (entry.kind != .file) continue;
        if (!std.mem.endsWith(u8, entry.name, ".dat")) continue;
        if (files_seen >= args.max_files) break;
        files_seen += 1;

        if (BrokerMetadataFile.isBrokerMetadataFile(entry.name)) {
            const node_id = parseBrokerNodeId(entry.name) orelse continue;
            if (args.broker_node_id) |wanted| {
                if (node_id != wanted) continue;
            }
                renderBroker(writer, args, node_id, now_ms, now_ns, self_metrics) catch {
                self_metrics.invalid_metadata_files_total += 1;
            };
        } else if (ServiceScanner.parseFileName(entry.name)) |parsed| {
            if (args.broker_node_id) |wanted| {
                if (parsed.node_id != wanted) continue;
            }
            renderService(writer, args, parsed, now_ms, self_metrics) catch {
                self_metrics.invalid_metadata_files_total += 1;
            };
        }
    }

    self_metrics.metadata_files = files_seen;
    self_metrics.ready = true;
    self_metrics.last_scan_timestamp_seconds = @divTrunc(now_ms, 1000);
    try renderSelfMetrics(writer, args.group, self_metrics);
}

fn renderBroker(writer: *std.Io.Writer, args: Args, node_id: i16, now_ms: i64, now_ns: i64, self_metrics: *SelfMetrics) !void {
    var broker = try BrokerMetadataFile.open(args.storage_path, args.group, node_id);
    defer broker.close();
    const alive: u8 = if (platform.isProcessAlive(@intCast(broker.header.pid))) 1 else 0;
    try writer.print("ringloom_metadata_file_up{{group=\"", .{});
    try prometheus.writeLabelValue(writer, args.group);
    try writer.print("\",owner_type=\"broker\",node_id=\"{d}\",service_id=\"0\",service_name=\"broker\"}} 1\n", .{node_id});
    try writer.print("ringloom_process_alive{{group=\"{s}\",owner_type=\"broker\",node_id=\"{d}\",service_id=\"0\",service_name=\"broker\"}} {d}\n", .{ args.group, node_id, alive });
    try writer.print("ringloom_heartbeat_age_seconds{{group=\"{s}\",owner_type=\"broker\",node_id=\"{d}\",service_id=\"0\",service_name=\"broker\"}} {d:.3}\n", .{ args.group, node_id, metadata_reader.heartbeatAgeSeconds(now_ms, broker.loadHeartbeat()) });
    try writer.print("ringloom_process_start_time_seconds{{group=\"{s}\",owner_type=\"broker\",node_id=\"{d}\",service_id=\"0\",service_name=\"broker\"}} {d:.3}\n", .{ args.group, node_id, epochMillisToSeconds(broker.header.start_timestamp_ms) });
    try writer.print("ringloom_metadata_version{{group=\"{s}\",owner_type=\"broker\",node_id=\"{d}\",service_id=\"0\",service_name=\"broker\"}} {d}\n", .{ args.group, node_id, broker.loadMetadataMonitoringVersion() });
    try renderRing(writer, args.group, "broker", node_id, 0, "broker", "control", broker.getControlBuffer(), @intCast(broker.header.control_buffer_length), now_ms);
    try renderRing(writer, args.group, "broker", node_id, 0, "broker", "send", broker.getSendBuffer(), @intCast(broker.header.messages_buffer_length), now_ms);
    try renderCounters(writer, args.group, "broker", node_id, 0, "broker", broker.getCounterValuesBuffer(), broker.getCounterMetadataBuffer());
    if (broker.getFlowControlRegion()) |region| try renderFlowControl(writer, args.group, node_id, region, now_ns);
    if (broker.getPeerSendCountersRegion()) |region| try renderPeerCounters(writer, args.group, node_id, region, now_ns);
    _ = self_metrics;
}

fn renderService(writer: *std.Io.Writer, args: Args, parsed: ServiceScanner.ParsedFileName, now_ms: i64, self_metrics: *SelfMetrics) !void {
    var service = try ServiceMetadataFile.open(args.storage_path, args.group, parsed.name, parsed.id, parsed.node_id);
    defer service.close();
    const alive: u8 = if (service.isProcessAlive()) 1 else 0;
    try writer.print("ringloom_metadata_file_up{{group=\"", .{});
    try prometheus.writeLabelValue(writer, args.group);
    try writer.print("\",owner_type=\"service\",node_id=\"{d}\",service_id=\"{d}\",service_name=\"", .{ parsed.node_id, parsed.id });
    try prometheus.writeLabelValue(writer, parsed.name);
    try writer.print("\"}} 1\n", .{});
    try writer.print("ringloom_process_alive{{group=\"{s}\",owner_type=\"service\",node_id=\"{d}\",service_id=\"{d}\",service_name=\"", .{ args.group, parsed.node_id, parsed.id });
    try prometheus.writeLabelValue(writer, parsed.name);
    try writer.print("\"}} {d}\n", .{alive});
    try writer.print("ringloom_heartbeat_age_seconds{{group=\"{s}\",owner_type=\"service\",node_id=\"{d}\",service_id=\"{d}\",service_name=\"", .{ args.group, parsed.node_id, parsed.id });
    try prometheus.writeLabelValue(writer, parsed.name);
    try writer.print("\"}} {d:.3}\n", .{metadata_reader.heartbeatAgeSeconds(now_ms, service.loadHeartbeat())});
    try writer.print("ringloom_process_start_time_seconds{{group=\"{s}\",owner_type=\"service\",node_id=\"{d}\",service_id=\"{d}\",service_name=\"", .{ args.group, parsed.node_id, parsed.id });
    try prometheus.writeLabelValue(writer, parsed.name);
    try writer.print("\"}} {d:.3}\n", .{epochMillisToSeconds(service.header.start_timestamp_ms)});
    try writer.print("ringloom_metadata_version{{group=\"{s}\",owner_type=\"service\",node_id=\"{d}\",service_id=\"{d}\",service_name=\"", .{ args.group, parsed.node_id, parsed.id });
    try prometheus.writeLabelValue(writer, parsed.name);
    try writer.print("\"}} {d}\n", .{service.loadMetadataMonitoringVersion()});
    try renderRing(writer, args.group, "service", parsed.node_id, parsed.id, parsed.name, "control", service.getControlBuffer(), @intCast(service.header.control_buffer_length), now_ms);
    try renderRing(writer, args.group, "service", parsed.node_id, parsed.id, parsed.name, "messages", service.getMessagesBuffer(), @intCast(service.header.messages_buffer_length), now_ms);
    try renderCounters(writer, args.group, "service", parsed.node_id, parsed.id, parsed.name, service.getCounterValuesBuffer(), service.getCounterMetadataBuffer());
    _ = self_metrics;
}

fn renderRing(writer: *std.Io.Writer, group: []const u8, owner_type: []const u8, node_id: i16, service_id: i32, service_name: []const u8, ring: []const u8, buffer: []u8, capacity: usize, now_ms: i64) !void {
    const stats = metadata_reader.deriveRingStats(buffer, capacity);
    try writer.print("ringloom_ring_capacity_bytes{{group=\"{s}\",owner_type=\"{s}\",node_id=\"{d}\",service_id=\"{d}\",service_name=\"", .{ group, owner_type, node_id, service_id });
    try prometheus.writeLabelValue(writer, service_name);
    try writer.print("\",ring=\"{s}\"}} {d}\n", .{ ring, stats.capacity });
    try writer.print("ringloom_ring_used_bytes{{group=\"{s}\",owner_type=\"{s}\",node_id=\"{d}\",service_id=\"{d}\",service_name=\"", .{ group, owner_type, node_id, service_id });
    try prometheus.writeLabelValue(writer, service_name);
    try writer.print("\",ring=\"{s}\"}} {d}\n", .{ ring, stats.used_bytes });
    try writer.print("ringloom_ring_free_bytes{{group=\"{s}\",owner_type=\"{s}\",node_id=\"{d}\",service_id=\"{d}\",service_name=\"", .{ group, owner_type, node_id, service_id });
    try prometheus.writeLabelValue(writer, service_name);
    try writer.print("\",ring=\"{s}\"}} {d}\n", .{ ring, stats.free_bytes });
    try writer.print("ringloom_ring_usage_ratio{{group=\"{s}\",owner_type=\"{s}\",node_id=\"{d}\",service_id=\"{d}\",service_name=\"", .{ group, owner_type, node_id, service_id });
    try prometheus.writeLabelValue(writer, service_name);
    try writer.print("\",ring=\"{s}\"}} {d:.6}\n", .{ ring, stats.usageRatio() });
    try writer.print("ringloom_ring_producer_position{{group=\"{s}\",owner_type=\"{s}\",node_id=\"{d}\",service_id=\"{d}\",service_name=\"", .{ group, owner_type, node_id, service_id });
    try prometheus.writeLabelValue(writer, service_name);
    try writer.print("\",ring=\"{s}\"}} {d}\n", .{ ring, stats.producer_position });
    try writer.print("ringloom_ring_consumer_position{{group=\"{s}\",owner_type=\"{s}\",node_id=\"{d}\",service_id=\"{d}\",service_name=\"", .{ group, owner_type, node_id, service_id });
    try prometheus.writeLabelValue(writer, service_name);
    try writer.print("\",ring=\"{s}\"}} {d}\n", .{ ring, stats.consumer_position });
    try writer.print("ringloom_ring_consumer_heartbeat_age_seconds{{group=\"{s}\",owner_type=\"{s}\",node_id=\"{d}\",service_id=\"{d}\",service_name=\"", .{ group, owner_type, node_id, service_id });
    try prometheus.writeLabelValue(writer, service_name);
    try writer.print("\",ring=\"{s}\"}} {d:.3}\n", .{ ring, metadata_reader.heartbeatAgeSeconds(now_ms, stats.consumer_heartbeat_ms) });
}

fn renderCounters(writer: *std.Io.Writer, group: []const u8, owner_type: []const u8, node_id: i16, service_id: i32, service_name: []const u8, values: []align(constants.cache_line_pad) u8, metadata: []u8) !void {
    const capacity = metadata_reader.counterCapacity(values, metadata);
    for (0..capacity) |id| {
        const sample = metadata_reader.readCounter(values, metadata, id) orelse continue;
        try writer.print("ringloom_counter{{group=\"{s}\",owner_type=\"{s}\",node_id=\"{d}\",service_id=\"{d}\",service_name=\"", .{ group, owner_type, node_id, service_id });
        try prometheus.writeLabelValue(writer, service_name);
        try writer.print("\",counter=\"", .{});
        try prometheus.writeLabelValue(writer, sample.label);
        try writer.print("\"}} {d}\n", .{sample.value});
        try renderDirectCounter(writer, group, owner_type, node_id, service_id, service_name, sample);
    }
}

fn renderFlowControl(writer: *std.Io.Writer, group: []const u8, source_node_id: i16, region: memory.FlowControlRegion, now_ns: i64) !void {
    for (0..region.max_entries) |i| {
        const entry = region.getEntry(@intCast(i)) orelse continue;
        if (entry.loadState() != .allocated) continue;
        const capacity = entry.capacity;
        const remaining = entry.loadRemainingBytes();
        const usage = if (capacity == 0) 0.0 else 1.0 - (@as(f64, @floatFromInt(remaining)) / @as(f64, @floatFromInt(capacity)));
        try writer.print("ringloom_flow_control_remaining_bytes{{group=\"{s}\",source_node_id=\"{d}\",node_id=\"{d}\",service_id=\"{d}\",slot=\"{d}\"}} {d}\n", .{ group, source_node_id, entry.node_id, entry.service_id, i, remaining });
        try writer.print("ringloom_flow_control_capacity_bytes{{group=\"{s}\",source_node_id=\"{d}\",node_id=\"{d}\",service_id=\"{d}\",slot=\"{d}\"}} {d}\n", .{ group, source_node_id, entry.node_id, entry.service_id, i, capacity });
        try writer.print("ringloom_flow_control_usage_ratio{{group=\"{s}\",source_node_id=\"{d}\",node_id=\"{d}\",service_id=\"{d}\",slot=\"{d}\"}} {d:.6}\n", .{ group, source_node_id, entry.node_id, entry.service_id, i, usage });
        try writer.print("ringloom_flow_control_pressure_state{{group=\"{s}\",source_node_id=\"{d}\",node_id=\"{d}\",service_id=\"{d}\",slot=\"{d}\"}} {d}\n", .{ group, source_node_id, entry.node_id, entry.service_id, i, @intFromEnum(entry.loadPressureState()) });
        try writer.print("ringloom_flow_control_update_age_seconds{{group=\"{s}\",source_node_id=\"{d}\",node_id=\"{d}\",service_id=\"{d}\",slot=\"{d}\"}} {d:.3}\n", .{ group, source_node_id, entry.node_id, entry.service_id, i, monotonicAgeSeconds(now_ns, entry.loadLastUpdateNs()) });
    }
}

fn renderPeerCounters(writer: *std.Io.Writer, group: []const u8, node_id: i16, region: memory.PeerSendCountersRegion, now_ns: i64) !void {
    for (0..region.entry_count) |i| {
        const entry = &region.entries[i];
        if (entry.loadState() != 1) continue;
        const connected: u8 = if (entry.loadConnectionState()) 1 else 0;
        try writer.print("ringloom_broker_peer_connected{{group=\"{s}\",node_id=\"{d}\",peer_node_id=\"{d}\"}} {d}\n", .{ group, node_id, entry.node_id, connected });
        try writer.print("ringloom_broker_peer_ring_pending_bytes{{group=\"{s}\",node_id=\"{d}\",peer_node_id=\"{d}\"}} {d}\n", .{ group, node_id, entry.node_id, entry.loadRingBytesPending() });
        try writer.print("ringloom_broker_peer_queue_pending_bytes{{group=\"{s}\",node_id=\"{d}\",peer_node_id=\"{d}\"}} {d}\n", .{ group, node_id, entry.node_id, entry.loadQueueBytesPending() });
        try writer.print("ringloom_broker_peer_queue_capacity_bytes{{group=\"{s}\",node_id=\"{d}\",peer_node_id=\"{d}\"}} {d}\n", .{ group, node_id, entry.node_id, entry.queue_capacity });
        try writer.print("ringloom_broker_peer_bytes_sent_total{{group=\"{s}\",node_id=\"{d}\",peer_node_id=\"{d}\"}} {d}\n", .{ group, node_id, entry.node_id, entry.loadTotalBytesSent() });
        try writer.print("ringloom_broker_peer_bytes_dropped_total{{group=\"{s}\",node_id=\"{d}\",peer_node_id=\"{d}\"}} {d}\n", .{ group, node_id, entry.node_id, entry.loadTotalBytesDropped() });
        try writer.print("ringloom_broker_peer_counter_update_age_seconds{{group=\"{s}\",node_id=\"{d}\",peer_node_id=\"{d}\"}} {d:.3}\n", .{ group, node_id, entry.node_id, monotonicAgeSeconds(now_ns, @intCast(entry.loadLastUpdateNs())) });
    }
}

fn renderDirectCounter(writer: *std.Io.Writer, group: []const u8, owner_type: []const u8, node_id: i16, service_id: i32, service_name: []const u8, sample: metadata_reader.CounterSample) !void {
    if (std.mem.eql(u8, owner_type, "broker")) {
        const metric = brokerDirectMetricName(sample) orelse return;
        try writer.print("{s}{{group=\"{s}\",node_id=\"{d}\"}} {d}\n", .{ metric, group, node_id, sample.value });
    } else if (std.mem.eql(u8, owner_type, "service")) {
        const metric = serviceDirectMetricName(sample) orelse return;
        try writer.print("{s}{{group=\"{s}\",node_id=\"{d}\",service_id=\"{d}\",service_name=\"", .{ metric, group, node_id, service_id });
        try prometheus.writeLabelValue(writer, service_name);
        try writer.print("\"}} {d}\n", .{sample.value});
    }
}

fn brokerDirectMetricName(sample: metadata_reader.CounterSample) ?[]const u8 {
    if (brokerRuntimeMetricName(sample.label)) |metric| return metric;

    if (sample.type_id < 0 or sample.type_id >= SystemCounter.count) return null;
    const counter: SystemCounter = @enumFromInt(@as(u8, @intCast(sample.type_id)));
    if (!std.mem.eql(u8, sample.label, counter.label())) return null;
    return switch (counter) {
        .bytes_sent => "ringloom_broker_bytes_sent_total",
        .bytes_received => "ringloom_broker_bytes_received_total",
        .messages_routed_local => "ringloom_broker_messages_routed_local_total",
        .messages_routed_remote => "ringloom_broker_messages_routed_remote_total",
        .tcp_connections_accepted => "ringloom_broker_tcp_connections_accepted_total",
        .tcp_connection_errors => "ringloom_broker_tcp_connection_errors_total",
        .tcp_handshake_failures => "ringloom_broker_tcp_handshake_failures_total",
        .tcp_reconnect_attempts => "ringloom_broker_tcp_reconnect_attempts_total",
        .heartbeats_sent => "ringloom_broker_heartbeats_sent_total",
        .heartbeats_received => "ringloom_broker_heartbeats_received_total",
        .heartbeat_timeouts => "ringloom_broker_heartbeat_timeouts_total",
        .services_registered => "ringloom_broker_services_registered_total",
        .services_removed => "ringloom_broker_services_removed_total",
        .send_rb_back_pressure => "ringloom_broker_send_ring_backpressure_total",
        .service_back_pressure => "ringloom_broker_service_backpressure_total",
        .unknown_service_drops => "ringloom_broker_unknown_service_drops_total",
        .service_full_drops => "ringloom_broker_service_full_drops_total",
        .peer_queue_overflow_drops => "ringloom_broker_peer_queue_overflow_drops_total",
        .peer_not_connected_drops => "ringloom_broker_peer_not_connected_drops_total",
        .invalid_frames => "ringloom_broker_invalid_frames_total",
        .control_loop_cycle_time_max => "ringloom_broker_control_loop_cycle_time_max_ns",
        .sender_cycle_time_max => "ringloom_broker_sender_cycle_time_max_ns",
        .receiver_cycle_time_max => "ringloom_broker_receiver_cycle_time_max_ns",
        .fc_updates_sent => "ringloom_broker_flow_control_updates_sent_total",
        .fc_updates_received => "ringloom_broker_flow_control_updates_received_total",
        .fc_pressure_events => "ringloom_broker_flow_control_pressure_events_total",
        .fc_recovery_events => "ringloom_broker_flow_control_recovery_events_total",
        .fc_client_backpressure => "ringloom_broker_flow_control_client_backpressure_total",
        .fc_client_spin_timeouts => "ringloom_broker_flow_control_client_spin_timeouts_total",
        .fc_slot_allocations => "ringloom_broker_flow_control_slot_allocations_total",
        .fc_slot_reclamations => "ringloom_broker_flow_control_slot_reclamations_total",
        .fc_peer_congestion_events => "ringloom_broker_flow_control_peer_congestion_events_total",
        .fc_peer_disconnected_sends_avoided => "ringloom_broker_flow_control_peer_disconnected_sends_avoided_total",
    };
}

fn brokerRuntimeMetricName(label: []const u8) ?[]const u8 {
    if (std.mem.eql(u8, label, "frames_sent")) return "ringloom_broker_frames_sent_total";
    if (std.mem.eql(u8, label, "bytes_sent")) return "ringloom_broker_bytes_sent_total";
    if (std.mem.eql(u8, label, "heartbeats_sent")) return "ringloom_broker_heartbeats_sent_total";
    if (std.mem.eql(u8, label, "send_back_pressure")) return "ringloom_broker_send_backpressure_total";
    if (std.mem.eql(u8, label, "malformed_messages_dropped")) return "ringloom_broker_malformed_messages_dropped_total";
    if (std.mem.eql(u8, label, "unknown_peer_messages_dropped")) return "ringloom_broker_unknown_peer_messages_dropped_total";
    if (std.mem.eql(u8, label, "peer_not_connected_drops")) return "ringloom_broker_peer_not_connected_drops_total";
    if (std.mem.eql(u8, label, "peer_queue_overflow_drops")) return "ringloom_broker_peer_queue_overflow_drops_total";
    if (std.mem.eql(u8, label, "send_errors")) return "ringloom_broker_send_errors_total";
    if (std.mem.eql(u8, label, "peers_connected")) return "ringloom_broker_peers_connected_total";
    if (std.mem.eql(u8, label, "peers_disconnected")) return "ringloom_broker_peers_disconnected_total";
    if (std.mem.eql(u8, label, "peers_timed_out")) return "ringloom_broker_peers_timed_out_total";
    if (std.mem.eql(u8, label, "reconnect_attempts")) return "ringloom_broker_reconnect_attempts_total";
    if (std.mem.eql(u8, label, "recv_bytes_received")) return "ringloom_broker_bytes_received_total";
    if (std.mem.eql(u8, label, "recv_frames_routed")) return "ringloom_broker_frames_routed_total";
    if (std.mem.eql(u8, label, "recv_heartbeats_received")) return "ringloom_broker_heartbeats_received_total";
    if (std.mem.eql(u8, label, "recv_unknown_service_drops")) return "ringloom_broker_unknown_service_drops_total";
    if (std.mem.eql(u8, label, "recv_service_full_drops")) return "ringloom_broker_service_full_drops_total";
    if (std.mem.eql(u8, label, "recv_invalid_frame_drops")) return "ringloom_broker_invalid_frame_drops_total";
    if (std.mem.eql(u8, label, "recv_unknown_peer_drops")) return "ringloom_broker_unknown_peer_drops_total";
    if (std.mem.eql(u8, label, "recv_connections_accepted")) return "ringloom_broker_tcp_connections_accepted_total";
    if (std.mem.eql(u8, label, "recv_handshake_failures")) return "ringloom_broker_tcp_handshake_failures_total";
    if (std.mem.eql(u8, label, "recv_connection_errors")) return "ringloom_broker_tcp_connection_errors_total";
    if (std.mem.eql(u8, label, "recv_heartbeat_timeouts")) return "ringloom_broker_heartbeat_timeouts_total";
    if (std.mem.eql(u8, label, "recv_peer_reconnects")) return "ringloom_broker_peer_reconnects_total";
    if (std.mem.eql(u8, label, "recv_admin_messages_received")) return "ringloom_broker_admin_messages_received_total";
    if (std.mem.eql(u8, label, "recv_admin_message_errors")) return "ringloom_broker_admin_message_errors_total";
    return null;
}

fn serviceDirectMetricName(sample: metadata_reader.CounterSample) ?[]const u8 {
    if (sample.type_id < 0 or sample.type_id >= ServiceCounter.count) return null;
    const counter: ServiceCounter = @enumFromInt(@as(u8, @intCast(sample.type_id)));
    if (!std.mem.eql(u8, sample.label, counter.label())) return null;
    return switch (counter) {
        .messages_sent => "ringloom_service_messages_sent_total",
        .bytes_sent => "ringloom_service_bytes_sent_total",
        .messages_received => "ringloom_service_messages_received_total",
        .bytes_received => "ringloom_service_bytes_received_total",
        .send_buffer_full => "ringloom_service_send_buffer_full_total",
        .backpressure => "ringloom_service_backpressure_total",
        .backpressure_timeouts => "ringloom_service_backpressure_timeouts_total",
        .peer_congestion => "ringloom_service_peer_congestion_total",
        .peer_disconnected => "ringloom_service_peer_disconnected_total",
        .no_available_instance => "ringloom_service_no_available_instance_total",
        .control_messages_received => "ringloom_service_control_messages_received_total",
        .heartbeats_sent => "ringloom_service_heartbeats_sent_total",
        .registrations_sent => "ringloom_service_registrations_sent_total",
        .unregisters_sent => "ringloom_service_unregisters_sent_total",
        .subscriptions_sent => "ringloom_service_subscriptions_sent_total",
    };
}

fn epochMillisToSeconds(epoch_ms: i64) f64 {
    if (epoch_ms <= 0) return 0.0;
    return @as(f64, @floatFromInt(epoch_ms)) / 1000.0;
}

fn monotonicAgeSeconds(now_ns: i64, last_update_ns: i64) f64 {
    if (last_update_ns <= 0 or now_ns <= last_update_ns) return 0.0;
    return @as(f64, @floatFromInt(now_ns - last_update_ns)) / @as(f64, @floatFromInt(std.time.ns_per_s));
}

fn renderSelfMetrics(writer: *std.Io.Writer, group: []const u8, self_metrics: *const SelfMetrics) !void {
    try writer.print("ringloom_observability_scrapes_total{{group=\"{s}\"}} {d}\n", .{ group, self_metrics.scrapes_total });
    try writer.print("ringloom_observability_scrape_errors_total{{group=\"{s}\"}} {d}\n", .{ group, self_metrics.scrape_errors_total });
    try writer.print("ringloom_observability_metadata_files{{group=\"{s}\"}} {d}\n", .{ group, self_metrics.metadata_files });
    try writer.print("ringloom_observability_metadata_scan_errors_total{{group=\"{s}\"}} {d}\n", .{ group, self_metrics.scan_errors_total });
    try writer.print("ringloom_observability_invalid_metadata_files_total{{group=\"{s}\"}} {d}\n", .{ group, self_metrics.invalid_metadata_files_total });
    try writer.print("ringloom_observability_invalid_regions_total{{group=\"{s}\"}} {d}\n", .{ group, self_metrics.invalid_regions_total });
    try writer.print("ringloom_observability_render_truncated_total{{group=\"{s}\"}} {d}\n", .{ group, self_metrics.render_truncated_total });
    try writer.print("ringloom_observability_last_scan_timestamp_seconds{{group=\"{s}\"}} {d}\n", .{ group, self_metrics.last_scan_timestamp_seconds });
}

fn writeResponse(fd: std.posix.fd_t, status: []const u8, content_type: []const u8, body: []const u8) !void {
    var header_buf: [512]u8 = undefined;
    const header = try std.fmt.bufPrint(&header_buf,
        "HTTP/1.1 {s}\r\nContent-Type: {s}\r\nCache-Control: no-cache\r\nContent-Length: {d}\r\nConnection: close\r\n\r\n",
        .{ status, content_type, body.len },
    );
    _ = try net.write(fd, header);
    if (body.len > 0) _ = try net.write(fd, body);
}

fn parsePath(request: []const u8) []const u8 {
    if (!std.mem.startsWith(u8, request, "GET ")) return "/";
    const rest = request[4..];
    const end = std.mem.indexOfScalar(u8, rest, ' ') orelse return "/";
    return rest[0..end];
}

fn parseListen(value: []const u8) !net.Address {
    const colon = std.mem.lastIndexOfScalar(u8, value, ':') orelse return error.InvalidListenAddress;
    const host = value[0..colon];
    const port = try std.fmt.parseInt(u16, value[colon + 1 ..], 10);
    return net.Address.parseIp4(host, port);
}

fn parseBrokerNodeId(file_name: []const u8) ?i16 {
    if (!BrokerMetadataFile.isBrokerMetadataFile(file_name)) return null;
    const id_text = file_name["broker_".len .. file_name.len - ".dat".len];
    return std.fmt.parseInt(i16, id_text, 10) catch null;
}

fn parseArgs(argv: []const []const u8, stderr: *std.Io.Writer) Args {
    var args: Args = .{};
    var i: usize = 1;
    while (i < argv.len) : (i += 1) {
        const arg = argv[i];
        if (std.mem.eql(u8, arg, "--help") or std.mem.eql(u8, arg, "-h")) {
            printUsage(stderr) catch {};
            stderr.flush() catch {};
            std.process.exit(0);
        } else if (std.mem.eql(u8, arg, "--storage-path")) {
            i += 1;
            if (i >= argv.len) fatal(stderr, "--storage-path requires a value", .{});
            args.storage_path = argv[i];
        } else if (std.mem.eql(u8, arg, "--group")) {
            i += 1;
            if (i >= argv.len) fatal(stderr, "--group requires a value", .{});
            args.group = argv[i];
        } else if (std.mem.eql(u8, arg, "--listen")) {
            i += 1;
            if (i >= argv.len) fatal(stderr, "--listen requires a value", .{});
            args.listen = argv[i];
        } else if (std.mem.eql(u8, arg, "--refresh-ms")) {
            i += 1;
            if (i >= argv.len) fatal(stderr, "--refresh-ms requires a value", .{});
            args.refresh_ms = std.fmt.parseInt(u64, argv[i], 10) catch fatal(stderr, "--refresh-ms must be an integer", .{});
        } else if (std.mem.eql(u8, arg, "--broker-node-id")) {
            i += 1;
            if (i >= argv.len) fatal(stderr, "--broker-node-id requires a value", .{});
            args.broker_node_id = std.fmt.parseInt(i16, argv[i], 10) catch fatal(stderr, "--broker-node-id must be an integer", .{});
        } else if (std.mem.eql(u8, arg, "--max-files")) {
            i += 1;
            if (i >= argv.len) fatal(stderr, "--max-files requires a value", .{});
            args.max_files = std.fmt.parseInt(usize, argv[i], 10) catch fatal(stderr, "--max-files must be an integer", .{});
        } else if (std.mem.eql(u8, arg, "--max-response-bytes")) {
            i += 1;
            if (i >= argv.len) fatal(stderr, "--max-response-bytes requires a value", .{});
            args.max_response_bytes = std.fmt.parseInt(usize, argv[i], 10) catch fatal(stderr, "--max-response-bytes must be an integer", .{});
        } else {
            fatal(stderr, "unknown option: {s}", .{arg});
        }
    }
    return args;
}

fn printUsage(writer: *std.Io.Writer) !void {
    try writer.writeAll(
        \\Usage:
        \\  ringloom-observability --storage-path PATH --group GROUP --listen 127.0.0.1:9464
        \\
    );
}

fn fatal(stderr: *std.Io.Writer, comptime fmt: []const u8, values: anytype) noreturn {
    stderr.print("error: " ++ fmt ++ "\n", values) catch {};
    printUsage(stderr) catch {};
    stderr.flush() catch {};
    std.process.exit(2);
}
