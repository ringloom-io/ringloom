//! ringloom-stat — External metadata inspection tool for RingLoom.
//!
//! Usage:
//!   ringloom-stat --storage-path /dev/shm --group ringloom-default
//!   ringloom-stat --storage-path /dev/shm --group ringloom-default --broker-node-id 1
//!   ringloom-stat /dev/shm/ringloom-default/services/broker_1.dat

const std = @import("std");
const ringloom_common = @import("ringloom_common");

const memory = ringloom_common.memory;
const platform = ringloom_common.platform;
const BrokerMetadataFile = memory.BrokerMetadataFile;
const BrokerMetadataHeader = memory.BrokerMetadataHeader;
const ServiceMetadataFile = memory.ServiceMetadataFile;
const ServiceScanner = memory.ServiceScanner;
const constants = memory.constants;
const metadata_reader = ringloom_common.monitoring.metadata_reader;
const aeron_cnc_reader = ringloom_common.monitoring.aeron_cnc_reader;

const Args = struct {
    storage_path: []const u8 = constants.default_storage_path,
    group: []const u8 = "default",
    broker_node_id: ?i16 = null,
    broker_path: ?[]const u8 = null,
};

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    const argv = try init.minimal.args.toSlice(init.arena.allocator());

    var stdout_buf: [4096]u8 = undefined;
    var stdout_w = std.Io.File.stdout().writer(io, &stdout_buf);
    const stdout = &stdout_w.interface;
    defer stdout.flush() catch {};

    var stderr_buf: [4096]u8 = undefined;
    var stderr_w = std.Io.File.stderr().writer(io, &stderr_buf);
    const stderr = &stderr_w.interface;
    defer stderr.flush() catch {};

    const args = parseArgs(argv, stderr);
    if (args.broker_path) |path| {
        try printBrokerPath(io, stdout, path);
    } else {
        try printStorage(io, stdout, args);
    }
}

fn parseArgs(argv: []const []const u8, stderr: *std.Io.Writer) Args {
    var args: Args = .{};
    var i: usize = 1;
    while (i < argv.len) {
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
        } else if (std.mem.eql(u8, arg, "--broker-node-id")) {
            i += 1;
            if (i >= argv.len) fatal(stderr, "--broker-node-id requires a value", .{});
            args.broker_node_id = std.fmt.parseInt(i16, argv[i], 10) catch
                fatal(stderr, "--broker-node-id must be an integer", .{});
        } else if (std.mem.startsWith(u8, arg, "--")) {
            fatal(stderr, "unknown option: {s}", .{arg});
        } else if (args.broker_path == null) {
            args.broker_path = arg;
        } else {
            fatal(stderr, "unexpected positional argument: {s}", .{arg});
        }
        i += 1;
    }
    return args;
}

fn printUsage(writer: *std.Io.Writer) !void {
    try writer.writeAll(
        \\Usage:
        \\  ringloom-stat --storage-path PATH --group GROUP [--broker-node-id N]
        \\  ringloom-stat PATH/TO/broker_N.dat
        \\
    );
}

fn printStorage(io: std.Io, stdout: *std.Io.Writer, args: Args) !void {
    var dir_path_buf: [std.fs.max_path_bytes]u8 = undefined;
    const dir_path = std.fmt.bufPrint(&dir_path_buf, "{s}/{s}/services", .{
        args.storage_path,
        args.group,
    }) catch return error.PathTooLong;

    try stdout.print("=== RingLoom Metadata Status ===\n", .{});
    try stdout.print("Storage: {s}\n", .{args.storage_path});
    try stdout.print("Group:   {s}\n", .{args.group});
    try stdout.print("Dir:     {s}\n\n", .{dir_path});

    var dir = std.Io.Dir.openDirAbsolute(io, dir_path, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => {
            try stdout.print("metadata directory not found\n", .{});
            return;
        },
        else => return err,
    };
    defer dir.close(io);

    const now_ms = platform.Clock.epochMillis();
    try stdout.print("--- Brokers ---\n", .{});
    var broker_count: usize = 0;
    var service_count: usize = 0;

    var iter = dir.iterate();
    while (try iter.next(io)) |entry| {
        if (entry.kind != .file) continue;
        if (!BrokerMetadataFile.isBrokerMetadataFile(entry.name)) continue;
        const node_id = parseBrokerNodeId(entry.name) orelse continue;
        if (args.broker_node_id) |wanted| {
            if (node_id != wanted) continue;
        }
        try printBrokerFromStorage(stdout, args.storage_path, args.group, node_id, now_ms);
        broker_count += 1;
    }
    if (broker_count == 0) {
        try stdout.print("  (none)\n", .{});
    }

    try stdout.print("\n--- Services ---\n", .{});
    iter = dir.iterate();
    while (try iter.next(io)) |entry| {
        if (entry.kind != .file) continue;
        if (BrokerMetadataFile.isBrokerMetadataFile(entry.name)) continue;
        const parsed = ServiceScanner.parseFileName(entry.name) orelse continue;
        if (args.broker_node_id) |wanted| {
            if (parsed.node_id != wanted) continue;
        }
        try printServiceFromStorage(stdout, args.storage_path, args.group, entry.name, parsed, now_ms);
        service_count += 1;
    }
    if (service_count == 0) {
        try stdout.print("  (none)\n", .{});
    }

    try stdout.print("\n--- Done ({d} brokers, {d} services) ---\n", .{ broker_count, service_count });
}

fn printBrokerFromStorage(
    stdout: *std.Io.Writer,
    storage_path: []const u8,
    group: []const u8,
    node_id: i16,
    now_ms: i64,
) !void {
    var broker = BrokerMetadataFile.open(storage_path, group, node_id) catch |err| {
        try stdout.print("  broker node={d}: open failed ({s})\n", .{ node_id, @errorName(err) });
        return;
    };
    defer broker.close();

    try printBrokerSummary(
        stdout,
        node_id,
        broker.header.service_id,
        broker.header.pid,
        broker.header.start_timestamp_ms,
        broker.loadHeartbeat(),
        broker.loadNextServiceId(),
        broker.header.control_buffer_length,
        broker.getAeronDiscovery().broker_ingress_stream_id,
        broker.loadFcBufferLength(),
        now_ms,
    );
    try printBrokerDetails(stdout, &broker, now_ms);
}

fn printBrokerPath(io: std.Io, stdout: *std.Io.Writer, path: []const u8) !void {
    const file = (if (std.fs.path.isAbsolute(path))
        std.Io.Dir.openFileAbsolute(io, path, .{ .mode = .read_only })
    else
        std.Io.Dir.cwd().openFile(io, path, .{ .mode = .read_only })) catch |err| {
        try stdout.print("error: cannot open '{s}': {s}\n", .{ path, @errorName(err) });
        std.process.exit(1);
    };
    defer file.close(io);

    const stat = try file.stat(io);
    if (stat.size < constants.metadata_header_length) {
        try stdout.print("error: file too small ({d} bytes) - not a valid broker metadata file\n", .{stat.size});
        std.process.exit(1);
    }

    const mapped = try std.posix.mmap(
        null,
        stat.size,
        .{ .READ = true },
        .{ .TYPE = .SHARED },
        file.handle,
        0,
    );
    defer std.posix.munmap(mapped);

    const header: *const BrokerMetadataHeader = @ptrCast(@alignCast(mapped.ptr));
    try stdout.print("=== RingLoom Broker Status ({s}) ===\n", .{path});
    try printBrokerSummary(
        stdout,
        header.node_id,
        header.service_id,
        header.pid,
        header.start_timestamp_ms,
        loadAt(i64, mapped, constants.heartbeat_offset_within_header),
        loadAt(i32, mapped, constants.next_service_id_offset_within_header),
        header.control_buffer_length,
        0,
        loadAt(i32, mapped, constants.fc_buffer_length_offset),
        platform.Clock.epochMillis(),
    );
}

fn printBrokerSummary(
    stdout: *std.Io.Writer,
    node_id: i16,
    service_id: i32,
    pid: i64,
    start_timestamp_ms: i64,
    heartbeat_ms: i64,
    next_service_id: i32,
    control_buffer_length: i32,
    broker_ingress_stream_id: i32,
    fc_buffer_length: i32,
    now_ms: i64,
) !void {
    const alive = platform.isProcessAlive(@intCast(pid));
    const age_ms = ageMillis(now_ms, heartbeat_ms);
    try stdout.print(
        "  node={d} status={s} pid={d} heartbeat_age_ms={d} service_id={d} next_service_id={d}\n" ++
            "    started_ms={d} control_buffer={d} aeron_ingress_stream={d} flow_control_bytes={d}\n",
        .{
            node_id,
            if (alive) "alive" else "dead",
            pid,
            age_ms,
            service_id,
            next_service_id,
            start_timestamp_ms,
            control_buffer_length,
            broker_ingress_stream_id,
            fc_buffer_length,
        },
    );
}

fn printServiceFromStorage(
    stdout: *std.Io.Writer,
    storage_path: []const u8,
    group: []const u8,
    file_name: []const u8,
    parsed: ServiceScanner.ParsedFileName,
    now_ms: i64,
) !void {
    var service = ServiceMetadataFile.open(
        storage_path,
        group,
        parsed.name,
        parsed.id,
        parsed.node_id,
    ) catch |err| {
        try stdout.print("  {s}: open failed ({s})\n", .{ file_name, @errorName(err) });
        return;
    };
    defer service.close();

    const heartbeat = service.loadHeartbeat();
    const timeout_ms: i64 = service.header.heartbeat_timeout_ms;
    const alive = service.isProcessAlive();
    const fresh = heartbeat > 0 and ageMillis(now_ms, heartbeat) <= timeout_ms;
    const status = if (alive and fresh)
        "live"
    else if (alive)
        "stale-heartbeat"
    else
        "dead";

    try stdout.print(
        "  node={d} id={d} status={s} pid={d} heartbeat_age_ms={d} timeout_ms={d} control_buffer={d} messages_buffer={d} name={s}\n",
        .{
            service.header.node_id,
            service.header.service_id,
            status,
            service.header.pid,
            ageMillis(now_ms, heartbeat),
            timeout_ms,
            service.header.control_buffer_length,
            service.header.messages_buffer_length,
            parsed.name,
        },
    );
    try printServiceDetails(stdout, &service, parsed.name, now_ms);
}

fn printBrokerDetails(stdout: *std.Io.Writer, broker: *BrokerMetadataFile, now_ms: i64) !void {
    const aeron = metadata_reader.readBrokerAeron(broker.getAeronDiscovery());
    try stdout.print(
        "    aeron_directory={s} ingress_stream={d} admin_stream_base={d} data_stream_base={d}\n",
        .{ aeron.aeron_directory, aeron.broker_ingress_stream_id, aeron.admin_stream_base, aeron.data_stream_base },
    );
    if (aeron.local_data_channel.len > 0 or aeron.local_admin_channel.len > 0) {
        try stdout.print("    data_channel={s} admin_channel={s}\n", .{
            aeron.local_data_channel,
            aeron.local_admin_channel,
        });
    }
    try printAeronCncDetails(
        stdout,
        aeron.aeron_directory,
        aeron.broker_ingress_stream_id,
        aeron.admin_stream_base,
        aeron.data_stream_base,
    );
    try stdout.print(
        "    monitoring_version={d} counter_values_bytes={d} counter_metadata_bytes={d} error_log_bytes={d} tail_offset={d} tail_bytes={d}\n",
        .{
            broker.loadMetadataMonitoringVersion(),
            broker.loadCounterValuesBufferLength(),
            broker.loadCounterMetadataBufferLength(),
            broker.loadErrorLogBufferLength(),
            broker.loadMonitoringTailOffset(),
            broker.loadMonitoringTailLength(),
        },
    );
    try printRing(stdout, "control", broker.getControlBuffer(), @intCast(broker.header.control_buffer_length), now_ms);
    try printCounters(stdout, broker.getCounterValuesBuffer(), broker.getCounterMetadataBuffer());
    try printErrors(stdout, broker.getErrorLogBuffer());
}

fn printServiceDetails(stdout: *std.Io.Writer, service: *ServiceMetadataFile, service_name: []const u8, now_ms: i64) !void {
    _ = service_name;
    const aeron = metadata_reader.readServiceAeron(service.getAeronDiscovery());
    try stdout.print(
        "    aeron_directory={s} broker_ingress_stream={d} broker_started_ms={d}\n",
        .{ aeron.aeron_directory, aeron.broker_ingress_stream_id, aeron.broker_start_timestamp_ms },
    );
    try stdout.print(
        "    monitoring_version={d} counter_values_bytes={d} counter_metadata_bytes={d} error_log_bytes={d} tail_offset={d} tail_bytes={d}\n",
        .{
            service.loadMetadataMonitoringVersion(),
            service.loadCounterValuesBufferLength(),
            service.loadCounterMetadataBufferLength(),
            service.loadErrorLogBufferLength(),
            service.loadMonitoringTailOffset(),
            service.loadMonitoringTailLength(),
        },
    );
    try printRing(stdout, "control", service.getControlBuffer(), @intCast(service.header.control_buffer_length), now_ms);
    try printRing(stdout, "messages", service.getMessagesBuffer(), @intCast(service.header.messages_buffer_length), now_ms);
    try printCounters(stdout, service.getCounterValuesBuffer(), service.getCounterMetadataBuffer());
    try printErrors(stdout, service.getErrorLogBuffer());
}

fn printAeronCncDetails(
    stdout: *std.Io.Writer,
    directory: []const u8,
    broker_ingress_stream_id: i32,
    admin_stream_base: i32,
    data_stream_base: i32,
) !void {
    var cnc = aeron_cnc_reader.CncFile.open(platform.defaultIo(), directory) catch |err| {
        try stdout.print("    aeron_driver_status=unavailable cnc_error={s}\n", .{@errorName(err)});
        return;
    };
    defer cnc.close(platform.defaultIo());

    const metadata = cnc.metadata;
    try stdout.print(
        "    aeron_driver_status={s} pid={d} started_ms={d} cnc_version={d} client_liveness_timeout_ns={d}\n",
        .{
            if (metadata.driverAlive()) "alive" else "dead",
            metadata.pid,
            metadata.start_timestamp_ms,
            metadata.cnc_version,
            metadata.client_liveness_timeout_ns,
        },
    );

    var printed = false;
    var counter_count: usize = 0;
    for (0..cnc.counterCapacity()) |id| {
        const sample = cnc.readCounter(id) orelse continue;
        counter_count += 1;
        if (!printed) {
            try stdout.print("    aeron_counters:\n", .{});
            printed = true;
        }
        try stdout.print("      [{d}] type={d} {s}={d}", .{ sample.id, sample.type_id, sample.label, sample.value });
        if (sample.systemCounter()) |system_counter| {
            try stdout.print(" system_id={d}", .{@intFromEnum(system_counter)});
        }
        if (sample.streamKey()) |stream_key| {
            const kind = aeron_cnc_reader.channelKind(stream_key, broker_ingress_stream_id, admin_stream_base, data_stream_base);
            try stdout.print(
                " channel_kind={s} stream_id={d} session_id={d}",
                .{ kind.label(), stream_key.stream_id, stream_key.session_id },
            );
        }
        try stdout.writeByte('\n');
    }
    if (!printed) {
        try stdout.print("    aeron_counters: (none allocated)\n", .{});
    } else {
        try stdout.print("    aeron_counter_count={d}\n", .{counter_count});
    }
}

fn printRing(stdout: *std.Io.Writer, name: []const u8, buffer: []u8, capacity: usize, now_ms: i64) !void {
    const stats = metadata_reader.deriveRingStats(buffer, capacity);
    try stdout.print(
        "    ring={s} capacity={d} used={d} free={d} usage={d:.3} producer={d} consumer={d} consumer_heartbeat_age_ms={d}\n",
        .{
            name,
            stats.capacity,
            stats.used_bytes,
            stats.free_bytes,
            stats.usageRatio(),
            stats.producer_position,
            stats.consumer_position,
            ageMillis(now_ms, stats.consumer_heartbeat_ms),
        },
    );
}

fn printCounters(
    stdout: *std.Io.Writer,
    values: []align(constants.cache_line_pad) u8,
    metadata: []u8,
) !void {
    const capacity = metadata_reader.counterCapacity(values, metadata);
    var printed = false;
    for (0..capacity) |id| {
        const sample = metadata_reader.readCounter(values, metadata, id) orelse continue;
        if (!printed) {
            try stdout.print("    counters:\n", .{});
            printed = true;
        }
        try stdout.print("      [{d}] type={d} {s}={d}\n", .{ sample.id, sample.type_id, sample.label, sample.value });
    }
    if (!printed) {
        try stdout.print("    counters: (none allocated)\n", .{});
    }
}

fn printErrors(stdout: *std.Io.Writer, error_log: []u8) !void {
    var offset: usize = 0;
    var count: usize = 0;
    while (metadata_reader.readErrorEntry(error_log, offset)) |result| {
        if (count == 0) try stdout.print("    errors:\n", .{});
        try stdout.print(
            "      [{d}x] first={d} last={d} {s}\n",
            .{
                result.entry.observation_count,
                result.entry.first_observation_timestamp,
                result.entry.last_observation_timestamp,
                result.entry.description,
            },
        );
        count += 1;
        offset = result.next_offset;
    }
    if (count == 0) {
        try stdout.print("    errors: (none)\n", .{});
    }
}

fn parseBrokerNodeId(file_name: []const u8) ?i16 {
    if (!BrokerMetadataFile.isBrokerMetadataFile(file_name)) return null;
    const id_text = file_name["broker_".len .. file_name.len - ".dat".len];
    return std.fmt.parseInt(i16, id_text, 10) catch null;
}

fn loadAt(comptime T: type, mapped: []align(std.heap.page_size_min) u8, offset: usize) T {
    const ptr: *const volatile T = @ptrCast(@alignCast(mapped.ptr + offset));
    return @atomicLoad(T, ptr, .acquire);
}

fn ageMillis(now_ms: i64, heartbeat_ms: i64) i64 {
    if (heartbeat_ms <= 0 or now_ms <= heartbeat_ms) return 0;
    return now_ms - heartbeat_ms;
}

fn fatal(stderr: *std.Io.Writer, comptime fmt: []const u8, values: anytype) noreturn {
    stderr.print("error: " ++ fmt ++ "\n", values) catch {};
    printUsage(stderr) catch {};
    stderr.flush() catch {};
    std.process.exit(2);
}
