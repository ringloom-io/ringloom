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
        broker.header.messages_buffer_length,
        broker.loadFcBufferLength(),
        broker.loadPeerSendCountersLength(),
        now_ms,
    );
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
        header.messages_buffer_length,
        loadAt(i32, mapped, constants.fc_buffer_length_offset),
        loadAt(i32, mapped, constants.peer_send_counters_length_offset),
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
    send_buffer_length: i32,
    fc_buffer_length: i32,
    peer_send_counters_length: i32,
    now_ms: i64,
) !void {
    const alive = platform.isProcessAlive(@intCast(pid));
    const age_ms = ageMillis(now_ms, heartbeat_ms);
    try stdout.print(
        "  node={d} status={s} pid={d} heartbeat_age_ms={d} service_id={d} next_service_id={d}\n" ++
            "    started_ms={d} control_buffer={d} send_buffer={d} flow_control_bytes={d} peer_send_counter_bytes={d}\n",
        .{
            node_id,
            if (alive) "alive" else "dead",
            pid,
            age_ms,
            service_id,
            next_service_id,
            start_timestamp_ms,
            control_buffer_length,
            send_buffer_length,
            fc_buffer_length,
            peer_send_counters_length,
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
