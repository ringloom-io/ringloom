const std = @import("std");
const builtin = @import("builtin");
const ringloom_broker = @import("ringloom_broker");

const BrokerApplicationFactory = ringloom_broker.BrokerApplicationFactory;
const ExitCode = enum(u8) {
    success = 0,
    usage_error = 1,
    config_error = 2,
    startup_error = 3,
    runtime_error = 4,
};

pub fn main(init: std.process.Init) !void {
    const io = init.io;
    var debug_alloc: std.heap.DebugAllocator(.{}) = .init;
    const allocator = switch (builtin.mode) {
        .Debug, .ReleaseSafe => debug_alloc.allocator(),
        .ReleaseFast, .ReleaseSmall => std.heap.smp_allocator,
    };
    defer if (builtin.mode == .Debug or builtin.mode == .ReleaseSafe) {
        _ = debug_alloc.deinit();
    };

    const args = try init.minimal.args.toSlice(init.arena.allocator());

    if (hasFlag(args, "--help") or hasFlag(args, "-h")) {
        try printHelp(io);
        return;
    }

    if (hasFlag(args, "--version") or hasFlag(args, "-v")) {
        try printVersion(io);
        return;
    }

    const config_path = parseConfigPath(args) catch {
        try printHelp(io);
        std.process.exit(@intFromEnum(ExitCode.usage_error));
    };

    var factory = BrokerApplicationFactory.init(allocator, io, init.environ_map);
    var bootstrap = factory.create(config_path) catch |err| {
        std.log.err("failed to load broker configuration: {}", .{err});
        std.process.exit(@intFromEnum(ExitCode.config_error));
    };
    defer bootstrap.deinit();

    var app = ringloom_broker.BrokerApplication.init(allocator, bootstrap.config, io) catch |err| {
        std.log.err("failed to initialize broker application: {}", .{err});
        std.process.exit(@intFromEnum(ExitCode.startup_error));
    };
    defer app.deinit();

    const exit_code = app.run() catch |err| {
        std.log.err("broker runtime failed: {}", .{err});
        std.process.exit(@intFromEnum(ExitCode.runtime_error));
    };

    std.process.exit(exit_code);
}

fn hasFlag(args: []const [:0]const u8, flag: []const u8) bool {
    for (args[1..]) |arg| {
        if (std.mem.eql(u8, arg, flag)) {
            return true;
        }
    }
    return false;
}

fn parseConfigPath(args: []const [:0]const u8) !?[]const u8 {
    var i: usize = 1;
    while (i < args.len) : (i += 1) {
        if (std.mem.eql(u8, args[i], "--config")) {
            if (i + 1 >= args.len) {
                return error.MissingConfigPath;
            }
            return args[i + 1];
        }
    }
    return null;
}

fn printHelp(io: std.Io) !void {
    var buf: [4096]u8 = undefined;
    var stdout_w = std.Io.File.stdout().writer(io, &buf);
    const stdout = &stdout_w.interface;
    try stdout.writeAll(
        \\RingLoom Broker — high-performance IPC framework
        \\
        \\Usage:
        \\  ringloom-broker [--config <path>] [--help] [--version]
        \\
        \\Options:
        \\  --config <path>   Path to broker properties file
        \\  --help, -h        Show this help message
        \\  --version, -v     Show version information
        \\
    );
    try stdout.flush();
}

fn printVersion(io: std.Io) !void {
    var buf: [4096]u8 = undefined;
    var stdout_w = std.Io.File.stdout().writer(io, &buf);
    const stdout = &stdout_w.interface;
    try stdout.writeAll("RingLoom Broker v0.0.0\n");
    try stdout.flush();
}

test "main module compiles" {
    _ = @import("ringloom_broker");
}
