const std = @import("std");
const builtin = @import("builtin");
const brz_broker = @import("brz_broker");

const BrokerApplicationFactory = brz_broker.BrokerApplicationFactory;
const ExitCode = enum(u8) {
    success = 0,
    usage_error = 1,
    config_error = 2,
    startup_error = 3,
    runtime_error = 4,
};

pub fn main() !void {
    var debug_alloc: std.heap.DebugAllocator(.{}) = .init;
    const allocator = switch (builtin.mode) {
        .Debug, .ReleaseSafe => debug_alloc.allocator(),
        .ReleaseFast, .ReleaseSmall => std.heap.smp_allocator,
    };
    defer if (builtin.mode == .Debug or builtin.mode == .ReleaseSafe) {
        _ = debug_alloc.deinit();
    };

    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    if (hasFlag(args, "--help") or hasFlag(args, "-h")) {
        try printHelp();
        return;
    }

    if (hasFlag(args, "--version") or hasFlag(args, "-v")) {
        try printVersion();
        return;
    }

    const config_path = parseConfigPath(args) catch {
        try printHelp();
        std.process.exit(@intFromEnum(ExitCode.usage_error));
    };

    var factory = BrokerApplicationFactory.init(allocator);
    var bootstrap = factory.create(config_path) catch |err| {
        std.log.err("failed to load broker configuration: {}", .{err});
        std.process.exit(@intFromEnum(ExitCode.config_error));
    };
    defer bootstrap.deinit();

    var app = brz_broker.BrokerApplication.init(allocator, bootstrap.config) catch |err| {
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

fn hasFlag(args: []const []const u8, flag: []const u8) bool {
    for (args[1..]) |arg| {
        if (std.mem.eql(u8, arg, flag)) {
            return true;
        }
    }
    return false;
}

fn parseConfigPath(args: []const []const u8) !?[]const u8 {
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

fn printHelp() !void {
    var buf: [4096]u8 = undefined;
    var stdout_w = std.fs.File.stdout().writer(&buf);
    const stdout = &stdout_w.interface;
    try stdout.writeAll(
        \\BRZ Broker — high-performance IPC framework
        \\
        \\Usage:
        \\  brz-broker [--config <path>] [--help] [--version]
        \\
        \\Options:
        \\  --config <path>   Path to broker properties file
        \\  --help, -h        Show this help message
        \\  --version, -v     Show version information
        \\
    );
    try stdout.flush();
}

fn printVersion() !void {
    var buf: [4096]u8 = undefined;
    var stdout_w = std.fs.File.stdout().writer(&buf);
    const stdout = &stdout_w.interface;
    try stdout.writeAll("BRZ Broker v0.0.0\n");
    try stdout.flush();
}

test "main module compiles" {
    _ = @import("brz_broker");
}
