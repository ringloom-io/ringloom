const std = @import("std");
const Allocator = std.mem.Allocator;

pub fn io() std.Io {
    return std.testing.io;
}

pub fn sleepMs(ms: u64) void {
    std.Io.sleep(io(), .fromMilliseconds(@intCast(ms)), .awake) catch unreachable;
}

pub fn createDirPath(path: []const u8) !void {
    try std.Io.Dir.cwd().createDirPath(io(), path);
}

pub fn deleteTree(path: []const u8) !void {
    try std.Io.Dir.cwd().deleteTree(io(), path);
}

pub fn deleteFile(path: []const u8) !void {
    if (std.fs.path.isAbsolute(path)) {
        try std.Io.Dir.deleteFileAbsolute(io(), path);
    } else {
        try std.Io.Dir.cwd().deleteFile(io(), path);
    }
}

pub fn access(path: []const u8) !void {
    if (std.fs.path.isAbsolute(path)) {
        try std.Io.Dir.accessAbsolute(io(), path, .{});
    } else {
        try std.Io.Dir.cwd().access(io(), path, .{});
    }
}

pub fn openDir(path: []const u8, options: std.Io.Dir.OpenOptions) !std.Io.Dir {
    if (std.fs.path.isAbsolute(path)) {
        return std.Io.Dir.openDirAbsolute(io(), path, options);
    }
    return std.Io.Dir.cwd().openDir(io(), path, options);
}

pub fn openFile(path: []const u8, options: std.Io.Dir.OpenFileOptions) !std.Io.File {
    if (std.fs.path.isAbsolute(path)) {
        return std.Io.Dir.openFileAbsolute(io(), path, options);
    }
    return std.Io.Dir.cwd().openFile(io(), path, options);
}

pub fn createFile(path: []const u8, flags: std.Io.Dir.CreateFileOptions) !std.Io.File {
    if (std.fs.path.isAbsolute(path)) {
        return std.Io.Dir.createFileAbsolute(io(), path, flags);
    }
    return std.Io.Dir.cwd().createFile(io(), path, flags);
}

pub fn writeFile(path: []const u8, data: []const u8) !void {
    var file = try createFile(path, .{ .truncate = true });
    defer file.close(io());
    try file.writeStreamingAll(io(), data);
}

pub fn readFileAlloc(allocator: Allocator, path: []const u8, max_bytes: usize) ![]u8 {
    return std.Io.Dir.cwd().readFileAlloc(io(), path, allocator, .limited(max_bytes));
}
