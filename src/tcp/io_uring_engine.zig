//! Linux io_uring I/O engine backend.
//!
//! Wraps the kernel's io_uring interface for asynchronous TCP I/O.
//! Submissions go into the SQ, completions are harvested from the CQ.
//! Only compiled on Linux targets.

const std = @import("std");
const posix = std.posix;
const linux = std.os.linux;
const io_engine = @import("io_engine.zig");
const socket_config_mod = @import("socket_config.zig");

const ConnectionHandle = io_engine.ConnectionHandle;
const Completion = io_engine.Completion;
const encodeUserData = io_engine.encodeUserData;
const decodeUserData = io_engine.decodeUserData;

pub const IoUringEngine = struct {
    ring: linux.IoUring,
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator, max_connections: u8) !IoUringEngine {
        // Entries must be power of 2, minimum 32 for adequate I/O depth.
        const entries: u13 = @max(32, std.math.ceilPowerOfTwo(u13, @as(u13, max_connections) * 4) catch 256);
        const ring = try linux.IoUring.init(entries, .{});
        return .{
            .ring = ring,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *IoUringEngine) void {
        self.ring.deinit();
    }

    pub fn submit_accept(self: *IoUringEngine, listen_handle: ConnectionHandle) void {
        const user_data = encodeUserData(listen_handle, .accept);
        _ = self.ring.accept(user_data, listen_handle.toFd(), null, null, 0) catch return;
    }

    pub fn submit_connect(self: *IoUringEngine, handle: ConnectionHandle, addr: std.net.Address) void {
        const user_data = encodeUserData(handle, .connect);
        _ = self.ring.connect(user_data, handle.toFd(), addr, @sizeOf(@TypeOf(addr))) catch return;
    }

    pub fn submit_recv(self: *IoUringEngine, handle: ConnectionHandle, buf: []u8) void {
        const user_data = encodeUserData(handle, .recv);
        _ = self.ring.recv(user_data, handle.toFd(), .{ .buffer = buf }, 0) catch return;
    }

    pub fn submit_send(self: *IoUringEngine, handle: ConnectionHandle, data: []const u8) void {
        const user_data = encodeUserData(handle, .send);
        _ = self.ring.send(user_data, handle.toFd(), .{ .buffer = data }, 0) catch return;
    }

    pub fn submit_close(self: *IoUringEngine, handle: ConnectionHandle) void {
        const user_data = encodeUserData(handle, .close);
        _ = self.ring.close(user_data, handle.toFd()) catch return;
    }

    pub fn harvest(self: *IoUringEngine, completions: []Completion) u32 {
        _ = self.ring.submit() catch return 0;

        var cqes: [64]linux.io_uring_cqe = undefined;
        const count = self.ring.copy_cqes(&cqes, 0);

        var out: u32 = 0;
        for (cqes[0..count]) |cqe| {
            if (out >= completions.len) break;

            const decoded = decodeUserData(cqe.user_data);
            completions[out] = .{
                .handle = decoded.handle,
                .op = decoded.op,
                .result = if (cqe.res > 0)
                    .{ .ok = .{ .bytes = @intCast(cqe.res) } }
                else if (cqe.res == 0 and decoded.op == .recv)
                    .eof
                else if (cqe.res == 0)
                    .{ .ok = .{ .bytes = 0 } }
                else
                    .{ .err = @enumFromInt(@as(u16, @intCast(-cqe.res))) },
            };
            out += 1;
        }
        return out;
    }
};

// Note: Tests for io_uring require Linux and root or CAP_SYS_ADMIN.
// Unit tests are covered via the mock engine and integration tests.

test "IoUringEngine satisfies IoEngine interface" {
    io_engine.assertValidEngine(IoUringEngine);
}
