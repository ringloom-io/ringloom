//! macOS/BSD kqueue I/O engine backend.
//!
//! Wraps the kqueue/kevent interface for async TCP I/O.
//! Only compiled on macOS/FreeBSD targets.

const std = @import("std");
const posix = std.posix;
const io_engine = @import("io_engine.zig");
const net = @import("net_compat.zig");
const socket_config_mod = @import("socket_config.zig");

const ConnectionHandle = io_engine.ConnectionHandle;
const Completion = io_engine.Completion;

pub const KqueueEngine = struct {
    kq_fd: posix.fd_t,
    allocator: std.mem.Allocator,

    // Pending operation tracking — kqueue is edge-triggered, so
    // we must track what operations are outstanding per handle.
    pending_ops: []PendingOps,

    const PendingOps = struct {
        recv_buf: ?[]u8 = null,
        send_buf: ?[]const u8 = null,
        accept_pending: bool = false,
        connect_pending: bool = false,
        close_pending: bool = false,
    };

    pub fn init(allocator: std.mem.Allocator, max_connections: u8) !KqueueEngine {
        const kq_fd = try posix.kqueue();
        errdefer posix.close(kq_fd);

        const pending = try allocator.alloc(PendingOps, max_connections);
        for (pending) |*p| p.* = .{};

        return .{
            .kq_fd = kq_fd,
            .allocator = allocator,
            .pending_ops = pending,
        };
    }

    pub fn deinit(self: *KqueueEngine) void {
        posix.close(self.kq_fd);
        self.allocator.free(self.pending_ops);
    }

    pub fn submit_accept(self: *KqueueEngine, listen_handle: ConnectionHandle) void {
        const idx = listen_handle.toIndex();
        if (idx >= self.pending_ops.len) return;
        self.pending_ops[idx].accept_pending = true;

        // Register for read events on the listen socket.
        var changelist = [_]posix.Kevent{.{
            .ident = @intCast(idx),
            .filter = posix.system.EVFILT.READ,
            .flags = posix.system.EV.ADD | posix.system.EV.ENABLE,
            .fflags = 0,
            .data = 0,
            .udata = @intFromEnum(listen_handle),
        }};
        _ = posix.kevent(self.kq_fd, &changelist, &.{}, null) catch {};
    }

    pub fn submit_connect(self: *KqueueEngine, handle: ConnectionHandle, _: net.Address) void {
        const idx = handle.toIndex();
        if (idx >= self.pending_ops.len) return;
        self.pending_ops[idx].connect_pending = true;
    }

    pub fn submit_recv(self: *KqueueEngine, handle: ConnectionHandle, buf: []u8) void {
        const idx = handle.toIndex();
        if (idx >= self.pending_ops.len) return;
        self.pending_ops[idx].recv_buf = buf;
    }

    pub fn submit_send(self: *KqueueEngine, handle: ConnectionHandle, data: []const u8) void {
        const idx = handle.toIndex();
        if (idx >= self.pending_ops.len) return;
        self.pending_ops[idx].send_buf = data;
    }

    pub fn submit_close(self: *KqueueEngine, handle: ConnectionHandle) void {
        const idx = handle.toIndex();
        if (idx >= self.pending_ops.len) return;
        self.pending_ops[idx].close_pending = true;
    }

    pub fn harvest(self: *KqueueEngine, completions: []Completion) u32 {
        _ = self;
        _ = completions;
        // Full kqueue polling implementation would go here. For now,
        // this is a placeholder — the actual broker uses io_uring on Linux.
        return 0;
    }
};

test "KqueueEngine satisfies IoEngine interface" {
    io_engine.assertValidEngine(KqueueEngine);
}
