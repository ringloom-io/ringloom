//! Platform-specific I/O backend — selected at comptime based on target OS.
//!
//! The sender and receiver event loops never reference io_uring, kqueue, or
//! IOCP directly. They use `NetworkIo`, which dispatches to the correct
//! backend at comptime.

const std = @import("std");
const builtin = @import("builtin");
const constants = @import("ringloom_common").platform.constants;

/// Platform-specific I/O backend. Selected at comptime based on target OS.
///
/// All methods have the same signatures regardless of backend, so the sender
/// and receiver event loops are platform-independent.
///
/// - Linux: io_uring (batched async I/O)
/// - macOS: kqueue + sendmsg/recvmsg (synchronous I/O with readiness notification)
/// - Others: compile error
pub const NetworkIo = if (builtin.os.tag == .linux)
    IoUringNetworkIo
else if (builtin.os.tag == .macos)
    KqueueNetworkIo
else
    @compileError("Unsupported OS for NetworkIo: " ++ @tagName(builtin.os.tag));

// ── Linux backend: io_uring ───────────────────────────────────────────

/// Linux backend wrapping io_uring with integrated buffer pools.
pub const IoUringNetworkIo = struct {
    ring: IoUringBackend,
    send_pool: *BufferPool,
    recv_pool: *BufferPool,

    const IoUringBackend = @import("io_uring.zig").IoUring;
    const IoUringConfig = @import("io_uring.zig").IoUringConfig;
    const BufferPool = @import("buffer_pool.zig").BufferPool;

    const Self = @This();

    pub fn init(config: IoUringConfig, send_pool: *BufferPool, recv_pool: *BufferPool) !Self {
        var ring = try IoUringBackend.initWithConfig(config);
        errdefer ring.deinit();

        // Register send buffers with the kernel for zero-copy I/O
        if (comptime builtin.os.tag == .linux) {
            const send_iovecs = try send_pool.toIovecs(send_pool.allocator);
            defer send_pool.allocator.free(send_iovecs);
            try ring.registerBuffers(send_iovecs);
        }

        return .{
            .ring = ring,
            .send_pool = send_pool,
            .recv_pool = recv_pool,
        };
    }

    pub fn deinit(self: *Self) void {
        if (self.ring.registered_buffers) {
            self.ring.unregisterBuffers() catch {};
        }
        self.ring.deinit();
    }

    pub fn prepareSend(
        self: *Self,
        socket_fd: std.posix.fd_t,
        buf: []const u8,
        dest_addr: *const std.posix.sockaddr,
        dest_addr_len: std.posix.socklen_t,
        msghdr_buf: *std.posix.msghdr_const,
        iov: *std.posix.iovec_const,
        user_data: u64,
    ) !void {
        try self.ring.prepareSend(
            socket_fd, buf, dest_addr, dest_addr_len,
            msghdr_buf, iov, user_data,
        );
    }

    pub fn prepareRecv(
        self: *Self,
        socket_fd: std.posix.fd_t,
        buf: []u8,
        src_addr: *std.posix.sockaddr,
        src_addr_len: *std.posix.socklen_t,
        msghdr_buf: *std.posix.msghdr,
        iov: *std.posix.iovec,
        user_data: u64,
    ) !void {
        try self.ring.prepareRecv(
            socket_fd, buf, src_addr, src_addr_len,
            msghdr_buf, iov, user_data,
        );
    }

    pub fn submit(self: *Self) !u32 {
        return self.ring.submit();
    }

    pub fn pollCompletions(
        self: *Self,
        comptime handler: fn (completion: IoUringBackend.Completion) void,
        limit: u32,
    ) u32 {
        return self.ring.pollCompletions(handler, limit);
    }

    pub fn pendingSubmissions(self: *const Self) u32 {
        return self.ring.pending_submissions;
    }
};

/// macOS backend: kqueue + sendmsg/recvmsg.
pub const KqueueNetworkIo = @import("kqueue.zig").KqueueNetworkIo;

// ── Tests ─────────────────────────────────────────────────────────────

test "NetworkIo type is selected at comptime" {
    // Verify that NetworkIo resolves to the correct backend for this platform
    if (builtin.os.tag == .linux) {
        try std.testing.expect(NetworkIo == IoUringNetworkIo);
    } else if (builtin.os.tag == .macos) {
        try std.testing.expect(NetworkIo == KqueueNetworkIo);
    }
}
