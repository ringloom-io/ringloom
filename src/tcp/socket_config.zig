//! TCP socket configuration and tuning.
//!
//! Applied to every TCP socket before it enters the connected state.
//! Controls kernel buffer sizes, keepalive, and Nagle algorithm.

const std = @import("std");
const posix = std.posix;

pub const SocketConfig = struct {
    /// SO_SNDBUF — kernel send buffer size.
    send_buffer_size: u32 = 262_144,
    /// SO_RCVBUF — kernel receive buffer size.
    recv_buffer_size: u32 = 262_144,
    /// TCP_NODELAY — disable Nagle algorithm for low latency.
    tcp_nodelay: bool = true,
    /// SO_KEEPALIVE — enable TCP keepalive probes.
    keepalive: bool = true,
    /// SO_REUSEADDR — allow rapid rebinding after restart.
    reuse_addr: bool = true,

    pub fn apply(self: SocketConfig, fd: posix.socket_t) !void {
        if (self.reuse_addr) {
            try posix.setsockopt(fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));
        }
        try posix.setsockopt(fd, posix.SOL.SOCKET, posix.SO.SNDBUF, &std.mem.toBytes(@as(c_int, @intCast(self.send_buffer_size))));
        try posix.setsockopt(fd, posix.SOL.SOCKET, posix.SO.RCVBUF, &std.mem.toBytes(@as(c_int, @intCast(self.recv_buffer_size))));
        if (self.tcp_nodelay) {
            try posix.setsockopt(fd, posix.IPPROTO.TCP, std.posix.TCP.NODELAY, &std.mem.toBytes(@as(c_int, 1)));
        }
        if (self.keepalive) {
            try posix.setsockopt(fd, posix.SOL.SOCKET, posix.SO.KEEPALIVE, &std.mem.toBytes(@as(c_int, 1)));
        }
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

test "SocketConfig defaults" {
    const cfg = SocketConfig{};
    try std.testing.expectEqual(@as(u32, 262_144), cfg.send_buffer_size);
    try std.testing.expectEqual(@as(u32, 262_144), cfg.recv_buffer_size);
    try std.testing.expect(cfg.tcp_nodelay);
    try std.testing.expect(cfg.keepalive);
    try std.testing.expect(cfg.reuse_addr);
}
