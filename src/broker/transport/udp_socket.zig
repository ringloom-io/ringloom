//! UDP socket management.
//!
//! Encapsulates platform-specific socket creation, configuration, and lifecycle.
//! All sockets are non-blocking.

const std = @import("std");
const posix = std.posix;
const builtin = @import("builtin");
const constants = @import("brz_common").platform.constants;

/// A non-blocking UDP socket.
pub const UdpSocket = struct {
    fd: posix.socket_t,

    const Self = @This();

    /// Create and bind a UDP socket to the given address.
    pub fn bind(address: std.net.Address) !UdpSocket {
        const fd = try posix.socket(
            address.any.family,
            posix.SOCK.DGRAM | posix.SOCK.NONBLOCK | posix.SOCK.CLOEXEC,
            0,
        );
        errdefer posix.close(fd);

        // Allow address reuse (for fast restart)
        try posix.setsockopt(fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, &std.mem.toBytes(@as(c_int, 1)));

        try posix.bind(fd, &address.any, address.getOsSockLen());

        return .{ .fd = fd };
    }

    /// Create an unbound UDP socket (for sending only).
    pub fn create(family: u32) !UdpSocket {
        const fd = try posix.socket(
            @intCast(family),
            posix.SOCK.DGRAM | posix.SOCK.NONBLOCK | posix.SOCK.CLOEXEC,
            0,
        );

        return .{ .fd = fd };
    }

    /// Set the OS-level send buffer size.
    pub fn setSendBufferSize(self: Self, size: u32) !void {
        try posix.setsockopt(
            self.fd,
            posix.SOL.SOCKET,
            posix.SO.SNDBUF,
            &std.mem.toBytes(@as(c_int, @intCast(size))),
        );
    }

    /// Set the OS-level receive buffer size.
    pub fn setRecvBufferSize(self: Self, size: u32) !void {
        try posix.setsockopt(
            self.fd,
            posix.SOL.SOCKET,
            posix.SO.RCVBUF,
            &std.mem.toBytes(@as(c_int, @intCast(size))),
        );
    }

    /// Get the file descriptor (for io_uring / kqueue / IOCP registration).
    pub fn getFd(self: Self) posix.socket_t {
        return self.fd;
    }

    /// Close the socket.
    pub fn close(self: Self) void {
        posix.close(self.fd);
    }
};

/// Broker socket pair: one receive socket (bound) + one send socket (unbound).
///
/// The broker uses one receive socket bound to its own address and one send
/// socket shared for all outbound traffic. The receiver identifies packets by
/// `source_node_id` in the frame header (not by source IP/port).
pub const BrokerSockets = struct {
    /// Receive socket — bound to broker's configured address.
    recv_socket: UdpSocket,

    /// Send socket — unbound, used for all outbound traffic.
    send_socket: UdpSocket,

    pub fn init(bind_address: std.net.Address, send_buf_size: u32, recv_buf_size: u32) !BrokerSockets {
        var recv_socket = try UdpSocket.bind(bind_address);
        errdefer recv_socket.close();

        try recv_socket.setRecvBufferSize(recv_buf_size);

        var send_socket = try UdpSocket.create(bind_address.any.family);
        errdefer send_socket.close();

        try send_socket.setSendBufferSize(send_buf_size);

        return .{
            .recv_socket = recv_socket,
            .send_socket = send_socket,
        };
    }

    pub fn deinit(self: *BrokerSockets) void {
        self.recv_socket.close();
        self.send_socket.close();
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

test "UdpSocket bind and close" {
    // Given: bind to loopback on any port
    const addr = try std.net.Address.parseIp4("127.0.0.1", 0);

    // When
    const sock = try UdpSocket.bind(addr);
    defer sock.close();

    // Then: fd is valid (positive)
    try std.testing.expect(sock.getFd() >= 0);
}

test "UdpSocket create unbound" {
    // Given / When
    const sock = try UdpSocket.create(posix.AF.INET);
    defer sock.close();

    // Then
    try std.testing.expect(sock.getFd() >= 0);
}

test "UdpSocket set buffer sizes" {
    // Given
    const sock = try UdpSocket.create(posix.AF.INET);
    defer sock.close();

    // When / Then: should not error
    try sock.setSendBufferSize(256 * 1024);
    try sock.setRecvBufferSize(256 * 1024);
}

test "BrokerSockets init and deinit" {
    // Given
    const addr = try std.net.Address.parseIp4("127.0.0.1", 0);

    // When
    var sockets = try BrokerSockets.init(addr, 256 * 1024, 256 * 1024);
    defer sockets.deinit();

    // Then
    try std.testing.expect(sockets.recv_socket.getFd() >= 0);
    try std.testing.expect(sockets.send_socket.getFd() >= 0);
    try std.testing.expect(sockets.recv_socket.getFd() != sockets.send_socket.getFd());
}

test "UdpSocket loopback send and receive" {
    // Given: bind a receiver
    const addr = try std.net.Address.parseIp4("127.0.0.1", 0);
    const recv_sock = try UdpSocket.bind(addr);
    defer recv_sock.close();

    // Get the actual bound address/port
    var bound_addr: posix.sockaddr = undefined;
    var addr_len: posix.socklen_t = @sizeOf(posix.sockaddr);
    try posix.getsockname(recv_sock.fd, &bound_addr, &addr_len);

    // Create a sender
    const send_sock = try UdpSocket.create(posix.AF.INET);
    defer send_sock.close();

    // When: send a packet
    const payload = "test-udp-packet";
    var iov = [_]posix.iovec_const{.{
        .base = payload.ptr,
        .len = payload.len,
    }};

    const msg = posix.msghdr_const{
        .name = &bound_addr,
        .namelen = addr_len,
        .iov = &iov,
        .iovlen = 1,
        .control = null,
        .controllen = 0,
        .flags = 0,
    };

    const sent = try posix.sendmsg(send_sock.fd, &msg, 0);
    try std.testing.expectEqual(payload.len, sent);

    // Then: receive the packet using recvfrom
    var recv_buf: [256]u8 = undefined;
    var received: usize = 0;

    // Retry a few times for non-blocking socket
    for (0..100) |_| {
        const result = posix.recvfrom(recv_sock.fd, &recv_buf, 0, null, null);
        if (result) |n| {
            received = n;
            break;
        } else |_| {
            std.Thread.sleep(1 * std.time.ns_per_ms);
        }
    }

    try std.testing.expectEqual(payload.len, received);
    try std.testing.expectEqualStrings(payload, recv_buf[0..received]);
}
