//! TCP socket helpers used by the broker's low-level networking paths.
//!
//! Zig 0.16 removed the old `std.net`/`std.posix` convenience layer for these
//! operations. The broker owns non-blocking descriptors directly so it can
//! integrate them with io_uring/kqueue, so this module keeps the raw syscall
//! boundary in one place while using `std.Io.net` for address parsing.

const std = @import("std");
const posix = std.posix;
const linux = std.os.linux;

pub const Address = extern union {
    any: posix.sockaddr,
    in: posix.sockaddr.in,
    in6: posix.sockaddr.in6,

    pub fn initIp4(bytes: [4]u8, port: u16) Address {
        return .{
            .in = .{
                .port = std.mem.nativeToBig(u16, port),
                .addr = @bitCast(bytes),
            },
        };
    }

    pub fn parseIp4(text: []const u8, port: u16) !Address {
        const ip4 = try std.Io.net.Ip4Address.parse(text, port);
        return initIp4(ip4.bytes, ip4.port);
    }

    pub fn getPort(a: Address) u16 {
        return switch (a.any.family) {
            posix.AF.INET => std.mem.bigToNative(u16, a.in.port),
            posix.AF.INET6 => std.mem.bigToNative(u16, a.in6.port),
            else => 0,
        };
    }

    pub fn getOsSockLen(a: Address) posix.socklen_t {
        return switch (a.any.family) {
            posix.AF.INET => @sizeOf(posix.sockaddr.in),
            posix.AF.INET6 => @sizeOf(posix.sockaddr.in6),
            else => @sizeOf(posix.sockaddr),
        };
    }
};

pub const SocketError = error{
    WouldBlock,
    PermissionDenied,
    AddressFamilyNotSupported,
    ProcessFdQuotaExceeded,
    SystemFdQuotaExceeded,
    SystemResources,
    ProtocolNotSupported,
    AddressInUse,
    ConnectionRefused,
    ConnectionResetByPeer,
    BrokenPipe,
    NetworkUnreachable,
    NotConnected,
    ConnectionAborted,
} || posix.UnexpectedError;

pub fn socket(domain: posix.sa_family_t, socket_type: u32, protocol: u32) SocketError!posix.fd_t {
    const rc = linux.socket(@intCast(domain), socket_type, protocol);
    switch (posix.errno(rc)) {
        .SUCCESS => return @intCast(rc),
        .ACCES => return error.PermissionDenied,
        .AFNOSUPPORT => return error.AddressFamilyNotSupported,
        .MFILE => return error.ProcessFdQuotaExceeded,
        .NFILE => return error.SystemFdQuotaExceeded,
        .NOBUFS, .NOMEM => return error.SystemResources,
        .PROTONOSUPPORT => return error.ProtocolNotSupported,
        else => |err| return posix.unexpectedErrno(err),
    }
}

pub fn bind(fd: posix.fd_t, addr: *const posix.sockaddr, addr_len: posix.socklen_t) SocketError!void {
    const rc = linux.bind(fd, addr, addr_len);
    switch (posix.errno(rc)) {
        .SUCCESS => return,
        .ACCES => return error.PermissionDenied,
        .ADDRINUSE => return error.AddressInUse,
        .AFNOSUPPORT => return error.AddressFamilyNotSupported,
        .NOBUFS, .NOMEM => return error.SystemResources,
        else => |err| return posix.unexpectedErrno(err),
    }
}

pub fn listen(fd: posix.fd_t, backlog: u32) SocketError!void {
    const rc = linux.listen(fd, backlog);
    switch (posix.errno(rc)) {
        .SUCCESS => return,
        .ADDRINUSE => return error.AddressInUse,
        .NOBUFS, .NOMEM => return error.SystemResources,
        else => |err| return posix.unexpectedErrno(err),
    }
}

pub fn accept(fd: posix.fd_t, addr: ?*posix.sockaddr, addr_len: ?*posix.socklen_t, flags: u32) SocketError!posix.fd_t {
    const rc = linux.accept4(fd, addr, addr_len, flags);
    switch (posix.errno(rc)) {
        .SUCCESS => return @intCast(rc),
        .AGAIN => return error.WouldBlock,
        .CONNABORTED => return error.ConnectionAborted,
        .NOBUFS, .NOMEM => return error.SystemResources,
        else => |err| return posix.unexpectedErrno(err),
    }
}

pub fn connect(fd: posix.fd_t, addr: *const posix.sockaddr, addr_len: posix.socklen_t) SocketError!void {
    const rc = linux.connect(fd, @ptrCast(addr), addr_len);
    switch (posix.errno(rc)) {
        .SUCCESS, .ISCONN => return,
        .AGAIN, .ALREADY, .INPROGRESS => return error.WouldBlock,
        .ACCES => return error.PermissionDenied,
        .CONNREFUSED => return error.ConnectionRefused,
        .NETUNREACH => return error.NetworkUnreachable,
        .AFNOSUPPORT => return error.AddressFamilyNotSupported,
        .NOBUFS, .NOMEM => return error.SystemResources,
        else => |err| return posix.unexpectedErrno(err),
    }
}

pub fn write(fd: posix.fd_t, buf: []const u8) SocketError!usize {
    const rc = linux.write(fd, buf.ptr, buf.len);
    switch (posix.errno(rc)) {
        .SUCCESS => return @intCast(rc),
        .AGAIN => return error.WouldBlock,
        .CONNRESET => return error.ConnectionResetByPeer,
        .PIPE => return error.BrokenPipe,
        .NOTCONN => return error.NotConnected,
        else => |err| return posix.unexpectedErrno(err),
    }
}

pub fn writev(fd: posix.fd_t, iov: []const posix.iovec_const) SocketError!usize {
    const rc = linux.writev(fd, iov.ptr, iov.len);
    switch (posix.errno(rc)) {
        .SUCCESS => return @intCast(rc),
        .AGAIN => return error.WouldBlock,
        .CONNRESET => return error.ConnectionResetByPeer,
        .PIPE => return error.BrokenPipe,
        .NOTCONN => return error.NotConnected,
        else => |err| return posix.unexpectedErrno(err),
    }
}

pub fn getSocketError(fd: posix.fd_t) SocketError!i32 {
    var value: i32 = 0;
    var value_len: posix.socklen_t = @sizeOf(i32);
    const rc = linux.getsockopt(fd, posix.SOL.SOCKET, posix.SO.ERROR, std.mem.asBytes(&value).ptr, &value_len);
    switch (posix.errno(rc)) {
        .SUCCESS => return value,
        .NOBUFS, .NOMEM => return error.SystemResources,
        else => |err| return posix.unexpectedErrno(err),
    }
}
