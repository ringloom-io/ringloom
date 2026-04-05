//! kqueue I/O backend for macOS.
//!
//! macOS does not have io_uring. This backend uses traditional kqueue +
//! sendmsg/recvmsg, but wraps them so the transport layer can use a
//! unified interface via NetworkIo.

const std = @import("std");
const posix = std.posix;
const constants = @import("../platform/constants.zig");

/// macOS kqueue-based network I/O backend.
///
/// Unlike io_uring, kqueue only tells us "the socket is readable/writable" —
/// we must then call recvmsg()/sendmsg() ourselves.
pub const KqueueNetworkIo = struct {
    kq_fd: posix.fd_t,
    events: [64]KEvent = undefined,

    const Self = @This();

    // Use the system's kevent type
    const KEvent = std.posix.Kevent;

    pub fn init() !Self {
        const kq_fd = try posix.kqueue();
        return .{ .kq_fd = kq_fd };
    }

    pub fn deinit(self: *Self) void {
        posix.close(self.kq_fd);
    }

    /// Register a socket for read events.
    pub fn registerRead(self: *Self, socket_fd: posix.fd_t) !void {
        var changelist = [_]KEvent{.{
            .ident = @intCast(socket_fd),
            .filter = std.posix.system.EVFILT.READ,
            .flags = std.posix.system.EV.ADD | std.posix.system.EV.ENABLE,
            .fflags = 0,
            .data = 0,
            .udata = 0,
        }};

        _ = try posix.kevent(self.kq_fd, &changelist, &.{}, null);
    }

    /// Poll for ready events. Returns the slice of ready events.
    ///
    /// Unlike io_uring, kqueue only tells us "the socket is readable" —
    /// we must then call recvmsg() ourselves.
    pub fn poll(self: *Self, timeout_ns: ?u64) ![]KEvent {
        var ts_val: posix.timespec = undefined;
        const ts_ptr: ?*const posix.timespec = if (timeout_ns) |ns| blk: {
            ts_val = .{
                .sec = @intCast(ns / std.time.ns_per_s),
                .nsec = @intCast(ns % std.time.ns_per_s),
            };
            break :blk &ts_val;
        } else null;

        const n = try posix.kevent(
            self.kq_fd,
            &.{},
            &self.events,
            ts_ptr,
        );

        return self.events[0..n];
    }

    /// Synchronous UDP send — kqueue does not batch sends like io_uring.
    /// Wraps sendmsg() directly.
    pub fn sendTo(
        socket_fd: posix.fd_t,
        buf: []const u8,
        dest_addr: *const posix.sockaddr,
        dest_addr_len: posix.socklen_t,
    ) !usize {
        var iov = [_]posix.iovec_const{.{
            .base = buf.ptr,
            .len = buf.len,
        }};

        const msg: posix.msghdr_const = .{
            .name = dest_addr,
            .namelen = dest_addr_len,
            .iov = &iov,
            .iovlen = 1,
            .control = null,
            .controllen = 0,
            .flags = 0,
        };

        return try posix.sendmsg(socket_fd, &msg, 0);
    }

    /// Synchronous UDP receive.
    pub fn recvFrom(
        socket_fd: posix.fd_t,
        buf: []u8,
        src_addr: *posix.sockaddr,
        src_addr_len: *posix.socklen_t,
    ) !usize {
        var iov = [_]posix.iovec{.{
            .base = buf.ptr,
            .len = buf.len,
        }};

        var msg: posix.msghdr = .{
            .name = src_addr,
            .namelen = src_addr_len.*,
            .iov = &iov,
            .iovlen = 1,
            .control = null,
            .controllen = 0,
            .flags = 0,
        };

        const n = try posix.recvmsg(socket_fd, &msg, 0);
        src_addr_len.* = msg.namelen;
        return n;
    }
};
