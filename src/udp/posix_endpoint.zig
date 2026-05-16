// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const builtin = @import("builtin");
const endpoint = @import("endpoint.zig");
const protocol = @import("protocol.zig");

const posix = std.posix;
const linux = std.os.linux;

pub const PosixEndpoint = if (builtin.os.tag == .linux) LinuxPosixEndpoint else UnsupportedPosixEndpoint;

const EndpointError = error{
    WouldBlock,
    MessageTooLarge,
    PeerUnreachable,
    PermissionDenied,
    AddressInUse,
    AddressFamilyNotSupported,
    SystemResources,
    ScratchTooSmall,
    UnsupportedAddressFamily,
    SocketClosed,
} || posix.UnexpectedError;

const LinuxPosixEndpoint = struct {
    fd_value: posix.fd_t,
    config: endpoint.EndpointConfig,
    bound_address: endpoint.Address,

    pub fn init(config: endpoint.EndpointConfig) !LinuxPosixEndpoint {
        try config.validateForEphemeralPort();
        const socket_fd = try createSocket();
        errdefer closeFd(socket_fd);
        try applySocketConfig(socket_fd, config);
        try bindSocket(socket_fd, config.local_address);
        return .{
            .fd_value = socket_fd,
            .config = config,
            .bound_address = try getLocalAddress(socket_fd),
        };
    }

    pub fn deinit(self: *LinuxPosixEndpoint) void {
        if (self.fd_value >= 0) {
            closeFd(self.fd_value);
            self.fd_value = -1;
        }
    }

    pub fn fd(self: *const LinuxPosixEndpoint) posix.fd_t {
        return self.fd_value;
    }

    pub fn localAddress(self: *const LinuxPosixEndpoint) !endpoint.Address {
        return self.bound_address;
    }

    pub fn poll(
        self: *LinuxPosixEndpoint,
        packets: []endpoint.PacketView,
        scratch: []u8,
    ) EndpointError!usize {
        if (self.fd_value < 0) return error.SocketClosed;
        if (packets.len == 0) return 0;
        const packet_limit = @min(packets.len, endpoint.max_batch_packets);
        const stride = self.config.mtu;
        if (scratch.len < packet_limit * stride) return error.ScratchTooSmall;

        var iovs: [endpoint.max_batch_packets]posix.iovec = undefined;
        var messages: [endpoint.max_batch_packets]linux.mmsghdr = undefined;
        var sources: [endpoint.max_batch_packets]posix.sockaddr.storage = undefined;

        for (0..packet_limit) |i| {
            const start = i * stride;
            iovs[i] = .{
                .base = scratch[start..].ptr,
                .len = stride,
            };
            messages[i] = .{
                .hdr = .{
                    .name = @ptrCast(&sources[i]),
                    .namelen = @sizeOf(posix.sockaddr.storage),
                    .iov = @ptrCast(&iovs[i]),
                    .iovlen = 1,
                    .control = null,
                    .controllen = 0,
                    .flags = 0,
                },
                .len = 0,
            };
        }

        const rc = linux.recvmmsg(self.fd_value, @ptrCast(&messages), @intCast(packet_limit), linux.MSG.DONTWAIT, null);
        switch (posix.errno(rc)) {
            .SUCCESS => {
                const count: usize = @intCast(rc);
                for (0..count) |i| {
                    const start = i * stride;
                    const len: usize = @intCast(messages[i].len);
                    const source = try endpoint.Address.fromSockaddr(@ptrCast(&sources[i]));
                    packets[i] = .{
                        .bytes = scratch[start..][0..len],
                        .source = source,
                        .local = self.bound_address,
                        .length = len,
                    };
                }
                return count;
            },
            .AGAIN => return 0,
            .CONNREFUSED => return error.PeerUnreachable,
            .MSGSIZE => return error.MessageTooLarge,
            else => |err| return posix.unexpectedErrno(err),
        }
    }

    pub fn send(
        self: *LinuxPosixEndpoint,
        packet: []const u8,
        destination: endpoint.Address,
    ) EndpointError!usize {
        if (self.fd_value < 0) return error.SocketClosed;
        if (packet.len > self.config.mtu or packet.len > endpoint.max_udp_payload) {
            return error.MessageTooLarge;
        }

        const sockaddr = destination.toSockaddr();
        const rc = linux.sendto(
            self.fd_value,
            packet.ptr,
            packet.len,
            linux.MSG.DONTWAIT | linux.MSG.NOSIGNAL,
            @ptrCast(&sockaddr),
            @sizeOf(posix.sockaddr.in),
        );
        return mapSendResult(rc);
    }

    pub fn sendBatch(
        self: *LinuxPosixEndpoint,
        batch: []const endpoint.OutboundPacket,
    ) EndpointError!usize {
        if (self.fd_value < 0) return error.SocketClosed;
        if (batch.len == 0) return 0;

        const packet_limit = @min(batch.len, endpoint.max_batch_packets);
        var iovs: [endpoint.max_batch_packets]posix.iovec = undefined;
        var messages: [endpoint.max_batch_packets]linux.mmsghdr = undefined;
        var destinations: [endpoint.max_batch_packets]posix.sockaddr.in = undefined;

        for (batch[0..packet_limit], 0..) |packet, i| {
            if (packet.bytes.len > self.config.mtu or packet.bytes.len > endpoint.max_udp_payload) {
                return error.MessageTooLarge;
            }
            destinations[i] = packet.destination.toSockaddr();
            iovs[i] = .{
                .base = @constCast(packet.bytes.ptr),
                .len = packet.bytes.len,
            };
            messages[i] = .{
                .hdr = .{
                    .name = @ptrCast(&destinations[i]),
                    .namelen = @sizeOf(posix.sockaddr.in),
                    .iov = @ptrCast(&iovs[i]),
                    .iovlen = 1,
                    .control = null,
                    .controllen = 0,
                    .flags = 0,
                },
                .len = 0,
            };
        }

        const rc = linux.sendmmsg(
            self.fd_value,
            @ptrCast(&messages),
            @intCast(packet_limit),
            linux.MSG.DONTWAIT | linux.MSG.NOSIGNAL,
        );
        switch (posix.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .AGAIN => return error.WouldBlock,
            .CONNREFUSED => return error.PeerUnreachable,
            .MSGSIZE => return error.MessageTooLarge,
            .ACCES => return error.PermissionDenied,
            .NOBUFS, .NOMEM => return error.SystemResources,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
};

const UnsupportedPosixEndpoint = struct {
    pub fn init(config: endpoint.EndpointConfig) !UnsupportedPosixEndpoint {
        _ = config;
        return error.UnsupportedOs;
    }
};

fn createSocket() EndpointError!posix.fd_t {
    const rc = linux.socket(linux.AF.INET, linux.SOCK.DGRAM | linux.SOCK.NONBLOCK | linux.SOCK.CLOEXEC, 0);
    switch (posix.errno(rc)) {
        .SUCCESS => return @intCast(rc),
        .ACCES => return error.PermissionDenied,
        .AFNOSUPPORT => return error.AddressFamilyNotSupported,
        .NOBUFS, .NOMEM => return error.SystemResources,
        else => |err| return posix.unexpectedErrno(err),
    }
}

fn bindSocket(fd: posix.fd_t, address: endpoint.Address) EndpointError!void {
    const sockaddr = address.toSockaddr();
    const rc = linux.bind(fd, @ptrCast(&sockaddr), @sizeOf(posix.sockaddr.in));
    switch (posix.errno(rc)) {
        .SUCCESS => return,
        .ACCES => return error.PermissionDenied,
        .ADDRINUSE => return error.AddressInUse,
        .AFNOSUPPORT => return error.AddressFamilyNotSupported,
        .NOBUFS, .NOMEM => return error.SystemResources,
        else => |err| return posix.unexpectedErrno(err),
    }
}

fn getLocalAddress(fd: posix.fd_t) EndpointError!endpoint.Address {
    var storage: posix.sockaddr.storage = undefined;
    var len: posix.socklen_t = @sizeOf(posix.sockaddr.storage);
    const rc = linux.getsockname(fd, @ptrCast(&storage), &len);
    switch (posix.errno(rc)) {
        .SUCCESS => return endpoint.Address.fromSockaddr(@ptrCast(&storage)),
        else => |err| return posix.unexpectedErrno(err),
    }
}

fn applySocketConfig(fd: posix.fd_t, config: endpoint.EndpointConfig) EndpointError!void {
    try setSocketInt(fd, linux.SOL.SOCKET, linux.SO.REUSEADDR, 1);
    try setSocketInt(fd, linux.SOL.SOCKET, linux.SO.SNDBUF, @intCast(config.send_buffer_size));
    try setSocketInt(fd, linux.SOL.SOCKET, linux.SO.RCVBUF, @intCast(config.recv_buffer_size));

    const pmtud_value: i32 = switch (config.pmtud) {
        .dont => linux.IP.PMTUDISC_DONT,
        .want => linux.IP.PMTUDISC_WANT,
        .do => linux.IP.PMTUDISC_DO,
    };
    try setSocketInt(fd, linux.SOL.IP, linux.IP.MTU_DISCOVER, pmtud_value);
}

fn setSocketInt(fd: posix.fd_t, level: i32, optname: u32, value: i32) EndpointError!void {
    const bytes = std.mem.asBytes(&value);
    const rc = linux.setsockopt(fd, level, optname, bytes.ptr, bytes.len);
    switch (posix.errno(rc)) {
        .SUCCESS => return,
        .ACCES => return error.PermissionDenied,
        .NOBUFS, .NOMEM => return error.SystemResources,
        else => |err| return posix.unexpectedErrno(err),
    }
}

fn mapSendResult(rc: usize) EndpointError!usize {
    switch (posix.errno(rc)) {
        .SUCCESS => return @intCast(rc),
        .AGAIN => return error.WouldBlock,
        .CONNREFUSED => return error.PeerUnreachable,
        .MSGSIZE => return error.MessageTooLarge,
        .ACCES => return error.PermissionDenied,
        .NOBUFS, .NOMEM => return error.SystemResources,
        else => |err| return posix.unexpectedErrno(err),
    }
}

fn closeFd(fd: posix.fd_t) void {
    _ = linux.close(fd);
}

fn loopbackConfig(port: u16) endpoint.EndpointConfig {
    return .{ .local_address = .initIp4(.{ 127, 0, 0, 1 }, port) };
}

test "endpoint init/deinit closes file descriptor" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;

    var ep = try PosixEndpoint.init(loopbackConfig(0));
    const fd = ep.fd();
    ep.deinit();

    var value: i32 = 0;
    var len: posix.socklen_t = @sizeOf(i32);
    const rc = linux.getsockopt(fd, linux.SOL.SOCKET, linux.SO.ERROR, std.mem.asBytes(&value).ptr, &len);
    try std.testing.expectEqual(linux.E.BADF, posix.errno(rc));
}

test "WouldBlock maps to no work on idle poll" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;

    var ep = try PosixEndpoint.init(loopbackConfig(0));
    defer ep.deinit();

    var packets: [4]endpoint.PacketView = undefined;
    var scratch: [protocol.default_mtu * 4]u8 = undefined;
    const count = try ep.poll(&packets, &scratch);

    try std.testing.expectEqual(@as(usize, 0), count);
}

test "two endpoints exchange one datagram on loopback" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;

    var sender = try PosixEndpoint.init(loopbackConfig(0));
    defer sender.deinit();
    var receiver = try PosixEndpoint.init(loopbackConfig(0));
    defer receiver.deinit();

    const destination = try receiver.localAddress();
    try std.testing.expectEqual(@as(usize, 5), try sender.send("hello", destination));

    var packets: [1]endpoint.PacketView = undefined;
    var scratch: [protocol.default_mtu]u8 = undefined;
    const count = try receiver.poll(&packets, &scratch);
    try std.testing.expectEqual(@as(usize, 1), count);
    try std.testing.expectEqualStrings("hello", packets[0].bytes);
}

test "batch send receive preserves datagram boundaries" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;

    var sender = try PosixEndpoint.init(loopbackConfig(0));
    defer sender.deinit();
    var receiver = try PosixEndpoint.init(loopbackConfig(0));
    defer receiver.deinit();

    const destination = try receiver.localAddress();
    const batch = [_]endpoint.OutboundPacket{
        .{ .bytes = "one", .destination = destination },
        .{ .bytes = "two-two", .destination = destination },
    };
    try std.testing.expectEqual(@as(usize, 2), try sender.sendBatch(&batch));

    var packets: [2]endpoint.PacketView = undefined;
    var scratch: [protocol.default_mtu * 2]u8 = undefined;
    const count = try receiver.poll(&packets, &scratch);
    try std.testing.expectEqual(@as(usize, 2), count);
    try std.testing.expectEqualStrings("one", packets[0].bytes);
    try std.testing.expectEqualStrings("two-two", packets[1].bytes);
}

test "oversized datagram returns message-too-large error" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;

    var sender = try PosixEndpoint.init(loopbackConfig(0));
    defer sender.deinit();
    var receiver = try PosixEndpoint.init(loopbackConfig(0));
    defer receiver.deinit();

    var payload: [protocol.default_mtu + 1]u8 = undefined;
    @memset(&payload, 0xaa);
    try std.testing.expectError(error.MessageTooLarge, sender.send(&payload, try receiver.localAddress()));
}

test "endpoint receives only packets sent to its bound port" {
    if (builtin.os.tag != .linux) return error.SkipZigTest;

    var sender = try PosixEndpoint.init(loopbackConfig(0));
    defer sender.deinit();
    var first = try PosixEndpoint.init(loopbackConfig(0));
    defer first.deinit();
    var second = try PosixEndpoint.init(loopbackConfig(0));
    defer second.deinit();

    _ = try sender.send("targeted", try second.localAddress());

    var packets: [1]endpoint.PacketView = undefined;
    var scratch: [protocol.default_mtu]u8 = undefined;
    try std.testing.expectEqual(@as(usize, 0), try first.poll(&packets, &scratch));
    try std.testing.expectEqual(@as(usize, 1), try second.poll(&packets, &scratch));
    try std.testing.expectEqualStrings("targeted", packets[0].bytes);
}
