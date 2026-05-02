const std = @import("std");
const posix = std.posix;

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
