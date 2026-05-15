// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const protocol = @import("protocol.zig");

pub const max_udp_payload: usize = 65_507;
pub const max_batch_packets: usize = 64;

pub const Address = struct {
    ip: [4]u8,
    port: u16,

    pub fn initIp4(ip: [4]u8, port: u16) Address {
        return .{ .ip = ip, .port = port };
    }

    pub fn parseIp4(text: []const u8, port: u16) !Address {
        const parsed = try std.Io.net.Ip4Address.parse(text, port);
        return initIp4(parsed.bytes, parsed.port);
    }

    pub fn toSockaddr(self: Address) std.posix.sockaddr.in {
        return .{
            .port = std.mem.nativeToBig(u16, self.port),
            .addr = @bitCast(self.ip),
        };
    }

    pub fn fromSockaddr(sockaddr: *const std.posix.sockaddr) !Address {
        if (sockaddr.family != std.posix.AF.INET) return error.UnsupportedAddressFamily;
        const in: *const std.posix.sockaddr.in = @ptrCast(@alignCast(sockaddr));
        return .{
            .ip = @bitCast(in.addr),
            .port = std.mem.bigToNative(u16, in.port),
        };
    }

    pub fn formatBuf(self: Address, buf: []u8) ![]u8 {
        return std.fmt.bufPrint(buf, "{d}.{d}.{d}.{d}:{d}", .{
            self.ip[0],
            self.ip[1],
            self.ip[2],
            self.ip[3],
            self.port,
        });
    }
};

pub const PmtudMode = enum {
    dont,
    want,
    do,
};

pub const EngineMode = enum {
    posix,
    prefer_af_xdp,
    require_af_xdp,
};

pub const AfXdpLinkMode = enum {
    zero_copy,
    copy,
    generic_allowed,
};

pub const AfXdpConfig = struct {
    interfaces: []const []const u8 = &.{},
    ports: []const u16 = &.{},
    rx_queue: u32 = 0,
    umem_frame_count: u32 = 4096,
    umem_frame_size: u32 = 2048,
    link_mode: AfXdpLinkMode = .zero_copy,
};

pub const EndpointConfig = struct {
    local_address: Address,
    mtu: u16 = protocol.default_mtu,
    send_buffer_size: u32 = 262_144,
    recv_buffer_size: u32 = 262_144,
    pmtud: PmtudMode = .do,
    engine_mode: EngineMode = .posix,
    af_xdp: AfXdpConfig = .{},

    pub fn validate(self: EndpointConfig) !void {
        if (self.local_address.port == 0) {
            return error.InvalidPort;
        }
        try validateCommonSocketConfig(self.mtu, self.send_buffer_size, self.recv_buffer_size);
    }

    pub fn validateForEphemeralPort(self: EndpointConfig) !void {
        try validateCommonSocketConfig(self.mtu, self.send_buffer_size, self.recv_buffer_size);
    }
};

fn validateCommonSocketConfig(mtu: u16, send_buffer_size: u32, recv_buffer_size: u32) !void {
    if (mtu < protocol.DataHeader.encoded_length or mtu > max_udp_payload) {
        return error.InvalidMtu;
    }
    if (send_buffer_size < mtu or recv_buffer_size < mtu) {
        return error.InvalidSocketBufferSize;
    }
}

pub const PacketView = struct {
    bytes: []const u8,
    source: Address,
    local: ?Address = null,
    ecn: ?u8 = null,
    length: usize,
};

pub const OutboundPacket = struct {
    bytes: []const u8,
    destination: Address,
};

pub const Engine = enum {
    posix,
    af_xdp,
};

pub const FallbackReason = enum {
    not_requested,
    unavailable,
    unsupported_os,
    invalid_config,
};

pub const EngineSelection = struct {
    engine: Engine,
    fell_back: bool = false,
    fallback_count: u64 = 0,
    reason: FallbackReason = .not_requested,
};

pub fn selectEngine(config: EndpointConfig, af_xdp_available: bool, failure_reason: FallbackReason) !EngineSelection {
    return switch (config.engine_mode) {
        .posix => .{ .engine = .posix },
        .prefer_af_xdp => if (af_xdp_available)
            .{ .engine = .af_xdp }
        else
            .{
                .engine = .posix,
                .fell_back = true,
                .fallback_count = 1,
                .reason = failure_reason,
            },
        .require_af_xdp => if (af_xdp_available)
            .{ .engine = .af_xdp }
        else
            error.AfXdpRequired,
    };
}

test "Address parses and formats IPv4 endpoints" {
    const address = try Address.parseIp4("127.0.0.1", 9000);
    try std.testing.expectEqual([4]u8{ 127, 0, 0, 1 }, address.ip);
    try std.testing.expectEqual(@as(u16, 9000), address.port);

    var buf: [32]u8 = undefined;
    try std.testing.expectEqualStrings("127.0.0.1:9000", try address.formatBuf(&buf));

    const sockaddr = address.toSockaddr();
    const round_trip = try Address.fromSockaddr(@ptrCast(&sockaddr));
    try std.testing.expectEqual(address, round_trip);
}

test "EndpointConfig validation rejects invalid MTU and socket buffers" {
    const base = EndpointConfig{ .local_address = .initIp4(.{ 127, 0, 0, 1 }, 9000) };

    var invalid_mtu = base;
    invalid_mtu.mtu = 32;
    try std.testing.expectError(error.InvalidMtu, invalid_mtu.validate());

    var invalid_send_buffer = base;
    invalid_send_buffer.send_buffer_size = 128;
    try std.testing.expectError(error.InvalidSocketBufferSize, invalid_send_buffer.validate());

    var invalid_recv_buffer = base;
    invalid_recv_buffer.recv_buffer_size = 128;
    try std.testing.expectError(error.InvalidSocketBufferSize, invalid_recv_buffer.validate());
}

test "engine selection falls back from preferred AF_XDP and rejects required AF_XDP" {
    const config = EndpointConfig{
        .local_address = .initIp4(.{ 127, 0, 0, 1 }, 9000),
        .engine_mode = .prefer_af_xdp,
    };
    const selected = try selectEngine(config, false, .unavailable);
    try std.testing.expectEqual(Engine.posix, selected.engine);
    try std.testing.expect(selected.fell_back);
    try std.testing.expectEqual(@as(u64, 1), selected.fallback_count);
    try std.testing.expectEqual(FallbackReason.unavailable, selected.reason);

    var required = config;
    required.engine_mode = .require_af_xdp;
    try std.testing.expectError(error.AfXdpRequired, selectEngine(required, false, .unavailable));
}
