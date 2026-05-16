// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const builtin = @import("builtin");
const endpoint = @import("endpoint.zig");

pub const ProbeResult = struct {
    available: bool,
    reason: endpoint.FallbackReason,
};

pub fn validateConfig(config: endpoint.AfXdpConfig) !void {
    if (config.interfaces.len == 0) return error.MissingInterface;
    if (config.ports.len == 0) return error.NoConfiguredPorts;
    if (config.rx_queue > 4095) return error.InvalidQueue;
    if (config.umem_frame_count == 0 or
        config.umem_frame_count > 65_536 or
        !std.math.isPowerOfTwo(config.umem_frame_count))
    {
        return error.InvalidUmemFrameCount;
    }
    if (config.umem_frame_size < 2048 or
        config.umem_frame_size > 16 * 1024 or
        !std.math.isPowerOfTwo(config.umem_frame_size))
    {
        return error.InvalidUmemFrameSize;
    }
}

pub fn probe(config: endpoint.AfXdpConfig) ProbeResult {
    validateConfig(config) catch return .{ .available = false, .reason = .invalid_config };
    if (builtin.os.tag != .linux) return .{ .available = false, .reason = .unsupported_os };
    return .{ .available = false, .reason = .not_implemented };
}

pub const AfXdpEndpoint = struct {
    pub fn init(config: endpoint.EndpointConfig) !AfXdpEndpoint {
        try validateConfig(config.af_xdp);
        return error.AfXdpUnavailable;
    }

    pub fn deinit(self: *AfXdpEndpoint) void {
        self.* = undefined;
    }

    pub fn localAddress(self: *const AfXdpEndpoint) !endpoint.Address {
        _ = self;
        return error.AfXdpUnavailable;
    }

    pub fn poll(
        self: *AfXdpEndpoint,
        packets: []endpoint.PacketView,
        scratch: []u8,
    ) !usize {
        _ = self;
        _ = packets;
        _ = scratch;
        return error.AfXdpUnavailable;
    }

    pub fn send(
        self: *AfXdpEndpoint,
        packet: []const u8,
        destination: endpoint.Address,
    ) !usize {
        _ = self;
        _ = packet;
        _ = destination;
        return error.AfXdpUnavailable;
    }

    pub fn sendBatch(
        self: *AfXdpEndpoint,
        batch: []const endpoint.OutboundPacket,
    ) !usize {
        _ = self;
        _ = batch;
        return error.AfXdpUnavailable;
    }
};

pub const UmemFrameAllocator = struct {
    allocator: std.mem.Allocator,
    free_stack: []u32,
    top: u32,
    frame_size: u32,

    pub fn init(allocator: std.mem.Allocator, frame_count: u32, frame_size: u32) !UmemFrameAllocator {
        if (frame_count == 0 or !std.math.isPowerOfTwo(frame_count)) return error.InvalidUmemFrameCount;
        if (frame_size < 2048 or !std.math.isPowerOfTwo(frame_size)) return error.InvalidUmemFrameSize;

        const free_stack = try allocator.alloc(u32, frame_count);
        for (free_stack, 0..) |*slot, i| {
            slot.* = @intCast(frame_count - 1 - i);
        }
        return .{
            .allocator = allocator,
            .free_stack = free_stack,
            .top = frame_count,
            .frame_size = frame_size,
        };
    }

    pub fn deinit(self: *UmemFrameAllocator) void {
        self.allocator.free(self.free_stack);
        self.* = undefined;
    }

    pub fn allocate(self: *UmemFrameAllocator) ?u64 {
        if (self.top == 0) return null;
        self.top -= 1;
        return @as(u64, self.free_stack[self.top]) * self.frame_size;
    }

    pub fn free(self: *UmemFrameAllocator, address: u64) !void {
        if (address % self.frame_size != 0) return error.InvalidFrameAddress;
        const frame_index = address / self.frame_size;
        if (frame_index >= self.free_stack.len) return error.InvalidFrameAddress;
        if (self.top >= self.free_stack.len) return error.UmemFrameDoubleFree;
        self.free_stack[self.top] = @intCast(frame_index);
        self.top += 1;
    }

    pub fn available(self: *const UmemFrameAllocator) u32 {
        return self.top;
    }
};

pub fn ipv4HeaderChecksum(header: []const u8) !u16 {
    if (header.len < 20 or header.len % 4 != 0) return error.InvalidIpv4Header;
    return onesComplementChecksum(&.{header});
}

pub fn udpChecksumIpv4(src_ip: [4]u8, dst_ip: [4]u8, udp_packet: []const u8) !u16 {
    if (udp_packet.len < 8 or udp_packet.len > endpoint.max_udp_payload) return error.InvalidUdpPacket;
    var pseudo: [12]u8 = undefined;
    @memcpy(pseudo[0..4], &src_ip);
    @memcpy(pseudo[4..8], &dst_ip);
    pseudo[8] = 0;
    pseudo[9] = 17;
    std.mem.writeInt(u16, pseudo[10..12], @intCast(udp_packet.len), .big);
    return onesComplementChecksum(&.{ &pseudo, udp_packet });
}

fn onesComplementChecksum(parts: []const []const u8) u16 {
    var sum: u32 = 0;
    for (parts) |part| {
        var i: usize = 0;
        while (i + 1 < part.len) : (i += 2) {
            sum += (@as(u32, part[i]) << 8) | part[i + 1];
        }
        if (i < part.len) {
            sum += @as(u32, part[i]) << 8;
        }
    }
    while ((sum >> 16) != 0) {
        sum = (sum & 0xffff) + (sum >> 16);
    }
    const result: u16 = @truncate(~sum);
    return if (result == 0) 0xffff else result;
}

test "AF_XDP config validation rejects missing interface invalid queue and missing ports" {
    try std.testing.expectError(error.MissingInterface, validateConfig(.{}));
    try std.testing.expectError(error.NoConfiguredPorts, validateConfig(.{ .interfaces = &.{"eth0"} }));
    try std.testing.expectError(error.InvalidQueue, validateConfig(.{
        .interfaces = &.{"eth0"},
        .ports = &.{9000},
        .rx_queue = 4096,
    }));
}

test "AF_XDP config validation rejects unbounded UMEM sizes" {
    try std.testing.expectError(error.InvalidUmemFrameCount, validateConfig(.{
        .interfaces = &.{"eth0"},
        .ports = &.{9000},
        .umem_frame_count = 131_072,
    }));
    try std.testing.expectError(error.InvalidUmemFrameSize, validateConfig(.{
        .interfaces = &.{"eth0"},
        .ports = &.{9000},
        .umem_frame_size = 32 * 1024,
    }));
}

test "AF_XDP require mode maps unavailable probe to startup error" {
    const config = endpoint.EndpointConfig{
        .local_address = .initIp4(.{ 127, 0, 0, 1 }, 9000),
        .engine_mode = .require_af_xdp,
        .af_xdp = .{ .interfaces = &.{"eth0"}, .ports = &.{9000} },
    };
    const probed = probe(config.af_xdp);
    try std.testing.expect(!probed.available);
    try std.testing.expectError(
        error.AfXdpRequired,
        endpoint.selectEngine(config, probed.available, probed.reason),
    );
}

test "UMEM frame allocator allocates frees and reports exhaustion" {
    var allocator = try UmemFrameAllocator.init(std.testing.allocator, 2, 2048);
    defer allocator.deinit();

    const first = allocator.allocate().?;
    const second = allocator.allocate().?;
    try std.testing.expect(allocator.allocate() == null);
    try std.testing.expectEqual(@as(u32, 0), allocator.available());

    try allocator.free(first);
    try std.testing.expectEqual(@as(u32, 1), allocator.available());
    try std.testing.expectEqual(first, allocator.allocate().?);

    try std.testing.expectError(error.InvalidFrameAddress, allocator.free(3));
    try allocator.free(second);
}

test "IPv4 and UDP checksum helpers produce stable checksums" {
    var ipv4 = [_]u8{
        0x45, 0x00, 0x00, 0x1c, 0xab, 0xcd, 0x00, 0x00,
        0x40, 0x11, 0x00, 0x00, 0x7f, 0x00, 0x00, 0x01,
        0x7f, 0x00, 0x00, 0x01,
    };
    const ip_checksum = try ipv4HeaderChecksum(&ipv4);
    std.mem.writeInt(u16, ipv4[10..12], ip_checksum, .big);
    try std.testing.expectEqual(@as(u16, 0xffff), try ipv4HeaderChecksum(&ipv4));

    var udp = [_]u8{ 0x23, 0x28, 0x23, 0x29, 0x00, 0x0c, 0x00, 0x00, 't', 'e', 's', 't' };
    const checksum = try udpChecksumIpv4(.{ 127, 0, 0, 1 }, .{ 127, 0, 0, 1 }, &udp);
    std.mem.writeInt(u16, udp[6..8], checksum, .big);
    try std.testing.expectEqual(@as(u16, 0xffff), try udpChecksumIpv4(.{ 127, 0, 0, 1 }, .{ 127, 0, 0, 1 }, &udp));
}
