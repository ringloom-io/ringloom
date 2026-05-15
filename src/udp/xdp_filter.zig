// SPDX-License-Identifier: Apache-2.0
const std = @import("std");

const ethernet_header_length = 14;
const ether_type_ipv4: u16 = 0x0800;
const ether_type_ipv6: u16 = 0x86dd;
const ip_protocol_udp: u8 = 17;
const ip_protocol_ipv6_fragment: u8 = 44;

pub const Decision = enum {
    pass,
    redirect,
};

pub const FilterConfig = struct {
    ports: []const u16,
    rx_queue: u32,
    packet_rx_queue: u32,
    xsk_present_for_queue: bool,
};

pub fn decide(packet: []const u8, config: FilterConfig) Decision {
    if (packet.len < ethernet_header_length) return .pass;
    const ether_type = readU16(packet[12..14]);
    return switch (ether_type) {
        ether_type_ipv4 => decideIpv4(packet[ethernet_header_length..], config),
        ether_type_ipv6 => decideIpv6(packet[ethernet_header_length..], config),
        else => .pass,
    };
}

fn decideIpv4(packet: []const u8, config: FilterConfig) Decision {
    if (packet.len < 20) return .pass;
    const ihl = @as(usize, packet[0] & 0x0f) * 4;
    if (ihl < 20 or packet.len < ihl + 8) return .pass;
    if (packet[9] != ip_protocol_udp) return .pass;

    const flags_fragment = readU16(packet[6..8]);
    if ((flags_fragment & 0x3fff) != 0) return .pass;

    const destination_port = readU16(packet[ihl + 2 .. ihl + 4]);
    return portDecision(destination_port, config);
}

fn decideIpv6(packet: []const u8, config: FilterConfig) Decision {
    if (packet.len < 40) return .pass;
    const next_header = packet[6];
    if (next_header == ip_protocol_ipv6_fragment) return .pass;
    if (next_header != ip_protocol_udp) return .pass;
    if (packet.len < 48) return .pass;

    const destination_port = readU16(packet[42..44]);
    return portDecision(destination_port, config);
}

fn portDecision(destination_port: u16, config: FilterConfig) Decision {
    if (config.packet_rx_queue != config.rx_queue) return .pass;
    if (!config.xsk_present_for_queue) return .pass;
    for (config.ports) |port| {
        if (port == destination_port) return .redirect;
    }
    return .pass;
}

fn readU16(bytes: []const u8) u16 {
    return (@as(u16, bytes[0]) << 8) | bytes[1];
}

fn writeEthernetHeader(buf: []u8, ether_type: u16) void {
    @memset(buf[0..12], 0);
    std.mem.writeInt(u16, buf[12..14], ether_type, .big);
}

test "XDP filter passes TCP packets" {
    var packet: [ethernet_header_length + 20]u8 = undefined;
    writeEthernetHeader(packet[0..ethernet_header_length], ether_type_ipv4);
    @memset(packet[ethernet_header_length..], 0);
    packet[ethernet_header_length] = 0x45;
    packet[ethernet_header_length + 9] = 6;

    try std.testing.expectEqual(Decision.pass, decide(&packet, .{
        .ports = &.{9000},
        .rx_queue = 0,
        .packet_rx_queue = 0,
        .xsk_present_for_queue = true,
    }));
}

test "XDP filter passes UDP packets for unconfigured ports" {
    var packet: [ethernet_header_length + 28]u8 = undefined;
    writeEthernetHeader(packet[0..ethernet_header_length], ether_type_ipv4);
    @memset(packet[ethernet_header_length..], 0);
    packet[ethernet_header_length] = 0x45;
    packet[ethernet_header_length + 9] = ip_protocol_udp;
    std.mem.writeInt(u16, packet[ethernet_header_length + 22 .. ethernet_header_length + 24], 9001, .big);

    try std.testing.expectEqual(Decision.pass, decide(&packet, .{
        .ports = &.{9000},
        .rx_queue = 0,
        .packet_rx_queue = 0,
        .xsk_present_for_queue = true,
    }));
}

test "XDP filter redirects configured UDP port when XSK exists" {
    var packet: [ethernet_header_length + 28]u8 = undefined;
    writeEthernetHeader(packet[0..ethernet_header_length], ether_type_ipv4);
    @memset(packet[ethernet_header_length..], 0);
    packet[ethernet_header_length] = 0x45;
    packet[ethernet_header_length + 9] = ip_protocol_udp;
    std.mem.writeInt(u16, packet[ethernet_header_length + 22 .. ethernet_header_length + 24], 9000, .big);

    try std.testing.expectEqual(Decision.redirect, decide(&packet, .{
        .ports = &.{9000},
        .rx_queue = 0,
        .packet_rx_queue = 0,
        .xsk_present_for_queue = true,
    }));
}

test "XDP filter passes configured UDP port when XSK target is missing" {
    var packet: [ethernet_header_length + 28]u8 = undefined;
    writeEthernetHeader(packet[0..ethernet_header_length], ether_type_ipv4);
    @memset(packet[ethernet_header_length..], 0);
    packet[ethernet_header_length] = 0x45;
    packet[ethernet_header_length + 9] = ip_protocol_udp;
    std.mem.writeInt(u16, packet[ethernet_header_length + 22 .. ethernet_header_length + 24], 9000, .big);

    try std.testing.expectEqual(Decision.pass, decide(&packet, .{
        .ports = &.{9000},
        .rx_queue = 0,
        .packet_rx_queue = 0,
        .xsk_present_for_queue = false,
    }));
}

test "XDP filter passes fragmented IPv4 packets and malformed packets" {
    try std.testing.expectEqual(Decision.pass, decide("short", .{
        .ports = &.{9000},
        .rx_queue = 0,
        .packet_rx_queue = 0,
        .xsk_present_for_queue = true,
    }));

    var packet: [ethernet_header_length + 28]u8 = undefined;
    writeEthernetHeader(packet[0..ethernet_header_length], ether_type_ipv4);
    @memset(packet[ethernet_header_length..], 0);
    packet[ethernet_header_length] = 0x45;
    packet[ethernet_header_length + 9] = ip_protocol_udp;
    std.mem.writeInt(u16, packet[ethernet_header_length + 6 .. ethernet_header_length + 8], 0x2000, .big);
    std.mem.writeInt(u16, packet[ethernet_header_length + 22 .. ethernet_header_length + 24], 9000, .big);

    try std.testing.expectEqual(Decision.pass, decide(&packet, .{
        .ports = &.{9000},
        .rx_queue = 0,
        .packet_rx_queue = 0,
        .xsk_present_for_queue = true,
    }));
}

test "XDP filter parses IPv6 UDP and passes IPv6 fragments" {
    var udp_packet: [ethernet_header_length + 48]u8 = undefined;
    writeEthernetHeader(udp_packet[0..ethernet_header_length], ether_type_ipv6);
    @memset(udp_packet[ethernet_header_length..], 0);
    udp_packet[ethernet_header_length + 6] = ip_protocol_udp;
    std.mem.writeInt(u16, udp_packet[ethernet_header_length + 42 .. ethernet_header_length + 44], 9000, .big);
    try std.testing.expectEqual(Decision.redirect, decide(&udp_packet, .{
        .ports = &.{9000},
        .rx_queue = 0,
        .packet_rx_queue = 0,
        .xsk_present_for_queue = true,
    }));

    var fragment_packet = udp_packet;
    fragment_packet[ethernet_header_length + 6] = ip_protocol_ipv6_fragment;
    try std.testing.expectEqual(Decision.pass, decide(&fragment_packet, .{
        .ports = &.{9000},
        .rx_queue = 0,
        .packet_rx_queue = 0,
        .xsk_present_for_queue = true,
    }));
}
