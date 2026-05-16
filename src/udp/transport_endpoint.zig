// SPDX-License-Identifier: Apache-2.0
const std = @import("std");

const af_xdp_endpoint = @import("af_xdp_endpoint.zig");
const endpoint = @import("endpoint.zig");
const posix_endpoint = @import("posix_endpoint.zig");

const log = std.log.scoped(.udp_endpoint);

pub const UdpEndpoint = struct {
    backend: Backend,
    selection: endpoint.EngineSelection,

    const Backend = union(enum) {
        posix: posix_endpoint.PosixEndpoint,
        af_xdp: af_xdp_endpoint.AfXdpEndpoint,
    };

    pub fn init(config: endpoint.EndpointConfig) !UdpEndpoint {
        switch (config.engine_mode) {
            .posix => return initPosix(config, .{ .engine = .posix }),
            .prefer_af_xdp, .require_af_xdp => {},
        }

        const probed = af_xdp_endpoint.probe(config.af_xdp);
        const selected = try endpoint.selectEngine(config, probed.available, probed.reason);

        switch (selected.engine) {
            .posix => {
                log.warn("AF_XDP unavailable; falling back to POSIX UDP ({s})", .{@tagName(selected.reason)});
                return initPosix(config, selected);
            },
            .af_xdp => {
                const af_xdp = af_xdp_endpoint.AfXdpEndpoint.init(config) catch |err| {
                    if (config.engine_mode == .require_af_xdp) return error.AfXdpRequired;
                    const fallback_selection = endpoint.EngineSelection{
                        .engine = .posix,
                        .fell_back = true,
                        .fallback_count = 1,
                        .reason = fallbackReasonFromInitError(err),
                    };
                    log.warn("AF_XDP setup failed; falling back to POSIX UDP ({s})", .{@tagName(fallback_selection.reason)});
                    return initPosix(config, fallback_selection);
                };
                return .{
                    .backend = .{ .af_xdp = af_xdp },
                    .selection = selected,
                };
            },
        }
    }

    pub fn deinit(self: *UdpEndpoint) void {
        switch (self.backend) {
            .posix => |*ep| ep.deinit(),
            .af_xdp => |*ep| ep.deinit(),
        }
        self.* = undefined;
    }

    pub fn poll(
        self: *UdpEndpoint,
        packets: []endpoint.PacketView,
        scratch: []u8,
    ) !usize {
        return switch (self.backend) {
            .posix => |*ep| ep.poll(packets, scratch),
            .af_xdp => |*ep| ep.poll(packets, scratch),
        };
    }

    pub fn send(
        self: *UdpEndpoint,
        packet: []const u8,
        destination: endpoint.Address,
    ) !usize {
        return switch (self.backend) {
            .posix => |*ep| ep.send(packet, destination),
            .af_xdp => |*ep| ep.send(packet, destination),
        };
    }

    pub fn sendBatch(
        self: *UdpEndpoint,
        batch: []const endpoint.OutboundPacket,
    ) !usize {
        return switch (self.backend) {
            .posix => |*ep| ep.sendBatch(batch),
            .af_xdp => |*ep| ep.sendBatch(batch),
        };
    }

    pub fn localAddress(self: *UdpEndpoint) !endpoint.Address {
        return switch (self.backend) {
            .posix => |*ep| ep.localAddress(),
            .af_xdp => |*ep| ep.localAddress(),
        };
    }

    pub fn engine(self: *const UdpEndpoint) endpoint.Engine {
        return self.selection.engine;
    }
};

fn initPosix(config: endpoint.EndpointConfig, selection: endpoint.EngineSelection) !UdpEndpoint {
    var posix_config = config;
    posix_config.engine_mode = .posix;
    posix_config.af_xdp = .{};
    return .{
        .backend = .{ .posix = try posix_endpoint.PosixEndpoint.init(posix_config) },
        .selection = selection,
    };
}

fn fallbackReasonFromInitError(err: anyerror) endpoint.FallbackReason {
    return switch (err) {
        error.MissingInterface,
        error.NoConfiguredPorts,
        error.InvalidQueue,
        error.InvalidUmemFrameCount,
        error.InvalidUmemFrameSize,
        => .invalid_config,
        error.UnsupportedOs => .unsupported_os,
        error.AfXdpUnavailable => .not_implemented,
        else => .unavailable,
    };
}

test "preferred AF_XDP falls back to POSIX when implementation is unavailable" {
    if (@import("builtin").os.tag != .linux) return error.SkipZigTest;

    var ep = try UdpEndpoint.init(.{
        .local_address = .initIp4(.{ 127, 0, 0, 1 }, 0),
        .engine_mode = .prefer_af_xdp,
        .af_xdp = .{ .interfaces = &.{"lo"}, .ports = &.{9000} },
    });
    defer ep.deinit();

    try std.testing.expectEqual(endpoint.Engine.posix, ep.selection.engine);
    try std.testing.expect(ep.selection.fell_back);
    try std.testing.expectEqual(endpoint.FallbackReason.not_implemented, ep.selection.reason);
}

test "required AF_XDP fails before binding POSIX fallback" {
    try std.testing.expectError(error.AfXdpRequired, UdpEndpoint.init(.{
        .local_address = .initIp4(.{ 127, 0, 0, 1 }, 0),
        .engine_mode = .require_af_xdp,
        .af_xdp = .{ .interfaces = &.{"lo"}, .ports = &.{9000} },
    }));
}
