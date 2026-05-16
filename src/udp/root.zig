// SPDX-License-Identifier: Apache-2.0
//! RingLoom reliable UDP protocol primitives.

pub const protocol = @import("protocol.zig");
pub const position = @import("position.zig");
pub const term_log = @import("term_log.zig");
pub const receive_window = @import("receive_window.zig");
pub const flow_control = @import("flow_control.zig");
pub const congestion_control = @import("congestion_control.zig");
pub const endpoint = @import("endpoint.zig");
pub const posix_endpoint = @import("posix_endpoint.zig");
pub const af_xdp_endpoint = @import("af_xdp_endpoint.zig");
pub const xdp_filter = @import("xdp_filter.zig");
pub const transport_endpoint = @import("transport_endpoint.zig");

pub const CommonHeader = protocol.CommonHeader;
pub const SetupHeader = protocol.SetupHeader;
pub const SetupResponseHeader = protocol.SetupResponseHeader;
pub const DataHeader = protocol.DataHeader;
pub const StatusHeader = protocol.StatusHeader;
pub const NakHeader = protocol.NakHeader;
pub const RttmHeader = protocol.RttmHeader;
pub const HeartbeatHeader = protocol.HeartbeatHeader;
pub const ErrorHeader = protocol.ErrorHeader;
pub const FrameType = protocol.FrameType;
pub const StreamId = protocol.StreamId;
pub const StreamKey = protocol.StreamKey;

pub const Position = position.Position;
pub const LossRange = term_log.LossRange;
pub const RetransmitAction = term_log.RetransmitAction;
pub const TermLog = term_log.TermLog;
pub const ReceiveWindow = receive_window.ReceiveWindow;
pub const ReceiverWindow = flow_control.ReceiverWindow;
pub const StaticCongestionControl = congestion_control.StaticCongestionControl;
pub const Address = endpoint.Address;
pub const EndpointConfig = endpoint.EndpointConfig;
pub const PacketView = endpoint.PacketView;
pub const OutboundPacket = endpoint.OutboundPacket;
pub const PosixEndpoint = posix_endpoint.PosixEndpoint;
pub const UdpEndpoint = transport_endpoint.UdpEndpoint;
pub const EngineSelection = endpoint.EngineSelection;
pub const AfXdpEndpoint = af_xdp_endpoint.AfXdpEndpoint;
pub const UmemFrameAllocator = af_xdp_endpoint.UmemFrameAllocator;

comptime {
    _ = @import("protocol.zig");
    _ = @import("position.zig");
    _ = @import("term_log.zig");
    _ = @import("receive_window.zig");
    _ = @import("flow_control.zig");
    _ = @import("congestion_control.zig");
    _ = @import("endpoint.zig");
    _ = @import("posix_endpoint.zig");
    _ = @import("af_xdp_endpoint.zig");
    _ = @import("xdp_filter.zig");
    _ = @import("transport_endpoint.zig");
}

test "udp module compiles" {
    _ = CommonHeader;
    _ = DataHeader;
    _ = TermLog;
    _ = ReceiveWindow;
    _ = ReceiverWindow;
    _ = StaticCongestionControl;
    _ = PosixEndpoint;
    _ = UdpEndpoint;
    _ = AfXdpEndpoint;
    _ = UmemFrameAllocator;
}
