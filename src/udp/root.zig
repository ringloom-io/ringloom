// SPDX-License-Identifier: Apache-2.0
//! RingLoom reliable UDP protocol primitives.

pub const protocol = @import("protocol.zig");
pub const position = @import("position.zig");
pub const term_log = @import("term_log.zig");
pub const receive_window = @import("receive_window.zig");

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

comptime {
    _ = @import("protocol.zig");
    _ = @import("position.zig");
    _ = @import("term_log.zig");
    _ = @import("receive_window.zig");
}

test "udp module compiles" {
    _ = CommonHeader;
    _ = DataHeader;
    _ = TermLog;
    _ = ReceiveWindow;
}
