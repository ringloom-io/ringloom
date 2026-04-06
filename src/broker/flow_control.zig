//! Flow control subsystem for the BRZ broker.
//!
//! This is the single import point for all flow-control-related functionality.
//! It re-exports the sender/receiver flow control state, Status Message
//! flyweight and scheduler, zero-window probe, back-pressure strategy,
//! and counter types.

pub const SenderFlowControl = @import("flow_control/sender_flow_control.zig").SenderFlowControl;
pub const ReceiverFlowControl = @import("flow_control/receiver_flow_control.zig").ReceiverFlowControl;
pub const markFrameConsumed = @import("flow_control/receiver_flow_control.zig").markFrameConsumed;
pub const frame_consumed_marker = @import("flow_control/receiver_flow_control.zig").frame_consumed_marker;
pub const StatusMessageFlyweight = @import("flow_control/status_message.zig").StatusMessageFlyweight;
pub const StatusMessageScheduler = @import("flow_control/status_message.zig").StatusMessageScheduler;
pub const sm_flags = @import("flow_control/status_message.zig").sm_flags;
pub const ZeroWindowProbe = @import("flow_control/zero_window_probe.zig").ZeroWindowProbe;
pub const BackPressureStrategy = @import("flow_control/back_pressure.zig").BackPressureStrategy;
pub const FlowControlCounterId = @import("flow_control/counters.zig").FlowControlCounterId;
pub const FramePositionValidity = @import("flow_control/counters.zig").FramePositionValidity;
pub const validateFramePosition = @import("flow_control/counters.zig").validateFramePosition;

// Ensure all flow control module tests are discovered by `zig build test`.
comptime {
    _ = @import("flow_control/sender_flow_control.zig");
    _ = @import("flow_control/receiver_flow_control.zig");
    _ = @import("flow_control/status_message.zig");
    _ = @import("flow_control/zero_window_probe.zig");
    _ = @import("flow_control/back_pressure.zig");
    _ = @import("flow_control/counters.zig");
    _ = @import("flow_control/test_flow_control.zig");
}
