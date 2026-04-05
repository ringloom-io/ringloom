//! Flow control counters and frame position validation.
//!
//! Counter IDs for monitoring flow control behaviour, plus validation
//! logic for detecting under-runs and over-runs on received frames.

const std = @import("std");
const CountersManager = @import("../concurrent/counters.zig").CountersManager;
const ReceiverFlowControl = @import("receiver_flow_control.zig").ReceiverFlowControl;

/// Flow-control-related counter IDs.
pub const FlowControlCounterId = enum(u16) {
    /// Send ring buffer back-pressure events. Incremented when the
    /// sender event loop cannot drain a message from the send ring
    /// buffer because canSend() returned false.
    ///
    /// High values indicate: receiver is slow, receiver window is too
    /// small, or network latency is causing SM delays.
    send_rb_back_pressure = 0x0100,

    /// Service message ring buffer back-pressure events. Incremented
    /// when the receiver event loop cannot route a message to a target
    /// service because the service's ring buffer is full.
    ///
    /// High values indicate: the target service is consuming too slowly.
    service_back_pressure = 0x0101,

    /// Flow control under-runs. Incremented when a received DATA frame
    /// has a position below the receiver's current consumption position.
    /// This means the frame is stale — either a duplicate retransmit or
    /// a severely delayed packet.
    ///
    /// Occasional under-runs are normal (retransmits after NAK). Frequent
    /// under-runs suggest aggressive retransmit timing or network issues.
    flow_control_under_runs = 0x0102,

    /// Flow control over-runs. Incremented when a received DATA frame
    /// has a position beyond `consumption_position + log_capacity`.
    /// This means the sender violated its flow control contract — it
    /// sent data beyond the advertised window.
    ///
    /// This should NEVER happen. Any non-zero value is a bug in the
    /// sender's flow control logic.
    flow_control_over_runs = 0x0103,

    /// Status Messages sent. Total count of SMs sent by the receiver
    /// to all peers.
    status_messages_sent = 0x0104,

    /// Status Messages received. Total count of SMs received by the
    /// sender from all peers.
    status_messages_received = 0x0105,

    /// Zero-window probes sent. Total count of zero-length heartbeat
    /// probes sent while in flow-controlled state.
    zero_window_probes_sent = 0x0106,

    /// Total bytes sent (across all peers). Useful for throughput
    /// calculation.
    bytes_sent = 0x0110,

    /// Total bytes received (across all peers).
    bytes_received = 0x0111,
};

/// Result of validating a received frame's position.
pub const FramePositionValidity = enum {
    valid,
    under_run,
    over_run,
};

/// Validate a received frame's position against flow control bounds.
/// Called before inserting into the receive log buffer.
pub fn validateFramePosition(
    fc: *const ReceiverFlowControl,
    frame_position: i64,
    frame_length: i32,
) FramePositionValidity {
    // Under-run: frame is behind consumption position
    if (frame_position + @as(i64, frame_length) <= fc.consumption_position) {
        return .under_run;
    }

    // Over-run: frame is beyond receivable range
    const max_receivable = fc.consumption_position + fc.log_capacity;
    if (frame_position >= max_receivable) {
        return .over_run;
    }

    return .valid;
}
