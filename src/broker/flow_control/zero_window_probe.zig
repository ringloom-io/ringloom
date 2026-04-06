//! Zero-window probe logic.
//!
//! When the receiver advertises `receiver_window = 0`, the sender must stop
//! all data transmission. Zero-window probes are periodic zero-length DATA
//! frames (heartbeats) that keep the connection alive and prompt the receiver
//! to re-evaluate its window.

const std = @import("std");
const constants = @import("brz_common").platform.constants;
const SenderFlowControl = @import("sender_flow_control.zig").SenderFlowControl;

/// Zero-window probe logic. Called by the sender event loop when
/// canSend() returns false.
pub const ZeroWindowProbe = struct {
    /// Interval between probes when in zero-window state.
    const probe_interval_ns: i64 = 100 * std.time.ns_per_ms; // 100ms

    /// Check if we should send a zero-window probe.
    /// Returns true if:
    ///   1. The window is zero (or effectively zero — less than one MTU)
    ///   2. Enough time has passed since the last probe
    pub fn shouldProbe(fc: *const SenderFlowControl, now_ns: i64) bool {
        const remaining = fc.remainingWindow();
        if (remaining > @as(i64, @intCast(constants.default_mtu_length))) return false;

        return (now_ns - fc.last_probe_sent_ns) >= probe_interval_ns;
    }
};
