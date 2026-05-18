//! Flow control configuration for ServiceClient instances.
//!
//! Determines how a ServiceClient reacts when flow control signals indicate
//! that a target service is under pressure.

/// Strategy for handling backpressure when flow control detects congestion.
pub const BackpressureStrategy = enum(u8) {
    /// Return an error immediately.
    drop = 0,

    /// Busy-wait (with spin-loop hint) until capacity is available or
    /// the spin timeout expires (then return BackPressureTimeout).
    spin = 1,
};

/// Per-ServiceClient flow control configuration.
pub const FlowControlConfig = struct {
    /// Backpressure strategy: drop or spin.
    strategy: BackpressureStrategy = .drop,

    /// Maximum time (milliseconds) to spin-wait before returning timeout.
    /// Only used when strategy == .spin.
    spin_timeout_ms: u32 = 1,

    /// Minimum remaining bytes threshold. If the FC counter reports
    /// remaining bytes below this value, backpressure is triggered.
    /// 0 = disabled (only react to PRESSURED state).
    min_remaining_bytes: u32 = 0,

    /// Whether flow control is enabled at all. When false, all FC checks
    /// are skipped and sends proceed as before.
    enabled: bool = false,
};
