//! Back-pressure strategies for service-level flow control.
//!
//! When the send ring buffer is full (because the sender event loop is
//! flow-controlled by a slow receiver), services must decide how to handle
//! the situation. This module defines the available strategies.

const std = @import("std");

/// Back-pressure strategies available to services when the send ring
/// buffer is full.
pub const BackPressureStrategy = enum {
    /// Return error.BufferFull immediately. The service must handle it
    /// (drop the message, queue it internally, or report to the user).
    /// Lowest latency impact — no blocking, no spinning.
    fail_fast,

    /// Spin-retry with yield. The service thread spins on tryClaim(),
    /// calling std.atomic.spinLoopHint() between attempts, up to a
    /// configurable maximum number of retries.
    spin_retry,

    /// Block until space is available. Only valid when the ring buffer
    /// is in blocking mode (has a blocking trailer with writer_wait_state).
    /// The thread parks on a futex/ulock and is woken by the consumer
    /// (sender event loop) when it advances the head position.
    blocking,
};
