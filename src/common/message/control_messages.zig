// SPDX-License-Identifier: Apache-2.0
//! Shared control-message header used by both the broker and the service.
//!
//! The broker's `src/broker/control/control_messages.zig` keeps the template
//! 1–6 message bodies and encoders; the broker-side `topic_messages.zig` keeps
//! the template 7–15 message bodies. Both depend on this 4-byte header, which
//! lives in common so the service runtime can encode/decode the same layout
//! without importing broker code.

const std = @import("std");

// ── Common Header ─────────────────────────────────────────────────────

/// 4-byte header prefixed to every control message.
/// The ring buffer's own record header (8 bytes) wraps this — the control
/// message header is the first thing inside the record payload.
pub const ControlMessageHeader = extern struct {
    /// Identifies the message type (see the template_id tables in the broker/
    /// service control modules).
    template_id: u16,

    /// Length of the message body AFTER this header, in bytes.
    /// Total message size = @sizeOf(ControlMessageHeader) + body_length.
    body_length: u16,
};

pub const header_size: usize = @sizeOf(ControlMessageHeader); // 4

test "header size is 4 bytes" {
    try std.testing.expectEqual(@as(usize, 4), header_size);
    try std.testing.expectEqual(@as(usize, 4), @sizeOf(ControlMessageHeader));
}
