// SPDX-License-Identifier: Apache-2.0

//! Minimal per-topic types used by the service-side client API.
//! Mirrors the broker-side `topic_config.zig` without a broker dependency.
//! The broker and service agree on these wire-compatible layouts.

const std = @import("std");

/// Per-PUBLISH acknowledgement mode.
pub const AckMode = enum(u8) {
    fire_and_forget = 0,
    replicate_once = 1,

    pub fn fromU8(v: u8) ?AckMode {
        return switch (v) {
            0 => .fire_and_forget,
            1 => .replicate_once,
            else => null,
        };
    }
};

/// Immutable per-topic queue geometry. Must match the broker's `TopicConfig` wire layout (32 bytes).
pub const TopicConfig = extern struct {
    roll_scheme_name: [16]u8 = [_]u8{0} ** 16,
    retention_cycles: u32 = 0,
    flags: u32 = 0,
    _reserved: [8]u8 = [_]u8{0} ** 8,

    comptime {
        std.debug.assert(@sizeOf(TopicConfig) == 32);
    }

    pub fn rollSchemeName(self: *const TopicConfig) []const u8 {
        const end = std.mem.indexOfScalar(u8, &self.roll_scheme_name, 0) orelse self.roll_scheme_name.len;
        return self.roll_scheme_name[0..end];
    }
};

/// Template IDs for topic control messages (mirrors broker topic_messages.zig).
pub const TEMPLATE_REGISTER_TOPIC_PUBLICATION: u16 = 7;
pub const TEMPLATE_TOPIC_PUBLICATION_RESPONSE: u16 = 8;
pub const TEMPLATE_SUBSCRIBE_TOPIC: u16 = 9;
pub const TEMPLATE_TOPIC_SUBSCRIPTION_RESPONSE: u16 = 10;
pub const TEMPLATE_TOPIC_ACK_FEEDBACK: u16 = 15;
