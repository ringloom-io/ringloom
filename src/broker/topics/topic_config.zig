// SPDX-License-Identifier: Apache-2.0

//! Immutable per-topic configuration and per-publish acknowledgement mode.
//!
//! `TopicConfig` carries only the ringloom-queue geometry; it is fixed at first
//! creation (first-creation-wins) and MUST be identical on the leader and every
//! replica or index-exact replication NACKs. Ack mode is selected per publish
//! (carried in `TopicPublishHeader`, spec 04), not stored per topic.

const std = @import("std");

const topic_id = @import("topic_id.zig");
const TopicId = topic_id.TopicId;

/// Per-PUBLISH acknowledgement mode (carried in `TopicPublishHeader`, spec 04 —
/// NOT part of `TopicConfig`).
pub const AckMode = enum(u8) {
    /// Default: no ack, never waits.
    fire_and_forget = 0,
    /// Ack once ≥1 replica applies (single-node broker → ack on leader append).
    replicate_once = 1,
    // reserved: quorum_durable = 2

    pub fn fromU8(v: u8) ?AckMode {
        return switch (v) {
            0 => .fire_and_forget,
            1 => .replicate_once,
            else => null,
        };
    }
};

/// bit0 of `TopicConfig.flags`: open queues with huge pages.
pub const flag_use_huge_pages: u32 = 1 << 0;

/// Immutable after first creation. Defines the ringloom-queue geometry; MUST be
/// identical on leader and every replica or index-exact replication NACKs.
pub const TopicConfig = extern struct {
    /// Roll scheme name, right-padded with 0 (e.g. "FAST_DAILY").
    roll_scheme_name: [16]u8 = [_]u8{0} ** 16,
    /// 0 = keep indefinitely.
    retention_cycles: u32 = 0,
    /// bit0 = use_huge_pages.
    flags: u32 = 0,
    _reserved: [8]u8 = [_]u8{0} ** 8,

    comptime {
        std.debug.assert(@sizeOf(TopicConfig) == 32);
    }

    /// Builds a config from a roll scheme name slice (truncated/padded to 16).
    pub fn fromName(name: []const u8, retention_cycles: u32, use_huge_pages: bool) TopicConfig {
        var cfg = TopicConfig{ .retention_cycles = retention_cycles };
        const n = @min(name.len, cfg.roll_scheme_name.len);
        @memcpy(cfg.roll_scheme_name[0..n], name[0..n]);
        if (use_huge_pages) cfg.flags |= flag_use_huge_pages;
        return cfg;
    }

    /// Returns the roll scheme name with trailing 0 padding trimmed.
    pub fn rollSchemeName(self: *const TopicConfig) []const u8 {
        const end = std.mem.indexOfScalar(u8, &self.roll_scheme_name, 0) orelse self.roll_scheme_name.len;
        return self.roll_scheme_name[0..end];
    }

    pub fn useHugePages(self: *const TopicConfig) bool {
        return (self.flags & flag_use_huge_pages) != 0;
    }

    /// Full-geometry equality: this IS the config identity for first-wins, since
    /// ack mode is per publish.
    pub fn eqlForReplication(self: *const TopicConfig, other: *const TopicConfig) bool {
        return std.mem.eql(u8, self.rollSchemeName(), other.rollSchemeName()) and
            self.retention_cycles == other.retention_cycles and
            self.useHugePages() == other.useHugePages();
    }
};

/// Result of first-creation-wins validation against an existing record.
pub const ValidateError = error{
    /// Same topic_id but a different stored name (hash collision; fail closed).
    TopicIdCollision,
    /// Same name but a different stored geometry (config is immutable).
    TopicConfigMismatch,
};

/// Validates a registration against an existing record (same `topic_id`).
/// Returns void for idempotent success, or an error for collision/mismatch.
pub fn validateAgainstExisting(
    stored_name: []const u8,
    stored_config: *const TopicConfig,
    name: []const u8,
    config: *const TopicConfig,
) ValidateError!void {
    if (!std.mem.eql(u8, stored_name, name)) return error.TopicIdCollision;
    if (!stored_config.eqlForReplication(config)) return error.TopicConfigMismatch;
}

test "TopicConfig is exactly 32 bytes" {
    try std.testing.expectEqual(@as(usize, 32), @sizeOf(TopicConfig));
}

test "rollSchemeName round-trips" {
    const cfg = TopicConfig.fromName("FAST_DAILY", 7, true);
    try std.testing.expectEqualStrings("FAST_DAILY", cfg.rollSchemeName());
    try std.testing.expectEqual(@as(u32, 7), cfg.retention_cycles);
    try std.testing.expect(cfg.useHugePages());
}

test "eqlForReplication compares full geometry" {
    const a = TopicConfig.fromName("FAST_DAILY", 7, false);
    const b = TopicConfig.fromName("FAST_DAILY", 7, false);
    const c = TopicConfig.fromName("DAILY", 7, false);
    const d = TopicConfig.fromName("FAST_DAILY", 8, false);
    try std.testing.expect(a.eqlForReplication(&b));
    try std.testing.expect(!a.eqlForReplication(&c));
    try std.testing.expect(!a.eqlForReplication(&d));
}

test "first-wins validation: collision and mismatch" {
    const stored = TopicConfig.fromName("FAST_DAILY", 7, false);
    const same = TopicConfig.fromName("FAST_DAILY", 7, false);
    const diff = TopicConfig.fromName("DAILY", 7, false);

    try validateAgainstExisting("orders", &stored, "orders", &same);
    try std.testing.expectError(error.TopicIdCollision, validateAgainstExisting("orders", &stored, "trades", &same));
    try std.testing.expectError(error.TopicConfigMismatch, validateAgainstExisting("orders", &stored, "orders", &diff));
}

test "AckMode.fromU8" {
    try std.testing.expectEqual(AckMode.fire_and_forget, AckMode.fromU8(0).?);
    try std.testing.expectEqual(AckMode.replicate_once, AckMode.fromU8(1).?);
    try std.testing.expect(AckMode.fromU8(9) == null);
    _ = TopicId;
}
