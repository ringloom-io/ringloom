// SPDX-License-Identifier: Apache-2.0

//! Deterministic, coordination-free topic identity.
//!
//! Every broker and every service client computes the same `topic_id` from the
//! topic name, so no ID allocation or propagation is required for agreement. A
//! name→id table is still kept (registry, spec 02) for collision detection and
//! observability.

const std = @import("std");

/// Topic identifier. A distinct ID space from `service_id` (`u16`); topic frames
/// are NEVER routed by `target_service_id`.
pub const TopicId = u64;

/// Reserved/invalid topic id. A name hashing to 0 is rejected (fail closed).
pub const invalid_topic_id: TopicId = 0;

/// Deterministic 64-bit hash of the topic name. Uses `std.hash.Wyhash` with a
/// FIXED seed (0) so all nodes and binaries agree on the same id.
pub fn topicIdOf(name: []const u8) TopicId {
    return std.hash.Wyhash.hash(0, name);
}

/// True when `id` is a usable (non-reserved) topic id.
pub fn isValid(id: TopicId) bool {
    return id != invalid_topic_id;
}

test "topicIdOf is stable across calls" {
    const a = topicIdOf("orders");
    const b = topicIdOf("orders");
    try std.testing.expectEqual(a, b);
}

test "topicIdOf matches hand-computed Wyhash" {
    try std.testing.expectEqual(std.hash.Wyhash.hash(0, "trades"), topicIdOf("trades"));
}

test "distinct names produce distinct ids" {
    const names = [_][]const u8{ "a", "b", "orders", "trades", "ticks", "fills" };
    for (names, 0..) |n1, i| {
        for (names[i + 1 ..]) |n2| {
            try std.testing.expect(topicIdOf(n1) != topicIdOf(n2));
        }
    }
}

test "invalid id is rejected by isValid" {
    try std.testing.expect(!isValid(invalid_topic_id));
    try std.testing.expect(isValid(topicIdOf("orders")));
}
