// SPDX-License-Identifier: Apache-2.0
//! Topic admin messages (broker↔broker, over the existing admin UDP stream).
//!
//! These reuse the generic `AdminMessageHeader` envelope from `admin_messages.zig`
//! (8-byte SBE-style header + fixed body) and the Aeron admin routing frame, so
//! topic metadata propagation rides the same admin transport as cluster admin.
//! Templates 12–18 (1–5, 9–11 are cluster admin).

const std = @import("std");
const admin = @import("../cluster/admin_messages.zig");
const topic_config_mod = @import("topic_config.zig");
const topic_id_mod = @import("topic_id.zig");

const TopicConfig = topic_config_mod.TopicConfig;
const TopicId = topic_id_mod.TopicId;

pub const TEMPLATE_TOPIC_CREATED: u16 = 12;
pub const TEMPLATE_TOPIC_LOOKUP: u16 = 13;
pub const TEMPLATE_TOPIC_INFO: u16 = 14;
pub const TEMPLATE_TOPIC_LEADER_CHANGED: u16 = 15;
pub const TEMPLATE_TOPIC_APPLIED_QUERY: u16 = 16;
pub const TEMPLATE_TOPIC_APPLIED_REPLY: u16 = 17;
pub const TEMPLATE_TOPIC_ACK_FEEDBACK: u16 = 18;

pub const topic_name_max: usize = 64;

/// templateId = 12 / 14: TopicCreated (and TopicInfo, which adds `exists`).
/// leader → all (CREATED) registers/refreshes a topic everywhere.
pub const TopicCreatedBody = extern struct {
    topic_id: u64 align(1),
    leader_epoch: u64 align(1),
    leader_node_id: u8,
    /// TopicInfo only: 1 if the topic exists, else 0. Ignored for CREATED.
    exists: u8,
    _pad: [6]u8 = [_]u8{0} ** 6,
    config: TopicConfig,
    name: [topic_name_max]u8,

    comptime {
        std.debug.assert(@sizeOf(TopicCreatedBody) == 8 + 8 + 1 + 1 + 6 + 32 + 64);
    }
};

/// templateId = 13: TopicLookup. any → topic leader.
pub const TopicLookupBody = extern struct {
    topic_id: u64 align(1),
};

/// templateId = 15: TopicLeaderChanged. new topic leader → all (0 = all topics).
pub const TopicLeaderChangedBody = extern struct {
    topic_id: u64 align(1),
    leader_epoch: u64 align(1),
    new_leader: u8,
    _pad: [7]u8 = [_]u8{0} ** 7,
};

/// templateId = 16: TopicAppliedQuery (catch-up barrier; 0 = all topics).
pub const TopicAppliedQueryBody = extern struct {
    topic_id: u64 align(1),
};

/// templateId = 17: TopicAppliedReply. peer → new topic leader.
pub const TopicAppliedReplyBody = extern struct {
    topic_id: u64 align(1),
    last_applied_index: u64 align(1),
    leader_epoch: u64 align(1),
    node: u8,
    _pad: [7]u8 = [_]u8{0} ** 7,
};

/// templateId = 18: TopicAckFeedback. topic leader → brokers with local producers.
pub const TopicAckFeedbackBody = extern struct {
    topic_id: u64 align(1),
    leader_epoch: u64 align(1),
    replicated_hwm: u64 align(1),
};

pub fn padTopicName(name: []const u8) [topic_name_max]u8 {
    var out: [topic_name_max]u8 = [_]u8{0} ** topic_name_max;
    const n = @min(name.len, topic_name_max);
    @memcpy(out[0..n], name[0..n]);
    return out;
}

pub fn trimTopicName(name: *const [topic_name_max]u8) []const u8 {
    const end = std.mem.indexOfScalar(u8, name, 0) orelse topic_name_max;
    return name[0..end];
}

/// Encode `body` into an 8-byte admin envelope + body. Returns bytes written.
pub fn encode(buf: []u8, comptime BodyType: type, template_id: u16, body: BodyType) usize {
    return admin.encodeAdminMessage(buf, BodyType, template_id, body);
}

/// Decode a body of `BodyType` from an admin payload (after the routing frame).
pub fn decodeBody(comptime BodyType: type, payload: []const u8) ?BodyType {
    const body = admin.bodySlice(payload);
    if (body.len < @sizeOf(BodyType)) return null;
    var out: BodyType = undefined;
    const dst: *[@sizeOf(BodyType)]u8 = @ptrCast(&out);
    @memcpy(dst, body[0..@sizeOf(BodyType)]);
    return out;
}

/// Returns the template id of an admin payload, or null if malformed.
pub fn templateId(payload: []const u8) ?u16 {
    const h = admin.decodeHeader(payload) orelse return null;
    return h.template_id;
}

// ── Tests ────────────────────────────────────────────────────────────────────

const testing = std.testing;

test "TopicCreated round-trips through admin envelope" {
    var buf: [256]u8 = undefined;
    const cfg = TopicConfig.fromName("FAST_DAILY", 7, false);
    const body = TopicCreatedBody{
        .topic_id = 0xABCDEF,
        .leader_epoch = 5,
        .leader_node_id = 2,
        .exists = 1,
        .config = cfg,
        .name = padTopicName("orders"),
    };
    const n = encode(&buf, TopicCreatedBody, TEMPLATE_TOPIC_CREATED, body);
    const payload = buf[0..n];

    try testing.expectEqual(@as(?u16, TEMPLATE_TOPIC_CREATED), templateId(payload));
    const d = decodeBody(TopicCreatedBody, payload).?;
    try testing.expectEqual(@as(u64, 0xABCDEF), d.topic_id);
    try testing.expectEqual(@as(u64, 5), d.leader_epoch);
    try testing.expectEqual(@as(u8, 2), d.leader_node_id);
    try testing.expectEqualStrings("orders", trimTopicName(&d.name));
    try testing.expect(d.config.eqlForReplication(&cfg));
}

test "TopicLeaderChanged + AppliedReply round-trip" {
    var buf: [128]u8 = undefined;

    const lc = TopicLeaderChangedBody{ .topic_id = 0, .leader_epoch = 9, .new_leader = 1 };
    const n1 = encode(&buf, TopicLeaderChangedBody, TEMPLATE_TOPIC_LEADER_CHANGED, lc);
    const d1 = decodeBody(TopicLeaderChangedBody, buf[0..n1]).?;
    try testing.expectEqual(@as(u64, 9), d1.leader_epoch);
    try testing.expectEqual(@as(u8, 1), d1.new_leader);

    const ar = TopicAppliedReplyBody{ .topic_id = 42, .last_applied_index = 12345, .leader_epoch = 9, .node = 3 };
    const n2 = encode(&buf, TopicAppliedReplyBody, TEMPLATE_TOPIC_APPLIED_REPLY, ar);
    const d2 = decodeBody(TopicAppliedReplyBody, buf[0..n2]).?;
    try testing.expectEqual(@as(u64, 12345), d2.last_applied_index);
    try testing.expectEqual(@as(u8, 3), d2.node);
}

test "padTopicName truncates oversize names" {
    const long = "x" ** 100;
    const padded = padTopicName(long);
    try testing.expectEqual(@as(usize, topic_name_max), trimTopicName(&padded).len);
}
