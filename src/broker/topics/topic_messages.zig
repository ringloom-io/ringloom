// SPDX-License-Identifier: Apache-2.0
//! Service↔broker topic control messages (spec 03), templates 7–15.
//!
//! These extend the control ring-buffer message family in `control_messages.zig`
//! (4-byte `ControlMessageHeader{template_id, body_length}` + extern body, with a
//! variable-length name/path tail). Fields after a variable-length region use
//! `align(1)` like `ServiceInstanceEntry`.

const std = @import("std");
const cm = @import("../control/control_messages.zig");
const topic_config_mod = @import("topic_config.zig");

const ControlMessageHeader = cm.ControlMessageHeader;
const header_size = cm.header_size;
const TopicConfig = topic_config_mod.TopicConfig;

pub const TEMPLATE_REGISTER_TOPIC_PUBLICATION: u16 = 7;
pub const TEMPLATE_TOPIC_PUBLICATION_RESPONSE: u16 = 8;
pub const TEMPLATE_SUBSCRIBE_TOPIC: u16 = 9;
pub const TEMPLATE_TOPIC_SUBSCRIPTION_RESPONSE: u16 = 10;
pub const TEMPLATE_UNREGISTER_TOPIC_PUBLICATION: u16 = 11;
pub const TEMPLATE_UNSUBSCRIBE_TOPIC: u16 = 12;
pub const TEMPLATE_TOPIC_LEADER_CHANGED: u16 = 13;
pub const TEMPLATE_TOPIC_ENDPOINT: u16 = 14;
pub const TEMPLATE_TOPIC_ACK_FEEDBACK: u16 = 15;

pub const PublicationStatus = enum(u8) {
    ok = 0,
    config_mismatch = 1,
    collision = 2,
    disabled = 3,
};

pub const SubscriptionStatus = enum(u8) {
    ok = 0,
    unknown_topic = 1,
    disabled = 2,
};

pub const StartPosition = enum(u8) {
    earliest = 0,
    latest = 1,
};

// ── Message bodies ───────────────────────────────────────────────────────────

pub const RegisterTopicPublicationMsg = extern struct {
    header: ControlMessageHeader, // template_id = 7
    local_service_id: i32,
    config: TopicConfig,
    name_length: u16,
    _pad: u16 = 0,
    // followed by name_length bytes of topic name
};

pub const TopicPublicationResponseMsg = extern struct {
    header: ControlMessageHeader, // template_id = 8
    topic_id: u64 align(1),
    leader_epoch: u64 align(1),
    leader_node_id: i16,
    status: u8,
    _pad: u8 = 0,
    effective_config: TopicConfig,
};

pub const SubscribeTopicMsg = extern struct {
    header: ControlMessageHeader, // template_id = 9
    local_service_id: i32,
    start_position: u8,
    _pad: u8 = 0,
    name_length: u16,
    // followed by name_length bytes of topic name
};

pub const TopicSubscriptionResponseMsg = extern struct {
    header: ControlMessageHeader, // template_id = 10
    topic_id: u64 align(1),
    start_index: u64 align(1),
    geometry: TopicConfig,
    status: u8,
    _pad: u8 = 0,
    queue_dir_length: u16,
    // followed by queue_dir_length bytes of absolute queue directory path
};

pub const UnregisterTopicPublicationMsg = extern struct {
    header: ControlMessageHeader, // template_id = 11
    local_service_id: i32,
    topic_id: u64 align(1),
};

pub const UnsubscribeTopicMsg = extern struct {
    header: ControlMessageHeader, // template_id = 12
    local_service_id: i32,
    topic_id: u64 align(1),
};

pub const TopicLeaderChangedMsg = extern struct {
    header: ControlMessageHeader, // template_id = 13
    topic_id: u64 align(1),
    leader_epoch: u64 align(1),
    leader_node_id: i16,
    _pad: [6]u8 = [_]u8{0} ** 6,
};

pub const TopicEndpointMsg = extern struct {
    header: ControlMessageHeader, // template_id = 14
    topic_id: u64 align(1),
    leader_node_id: i16,
    pub_stream_id: i32 align(1),
    _pad: u16 = 0,
    // followed by an endpoint host:port string (endpoint_length bytes)
    endpoint_length: u16,
};

pub const TopicAckFeedbackMsg = extern struct {
    header: ControlMessageHeader, // template_id = 15
    topic_id: u64 align(1),
    leader_epoch: u64 align(1),
    replicated_hwm: u64 align(1),
};

comptime {
    std.debug.assert(@sizeOf(RegisterTopicPublicationMsg) == header_size + 4 + 32 + 4);
    std.debug.assert(@sizeOf(TopicPublicationResponseMsg) == header_size + 8 + 8 + 2 + 1 + 1 + 32);
    std.debug.assert(@sizeOf(SubscribeTopicMsg) == header_size + 8);
    std.debug.assert(@sizeOf(TopicSubscriptionResponseMsg) == header_size + 8 + 8 + 32 + 1 + 1 + 2);
    std.debug.assert(@sizeOf(UnregisterTopicPublicationMsg) == header_size + 4 + 8);
    std.debug.assert(@sizeOf(TopicAckFeedbackMsg) == header_size + 8 + 8 + 8);
}

// ── Encode helpers ───────────────────────────────────────────────────────────

fn writeHeader(buf: []u8, comptime T: type, template_id: u16, tail_len: usize) void {
    const body_len: u16 = @intCast(@sizeOf(T) - header_size + tail_len);
    var hdr = ControlMessageHeader{ .template_id = template_id, .body_length = body_len };
    @memcpy(buf[0..header_size], std.mem.asBytes(&hdr)[0..header_size]);
}

fn writeStruct(buf: []u8, comptime T: type, value: T) void {
    var v = value;
    @memcpy(buf[0..@sizeOf(T)], std.mem.asBytes(&v)[0..@sizeOf(T)]);
}

pub fn encodeRegisterTopicPublication(buf: []u8, local_service_id: i32, config: TopicConfig, name: []const u8) usize {
    const total = @sizeOf(RegisterTopicPublicationMsg) + name.len;
    std.debug.assert(buf.len >= total);
    const msg = RegisterTopicPublicationMsg{
        .header = undefined,
        .local_service_id = local_service_id,
        .config = config,
        .name_length = @intCast(name.len),
    };
    writeStruct(buf, RegisterTopicPublicationMsg, msg);
    writeHeader(buf, RegisterTopicPublicationMsg, TEMPLATE_REGISTER_TOPIC_PUBLICATION, name.len);
    @memcpy(buf[@sizeOf(RegisterTopicPublicationMsg)..][0..name.len], name);
    return total;
}

pub fn encodeSubscribeTopic(buf: []u8, local_service_id: i32, start: StartPosition, name: []const u8) usize {
    const total = @sizeOf(SubscribeTopicMsg) + name.len;
    std.debug.assert(buf.len >= total);
    const msg = SubscribeTopicMsg{
        .header = undefined,
        .local_service_id = local_service_id,
        .start_position = @intFromEnum(start),
        .name_length = @intCast(name.len),
    };
    writeStruct(buf, SubscribeTopicMsg, msg);
    writeHeader(buf, SubscribeTopicMsg, TEMPLATE_SUBSCRIBE_TOPIC, name.len);
    @memcpy(buf[@sizeOf(SubscribeTopicMsg)..][0..name.len], name);
    return total;
}

pub fn encodePublicationResponse(
    buf: []u8,
    topic_id: u64,
    leader_node_id: i16,
    leader_epoch: u64,
    status: PublicationStatus,
    effective_config: TopicConfig,
) usize {
    const msg = TopicPublicationResponseMsg{
        .header = .{ .template_id = TEMPLATE_TOPIC_PUBLICATION_RESPONSE, .body_length = @intCast(@sizeOf(TopicPublicationResponseMsg) - header_size) },
        .topic_id = topic_id,
        .leader_epoch = leader_epoch,
        .leader_node_id = leader_node_id,
        .status = @intFromEnum(status),
        .effective_config = effective_config,
    };
    writeStruct(buf, TopicPublicationResponseMsg, msg);
    return @sizeOf(TopicPublicationResponseMsg);
}

pub fn encodeSubscriptionResponse(
    buf: []u8,
    topic_id: u64,
    status: SubscriptionStatus,
    start_index: u64,
    geometry: TopicConfig,
    queue_dir: []const u8,
) usize {
    const total = @sizeOf(TopicSubscriptionResponseMsg) + queue_dir.len;
    std.debug.assert(buf.len >= total);
    const msg = TopicSubscriptionResponseMsg{
        .header = undefined,
        .topic_id = topic_id,
        .start_index = start_index,
        .geometry = geometry,
        .status = @intFromEnum(status),
        .queue_dir_length = @intCast(queue_dir.len),
    };
    writeStruct(buf, TopicSubscriptionResponseMsg, msg);
    writeHeader(buf, TopicSubscriptionResponseMsg, TEMPLATE_TOPIC_SUBSCRIPTION_RESPONSE, queue_dir.len);
    @memcpy(buf[@sizeOf(TopicSubscriptionResponseMsg)..][0..queue_dir.len], queue_dir);
    return total;
}

pub fn encodeAckFeedback(buf: []u8, topic_id: u64, leader_epoch: u64, replicated_hwm: u64) usize {
    const msg = TopicAckFeedbackMsg{
        .header = .{ .template_id = TEMPLATE_TOPIC_ACK_FEEDBACK, .body_length = @intCast(@sizeOf(TopicAckFeedbackMsg) - header_size) },
        .topic_id = topic_id,
        .leader_epoch = leader_epoch,
        .replicated_hwm = replicated_hwm,
    };
    writeStruct(buf, TopicAckFeedbackMsg, msg);
    return @sizeOf(TopicAckFeedbackMsg);
}

pub fn encodeTopicLeaderChanged(buf: []u8, topic_id: u64, leader_node_id: i16, leader_epoch: u64) usize {
    const msg = TopicLeaderChangedMsg{
        .header = .{ .template_id = TEMPLATE_TOPIC_LEADER_CHANGED, .body_length = @intCast(@sizeOf(TopicLeaderChangedMsg) - header_size) },
        .topic_id = topic_id,
        .leader_epoch = leader_epoch,
        .leader_node_id = leader_node_id,
    };
    writeStruct(buf, TopicLeaderChangedMsg, msg);
    return @sizeOf(TopicLeaderChangedMsg);
}

// ── Decode helpers ───────────────────────────────────────────────────────────

pub fn decode(comptime T: type, bytes: []const u8) ?T {
    if (bytes.len < @sizeOf(T)) return null;
    var out: T = undefined;
    const dst: *[@sizeOf(T)]u8 = @ptrCast(&out);
    @memcpy(dst, bytes[0..@sizeOf(T)]);
    return out;
}

/// Returns the variable-length name tail of a RegisterTopicPublication frame.
pub fn registerTopicName(bytes: []const u8) []const u8 {
    const m = decode(RegisterTopicPublicationMsg, bytes) orelse return bytes[0..0];
    const start = @sizeOf(RegisterTopicPublicationMsg);
    const end = @min(start + @as(usize, m.name_length), bytes.len);
    return bytes[start..end];
}

pub fn subscribeTopicName(bytes: []const u8) []const u8 {
    const m = decode(SubscribeTopicMsg, bytes) orelse return bytes[0..0];
    const start = @sizeOf(SubscribeTopicMsg);
    const end = @min(start + @as(usize, m.name_length), bytes.len);
    return bytes[start..end];
}

pub fn subscriptionResponseQueueDir(bytes: []const u8) []const u8 {
    const m = decode(TopicSubscriptionResponseMsg, bytes) orelse return bytes[0..0];
    const start = @sizeOf(TopicSubscriptionResponseMsg);
    const end = @min(start + @as(usize, m.queue_dir_length), bytes.len);
    return bytes[start..end];
}

// ── Tests ────────────────────────────────────────────────────────────────────

const testing = std.testing;

test "register topic publication round-trips with name tail" {
    var buf: [256]u8 = undefined;
    const cfg = TopicConfig.fromName("FAST_DAILY", 7, false);
    const n = encodeRegisterTopicPublication(&buf, 42, cfg, "orders");
    const m = decode(RegisterTopicPublicationMsg, buf[0..n]).?;
    try testing.expectEqual(@as(u16, TEMPLATE_REGISTER_TOPIC_PUBLICATION), m.header.template_id);
    try testing.expectEqual(@as(i32, 42), m.local_service_id);
    try testing.expectEqual(@as(u16, 6), m.name_length);
    try testing.expectEqualStrings("orders", registerTopicName(buf[0..n]));
    try testing.expect(m.config.eqlForReplication(&cfg));
}

test "subscribe + subscription response round-trip with queue_dir tail" {
    var buf: [256]u8 = undefined;
    const n = encodeSubscribeTopic(&buf, 9, .latest, "metrics");
    const s = decode(SubscribeTopicMsg, buf[0..n]).?;
    try testing.expectEqual(@as(u8, @intFromEnum(StartPosition.latest)), s.start_position);
    try testing.expectEqualStrings("metrics", subscribeTopicName(buf[0..n]));

    const cfg = TopicConfig.fromName("FAST_DAILY", 7, false);
    var buf2: [256]u8 = undefined;
    const n2 = encodeSubscriptionResponse(&buf2, 1234, .ok, 99, cfg, "/dev/shm/topics/g/node-1/t_x");
    const r = decode(TopicSubscriptionResponseMsg, buf2[0..n2]).?;
    try testing.expectEqual(@as(u64, 1234), r.topic_id);
    try testing.expectEqual(@as(u64, 99), r.start_index);
    try testing.expectEqual(@as(u8, @intFromEnum(SubscriptionStatus.ok)), r.status);
    try testing.expectEqualStrings("/dev/shm/topics/g/node-1/t_x", subscriptionResponseQueueDir(buf2[0..n2]));
}

test "publication response + ack feedback fixed-size encode" {
    var buf: [128]u8 = undefined;
    const cfg = TopicConfig.fromName("FAST_DAILY", 7, false);
    const n = encodePublicationResponse(&buf, 7, 2, 5, .ok, cfg);
    const m = decode(TopicPublicationResponseMsg, buf[0..n]).?;
    try testing.expectEqual(@as(u64, 7), m.topic_id);
    try testing.expectEqual(@as(i16, 2), m.leader_node_id);
    try testing.expectEqual(@as(u64, 5), m.leader_epoch);

    const n2 = encodeAckFeedback(&buf, 7, 5, 4096);
    const a = decode(TopicAckFeedbackMsg, buf[0..n2]).?;
    try testing.expectEqual(@as(u64, 4096), a.replicated_hwm);
}
