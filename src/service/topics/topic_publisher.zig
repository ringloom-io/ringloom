// SPDX-License-Identifier: Apache-2.0

//! TopicPublisher — producer-side API for persistent topics (spec 09).
//!
//! Wraps registration, publish to the topic leader, and replicate_once ack
//! tracking. Built on the service's existing Aeron runtime for direct
//! publications to the topic leader (IPC or UDP).

const std = @import("std");
const ringloom_common = @import("ringloom_common");
const ringloom_aeron = @import("ringloom_aeron");

const topic_types = @import("topic_types.zig");
const topic_data_header = ringloom_common.message.topic_data_header;
const data_header = ringloom_common.message.data_header;
const constants = ringloom_common.memory.constants;

const AckMode = topic_types.AckMode;
const TopicConfig = topic_types.TopicConfig;
const TopicPublishHeader = topic_data_header.TopicPublishHeader;

/// Registration status for a topic publication.
pub const RegistrationStatus = enum(u8) {
    ok = 0,
    config_mismatch = 1,
    collision = 2,
    disabled = 3,
    not_leader = 4,
    internal_error = 5,
};

/// Result of a registration response.
pub const RegistrationResult = struct {
    topic_id: u64,
    leader_node_id: u8,
    leader_epoch: u64,
    effective_config: TopicConfig,
    status: RegistrationStatus,
};

/// Result of a publish attempt.
pub const PublishResult = enum(u8) {
    ok = 0,
    not_registered = 1,
    not_connected = 2,
    back_pressured = 3,
    failed = 4,
};

/// A registered topic publisher. Created via registerPublication and used
/// for sending messages to the topic leader.
pub const TopicPublisher = struct {
    topic_id: u64,
    leader_node_id: u8,
    leader_epoch: u64,
    /// Aeron publication to the topic leader (IPC if co-located, UDP otherwise).
    leader_pub: ?ringloom_aeron.Publication = null,
    /// Highest index replicated to >=1 replica (for replicate_once acks).
    replicated_hwm: u64 = 0,

    pub fn deinit(self: *TopicPublisher) void {
        if (self.leader_pub) |*pub_| {
            pub_.close() catch {};
        }
        self.* = undefined;
    }

    /// Non-blocking publish. Builds RingLoomDataHeader(flag_topic) +
    /// TopicPublishHeader(ack_mode) + payload and offers to the leader publication.
    pub fn publish(
        self: *TopicPublisher,
        payload: []const u8,
        correlation_id: i64,
        ack_mode: AckMode,
    ) PublishResult {
        if (self.leader_pub == null) return .not_connected;
        const pub_: *ringloom_aeron.Publication = &self.leader_pub.?;

        // Build the combined frame: data header + topic publish header + payload.
        const total = data_header.RingLoomDataHeader.encoded_length +
            TopicPublishHeader.encoded_length + payload.len;

        // Use Aeron's tryClaim or offer; for simplicity, build in a stack buffer.
        var buf: [4096]u8 = undefined;
        if (total > buf.len) return .failed;

        // 1. RingLoomDataHeader with flag_topic.
        var dh = std.mem.zeroes(data_header.RingLoomDataHeader);
        @memcpy(&dh.magic, &data_header.magic_bytes);
        dh.version = data_header.header_version;
        dh.flags = constants.flag_topic;
        dh.header_length = data_header.RingLoomDataHeader.encoded_length;
        dh.source_node_id = 0; // filled by transport
        dh.source_service_id = 0; // topic publishes don't use service IDs
        dh.target_node_id = 0; // transport supplies
        dh.target_service_id = 0;
        dh.payload_length = @intCast(TopicPublishHeader.encoded_length + payload.len);

        // 2. TopicPublishHeader.
        const tph = TopicPublishHeader{
            .topic_id = self.topic_id,
            .leader_epoch = self.leader_epoch,
            .correlation_id = correlation_id,
            .ack_mode = @intFromEnum(ack_mode),
        };

        // Copy into buffer.
        const dh_bytes = @as(*const [data_header.RingLoomDataHeader.encoded_length]u8, @ptrCast(&dh));
        @memcpy(buf[0..data_header.RingLoomDataHeader.encoded_length], dh_bytes);
        const tph_bytes = @as(*const [TopicPublishHeader.encoded_length]u8, @ptrCast(&tph));
        @memcpy(buf[data_header.RingLoomDataHeader.encoded_length..][0..TopicPublishHeader.encoded_length], tph_bytes);
        @memcpy(buf[data_header.RingLoomDataHeader.encoded_length + TopicPublishHeader.encoded_length ..][0..payload.len], payload);

        // 3. Offer to Aeron.
        const result = pub_.offer(buf[0..total]);
        return switch (result) {
            .position => .ok,
            .back_pressured, .admin_action => .back_pressured,
            .not_connected => .not_connected,
            else => .failed,
        };
    }

    /// Check whether a replicate_once publish has been acknowledged.
    /// True once replicated_hwm >= the index assigned to that publish.
    pub fn isAcked(self: *const TopicPublisher, publish_index: u64) bool {
        return self.replicated_hwm >= publish_index;
    }
};
