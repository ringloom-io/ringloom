// SPDX-License-Identifier: Apache-2.0

//! TopicPublisher — producer-side API for persistent topics (spec 09).
//!
//! Wraps registration, publish to the topic leader, and replicate_once ack
//! tracking. Built on the service's existing Aeron runtime for direct
//! publications to the topic leader (IPC or UDP).

const std = @import("std");
const ringloom_common = @import("ringloom_common");
const ringloom_aeron = @import("ringloom_aeron");

const topic_data_header = ringloom_common.message.topic_data_header;
const data_header = ringloom_common.message.data_header;
const constants = ringloom_common.memory.constants;

const AckMode = ringloom_common.topics.AckMode;
const TopicConfig = ringloom_common.topics.TopicConfig;
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
    /// Identifies this publisher to the leader, which tracks replicate_once acks
    /// per publisher (see `newPublisherId`). Must be unique across the cluster.
    publisher_id: u64,
    leader_node_id: u8,
    leader_epoch: u64,
    /// Aeron publication to the topic leader (IPC if co-located, UDP otherwise).
    leader_pub: ?ringloom_aeron.ExclusivePublication = null,
    /// Highest queue index replicated to >=1 replica (for diagnostics). Advanced
    /// from TopicAckFeedback (template 15) on the control thread.
    replicated_hwm: u64 = 0,
    /// Highest of this publisher's sequence tokens that the leader reports as
    /// replicated to >=1 replica (or appended, single-node). Written by the
    /// control thread, read by the publishing thread.
    acked_seq: std.atomic.Value(u64) = .init(0),
    /// This publisher's publish sequence: the token of the most recent
    /// successful publish. Sent in the frame as `publish_seq` and returned to
    /// the caller as the `out_index` token.
    next_token: u64 = 0,

    /// Returns a fresh, non-zero publisher id. Mixes wall-clock and monotonic
    /// time, a process-wide counter and a per-call address, which makes
    /// collisions across processes and hosts vanishingly unlikely.
    pub fn newPublisherId() u64 {
        const S = struct {
            var counter: std.atomic.Value(u64) = .init(0);
        };
        var seed: [4]u64 = undefined;
        seed[0] = @bitCast(ringloom_common.platform.Clock.epochMillis());
        seed[1] = @bitCast(ringloom_common.platform.Clock.monotonicNanos());
        seed[2] = S.counter.fetchAdd(1, .monotonic);
        seed[3] = @intFromPtr(&seed);
        const id = std.hash.Wyhash.hash(0x5249_4e47_4c4f_4f4d, std.mem.asBytes(&seed));
        return if (id == 0) 1 else id;
    }

    pub fn deinit(self: *TopicPublisher) void {
        // `leader_pub` is borrowed from the owning ServiceAeronRuntime, which
        // closes it at runtime shutdown; do not close it here.
        self.* = undefined;
    }

    /// Non-blocking publish. Builds RingLoomDataHeader(flag_topic) +
    /// TopicPublishHeader(ack_mode) + payload and offers to the leader publication.
    /// On success assigns the next per-topic sequence token, available via
    /// `lastAssignedToken()` so the C ABI can return it as `out_index`.
    pub fn publish(
        self: *TopicPublisher,
        payload: []const u8,
        correlation_id: i64,
        ack_mode: AckMode,
    ) PublishResult {
        if (self.leader_pub == null) return .not_connected;
        const pub_: *ringloom_aeron.ExclusivePublication = &self.leader_pub.?;

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
        // The token is committed only if the offer succeeds, so a retried
        // publish after back-pressure reuses the same sequence.
        const seq = self.next_token + 1;
        const tph = TopicPublishHeader{
            .topic_id = self.topic_id,
            .leader_epoch = self.leader_epoch,
            .correlation_id = correlation_id,
            .ack_mode = @backingInt(ack_mode),
            .publisher_id = self.publisher_id,
            .publish_seq = seq,
        };

        // Copy into buffer.
        const dh_bytes = @as(*const [data_header.RingLoomDataHeader.encoded_length]u8, @ptrCast(&dh));
        @memcpy(buf[0..data_header.RingLoomDataHeader.encoded_length], dh_bytes);
        const tph_bytes = @as(*const [TopicPublishHeader.encoded_length]u8, @ptrCast(&tph));
        @memcpy(buf[data_header.RingLoomDataHeader.encoded_length..][0..TopicPublishHeader.encoded_length], tph_bytes);
        @memcpy(buf[data_header.RingLoomDataHeader.encoded_length + TopicPublishHeader.encoded_length ..][0..payload.len], payload);

        // 3. Offer to Aeron.
        const result = pub_.offer(buf[0..total]);
        switch (result) {
            .position => {
                self.next_token = seq;
                return .ok;
            },
            .back_pressured, .admin_action => return .back_pressured,
            .not_connected => return .not_connected,
            else => return .failed,
        }
    }

    /// The sequence token assigned by the most recent successful `publish`.
    /// The C ABI returns this as `out_index` for `replicate_once`.
    pub fn lastAssignedToken(self: *const TopicPublisher) u64 {
        return self.next_token;
    }

    /// Check whether a `replicate_once` publish has been acknowledged.
    /// `token` is the per-publish sequence returned in `out_index`; true once
    /// the leader reports this publisher's acked sequence at or past it.
    pub fn isAcked(self: *const TopicPublisher, token: u64) bool {
        return self.ackedSeq() >= token;
    }

    /// Highest token acknowledged so far.
    pub fn ackedSeq(self: *const TopicPublisher) u64 {
        return self.acked_seq.load(.acquire);
    }

    /// Control-thread: apply ack feedback (template 15). Ignores feedback for
    /// other publishers and from stale epochs; `acked_seq` only moves forward.
    pub fn applyAckFeedback(self: *TopicPublisher, leader_epoch: u64, publisher_id: u64, replicated_hwm: u64, acked_seq: u64) void {
        if (publisher_id != self.publisher_id) return;
        if (leader_epoch < self.leader_epoch) return; // stale feedback
        if (replicated_hwm > self.replicated_hwm) self.replicated_hwm = replicated_hwm;
        if (acked_seq > self.acked_seq.load(.monotonic)) self.acked_seq.store(acked_seq, .release);
    }

    /// Control-thread: apply a leader change (template 13). Re-targets the
    /// publisher's leader identity; the Aeron publication is re-opened lazily by
    /// the owning runtime when `leader_node_id` changes.
    pub fn applyLeaderChanged(self: *TopicPublisher, leader_node_id: u8, leader_epoch: u64) void {
        if (leader_epoch < self.leader_epoch) return; // stale
        self.leader_node_id = leader_node_id;
        self.leader_epoch = leader_epoch;
    }
};

// ── Tests ────────────────────────────────────────────────────────────────────

test "ack feedback for another publisher on the same topic is ignored" {
    var p = TopicPublisher{ .topic_id = 7, .publisher_id = 0xA, .leader_node_id = 1, .leader_epoch = 2 };
    p.next_token = 3;

    p.applyAckFeedback(2, 0xB, 500, 3); // someone else's acks
    try std.testing.expect(!p.isAcked(1));

    p.applyAckFeedback(2, 0xA, 500, 2);
    try std.testing.expect(p.isAcked(2));
    try std.testing.expect(!p.isAcked(3));

    p.applyAckFeedback(1, 0xA, 900, 3); // stale epoch
    try std.testing.expect(!p.isAcked(3));

    p.applyAckFeedback(2, 0xA, 400, 1); // never moves backwards
    try std.testing.expect(p.isAcked(2));
}

test "newPublisherId is non-zero and distinct" {
    var seen = std.AutoHashMap(u64, void).init(std.testing.allocator);
    defer seen.deinit();
    for (0..1000) |_| {
        const id = TopicPublisher.newPublisherId();
        try std.testing.expect(id != 0);
        try std.testing.expect(!seen.contains(id));
        try seen.put(id, {});
    }
}
