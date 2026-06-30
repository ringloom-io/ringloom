// SPDX-License-Identifier: Apache-2.0

//! Byte-exact wire formats for topic publish frames and topic replication
//! envelopes (spec 04).
//!
//! A topic publish frame is an existing `RingLoomDataHeader` (with the
//! `flag_topic` bit set, `target_service_id = 0`) followed by a
//! `TopicPublishHeader` and then the application payload. The receiver loop
//! demuxes on `flag_topic` before service routing.
//!
//! A topic replication envelope wraps one whole ringloom-queue repl frame so a
//! single Aeron stream can carry many topics in both directions.

const std = @import("std");

/// Header that follows the `RingLoomDataHeader` on a topic publish frame.
/// Exactly 32 bytes.
pub const TopicPublishHeader = extern struct {
    topic_id: u64 = 0,
    /// Epoch the producer believes is current (fenced at the leader, spec 08).
    leader_epoch: u64 = 0,
    /// App-supplied; used by the leader for replicate_once ack accounting.
    correlation_id: i64 = 0,
    /// AckMode (spec 01): 0=fire_and_forget, 1=replicate_once.
    ack_mode: u8 = 0,
    flags: u8 = 0,
    _reserved: [6]u8 = [_]u8{0} ** 6,

    comptime {
        std.debug.assert(@sizeOf(TopicPublishHeader) == 32);
    }

    pub const encoded_length: usize = @sizeOf(TopicPublishHeader);

    /// Encodes the header into `buf` (must be >= 32 bytes); returns bytes written.
    pub fn encode(self: TopicPublishHeader, buf: []u8) usize {
        std.debug.assert(buf.len >= encoded_length);
        var tmp = self;
        @memcpy(buf[0..encoded_length], std.mem.asBytes(&tmp)[0..encoded_length]);
        return encoded_length;
    }

    /// Decodes a header from the front of `bytes` (borrowed copy).
    pub fn decode(bytes: []const u8) !TopicPublishHeader {
        if (bytes.len < encoded_length) return error.ShortBuffer;
        var hdr: TopicPublishHeader = undefined;
        const dst: *[encoded_length]u8 = @ptrCast(&hdr);
        @memcpy(dst, bytes[0..encoded_length]);
        return hdr;
    }
};

pub const topic_repl_magic = [4]u8{ 'R', 'T', 'P', '1' };

/// Direction of a wrapped ringloom-queue replication frame.
pub const ReplDirection = enum(u8) {
    source_to_sink = 0,
    sink_to_source = 1,

    pub fn fromU8(v: u8) ?ReplDirection {
        return switch (v) {
            0 => .source_to_sink,
            1 => .sink_to_source,
            else => null,
        };
    }
};

/// Routing envelope wrapping one whole ringloom-queue repl frame. Exactly 32
/// bytes; followed by `frame_length` bytes of the wrapped frame.
pub const TopicReplEnvelope = extern struct {
    magic: [4]u8 = topic_repl_magic,
    version: u8 = 1,
    /// ReplDirection.
    direction: u8 = 0,
    _pad: u16 = 0,
    topic_id: u64 = 0,
    /// Fencing (spec 08); stale epochs dropped.
    leader_epoch: u64 = 0,
    /// Demux target; drop if != local node id.
    target_node_id: u16 = 0,
    source_node_id: u16 = 0,
    /// Length of the wrapped ringloom-queue repl frame.
    frame_length: u32 = 0,

    comptime {
        std.debug.assert(@sizeOf(TopicReplEnvelope) == 32);
    }

    pub const encoded_length: usize = @sizeOf(TopicReplEnvelope);

    /// Builds the envelope + frame into `buf`; returns total bytes written.
    /// `buf` must be >= 32 + frame.len.
    pub fn encodeWithFrame(self: TopicReplEnvelope, buf: []u8, frame: []const u8) usize {
        std.debug.assert(buf.len >= encoded_length + frame.len);
        var env = self;
        env.frame_length = @intCast(frame.len);
        @memcpy(buf[0..encoded_length], std.mem.asBytes(&env)[0..encoded_length]);
        @memcpy(buf[encoded_length..][0..frame.len], frame);
        return encoded_length + frame.len;
    }

    /// Decodes the envelope header from the front of `bytes`.
    pub fn decode(bytes: []const u8) !TopicReplEnvelope {
        if (bytes.len < encoded_length) return error.ShortBuffer;
        var env: TopicReplEnvelope = undefined;
        const dst: *[encoded_length]u8 = @ptrCast(&env);
        @memcpy(dst, bytes[0..encoded_length]);
        if (!std.mem.eql(u8, &env.magic, &topic_repl_magic)) return error.BadMagic;
        if (env.frame_length > bytes.len - encoded_length) return error.TruncatedFrame;
        return env;
    }

    /// Returns the borrowed wrapped repl frame slice from `bytes`.
    pub fn frameSlice(self: TopicReplEnvelope, bytes: []const u8) []const u8 {
        return bytes[encoded_length..][0..self.frame_length];
    }
};

test "TopicPublishHeader is 32 bytes and round-trips" {
    try std.testing.expectEqual(@as(usize, 32), @sizeOf(TopicPublishHeader));
    var buf: [64]u8 = undefined;
    const h = TopicPublishHeader{
        .topic_id = 0xDEADBEEF,
        .leader_epoch = 5,
        .correlation_id = -7,
        .ack_mode = 1,
    };
    const n = h.encode(&buf);
    try std.testing.expectEqual(@as(usize, 32), n);
    const d = try TopicPublishHeader.decode(buf[0..n]);
    try std.testing.expectEqual(h.topic_id, d.topic_id);
    try std.testing.expectEqual(h.leader_epoch, d.leader_epoch);
    try std.testing.expectEqual(h.correlation_id, d.correlation_id);
    try std.testing.expectEqual(h.ack_mode, d.ack_mode);
}

test "TopicReplEnvelope is 32 bytes and wraps a frame exactly" {
    try std.testing.expectEqual(@as(usize, 32), @sizeOf(TopicReplEnvelope));
    var buf: [128]u8 = undefined;
    const frame = "hello-repl-frame";
    const env = TopicReplEnvelope{
        .direction = @intFromEnum(ReplDirection.source_to_sink),
        .target_node_id = 2,
        .source_node_id = 1,
        .topic_id = 99,
        .leader_epoch = 3,
    };
    const n = env.encodeWithFrame(&buf, frame);
    try std.testing.expectEqual(@as(usize, 32 + frame.len), n);
    const d = try TopicReplEnvelope.decode(buf[0..n]);
    try std.testing.expectEqual(@as(u32, frame.len), d.frame_length);
    try std.testing.expectEqual(@as(u64, 99), d.topic_id);
    try std.testing.expectEqualStrings(frame, d.frameSlice(buf[0..n]));
}

test "TopicReplEnvelope rejects bad magic" {
    var buf: [64]u8 = undefined;
    @memset(&buf, 0);
    try std.testing.expectError(error.BadMagic, TopicReplEnvelope.decode(&buf));
}
