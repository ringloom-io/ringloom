//! TCP connection handshake protocol.
//!
//! Every new TCP connection must complete an application-level handshake
//! before message I/O begins. The handshake validates node identity, group
//! membership, and detects stale connections from previous broker instances.

const std = @import("std");

/// 24-byte handshake frame sent at the start of every TCP connection.
///
/// Wire layout:
///   Offset  Size  Type   Field
///   0       4     u32    magic (0x474E4952, "RING" little-endian)
///   4       1     u8     protocol_version
///   5       1     u8     source_node_id
///   6       1     u8     target_node_id
///   7       1     u8     direction
///   8       8     u64    session_epoch
///   16      4     u32    group_name_hash (FNV-1a)
///   20      4     u32    reserved (zero)
pub const HandshakeFrame = extern struct {
    magic: u32 = magic_value,
    protocol_version: u8 = protocol_version_current,
    source_node_id: u8,
    target_node_id: u8,
    direction: Direction,
    session_epoch: u64 align(4),
    group_name_hash: u32,
    reserved: u32 = 0,

    pub const magic_value: u32 = 0x474E4952;
    pub const protocol_version_current: u8 = 1;
    pub const size: usize = @sizeOf(HandshakeFrame);

    pub const Direction = enum(u8) {
        outbound = 0x01,
        inbound = 0x02,
    };

    comptime {
        std.debug.assert(@sizeOf(HandshakeFrame) == 24);
    }

    pub fn toBytes(self: *const HandshakeFrame) *const [size]u8 {
        return @ptrCast(self);
    }

    pub fn fromBytes(bytes: *const [size]u8) *const HandshakeFrame {
        return @ptrCast(@alignCast(bytes));
    }

    /// Compute FNV-1a hash of the cluster group name.
    pub fn hashGroupName(name: []const u8) u32 {
        var hash: u32 = 0x811c9dc5; // FNV offset basis
        for (name) |byte| {
            hash ^= byte;
            hash *%= 0x01000193; // FNV prime
        }
        return hash;
    }

    /// Validate a received handshake frame.
    pub fn validate(
        frame: HandshakeFrame,
        local_node_id: u8,
        expected_group_hash: u32,
    ) !void {
        if (frame.magic != magic_value) {
            return error.InvalidMagic;
        }
        if (frame.protocol_version != protocol_version_current) {
            return error.UnsupportedProtocolVersion;
        }
        if (frame.target_node_id != local_node_id) {
            return error.WrongTargetNode;
        }
        if (frame.source_node_id == local_node_id) {
            return error.SelfConnection;
        }
        if (frame.group_name_hash != expected_group_hash) {
            return error.GroupMismatch;
        }
        if (frame.reserved != 0) {
            return error.InvalidReservedField;
        }
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "HandshakeFrame size is 24 bytes" {
    try testing.expectEqual(@as(usize, 24), @sizeOf(HandshakeFrame));
}

test "HandshakeFrame field offsets" {
    try testing.expectEqual(@as(usize, 0), @offsetOf(HandshakeFrame, "magic"));
    try testing.expectEqual(@as(usize, 4), @offsetOf(HandshakeFrame, "protocol_version"));
    try testing.expectEqual(@as(usize, 5), @offsetOf(HandshakeFrame, "source_node_id"));
    try testing.expectEqual(@as(usize, 6), @offsetOf(HandshakeFrame, "target_node_id"));
    try testing.expectEqual(@as(usize, 7), @offsetOf(HandshakeFrame, "direction"));
    try testing.expectEqual(@as(usize, 8), @offsetOf(HandshakeFrame, "session_epoch"));
    try testing.expectEqual(@as(usize, 16), @offsetOf(HandshakeFrame, "group_name_hash"));
    try testing.expectEqual(@as(usize, 20), @offsetOf(HandshakeFrame, "reserved"));
}

test "HandshakeFrame roundtrip" {
    const frame = HandshakeFrame{
        .source_node_id = 1,
        .target_node_id = 2,
        .direction = .outbound,
        .session_epoch = 1_700_000_000_000_000_000,
        .group_name_hash = HandshakeFrame.hashGroupName("test-cluster"),
    };

    const bytes = frame.toBytes();
    const decoded = HandshakeFrame.fromBytes(bytes).*;

    try testing.expectEqual(frame.magic, decoded.magic);
    try testing.expectEqual(frame.source_node_id, decoded.source_node_id);
    try testing.expectEqual(frame.target_node_id, decoded.target_node_id);
    try testing.expectEqual(frame.session_epoch, decoded.session_epoch);
    try testing.expectEqual(frame.group_name_hash, decoded.group_name_hash);
}

test "HandshakeFrame validation rejects wrong magic" {
    var frame = HandshakeFrame{
        .source_node_id = 1,
        .target_node_id = 2,
        .direction = .outbound,
        .session_epoch = 1_000,
        .group_name_hash = 0,
    };
    frame.magic = 0xDEADBEEF;

    try testing.expectError(error.InvalidMagic, HandshakeFrame.validate(frame, 2, 0));
}

test "HandshakeFrame validation rejects self-connection" {
    const frame = HandshakeFrame{
        .source_node_id = 1,
        .target_node_id = 1,
        .direction = .outbound,
        .session_epoch = 1_000,
        .group_name_hash = 0,
    };
    try testing.expectError(error.SelfConnection, HandshakeFrame.validate(frame, 1, 0));
}

test "HandshakeFrame validation rejects wrong target" {
    const frame = HandshakeFrame{
        .source_node_id = 1,
        .target_node_id = 3,
        .direction = .outbound,
        .session_epoch = 1_000,
        .group_name_hash = 0,
    };
    try testing.expectError(error.WrongTargetNode, HandshakeFrame.validate(frame, 2, 0));
}

test "HandshakeFrame validation rejects group mismatch" {
    const frame = HandshakeFrame{
        .source_node_id = 1,
        .target_node_id = 2,
        .direction = .outbound,
        .session_epoch = 1_000,
        .group_name_hash = HandshakeFrame.hashGroupName("cluster-a"),
    };
    const expected_hash = HandshakeFrame.hashGroupName("cluster-b");
    try testing.expectError(error.GroupMismatch, HandshakeFrame.validate(frame, 2, expected_hash));
}

test "HandshakeFrame validation accepts valid frame" {
    const group_hash = HandshakeFrame.hashGroupName("test-cluster");
    const frame = HandshakeFrame{
        .source_node_id = 1,
        .target_node_id = 2,
        .direction = .outbound,
        .session_epoch = 1_000,
        .group_name_hash = group_hash,
    };
    try HandshakeFrame.validate(frame, 2, group_hash);
}

test "FNV-1a hash deterministic" {
    const hash1 = HandshakeFrame.hashGroupName("cluster-a");
    const hash2 = HandshakeFrame.hashGroupName("cluster-b");
    try testing.expect(hash1 != hash2);

    const hash3 = HandshakeFrame.hashGroupName("cluster-a");
    try testing.expectEqual(hash1, hash3);
}
