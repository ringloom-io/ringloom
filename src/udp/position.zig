// SPDX-License-Identifier: Apache-2.0
const std = @import("std");

pub const Position = struct {
    pub fn pack(term_id: i32, term_offset: u32) u64 {
        return (@as(u64, @bitCast(@as(i64, term_id))) << 32) | term_offset;
    }

    pub fn unpackTermId(packed_position: u64) i32 {
        return @bitCast(@as(u32, @intCast(packed_position >> 32)));
    }

    pub fn unpackTermOffset(packed_position: u64) u32 {
        return @intCast(packed_position & 0xffff_ffff);
    }

    pub fn absolute(initial_term_id: i32, active_term_id: i32, term_offset: u32, term_length: u32) u64 {
        const term_count: u64 = @intCast(active_term_id - initial_term_id);
        return term_count * term_length + term_offset;
    }

    pub fn partitionIndex(initial_term_id: i32, active_term_id: i32) usize {
        return @intCast(@mod(active_term_id - initial_term_id, 3));
    }

    pub fn alignFrameLength(length: usize) usize {
        return (length + 31) & ~@as(usize, 31);
    }
};

test "position pack and unpack round trip" {
    const packed_position = Position.pack(42, 128);
    try std.testing.expectEqual(@as(i32, 42), Position.unpackTermId(packed_position));
    try std.testing.expectEqual(@as(u32, 128), Position.unpackTermOffset(packed_position));
}

test "absolute position and partition index" {
    try std.testing.expectEqual(@as(u64, 4096 + 64), Position.absolute(7, 8, 64, 4096));
    try std.testing.expectEqual(@as(usize, 2), Position.partitionIndex(7, 9));
    try std.testing.expectEqual(@as(usize, 64), Position.alignFrameLength(33));
}
