// SPDX-License-Identifier: Apache-2.0
//! Local operator requests only; never forwarded to the topic leader.
const std = @import("std");
pub const TEMPLATE_REBUILD_KV: u16 = 25;
pub const rebuild_size = 12;
pub fn encodeRebuild(out: *[rebuild_size]u8, store_id: u64) void {
    std.mem.writeInt(u16, out[0..2], TEMPLATE_REBUILD_KV, .little);
    std.mem.writeInt(u16, out[2..4], 8, .little);
    std.mem.writeInt(u64, out[4..12], store_id, .little);
}
pub fn decodeRebuild(bytes: []const u8) ?u64 {
    if (bytes.len != rebuild_size) return null;
    if (std.mem.readInt(u16, bytes[0..2], .little) != TEMPLATE_REBUILD_KV or
        std.mem.readInt(u16, bytes[2..4], .little) != 8) return null;
    return std.mem.readInt(u64, bytes[4..12], .little);
}
test "rebuild rejects malformed messages" {
    var bytes: [rebuild_size]u8 = undefined;
    encodeRebuild(&bytes, 123);
    try std.testing.expectEqual(@as(?u64, 123), decodeRebuild(&bytes));
    try std.testing.expectEqual(@as(?u64, null), decodeRebuild(bytes[0..11]));
    bytes[2] = 7;
    try std.testing.expectEqual(@as(?u64, null), decodeRebuild(&bytes));
}
