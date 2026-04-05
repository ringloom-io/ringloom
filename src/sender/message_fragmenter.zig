const std = @import("std");
const constants = @import("../platform/constants.zig");

/// Record describing a single fragment, used for testing and by the sender event loop.
pub const FragmentRecord = struct {
    offset: usize,
    len: usize,
    flags: u8,
};

/// Compute the fragments needed for a payload of the given length.
/// Does not actually send — just computes the offset/length/flags for each fragment.
/// The caller (SenderEventLoop) handles the actual sending.
pub fn computeFragments(
    payload_len: usize,
    max_payload: usize,
    result: *std.ArrayList(FragmentRecord),
    allocator: std.mem.Allocator,
) !void {
    if (payload_len <= max_payload) {
        // Single frame — unfragmented
        try result.append(allocator, .{
            .offset = 0,
            .len = payload_len,
            .flags = constants.flag_unfragmented,
        });
        return;
    }

    var offset: usize = 0;
    var is_first: bool = true;

    while (offset < payload_len) {
        const remaining = payload_len - offset;
        const chunk_len = @min(remaining, max_payload);
        const is_last = (offset + chunk_len == payload_len);

        var flags: u8 = 0;
        if (is_first) flags |= constants.flag_begin;
        if (is_last) flags |= constants.flag_end;

        try result.append(allocator, .{
            .offset = offset,
            .len = chunk_len,
            .flags = flags,
        });

        offset += chunk_len;
        is_first = false;
    }
}

/// Returns the maximum payload size per fragment given the MTU.
pub fn maxPayloadSize() usize {
    return constants.default_mtu_length - constants.data_frame_header_length;
}

// ── Tests ─────────────────────────────────────────────────────────────

test "fragment message into correct number of frames with correct flags" {
    const allocator = std.testing.allocator;
    const max_payload: usize = 100;
    const payload_len: usize = 3 * max_payload; // exactly 3 full fragments

    var result: std.ArrayList(FragmentRecord) = .empty;
    defer result.deinit(allocator);

    try computeFragments(payload_len, max_payload, &result, allocator);

    try std.testing.expectEqual(@as(usize, 3), result.items.len);

    // First fragment: BEGIN only
    try std.testing.expectEqual(@as(usize, 0), result.items[0].offset);
    try std.testing.expectEqual(@as(usize, 100), result.items[0].len);
    try std.testing.expectEqual(constants.flag_begin, result.items[0].flags);

    // Middle fragment: no flags
    try std.testing.expectEqual(@as(usize, 100), result.items[1].offset);
    try std.testing.expectEqual(@as(usize, 100), result.items[1].len);
    try std.testing.expectEqual(@as(u8, 0), result.items[1].flags);

    // Last fragment: END only
    try std.testing.expectEqual(@as(usize, 200), result.items[2].offset);
    try std.testing.expectEqual(@as(usize, 100), result.items[2].len);
    try std.testing.expectEqual(constants.flag_end, result.items[2].flags);
}

test "single frame message gets UNFRAGMENTED flag" {
    const allocator = std.testing.allocator;
    const max_payload: usize = 100;
    const payload_len: usize = 50; // smaller than max_payload

    var result: std.ArrayList(FragmentRecord) = .empty;
    defer result.deinit(allocator);

    try computeFragments(payload_len, max_payload, &result, allocator);

    try std.testing.expectEqual(@as(usize, 1), result.items.len);
    try std.testing.expectEqual(@as(usize, 0), result.items[0].offset);
    try std.testing.expectEqual(@as(usize, 50), result.items[0].len);
    try std.testing.expectEqual(@as(u8, 0xC0), result.items[0].flags);
}

test "fragment last chunk smaller than max payload" {
    const allocator = std.testing.allocator;
    const max_payload: usize = 100;
    const payload_len: usize = 250; // 2.5x max_payload → 3 fragments

    var result: std.ArrayList(FragmentRecord) = .empty;
    defer result.deinit(allocator);

    try computeFragments(payload_len, max_payload, &result, allocator);

    try std.testing.expectEqual(@as(usize, 3), result.items.len);

    // First fragment: BEGIN
    try std.testing.expectEqual(@as(usize, 0), result.items[0].offset);
    try std.testing.expectEqual(@as(usize, 100), result.items[0].len);
    try std.testing.expectEqual(constants.flag_begin, result.items[0].flags);

    // Middle fragment: no flags
    try std.testing.expectEqual(@as(usize, 100), result.items[1].offset);
    try std.testing.expectEqual(@as(usize, 100), result.items[1].len);
    try std.testing.expectEqual(@as(u8, 0), result.items[1].flags);

    // Last fragment: END, half the max_payload
    try std.testing.expectEqual(@as(usize, 200), result.items[2].offset);
    try std.testing.expectEqual(@as(usize, 50), result.items[2].len);
    try std.testing.expectEqual(constants.flag_end, result.items[2].flags);
}

test "zero-length payload gets unfragmented flag" {
    const allocator = std.testing.allocator;
    const max_payload: usize = 100;

    var result: std.ArrayList(FragmentRecord) = .empty;
    defer result.deinit(allocator);

    try computeFragments(0, max_payload, &result, allocator);

    try std.testing.expectEqual(@as(usize, 1), result.items.len);
    try std.testing.expectEqual(@as(usize, 0), result.items[0].offset);
    try std.testing.expectEqual(@as(usize, 0), result.items[0].len);
    try std.testing.expectEqual(constants.flag_unfragmented, result.items[0].flags);
}
