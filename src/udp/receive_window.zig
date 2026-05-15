// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const protocol = @import("protocol.zig");
const Position = @import("position.zig").Position;

const Slot = struct {
    position: u64 = 0,
    length: u16 = 0,
    committed: bool = false,
};

pub const ReceiveWindow = struct {
    allocator: std.mem.Allocator,
    data: []align(32) u8,
    slots: []Slot,
    initial_term_id: i32,
    term_length: u32,
    consumed_position: u64,
    rebuild_position: u64,
    high_water_mark: u64,

    pub fn init(
        allocator: std.mem.Allocator,
        initial_term_id: i32,
        term_length: u32,
        window_length: u32,
    ) !ReceiveWindow {
        if (window_length == 0 or !std.math.isPowerOfTwo(window_length)) return error.WindowLengthNotPowerOfTwo;
        const data = try allocator.alignedAlloc(u8, .@"32", window_length);
        errdefer allocator.free(data);
        @memset(data, 0);
        const slot_count = window_length / 32;
        const slots = try allocator.alloc(Slot, slot_count);
        @memset(slots, .{});
        return .{
            .allocator = allocator,
            .data = data,
            .slots = slots,
            .initial_term_id = initial_term_id,
            .term_length = term_length,
            .consumed_position = 0,
            .rebuild_position = 0,
            .high_water_mark = 0,
        };
    }

    pub fn deinit(self: *ReceiveWindow) void {
        self.allocator.free(self.data);
        self.allocator.free(self.slots);
        self.* = undefined;
    }

    pub fn insert(self: *ReceiveWindow, header: protocol.DataHeader, payload: []const u8) !void {
        const frame_len = protocol.DataHeader.encoded_length + payload.len;
        const aligned_len = Position.alignFrameLength(frame_len);
        if (aligned_len > self.data.len) return error.FrameTooLarge;
        const pos = Position.absolute(self.initial_term_id, header.term_id, header.term_offset, self.term_length);
        if (pos + aligned_len < self.consumed_position) return error.StaleFrame;
        const data_offset: usize = @intCast(pos % self.data.len);
        if (data_offset + aligned_len > self.data.len) return error.FrameWrapUnsupported;

        @memcpy(self.data[data_offset..][0..protocol.DataHeader.encoded_length], std.mem.asBytes(&header));
        if (payload.len > 0) {
            @memcpy(self.data[data_offset + protocol.DataHeader.encoded_length ..][0..payload.len], payload);
        }

        const slot = &self.slots[(data_offset / 32) % self.slots.len];
        slot.* = .{
            .position = pos,
            .length = @intCast(frame_len),
            .committed = true,
        };
        self.high_water_mark = @max(self.high_water_mark, pos + aligned_len);
    }

    pub fn nextContiguous(self: *const ReceiveWindow) ?struct { header: *const protocol.DataHeader, payload: []const u8 } {
        const data_offset: usize = @intCast(self.rebuild_position % self.data.len);
        const slot = &self.slots[(data_offset / 32) % self.slots.len];
        if (!slot.committed or slot.position != self.rebuild_position) return null;
        const header: *const protocol.DataHeader = @ptrCast(@alignCast(self.data[data_offset..].ptr));
        return .{
            .header = header,
            .payload = self.data[data_offset + protocol.DataHeader.encoded_length ..][0 .. slot.length - protocol.DataHeader.encoded_length],
        };
    }

    pub fn advanceRebuild(self: *ReceiveWindow) bool {
        const data_offset: usize = @intCast(self.rebuild_position % self.data.len);
        const slot = &self.slots[(data_offset / 32) % self.slots.len];
        if (!slot.committed or slot.position != self.rebuild_position) return false;
        self.rebuild_position += Position.alignFrameLength(slot.length);
        self.consumed_position = self.rebuild_position;
        return true;
    }
};

test "receive window inserts and rebuilds contiguous unfragmented frame" {
    var window = try ReceiveWindow.init(std.testing.allocator, 1, 1024, 1024);
    defer window.deinit();
    const header = protocol.DataHeader.init(.{
        .session_id = 1,
        .stream_id = 2,
        .term_id = 1,
        .term_offset = 0,
        .message_length = 5,
        .payload_length = 5,
        .source_node_id = 1,
        .target_node_id = 2,
        .route_flags = 0xc0,
        .source_service_id = 10,
        .target_service_id = 20,
        .template_id = 42,
    });
    try window.insert(header, "hello");
    const next = window.nextContiguous().?;
    try std.testing.expectEqualStrings("hello", next.payload);
    try std.testing.expect(window.advanceRebuild());
    try std.testing.expectEqual(@as(u64, 96), window.consumed_position);
}
