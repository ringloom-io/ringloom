//! Flow Control Admin Message Encoding/Decoding
//!
//! Defines the wire-format structs for the three FC-related admin messages:
//!
//!   template_id = 9:  RemainingBytesUpdate
//!   template_id = 10: FlowControlSnapshot
//!   template_id = 11: ServiceCapacityUpdate
//!
//! All admin messages share the standard AdminMessageHeader framing (8 bytes).
//! These structs define the payload that follows the AdminMessageHeader.
//!
//! All structs are extern with comptime size assertions to guarantee wire
//! compatibility. Encoding and decoding use direct pointer overlay on the
//! shared-memory / network buffer (zero-copy flyweight pattern).

const std = @import("std");

// ── Template IDs ─────────────────────────────────────────────────────

pub const remaining_bytes_update_template_id: u16 = 9;
pub const flow_control_snapshot_template_id: u16 = 10;
pub const service_capacity_update_template_id: u16 = 11;

// ── RemainingBytesUpdate (template_id = 9) ───────────────────────────

/// Header for the RemainingBytesUpdate payload (after AdminMessageHeader).
pub const RemainingBytesUpdateHeader = extern struct {
    entry_count: u16,
    _reserved: u16 = 0,

    comptime {
        std.debug.assert(@sizeOf(RemainingBytesUpdateHeader) == 4);
    }
};

/// Per-service entry in a RemainingBytesUpdate message (12 bytes).
pub const RemainingBytesUpdateEntry = extern struct {
    service_id: i32,
    remaining_bytes: u32,
    capacity: u32,

    comptime {
        std.debug.assert(@sizeOf(RemainingBytesUpdateEntry) == 12);
    }
};

/// Decode a RemainingBytesUpdate payload.
/// `payload` is the slice starting after the AdminMessageHeader.
pub fn decodeRemainingBytesUpdate(payload: []const u8) ?RemainingBytesUpdate {
    if (payload.len < @sizeOf(RemainingBytesUpdateHeader))
        return null;

    const header: *const RemainingBytesUpdateHeader = @ptrCast(@alignCast(payload.ptr));
    const entry_count: usize = header.entry_count;
    const entries_offset = @sizeOf(RemainingBytesUpdateHeader);
    const entries_size = entry_count * @sizeOf(RemainingBytesUpdateEntry);

    if (payload.len < entries_offset + entries_size)
        return null;

    const entries_ptr: [*]const RemainingBytesUpdateEntry = @ptrCast(@alignCast(payload.ptr + entries_offset));

    return RemainingBytesUpdate{
        .entry_count = @intCast(entry_count),
        .entries = entries_ptr[0..entry_count],
    };
}

pub const RemainingBytesUpdate = struct {
    entry_count: u16,
    entries: []const RemainingBytesUpdateEntry,
};

/// Encode a RemainingBytesUpdate into the provided buffer.
/// Returns the number of bytes written, or null if buffer too small.
pub fn encodeRemainingBytesUpdate(
    buf: []u8,
    entries: []const RemainingBytesUpdateEntry,
) ?usize {
    const header_size = @sizeOf(RemainingBytesUpdateHeader);
    const entries_size = entries.len * @sizeOf(RemainingBytesUpdateEntry);
    const total = header_size + entries_size;

    if (buf.len < total)
        return null;

    const header: *RemainingBytesUpdateHeader = @ptrCast(@alignCast(buf.ptr));
    header.* = .{
        .entry_count = @intCast(entries.len),
    };

    const dest: [*]RemainingBytesUpdateEntry = @ptrCast(@alignCast(buf.ptr + header_size));
    for (entries, 0..) |entry, i| {
        dest[i] = entry;
    }

    return total;
}

// ── FlowControlSnapshot (template_id = 10) ──────────────────────────

/// Header for the FlowControlSnapshot payload (after AdminMessageHeader).
pub const FlowControlSnapshotHeader = extern struct {
    source_node_id: u8,
    _reserved: u8 = 0,
    entry_count: u16,

    comptime {
        std.debug.assert(@sizeOf(FlowControlSnapshotHeader) == 4);
    }
};

/// Per-service entry in a FlowControlSnapshot message (12 bytes).
/// Uses i32 for service_id to match FlowControlEntry (internal representation).
pub const FlowControlSnapshotEntry = extern struct {
    service_id: i32,
    messages_buffer_capacity: u32,
    current_remaining_bytes: u32,

    comptime {
        std.debug.assert(@sizeOf(FlowControlSnapshotEntry) == 12);
    }
};

/// Decode a FlowControlSnapshot payload.
pub fn decodeFlowControlSnapshot(payload: []const u8) ?FlowControlSnapshot {
    if (payload.len < @sizeOf(FlowControlSnapshotHeader))
        return null;

    const header: *const FlowControlSnapshotHeader = @ptrCast(@alignCast(payload.ptr));
    const entry_count: usize = header.entry_count;
    const entries_offset = @sizeOf(FlowControlSnapshotHeader);
    const entries_size = entry_count * @sizeOf(FlowControlSnapshotEntry);

    if (payload.len < entries_offset + entries_size)
        return null;

    const entries_ptr: [*]const FlowControlSnapshotEntry = @ptrCast(@alignCast(payload.ptr + entries_offset));

    return FlowControlSnapshot{
        .source_node_id = header.source_node_id,
        .entry_count = @intCast(entry_count),
        .entries = entries_ptr[0..entry_count],
    };
}

pub const FlowControlSnapshot = struct {
    source_node_id: u8,
    entry_count: u16,
    entries: []const FlowControlSnapshotEntry,
};

/// Encode a FlowControlSnapshot into the provided buffer.
pub fn encodeFlowControlSnapshot(
    buf: []u8,
    source_node_id: u8,
    entries: []const FlowControlSnapshotEntry,
) ?usize {
    const header_size = @sizeOf(FlowControlSnapshotHeader);
    const entries_size = entries.len * @sizeOf(FlowControlSnapshotEntry);
    const total = header_size + entries_size;

    if (buf.len < total)
        return null;

    const header: *FlowControlSnapshotHeader = @ptrCast(@alignCast(buf.ptr));
    header.* = .{
        .source_node_id = source_node_id,
        .entry_count = @intCast(entries.len),
    };

    const dest: [*]FlowControlSnapshotEntry = @ptrCast(@alignCast(buf.ptr + header_size));
    for (entries, 0..) |entry, i| {
        dest[i] = entry;
    }

    return total;
}

// ── ServiceCapacityUpdate (template_id = 11) ─────────────────────────

/// Payload for ServiceCapacityUpdate (after AdminMessageHeader).
/// Uses i32 for service_id to match FlowControlEntry (internal representation).
pub const ServiceCapacityUpdatePayload = extern struct {
    source_node_id: u8,
    _reserved: [3]u8 = .{ 0, 0, 0 },
    service_id: i32,
    messages_buffer_capacity: u32,
    current_remaining_bytes: u32,

    comptime {
        std.debug.assert(@sizeOf(ServiceCapacityUpdatePayload) == 16);
    }
};

/// Decode a ServiceCapacityUpdate payload.
pub fn decodeServiceCapacityUpdate(payload: []const u8) ?ServiceCapacityUpdatePayload {
    if (payload.len < @sizeOf(ServiceCapacityUpdatePayload))
        return null;

    const ptr: *const ServiceCapacityUpdatePayload = @ptrCast(@alignCast(payload.ptr));
    return ptr.*;
}

/// Encode a ServiceCapacityUpdate into the provided buffer.
pub fn encodeServiceCapacityUpdate(buf: []u8, update: ServiceCapacityUpdatePayload) ?usize {
    const size = @sizeOf(ServiceCapacityUpdatePayload);
    if (buf.len < size)
        return null;

    const dest: *ServiceCapacityUpdatePayload = @ptrCast(@alignCast(buf.ptr));
    dest.* = update;
    return size;
}

// ── Tests ────────────────────────────────────────────────────────────

const testing = std.testing;

test "RemainingBytesUpdateEntry size" {
    try testing.expectEqual(@as(usize, 12), @sizeOf(RemainingBytesUpdateEntry));
}

test "FlowControlSnapshotEntry size" {
    try testing.expectEqual(@as(usize, 12), @sizeOf(FlowControlSnapshotEntry));
}

test "ServiceCapacityUpdatePayload size" {
    try testing.expectEqual(@as(usize, 16), @sizeOf(ServiceCapacityUpdatePayload));
}

test "encode and decode RemainingBytesUpdate round-trip" {
    const entries = [_]RemainingBytesUpdateEntry{
        .{ .service_id = 1, .remaining_bytes = 50_000, .capacity = 100_000 },
        .{ .service_id = 2, .remaining_bytes = 0, .capacity = 200_000 },
    };

    var buf: [256]u8 align(4) = undefined;
    const written = encodeRemainingBytesUpdate(&buf, &entries).?;
    try testing.expectEqual(@as(usize, 4 + 2 * 12), written);

    const decoded = decodeRemainingBytesUpdate(buf[0..written]).?;
    try testing.expectEqual(@as(u16, 2), decoded.entry_count);
    try testing.expectEqual(@as(i32, 1), decoded.entries[0].service_id);
    try testing.expectEqual(@as(u32, 50_000), decoded.entries[0].remaining_bytes);
    try testing.expectEqual(@as(i32, 2), decoded.entries[1].service_id);
    try testing.expectEqual(@as(u32, 0), decoded.entries[1].remaining_bytes);
}

test "encode and decode FlowControlSnapshot round-trip" {
    const entries = [_]FlowControlSnapshotEntry{
        .{ .service_id = 10, .messages_buffer_capacity = 1_000_000, .current_remaining_bytes = 500_000 },
    };

    var buf: [256]u8 align(4) = undefined;
    const written = encodeFlowControlSnapshot(&buf, 3, &entries).?;
    try testing.expectEqual(@as(usize, 4 + 12), written);

    const decoded = decodeFlowControlSnapshot(buf[0..written]).?;
    try testing.expectEqual(@as(u8, 3), decoded.source_node_id);
    try testing.expectEqual(@as(u16, 1), decoded.entry_count);
    try testing.expectEqual(@as(i32, 10), decoded.entries[0].service_id);
    try testing.expectEqual(@as(u32, 1_000_000), decoded.entries[0].messages_buffer_capacity);
}

test "encode and decode ServiceCapacityUpdate round-trip" {
    const update = ServiceCapacityUpdatePayload{
        .source_node_id = 5,
        .service_id = 42,
        .messages_buffer_capacity = 2_000_000,
        .current_remaining_bytes = 1_800_000,
    };

    var buf: [32]u8 align(4) = undefined;
    const written = encodeServiceCapacityUpdate(&buf, update).?;
    try testing.expectEqual(@as(usize, 16), written);

    const decoded = decodeServiceCapacityUpdate(buf[0..written]).?;
    try testing.expectEqual(@as(u8, 5), decoded.source_node_id);
    try testing.expectEqual(@as(i32, 42), decoded.service_id);
    try testing.expectEqual(@as(u32, 2_000_000), decoded.messages_buffer_capacity);
    try testing.expectEqual(@as(u32, 1_800_000), decoded.current_remaining_bytes);
}

test "decode too-short payload returns null" {
    const short: [2]u8 = .{ 0, 0 };
    try testing.expect(decodeRemainingBytesUpdate(&short) == null);
    try testing.expect(decodeFlowControlSnapshot(&short) == null);
    try testing.expect(decodeServiceCapacityUpdate(&short) == null);
}

test "encode returns null when buffer too small" {
    var small_buf: [2]u8 align(4) = undefined;
    const entries = [_]RemainingBytesUpdateEntry{
        .{ .service_id = 1, .remaining_bytes = 100, .capacity = 200 },
    };
    try testing.expect(encodeRemainingBytesUpdate(&small_buf, &entries) == null);
}
