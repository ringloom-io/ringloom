// SPDX-License-Identifier: Apache-2.0
//! Broker v2 per-destination send-buffer directory.
//!
//! The directory is a shared-memory flyweight over fixed pre-sliced ring-buffer
//! slots. Entries are keyed by remote destination service so one blocked
//! destination cannot prevent unrelated destinations from being drained.

const std = @import("std");
const constants = @import("constants.zig");

pub const SendBufferEntryState = enum(u8) {
    free = 0,
    provisioning = 1,
    active = 2,
    draining = 3,
    closed = 4,
};

pub const SendBufferPressureState = enum(u8) {
    unknown = 0,
    normal = 1,
    flow_blocked = 2,
    congested = 3,
    closed = 4,
};

pub const SendBufferHandle = extern struct {
    index: u32,
    generation: u16,
    target_node_id: i16,
    target_service_id: i32,
    stream_id: u32,
    ring_offset: u64,
    ring_capacity: u32,
    max_message_length: u32,
};

pub const SendBufferDirectoryHeader = extern struct {
    version: u32,
    entry_count: u32,
    entry_size: u32,
    _reserved0: u32 = 0,
    send_region_offset: u64,
    send_region_length: u64,
    destination_ring_capacity: u32,
    _reserved1: u32 = 0,
    _padding: [24]u8 = [_]u8{0} ** 24,

    pub const encoded_length: usize = @sizeOf(SendBufferDirectoryHeader);

    comptime {
        std.debug.assert(@sizeOf(SendBufferDirectoryHeader) == 64);
    }
};

pub const SendBufferEntry = extern struct {
    state: u8 = @intFromEnum(SendBufferEntryState.free),
    pressure_state: u8 = @intFromEnum(SendBufferPressureState.unknown),
    generation: u16 = 1,
    target_node_id: i16 = 0,
    _reserved0: u16 = 0,
    target_service_id: i32 = 0,
    stream_id: u32 = 0,
    ring_offset: u64 = 0,
    ring_capacity: u32 = 0,
    max_message_length: u32 = 0,
    producer_count: u32 = 0,
    _reserved1: u32 = 0,
    last_activity_ns: i64 = 0,
    bytes_pending: u64 = 0,
    messages_pending: u64 = 0,
    lifetime_messages_written: u64 = 0,
    lifetime_messages_dropped: u64 = 0,
    _padding: [48]u8 = [_]u8{0} ** 48,

    comptime {
        std.debug.assert(@sizeOf(SendBufferEntry) == constants.send_buffer_entry_length);
    }

    pub fn loadState(self: *const SendBufferEntry) SendBufferEntryState {
        return @enumFromInt(@atomicLoad(u8, self.statePtr(), .acquire));
    }

    pub fn storeState(self: *SendBufferEntry, state: SendBufferEntryState) void {
        @atomicStore(u8, self.statePtr(), @intFromEnum(state), .release);
    }

    pub fn compareExchangeState(
        self: *SendBufferEntry,
        expected: SendBufferEntryState,
        desired: SendBufferEntryState,
    ) bool {
        return @cmpxchgStrong(
            u8,
            self.statePtr(),
            @intFromEnum(expected),
            @intFromEnum(desired),
            .acq_rel,
            .acquire,
        ) == null;
    }

    pub fn loadPressureState(self: *const SendBufferEntry) SendBufferPressureState {
        return @enumFromInt(@atomicLoad(u8, self.pressureStatePtr(), .acquire));
    }

    pub fn storePressureState(self: *SendBufferEntry, state: SendBufferPressureState) void {
        @atomicStore(u8, self.pressureStatePtr(), @intFromEnum(state), .release);
    }

    pub fn addPending(self: *SendBufferEntry, bytes: u64) void {
        _ = @atomicRmw(u64, self.bytesPendingPtr(), .Add, bytes, .acq_rel);
        _ = @atomicRmw(u64, self.messagesPendingPtr(), .Add, 1, .acq_rel);
        _ = @atomicRmw(u64, self.lifetimeMessagesWrittenPtr(), .Add, 1, .acq_rel);
    }

    pub fn subtractPending(self: *SendBufferEntry, bytes: u64) void {
        subtractSaturating(self.bytesPendingPtr(), bytes);
        subtractSaturating(self.messagesPendingPtr(), 1);
    }

    pub fn addDropped(self: *SendBufferEntry) void {
        _ = @atomicRmw(u64, self.lifetimeMessagesDroppedPtr(), .Add, 1, .acq_rel);
    }

    fn statePtr(self: *const SendBufferEntry) *volatile u8 {
        return @ptrCast(@constCast(&self.state));
    }

    fn pressureStatePtr(self: *const SendBufferEntry) *volatile u8 {
        return @ptrCast(@constCast(&self.pressure_state));
    }

    fn bytesPendingPtr(self: *const SendBufferEntry) *volatile u64 {
        return @ptrCast(@constCast(&self.bytes_pending));
    }

    fn messagesPendingPtr(self: *const SendBufferEntry) *volatile u64 {
        return @ptrCast(@constCast(&self.messages_pending));
    }

    fn lifetimeMessagesWrittenPtr(self: *const SendBufferEntry) *volatile u64 {
        return @ptrCast(@constCast(&self.lifetime_messages_written));
    }

    fn lifetimeMessagesDroppedPtr(self: *const SendBufferEntry) *volatile u64 {
        return @ptrCast(@constCast(&self.lifetime_messages_dropped));
    }
};

pub const SendBufferDirectory = struct {
    bytes: []u8,
    header: *SendBufferDirectoryHeader,
    entries: []SendBufferEntry,

    pub const Error = error{
        DirectoryTooSmall,
        InvalidDirectoryVersion,
        InvalidEntrySize,
        InvalidEntryCount,
        InvalidSendRegion,
        DirectoryFull,
        InvalidHandle,
        EntryNotClosed,
        RingSliceOutOfBounds,
    };

    pub fn regionSize(entry_count: u32) usize {
        return constants.alignUp(
            SendBufferDirectoryHeader.encoded_length +
                @as(usize, entry_count) * constants.send_buffer_entry_length,
            constants.cache_line_pad,
        );
    }

    pub fn slotLength(ring_capacity: usize) usize {
        return constants.alignUp(
            ring_capacity + constants.ring_buffer_trailer_length,
            constants.cache_line_pad,
        );
    }

    pub fn initNew(
        bytes: []u8,
        entry_count: u32,
        send_region_offset: usize,
        send_region_length: usize,
        destination_ring_capacity: usize,
    ) Error!SendBufferDirectory {
        if (entry_count == 0) return Error.InvalidEntryCount;
        const required = regionSize(entry_count);
        if (bytes.len < required) return Error.DirectoryTooSmall;
        if (!constants.isPowerOfTwo(destination_ring_capacity)) return Error.InvalidSendRegion;

        const slot_len = slotLength(destination_ring_capacity);
        if (send_region_length < slot_len * @as(usize, entry_count)) {
            return Error.InvalidSendRegion;
        }

        @memset(bytes[0..required], 0);

        const header: *SendBufferDirectoryHeader = @ptrCast(@alignCast(bytes.ptr));
        header.* = .{
            .version = constants.metadata_version_v2,
            .entry_count = entry_count,
            .entry_size = constants.send_buffer_entry_length,
            .send_region_offset = @intCast(send_region_offset),
            .send_region_length = @intCast(send_region_length),
            .destination_ring_capacity = @intCast(destination_ring_capacity),
        };

        const entries = entriesSlice(bytes, entry_count);
        for (entries, 0..) |*entry, i| {
            entry.* = .{
                .generation = 1,
                .ring_offset = @intCast(send_region_offset + i * slot_len),
                .ring_capacity = @intCast(destination_ring_capacity),
                .max_message_length = @intCast(destination_ring_capacity / 8),
            };
        }

        return .{
            .bytes = bytes[0..required],
            .header = header,
            .entries = entries,
        };
    }

    pub fn initExisting(bytes: []u8) Error!SendBufferDirectory {
        if (bytes.len < SendBufferDirectoryHeader.encoded_length) return Error.DirectoryTooSmall;
        const header: *SendBufferDirectoryHeader = @ptrCast(@alignCast(bytes.ptr));
        if (header.version != constants.metadata_version_v2) return Error.InvalidDirectoryVersion;
        if (header.entry_size != constants.send_buffer_entry_length) return Error.InvalidEntrySize;
        if (header.entry_count == 0) return Error.InvalidEntryCount;
        const required = regionSize(header.entry_count);
        if (bytes.len < required) return Error.DirectoryTooSmall;

        return .{
            .bytes = bytes[0..required],
            .header = header,
            .entries = entriesSlice(bytes, header.entry_count),
        };
    }

    pub fn findByDestination(
        self: *const SendBufferDirectory,
        target_node_id: i16,
        target_service_id: i32,
    ) ?SendBufferHandle {
        for (self.entries, 0..) |*entry, i| {
            const state = entry.loadState();
            if (state != .active and state != .draining) continue;
            if (entry.target_node_id == target_node_id and
                entry.target_service_id == target_service_id)
            {
                return handleForEntry(@intCast(i), entry);
            }
        }
        return null;
    }

    pub fn findOrAllocateDestination(
        self: *SendBufferDirectory,
        mapped_bytes: []u8,
        target_node_id: i16,
        target_service_id: i32,
    ) Error!SendBufferHandle {
        if (self.findByDestination(target_node_id, target_service_id)) |handle| {
            return handle;
        }

        for (self.entries, 0..) |*entry, i| {
            if (!entry.compareExchangeState(.free, .provisioning)) continue;

            const ring = try self.ringSliceForEntry(mapped_bytes, entry);
            @memset(ring, 0);

            entry.target_node_id = target_node_id;
            entry.target_service_id = target_service_id;
            entry.stream_id = streamId(target_node_id, target_service_id, entry.generation);
            entry.producer_count = 0;
            entry.last_activity_ns = 0;
            entry.bytes_pending = 0;
            entry.messages_pending = 0;
            entry.lifetime_messages_written = 0;
            entry.lifetime_messages_dropped = 0;
            entry.storePressureState(.normal);
            entry.storeState(.active);
            return handleForEntry(@intCast(i), entry);
        }

        return Error.DirectoryFull;
    }

    pub fn validateHandle(self: *const SendBufferDirectory, handle: SendBufferHandle) Error!*SendBufferEntry {
        if (handle.index >= self.entries.len) return Error.InvalidHandle;
        const entry = &self.entries[handle.index];
        if (entry.generation != handle.generation) return Error.InvalidHandle;
        if (entry.loadState() != .active) return Error.InvalidHandle;
        if (entry.target_node_id != handle.target_node_id or
            entry.target_service_id != handle.target_service_id)
        {
            return Error.InvalidHandle;
        }
        return entry;
    }

    pub fn ringSliceForHandle(
        self: *const SendBufferDirectory,
        mapped_bytes: []u8,
        handle: SendBufferHandle,
    ) Error![]align(constants.ring_buffer_alignment) u8 {
        const entry = try self.validateHandle(handle);
        return self.ringSliceForEntry(mapped_bytes, entry);
    }

    pub fn ringSliceForEntry(
        self: *const SendBufferDirectory,
        mapped_bytes: []u8,
        entry: *const SendBufferEntry,
    ) Error![]align(constants.ring_buffer_alignment) u8 {
        _ = self;
        const offset: usize = @intCast(entry.ring_offset);
        const len = slotLength(entry.ring_capacity);
        if (offset + len > mapped_bytes.len) return Error.RingSliceOutOfBounds;
        return @alignCast(mapped_bytes[offset..][0..len]);
    }

    pub fn markDraining(self: *SendBufferDirectory, handle: SendBufferHandle) Error!void {
        const entry = try self.validateHandle(handle);
        entry.storeState(.draining);
    }

    pub fn markClosed(self: *SendBufferDirectory, handle: SendBufferHandle) Error!void {
        if (handle.index >= self.entries.len) return Error.InvalidHandle;
        const entry = &self.entries[handle.index];
        if (entry.generation != handle.generation) return Error.InvalidHandle;
        entry.storePressureState(.closed);
        entry.storeState(.closed);
    }

    pub fn reclaimClosed(self: *SendBufferDirectory, handle: SendBufferHandle) Error!void {
        if (handle.index >= self.entries.len) return Error.InvalidHandle;
        const entry = &self.entries[handle.index];
        if (entry.generation != handle.generation) return Error.InvalidHandle;
        if (!entry.compareExchangeState(.closed, .free)) return Error.EntryNotClosed;
        entry.generation +%= 1;
        if (entry.generation == 0) entry.generation = 1;
        entry.target_node_id = 0;
        entry.target_service_id = 0;
        entry.stream_id = 0;
        entry.producer_count = 0;
        entry.last_activity_ns = 0;
        entry.bytes_pending = 0;
        entry.messages_pending = 0;
        entry.lifetime_messages_written = 0;
        entry.lifetime_messages_dropped = 0;
        entry.storePressureState(.unknown);
    }
};

fn entriesSlice(bytes: []u8, entry_count: u32) []SendBufferEntry {
    const entries_ptr: [*]SendBufferEntry = @ptrCast(@alignCast(bytes.ptr + SendBufferDirectoryHeader.encoded_length));
    return entries_ptr[0..entry_count];
}

fn handleForEntry(index: u32, entry: *const SendBufferEntry) SendBufferHandle {
    return .{
        .index = index,
        .generation = entry.generation,
        .target_node_id = entry.target_node_id,
        .target_service_id = entry.target_service_id,
        .stream_id = entry.stream_id,
        .ring_offset = entry.ring_offset,
        .ring_capacity = entry.ring_capacity,
        .max_message_length = entry.max_message_length,
    };
}

fn streamId(target_node_id: i16, target_service_id: i32, generation: u16) u32 {
    var hash: u32 = 2166136261;
    inline for (.{ target_node_id, target_service_id, generation }) |value| {
        const bytes = std.mem.asBytes(&value);
        for (bytes) |byte| {
            hash = (hash ^ byte) *% 16777619;
        }
    }
    return if (hash == 0) 1 else hash;
}

fn subtractSaturating(ptr: *volatile u64, amount: u64) void {
    while (true) {
        const current = @atomicLoad(u64, ptr, .acquire);
        const next = if (current > amount) current - amount else 0;
        if (@cmpxchgWeak(u64, ptr, current, next, .acq_rel, .acquire) == null) return;
    }
}

const testing = std.testing;

test "send buffer directory initializes fixed slots" {
    var dir_buf: [SendBufferDirectory.regionSize(2)]u8 align(constants.cache_line_pad) = undefined;
    var mapped: [8192]u8 align(constants.page_size) = undefined;
    @memset(&mapped, 0);

    var directory = try SendBufferDirectory.initNew(
        &dir_buf,
        2,
        1024,
        SendBufferDirectory.slotLength(1024) * 2,
        1024,
    );

    try testing.expectEqual(constants.metadata_version_v2, directory.header.version);
    try testing.expectEqual(@as(u32, 2), directory.header.entry_count);
    try testing.expectEqual(@as(u64, 1024), directory.entries[0].ring_offset);
    try testing.expectEqual(@as(u64, 1024 + SendBufferDirectory.slotLength(1024)), directory.entries[1].ring_offset);
    try testing.expectEqual(SendBufferEntryState.free, directory.entries[0].loadState());
}

test "findOrAllocateDestination returns stable handle" {
    var dir_buf: [SendBufferDirectory.regionSize(2)]u8 align(constants.cache_line_pad) = undefined;
    var mapped: [8192]u8 align(constants.page_size) = undefined;
    @memset(&mapped, 0);

    var directory = try SendBufferDirectory.initNew(
        &dir_buf,
        2,
        1024,
        SendBufferDirectory.slotLength(1024) * 2,
        1024,
    );

    const first = try directory.findOrAllocateDestination(&mapped, 2, 7);
    const second = try directory.findOrAllocateDestination(&mapped, 2, 7);

    try testing.expectEqual(first.index, second.index);
    try testing.expectEqual(first.generation, second.generation);
    try testing.expectEqual(SendBufferEntryState.active, directory.entries[first.index].loadState());
}

test "generation mismatch invalidates stale handle" {
    var dir_buf: [SendBufferDirectory.regionSize(1)]u8 align(constants.cache_line_pad) = undefined;
    var mapped: [4096]u8 align(constants.page_size) = undefined;
    @memset(&mapped, 0);

    var directory = try SendBufferDirectory.initNew(
        &dir_buf,
        1,
        1024,
        SendBufferDirectory.slotLength(1024),
        1024,
    );

    const handle = try directory.findOrAllocateDestination(&mapped, 2, 7);
    try directory.markClosed(handle);
    try directory.reclaimClosed(handle);

    try testing.expectError(error.InvalidHandle, directory.validateHandle(handle));
}

test "directory full returns deterministic error" {
    var dir_buf: [SendBufferDirectory.regionSize(1)]u8 align(constants.cache_line_pad) = undefined;
    var mapped: [4096]u8 align(constants.page_size) = undefined;
    @memset(&mapped, 0);

    var directory = try SendBufferDirectory.initNew(
        &dir_buf,
        1,
        1024,
        SendBufferDirectory.slotLength(1024),
        1024,
    );

    _ = try directory.findOrAllocateDestination(&mapped, 2, 7);
    try testing.expectError(error.DirectoryFull, directory.findOrAllocateDestination(&mapped, 3, 8));
}

test "draining entry rejects handle validation for producers" {
    var dir_buf: [SendBufferDirectory.regionSize(1)]u8 align(constants.cache_line_pad) = undefined;
    var mapped: [4096]u8 align(constants.page_size) = undefined;
    @memset(&mapped, 0);

    var directory = try SendBufferDirectory.initNew(
        &dir_buf,
        1,
        1024,
        SendBufferDirectory.slotLength(1024),
        1024,
    );

    const handle = try directory.findOrAllocateDestination(&mapped, 2, 7);
    try directory.markDraining(handle);
    try testing.expectError(error.InvalidHandle, directory.validateHandle(handle));
    try testing.expect(directory.findByDestination(2, 7) != null);
}
