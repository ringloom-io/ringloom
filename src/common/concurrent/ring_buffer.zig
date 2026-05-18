//! MPSC Ring Buffer — a many-producer, single-consumer ring buffer.
//!
//! This is a flyweight over an existing byte slice (typically memory-mapped).
//! It supports both non-blocking and blocking (futex-backed) operation modes.
//!
//! Record layout (8 bytes):
//!   i32 length      — negative = uncommitted, positive = committed
//!   i32 msg_type_id — ≥1 valid, -1 = padding
//!
//! The trailer region (768 bytes) lives at the end of the buffer and contains
//! cache-line-padded atomic fields for tail, head_cache, head, correlation
//! counter, and consumer heartbeat.

const std = @import("std");
const constants = @import("../platform/constants.zig");
const ProcessSynchronizer = @import("../platform/process_sync.zig").ProcessSynchronizer;

// ── Ring Buffer Constants ─────────────────────────────────────────────

pub const record_header_length: usize = constants.ring_buffer_record_header_length; // 8
pub const record_alignment: usize = constants.ring_buffer_alignment; // 8
pub const trailer_length: usize = constants.ring_buffer_trailer_length; // 768
pub const insufficient_capacity: i32 = -1;

// Trailer field offsets relative to start of trailer region.
pub const tail_position_offset: usize = constants.cache_line_pad * 1; // 128
pub const head_cache_position_offset: usize = constants.cache_line_pad * 2; // 256
pub const head_position_offset: usize = constants.cache_line_pad * 3; // 384
pub const correlation_counter_offset: usize = constants.cache_line_pad * 4; // 512
pub const consumer_heartbeat_offset: usize = constants.cache_line_pad * 5; // 640

pub const blocking_prefix_length: usize = constants.blocking_trailer_length; // 384

// ── RingBuffer Struct ─────────────────────────────────────────────────

pub const RingBuffer = struct {
    buffer: []align(record_alignment) u8,
    capacity: usize,
    max_msg_length: usize,
    trailer_offset: usize,
    capacity_mask: u63,
    blocking: bool,
    process_synchronizer: ?ProcessSynchronizer,
    writer_wait_state: ?*volatile i32,
    reader_wait_state: ?*volatile i32,

    pub const InitError = error{
        BufferTooSmall,
        CapacityNotPowerOfTwo,
    };

    pub const WriteError = error{
        BufferFull,
        InvalidMsgTypeId,
        MessageTooLong,
    };

    pub const Claim = struct {
        buffer: []u8,
        ring_buffer: *RingBuffer,
        header_index: usize,
        record_length: i32,

        pub fn commit(self: *Claim) void {
            self.ring_buffer.commitAt(self.header_index, self.record_length);
        }

        pub fn abort(self: *Claim) void {
            self.ring_buffer.abortAt(self.header_index, self.record_length);
        }
    };

    pub const MessageHandler = *const fn (msg_type_id: i32, payload: []const u8) void;
    pub const ContextMessageHandler = *const fn (context: *anyopaque, msg_type_id: i32, payload: []const u8) void;
    pub const ControlledReadAction = enum {
        abort,
        break_,
        commit,
        continue_,
    };
    pub const ControlledMessageHandler = *const fn (msg_type_id: i32, payload: []const u8) ControlledReadAction;
    pub const ControlledContextMessageHandler = *const fn (
        context: *anyopaque,
        msg_type_id: i32,
        payload: []const u8,
    ) ControlledReadAction;

    // ── Initialization ────────────────────────────────────────────────

    pub fn init(
        buffer: []align(record_alignment) u8,
        blocking: bool,
        process_synchronizer: ?ProcessSynchronizer,
        blocking_prefix: ?[*]u8,
    ) InitError!RingBuffer {
        if (buffer.len <= trailer_length) {
            return InitError.BufferTooSmall;
        }

        const data_capacity = buffer.len - trailer_length;
        if (!constants.isPowerOfTwo(data_capacity)) {
            return InitError.CapacityNotPowerOfTwo;
        }

        var writer_ws: ?*volatile i32 = null;
        var reader_ws: ?*volatile i32 = null;

        if (blocking) {
            if (blocking_prefix) |prefix| {
                writer_ws = @ptrCast(@alignCast(prefix));
                reader_ws = @ptrCast(@alignCast(prefix + constants.cache_line_pad));
            }
        }

        return RingBuffer{
            .buffer = buffer,
            .capacity = data_capacity,
            .max_msg_length = data_capacity / 8,
            .trailer_offset = data_capacity,
            .capacity_mask = @intCast(data_capacity - 1),
            .blocking = blocking,
            .process_synchronizer = process_synchronizer,
            .writer_wait_state = writer_ws,
            .reader_wait_state = reader_ws,
        };
    }

    pub fn calculateRequiredSize(data_capacity: usize, blocking: bool) usize {
        const prefix: usize = if (blocking) blocking_prefix_length else 0;
        return prefix + data_capacity + trailer_length;
    }

    // ── Trailer Field Accessors ───────────────────────────────────────

    inline fn trailerFieldPtr(self: *RingBuffer, field_offset: usize) *i64 {
        const offset = self.trailer_offset + field_offset;
        return @ptrCast(@alignCast(self.buffer.ptr + offset));
    }

    inline fn trailerFieldPtrConst(self: *const RingBuffer, field_offset: usize) *i64 {
        const offset = self.trailer_offset + field_offset;
        return @ptrCast(@alignCast(self.buffer.ptr + offset));
    }

    inline fn loadTailPosition(self: *RingBuffer, order: std.builtin.AtomicOrder) i64 {
        return @atomicLoad(i64, self.trailerFieldPtr(tail_position_offset), order);
    }

    inline fn loadHeadCache(self: *RingBuffer, order: std.builtin.AtomicOrder) i64 {
        return @atomicLoad(i64, self.trailerFieldPtr(head_cache_position_offset), order);
    }

    inline fn storeHeadCache(self: *RingBuffer, value: i64, order: std.builtin.AtomicOrder) void {
        @atomicStore(i64, self.trailerFieldPtr(head_cache_position_offset), value, order);
    }

    inline fn loadHeadPosition(self: *RingBuffer, order: std.builtin.AtomicOrder) i64 {
        return @atomicLoad(i64, self.trailerFieldPtr(head_position_offset), order);
    }

    inline fn storeHeadPosition(self: *RingBuffer, value: i64, order: std.builtin.AtomicOrder) void {
        @atomicStore(i64, self.trailerFieldPtr(head_position_offset), value, order);
    }

    inline fn loadCorrelationCounter(self: *const RingBuffer, order: std.builtin.AtomicOrder) i64 {
        return @atomicLoad(i64, self.trailerFieldPtrConst(correlation_counter_offset), order);
    }

    inline fn loadConsumerHeartbeat(self: *const RingBuffer, order: std.builtin.AtomicOrder) i64 {
        return @atomicLoad(i64, self.trailerFieldPtrConst(consumer_heartbeat_offset), order);
    }

    inline fn storeConsumerHeartbeat(self: *RingBuffer, value: i64, order: std.builtin.AtomicOrder) void {
        @atomicStore(i64, self.trailerFieldPtr(consumer_heartbeat_offset), value, order);
    }

    inline fn recordLengthPtr(self: *RingBuffer, index: usize) *volatile i32 {
        return @ptrCast(@alignCast(self.buffer.ptr + index));
    }

    inline fn recordMsgTypeIdPtr(self: *RingBuffer, index: usize) *volatile i32 {
        return @ptrCast(@alignCast(self.buffer.ptr + index + @sizeOf(i32)));
    }

    // ── Write (copy-based) ────────────────────────────────────────────

    pub fn write(self: *RingBuffer, msg_type_id: i32, payload: []const u8) WriteError!void {
        if (msg_type_id < 1) {
            return WriteError.InvalidMsgTypeId;
        }
        if (payload.len > self.max_msg_length) {
            return WriteError.MessageTooLong;
        }

        const record_length = payload.len + record_header_length;
        const aligned_length = alignUp(record_length);
        const claim_index = self.claimCapacity(aligned_length) orelse return WriteError.BufferFull;

        self.writeRecord(claim_index, msg_type_id, payload, @intCast(record_length));
    }

    fn writeRecord(self: *RingBuffer, index: usize, msg_type_id: i32, payload: []const u8, record_length: i32) void {
        // Write negative length as sentinel (uncommitted).
        @atomicStore(i32, self.recordLengthPtr(index), -record_length, .release);

        // Write msg_type_id.
        @atomicStore(i32, self.recordMsgTypeIdPtr(index), msg_type_id, .monotonic);

        // Copy payload.
        const payload_offset = index + record_header_length;
        if (payload.len > 0) {
            @memcpy(self.buffer[payload_offset .. payload_offset + payload.len], payload);
        }

        // Commit: store positive length (release).
        @atomicStore(i32, self.recordLengthPtr(index), record_length, .release);

        // Wake reader if blocking.
        self.awakeReader();
    }

    fn writePaddingRecord(self: *RingBuffer, index: usize, padding_length: i32) void {
        @atomicStore(i32, self.recordMsgTypeIdPtr(index), constants.padding_msg_type_id, .monotonic);
        @atomicStore(i32, self.recordLengthPtr(index), padding_length, .release);
    }

    // ── Claim Capacity (CAS loop) ────────────────────────────────────

    fn claimCapacity(self: *RingBuffer, required_capacity: usize) ?usize {
        const cap = self.capacity;
        const mask: usize = @intCast(self.capacity_mask);
        var head_cache = self.loadHeadCache(.acquire);

        while (true) {
            const tail = self.loadTailPosition(.acquire);
            const available_capacity: usize = cap - @as(usize, @intCast(tail - head_cache));

            if (required_capacity > available_capacity) {
                // Stale head_cache — refresh it.
                const head = self.loadHeadPosition(.acquire);
                const refreshed_available: usize = cap - @as(usize, @intCast(tail - head));
                if (required_capacity > refreshed_available) {
                    return null; // truly full
                }
                head_cache = head;
                self.storeHeadCache(head, .release);
            }

            const tail_index: usize = @intCast(@as(i64, @intCast(mask)) & tail);
            const to_end = cap - tail_index;

            if (required_capacity > to_end) {
                // Need to wrap around.
                // Check if there's room at the beginning.
                const head = self.loadHeadPosition(.acquire);
                const head_index: usize = @intCast(@as(i64, @intCast(mask)) & head);

                if (required_capacity > head_index) {
                    // Refresh head cache and try again.
                    head_cache = head;
                    self.storeHeadCache(head, .release);
                    if (required_capacity > head_index) {
                        return null;
                    }
                }

                // CAS: advance tail by to_end (padding) + required_capacity (data).
                const new_tail = tail + @as(i64, @intCast(to_end)) + @as(i64, @intCast(required_capacity));
                const tail_ptr = self.trailerFieldPtr(tail_position_offset);
                if (@cmpxchgWeak(i64, tail_ptr, tail, new_tail, .acq_rel, .acquire)) |_| {
                    // CAS failed — retry.
                    continue;
                }

                // Write padding record spanning `to_end` bytes.
                self.writePaddingRecord(tail_index, @intCast(to_end));

                // Data goes at index 0.
                return 0;
            } else {
                // No wrap needed — claim in place.
                const new_tail = tail + @as(i64, @intCast(required_capacity));
                const tail_ptr = self.trailerFieldPtr(tail_position_offset);
                if (@cmpxchgWeak(i64, tail_ptr, tail, new_tail, .acq_rel, .acquire)) |_| {
                    continue;
                }
                return tail_index;
            }
        }
    }

    // ── Read (single consumer) ────────────────────────────────────────

    pub fn read(self: *RingBuffer, handler: MessageHandler, limit: u32) u32 {
        return self.readInternal(false, {}, handler, limit);
    }

    pub fn readWithContext(
        self: *RingBuffer,
        context: *anyopaque,
        handler: ContextMessageHandler,
        limit: u32,
    ) u32 {
        return self.readInternal(true, context, handler, limit);
    }

    pub fn controlledRead(
        self: *RingBuffer,
        handler: ControlledMessageHandler,
        limit: u32,
    ) u32 {
        return self.controlledReadInternal(false, {}, handler, limit);
    }

    pub fn controlledReadWithContext(
        self: *RingBuffer,
        context: *anyopaque,
        handler: ControlledContextMessageHandler,
        limit: u32,
    ) u32 {
        return self.controlledReadInternal(true, context, handler, limit);
    }

    fn readInternal(
        self: *RingBuffer,
        comptime with_context: bool,
        context: if (with_context) *anyopaque else void,
        handler: if (with_context) ContextMessageHandler else MessageHandler,
        limit: u32,
    ) u32 {
        const head = self.loadHeadPosition(.monotonic);
        const tail = self.loadTailPosition(.acquire);

        const head_index: usize = @intCast(@as(i64, @intCast(self.capacity_mask)) & head);
        const available_bytes: usize = @min(
            @as(usize, @intCast(tail - head)),
            self.capacity - head_index,
        );

        if (available_bytes == 0) {
            return 0;
        }

        var bytes_consumed: usize = 0;
        var messages_read: u32 = 0;

        while (bytes_consumed < available_bytes and messages_read < limit) {
            const record_offset = head_index + bytes_consumed;

            // Load the committed length with acquire.
            const raw_length = @atomicLoad(i32, self.recordLengthPtr(record_offset), .acquire);
            if (raw_length <= 0) {
                break; // Not yet committed.
            }

            const record_length: usize = @intCast(raw_length);
            const aligned_length = alignUp(record_length);

            const msg_type_id = @atomicLoad(i32, self.recordMsgTypeIdPtr(record_offset), .monotonic);

            if (msg_type_id != constants.padding_msg_type_id) {
                // Deliver payload to handler.
                const payload_offset = record_offset + record_header_length;
                const payload_len = record_length - record_header_length;
                const payload = self.buffer[payload_offset .. payload_offset + payload_len];
                if (with_context) {
                    handler(context, msg_type_id, payload);
                } else {
                    handler(msg_type_id, payload);
                }
                messages_read += 1;
            }

            // Clear only the guard word needed to prevent stale-positive reads on
            // reuse; clearing payload bytes dirties cache lines for producers.
            @atomicStore(i32, self.recordLengthPtr(record_offset), 0, .monotonic);
            bytes_consumed += aligned_length;
        }

        if (bytes_consumed > 0) {
            // Advance head.
            self.storeHeadPosition(head + @as(i64, @intCast(bytes_consumed)), .release);

            // Wake writer if blocking.
            self.awakeWriter();
        }

        return messages_read;
    }

    fn controlledReadInternal(
        self: *RingBuffer,
        comptime with_context: bool,
        context: if (with_context) *anyopaque else void,
        handler: if (with_context) ControlledContextMessageHandler else ControlledMessageHandler,
        limit: u32,
    ) u32 {
        var head = self.loadHeadPosition(.monotonic);
        const tail = self.loadTailPosition(.acquire);

        var head_index: usize = @intCast(@as(i64, @intCast(self.capacity_mask)) & head);
        var remaining_bytes: usize = @min(
            @as(usize, @intCast(tail - head)),
            self.capacity - head_index,
        );

        if (remaining_bytes == 0) {
            return 0;
        }

        var bytes_consumed: usize = 0;
        var messages_read: u32 = 0;

        while (bytes_consumed < remaining_bytes and messages_read < limit) {
            const record_offset = head_index + bytes_consumed;
            const raw_length = @atomicLoad(i32, self.recordLengthPtr(record_offset), .acquire);
            if (raw_length <= 0) {
                break;
            }

            const record_length: usize = @intCast(raw_length);
            const aligned_length = alignUp(record_length);
            const msg_type_id = @atomicLoad(i32, self.recordMsgTypeIdPtr(record_offset), .monotonic);

            if (msg_type_id == constants.padding_msg_type_id) {
                @atomicStore(i32, self.recordLengthPtr(record_offset), 0, .monotonic);
                bytes_consumed += aligned_length;
                continue;
            }

            const payload_offset = record_offset + record_header_length;
            const payload_len = record_length - record_header_length;
            const payload = self.buffer[payload_offset .. payload_offset + payload_len];
            const action = if (with_context)
                handler(context, msg_type_id, payload)
            else
                handler(msg_type_id, payload);

            if (action == .abort) {
                break;
            }

            @atomicStore(i32, self.recordLengthPtr(record_offset), 0, .monotonic);
            bytes_consumed += aligned_length;
            messages_read += 1;

            switch (action) {
                .abort => unreachable,
                .break_ => break,
                .commit => {
                    head += @as(i64, @intCast(bytes_consumed));
                    self.storeHeadPosition(head, .release);
                    self.awakeWriter();
                    head_index += bytes_consumed;
                    remaining_bytes -= bytes_consumed;
                    bytes_consumed = 0;
                },
                .continue_ => {},
            }
        }

        if (bytes_consumed > 0) {
            self.storeHeadPosition(head + @as(i64, @intCast(bytes_consumed)), .release);
            self.awakeWriter();
        }

        return messages_read;
    }

    // ── Try-Claim API ─────────────────────────────────────────────────

    pub fn tryClaim(self: *RingBuffer, msg_type_id: i32, length: usize) ?Claim {
        if (msg_type_id < 1) {
            return null;
        }
        if (length > self.max_msg_length) {
            return null;
        }

        const record_length = length + record_header_length;
        const aligned_length = alignUp(record_length);
        const claim_index = self.claimCapacity(aligned_length) orelse return null;

        // Write negative length as sentinel.
        @atomicStore(i32, self.recordLengthPtr(claim_index), -@as(i32, @intCast(record_length)), .release);

        // Write msg_type_id.
        @atomicStore(i32, self.recordMsgTypeIdPtr(claim_index), msg_type_id, .monotonic);

        // Return claim with writable buffer slice.
        const payload_offset = claim_index + record_header_length;
        return Claim{
            .buffer = self.buffer[payload_offset .. payload_offset + length],
            .ring_buffer = self,
            .header_index = claim_index,
            .record_length = @intCast(record_length),
        };
    }

    // ── Commit and Abort ──────────────────────────────────────────────

    fn commitAt(self: *RingBuffer, header_index: usize, record_len: i32) void {
        // Release store positive length.
        @atomicStore(i32, self.recordLengthPtr(header_index), record_len, .release);
        self.awakeReader();
    }

    fn abortAt(self: *RingBuffer, header_index: usize, record_len: i32) void {
        // Set msg_type_id to padding so consumer skips it.
        @atomicStore(i32, self.recordMsgTypeIdPtr(header_index), constants.padding_msg_type_id, .monotonic);
        // Release store positive length so consumer can advance past it.
        @atomicStore(i32, self.recordLengthPtr(header_index), record_len, .release);
        self.awakeReader();
    }

    // ── Unblock ───────────────────────────────────────────────────────

    pub fn unblock(self: *RingBuffer) bool {
        const head = self.loadHeadPosition(.acquire);
        const tail = self.loadTailPosition(.acquire);

        if (head == tail) {
            return false; // empty
        }

        const mask: usize = @intCast(self.capacity_mask);
        const head_index: usize = @intCast(@as(i64, @intCast(mask)) & head);

        const raw_length = @atomicLoad(i32, self.recordLengthPtr(head_index), .acquire);

        if (raw_length < 0) {
            // Crashed producer left a negative (uncommitted) length — convert to padding.
            @atomicStore(i32, self.recordMsgTypeIdPtr(head_index), constants.padding_msg_type_id, .monotonic);
            @atomicStore(i32, self.recordLengthPtr(head_index), -raw_length, .release);
            return true;
        } else if (raw_length == 0) {
            const tail_index: usize = @intCast(@as(i64, @intCast(mask)) & tail);
            const limit: usize = if (tail_index > head_index) tail_index else self.capacity;
            var index = head_index + record_alignment;

            while (index < limit) : (index += record_alignment) {
                const length = @atomicLoad(i32, self.recordLengthPtr(index), .acquire);
                if (length != 0) {
                    if (scanBackToConfirmStillZeroed(self, index, head_index)) {
                        @atomicStore(i32, self.recordMsgTypeIdPtr(head_index), constants.padding_msg_type_id, .monotonic);
                        @atomicStore(i32, self.recordLengthPtr(head_index), @intCast(index - head_index), .release);
                        return true;
                    }
                    break;
                }
            }
        }

        return false;
    }

    // ── Blocking Extensions ───────────────────────────────────────────

    pub fn writeBlocking(self: *RingBuffer, msg_type_id: i32, payload: []const u8, timeout_ns: ?i64) WriteError!void {
        // Try non-blocking first.
        self.write(msg_type_id, payload) catch |err| switch (err) {
            WriteError.BufferFull => {
                if (!self.blocking) return WriteError.BufferFull;

                // Wait and retry.
                if (self.process_synchronizer) |sync| {
                    if (self.writer_wait_state) |ws| {
                        _ = sync.wait(@volatileCast(ws), @atomicLoad(i32, @volatileCast(ws), .acquire), timeout_ns);
                    }
                }

                // Retry once after wakeup.
                return self.write(msg_type_id, payload);
            },
            else => return err,
        };
    }

    pub fn readBlocking(self: *RingBuffer, handler: MessageHandler, limit: u32, timeout_ns: ?i64) u32 {
        // Try non-blocking first.
        const count = self.read(handler, limit);
        if (count > 0) return count;

        if (!self.blocking) return 0;

        // Wait for data.
        if (self.process_synchronizer) |sync| {
            if (self.reader_wait_state) |rs| {
                _ = sync.wait(@volatileCast(rs), @atomicLoad(i32, @volatileCast(rs), .acquire), timeout_ns);
            }
        }

        // Retry once after wakeup.
        return self.read(handler, limit);
    }

    // ── Wake Helpers ──────────────────────────────────────────────────

    fn awakeReader(self: *RingBuffer) void {
        if (self.process_synchronizer) |sync| {
            if (self.reader_wait_state) |rs| {
                sync.wakeAll(@volatileCast(rs));
            }
        }
    }

    fn awakeWriter(self: *RingBuffer) void {
        if (self.process_synchronizer) |sync| {
            if (self.writer_wait_state) |ws| {
                sync.wakeAll(@volatileCast(ws));
            }
        }
    }

    // ── Utility Accessors ─────────────────────────────────────────────

    pub fn getCapacity(self: *const RingBuffer) usize {
        return self.capacity;
    }

    pub fn maxMessageLength(self: *const RingBuffer) usize {
        return self.max_msg_length;
    }

    pub fn producerPosition(self: *RingBuffer) i64 {
        return self.loadTailPosition(.acquire);
    }

    pub fn consumerPosition(self: *RingBuffer) i64 {
        return self.loadHeadPosition(.acquire);
    }

    pub fn size(self: *RingBuffer) usize {
        // Retry loop for consistent snapshot of head/tail.
        var head: i64 = undefined;
        while (true) {
            head = self.loadHeadPosition(.acquire);
            const tail = self.loadTailPosition(.acquire);
            const head_after = self.loadHeadPosition(.acquire);
            if (head == head_after) {
                return @intCast(tail - head);
            }
        }
    }

    pub fn nextCorrelationId(self: *RingBuffer) i64 {
        return @atomicRmw(i64, self.trailerFieldPtr(correlation_counter_offset), .Add, 1, .acq_rel);
    }

    pub fn setConsumerHeartbeatTime(self: *RingBuffer, epoch_ms: i64) void {
        self.storeConsumerHeartbeat(epoch_ms, .release);
    }

    pub fn consumerHeartbeatTime(self: *const RingBuffer) i64 {
        return self.loadConsumerHeartbeat(.acquire);
    }

    pub fn isBlocking(self: *const RingBuffer) bool {
        return self.blocking;
    }
};

// ── Helper Functions ──────────────────────────────────────────────────

fn alignUp(value: usize) usize {
    return (value + (record_alignment - 1)) & ~@as(usize, record_alignment - 1);
}

fn scanBackToConfirmStillZeroed(self: *RingBuffer, from: usize, limit: usize) bool {
    var index = from;
    while (index > limit) {
        index -= record_alignment;
        if (@atomicLoad(i32, self.recordLengthPtr(index), .acquire) != 0) {
            return false;
        }
    }

    return true;
}

fn allocateAlignedBuffer(allocator: std.mem.Allocator, buf_size: usize) ![]align(record_alignment) u8 {
    const buf = try allocator.alignedAlloc(u8, @enumFromInt(std.math.log2(record_alignment)), buf_size);
    @memset(buf, 0);
    return buf;
}

// ── Tests ─────────────────────────────────────────────────────────────

// File-level state for handler callbacks (Zig fn ptrs can't capture mutable locals).
var test_received_type: i32 = 0;
var test_received_payload_len: usize = 0;
var test_received_payload_buf: [256]u8 = undefined;
var test_messages_received: u32 = 0;
var test_total_payload_bytes: usize = 0;

fn resetTestState() void {
    test_received_type = 0;
    test_received_payload_len = 0;
    test_received_payload_buf = undefined;
    test_messages_received = 0;
    test_total_payload_bytes = 0;
}

fn testCaptureHandler(msg_type_id: i32, payload: []const u8) void {
    test_received_type = msg_type_id;
    test_received_payload_len = payload.len;
    if (payload.len <= test_received_payload_buf.len) {
        @memcpy(test_received_payload_buf[0..payload.len], payload);
    }
    test_messages_received += 1;
    test_total_payload_bytes += payload.len;
}

fn testCountHandler(_: i32, _: []const u8) void {
    test_messages_received += 1;
}

fn testNoopHandler(_: i32, _: []const u8) void {}

test "write and read single message" {
    const allocator = std.testing.allocator;
    const buf = try allocateAlignedBuffer(allocator, 1024 + trailer_length);
    defer allocator.free(buf);

    var rb = try RingBuffer.init(buf, false, null, null);

    const payload = "hello, ring buffer!";
    try rb.write(1, payload);

    resetTestState();
    const count = rb.read(&testCaptureHandler, 10);

    try std.testing.expectEqual(@as(u32, 1), count);
    try std.testing.expectEqual(@as(i32, 1), test_received_type);
    try std.testing.expectEqual(payload.len, test_received_payload_len);
    try std.testing.expectEqualSlices(u8, payload, test_received_payload_buf[0..payload.len]);
}

test "write N messages then read all" {
    const allocator = std.testing.allocator;
    const buf = try allocateAlignedBuffer(allocator, 4096 + trailer_length);
    defer allocator.free(buf);

    var rb = try RingBuffer.init(buf, false, null, null);

    const n: u32 = 10;
    var i: u32 = 0;
    while (i < n) : (i += 1) {
        const msg = "test message";
        try rb.write(@as(i32, @intCast(i + 1)), msg);
    }

    resetTestState();
    const count = rb.read(&testCountHandler, 100);
    try std.testing.expectEqual(n, count);
    try std.testing.expectEqual(n, test_messages_received);
}

test "wrap-around: write fills to end, next message wraps to start" {
    const allocator = std.testing.allocator;
    // Use a small power-of-two capacity.
    const cap: usize = 256;
    const buf = try allocateAlignedBuffer(allocator, cap + trailer_length);
    defer allocator.free(buf);

    var rb = try RingBuffer.init(buf, false, null, null);

    // Fill most of the buffer with messages.
    // Each record: 8 (header) + 32 (payload) = 40 bytes aligned.
    const payload_large = "AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA"; // 32 bytes
    // 256 / 40 = 6 messages fit (240 bytes used, 16 bytes remain).
    var i: u32 = 0;
    while (i < 6) : (i += 1) {
        try rb.write(1, payload_large);
    }

    // Read all to free up space.
    resetTestState();
    const read_count_1 = rb.read(&testCountHandler, 100);
    try std.testing.expectEqual(@as(u32, 6), read_count_1);

    // Now head is at 240, tail is at 240.
    // Write another message that won't fit in the 16 remaining bytes — forces wrap.
    const wrap_payload = "wrap around payload!"; // 20 bytes + 8 header = 28 aligned to 32
    try rb.write(42, wrap_payload);

    resetTestState();
    const read_count_2 = rb.read(&testCaptureHandler, 10);
    try std.testing.expectEqual(@as(u32, 0), read_count_2);

    const read_count_3 = rb.read(&testCaptureHandler, 10);
    try std.testing.expectEqual(@as(u32, 1), read_count_3);
    try std.testing.expectEqual(@as(i32, 42), test_received_type);
    try std.testing.expectEqual(wrap_payload.len, test_received_payload_len);
    try std.testing.expectEqualSlices(u8, wrap_payload, test_received_payload_buf[0..wrap_payload.len]);
}

test "write returns error.BufferFull when buffer is exhausted" {
    const allocator = std.testing.allocator;
    const cap: usize = 128;
    const buf = try allocateAlignedBuffer(allocator, cap + trailer_length);
    defer allocator.free(buf);

    var rb = try RingBuffer.init(buf, false, null, null);

    // max_msg_length = 128 / 8 = 16 bytes.
    // Fill the buffer: each record is 8 + 8 = 16 aligned, so 128 / 16 = 8 records.
    const payload = "12345678"; // 8 bytes
    var i: u32 = 0;
    while (i < 8) : (i += 1) {
        try rb.write(1, payload);
    }

    // Next write should fail.
    const result = rb.write(1, payload);
    try std.testing.expectError(RingBuffer.WriteError.BufferFull, result);
}

test "tryClaim and commit" {
    const allocator = std.testing.allocator;
    const buf = try allocateAlignedBuffer(allocator, 1024 + trailer_length);
    defer allocator.free(buf);

    var rb = try RingBuffer.init(buf, false, null, null);

    const payload = "claimed message!";
    var claim = rb.tryClaim(7, payload.len) orelse {
        return error.TestUnexpectedResult;
    };

    // Write payload into the claimed buffer.
    @memcpy(claim.buffer[0..payload.len], payload);
    claim.commit();

    resetTestState();
    const count = rb.read(&testCaptureHandler, 10);
    try std.testing.expectEqual(@as(u32, 1), count);
    try std.testing.expectEqual(@as(i32, 7), test_received_type);
    try std.testing.expectEqual(payload.len, test_received_payload_len);
    try std.testing.expectEqualSlices(u8, payload, test_received_payload_buf[0..payload.len]);
}

test "tryClaim and abort is skipped by consumer" {
    const allocator = std.testing.allocator;
    const buf = try allocateAlignedBuffer(allocator, 1024 + trailer_length);
    defer allocator.free(buf);

    var rb = try RingBuffer.init(buf, false, null, null);

    // Claim and abort.
    var claim = rb.tryClaim(3, 16) orelse {
        return error.TestUnexpectedResult;
    };
    @memset(claim.buffer, 0xAB);
    claim.abort();

    // Write a real message after the aborted one.
    const real_payload = "real message";
    try rb.write(5, real_payload);

    resetTestState();
    // Read should skip the aborted record and deliver only the real message.
    const count = rb.read(&testCaptureHandler, 10);
    try std.testing.expectEqual(@as(u32, 1), count);
    try std.testing.expectEqual(@as(i32, 5), test_received_type);
    try std.testing.expectEqual(real_payload.len, test_received_payload_len);
}

test "unblock recovers from crashed producer (negative length)" {
    const allocator = std.testing.allocator;
    const buf = try allocateAlignedBuffer(allocator, 1024 + trailer_length);
    defer allocator.free(buf);

    var rb = try RingBuffer.init(buf, false, null, null);

    // Simulate a crashed producer: tryClaim but don't commit.
    const claim = rb.tryClaim(1, 16) orelse {
        return error.TestUnexpectedResult;
    };
    _ = claim;

    // The record at head has negative length (uncommitted). Reader would be stuck.
    const unblocked = rb.unblock();
    try std.testing.expect(unblocked);

    // Now read should skip it as padding.
    resetTestState();
    const count = rb.read(&testCountHandler, 10);
    // The unblocked record becomes padding — consumer sees it and skips it,
    // but it still counts as "consumed bytes" not "messages_read".
    // It was turned to padding so the handler isn't called.
    // However, there may be no more records after it.
    _ = count;

    // After unblock + read, the buffer should be empty now.
    try std.testing.expectEqual(@as(usize, 0), rb.size());
}

test "controlledReadWithContext supports commit and abort" {
    const allocator = std.testing.allocator;
    const buf = try allocateAlignedBuffer(allocator, 1024 + trailer_length);
    defer allocator.free(buf);

    var rb = try RingBuffer.init(buf, false, null, null);
    try rb.write(1, "first");
    try rb.write(2, "second");
    try rb.write(3, "third");

    const Context = struct {
        seen: [4]i32 = [_]i32{0} ** 4,
        len: usize = 0,

        fn handler(ctx: *anyopaque, msg_type_id: i32, _: []const u8) RingBuffer.ControlledReadAction {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.seen[self.len] = msg_type_id;
            self.len += 1;
            return switch (msg_type_id) {
                1 => .commit,
                2 => .abort,
                else => .continue_,
            };
        }
    };

    var context = Context{};
    const count = rb.controlledReadWithContext(&context, Context.handler, 10);
    try std.testing.expectEqual(@as(u32, 1), count);
    try std.testing.expectEqual(@as(usize, 2), context.len);
    try std.testing.expectEqual(@as(i32, 1), context.seen[0]);
    try std.testing.expectEqual(@as(i32, 2), context.seen[1]);

    resetTestState();
    const remaining = rb.read(&testCaptureHandler, 10);
    try std.testing.expectEqual(@as(u32, 2), remaining);
    try std.testing.expectEqual(@as(i32, 3), test_received_type);
}

test "unblock converts confirmed zero gap to padding" {
    const allocator = std.testing.allocator;
    const capacity: usize = 64;
    const buf = try allocateAlignedBuffer(allocator, capacity + trailer_length);
    defer allocator.free(buf);

    var rb = try RingBuffer.init(buf, false, null, null);

    @atomicStore(i64, rb.trailerFieldPtr(tail_position_offset), 32, .release);
    @atomicStore(i32, rb.recordMsgTypeIdPtr(16), 7, .monotonic);
    @memcpy(rb.buffer[24..32], "payload8");
    @atomicStore(i32, rb.recordLengthPtr(16), 16, .release);

    const unblocked = rb.unblock();
    try std.testing.expect(unblocked);
    try std.testing.expectEqual(@as(i32, 16), @atomicLoad(i32, rb.recordLengthPtr(0), .acquire));
    try std.testing.expectEqual(constants.padding_msg_type_id, @atomicLoad(i32, rb.recordMsgTypeIdPtr(0), .monotonic));

    resetTestState();
    const count = rb.read(&testCaptureHandler, 10);
    try std.testing.expectEqual(@as(u32, 1), count);
    try std.testing.expectEqual(@as(i32, 7), test_received_type);
    try std.testing.expectEqualStrings("payload8", test_received_payload_buf[0..8]);
}

test "utility accessors return correct values" {
    const allocator = std.testing.allocator;
    const cap: usize = 1024;
    const buf = try allocateAlignedBuffer(allocator, cap + trailer_length);
    defer allocator.free(buf);

    var rb = try RingBuffer.init(buf, false, null, null);

    try std.testing.expectEqual(cap, rb.getCapacity());
    try std.testing.expectEqual(cap / 8, rb.maxMessageLength());
    try std.testing.expectEqual(false, rb.isBlocking());
    try std.testing.expectEqual(@as(i64, 0), rb.producerPosition());
    try std.testing.expectEqual(@as(i64, 0), rb.consumerPosition());
    try std.testing.expectEqual(@as(usize, 0), rb.size());

    // Write a message and check that positions advance.
    try rb.write(1, "hello");
    try std.testing.expect(rb.producerPosition() > 0);
    try std.testing.expectEqual(@as(i64, 0), rb.consumerPosition());
    try std.testing.expect(rb.size() > 0);

    // Set and read consumer heartbeat.
    rb.setConsumerHeartbeatTime(123456789);
    try std.testing.expectEqual(@as(i64, 123456789), rb.consumerHeartbeatTime());
}

test "nextCorrelationId returns unique incrementing values" {
    const allocator = std.testing.allocator;
    const buf = try allocateAlignedBuffer(allocator, 1024 + trailer_length);
    defer allocator.free(buf);

    var rb = try RingBuffer.init(buf, false, null, null);

    const id1 = rb.nextCorrelationId();
    const id2 = rb.nextCorrelationId();
    const id3 = rb.nextCorrelationId();

    try std.testing.expectEqual(@as(i64, 0), id1);
    try std.testing.expectEqual(@as(i64, 1), id2);
    try std.testing.expectEqual(@as(i64, 2), id3);
}

test "init rejects buffer too small" {
    const allocator = std.testing.allocator;
    // Exactly trailer_length — no data capacity.
    const buf = try allocateAlignedBuffer(allocator, trailer_length);
    defer allocator.free(buf);

    const result = RingBuffer.init(buf, false, null, null);
    try std.testing.expectError(RingBuffer.InitError.BufferTooSmall, result);
}

test "init rejects non-power-of-two capacity" {
    const allocator = std.testing.allocator;
    // 300 bytes data capacity (not a power of 2) + trailer.
    const buf = try allocateAlignedBuffer(allocator, 300 + trailer_length);
    defer allocator.free(buf);

    const result = RingBuffer.init(buf, false, null, null);
    try std.testing.expectError(RingBuffer.InitError.CapacityNotPowerOfTwo, result);
}

test "write rejects invalid msg_type_id" {
    const allocator = std.testing.allocator;
    const buf = try allocateAlignedBuffer(allocator, 1024 + trailer_length);
    defer allocator.free(buf);

    var rb = try RingBuffer.init(buf, false, null, null);

    const result_zero = rb.write(0, "hello");
    try std.testing.expectError(RingBuffer.WriteError.InvalidMsgTypeId, result_zero);

    const result_neg = rb.write(-5, "hello");
    try std.testing.expectError(RingBuffer.WriteError.InvalidMsgTypeId, result_neg);
}

test "write rejects message too long" {
    const allocator = std.testing.allocator;
    const cap: usize = 128;
    const buf = try allocateAlignedBuffer(allocator, cap + trailer_length);
    defer allocator.free(buf);

    var rb = try RingBuffer.init(buf, false, null, null);
    // max_msg_length = 128 / 8 = 16 bytes.
    const too_long: [17]u8 = .{0} ** 17;
    const result = rb.write(1, &too_long);
    try std.testing.expectError(RingBuffer.WriteError.MessageTooLong, result);
}

test "calculateRequiredSize" {
    const size_non_blocking = RingBuffer.calculateRequiredSize(1024, false);
    try std.testing.expectEqual(1024 + trailer_length, size_non_blocking);

    const size_blocking = RingBuffer.calculateRequiredSize(1024, true);
    try std.testing.expectEqual(blocking_prefix_length + 1024 + trailer_length, size_blocking);
}

test "read with limit returns at most limit messages" {
    const allocator = std.testing.allocator;
    const buf = try allocateAlignedBuffer(allocator, 4096 + trailer_length);
    defer allocator.free(buf);

    var rb = try RingBuffer.init(buf, false, null, null);

    // Write 5 messages.
    var i: u32 = 0;
    while (i < 5) : (i += 1) {
        try rb.write(1, "msg");
    }

    resetTestState();
    // Read with limit of 3.
    const count = rb.read(&testCountHandler, 3);
    try std.testing.expectEqual(@as(u32, 3), count);

    resetTestState();
    // Read remaining.
    const count2 = rb.read(&testCountHandler, 10);
    try std.testing.expectEqual(@as(u32, 2), count2);
}
