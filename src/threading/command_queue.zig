//! Inter-event-loop command queue backed by an MPSC ring buffer.
//!
//! Commands are the only mechanism for cross-thread communication between
//! event loops. Each command carries a self-dispatching function pointer
//! and a data pointer.
//!
//! The queue wraps the MPSC ring buffer from the concurrent module,
//! serializing Command structs as ring buffer records.

const std = @import("std");
const RingBuffer = @import("../concurrent/ring_buffer.zig").RingBuffer;
const Command = @import("command.zig").Command;

/// A command queue backed by an MPSC ring buffer.
/// Multiple producers (any event loop) can enqueue commands.
/// A single consumer (the owning event loop) drains them.
pub const CommandQueue = struct {
    ring_buffer: *RingBuffer,

    /// The context pointer for the owning event loop.
    /// Passed as the first argument to every command handler.
    loop_context: *anyopaque,

    const command_msg_type_id: i32 = 1;

    pub fn init(ring_buffer: *RingBuffer, loop_context: *anyopaque) CommandQueue {
        return .{
            .ring_buffer = ring_buffer,
            .loop_context = loop_context,
        };
    }

    /// Enqueue a command. Called from any thread.
    /// Returns error if the ring buffer is full (back-pressure).
    pub fn enqueue(self: *CommandQueue, cmd: Command) !void {
        const bytes = std.mem.asBytes(&cmd);
        try self.ring_buffer.write(command_msg_type_id, bytes);
    }

    /// Drain up to `limit` commands, executing each handler inline.
    /// Returns the number of commands processed.
    /// Called only from the owning event loop's thread.
    pub fn drain(self: *CommandQueue, limit: u32) u32 {
        // Set the threadlocal so the static handler function can find us.
        current_draining_queue = self;
        defer current_draining_queue = null;

        return self.ring_buffer.read(dispatchCommand, limit);
    }

    fn dispatchCommand(_: i32, payload: []const u8) void {
        const queue = current_draining_queue.?;
        const cmd: *const Command = @ptrCast(@alignCast(payload.ptr));
        cmd.handler(queue.loop_context, cmd);
    }
};

// Thread-local used during drain. Set before read(), cleared after.
threadlocal var current_draining_queue: ?*CommandQueue = null;

/// Default buffer size for command queues (8 KiB — generous for command traffic).
pub const command_queue_buffer_length: usize = 8 * 1024;

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;
const record_alignment = @import("../concurrent/ring_buffer.zig").record_alignment;
const trailer_length = @import("../concurrent/ring_buffer.zig").trailer_length;

fn allocateAlignedBuffer(allocator: std.mem.Allocator, buf_size: usize) ![]align(record_alignment) u8 {
    const buf = try allocator.alignedAlloc(u8, @enumFromInt(std.math.log2(record_alignment)), buf_size);
    @memset(buf, 0);
    return buf;
}

test "enqueue and drain single command" {
    // Given — a command queue backed by a small ring buffer.
    const allocator = testing.allocator;
    const buf = try allocateAlignedBuffer(allocator, 1024 + trailer_length);
    defer allocator.free(buf);

    var rb = try RingBuffer.init(buf, false, null, null);

    const TestCtx = struct {
        value: u32 = 0,
    };
    var ctx = TestCtx{};

    var queue = CommandQueue.init(&rb, @ptrCast(&ctx));

    // When — enqueue a command that sets ctx.value to 42.
    const cmd = Command{
        .handler = struct {
            fn handle(loop_ctx: *anyopaque, _: *const Command) void {
                const c: *TestCtx = @ptrCast(@alignCast(loop_ctx));
                c.value = 42;
            }
        }.handle,
        .data = null,
    };
    try queue.enqueue(cmd);

    // Then — drain executes the handler.
    const drained = queue.drain(10);
    try testing.expectEqual(@as(u32, 1), drained);
    try testing.expectEqual(@as(u32, 42), ctx.value);
}

test "drain returns 0 when queue is empty" {
    // Given
    const allocator = testing.allocator;
    const buf = try allocateAlignedBuffer(allocator, 1024 + trailer_length);
    defer allocator.free(buf);

    var rb = try RingBuffer.init(buf, false, null, null);
    var dummy: u32 = 0;
    var queue = CommandQueue.init(&rb, @ptrCast(&dummy));

    // When / Then
    const drained = queue.drain(10);
    try testing.expectEqual(@as(u32, 0), drained);
}

test "enqueue multiple commands and drain with limit" {
    // Given
    const allocator = testing.allocator;
    const buf = try allocateAlignedBuffer(allocator, 4096 + trailer_length);
    defer allocator.free(buf);

    var rb = try RingBuffer.init(buf, false, null, null);

    const TestCtx = struct {
        count: u32 = 0,
    };
    var ctx = TestCtx{};
    var queue = CommandQueue.init(&rb, @ptrCast(&ctx));

    // When — enqueue 5 commands.
    var i: u32 = 0;
    while (i < 5) : (i += 1) {
        const cmd = Command{
            .handler = struct {
                fn handle(loop_ctx: *anyopaque, _: *const Command) void {
                    const c: *TestCtx = @ptrCast(@alignCast(loop_ctx));
                    c.count += 1;
                }
            }.handle,
            .data = null,
        };
        try queue.enqueue(cmd);
    }

    // Then — drain with limit 2 processes only 2.
    var drained = queue.drain(2);
    try testing.expectEqual(@as(u32, 2), drained);
    try testing.expectEqual(@as(u32, 2), ctx.count);

    // And the remaining 3 are still there.
    drained = queue.drain(10);
    try testing.expectEqual(@as(u32, 3), drained);
    try testing.expectEqual(@as(u32, 5), ctx.count);
}

test "command handler receives data pointer through ring buffer" {
    // Given — a command whose data pointer carries a value.
    const allocator = testing.allocator;
    const buf = try allocateAlignedBuffer(allocator, 1024 + trailer_length);
    defer allocator.free(buf);

    var rb = try RingBuffer.init(buf, false, null, null);

    const TestCtx = struct {
        received: u64 = 0,
    };
    var ctx = TestCtx{};
    var queue = CommandQueue.init(&rb, @ptrCast(&ctx));

    // Note: data is a pointer that is serialized as bytes into the ring buffer.
    // The handler receives a copy of the Command struct, so cmd.data still
    // holds the original pointer value (valid within the same process).
    var payload: u64 = 0xDEAD_BEEF;
    const cmd = Command{
        .handler = struct {
            fn handle(loop_ctx: *anyopaque, cmd_ptr: *const Command) void {
                const c: *TestCtx = @ptrCast(@alignCast(loop_ctx));
                if (cmd_ptr.data) |d| {
                    const val: *const u64 = @ptrCast(@alignCast(d));
                    c.received = val.*;
                }
            }
        }.handle,
        .data = @ptrCast(&payload),
    };
    try queue.enqueue(cmd);

    // When
    const drained = queue.drain(10);

    // Then
    try testing.expectEqual(@as(u32, 1), drained);
    try testing.expectEqual(@as(u64, 0xDEAD_BEEF), ctx.received);
}

test "threadlocal is null outside of drain" {
    // Given / When / Then — the threadlocal is null when no drain is in progress.
    try testing.expectEqual(@as(?*CommandQueue, null), current_draining_queue);
}
