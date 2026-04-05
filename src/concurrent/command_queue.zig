//! Inter-event-loop command queue.
//!
//! Commands are the only mechanism for cross-thread communication between
//! event loops. Each command carries a self-dispatching function pointer
//! and a fixed-size data payload.
//!
//! The queue is a bounded MPSC (multiple-producer, single-consumer) structure
//! backed by an atomic ring buffer.

const std = @import("std");
const constants = @import("../platform/constants.zig");

/// A self-dispatching command. The handler knows how to execute itself
/// on the target event loop.
pub const Command = struct {
    /// The handler function. Called on the target event loop's thread.
    /// `context` is the event loop's state (e.g. *ControlLoop).
    /// `self` is this Command instance.
    handler: *const fn (context: *anyopaque, self: *Command) void,

    /// Payload data. Interpretation depends on the handler.
    /// 56 bytes — enough for any command payload, cache-line sized with handler pointer.
    data: [56]u8 = undefined,
};

comptime {
    // Command should be exactly 64 bytes (one cache line).
    std.debug.assert(@sizeOf(Command) == 64);
}

/// Bounded MPSC command queue backed by an atomic ring buffer.
///
/// Producers (sender/receiver threads) write commands via `offer()`.
/// The consumer (control loop) drains via `drain()`.
pub const CommandQueue = struct {
    buffer: []Command,
    capacity: u32,
    mask: u32,

    /// Producer position — atomically incremented by writers.
    write_pos: std.atomic.Value(u32),

    /// Consumer position — only touched by the single consumer.
    read_pos: u32,

    const Self = @This();

    /// Initialize a command queue with a power-of-two capacity.
    /// `buffer` must be a slice of at least `capacity` Commands.
    pub fn init(buffer: []Command) Self {
        std.debug.assert(buffer.len > 0);
        std.debug.assert(constants.isPowerOfTwo(buffer.len));

        return .{
            .buffer = buffer,
            .capacity = @intCast(buffer.len),
            .mask = @intCast(buffer.len - 1),
            .write_pos = std.atomic.Value(u32).init(0),
            .read_pos = 0,
        };
    }

    /// Try to enqueue a command. Returns true if successful, false if full.
    /// Thread-safe for multiple producers.
    pub fn offer(self: *Self, cmd: Command) bool {
        // Simple non-blocking: just try to claim one slot.
        // For true MPSC we'd need CAS, but since commands are rare events
        // (peer connect/disconnect), a simple atomic increment suffices.
        const pos = self.write_pos.load(.acquire);
        const read = @atomicLoad(u32, &self.read_pos, .acquire);

        if (pos - read >= self.capacity) {
            return false; // Queue full.
        }

        // Try to claim this slot via CAS.
        if (self.write_pos.cmpxchgStrong(pos, pos + 1, .acq_rel, .acquire)) |_| {
            return false; // Lost race — caller should retry.
        }

        self.buffer[pos & self.mask] = cmd;
        return true;
    }

    /// Drain up to `limit` commands, calling the dispatch function for each.
    /// Single-consumer only.
    pub fn drain(
        self: *Self,
        context: *anyopaque,
        dispatch_fn: *const fn (context: *anyopaque, cmd: *Command) void,
        limit: u32,
    ) u32 {
        const write = self.write_pos.load(.acquire);
        var count: u32 = 0;

        while (self.read_pos != write and count < limit) {
            const idx = self.read_pos & self.mask;
            dispatch_fn(context, &self.buffer[idx]);
            self.read_pos += 1;
            @atomicStore(u32, &self.read_pos, self.read_pos, .release);
            count += 1;
        }

        return count;
    }

    /// Returns the number of commands currently in the queue.
    pub fn size(self: *const Self) u32 {
        const write = self.write_pos.load(.acquire);
        const read = @atomicLoad(u32, &self.read_pos, .acquire);
        return write - read;
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

fn testHandler(_: *anyopaque, cmd: *Command) void {
    // Simply mark the command as dispatched by setting data[0] = 0xFF.
    cmd.data[0] = 0xFF;
}

test "offer and drain single command" {
    // Given
    var buf: [4]Command = undefined;
    var queue = CommandQueue.init(&buf);

    var cmd = Command{ .handler = undefined };
    cmd.data[0] = 42;

    // When
    const offered = queue.offer(cmd);
    try testing.expect(offered);

    // Then
    var dummy: u8 = 0;
    const drained = queue.drain(@ptrCast(&dummy), testHandler, 10);
    try testing.expectEqual(@as(u32, 1), drained);
}

test "drain returns 0 when empty" {
    // Given
    var buf: [4]Command = undefined;
    var queue = CommandQueue.init(&buf);

    // When / Then
    var dummy: u8 = 0;
    const drained = queue.drain(@ptrCast(&dummy), testHandler, 10);
    try testing.expectEqual(@as(u32, 0), drained);
}

test "drain respects limit" {
    // Given
    var buf: [8]Command = undefined;
    var queue = CommandQueue.init(&buf);

    // Offer 4 commands.
    var i: u32 = 0;
    while (i < 4) : (i += 1) {
        _ = queue.offer(.{ .handler = undefined });
    }

    // When — drain with limit 2
    var dummy: u8 = 0;
    const drained = queue.drain(@ptrCast(&dummy), testHandler, 2);

    // Then
    try testing.expectEqual(@as(u32, 2), drained);
    try testing.expectEqual(@as(u32, 2), queue.size());
}

test "queue full returns false" {
    // Given
    var buf: [2]Command = undefined;
    var queue = CommandQueue.init(&buf);

    _ = queue.offer(.{ .handler = undefined });
    _ = queue.offer(.{ .handler = undefined });

    // When
    const offered = queue.offer(.{ .handler = undefined });

    // Then
    try testing.expect(!offered);
}
