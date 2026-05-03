//! Composite event loop that combines two event loops into one.
//!
//! Used in SHARED_NETWORK mode (sender + receiver on one thread) and
//! SHARED mode (all three loops on one thread via nesting).
//!
//! doWork() calls both inner loops and sums their work counts.
//! onClose() calls both inner loops' onClose in order.

const platform = @import("ringloom_common").platform;
const EventLoop = platform.EventLoop;

/// Combines two event loops into one. doWork() calls both, summing work counts.
pub const CompositeEventLoop = struct {
    first: EventLoop,
    second: EventLoop,

    /// Return an EventLoop interface backed by this composite.
    pub fn eventLoop(self: *CompositeEventLoop) EventLoop {
        return .{
            .context = @ptrCast(self),
            .doWorkFn = doWorkFn,
            .onCloseFn = onCloseFn,
        };
    }

    fn doWorkFn(ctx: *anyopaque) u32 {
        const self: *CompositeEventLoop = @ptrCast(@alignCast(ctx));
        var work: u32 = 0;
        work += self.first.doWork();
        work += self.second.doWork();
        return work;
    }

    fn onCloseFn(ctx: *anyopaque) void {
        const self: *CompositeEventLoop = @ptrCast(@alignCast(ctx));
        self.first.onClose();
        self.second.onClose();
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const std = @import("std");
const testing = std.testing;

test "CompositeEventLoop calls both doWork functions" {
    // Given
    const Counter = struct {
        value: u32 = 0,

        fn doWork(ctx: *anyopaque) u32 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.value += 1;
            return 1;
        }

        fn onClose(_: *anyopaque) void {}
    };

    var first = Counter{};
    var second = Counter{};

    var composite = CompositeEventLoop{
        .first = .{
            .context = @ptrCast(&first),
            .doWorkFn = Counter.doWork,
            .onCloseFn = Counter.onClose,
        },
        .second = .{
            .context = @ptrCast(&second),
            .doWorkFn = Counter.doWork,
            .onCloseFn = Counter.onClose,
        },
    };

    // When
    const el = composite.eventLoop();
    const work = el.doWork();

    // Then — both were called, work count is sum.
    try testing.expectEqual(@as(u32, 2), work);
    try testing.expectEqual(@as(u32, 1), first.value);
    try testing.expectEqual(@as(u32, 1), second.value);
}

test "CompositeEventLoop onClose calls both" {
    // Given
    const Closer = struct {
        closed: bool = false,

        fn doWork(_: *anyopaque) u32 {
            return 0;
        }

        fn onClose(ctx: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.closed = true;
        }
    };

    var first = Closer{};
    var second = Closer{};

    var composite = CompositeEventLoop{
        .first = .{
            .context = @ptrCast(&first),
            .doWorkFn = Closer.doWork,
            .onCloseFn = Closer.onClose,
        },
        .second = .{
            .context = @ptrCast(&second),
            .doWorkFn = Closer.doWork,
            .onCloseFn = Closer.onClose,
        },
    };

    // When
    const el = composite.eventLoop();
    el.onClose();

    // Then
    try testing.expect(first.closed);
    try testing.expect(second.closed);
}

test "CompositeEventLoop multiple doWork calls accumulate" {
    // Given
    const Counter = struct {
        value: u32 = 0,

        fn doWork(ctx: *anyopaque) u32 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.value += 1;
            return 1;
        }

        fn onClose(_: *anyopaque) void {}
    };

    var first = Counter{};
    var second = Counter{};

    var composite = CompositeEventLoop{
        .first = .{
            .context = @ptrCast(&first),
            .doWorkFn = Counter.doWork,
            .onCloseFn = Counter.onClose,
        },
        .second = .{
            .context = @ptrCast(&second),
            .doWorkFn = Counter.doWork,
            .onCloseFn = Counter.onClose,
        },
    };

    // When — call doWork 3 times
    const el = composite.eventLoop();
    _ = el.doWork();
    _ = el.doWork();
    _ = el.doWork();

    // Then
    try testing.expectEqual(@as(u32, 3), first.value);
    try testing.expectEqual(@as(u32, 3), second.value);
}
