//! BrokerThreads — owns three ThreadRunners for the broker's event loops.
//!
//! Manages the lifecycle of the control, sender, and receiver threads
//! in DEDICATED mode. Start order: receiver → sender → control.
//! Shutdown order: receiver → sender → control.

const std = @import("std");
const platform = @import("ringloom_common").platform;
const ThreadRunner = platform.ThreadRunner;
const EventLoop = platform.EventLoop;
const IdleStrategy = platform.IdleStrategy;

pub const BrokerThreads = struct {
    control_runner: ThreadRunner,
    sender_runner: ThreadRunner,
    receiver_runner: ThreadRunner,

    pub fn init(
        control_loop: EventLoop,
        sender_loop: EventLoop,
        receiver_loop: EventLoop,
        control_idle: IdleStrategy,
        sender_idle: IdleStrategy,
        receiver_idle: IdleStrategy,
    ) BrokerThreads {
        return .{
            .control_runner = ThreadRunner.init("ringloom-control", control_loop, control_idle),
            .sender_runner = ThreadRunner.init("ringloom-sender", sender_loop, sender_idle),
            .receiver_runner = ThreadRunner.init("ringloom-receiver", receiver_loop, receiver_idle),
        };
    }

    /// Start all threads. Order: receiver first, then sender, then control.
    /// Receiver starts first so it's ready to process incoming packets by the
    /// time the sender or control loop triggers outbound traffic.
    pub fn start(self: *BrokerThreads) !void {
        try self.receiver_runner.start();
        try self.sender_runner.start();
        try self.control_runner.start();
    }

    /// Graceful shutdown. Order: receiver, sender, control (reverse of hot-path
    /// dependency — stop accepting new work before stopping producers).
    pub fn shutdown(self: *BrokerThreads) void {
        // 1. Stop receiver first — no new incoming messages.
        self.receiver_runner.stop();
        self.receiver_runner.join();

        // 2. Stop sender — flush remaining sends, then exit.
        self.sender_runner.stop();
        self.sender_runner.join();

        // 3. Stop control loop last — it may need to process final deregistrations.
        self.control_runner.stop();
        self.control_runner.join();
    }

    /// Returns true if the control runner is still running.
    pub fn isRunning(self: *const BrokerThreads) bool {
        return self.control_runner.running.load();
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "BrokerThreads start and shutdown" {
    // Given — three trivial event loops.
    const TestLoop = struct {
        call_count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

        fn doWork(ctx: *anyopaque) u32 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            _ = self.call_count.fetchAdd(1, .monotonic);
            return 0;
        }

        fn onClose(_: *anyopaque) void {}
    };

    var ctrl = TestLoop{};
    var send = TestLoop{};
    var recv = TestLoop{};

    var threads = BrokerThreads.init(
        EventLoop{ .context = @ptrCast(&ctrl), .doWorkFn = TestLoop.doWork, .onCloseFn = TestLoop.onClose },
        EventLoop{ .context = @ptrCast(&send), .doWorkFn = TestLoop.doWork, .onCloseFn = TestLoop.onClose },
        EventLoop{ .context = @ptrCast(&recv), .doWorkFn = TestLoop.doWork, .onCloseFn = TestLoop.onClose },
        .sleeping,
        .sleeping,
        .sleeping,
    );

    // When
    try threads.start();
    try testing.expect(threads.isRunning());
    platform.sleepNanos(5 * std.time.ns_per_ms);

    threads.shutdown();

    // Then — all threads ran and stopped.
    try testing.expect(!threads.isRunning());
    try testing.expect(ctrl.call_count.load(.monotonic) > 0);
    try testing.expect(send.call_count.load(.monotonic) > 0);
    try testing.expect(recv.call_count.load(.monotonic) > 0);
}

test "BrokerThreads isRunning returns false before start" {
    // Given
    const TestLoop = struct {
        fn doWork(_: *anyopaque) u32 {
            return 0;
        }
        fn onClose(_: *anyopaque) void {}
    };

    var dummy: u8 = 0;
    const el = EventLoop{ .context = @ptrCast(&dummy), .doWorkFn = TestLoop.doWork, .onCloseFn = TestLoop.onClose };

    var threads = BrokerThreads.init(el, el, el, .sleeping, .sleeping, .sleeping);

    // When / Then
    try testing.expect(!threads.isRunning());
}
