//! BrokerThreads — owns the ThreadRunners for the broker's event loops.
//!
//! Manages DEDICATED, SHARED_NETWORK, and SHARED layouts. Start order keeps
//! network work available before control can trigger traffic. Shutdown stops
//! network work before control cleanup.

const std = @import("std");
const platform = @import("ringloom_common").platform;
const ThreadRunner = platform.ThreadRunner;
const EventLoop = platform.EventLoop;
const IdleStrategy = platform.IdleStrategy;
const ThreadingMode = @import("threading_mode.zig").ThreadingMode;

pub const BrokerThreads = struct {
    layout: Layout,

    const Dedicated = struct {
        control_runner: ThreadRunner,
        sender_runner: ThreadRunner,
        receiver_runner: ThreadRunner,
    };

    const SharedNetwork = struct {
        control_runner: ThreadRunner,
        network_runner: ThreadRunner,
    };

    const Shared = struct {
        runner: ThreadRunner,
    };

    const Layout = union(ThreadingMode) {
        dedicated: Dedicated,
        shared_network: SharedNetwork,
        shared: Shared,
    };

    pub fn init(
        control_loop: EventLoop,
        sender_loop: EventLoop,
        receiver_loop: EventLoop,
        control_idle: IdleStrategy,
        sender_idle: IdleStrategy,
        receiver_idle: IdleStrategy,
    ) BrokerThreads {
        return initDedicated(
            control_loop,
            sender_loop,
            receiver_loop,
            control_idle,
            sender_idle,
            receiver_idle,
        );
    }

    pub fn initDedicated(
        control_loop: EventLoop,
        sender_loop: EventLoop,
        receiver_loop: EventLoop,
        control_idle: IdleStrategy,
        sender_idle: IdleStrategy,
        receiver_idle: IdleStrategy,
    ) BrokerThreads {
        return .{
            .layout = .{
                .dedicated = .{
                    .control_runner = ThreadRunner.init("ringloom-control", control_loop, control_idle),
                    .sender_runner = ThreadRunner.init("ringloom-sender", sender_loop, sender_idle),
                    .receiver_runner = ThreadRunner.init("ringloom-recv", receiver_loop, receiver_idle),
                },
            },
        };
    }

    pub fn initSharedNetwork(
        control_loop: EventLoop,
        network_loop: EventLoop,
        control_idle: IdleStrategy,
        network_idle: IdleStrategy,
    ) BrokerThreads {
        return .{
            .layout = .{
                .shared_network = .{
                    .control_runner = ThreadRunner.init("ringloom-control", control_loop, control_idle),
                    .network_runner = ThreadRunner.init("ringloom-network", network_loop, network_idle),
                },
            },
        };
    }

    pub fn initShared(
        shared_loop: EventLoop,
        idle: IdleStrategy,
    ) BrokerThreads {
        return .{
            .layout = .{
                .shared = .{
                    .runner = ThreadRunner.init("ringloom-shared", shared_loop, idle),
                },
            },
        };
    }

    pub fn setCpuAffinities(
        self: *BrokerThreads,
        sender_cpu_affinity: ?u32,
        receiver_cpu_affinity: ?u32,
    ) void {
        const network_affinity = receiver_cpu_affinity orelse sender_cpu_affinity;
        switch (self.layout) {
            .dedicated => |*layout| {
                layout.sender_runner.cpu_affinity = sender_cpu_affinity;
                layout.receiver_runner.cpu_affinity = receiver_cpu_affinity;
            },
            .shared_network => |*layout| {
                layout.network_runner.cpu_affinity = network_affinity;
            },
            .shared => |*layout| {
                layout.runner.cpu_affinity = network_affinity;
            },
        }
    }

    /// Start all threads. Network loops start before control.
    pub fn start(self: *BrokerThreads) !void {
        switch (self.layout) {
            .dedicated => |*layout| {
                try layout.sender_runner.start();
                errdefer layout.sender_runner.stopAndJoin();
                try layout.receiver_runner.start();
                errdefer layout.receiver_runner.stopAndJoin();
                try layout.control_runner.start();
            },
            .shared_network => |*layout| {
                try layout.network_runner.start();
                errdefer layout.network_runner.stopAndJoin();
                try layout.control_runner.start();
            },
            .shared => |*layout| {
                try layout.runner.start();
            },
        }
    }

    /// Graceful shutdown. Stop network work before control cleanup.
    pub fn shutdown(self: *BrokerThreads) void {
        switch (self.layout) {
            .dedicated => |*layout| {
                layout.sender_runner.stopAndJoin();
                layout.receiver_runner.stopAndJoin();
                layout.control_runner.stopAndJoin();
            },
            .shared_network => |*layout| {
                layout.network_runner.stopAndJoin();
                layout.control_runner.stopAndJoin();
            },
            .shared => |*layout| {
                layout.runner.stopAndJoin();
            },
        }
    }

    /// Returns true if the primary runner is still running.
    pub fn isRunning(self: *const BrokerThreads) bool {
        return switch (self.layout) {
            .dedicated => |*layout| layout.control_runner.running.load(),
            .shared_network => |*layout| layout.control_runner.running.load(),
            .shared => |*layout| layout.runner.running.load(),
        };
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "BrokerThreads start and shutdown" {
    // Given — two trivial event loops.
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
    var sender = TestLoop{};
    var receiver = TestLoop{};

    var threads = BrokerThreads.initDedicated(
        EventLoop{ .context = @ptrCast(&ctrl), .doWorkFn = TestLoop.doWork, .onCloseFn = TestLoop.onClose },
        EventLoop{ .context = @ptrCast(&sender), .doWorkFn = TestLoop.doWork, .onCloseFn = TestLoop.onClose },
        EventLoop{ .context = @ptrCast(&receiver), .doWorkFn = TestLoop.doWork, .onCloseFn = TestLoop.onClose },
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
    try testing.expect(sender.call_count.load(.monotonic) > 0);
    try testing.expect(receiver.call_count.load(.monotonic) > 0);
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

test "BrokerThreads shared network starts control and network runners" {
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
    var network = TestLoop{};

    var threads = BrokerThreads.initSharedNetwork(
        EventLoop{ .context = @ptrCast(&ctrl), .doWorkFn = TestLoop.doWork, .onCloseFn = TestLoop.onClose },
        EventLoop{ .context = @ptrCast(&network), .doWorkFn = TestLoop.doWork, .onCloseFn = TestLoop.onClose },
        .sleeping,
        .sleeping,
    );

    try threads.start();
    platform.sleepNanos(5 * std.time.ns_per_ms);
    threads.shutdown();

    try testing.expect(ctrl.call_count.load(.monotonic) > 0);
    try testing.expect(network.call_count.load(.monotonic) > 0);
}

test "BrokerThreads shared starts one runner" {
    const TestLoop = struct {
        call_count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

        fn doWork(ctx: *anyopaque) u32 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            _ = self.call_count.fetchAdd(1, .monotonic);
            return 0;
        }

        fn onClose(_: *anyopaque) void {}
    };

    var shared = TestLoop{};

    var threads = BrokerThreads.initShared(
        EventLoop{ .context = @ptrCast(&shared), .doWorkFn = TestLoop.doWork, .onCloseFn = TestLoop.onClose },
        .sleeping,
    );

    try threads.start();
    platform.sleepNanos(5 * std.time.ns_per_ms);
    threads.shutdown();

    try testing.expect(shared.call_count.load(.monotonic) > 0);
}
