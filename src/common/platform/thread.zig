//! Thread management for the BRZ broker.
//!
//! Provides EventLoop interface, IdleStrategy, and ThreadRunner for
//! running named event loops on dedicated threads.

const std = @import("std");
const builtin = @import("builtin");
const AtomicBool = @import("atomic.zig").AtomicBool;
const platform_io = @import("io.zig");

/// Interface that all event loops implement.
/// The ThreadRunner calls doWork() in a loop and onClose() when stopping.
pub const EventLoop = struct {
    /// Pointer to the concrete event loop implementation.
    context: *anyopaque,

    /// Perform one unit of work.
    /// Returns the number of work items processed (0 = idle).
    doWorkFn: *const fn (context: *anyopaque) u32,

    /// Called once after the event loop exits.
    /// Used for cleanup (closing sockets, flushing buffers, etc.).
    onCloseFn: *const fn (context: *anyopaque) void,

    pub fn doWork(self: EventLoop) u32 {
        return self.doWorkFn(self.context);
    }

    pub fn onClose(self: EventLoop) void {
        self.onCloseFn(self.context);
    }
};

/// Idle strategy called when the event loop has no work to do.
/// Matches the architecture doc's idle strategy table.
pub const IdleStrategy = union(enum) {
    /// CPU pause instruction when idle. Lowest latency, highest CPU usage.
    busy_spin,

    /// sched_yield() when idle. Low latency, shares CPU.
    yielding,

    /// nanosleep(1µs) when idle. Balanced.
    sleeping,

    /// Spin → yield → sleep (exponential backoff). Production default.
    backoff: BackoffState,

    /// Kernel-level blocking (futex/ulock). Lowest CPU, higher latency.
    /// The process synchronizer is used to wake the thread.
    blocking: *const fn () void,

    pub fn idle(self: *IdleStrategy, work_count: u32) void {
        if (work_count > 0) {
            // Reset backoff state if we did work.
            if (self.* == .backoff) {
                self.backoff.reset();
            }
            return;
        }

        switch (self.*) {
            .busy_spin => std.atomic.spinLoopHint(),
            .yielding => std.Thread.yield() catch {},
            .sleeping => sleepNanos(1_000), // 1µs
            .backoff => |*state| state.step(),
            .blocking => |wake_fn| wake_fn(),
        }
    }
};

pub const BackoffState = struct {
    spins: u32 = 0,
    yields: u32 = 0,

    const max_spins: u32 = 10;
    const max_yields: u32 = 20;

    pub fn step(self: *BackoffState) void {
        if (self.spins < max_spins) {
            self.spins += 1;
            std.atomic.spinLoopHint();
        } else if (self.yields < max_yields) {
            self.yields += 1;
            std.Thread.yield() catch {};
        } else {
            sleepNanos(1_000); // 1µs
        }
    }

    pub fn reset(self: *BackoffState) void {
        self.spins = 0;
        self.yields = 0;
    }
};

pub fn sleepNanos(duration_ns: u64) void {
    const io = platform_io.default();
    std.Io.sleep(io, .fromNanoseconds(@intCast(duration_ns)), .awake) catch unreachable;
}

pub const ThreadRunner = struct {
    /// Name shown in `ps`, `top`, `htop` (max 15 chars on Linux).
    name: []const u8,

    /// The event loop to run on this thread.
    event_loop: EventLoop,

    /// Idle strategy for when the event loop has no work.
    idle_strategy: IdleStrategy,

    /// Atomic flag — set to false to stop the event loop.
    running: AtomicBool,

    /// Handle to the spawned thread (set after start()).
    thread: ?std.Thread = null,

    /// Optional CPU core to pin this thread to (Linux only).
    /// When set, `setThreadAffinity` is called after thread creation.
    cpu_affinity: ?u32 = null,

    const Self = @This();

    pub fn init(
        name: []const u8,
        event_loop: EventLoop,
        idle_strategy: IdleStrategy,
    ) Self {
        return .{
            .name = name,
            .event_loop = event_loop,
            .idle_strategy = idle_strategy,
            .running = AtomicBool.init(false),
            .thread = null,
            .cpu_affinity = null,
        };
    }

    /// Spawn the thread and start the event loop.
    pub fn start(self: *Self) !void {
        self.running.store(true);
        self.thread = try std.Thread.spawn(.{}, threadMain, .{self});
    }

    /// Signal the event loop to stop. Non-blocking.
    pub fn stop(self: *Self) void {
        self.running.store(false);
    }

    /// Wait for the thread to finish. Blocks until the thread exits.
    pub fn join(self: *Self) void {
        if (self.thread) |t| {
            t.join();
            self.thread = null;
        }
    }

    /// Stop and join in one call.
    pub fn stopAndJoin(self: *Self) void {
        self.stop();
        self.join();
    }

    /// The thread entry point.
    fn threadMain(self: *Self) void {
        // Step 1: Set the thread name for debugging.
        setThreadName(self.name);

        // Step 2: Apply CPU affinity if configured.
        if (self.cpu_affinity) |core_id| {
            setThreadAffinity(core_id) catch {};
        }

        // Step 3: Run the event loop.
        while (self.running.load()) {
            const work_count = self.event_loop.doWork();
            self.idle_strategy.idle(work_count);
        }

        // Step 4: Cleanup.
        self.event_loop.onClose();
    }
};

/// Set the current thread's name. Truncates to platform limits silently.
///
/// Linux: max 15 chars. macOS: max 63 chars. Windows: no limit.
pub fn setThreadName(name: []const u8) void {
    switch (comptime builtin.os.tag) {
        .linux => setThreadNameLinux(name),
        .macos => setThreadNameMacos(name),
        else => {}, // Unsupported — silently ignore.
    }
}

fn setThreadNameLinux(name: []const u8) void {
    // Linux limit: 15 chars + null terminator = 16 bytes.
    var buf: [16]u8 = undefined;
    const len = @min(name.len, 15);
    @memcpy(buf[0..len], name[0..len]);
    buf[len] = 0;

    _ = std.os.linux.prctl(
        @intFromEnum(std.os.linux.PR.SET_NAME),
        @intFromPtr(&buf),
        0,
        0,
        0,
    );
}

fn setThreadNameMacos(name: []const u8) void {
    // macOS: pthread_setname_np(name) — sets the current thread's name.
    // Max 63 chars.
    var buf: [64]u8 = undefined;
    const len = @min(name.len, 63);
    @memcpy(buf[0..len], name[0..len]);
    buf[len] = 0;

    // Use Zig's std.c for the pthread call.
    // On macOS, pthread_setname_np takes only one argument (the name).
    if (comptime builtin.os.tag == .macos) {
        // macOS-specific: pthread_setname_np only takes the name.
        const c = struct {
            extern "c" fn pthread_setname_np(name: [*:0]const u8) c_int;
        };
        _ = c.pthread_setname_np(@ptrCast(&buf));
    }
}

/// Pin the current thread to a specific CPU core (Linux only).
///
/// Returns error on failure or if the platform doesn't support affinity.
pub fn setThreadAffinity(cpu_id: u32) !void {
    switch (comptime builtin.os.tag) {
        .linux => {
            var mask: std.os.linux.cpu_set_t = @splat(0);
            mask[cpu_id / @bitSizeOf(usize)] = @as(usize, 1) << @intCast(cpu_id % @bitSizeOf(usize));
            std.os.linux.sched_setaffinity(0, &mask) catch return error.AffinitySetFailed;
        },
        else => return error.NotSupported,
    }
}

// ── Tests ─────────────────────────────────────────────────────────────

test "ThreadRunner start and stop" {
    const TestLoop = struct {
        call_count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

        fn doWork(ctx: *anyopaque) u32 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            _ = self.call_count.fetchAdd(1, .monotonic);
            return 1; // Did work.
        }

        fn onClose(_: *anyopaque) void {}
    };

    var loop = TestLoop{};
    var runner = ThreadRunner.init(
        "test-thread",
        .{
            .context = @ptrCast(&loop),
            .doWorkFn = TestLoop.doWork,
            .onCloseFn = TestLoop.onClose,
        },
        .{ .backoff = .{} },
    );

    try runner.start();
    // Let it run briefly.
    sleepNanos(5 * std.time.ns_per_ms);
    runner.stopAndJoin();

    // It should have run at least once.
    try std.testing.expect(loop.call_count.load(.monotonic) > 0);
}

test "IdleStrategy backoff resets on work" {
    var strategy = IdleStrategy{ .backoff = .{} };

    // Idle several times.
    strategy.idle(0);
    strategy.idle(0);
    strategy.idle(0);
    try std.testing.expect(strategy.backoff.spins == 3);

    // Do work → reset.
    strategy.idle(1);
    try std.testing.expect(strategy.backoff.spins == 0);
    try std.testing.expect(strategy.backoff.yields == 0);
}

test "BackoffState progresses through phases" {
    var state = BackoffState{};

    // Spin phase.
    var i: u32 = 0;
    while (i < BackoffState.max_spins) : (i += 1) {
        state.step();
    }
    try std.testing.expectEqual(BackoffState.max_spins, state.spins);
    try std.testing.expectEqual(@as(u32, 0), state.yields);

    // Yield phase.
    i = 0;
    while (i < BackoffState.max_yields) : (i += 1) {
        state.step();
    }
    try std.testing.expectEqual(BackoffState.max_yields, state.yields);

    // Sleep phase (just verify it doesn't crash).
    state.step();

    // Reset.
    state.reset();
    try std.testing.expectEqual(@as(u32, 0), state.spins);
    try std.testing.expectEqual(@as(u32, 0), state.yields);
}
