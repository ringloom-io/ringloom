// SPDX-License-Identifier: Apache-2.0
//! TopicPrefetcher — the single extra thread topics introduce (spec 05 §4).
//!
//! Round-robins ringloom-queue `maintenancePoll(budget)` over every open topic
//! queue so the receiver loop's append/writeAtIndex hits resident pages (write
//! pre-touch, read prefetch, retention/cleaning). It only touches mmap pages and
//! metadata — never queue contents — so the single-writer invariant holds.
//!
//! The store is owned by the receiver loop; the prefetcher reads the queue set.
//! Because queues may be opened/closed concurrently by the receiver thread, the
//! store iteration is guarded by a lightweight mutex that the receiver also takes
//! around structural mutations (open/close). maintenancePoll itself is safe to
//! run alongside appends on a given queue.

const std = @import("std");
const topic_store = @import("topic_store.zig");

const TopicStore = topic_store.TopicStore;

const maintenance_budget: u32 = 32;

/// Minimal spinlock guarding structural store mutation (open/close) against the
/// prefetcher's iteration. Held only briefly; contention is rare.
pub const SpinLock = struct {
    flag: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    pub fn lock(self: *SpinLock) void {
        while (self.flag.swap(true, .acquire)) {
            std.atomic.spinLoopHint();
        }
    }

    pub fn unlock(self: *SpinLock) void {
        self.flag.store(false, .release);
    }
};

pub const TopicPrefetcher = struct {
    store: *TopicStore,
    mutex: *SpinLock,
    running: std.atomic.Value(bool),
    thread: ?std.Thread = null,
    cpu_affinity: i32 = -1,
    idle_sleep_ns: u64 = 50 * std.time.ns_per_us,

    pub fn init(store: *TopicStore, mutex: *SpinLock, cpu_affinity: i32) TopicPrefetcher {
        return .{
            .store = store,
            .mutex = mutex,
            .running = std.atomic.Value(bool).init(false),
            .cpu_affinity = cpu_affinity,
        };
    }

    pub fn start(self: *TopicPrefetcher) !void {
        if (self.thread != null) return;
        self.running.store(true, .release);
        self.thread = try std.Thread.spawn(.{}, run, .{self});
    }

    pub fn stop(self: *TopicPrefetcher) void {
        self.running.store(false, .release);
        if (self.thread) |t| {
            t.join();
            self.thread = null;
        }
    }

    fn run(self: *TopicPrefetcher) void {
        if (self.cpu_affinity >= 0) setAffinity(self.cpu_affinity);
        while (self.running.load(.acquire)) {
            const did = self.pollOnce();
            if (did == 0) {
                // Brief idle backoff — use platform nanosleep.
                const builtin = @import("builtin");
                if (builtin.os.tag == .linux) {
                    const ts: std.os.linux.timespec = .{ .sec = 0, .nsec = @intCast(@min(self.idle_sleep_ns, 999_999_999)) };
                    _ = std.os.linux.nanosleep(&ts, null);
                }
            }
        }
    }

    /// One bounded maintenance pass over all open queues. Returns total work.
    pub fn pollOnce(self: *TopicPrefetcher) u32 {
        var did: u32 = 0;
        self.mutex.lock();
        defer self.mutex.unlock();
        var it = self.store.openQueues();
        while (it.next()) |tqp| {
            const res = tqp.*.maintenancePoll(maintenance_budget) catch continue;
            if (res != .idle) did += 1;
        }
        return did;
    }

    fn setAffinity(cpu: i32) void {
        if (@import("builtin").os.tag != .linux) return;
        var set: std.os.linux.cpu_set_t = std.mem.zeroes(std.os.linux.cpu_set_t);
        const idx: usize = @intCast(@divFloor(cpu, @bitSizeOf(usize)));
        const bit: usize = @intCast(@mod(cpu, @bitSizeOf(usize)));
        if (idx < set.len) set[idx] |= (@as(usize, 1) << @intCast(bit));
        _ = std.os.linux.sched_setaffinity(0, &set) catch {};
    }
};

const testing = std.testing;
const TopicConfig = @import("topic_config.zig").TopicConfig;

test "prefetcher poll runs maintenance without mutating queue contents" {
    const allocator = testing.allocator;
    var tmp = testing.tmpDir(.{ .iterate = true });
    defer tmp.cleanup();
    const base = try std.fmt.allocPrint(allocator, ".zig-cache/tmp/{s}", .{tmp.sub_path[0..]});
    defer allocator.free(base);

    var store = TopicStore.init(allocator, base, 1 << 20, 1 << 20);
    defer store.deinit();
    var mutex = SpinLock{};

    const cfg = TopicConfig.fromName("FAST_DAILY", 4, false);
    const m = try store.openMaster(1, cfg, 1);
    _ = try m.append("a");
    _ = try m.append("b");

    var pf = TopicPrefetcher.init(&store, &mutex, -1);
    _ = pf.pollOnce(); // should not crash or alter contents

    var tailer = try m.queue.tailer(0);
    defer tailer.deinit();
    const e0 = (try tailer.poll()).?;
    try testing.expectEqualStrings("a", e0.message);
    const e1 = (try tailer.poll()).?;
    try testing.expectEqualStrings("b", e1.message);
}
