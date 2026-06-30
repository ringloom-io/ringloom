// SPDX-License-Identifier: Apache-2.0

//! TopicSubscription — subscriber-side API for persistent topics (spec 09).
//!
//! Wraps a ringloom-queue Tailer opened on the local replica (or master) queue
//! directory. The broker is not involved on the read hot path — the subscriber
//! reads directly from the mmap'd queue.

const std = @import("std");
const rq = @import("ringloom_queue");

const topic_types = @import("topic_types.zig");
const TopicConfig = topic_types.TopicConfig;

const RawQueue = rq.Queue([]const u8);

/// Start position for a subscription.
pub const StartPosition = enum(u8) {
    earliest = 0,
    latest = 1,
};

/// A topic subscription. Opens a ringloom-queue tailer on the queue_dir
/// returned by the broker's TopicSubscriptionResponse.
pub const TopicSubscription = struct {
    topic_id: u64,
    tailer: rq.PublicTailer([]const u8),
    queue: RawQueue,

    /// Open a subscription at the given queue directory and start index.
    /// The geometry must match the broker's TopicSubscriptionResponse.
    pub fn open(
        allocator: std.mem.Allocator,
        queue_dir: []const u8,
        start_index: u64,
        config: TopicConfig,
    ) !TopicSubscription {
        const scheme = rq.roll.findSchemeByName(config.rollSchemeName()) orelse
            return error.UnknownRollScheme;

        var queue = try RawQueue.open(.{
            .dir = queue_dir,
            .roll_scheme = scheme,
            .create = false,
            .use_huge_pages = config.flags & 1 != 0,
            .enable_prefetcher = false,
            .enable_cleaner = false,
            .spawn_helper_threads = false,
            .retention_cycles = config.retention_cycles,
            .allocator = allocator,
        }, rq.codec.RawCodec);

        // Open the tailer at the requested start index.
        const tailer = try queue.tailer(start_index);

        return .{
            .topic_id = 0,
            .tailer = tailer,
            .queue = queue,
        };
    }

    pub fn deinit(self: *TopicSubscription) void {
        self.tailer.deinit();
        self.queue.deinit();
        self.* = undefined;
    }

    /// Poll for the next message. Returns null if no message is available.
    /// The returned slice is borrowed and valid until the next poll.
    pub fn poll(self: *TopicSubscription) !?[]const u8 {
        if (try self.tailer.poll()) |entry| {
            return entry.message;
        }
        return null;
    }

    /// Returns the current tailer index (last read position).
    pub fn index(self: *const TopicSubscription) u64 {
        return self.tailer.getIndex();
    }

    /// Drives queue-level prefetcher and cleaner work within a bounded budget.
    pub fn maintenancePoll(self: *TopicSubscription, max_work_units: u32) void {
        _ = self.queue.maintenancePoll(max_work_units) catch {};
    }
};
