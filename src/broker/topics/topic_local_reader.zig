// SPDX-License-Identifier: Apache-2.0
//! LocalTailer — an independent read handle on a topic's local queue directory,
//! for broker-internal consumers on their own thread (the KV applier,
//! docs/kv-store-spec.md §8).
//!
//! It mirrors what a service does for `TopicSubscription`: it opens its own
//! read-only ringloom-queue handle on the directory instead of sharing the
//! receiver thread's `TopicQueue`. That keeps it off the receiver's data
//! structures (no `store.lock` while polling) and keeps its memory valid when
//! the receiver closes or resets the queue (`Queue.deinit` would destroy a
//! tailer registered on the shared queue).
//!
//! Resets: after `TopicEngine.resetReplica` deletes and recreates the directory,
//! an open `LocalTailer` still maps the unlinked OLD files and never sees the
//! new history. Consumers MUST close and reopen on a reset notification
//! (`TopicEngine.addResetListener` / `EngineStatus.replica_reset`).
//!
//! Indices are ringloom-queue public indices (`cycle << 32 | seqnum`), not
//! dense counters: consecutive entries differ by 1 within a cycle but jump to
//! `(cycle+1) << 32` at a roll, and the first index of a log is
//! `cycle_of_first_append << 32`, not 0.

const std = @import("std");
const rq = @import("ringloom_queue");
const topic_config_mod = @import("topic_config.zig");

const TopicConfig = topic_config_mod.TopicConfig;
const RawQueue = rq.Queue([]const u8);

pub const LocalTailer = struct {
    queue: RawQueue,
    tailer: rq.PublicTailer([]const u8),

    pub const Entry = rq.PublicTailer([]const u8).Entry;

    /// Opens a read-only handle on `queue_dir` and positions the tailer at
    /// `start_index` (0 = first retained entry). Setup path: allocates.
    pub fn open(allocator: std.mem.Allocator, queue_dir: []const u8, config: TopicConfig, start_index: u64) !LocalTailer {
        const scheme = rq.roll.findSchemeByName(config.rollSchemeName()) orelse
            return error.UnknownRollScheme;
        var queue = try RawQueue.open(.{
            .dir = queue_dir,
            .roll_scheme = scheme,
            .create = false,
            .use_huge_pages = config.useHugePages(),
            .enable_prefetcher = false,
            .enable_cleaner = false,
            .spawn_helper_threads = false,
            .retention_cycles = config.retention_cycles,
            .allocator = allocator,
        }, rq.codec.RawCodec);
        errdefer queue.deinit();
        const tailer = try queue.tailer(start_index);
        return .{ .queue = queue, .tailer = tailer };
    }

    pub fn deinit(self: *LocalTailer) void {
        self.tailer.deinit();
        self.queue.deinit();
        self.* = undefined;
    }

    /// Next entry, or null when caught up with the local log tip. The message
    /// is borrowed from the mapping and valid until the next `poll`/`deinit`.
    /// Does not allocate.
    pub fn poll(self: *LocalTailer) !?Entry {
        return self.tailer.poll();
    }

    /// Index the next `poll` will attempt to read.
    pub fn nextIndex(self: *const LocalTailer) u64 {
        return self.tailer.getIndex();
    }
};
