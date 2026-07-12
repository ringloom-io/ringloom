// SPDX-License-Identifier: Apache-2.0
//! Thread-safe registry of local topic publishers keyed by `topic_id`.
//!
//! Publishers are registered from the C ABI (caller thread) at startup and
//! looked up from the service control thread (which dispatches ack-feedback /
//! leader-change frames). A mutex guards the small map; the hot publish path
//! never touches this — it operates on a `*TopicPublisher` obtained at
//! registration time.

const std = @import("std");
const topic_publisher_mod = @import("topic_publisher.zig");
const TopicPublisher = topic_publisher_mod.TopicPublisher;

pub const TopicPublisherRegistry = struct {
    allocator: std.mem.Allocator,
    mutex: std.atomic.Mutex = .unlocked,
    by_topic_id: std.AutoHashMap(u64, *TopicPublisher),

    pub fn init(allocator: std.mem.Allocator) TopicPublisherRegistry {
        return .{
            .allocator = allocator,
            .by_topic_id = std.AutoHashMap(u64, *TopicPublisher).init(allocator),
        };
    }

    pub fn deinit(self: *TopicPublisherRegistry) void {
        self.by_topic_id.deinit();
        self.* = undefined;
    }

    fn lock(self: *TopicPublisherRegistry) void {
        while (!self.mutex.tryLock()) std.atomic.spinLoopHint();
    }

    fn unlock(self: *TopicPublisherRegistry) void {
        self.mutex.unlock();
    }

    pub fn register(self: *TopicPublisherRegistry, topic_id: u64, publisher: *TopicPublisher) !void {
        self.lock();
        defer self.unlock();
        try self.by_topic_id.put(topic_id, publisher);
    }

    pub fn unregister(self: *TopicPublisherRegistry, topic_id: u64) void {
        self.lock();
        defer self.unlock();
        _ = self.by_topic_id.remove(topic_id);
    }

    /// Returns a borrowed publisher pointer for `topic_id`, or null. The caller
    /// must not retain the pointer beyond the registry's lifetime; in practice
    /// the publisher is closed only at runtime shutdown.
    pub fn get(self: *TopicPublisherRegistry, topic_id: u64) ?*TopicPublisher {
        self.lock();
        defer self.unlock();
        return self.by_topic_id.get(topic_id);
    }
};
