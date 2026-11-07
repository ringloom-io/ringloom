// SPDX-License-Identifier: Apache-2.0
//! Leader-side `replicate_once` ack accounting, per producer (spec 05 §3.1).
//!
//! Each producer (`TopicPublishHeader.publisher_id`) numbers its publishes
//! 1, 2, 3, ... (`publish_seq`). For every `replicate_once` append the leader
//! remembers `(publish_seq, queue_index)`. Once the topic's `replicated_hwm`
//! reaches that index the producer's `acked_seq` advances to that sequence.
//!
//! Correctness rests on two orderings: a producer's sequences increase, and the
//! leader's queue indices increase (`cycle << 32 | seqnum`, monotonic across
//! cycle rolls). So "index <= replicated_hwm" for one entry implies it for every
//! earlier entry, and `acked_seq` never covers a publish that is not replicated.
//!
//! Owned by the receiver thread (the topic engine); not thread-safe.

const std = @import("std");
const topic_id_mod = @import("topic_id.zig");

const TopicId = topic_id_mod.TopicId;

/// Maximum outstanding (un-replicated) `replicate_once` entries remembered per
/// producer. When full, the newest entry is overwritten by the next one: the
/// overwritten publish is then acked together with the later one. That delays
/// its ack but never acks anything early.
pub const max_pending_per_producer: usize = 1024;

pub const ProducerKey = struct {
    topic_id: TopicId,
    publisher_id: u64,
};

const Entry = struct {
    seq: u64,
    index: u64,
};

/// Pending entries for one producer on one topic, oldest first.
pub const ProducerAcks = struct {
    entries: [max_pending_per_producer]Entry = undefined,
    head: usize = 0,
    len: usize = 0,
    /// Highest sequence known to be replicated.
    acked_seq: u64 = 0,
    /// Highest `acked_seq` handed to the control loop.
    reported_seq: u64 = 0,

    fn at(self: *const ProducerAcks, i: usize) Entry {
        return self.entries[(self.head + i) % max_pending_per_producer];
    }

    pub fn note(self: *ProducerAcks, seq: u64, index: u64) void {
        if (self.len == max_pending_per_producer) {
            // Full: replace the newest entry (see max_pending_per_producer).
            const last = (self.head + self.len - 1) % max_pending_per_producer;
            self.entries[last] = .{ .seq = seq, .index = index };
            return;
        }
        self.entries[(self.head + self.len) % max_pending_per_producer] = .{ .seq = seq, .index = index };
        self.len += 1;
    }

    /// Retires every entry whose index is replicated. Returns true if
    /// `acked_seq` advanced.
    pub fn advance(self: *ProducerAcks, replicated_hwm: u64) bool {
        // replicated_hwm == 0 means "nothing replicated yet".
        if (replicated_hwm == 0) return false;
        var advanced = false;
        while (self.len > 0) {
            const e = self.at(0);
            if (e.index > replicated_hwm) break;
            if (e.seq > self.acked_seq) {
                self.acked_seq = e.seq;
                advanced = true;
            }
            self.head = (self.head + 1) % max_pending_per_producer;
            self.len -= 1;
        }
        return advanced;
    }

    pub fn needsReport(self: *const ProducerAcks) bool {
        return self.acked_seq > self.reported_seq;
    }
};

pub const AckTracker = struct {
    allocator: std.mem.Allocator,
    producers: std.AutoHashMap(ProducerKey, *ProducerAcks),

    pub fn init(allocator: std.mem.Allocator) AckTracker {
        return .{
            .allocator = allocator,
            .producers = std.AutoHashMap(ProducerKey, *ProducerAcks).init(allocator),
        };
    }

    pub fn deinit(self: *AckTracker) void {
        var it = self.producers.valueIterator();
        while (it.next()) |p| self.allocator.destroy(p.*);
        self.producers.deinit();
        self.* = undefined;
    }

    /// Records a `replicate_once` append. Allocation failure drops the entry,
    /// which can only delay that publish's ack (it is covered by a later one).
    pub fn note(self: *AckTracker, key: ProducerKey, seq: u64, index: u64) void {
        const gop = self.producers.getOrPut(key) catch return;
        if (!gop.found_existing) {
            const p = self.allocator.create(ProducerAcks) catch {
                _ = self.producers.remove(key);
                return;
            };
            p.* = .{};
            gop.value_ptr.* = p;
        }
        gop.value_ptr.*.note(seq, index);
    }

    pub fn get(self: *AckTracker, key: ProducerKey) ?*ProducerAcks {
        return self.producers.get(key);
    }

    pub const Iterator = std.AutoHashMap(ProducerKey, *ProducerAcks).Iterator;

    pub fn iterator(self: *AckTracker) Iterator {
        return self.producers.iterator();
    }

    /// Drops all state for a topic (queue closed or reset).
    pub fn removeTopic(self: *AckTracker, topic_id: TopicId) void {
        var doomed: [64]ProducerKey = undefined;
        while (true) {
            var n: usize = 0;
            var it = self.producers.keyIterator();
            while (it.next()) |k| {
                if (k.topic_id != topic_id) continue;
                doomed[n] = k.*;
                n += 1;
                if (n == doomed.len) break;
            }
            if (n == 0) return;
            for (doomed[0..n]) |k| {
                if (self.producers.fetchRemove(k)) |kv| self.allocator.destroy(kv.value);
            }
        }
    }
};

// ── Tests ────────────────────────────────────────────────────────────────────

const testing = std.testing;

fn idx(cycle: u64, seqnum: u64) u64 {
    return (cycle << 32) | seqnum;
}

test "acks only the publishes whose index is replicated" {
    var p = ProducerAcks{};
    p.note(1, idx(100, 0));
    p.note(2, idx(100, 1));
    p.note(3, idx(100, 2));

    try testing.expect(!p.advance(0));
    try testing.expectEqual(@as(u64, 0), p.acked_seq);

    try testing.expect(p.advance(idx(100, 1)));
    try testing.expectEqual(@as(u64, 2), p.acked_seq);

    try testing.expect(p.advance(idx(100, 5)));
    try testing.expectEqual(@as(u64, 3), p.acked_seq);
    try testing.expectEqual(@as(usize, 0), p.len);
}

test "interleaved producers on one topic are acked independently" {
    var t = AckTracker.init(testing.allocator);
    defer t.deinit();

    const a = ProducerKey{ .topic_id = 7, .publisher_id = 0xA };
    const b = ProducerKey{ .topic_id = 7, .publisher_id = 0xB };

    // Arrival order at the leader: A1, B1, B2, A2, B3.
    t.note(a, 1, idx(100, 0));
    t.note(b, 1, idx(100, 1));
    t.note(b, 2, idx(100, 2));
    t.note(a, 2, idx(100, 3));
    t.note(b, 3, idx(100, 4));

    // Replicas have applied through B2's entry.
    _ = t.get(a).?.advance(idx(100, 2));
    _ = t.get(b).?.advance(idx(100, 2));

    // A's second publish (index 3) is not replicated, so A stays at 1 even
    // though three messages on the topic are.
    try testing.expectEqual(@as(u64, 1), t.get(a).?.acked_seq);
    try testing.expectEqual(@as(u64, 2), t.get(b).?.acked_seq);
}

test "acks keep advancing across a cycle roll" {
    var p = ProducerAcks{};
    p.note(1, idx(100, 41));
    p.note(2, idx(101, 0)); // new cycle: seqnum resets, index still grows
    p.note(3, idx(101, 1));

    try testing.expect(p.advance(idx(101, 0)));
    try testing.expectEqual(@as(u64, 2), p.acked_seq);
    try testing.expect(p.advance(idx(101, 1)));
    try testing.expectEqual(@as(u64, 3), p.acked_seq);
}

test "overflow coarsens acks but never acks early" {
    var p = ProducerAcks{};
    var s: u64 = 1;
    while (s <= max_pending_per_producer + 10) : (s += 1) p.note(s, idx(100, s));
    try testing.expectEqual(max_pending_per_producer, p.len);

    // Replicated up to just below the newest publish: everything except the
    // final (merged) entry is acked, and acked_seq stays below that entry.
    const newest: u64 = max_pending_per_producer + 10;
    _ = p.advance(idx(100, newest - 1));
    try testing.expect(p.acked_seq < newest);
    try testing.expectEqual(@as(u64, max_pending_per_producer - 1), p.acked_seq);

    _ = p.advance(idx(100, newest));
    try testing.expectEqual(newest, p.acked_seq);
}

test "removeTopic drops only that topic's producers" {
    var t = AckTracker.init(testing.allocator);
    defer t.deinit();
    var i: u64 = 0;
    while (i < 100) : (i += 1) {
        t.note(.{ .topic_id = 1, .publisher_id = i }, 1, idx(1, 0));
        t.note(.{ .topic_id = 2, .publisher_id = i }, 1, idx(1, 0));
    }
    t.removeTopic(1);
    try testing.expectEqual(@as(u32, 100), t.producers.count());
    try testing.expect(t.get(.{ .topic_id = 2, .publisher_id = 5 }) != null);
    try testing.expect(t.get(.{ .topic_id = 1, .publisher_id = 5 }) == null);
}
