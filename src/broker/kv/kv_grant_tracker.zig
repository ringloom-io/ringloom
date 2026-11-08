// SPDX-License-Identifier: Apache-2.0
//! KV grant tracker (docs/kv-store-spec.md §3.1, §9).
//!
//! Runs on **every topics-enabled broker** (KV enabled or not): any of them can
//! become topic leader and must already know each store's current writer lease.
//! It reads the local `__kv.*` logs through independent `LocalTailer`s and looks
//! only at frame *headers* (`peekHeader`); only the (rare) leader-authored
//! `store_header` / `writer_grant` / `writer_release` frames get their body
//! decoded. `data` frames are never decoded.
//!
//! ## Threading
//!
//! `GrantTracker.poll` is called from the **control-loop thread** (the same
//! thread that runs lease arbitration, so tracker state needs no locking) once
//! per duty-cycle iteration. Latency is bounded:
//!   * at most `poll_budget_entries` log entries are consumed per call
//!     (default 256, each a ~30 ns header peek), and every slot is visited once
//!     per call with a null-poll when idle (a few µs for 64 stores);
//!   * when everything is caught up the whole pass is rate-limited to once per
//!     `idle_poll_interval_ns` (200 µs);
//!   * store discovery (registry scan) runs every `discovery_interval_ns`
//!     (500 ms), and tailer (re)open attempts for a slot are throttled to once
//!     per `open_retry_ns` (200 ms) — opening a tailer allocates/maps files, so
//!     it is the only non-trivial step and happens once per store per reset.
//! A busy store (heavy data traffic) is consumed in `poll_budget_entries`
//! slices; it simply reports `caught_up == false` between slices.
//!
//! ## Resets
//!
//! `TopicEngine` resets a replica queue (sink-ahead recovery, spec 08 §4) by
//! deleting and recreating its directory. An open tailer still maps the old
//! unlinked files, so the tracker must restart from index 0. The reset listener
//! (`noteReset`, called on the receiver thread) only bumps a per-slot atomic
//! counter; the control thread notices the change in `poll`, drops the tailer,
//! clears the slot state and re-reads the (new) log from the start.
//!
//! ## "Caught up"
//!
//! `Slot.caught_up` is true when the last pass reached the end of the local log
//! (`poll() == null`) and everything up to the log tip that was visible *before*
//! the pass started had been consumed. Lease-relevant frames (grant/release)
//! are only ever appended by the leader itself, which also waits for its own
//! append indices (`tracked_index`) before the next decision, so a busy data
//! stream cannot starve arbitration.

const std = @import("std");
const common = @import("ringloom_common");
const kv = common.kv;

const KvStoreConfig = kv.KvStoreConfig;
const FenceState = kv.FenceState;

/// The current writer lease holder as recorded in the log.
pub const Holder = struct {
    epoch: u64,
    nonce: u64,
    service_id: u32,
    node_id: u16,
    granted_at_ns: u64,
};

/// Pure per-store log state: no I/O, no allocation. Fed with every entry of one
/// store's log in index order. This is the state the leader arbitrates from.
pub const StoreTrack = struct {
    fence: FenceState = .{},
    /// Valid iff `fence.current_writer_epoch != 0`.
    holder_rec: Holder = undefined,
    /// First `store_header` in the log (immutable, first wins).
    config: ?KvStoreConfig = null,
    /// Index of the last entry consumed; 0 before the first.
    tracked_index: u64 = 0,
    entries: u64 = 0,
    grants: u64 = 0,
    releases: u64 = 0,
    /// Frames (any kind) failing header/body validation.
    malformed: u64 = 0,

    pub fn reset(self: *StoreTrack) void {
        self.* = .{};
    }

    /// Consume one log entry (`message` is the raw topic payload).
    pub fn onEntry(self: *StoreTrack, index: u64, message: []const u8) void {
        self.tracked_index = index;
        self.entries += 1;
        const h = kv.peekHeader(message) orelse {
            self.malformed += 1;
            return;
        };
        switch (h.kind) {
            // Data frames never affect the lease: skip without decoding.
            .data => {},
            .store_header, .writer_grant, .writer_release => {
                // Exact-body validation; limits are irrelevant for non-data frames.
                const view = kv.decodeFrame(message, kv.format.engine_max_key_len, std.math.maxInt(u32)) catch {
                    self.malformed += 1;
                    return;
                };
                switch (view.payload) {
                    .store_header => |cfg| if (self.config == null) {
                        self.config = cfg;
                    },
                    .writer_grant => |g| {
                        self.fence.onGrant(g.writer_epoch);
                        self.holder_rec = .{
                            .epoch = g.writer_epoch,
                            .nonce = g.holder_session_nonce,
                            .service_id = g.holder_service_id,
                            .node_id = g.holder_node_id,
                            .granted_at_ns = g.granted_at_ns,
                        };
                        self.grants += 1;
                    },
                    .writer_release => |r| {
                        self.fence.onRelease(r.writer_epoch);
                        self.releases += 1;
                    },
                    .data => unreachable,
                }
            },
        }
    }

    /// Current lease holder (latest grant not released), if any.
    pub fn holder(self: *const StoreTrack) ?Holder {
        if (self.fence.current_writer_epoch == 0) return null;
        return self.holder_rec;
    }

    /// Highest grant epoch ever recorded; the next grant uses this + 1.
    pub fn lastEpoch(self: *const StoreTrack) u64 {
        return self.fence.last_grant_epoch;
    }
};

pub const Config = struct {
    max_stores: u32 = 64,
    poll_budget_entries: u32 = 256,
    idle_poll_interval_ns: i64 = 200 * std.time.ns_per_us,
    discovery_interval_ns: i64 = 500 * std.time.ns_per_ms,
    open_retry_ns: i64 = 200 * std.time.ns_per_ms,
};

pub const max_name_len: usize = kv.format.max_topic_name_len;

/// One discovered KV topic.
pub const TopicRef = struct {
    topic_id: u64,
    /// Full topic name `__kv.<store>`; borrowed, valid only during the call.
    name: []const u8,
};

/// `Source` abstracts the topic subsystem so the tracker is testable with a
/// synthetic log. Required declarations:
///
///   pub const Tailer: type   // .poll() !?struct{ index: u64, message: []const u8 }, .deinit()
///   pub fn listKvTopics(self: *Source, out: []TopicRef) u32
///   pub fn openTailer(self: *Source, allocator, topic_id: u64) !Tailer   // from index 0
///   pub fn hwm(self: *Source, topic_id: u64) u64                         // local log tip, 0 = empty
pub fn GrantTracker(comptime Source: type) type {
    return struct {
        const Self = @This();

        pub const Slot = struct {
            used: bool = false,
            topic_id: u64 = 0,
            name_len: u8 = 0,
            name: [max_name_len]u8 = undefined,
            track: StoreTrack = .{},
            tailer: ?Source.Tailer = null,
            /// `reset_seqs[i]` value this slot has reacted to.
            seen_reset: u64 = 0,
            caught_up: bool = false,
            next_open_ns: i64 = 0,
            resets: u32 = 0,

            pub fn topicName(self: *const Slot) []const u8 {
                return self.name[0..self.name_len];
            }
            /// Store name without the `__kv.` prefix.
            pub fn storeName(self: *const Slot) []const u8 {
                return self.name[kv.format.kv_topic_prefix.len..self.name_len];
            }
            pub fn isTailing(self: *const Slot) bool {
                return self.tailer != null;
            }
        };

        allocator: std.mem.Allocator,
        source: *Source,
        cfg: Config,
        slots: []Slot,
        discovery_refs: []TopicRef,
        /// Mirrors of `slots[i].topic_id` / a reset counter, for the receiver-thread
        /// reset listener (atomics only; the slot struct itself is control-thread-only).
        slot_ids: []std.atomic.Value(u64),
        reset_seqs: []std.atomic.Value(u64),
        cursor: u32 = 0,
        next_discovery_ns: i64 = 0,
        next_poll_ns: i64 = 0,
        /// Stores that could not be tracked because all slots were taken.
        overflow: u64 = 0,

        pub fn init(allocator: std.mem.Allocator, source: *Source, cfg: Config) !Self {
            const slots = try allocator.alloc(Slot, cfg.max_stores);
            errdefer allocator.free(slots);
            for (slots) |*s| s.* = .{};
            const ids = try allocator.alloc(std.atomic.Value(u64), cfg.max_stores);
            errdefer allocator.free(ids);
            const seqs = try allocator.alloc(std.atomic.Value(u64), cfg.max_stores);
            errdefer allocator.free(seqs);
            // One extra entry keeps capacity overflow observable.
            const refs = try allocator.alloc(TopicRef, @as(usize, cfg.max_stores) + 1);
            for (ids) |*i| i.* = std.atomic.Value(u64).init(0);
            for (seqs) |*i| i.* = std.atomic.Value(u64).init(0);
            return .{ .allocator = allocator, .source = source, .cfg = cfg, .slots = slots, .discovery_refs = refs, .slot_ids = ids, .reset_seqs = seqs };
        }

        pub fn deinit(self: *Self) void {
            for (self.slots) |*s| if (s.tailer) |*t| t.deinit();
            self.allocator.free(self.slots);
            self.allocator.free(self.discovery_refs);
            self.allocator.free(self.slot_ids);
            self.allocator.free(self.reset_seqs);
        }

        /// Any thread (receiver thread from the topic engine's reset listener):
        /// a replica queue of `topic_id` was reset. Atomics only.
        pub fn noteReset(self: *Self, topic_id: u64) void {
            for (self.slot_ids, 0..) |*id, i| {
                if (id.load(.acquire) == topic_id) {
                    _ = self.reset_seqs[i].fetchAdd(1, .release);
                    return;
                }
            }
        }

        pub fn find(self: *Self, topic_id: u64) ?*Slot {
            for (self.slots) |*s| if (s.used and s.topic_id == topic_id) return s;
            return null;
        }

        /// Control thread. Returns the amount of work done (entries consumed +
        /// slots opened/reset), 0 when idle.
        pub fn poll(self: *Self, now_ns: i64) u32 {
            var work: u32 = 0;
            if (now_ns >= self.next_discovery_ns) {
                self.next_discovery_ns = now_ns + self.cfg.discovery_interval_ns;
                work += self.discover();
            }
            if (now_ns < self.next_poll_ns) return work;

            var budget: u32 = self.cfg.poll_budget_entries;
            var all_idle = true;
            const n: u32 = @intCast(self.slots.len);
            var visited: u32 = 0;
            while (visited < n) : (visited += 1) {
                const i = (self.cursor + visited) % n;
                const slot = &self.slots[i];
                if (!slot.used) continue;
                const consumed = self.pollSlot(slot, i, now_ns, &budget, &work);
                if (consumed or !slot.caught_up) all_idle = false;
                if (budget == 0) {
                    // Resume at the next slot so one busy store cannot starve others.
                    self.cursor = (i + 1) % n;
                    break;
                }
            }
            if (visited >= n) self.cursor = (self.cursor + 1) % n;
            self.next_poll_ns = if (all_idle) now_ns + self.cfg.idle_poll_interval_ns else 0;
            return work;
        }

        fn discover(self: *Self) u32 {
            const refs = self.discovery_refs;
            const count = self.source.listKvTopics(refs);
            var added: u32 = 0;
            for (refs[0..count]) |ref| {
                if (self.find(ref.topic_id) != null) continue;
                if (ref.name.len > max_name_len or ref.name.len <= kv.format.kv_topic_prefix.len) continue;
                const idx = self.freeSlot() orelse {
                    self.overflow += 1;
                    continue;
                };
                const s = &self.slots[idx];
                s.* = .{ .used = true, .topic_id = ref.topic_id, .name_len = @intCast(ref.name.len) };
                @memcpy(s.name[0..ref.name.len], ref.name);
                s.seen_reset = self.reset_seqs[idx].load(.acquire);
                self.slot_ids[idx].store(ref.topic_id, .release);
                added += 1;
            }
            return added;
        }

        fn freeSlot(self: *Self) ?usize {
            for (self.slots, 0..) |*s, i| if (!s.used) return i;
            return null;
        }

        fn dropTailer(slot: *Slot) void {
            if (slot.tailer) |*t| t.deinit();
            slot.tailer = null;
        }

        /// Returns true if at least one entry was consumed.
        fn pollSlot(self: *Self, slot: *Slot, idx: u32, now_ns: i64, budget: *u32, work: *u32) bool {
            // Replica reset: restart from index 0 on the recreated log.
            const rs = self.reset_seqs[idx].load(.acquire);
            if (rs != slot.seen_reset) {
                slot.seen_reset = rs;
                dropTailer(slot);
                slot.track.reset();
                slot.caught_up = false;
                slot.next_open_ns = 0;
                slot.resets += 1;
                work.* += 1;
            }
            if (slot.tailer == null) {
                if (now_ns < slot.next_open_ns) return false;
                slot.next_open_ns = now_ns + self.cfg.open_retry_ns;
                // Read the reset counter *before* opening so a reset racing
                // with the open is noticed on the next pass.
                slot.seen_reset = self.reset_seqs[idx].load(.acquire);
                slot.tailer = self.source.openTailer(self.allocator, slot.topic_id) catch return false;
                slot.track.reset();
                work.* += 1;
            }
            const hwm_before = self.source.hwm(slot.topic_id);
            var consumed = false;
            var reached_end = false;
            while (budget.* > 0) {
                const e = (slot.tailer.?.poll() catch {
                    // Unreadable queue (e.g. mid-reset): reopen on the next pass.
                    dropTailer(slot);
                    slot.caught_up = false;
                    return consumed;
                }) orelse {
                    reached_end = true;
                    break;
                };
                slot.track.onEntry(e.index, e.message);
                budget.* -= 1;
                work.* += 1;
                consumed = true;
            }
            slot.caught_up = reached_end and slot.track.tracked_index >= hwm_before;
            return consumed;
        }
    };
}

// ── Production source ────────────────────────────────────────────────────────

/// `Source` over the broker's `TopicSubsystem`. Control thread only for
/// `listKvTopics` (registry scan); `openTailer`/`hwm` are thread-safe.
pub const TopicSource = struct {
    const topics = @import("../topics.zig");
    ts: *topics.TopicSubsystem,

    pub const Tailer = struct {
        inner: topics.subsystem.LocalTailer,

        pub fn poll(self: *Tailer) !?struct { index: u64, message: []const u8 } {
            const e = (try self.inner.poll()) orelse return null;
            return .{ .index = e.index, .message = e.message };
        }
        pub fn deinit(self: *Tailer) void {
            self.inner.deinit();
        }
    };

    pub fn listKvTopics(self: *TopicSource, out: []TopicRef) u32 {
        var n: u32 = 0;
        var it = self.ts.registry.iterator();
        while (it.next()) |r| {
            if (n >= out.len) break;
            if (!r.config.isKvStore()) continue;
            out[n] = .{ .topic_id = r.topic_id, .name = r.name };
            n += 1;
        }
        return n;
    }

    pub fn openTailer(self: *TopicSource, allocator: std.mem.Allocator, topic_id: u64) !Tailer {
        return .{ .inner = try self.ts.openLocalTailer(allocator, topic_id, 0) };
    }

    pub fn hwm(self: *TopicSource, topic_id: u64) u64 {
        return self.ts.localHwm(topic_id);
    }

    /// Registers the tracker's reset hook on the topic engine. Call before the
    /// receiver loop starts (see `TopicSubsystem.addResetListener`). Returns
    /// false if the engine has no free listener slot.
    pub fn attachResetListener(ts: *topics.TopicSubsystem, tracker: anytype) bool {
        const Tr = @TypeOf(tracker.*);
        const Hook = struct {
            fn pre(_: ?*anyopaque, _: u64, _: bool) void {}
            fn post(ctx: ?*anyopaque, topic_id: u64, kv_store: bool, _: u64) void {
                if (!kv_store) return;
                const t: *Tr = @ptrCast(@alignCast(ctx.?));
                t.noteReset(topic_id);
            }
        };
        return ts.addResetListener(.{ .ctx = tracker, .on_pre_reset = Hook.pre, .on_post_reset = Hook.post });
    }
};

// ── Tests ────────────────────────────────────────────────────────────────────

const testing = std.testing;

fn frameStoreHeader(buf: []u8, cfg: KvStoreConfig) []const u8 {
    return kv.format.encodeStoreHeader(buf, 1, cfg) catch unreachable;
}
fn frameGrant(buf: []u8, epoch: u64, nonce: u64, svc: u32, node: u16) []const u8 {
    return kv.format.encodeWriterGrant(buf, 1, .{
        .writer_epoch = epoch,
        .holder_session_nonce = nonce,
        .holder_service_id = svc,
        .holder_node_id = node,
        .granted_at_ns = 5,
    }) catch unreachable;
}
fn frameRelease(buf: []u8, epoch: u64) []const u8 {
    return kv.format.encodeWriterRelease(buf, 1, .{ .writer_epoch = epoch, .reason = 0 }) catch unreachable;
}
fn frameData(buf: []u8, epoch: u64) []const u8 {
    var b = kv.DataFrameBuilder.init(buf, epoch, 1);
    b.addPut("k", "v", 0) catch unreachable;
    return b.finish();
}

test "StoreTrack: header, grant, release, data frames, last_epoch" {
    var buf: [256]u8 = undefined;
    var t = StoreTrack{};
    try testing.expect(t.holder() == null);
    try testing.expectEqual(@as(u64, 0), t.lastEpoch());

    var cfg = KvStoreConfig{};
    cfg.default_ttl_ms = 9;
    t.onEntry(10, frameStoreHeader(&buf, cfg));
    try testing.expectEqual(@as(u64, 9), t.config.?.default_ttl_ms);
    // A second header never replaces the first (first creation wins).
    var cfg2 = KvStoreConfig{};
    cfg2.default_ttl_ms = 99;
    t.onEntry(11, frameStoreHeader(&buf, cfg2));
    try testing.expectEqual(@as(u64, 9), t.config.?.default_ttl_ms);

    t.onEntry(12, frameGrant(&buf, 1, 0xAA, 7, 2));
    var h = t.holder().?;
    try testing.expectEqual(@as(u64, 1), h.epoch);
    try testing.expectEqual(@as(u64, 0xAA), h.nonce);
    try testing.expectEqual(@as(u32, 7), h.service_id);
    try testing.expectEqual(@as(u16, 2), h.node_id);

    // Data frames do not change lease state.
    t.onEntry(13, frameData(&buf, 1));
    try testing.expect(t.holder() != null);
    try testing.expectEqual(@as(u64, 13), t.tracked_index);

    // Release of a stale epoch is ignored; matching release clears the holder
    // but last_epoch is retained.
    t.onEntry(14, frameRelease(&buf, 5));
    try testing.expect(t.holder() != null);
    t.onEntry(15, frameRelease(&buf, 1));
    try testing.expect(t.holder() == null);
    try testing.expectEqual(@as(u64, 1), t.lastEpoch());

    t.onEntry(16, frameGrant(&buf, 2, 0xBB, 8, 3));
    h = t.holder().?;
    try testing.expectEqual(@as(u64, 2), h.epoch);
    try testing.expectEqual(@as(u64, 0xBB), h.nonce);
    try testing.expectEqual(@as(u64, 2), t.lastEpoch());
    try testing.expectEqual(@as(u64, 2), t.grants);
    try testing.expectEqual(@as(u64, 2), t.releases);
}

test "StoreTrack: malformed frames are counted and ignored" {
    var buf: [256]u8 = undefined;
    var t = StoreTrack{};
    t.onEntry(1, "garbage");
    const zeros: [40]u8 = @splat(0);
    t.onEntry(2, &zeros);
    // Valid header claiming a grant with a truncated body.
    const g = frameGrant(&buf, 1, 1, 1, 1);
    t.onEntry(3, g[0 .. g.len - 8]);
    try testing.expect(t.holder() == null);
    try testing.expectEqual(@as(u64, 3), t.malformed);
    try testing.expectEqual(@as(u64, 3), t.tracked_index);
    // A later valid grant still works.
    t.onEntry(4, g);
    try testing.expect(t.holder() != null);
}

test "StoreTrack: release then re-grant of the same epoch number does not resurrect" {
    var buf: [256]u8 = undefined;
    var t = StoreTrack{};
    t.onEntry(1, frameGrant(&buf, 3, 1, 1, 1));
    t.onEntry(2, frameRelease(&buf, 3));
    try testing.expect(t.holder() == null);
    try testing.expectEqual(@as(u64, 3), t.lastEpoch());
}

// Synthetic log source: per-topic list of entries.
const FakeLog = struct {
    const max_entries = 64;
    const Entry = struct { index: u64, len: u16, bytes: [128]u8 };
    topic_id: u64,
    name: []const u8,
    entries: [max_entries]Entry = undefined,
    count: u32 = 0,
    /// Pretend the log tip is this far ahead of what's readable (simulates lag).
    hwm_override: ?u64 = null,
    open_fails: bool = false,

    fn append(self: *FakeLog, msg: []const u8) u64 {
        const index: u64 = (@as(u64, 1) << 32) | (self.count + 1);
        var e: Entry = .{ .index = index, .len = @intCast(msg.len), .bytes = undefined };
        @memcpy(e.bytes[0..msg.len], msg);
        self.entries[self.count] = e;
        self.count += 1;
        return index;
    }
    fn clear(self: *FakeLog) void {
        self.count = 0;
    }
};

const FakeSource = struct {
    logs: []*FakeLog,
    opens: u32 = 0,

    pub const Tailer = struct {
        log: *FakeLog,
        pos: u32 = 0,
        pub fn poll(self: *Tailer) !?struct { index: u64, message: []const u8 } {
            if (self.pos >= self.log.count) return null;
            const e = &self.log.entries[self.pos];
            self.pos += 1;
            return .{ .index = e.index, .message = e.bytes[0..e.len] };
        }
        pub fn deinit(_: *Tailer) void {}
    };

    pub fn listKvTopics(self: *FakeSource, out: []TopicRef) u32 {
        var n: u32 = 0;
        for (self.logs) |l| {
            if (n >= out.len) break;
            out[n] = .{ .topic_id = l.topic_id, .name = l.name };
            n += 1;
        }
        return n;
    }
    pub fn openTailer(self: *FakeSource, _: std.mem.Allocator, topic_id: u64) !Tailer {
        for (self.logs) |l| if (l.topic_id == topic_id) {
            if (l.open_fails) return error.QueueNotOpen;
            self.opens += 1;
            return .{ .log = l };
        };
        return error.QueueNotOpen;
    }
    pub fn hwm(self: *FakeSource, topic_id: u64) u64 {
        for (self.logs) |l| if (l.topic_id == topic_id) {
            if (l.hwm_override) |h| return h;
            return if (l.count == 0) 0 else l.entries[l.count - 1].index;
        };
        return 0;
    }
};

test "GrantTracker: discovers stores, tails logs, reports holder and caught_up" {
    var buf: [256]u8 = undefined;
    var log = FakeLog{ .topic_id = 100, .name = "__kv.prices" };
    _ = log.append(frameStoreHeader(&buf, .{}));
    _ = log.append(frameGrant(&buf, 1, 0xAA, 4, 1));
    var logs = [_]*FakeLog{&log};
    var src = FakeSource{ .logs = &logs };
    var tr = try GrantTracker(FakeSource).init(testing.allocator, &src, .{ .max_stores = 4 });
    defer tr.deinit();

    try testing.expect(tr.find(100) == null);
    const w = tr.poll(1_000);
    try testing.expect(w > 0);
    const s = tr.find(100).?;
    try testing.expectEqualStrings("__kv.prices", s.topicName());
    try testing.expectEqualStrings("prices", s.storeName());
    try testing.expect(s.caught_up);
    try testing.expectEqual(@as(u64, 1), s.track.holder().?.epoch);
    try testing.expect(s.track.config != null);

    // New entries appear on a later pass.
    _ = log.append(frameData(&buf, 1));
    _ = log.append(frameRelease(&buf, 1));
    _ = tr.poll(10_000_000);
    try testing.expect(s.track.holder() == null);
    try testing.expectEqual(log.entries[3].index, s.track.tracked_index);
    try testing.expect(s.caught_up);
}

test "GrantTracker: not caught up while behind the log tip; budget slices work" {
    var buf: [256]u8 = undefined;
    var log = FakeLog{ .topic_id = 7, .name = "__kv.a" };
    _ = log.append(frameStoreHeader(&buf, .{}));
    var i: u32 = 0;
    while (i < 20) : (i += 1) _ = log.append(frameData(&buf, 0));
    var logs = [_]*FakeLog{&log};
    var src = FakeSource{ .logs = &logs };
    var tr = try GrantTracker(FakeSource).init(testing.allocator, &src, .{ .max_stores = 2, .poll_budget_entries = 8 });
    defer tr.deinit();

    _ = tr.poll(1);
    const s = tr.find(7).?;
    try testing.expect(!s.caught_up); // 8 of 21 consumed
    try testing.expectEqual(@as(u64, 8), s.track.entries);
    _ = tr.poll(2);
    _ = tr.poll(3);
    try testing.expect(s.caught_up);
    try testing.expectEqual(@as(u64, 21), s.track.entries);

    // The tip claims more than is readable (replica still receiving): not caught up.
    log.hwm_override = log.entries[log.count - 1].index + 5;
    _ = tr.poll(100_000_000);
    try testing.expect(!s.caught_up);
    log.hwm_override = null;
    _ = tr.poll(200_000_000);
    try testing.expect(s.caught_up);
}

test "GrantTracker: replica reset restarts from index 0 on the new log" {
    var buf: [256]u8 = undefined;
    var log = FakeLog{ .topic_id = 9, .name = "__kv.b" };
    _ = log.append(frameStoreHeader(&buf, .{}));
    _ = log.append(frameGrant(&buf, 1, 0x11, 1, 1));
    _ = log.append(frameGrant(&buf, 2, 0x22, 2, 1)); // history of the losing side
    var logs = [_]*FakeLog{&log};
    var src = FakeSource{ .logs = &logs };
    var tr = try GrantTracker(FakeSource).init(testing.allocator, &src, .{ .max_stores = 2 });
    defer tr.deinit();
    _ = tr.poll(1);
    const s = tr.find(9).?;
    try testing.expectEqual(@as(u64, 2), s.track.holder().?.epoch);
    try testing.expectEqual(@as(u32, 1), src.opens);

    // Reset: log replaced by the winner's shorter history.
    log.clear();
    _ = log.append(frameStoreHeader(&buf, .{}));
    _ = log.append(frameGrant(&buf, 1, 0x11, 1, 1));
    tr.noteReset(9);
    tr.noteReset(12345); // unknown topic: ignored
    _ = tr.poll(1_000_000_000);
    try testing.expectEqual(@as(u32, 1), s.resets);
    try testing.expectEqual(@as(u32, 2), src.opens); // tailer reopened from 0
    try testing.expectEqual(@as(u64, 1), s.track.holder().?.epoch);
    try testing.expectEqual(@as(u64, 0x11), s.track.holder().?.nonce);
    try testing.expectEqual(@as(u64, 2), s.track.entries);
    try testing.expect(s.caught_up);
}

test "GrantTracker: tailer open failure is retried after the throttle; slots overflow is counted" {
    var buf: [256]u8 = undefined;
    var l1 = FakeLog{ .topic_id = 1, .name = "__kv.one", .open_fails = true };
    var l2 = FakeLog{ .topic_id = 2, .name = "__kv.two" };
    _ = l1.append(frameStoreHeader(&buf, .{}));
    _ = l2.append(frameStoreHeader(&buf, .{}));
    var logs = [_]*FakeLog{ &l1, &l2 };
    var src = FakeSource{ .logs = &logs };
    var tr = try GrantTracker(FakeSource).init(testing.allocator, &src, .{ .max_stores = 1 });
    defer tr.deinit();
    _ = tr.poll(1);
    try testing.expect(tr.find(1) != null);
    try testing.expect(tr.find(2) == null);
    try testing.expect(tr.overflow >= 1);
    const s = tr.find(1).?;
    try testing.expect(!s.isTailing());
    try testing.expect(!s.caught_up);

    l1.open_fails = false;
    _ = tr.poll(10); // inside the retry throttle: still not open
    try testing.expect(!s.isTailing());
    _ = tr.poll(300 * std.time.ns_per_ms);
    try testing.expect(s.isTailing());
    try testing.expect(s.caught_up);
    try testing.expect(s.track.config != null);
}

test "GrantTracker: discovers more than 64 stores up to configured capacity" {
    const count = 80;
    const logs = try testing.allocator.alloc(FakeLog, count);
    defer testing.allocator.free(logs);
    var pointers: [count]*FakeLog = undefined;
    var buf: [256]u8 = undefined;
    for (logs, 0..) |*log, i| {
        log.* = .{ .topic_id = i + 1, .name = "__kv.store" };
        _ = log.append(frameStoreHeader(&buf, .{}));
        _ = log.append(frameGrant(&buf, 1, i + 1, 1, 1));
        pointers[i] = log;
    }
    var source = FakeSource{ .logs = &pointers };
    var tracker = try GrantTracker(FakeSource).init(testing.allocator, &source, .{ .max_stores = count });
    defer tracker.deinit();
    _ = tracker.poll(0);
    try testing.expectEqual(@as(u32, count), source.opens);
    for (logs) |log| {
        const slot = tracker.find(log.topic_id).?;
        try testing.expect(slot.caught_up);
        try testing.expectEqual(log.topic_id, slot.track.holder().?.nonce);
    }
}

test "TopicSource: lists more than 64 KV registry topics without staging limit" {
    const topics = @import("../topics.zig");
    const registry = @import("../topics/topic_registry.zig");
    const config = @import("../topics/topic_config.zig");
    // Enumeration only reads the control-thread registry.
    var subsystem: topics.TopicSubsystem = undefined;
    subsystem.registry = registry.TopicRegistry.init(testing.allocator);
    defer subsystem.registry.deinit();
    var name_buf: [64]u8 = undefined;
    for (0..80) |i| {
        const name = try std.fmt.bufPrint(&name_buf, "__kv.store{d}", .{i});
        _ = try subsystem.registry.upsert(i + 1, name, config.TopicConfig.fromName("FAST_DAILY", 0, false).withKvStore(true), 1, 1, 0);
    }
    _ = try subsystem.registry.upsert(100, "ordinary", config.TopicConfig.fromName("FAST_DAILY", 0, false), 1, 1, 0);
    var source = TopicSource{ .ts = &subsystem };
    var refs: [81]TopicRef = undefined;
    try testing.expectEqual(@as(u32, 80), source.listKvTopics(&refs));
    var seen: [80]bool = @splat(false);
    for (refs[0..80]) |ref| {
        try testing.expect(ref.topic_id >= 1 and ref.topic_id <= 80);
        try testing.expect(!seen[ref.topic_id - 1]);
        seen[ref.topic_id - 1] = true;
        try testing.expect(std.mem.startsWith(u8, ref.name, "__kv.store"));
    }
    try testing.expectEqual(@as(u32, 3), source.listKvTopics(refs[0..3]));
    try testing.expectEqual(@as(u32, 0), source.listKvTopics(refs[0..0]));
}
