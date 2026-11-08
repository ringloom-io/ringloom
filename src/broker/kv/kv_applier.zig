// SPDX-License-Identifier: Apache-2.0
//! Transactional log application. All state advances only after commit.
const std = @import("std");
const kv = @import("ringloom_common").kv;

pub const Applier = struct {
    config: kv.KvStoreConfig,
    store_id: u64,
    fence: kv.FenceState = .{},
    index: ?u64 = null,
    leader_epoch: u64 = 0,
    malformed: u64 = 0,
    fenced: u64 = 0,
    frames: u64 = 0,
    puts: u64 = 0,
    deletes: u64 = 0,
    clears: u64 = 0,

    /// Queue indices contain cycle and sequence, rather than dense offsets.
    pub fn follows(previous: u64, next: u64) bool {
        return next == previous +| 1 or (next > previous and @as(u32, @truncate(next)) == 0 and next >> 32 > previous >> 32);
    }

    pub fn apply(self: *Applier, txn: *kv.engine.WriteTxn, index: u64, bytes: []const u8, now: u64) !void {
        if (self.index) |last| if (!follows(last, index)) return error.IndexDiscontinuity;
        const frame = kv.decodeFrame(bytes, self.config.max_key_len, self.config.max_value_len) catch {
            self.malformed += 1;
            self.index = index;
            return;
        };
        self.index = index;
        self.leader_epoch = frame.header.leader_epoch;
        switch (frame.payload) {
            .store_header => |cfg| if (!cfg.eqlIgnoringMapSize(self.config)) {
                self.malformed += 1;
                return;
            },
            .writer_grant => |g| self.fence.onGrant(g.writer_epoch),
            .writer_release => |r| self.fence.onRelease(r.writer_epoch),
            .data => |records| {
                if (!self.fence.acceptsData(frame.header.writer_epoch)) {
                    self.fenced += 1;
                    return;
                }
                var it = records;
                while (it.next()) |r| {
                    if (r.op == .clear) {
                        try txn.clear();
                        self.clears += 1;
                        continue;
                    }
                    if (try txn.getHeader(r.key)) |old| {
                        if (old.expire_at_ns != 0) try txn.expiryRemove(old.expire_at_ns, r.key);
                    }
                    if (r.op == .delete) self.deletes += 1 else self.puts += 1;
                    if (r.op == .delete or kv.format.isExpired(r.expire_at_ns, now)) {
                        try txn.delete(r.key);
                    } else {
                        try txn.put(r.key, .{ .write_index = index, .expire_at_ns = r.expire_at_ns }, r.value);
                        if (r.expire_at_ns != 0) try txn.expiryInsert(r.expire_at_ns, r.key);
                    }
                }
            },
        }
        self.frames += 1;
    }

    pub fn setMeta(self: *const Applier, txn: *kv.engine.WriteTxn) !void {
        try txn.setMeta(.{ .engine_kind = kv.engine.engine_kind, .store_id = self.store_id, .applied_index = self.index orelse 0, .applied_leader_epoch = self.leader_epoch, .writer_epoch = self.fence.current_writer_epoch });
    }
};

test "queue index continuity accepts cycle rolls but rejects holes" {
    try std.testing.expect(Applier.follows(0x100000004, 0x100000005));
    try std.testing.expect(Applier.follows(0x100000004, 0x200000000));
    try std.testing.expect(!Applier.follows(0x100000004, 0x100000006));
    try std.testing.expect(!Applier.follows(9, 8));
}
