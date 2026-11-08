// SPDX-License-Identifier: Apache-2.0
//! `KvLocalStores`: the small interface the control loop uses to answer
//! `OpenKvReader` (docs/kv-store-spec.md §11.1, §12) without knowing about the
//! applier. The KV subsystem (K05, `kv_subsystem.zig`) provides the real
//! implementation; until then (and whenever `broker.kv.enabled=false`) the
//! default `unavailable` value makes the control loop reply
//! `KV_NOT_AVAILABLE_LOCALLY`.
//!
//! ## Contract for the implementation
//!
//! * `lookup` runs on the **control-loop thread**, must not block or allocate,
//!   and may be called at service-open frequency (not per read).
//! * It copies the absolute store directory (the directory that contains
//!   `control.shm`, §5) into `dir_out` and returns its length in
//!   `LocalStore.dir_len`. `dir_out` is `kv_control_messages.kv_store_dir_max`
//!   bytes; a longer path must be reported as "not found" (never truncated).
//! * Return `null` if the store is not known locally yet (no `__kv.<name>` log
//!   replicated here, or the applier has not created it); the broker answers
//!   `UNKNOWN_STORE`.
//! * `ready == false` means the store exists but has no published generation
//!   yet (`KvStoreState.initializing`); the broker answers `NOT_READY` and the
//!   client may retry or open anyway and see `NOT_READY` from the control header.
//! * `config` is the replicated config from the log's `store_header`.

const std = @import("std");
const common = @import("ringloom_common");
const kv = common.kv;
const msgs = common.message.kv_control_messages;

pub const LocalStore = struct {
    store_id: u64,
    dir_len: u16,
    /// `kv_format.engine_kind_*` (what `control.shm` records).
    engine_kind: u32,
    config: kv.KvStoreConfig,
    ready: bool,
};

pub const KvLocalStores = struct {
    /// `broker.kv.enabled`. False => every lookup is `KV_NOT_AVAILABLE_LOCALLY`.
    enabled: bool = false,
    ctx: ?*anyopaque = null,
    lookup_fn: ?*const fn (ctx: ?*anyopaque, store_name: []const u8, dir_out: *[msgs.kv_store_dir_max]u8) ?LocalStore = null,

    /// Stub used until the KV subsystem registers itself.
    pub const unavailable: KvLocalStores = .{};

    pub fn lookup(self: *const KvLocalStores, store_name: []const u8, dir_out: *[msgs.kv_store_dir_max]u8) ?LocalStore {
        const f = self.lookup_fn orelse return null;
        return f(self.ctx, store_name, dir_out);
    }

    /// Status for `OpenKvReader` plus the data for the response.
    pub const OpenResult = struct { status: msgs.KvStatus, store: ?LocalStore = null };

    pub fn open(self: *const KvLocalStores, store_name: []const u8, dir_out: *[msgs.kv_store_dir_max]u8) OpenResult {
        if (!self.enabled) return .{ .status = .kv_not_available_locally };
        const s = self.lookup(store_name, dir_out) orelse return .{ .status = .unknown_store };
        return .{ .status = if (s.ready) .ok else .not_ready, .store = s };
    }
};

const testing = std.testing;

test "unavailable stub reports KV_NOT_AVAILABLE_LOCALLY" {
    var dir: [msgs.kv_store_dir_max]u8 = undefined;
    const r = KvLocalStores.unavailable.open("prices", &dir);
    try testing.expectEqual(msgs.KvStatus.kv_not_available_locally, r.status);
    try testing.expect(r.store == null);
}

const Fake = struct {
    ready: bool,
    fn lookup(ctx: ?*anyopaque, name: []const u8, out: *[msgs.kv_store_dir_max]u8) ?LocalStore {
        const self: *Fake = @ptrCast(@alignCast(ctx.?));
        if (!std.mem.eql(u8, name, "prices")) return null;
        const dir = "/dev/shm/kv/g/node-1/prices";
        @memcpy(out[0..dir.len], dir);
        return .{ .store_id = 5, .dir_len = dir.len, .engine_kind = kv.format.engine_kind_lmdb, .config = .{}, .ready = self.ready };
    }
};

test "enabled lookup: ok, not_ready, unknown_store" {
    var f = Fake{ .ready = true };
    const ls = KvLocalStores{ .enabled = true, .ctx = &f, .lookup_fn = Fake.lookup };
    var dir: [msgs.kv_store_dir_max]u8 = undefined;
    const ok = ls.open("prices", &dir);
    try testing.expectEqual(msgs.KvStatus.ok, ok.status);
    try testing.expectEqualStrings("/dev/shm/kv/g/node-1/prices", dir[0..ok.store.?.dir_len]);
    f.ready = false;
    try testing.expectEqual(msgs.KvStatus.not_ready, ls.open("prices", &dir).status);
    try testing.expectEqual(msgs.KvStatus.unknown_store, ls.open("nope", &dir).status);
    // Enabled flag false overrides any lookup fn.
    const off = KvLocalStores{ .enabled = false, .ctx = &f, .lookup_fn = Fake.lookup };
    try testing.expectEqual(msgs.KvStatus.kv_not_available_locally, off.open("prices", &dir).status);
}
