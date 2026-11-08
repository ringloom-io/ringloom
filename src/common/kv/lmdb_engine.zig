// SPDX-License-Identifier: Apache-2.0
//! LMDB implementation of the KV storage engine interface
//! (docs/kv-store-spec.md §7.1/§7.2). Select via `engine.zig`.
//!
//! Layout of one environment (one directory per store generation):
//!   `data`   : key → `KvValueHeader(16 B LE) ‖ value`
//!   `expiry` : `be64(expire_at_ns) ‖ key` → empty (only keys with a TTL)
//!   `meta`   : fixed keys, each a little-endian u64
//!
//! **Entry / get values include the header.** Everything handed back by
//! `ReadSession.get`, `cursorFirst` and `cursorNext` is the stored form
//! `header ‖ value`; decode the first `value_header_len` bytes with
//! `KvValueHeader.read` and treat the rest as the user value. Views point
//! straight into the memory map and stay valid until `ReadSession.end()`.
//!
//! One writer per environment (LMDB enforces it with a lock; the applier owns
//! it). Not thread-safe: a `WriteEnv`/`WriteTxn`/`ReadSession` must be used by
//! one thread at a time (sessions are NOTLS, so they may migrate between
//! threads between `end()` and the next `begin()`).
const std = @import("std");
const format = @import("kv_format.zig");

const KvValueHeader = format.KvValueHeader;
const KvEngineMeta = format.KvEngineMeta;

pub const value_header_len: usize = @sizeOf(KvValueHeader);

pub const Error = error{
    /// The map is full (`MDB_MAP_FULL`): abort and rebuild into a larger generation.
    MapFull,
    /// Too many dirty pages for one transaction (`MDB_TXN_FULL`): commit smaller batches.
    TxnFull,
    /// All reader slots are taken (`MDB_READERS_FULL`).
    ReadersFull,
    /// Key empty or longer than the engine limit.
    InvalidKey,
    /// Opening or creating the environment failed (missing dir, permissions, ...).
    OpenFailed,
    /// Any other engine failure; see `last_error_code`.
    EngineFailure,
};

/// One key/value pair. `value` is `KvValueHeader ‖ user value` (see file docs).
pub const Entry = struct {
    key: []const u8,
    value: []const u8,
};

pub const EnvStats = struct {
    /// Configured map size in bytes.
    map_size: u64,
    /// High-water mark of pages ever allocated, in bytes (`(last_pgno + 1) * page_size`).
    /// Recycled pages are reused first, so this only grows until the map is full.
    used_bytes: u64,
    page_size: u32,
    max_readers: u32,
    /// Active snapshot slots, including stale dead processes until readerCheck.
    /// Idle/reset sessions are excluded; concurrent changes are best effort.
    readers_in_use: u32,
    /// LMDB exposes transaction IDs, not snapshot start times. Never infer time
    /// from transaction distance; cross-process age requires external telemetry.
    oldest_reader_age_ms: ?u64 = null,
};

/// Last raw LMDB return code that was mapped to `Error.EngineFailure`/`OpenFailed`
/// (per thread; for logging).
pub threadlocal var last_error_code: c_int = 0;

// ── Allocation counters (alloc_shim.c) ───────────────────────────────────────

extern fn ringloom_kv_alloc_scope(owner: ?*u64) ?*u64;
const AllocScope = struct {
    previous: ?*u64,
    fn begin(owner: *u64) AllocScope {
        return .{ .previous = ringloom_kv_alloc_scope(owner) };
    }
    fn end(self: AllocScope) void {
        _ = ringloom_kv_alloc_scope(self.previous);
    }
};

extern fn ringloom_kv_engine_allocs() u64;
extern fn ringloom_kv_engine_frees() u64;
extern fn ringloom_kv_engine_alloc_bytes() u64;
extern fn ringloom_kv_engine_alloc_reset() void;

/// Process-wide counts of malloc/calloc/realloc/free calls made inside LMDB.
pub const alloc_stats = struct {
    /// malloc + calloc + realloc calls.
    pub fn allocs() u64 {
        return ringloom_kv_engine_allocs();
    }
    pub fn frees() u64 {
        return ringloom_kv_engine_frees();
    }
    pub fn bytes() u64 {
        return ringloom_kv_engine_alloc_bytes();
    }
    pub fn reset() void {
        ringloom_kv_engine_alloc_reset();
    }
};

// ── Raw LMDB bindings ─────────────────────────────────────────────────────────

const MdbEnv = opaque {};
const MdbTxn = opaque {};
const MdbCursor = opaque {};
const Dbi = c_uint;

const MdbVal = extern struct {
    size: usize,
    data: ?*anyopaque,
};
const MdbStat = extern struct {
    psize: c_uint,
    depth: c_uint,
    branch_pages: usize,
    leaf_pages: usize,
    overflow_pages: usize,
    entries: usize,
};
const MdbEnvInfo = extern struct {
    mapaddr: ?*anyopaque,
    mapsize: usize,
    last_pgno: usize,
    last_txnid: usize,
    maxreaders: c_uint,
    numreaders: c_uint,
};

const MDB_WRITEMAP: c_uint = 0x80000;
const MDB_NOSYNC: c_uint = 0x10000;
const MDB_NOMETASYNC: c_uint = 0x40000;
const MDB_NOTLS: c_uint = 0x200000;
const MDB_NORDAHEAD: c_uint = 0x800000;
const MDB_RDONLY: c_uint = 0x20000;
const MDB_CREATE: c_uint = 0x40000;
const MDB_RESERVE: c_uint = 0x10000;

const MDB_FIRST: c_uint = 0;
const MDB_NEXT: c_uint = 8;

const MDB_NOTFOUND: c_int = -30798;
const MDB_MAP_FULL: c_int = -30792;
const MDB_READERS_FULL: c_int = -30790;
const MDB_TXN_FULL: c_int = -30788;
const MDB_BAD_VALSIZE: c_int = -30781;

extern fn mdb_env_create(env: *?*MdbEnv) c_int;
extern fn mdb_env_open(env: *MdbEnv, path: [*:0]const u8, flags: c_uint, mode: c_uint) c_int;
extern fn mdb_env_close(env: *MdbEnv) void;
extern fn mdb_env_set_mapsize(env: *MdbEnv, size: usize) c_int;
extern fn mdb_env_set_maxreaders(env: *MdbEnv, readers: c_uint) c_int;
extern fn mdb_env_set_maxdbs(env: *MdbEnv, dbs: Dbi) c_int;
extern fn mdb_env_get_maxkeysize(env: *MdbEnv) c_int;
extern fn mdb_env_stat(env: *MdbEnv, stat: *MdbStat) c_int;
extern fn mdb_env_info(env: *MdbEnv, info: *MdbEnvInfo) c_int;
extern fn mdb_reader_list(env: *MdbEnv, callback: *const fn ([*:0]const u8, ?*anyopaque) callconv(.c) c_int, ctx: ?*anyopaque) c_int;
extern fn mdb_reader_check(env: *MdbEnv, dead: *c_int) c_int;
extern fn mdb_txn_begin(env: *MdbEnv, parent: ?*MdbTxn, flags: c_uint, txn: *?*MdbTxn) c_int;
extern fn mdb_txn_commit(txn: *MdbTxn) c_int;
extern fn mdb_txn_abort(txn: *MdbTxn) void;
extern fn mdb_txn_reset(txn: *MdbTxn) void;
extern fn mdb_txn_renew(txn: *MdbTxn) c_int;
extern fn mdb_dbi_open(txn: *MdbTxn, name: [*:0]const u8, flags: c_uint, dbi: *Dbi) c_int;
extern fn mdb_stat(txn: *MdbTxn, dbi: Dbi, stat: *MdbStat) c_int;
extern fn mdb_drop(txn: *MdbTxn, dbi: Dbi, del: c_int) c_int;
extern fn mdb_get(txn: *MdbTxn, dbi: Dbi, key: *MdbVal, data: *MdbVal) c_int;
extern fn mdb_put(txn: *MdbTxn, dbi: Dbi, key: *MdbVal, data: *MdbVal, flags: c_uint) c_int;
extern fn mdb_del(txn: *MdbTxn, dbi: Dbi, key: *MdbVal, data: ?*MdbVal) c_int;
extern fn mdb_cursor_open(txn: *MdbTxn, dbi: Dbi, cursor: *?*MdbCursor) c_int;
extern fn mdb_cursor_close(cursor: *MdbCursor) void;
extern fn mdb_cursor_renew(txn: *MdbTxn, cursor: *MdbCursor) c_int;
extern fn mdb_cursor_get(cursor: *MdbCursor, key: *MdbVal, data: *MdbVal, op: c_uint) c_int;
extern fn mdb_cursor_del(cursor: *MdbCursor, flags: c_uint) c_int;
extern "c" fn mkdir(path: [*:0]const u8, mode: c_uint) c_int;

inline fn val(bytes: []const u8) MdbVal {
    return .{ .size = bytes.len, .data = @ptrCast(@constCast(bytes.ptr)) };
}

inline fn slice(v: MdbVal) []const u8 {
    if (v.size == 0) return &.{};
    const p: [*]const u8 = @ptrCast(v.data.?);
    return p[0..v.size];
}

fn mapError(rc: c_int) Error {
    switch (rc) {
        MDB_MAP_FULL => return error.MapFull,
        MDB_TXN_FULL => return error.TxnFull,
        MDB_READERS_FULL => return error.ReadersFull,
        MDB_BAD_VALSIZE => return error.InvalidKey,
        else => {
            last_error_code = rc;
            return error.EngineFailure;
        },
    }
}

inline fn check(rc: c_int) Error!void {
    if (rc != 0) return mapError(rc);
}

/// Copies `dir` into `buf` as a NUL-terminated path.
fn pathZ(buf: []u8, dir: []const u8) Error![:0]const u8 {
    if (dir.len == 0 or dir.len >= buf.len) return error.OpenFailed;
    @memcpy(buf[0..dir.len], dir);
    buf[dir.len] = 0;
    return buf[0..dir.len :0];
}

/// `mkdir -p`; failures surface later from `mdb_env_open`.
fn makePath(path_buf: []u8, dir: []const u8) void {
    var i: usize = 1;
    while (i <= dir.len) : (i += 1) {
        if (i == dir.len or dir[i] == '/') {
            const p = pathZ(path_buf, dir[0..i]) catch return;
            _ = mkdir(p.ptr, 0o777);
        }
    }
}

// Supported reader-list API emits pid, thread and txnid; reset slots use "-".
fn countReader(message: [*:0]const u8, context: ?*anyopaque) callconv(.c) c_int {
    const count: *u32 = @ptrCast(@alignCast(context.?));
    var fields = std.mem.tokenizeAny(u8, std.mem.span(message), " \t\r\n");
    const pid = fields.next() orelse return 0;
    _ = std.fmt.parseInt(u32, pid, 10) catch return 0;
    _ = fields.next() orelse return 0;
    const txn = fields.next() orelse return 0;
    _ = std.fmt.parseInt(u64, txn, 10) catch return 0;
    count.* += 1;
    return 0;
}

pub const DataStats = struct { entries: u64, live_bytes: u64 };

// ── Writer side ────────────────────────────────────────────────────────────

/// Largest key (plus 8-byte expiry prefix) the stack staging buffer supports.
const max_expiry_key_len = 2048 + 8;

const meta_keys = struct {
    const live_bytes = "metric_live_user_bytes";
    const format_version = "format_version";
    const store_id = "store_id";
    const applied_index = "applied_index";
    const applied_leader_epoch = "applied_leader_epoch";
    const writer_epoch = "writer_epoch";
    const engine_kind = "engine_kind";
};

pub const WriteEnv = struct {
    env: *MdbEnv,
    dbi_data: Dbi,
    dbi_expiry: Dbi,
    dbi_meta: Dbi,
    map_size: u64,
    owned_allocs: u64 = 0,

    /// Creates (or reopens) the environment in `dir` (created with parents if
    /// missing) with a fixed `map_size`. Setup path: allocates.
    pub fn create(dir: []const u8, map_size: u64, max_readers: u32) Error!WriteEnv {
        var owned_allocs: u64 = 0;
        const scope = AllocScope.begin(&owned_allocs);
        defer scope.end();
        var pbuf: [4096]u8 = undefined;
        makePath(&pbuf, dir);
        const path = try pathZ(&pbuf, dir);

        var env_opt: ?*MdbEnv = null;
        try check(mdb_env_create(&env_opt));
        const env = env_opt.?;
        errdefer mdb_env_close(env);
        try check(mdb_env_set_maxdbs(env, 3));
        try check(mdb_env_set_maxreaders(env, max_readers));
        try check(mdb_env_set_mapsize(env, @intCast(map_size)));
        const flags = MDB_WRITEMAP | MDB_NOSYNC | MDB_NOMETASYNC | MDB_NOTLS | MDB_NORDAHEAD;
        // 0o666 & ~umask: reader processes need write access to lock.mdb.
        const rc = mdb_env_open(env, path.ptr, flags, 0o666);
        if (rc != 0) {
            last_error_code = rc;
            return error.OpenFailed;
        }

        var txn_opt: ?*MdbTxn = null;
        try check(mdb_txn_begin(env, null, 0, &txn_opt));
        const txn = txn_opt.?;
        errdefer if (txn_opt) |pending| mdb_txn_abort(pending);
        var self: WriteEnv = .{ .env = env, .dbi_data = 0, .dbi_expiry = 0, .dbi_meta = 0, .map_size = map_size };
        try check(mdb_dbi_open(txn, "data", MDB_CREATE, &self.dbi_data));
        try check(mdb_dbi_open(txn, "expiry", MDB_CREATE, &self.dbi_expiry));
        try check(mdb_dbi_open(txn, "meta", MDB_CREATE, &self.dbi_meta));
        // Seed older environments once at open; later mutations update this
        // metadata in the same transaction as data, including abort/rollback.
        var metric_txn: WriteTxn = .{ .txn = txn, .dbi_data = self.dbi_data, .dbi_expiry = self.dbi_expiry, .dbi_meta = self.dbi_meta, .owner_allocs = &owned_allocs };
        if (try metric_txn.getMeta(meta_keys.live_bytes) == null) {
            const initial = try scanData(txn, self.dbi_data);
            try metric_txn.putMeta(meta_keys.live_bytes, initial.live_bytes);
        }
        // Commit consumes the transaction on success AND failure (lmdb.h).
        // Transfer ownership before checking its result so errdefer cannot
        // abort a freed handle; environment cleanup remains armed.
        txn_opt = null;
        try check(mdb_txn_commit(txn));
        self.owned_allocs = owned_allocs;
        return self;
    }

    pub fn close(self: *WriteEnv) void {
        const scope = AllocScope.begin(&self.owned_allocs);
        defer scope.end();
        mdb_env_close(self.env);
        self.* = undefined;
    }

    /// Starts the (single) write transaction. Blocks if another is open.
    pub fn begin(self: *WriteEnv) Error!WriteTxn {
        const scope = AllocScope.begin(&self.owned_allocs);
        defer scope.end();
        var txn: ?*MdbTxn = null;
        try check(mdb_txn_begin(self.env, null, 0, &txn));
        return .{ .txn = txn.?, .dbi_data = self.dbi_data, .dbi_expiry = self.dbi_expiry, .dbi_meta = self.dbi_meta, .owner_allocs = &self.owned_allocs };
    }

    /// Clears slots held by dead reader processes (`mdb_reader_check`). Returns
    /// the number of stale slots released.
    pub fn readerCheck(self: *WriteEnv) Error!u32 {
        const scope = AllocScope.begin(&self.owned_allocs);
        defer scope.end();
        var dead: c_int = 0;
        try check(mdb_reader_check(self.env, &dead));
        return @intCast(dead);
    }

    /// Map size, high-water used bytes and reader slots (for `kv_map_used_pct`
    /// and capacity growth, §7.3). No allocation.
    pub fn stats(self: *WriteEnv) Error!EnvStats {
        const scope = AllocScope.begin(&self.owned_allocs);
        defer scope.end();
        var st: MdbStat = undefined;
        var info: MdbEnvInfo = undefined;
        try check(mdb_env_stat(self.env, &st));
        try check(mdb_env_info(self.env, &info));
        var active: u32 = 0;
        try check(mdb_reader_list(self.env, countReader, &active));
        return .{
            .map_size = info.mapsize,
            .used_bytes = (@as(u64, info.last_pgno) + 1) * st.psize,
            .page_size = st.psize,
            .max_readers = info.maxreaders,
            .readers_in_use = active,
        };
    }

    /// Allocation attempts inside this writer handle and its transactions,
    /// including setup and diagnostics. Independent read handles and other
    /// environments are excluded. Single-thread owner, like WriteEnv itself.
    pub fn engineAllocs(self: *const WriteEnv) u64 {
        return self.owned_allocs;
    }

    /// Exact committed user key + value bytes (excluding headers, metadata,
    /// and expiry index). Expired records count until physically swept.
    /// Opens a short read transaction; constant work, no data scan. Entries
    /// and bytes share one snapshot. Older environments are seeded at create.
    pub fn measureData(self: *WriteEnv) Error!DataStats {
        const scope = AllocScope.begin(&self.owned_allocs);
        defer scope.end();
        var txn: ?*MdbTxn = null;
        try check(mdb_txn_begin(self.env, null, MDB_RDONLY, &txn));
        defer mdb_txn_abort(txn.?);
        var st: MdbStat = undefined;
        try check(mdb_stat(txn.?, self.dbi_data, &st));
        var key = val(meta_keys.live_bytes);
        var data: MdbVal = undefined;
        try check(mdb_get(txn.?, self.dbi_meta, &key, &data));
        if (data.size != 8) return error.EngineFailure;
        return .{ .entries = st.entries, .live_bytes = std.mem.readInt(u64, slice(data)[0..8], .little) };
    }

    pub fn usedBytes(self: *WriteEnv) Error!u64 {
        const scope = AllocScope.begin(&self.owned_allocs);
        defer scope.end();
        return (try self.stats()).used_bytes;
    }

    pub fn mapSize(self: *const WriteEnv) u64 {
        return self.map_size;
    }

    /// Longest key the engine accepts (derived from the page size).
    pub fn maxKeyLen(self: *const WriteEnv) usize {
        return @intCast(mdb_env_get_maxkeysize(self.env));
    }

    /// Number of entries in `data`, as of the latest commit. Allocates (opens a
    /// short read transaction); intended for stats, not the hot path. Inside a
    /// write transaction prefer `WriteTxn.entryCount`.
    pub fn entryCount(self: *WriteEnv) Error!u64 {
        const scope = AllocScope.begin(&self.owned_allocs);
        defer scope.end();
        var txn_opt: ?*MdbTxn = null;
        try check(mdb_txn_begin(self.env, null, MDB_RDONLY, &txn_opt));
        const txn = txn_opt.?;
        defer mdb_txn_abort(txn);
        var st: MdbStat = undefined;
        try check(mdb_stat(txn, self.dbi_data, &st));
        return st.entries;
    }
};

/// A write transaction. `abort()` after `commit()` (or a second `abort()`) is a
/// no-op, so `defer txn.abort()` is always safe.
pub const WriteTxn = struct {
    txn: *MdbTxn,
    finished: bool = false,
    dbi_data: Dbi,
    dbi_expiry: Dbi,
    dbi_meta: Dbi,
    owner_allocs: *u64,

    /// Stores `hdr ‖ value` under `key` and updates transactional live bytes.
    /// Uses `MDB_RESERVE`; LMDB may allocate when dirtying metadata pages.
    pub fn put(self: *WriteTxn, key: []const u8, hdr: KvValueHeader, value: []const u8) Error!void {
        const scope = AllocScope.begin(self.owner_allocs);
        defer scope.end();
        const previous = try self.userBytes(key);
        const total = (try self.getMeta(meta_keys.live_bytes)) orelse return error.EngineFailure;
        var k = val(key);
        var d = MdbVal{ .size = value_header_len + value.len, .data = null };
        try check(mdb_put(self.txn, self.dbi_data, &k, &d, MDB_RESERVE));
        const out: [*]u8 = @ptrCast(d.data.?);
        hdr.write(out[0..value_header_len]);
        @memcpy(out[value_header_len..][0..value.len], value);
        try self.putMeta(meta_keys.live_bytes, total - previous + key.len + value.len);
    }

    /// Removes `key` from `data`. A missing key is not an error. Does not touch
    /// the expiry index (call `expiryRemove` with the old expiry).
    pub fn delete(self: *WriteTxn, key: []const u8) Error!void {
        const scope = AllocScope.begin(self.owner_allocs);
        defer scope.end();
        var k = val(key);
        const previous = try self.userBytes(key);
        const total = (try self.getMeta(meta_keys.live_bytes)) orelse return error.EngineFailure;
        const rc = mdb_del(self.txn, self.dbi_data, &k, null);
        if (rc != 0 and rc != MDB_NOTFOUND) return mapError(rc);
        if (rc == 0) try self.putMeta(meta_keys.live_bytes, total - previous);
    }

    /// The header of the currently stored value for `key` as seen by this
    /// transaction (including its own uncommitted writes), or null. Used by the
    /// applier to find the old expiry before overwriting/deleting (§8.2).
    pub fn getHeader(self: *WriteTxn, key: []const u8) Error!?KvValueHeader {
        const scope = AllocScope.begin(self.owner_allocs);
        defer scope.end();
        var k = val(key);
        var d: MdbVal = undefined;
        const rc = mdb_get(self.txn, self.dbi_data, &k, &d);
        if (rc == MDB_NOTFOUND) return null;
        try check(rc);
        if (d.size < value_header_len) return null;
        return KvValueHeader.read(slice(d));
    }

    /// Empties `data` and `expiry` (`meta` is kept).
    pub fn clear(self: *WriteTxn) Error!void {
        const scope = AllocScope.begin(self.owner_allocs);
        defer scope.end();
        try check(mdb_drop(self.txn, self.dbi_data, 0));
        try check(mdb_drop(self.txn, self.dbi_expiry, 0));
        try self.putMeta(meta_keys.live_bytes, 0);
    }

    pub fn expiryInsert(self: *WriteTxn, expire_at_ns: u64, key: []const u8) Error!void {
        const scope = AllocScope.begin(self.owner_allocs);
        defer scope.end();
        var buf: [max_expiry_key_len]u8 = undefined;
        var k = try expiryKey(&buf, expire_at_ns, key);
        var d = MdbVal{ .size = 0, .data = null };
        try check(mdb_put(self.txn, self.dbi_expiry, &k, &d, 0));
    }

    pub fn expiryRemove(self: *WriteTxn, expire_at_ns: u64, key: []const u8) Error!void {
        const scope = AllocScope.begin(self.owner_allocs);
        defer scope.end();
        var buf: [max_expiry_key_len]u8 = undefined;
        var k = try expiryKey(&buf, expire_at_ns, key);
        const rc = mdb_del(self.txn, self.dbi_expiry, &k, null);
        if (rc != 0 and rc != MDB_NOTFOUND) return mapError(rc);
    }

    /// Deletes up to `max` entries whose expiry is `<= now_ns` from `expiry`
    /// and, when the data entry still carries that same expiry, from `data`.
    /// Opens one cursor (allocates; sweep setup is allowed to, §14.1).
    pub fn sweepExpired(self: *WriteTxn, now_ns: u64, max: u32) Error!u32 {
        const scope = AllocScope.begin(self.owner_allocs);
        defer scope.end();
        var cur_opt: ?*MdbCursor = null;
        try check(mdb_cursor_open(self.txn, self.dbi_expiry, &cur_opt));
        const cur = cur_opt.?;
        defer mdb_cursor_close(cur);
        var removed: u32 = 0;
        while (removed < max) {
            var k: MdbVal = undefined;
            var d: MdbVal = undefined;
            const rc = mdb_cursor_get(cur, &k, &d, MDB_FIRST);
            if (rc == MDB_NOTFOUND) break;
            try check(rc);
            const ek = slice(k);
            if (ek.len < 8) {
                try check(mdb_cursor_del(cur, 0)); // corrupt entry; drop it
                continue;
            }
            const expire_at = std.mem.readInt(u64, ek[0..8], .big);
            if (expire_at > now_ns) break;
            // Copy-free: `ek` points into the map; mdb_del on another db may
            // dirty pages, so use the key before deleting the expiry entry.
            var dk = val(ek[8..]);
            var dv: MdbVal = undefined;
            const grc = mdb_get(self.txn, self.dbi_data, &dk, &dv);
            if (grc == 0) {
                if (dv.size >= value_header_len and KvValueHeader.read(slice(dv)).expire_at_ns == expire_at) {
                    try self.delete(slice(dk));
                }
            } else if (grc != MDB_NOTFOUND) return mapError(grc);
            try check(mdb_cursor_del(cur, 0));
            removed += 1;
        }
        return removed;
    }

    /// Writes the `meta` keys (each as a little-endian u64).
    pub fn setMeta(self: *WriteTxn, meta: KvEngineMeta) Error!void {
        const scope = AllocScope.begin(self.owner_allocs);
        defer scope.end();
        try self.putMeta(meta_keys.format_version, meta.format_version);
        try self.putMeta(meta_keys.engine_kind, meta.engine_kind);
        try self.putMeta(meta_keys.store_id, meta.store_id);
        try self.putMeta(meta_keys.applied_index, meta.applied_index);
        try self.putMeta(meta_keys.applied_leader_epoch, meta.applied_leader_epoch);
        try self.putMeta(meta_keys.writer_epoch, meta.writer_epoch);
    }

    /// Reads one meta value (little-endian u64) by key; null when absent.
    pub fn getMeta(self: *WriteTxn, key: []const u8) Error!?u64 {
        const scope = AllocScope.begin(self.owner_allocs);
        defer scope.end();
        var k = val(key);
        var d: MdbVal = undefined;
        const rc = mdb_get(self.txn, self.dbi_meta, &k, &d);
        if (rc == MDB_NOTFOUND) return null;
        try check(rc);
        if (d.size != 8) return null;
        return std.mem.readInt(u64, slice(d)[0..8], .little);
    }

    /// Number of entries in `data` as seen by this transaction.
    pub fn entryCount(self: *WriteTxn) Error!u64 {
        const scope = AllocScope.begin(self.owner_allocs);
        defer scope.end();
        var st: MdbStat = undefined;
        try check(mdb_stat(self.txn, self.dbi_data, &st));
        return st.entries;
    }

    /// Number of entries in the `expiry` index as seen by this transaction.
    pub fn expiryCount(self: *WriteTxn) Error!u64 {
        const scope = AllocScope.begin(self.owner_allocs);
        defer scope.end();
        var st: MdbStat = undefined;
        try check(mdb_stat(self.txn, self.dbi_expiry, &st));
        return st.entries;
    }

    pub fn commit(self: *WriteTxn) Error!void {
        const scope = AllocScope.begin(self.owner_allocs);
        defer scope.end();
        std.debug.assert(!self.finished);
        // mdb_txn_commit frees the transaction on success and on failure.
        self.finished = true;
        try check(mdb_txn_commit(self.txn));
    }

    pub fn abort(self: *WriteTxn) void {
        const scope = AllocScope.begin(self.owner_allocs);
        defer scope.end();
        if (self.finished) return;
        self.finished = true;
        mdb_txn_abort(self.txn);
    }

    fn userBytes(self: *WriteTxn, key: []const u8) Error!u64 {
        var k = val(key);
        var data: MdbVal = undefined;
        const rc = mdb_get(self.txn, self.dbi_data, &k, &data);
        if (rc == MDB_NOTFOUND) return 0;
        try check(rc);
        if (data.size < value_header_len) return error.EngineFailure;
        return key.len + data.size - value_header_len;
    }

    fn putMeta(self: *WriteTxn, key: []const u8, v: u64) Error!void {
        var k = val(key);
        var bytes: [8]u8 = undefined;
        std.mem.writeInt(u64, &bytes, v, .little);
        var d = val(&bytes);
        try check(mdb_put(self.txn, self.dbi_meta, &k, &d, 0));
    }
};

// One-time migration for environments created before live-byte bookkeeping.
fn scanData(txn: *MdbTxn, dbi: Dbi) Error!DataStats {
    var cursor: ?*MdbCursor = null;
    try check(mdb_cursor_open(txn, dbi, &cursor));
    defer mdb_cursor_close(cursor.?);
    var result: DataStats = .{ .entries = 0, .live_bytes = 0 };
    var key: MdbVal = undefined;
    var data: MdbVal = undefined;
    var op: c_uint = MDB_FIRST;
    while (true) {
        const rc = mdb_cursor_get(cursor.?, &key, &data, op);
        if (rc == MDB_NOTFOUND) break;
        try check(rc);
        if (data.size < value_header_len) return error.EngineFailure;
        result.entries += 1;
        result.live_bytes += key.size + data.size - value_header_len;
        op = MDB_NEXT;
    }
    return result;
}

fn expiryKey(buf: *[max_expiry_key_len]u8, expire_at_ns: u64, key: []const u8) Error!MdbVal {
    if (key.len == 0 or key.len + 8 > buf.len) return error.InvalidKey;
    std.mem.writeInt(u64, buf[0..8], expire_at_ns, .big);
    @memcpy(buf[8..][0..key.len], key);
    return val(buf[0 .. 8 + key.len]);
}

// ── Reader side ────────────────────────────────────────────────────────────

pub const ReadEnv = struct {
    env: *MdbEnv,
    dbi_data: Dbi,

    /// Opens an existing environment read-only. Setup path: allocates.
    pub fn open(dir: []const u8, max_readers: u32) Error!ReadEnv {
        var pbuf: [4096]u8 = undefined;
        const path = try pathZ(&pbuf, dir);

        var env_opt: ?*MdbEnv = null;
        try check(mdb_env_create(&env_opt));
        const env = env_opt.?;
        errdefer mdb_env_close(env);
        try check(mdb_env_set_maxdbs(env, 3));
        try check(mdb_env_set_maxreaders(env, max_readers));
        const rc = mdb_env_open(env, path.ptr, MDB_RDONLY | MDB_NOTLS | MDB_NORDAHEAD, 0o666);
        if (rc != 0) {
            last_error_code = rc;
            return error.OpenFailed;
        }
        var txn_opt: ?*MdbTxn = null;
        try check(mdb_txn_begin(env, null, MDB_RDONLY, &txn_opt));
        const txn = txn_opt.?;
        var dbi: Dbi = 0;
        const drc = mdb_dbi_open(txn, "data", 0, &dbi);
        if (drc != 0) {
            mdb_txn_abort(txn);
            last_error_code = drc;
            return error.OpenFailed;
        }
        // Committing a read-only txn publishes the handle to the environment.
        try check(mdb_txn_commit(txn));
        return .{ .env = env, .dbi_data = dbi };
    }

    /// Closes the environment. All sessions must be `deinit`ed first.
    pub fn close(self: *ReadEnv) void {
        mdb_env_close(self.env);
        self.* = undefined;
    }

    /// Claims a reader slot (`READERS_FULL` when exhausted) and prepares a
    /// reusable cursor. Setup path: allocates.
    pub fn newSession(self: *ReadEnv) Error!ReadSession {
        var txn_opt: ?*MdbTxn = null;
        try check(mdb_txn_begin(self.env, null, MDB_RDONLY, &txn_opt));
        const txn = txn_opt.?;
        errdefer mdb_txn_abort(txn);
        var cur_opt: ?*MdbCursor = null;
        try check(mdb_cursor_open(txn, self.dbi_data, &cur_opt));
        mdb_txn_reset(txn);
        return .{ .txn = txn, .cursor = cur_opt.?, .dbi_data = self.dbi_data, .active = false };
    }

    /// Entry count of the current snapshot (opens a transaction; allocates).
    pub fn entryCount(self: *ReadEnv) Error!u64 {
        var txn_opt: ?*MdbTxn = null;
        try check(mdb_txn_begin(self.env, null, MDB_RDONLY, &txn_opt));
        const txn = txn_opt.?;
        defer mdb_txn_abort(txn);
        var st: MdbStat = undefined;
        try check(mdb_stat(txn, self.dbi_data, &st));
        return st.entries;
    }
};

pub const ReadSession = struct {
    txn: *MdbTxn,
    cursor: *MdbCursor,
    dbi_data: Dbi,
    active: bool,

    /// Starts a snapshot of the latest committed state. No allocation.
    pub fn begin(self: *ReadSession) Error!void {
        std.debug.assert(!self.active);
        try check(mdb_txn_renew(self.txn));
        self.active = true;
    }

    /// Looks up `key`; the result is `KvValueHeader ‖ value`, a view into the
    /// map valid until `end()`. Null when absent. No allocation.
    pub fn get(self: *ReadSession, key: []const u8) Error!?[]const u8 {
        std.debug.assert(self.active);
        var k = val(key);
        var d: MdbVal = undefined;
        const rc = mdb_get(self.txn, self.dbi_data, &k, &d);
        if (rc == MDB_NOTFOUND) return null;
        try check(rc);
        return slice(d);
    }

    /// Positions the (reused) cursor on the first entry in engine order.
    pub fn cursorFirst(self: *ReadSession) Error!?Entry {
        std.debug.assert(self.active);
        try check(mdb_cursor_renew(self.txn, self.cursor));
        return self.cursorOp(MDB_FIRST);
    }

    pub fn cursorNext(self: *ReadSession) Error!?Entry {
        std.debug.assert(self.active);
        return self.cursorOp(MDB_NEXT);
    }

    /// Ends the snapshot; all views are invalid afterwards. No allocation.
    pub fn end(self: *ReadSession) void {
        if (!self.active) return;
        mdb_txn_reset(self.txn);
        self.active = false;
    }

    /// Releases the reader slot and frees the session.
    pub fn deinit(self: *ReadSession) void {
        mdb_cursor_close(self.cursor);
        mdb_txn_abort(self.txn);
        self.* = undefined;
    }

    fn cursorOp(self: *ReadSession, op: c_uint) Error!?Entry {
        var k: MdbVal = undefined;
        var d: MdbVal = undefined;
        const rc = mdb_cursor_get(self.cursor, &k, &d, op);
        if (rc == MDB_NOTFOUND) return null;
        try check(rc);
        return .{ .key = slice(k), .value = slice(d) };
    }
};
