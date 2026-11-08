// SPDX-License-Identifier: Apache-2.0

//! KV store control header (`control.shm`) and generation directory helpers
//! (docs/kv-store-spec.md §5, §8.3).
//!
//! The broker (applier) maps the 4 KiB control file read-write; reader
//! processes map it read-only. Each cache line has a single writer.

const std = @import("std");
const posix = std.posix;
const platform_io = @import("../platform/io.zig");

const Atomic = std.atomic.Value;

/// File name of the control file inside the store directory.
pub const control_file_name = "control.shm";
/// Fixed control file size.
pub const control_file_size: usize = 4096;
/// `KvControlHeader.magic` ("RGLOOMKV").
pub const control_magic: u64 = 0x564B_4D4F_4F4C_4752;
/// Current control layout version.
pub const control_layout_version: u32 = 1;
/// Generation directory prefix.
pub const gen_dir_prefix = "gen-";
/// Longest generation directory name: `gen-` + 8 digits (more for n > 99,999,999).
pub const max_gen_dir_name_len: usize = 4 + 20;

pub const KvStoreState = enum(u32) {
    initializing = 0,
    catching_up = 1,
    live = 2,
    rebuilding = 3,
    failed = 4,
    _,
};

pub const KvControlHeader = extern struct {
    // ---- line 0: immutable after creation ----
    magic: Atomic(u64),
    layout_version: u32,
    engine_kind: u32,
    store_id: u64,
    max_key_len: u32,
    max_value_len: u32,
    _pad0: [32]u8,
    // ---- line 1: generation switch ----
    generation: Atomic(u64),
    state: Atomic(u32),
    _pad1: [52]u8,
    // ---- line 2: progress ----
    applied_index: Atomic(u64),
    applied_leader_epoch: Atomic(u64),
    applied_at_ns: Atomic(u64),
    _pad2: [40]u8,
    // ---- line 3: liveness / lag ----
    applier_heartbeat_ns: Atomic(u64),
    local_log_tip_index: Atomic(u64),
    replication_live: Atomic(u32),
    _pad3: [44]u8,
    // remainder of the 4 KiB page reserved
    _reserved: [control_file_size - 256]u8,

    comptime {
        std.debug.assert(@offsetOf(KvControlHeader, "magic") == 0);
        std.debug.assert(@offsetOf(KvControlHeader, "generation") == 64);
        std.debug.assert(@offsetOf(KvControlHeader, "applied_index") == 128);
        std.debug.assert(@offsetOf(KvControlHeader, "applier_heartbeat_ns") == 192);
        std.debug.assert(@sizeOf(KvControlHeader) <= 4096);
        std.debug.assert(@sizeOf(KvControlHeader) == control_file_size);
    }
};

pub const OpenError = error{
    BadMagic,
    LayoutMismatch,
    FileNotFound,
    InvalidFileSize,
    PathTooLong,
};

/// Progress snapshot read from line 2.
pub const Progress = struct {
    applied_index: u64,
    applied_leader_epoch: u64,
    applied_at_ns: u64,
};

/// A mapped control file. Create with `createOrOpen` (broker) or `openReadOnly`
/// (readers). `close` unmaps it.
pub const ControlFile = struct {
    header: *KvControlHeader,
    mapping: []align(std.heap.page_size_min) u8,
    writable: bool,

    pub const CreateError = OpenError || std.Io.File.OpenError || std.Io.Dir.CreateDirPathError ||
        posix.MMapError || std.Io.File.SetLengthError || std.Io.File.LengthError;

    /// Broker side: create `<store_dir>/control.shm` (4096 bytes) or validate the
    /// existing one. A new or never-initialised file gets line 0 written (magic
    /// last, with release ordering); an existing file must match magic, layout
    /// version, engine kind, store id and key/value limits, else
    /// `BadMagic`/`LayoutMismatch`. Existing runtime state is preserved so
    /// readers keep serving across a broker restart.
    pub fn createOrOpen(
        store_dir: []const u8,
        store_id: u64,
        engine_kind: u32,
        max_key_len: u32,
        max_value_len: u32,
    ) CreateError!ControlFile {
        const io = platform_io.default();
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const path = std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ store_dir, control_file_name }) catch
            return error.PathTooLong;

        std.Io.Dir.cwd().createDirPath(io, store_dir) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => |e| return e,
        };

        const file = try std.Io.Dir.cwd().createFile(io, path, .{ .read = true, .truncate = false });
        // The mapping stays valid after the descriptor is closed.
        defer file.close(io);
        const len = try file.length(io);
        if (len == 0) {
            try file.setLength(io, control_file_size);
        } else if (len != control_file_size) {
            return error.InvalidFileSize;
        }

        const mapped = try posix.mmap(
            null,
            control_file_size,
            .{ .READ = true, .WRITE = true },
            .{ .TYPE = .SHARED },
            file.handle,
            0,
        );
        errdefer posix.munmap(mapped);

        const hdr: *KvControlHeader = @ptrCast(mapped.ptr);
        const magic = hdr.magic.load(.acquire);
        if (magic == 0) {
            hdr.layout_version = control_layout_version;
            hdr.engine_kind = engine_kind;
            hdr.store_id = store_id;
            hdr.max_key_len = max_key_len;
            hdr.max_value_len = max_value_len;
            hdr.magic.store(control_magic, .release);
        } else {
            try validate(hdr);
            if (hdr.engine_kind != engine_kind or hdr.store_id != store_id or
                hdr.max_key_len != max_key_len or hdr.max_value_len != max_value_len)
                return error.LayoutMismatch;
        }
        return .{ .header = hdr, .mapping = mapped, .writable = true };
    }

    /// Reader side: map `path` (the full path of `control.shm`) with `PROT_READ`
    /// and validate magic and layout version.
    pub fn openReadOnly(path: []const u8) (CreateError)!ControlFile {
        const io = platform_io.default();
        const file = std.Io.Dir.cwd().openFile(io, path, .{ .mode = .read_only }) catch |err| switch (err) {
            error.FileNotFound => return error.FileNotFound,
            else => |e| return e,
        };
        defer file.close(io);
        const len = try file.length(io);
        if (len != control_file_size) return error.InvalidFileSize;

        const mapped = try posix.mmap(
            null,
            control_file_size,
            .{ .READ = true },
            .{ .TYPE = .SHARED },
            file.handle,
            0,
        );
        errdefer posix.munmap(mapped);
        const hdr: *KvControlHeader = @ptrCast(mapped.ptr);
        try validate(hdr);
        return .{ .header = hdr, .mapping = mapped, .writable = false };
    }

    /// Open read-only given the store directory.
    pub fn openReadOnlyInDir(store_dir: []const u8) CreateError!ControlFile {
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const path = std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ store_dir, control_file_name }) catch
            return error.PathTooLong;
        return openReadOnly(path);
    }

    fn validate(hdr: *const KvControlHeader) OpenError!void {
        if (hdr.magic.load(.acquire) != control_magic) return error.BadMagic;
        if (hdr.layout_version != control_layout_version) return error.LayoutMismatch;
    }

    pub fn close(self: *ControlFile) void {
        posix.munmap(self.mapping);
        self.* = undefined;
    }

    // ── Immutable line 0 ──

    pub fn storeId(self: *const ControlFile) u64 {
        return self.header.store_id;
    }
    pub fn engineKind(self: *const ControlFile) u32 {
        return self.header.engine_kind;
    }
    pub fn maxKeyLen(self: *const ControlFile) u32 {
        return self.header.max_key_len;
    }
    pub fn maxValueLen(self: *const ControlFile) u32 {
        return self.header.max_value_len;
    }

    // ── Generation / state (line 1) ──

    /// Publish a generation with release ordering (writer only).
    pub fn publishGeneration(self: *ControlFile, n: u64) void {
        std.debug.assert(self.writable);
        self.header.generation.store(n, .release);
    }

    /// Acquire load of the current generation; 0 = none ready yet.
    pub fn loadGeneration(self: *const ControlFile) u64 {
        return self.header.generation.load(.acquire);
    }

    pub fn setState(self: *ControlFile, state: KvStoreState) void {
        std.debug.assert(self.writable);
        self.header.state.store(@intFromEnum(state), .release);
    }

    pub fn getState(self: *const ControlFile) KvStoreState {
        return @enumFromInt(self.header.state.load(.acquire));
    }

    // ── Progress (line 2) ──

    /// Record applied progress after a commit (writer only). `applied_index`
    /// is stored last with release ordering.
    pub fn storeProgress(self: *ControlFile, applied_index: u64, leader_epoch: u64, at_ns: u64) void {
        std.debug.assert(self.writable);
        self.header.applied_leader_epoch.store(leader_epoch, .monotonic);
        self.header.applied_at_ns.store(at_ns, .monotonic);
        self.header.applied_index.store(applied_index, .release);
    }

    pub fn loadProgress(self: *const ControlFile) Progress {
        const idx = self.header.applied_index.load(.acquire);
        return .{
            .applied_index = idx,
            .applied_leader_epoch = self.header.applied_leader_epoch.load(.monotonic),
            .applied_at_ns = self.header.applied_at_ns.load(.monotonic),
        };
    }

    // ── Liveness (line 3) ──

    pub fn storeHeartbeat(self: *ControlFile, now_ns: u64, local_log_tip_index: u64, replication_live: bool) void {
        std.debug.assert(self.writable);
        self.header.local_log_tip_index.store(local_log_tip_index, .monotonic);
        self.header.replication_live.store(@intFromBool(replication_live), .monotonic);
        self.header.applier_heartbeat_ns.store(now_ns, .release);
    }

    pub fn loadHeartbeatNs(self: *const ControlFile) u64 {
        return self.header.applier_heartbeat_ns.load(.acquire);
    }

    pub fn loadLogTipIndex(self: *const ControlFile) u64 {
        return self.header.local_log_tip_index.load(.monotonic);
    }

    pub fn loadReplicationLive(self: *const ControlFile) bool {
        return self.header.replication_live.load(.monotonic) != 0;
    }
};

// ── Paths and generations ────────────────────────────────────────────

/// `<kv_path>/<group>/node-<id>/<store>`
pub fn storeDir(buf: []u8, kv_path: []const u8, group: []const u8, node_id: u32, store: []const u8) error{PathTooLong}![]const u8 {
    return std.fmt.bufPrint(buf, "{s}/{s}/node-{d}/{s}", .{ kv_path, group, node_id, store }) catch
        error.PathTooLong;
}

/// `gen-%08d`
pub fn genDirName(buf: []u8, n: u64) error{PathTooLong}![]const u8 {
    return std.fmt.bufPrint(buf, gen_dir_prefix ++ "{d:0>8}", .{n}) catch error.PathTooLong;
}

/// Parse a generation directory name; null if it is not exactly `gen-<digits>`.
pub fn parseGenDirName(name: []const u8) ?u64 {
    if (!std.mem.startsWith(u8, name, gen_dir_prefix)) return null;
    const digits = name[gen_dir_prefix.len..];
    if (digits.len < 8) return null;
    for (digits) |c| if (c < '0' or c > '9') return null;
    return std.fmt.parseInt(u64, digits, 10) catch null;
}

/// Join `<store_dir>/<gen-dir-name>`.
pub fn genDir(buf: []u8, store_dir: []const u8, n: u64) error{PathTooLong}![]const u8 {
    var name_buf: [max_gen_dir_name_len]u8 = undefined;
    const name = try genDirName(&name_buf, n);
    return std.fmt.bufPrint(buf, "{s}/{s}", .{ store_dir, name }) catch error.PathTooLong;
}

/// Delete generation directories under `store_dir` with number < `keep_from`
/// (callers pass the current generation to remove superseded ones, after the
/// grace period). Non-generation entries such as `control.shm` are untouched.
/// Returns the number of directories removed.
pub fn removeGenerationsBefore(store_dir: []const u8, keep_from: u64) !u32 {
    const io = platform_io.default();
    var dir = std.Io.Dir.cwd().openDir(io, store_dir, .{ .iterate = true }) catch |err| switch (err) {
        error.FileNotFound => return 0,
        else => return err,
    };
    defer dir.close(io);

    var removed: u32 = 0;
    var it = dir.iterate();
    while (try it.next(io)) |entry| {
        if (entry.kind != .directory) continue;
        const n = parseGenDirName(entry.name) orelse continue;
        if (n >= keep_from) continue;
        dir.deleteTree(io, entry.name) catch continue;
        removed += 1;
    }
    return removed;
}

// ── Tests ────────────────────────────────────────────────────────────

const testing = std.testing;

const test_root = "/tmp/ringloom-test-kv-control-";

fn cleanDir(path: []const u8) void {
    std.Io.Dir.cwd().deleteTree(platform_io.default(), path) catch {};
}

test "control header layout" {
    try testing.expectEqual(@as(usize, 0), @offsetOf(KvControlHeader, "magic"));
    try testing.expectEqual(@as(usize, 64), @offsetOf(KvControlHeader, "generation"));
    try testing.expectEqual(@as(usize, 128), @offsetOf(KvControlHeader, "applied_index"));
    try testing.expectEqual(@as(usize, 192), @offsetOf(KvControlHeader, "applier_heartbeat_ns"));
    try testing.expectEqual(@as(usize, 4096), @sizeOf(KvControlHeader));
}

test "path helpers" {
    var b1: [128]u8 = undefined;
    try testing.expectEqualStrings("/dev/shm/kv/g/node-3/prices", try storeDir(&b1, "/dev/shm/kv", "g", 3, "prices"));
    var small: [8]u8 = undefined;
    try testing.expectError(error.PathTooLong, storeDir(&small, "/dev/shm/kv", "g", 3, "prices"));

    var b2: [32]u8 = undefined;
    try testing.expectEqualStrings("gen-00000001", try genDirName(&b2, 1));
    try testing.expectEqualStrings("gen-12345678", try genDirName(&b2, 12345678));
    try testing.expectEqualStrings("gen-123456789", try genDirName(&b2, 123456789));

    try testing.expectEqual(@as(?u64, 1), parseGenDirName("gen-00000001"));
    try testing.expectEqual(@as(?u64, 123456789), parseGenDirName("gen-123456789"));
    try testing.expectEqual(@as(?u64, null), parseGenDirName("gen-1"));
    try testing.expectEqual(@as(?u64, null), parseGenDirName("gen-0000000x"));
    try testing.expectEqual(@as(?u64, null), parseGenDirName("control.shm"));
    try testing.expectEqual(@as(?u64, null), parseGenDirName("gen-"));

    var b3: [64]u8 = undefined;
    try testing.expectEqualStrings("/s/gen-00000002", try genDir(&b3, "/s", 2));
}

test "control create, reopen, readonly, generation publish" {
    const dir = test_root ++ "basic";
    cleanDir(dir);
    defer std.Io.Dir.cwd().deleteTree(platform_io.default(), dir) catch {};

    var cf = try ControlFile.createOrOpen(dir, 0xABCD, 1, 511, 65536);
    try testing.expectEqual(@as(u64, 0), cf.loadGeneration());
    try testing.expectEqual(KvStoreState.initializing, cf.getState());
    try testing.expectEqual(@as(u64, 0xABCD), cf.storeId());

    cf.publishGeneration(1);
    cf.setState(.live);
    cf.storeProgress(42, 3, 999);
    cf.storeHeartbeat(1234, 50, true);

    var ro = try ControlFile.openReadOnlyInDir(dir);
    try testing.expectEqual(@as(u64, 1), ro.loadGeneration());
    try testing.expectEqual(KvStoreState.live, ro.getState());
    const p = ro.loadProgress();
    try testing.expectEqual(@as(u64, 42), p.applied_index);
    try testing.expectEqual(@as(u64, 3), p.applied_leader_epoch);
    try testing.expectEqual(@as(u64, 999), p.applied_at_ns);
    try testing.expectEqual(@as(u64, 1234), ro.loadHeartbeatNs());
    try testing.expectEqual(@as(u64, 50), ro.loadLogTipIndex());
    try testing.expect(ro.loadReplicationLive());
    try testing.expectEqual(@as(u32, 1), ro.engineKind());
    try testing.expectEqual(@as(u32, 511), ro.maxKeyLen());
    try testing.expectEqual(@as(u32, 65536), ro.maxValueLen());

    cf.publishGeneration(2);
    try testing.expectEqual(@as(u64, 2), ro.loadGeneration());

    // Reopen by the broker preserves runtime state.
    cf.close();
    var cf2 = try ControlFile.createOrOpen(dir, 0xABCD, 1, 511, 65536);
    try testing.expectEqual(@as(u64, 2), cf2.loadGeneration());
    try testing.expectEqual(KvStoreState.live, cf2.getState());
    cf2.close();

    // Mismatching identity is rejected.
    try testing.expectError(error.LayoutMismatch, ControlFile.createOrOpen(dir, 0x1111, 1, 511, 65536));
    try testing.expectError(error.LayoutMismatch, ControlFile.createOrOpen(dir, 0xABCD, 2, 511, 65536));
    try testing.expectError(error.LayoutMismatch, ControlFile.createOrOpen(dir, 0xABCD, 1, 100, 65536));

    ro.close();
}

test "control readonly rejects bad magic, bad layout, missing and wrong size" {
    const dir = test_root ++ "bad";
    cleanDir(dir);
    defer std.Io.Dir.cwd().deleteTree(platform_io.default(), dir) catch {};

    var cf = try ControlFile.createOrOpen(dir, 1, 1, 511, 1024);
    var path_buf: [256]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buf, "{s}/{s}", .{ dir, control_file_name });

    // Layout mismatch.
    cf.header.layout_version = 99;
    try testing.expectError(error.LayoutMismatch, ControlFile.openReadOnly(path));
    try testing.expectError(error.LayoutMismatch, ControlFile.createOrOpen(dir, 1, 1, 511, 1024));
    cf.header.layout_version = control_layout_version;

    // Bad magic.
    cf.header.magic.store(0xDEAD, .release);
    try testing.expectError(error.BadMagic, ControlFile.openReadOnly(path));
    try testing.expectError(error.BadMagic, ControlFile.createOrOpen(dir, 1, 1, 511, 1024));
    cf.header.magic.store(control_magic, .release);
    var ok = try ControlFile.openReadOnly(path);
    ok.close();
    cf.close();

    try testing.expectError(error.FileNotFound, ControlFile.openReadOnly(dir ++ "/nope.shm"));

    // Wrong size.
    const f = try std.Io.Dir.cwd().createFile(platform_io.default(), dir ++ "/short.shm", .{});
    f.close(platform_io.default());
    try testing.expectError(error.InvalidFileSize, ControlFile.openReadOnly(dir ++ "/short.shm"));
}

test "removeGenerationsBefore" {
    const dir = test_root ++ "gens";
    cleanDir(dir);
    defer std.Io.Dir.cwd().deleteTree(platform_io.default(), dir) catch {};
    const io = platform_io.default();

    var cf = try ControlFile.createOrOpen(dir, 1, 1, 511, 1024);
    defer cf.close();
    try std.Io.Dir.cwd().createDirPath(io, dir ++ "/gen-00000001");
    try std.Io.Dir.cwd().createDirPath(io, dir ++ "/gen-00000002");
    try std.Io.Dir.cwd().createDirPath(io, dir ++ "/gen-00000003");
    try std.Io.Dir.cwd().createDirPath(io, dir ++ "/other");

    try testing.expectEqual(@as(u32, 2), try removeGenerationsBefore(dir, 3));
    try testing.expectError(error.FileNotFound, std.Io.Dir.cwd().access(io, dir ++ "/gen-00000001", .{}));
    try std.Io.Dir.cwd().access(io, dir ++ "/gen-00000003", .{});
    try std.Io.Dir.cwd().access(io, dir ++ "/other", .{});
    try std.Io.Dir.cwd().access(io, dir ++ "/" ++ control_file_name, .{});
    try testing.expectEqual(@as(u32, 0), try removeGenerationsBefore(dir ++ "/missing", 3));
}
