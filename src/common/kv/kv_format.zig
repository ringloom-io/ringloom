// SPDX-License-Identifier: Apache-2.0

//! Wire and storage formats for replicated KV stores (docs/kv-store-spec.md §4.2, §6, §7.2).
//!
//! Everything here is allocation-free: encoders write into caller-provided
//! buffers and decoders return views into the entry bytes. All multi-byte
//! fields are little-endian; entries may be unaligned, so decoding uses
//! unaligned reads only.

const std = @import("std");
const topic_id = @import("../topics/topic_id.zig");
const constants = @import("../platform/constants.zig");
const TopicPublishHeader = @import("../message/topic_data_header.zig").TopicPublishHeader;

comptime {
    // The extern structs are copied byte-for-byte in a few places.
    std.debug.assert(@import("builtin").cpu.arch.endian() == .little);
}

// ── Constants ────────────────────────────────────────────────────────

/// `KvFrameHeader.magic` ("RLKV").
pub const frame_magic: u32 = 0x564B_4C52;
/// Current frame format version.
pub const frame_version: u8 = 1;
/// Current store-config format version.
pub const config_format_version: u16 = 1;
/// Engine limit on key length (LMDB default MDB_MAXKEYSIZE).
pub const engine_max_key_len: u16 = 511;
/// Reserved topic-name prefix for KV store topics.
pub const kv_topic_prefix = "__kv.";
/// Maximum store name length in bytes.
pub const max_store_name_len: usize = 64;
/// Maximum length of a backing topic name (`__kv.` + store name).
pub const max_topic_name_len: usize = kv_topic_prefix.len + max_store_name_len;
/// Engine kind recorded in the control header and engine meta for LMDB.
pub const engine_kind_lmdb: u32 = 1;

/// Largest default value fitting a single maximum-key record in the default
/// broker payload budget. TopicPublishHeader is inside that budget; the outer
/// RingLoomDataHeader is not. Round the available record bytes down for pad8.
pub const default_max_value_len: u32 =
    ((constants.default_max_frame_length - TopicPublishHeader.encoded_length - frame_header_size) & ~@as(u32, 7)) -
    record_header_size - engine_max_key_len;

// ── Store configuration (§4.2) ───────────────────────────────────────

const default_roll_scheme: [16]u8 = .{ 'F', 'A', 'S', 'T', '_', 'D', 'A', 'I', 'L', 'Y', 0, 0, 0, 0, 0, 0 };

/// Replicated, immutable store configuration; body of the `store_header` frame.
pub const KvStoreConfig = extern struct {
    size: u32 = @sizeOf(KvStoreConfig),
    format_version: u16 = config_format_version,
    _pad0: u16 = 0,
    max_key_len: u16 = engine_max_key_len,
    _pad1: u16 = 0,
    max_value_len: u32 = default_max_value_len,
    default_ttl_ms: u64 = 0,
    map_size_hint: u64 = 1 << 30,
    topic_roll_scheme: [16]u8 = default_roll_scheme,
    flags: u32 = 0,
    _reserved: [28]u8 = @splat(0),

    comptime {
        std.debug.assert(@sizeOf(KvStoreConfig) == 80);
    }

    pub const ValidateError = error{
        UnsupportedVersion,
        InvalidSize,
        InvalidKeyLen,
        InvalidValueLen,
    };

    /// Equality ignoring `map_size_hint` (a local capacity hint, §4.2).
    pub fn eqlIgnoringMapSize(self: KvStoreConfig, other: KvStoreConfig) bool {
        var a = self;
        a.map_size_hint = other.map_size_hint;
        return std.mem.eql(u8, std.mem.asBytes(&a), std.mem.asBytes(&other));
    }

    /// Validate against the KV payload capacity (broker `max_frame_length`
    /// minus TopicPublishHeader.encoded_length): the largest key and
    /// value must fit in one data frame carrying a single record.
    pub fn validate(self: KvStoreConfig, max_frame_length: u32) ValidateError!void {
        if (self.format_version != config_format_version) return error.UnsupportedVersion;
        if (self.size < @sizeOf(KvStoreConfig)) return error.InvalidSize;
        if (self.max_key_len == 0 or self.max_key_len > engine_max_key_len) return error.InvalidKeyLen;
        const record: u64 = @as(u64, record_header_size) + self.max_key_len + self.max_value_len;
        const frame: u64 = @as(u64, frame_header_size) + pad8(record);
        if (self.max_value_len == 0 or frame > max_frame_length) return error.InvalidValueLen;
        // The store_header frame itself must fit too.
        if (frame_header_size + @sizeOf(KvStoreConfig) > max_frame_length) return error.InvalidValueLen;
    }
};

// ── Frame structs (§6.1) ─────────────────────────────────────────────

pub const KvFrameKind = enum(u8) {
    store_header = 1,
    data = 2,
    writer_grant = 3,
    writer_release = 4,
};

pub const KvFrameHeader = extern struct {
    magic: u32 = frame_magic,
    version: u8 = frame_version,
    kind: KvFrameKind,
    flags: u16 = 0,
    record_count: u32,
    body_len: u32,
    leader_epoch: u64,
    writer_epoch: u64,

    comptime {
        std.debug.assert(@sizeOf(KvFrameHeader) == 32);
    }
};

pub const KvOp = enum(u8) { put = 1, delete = 2, clear = 3 };

pub const KvRecordHeader = extern struct {
    op: KvOp,
    flags: u8 = 0,
    key_len: u16,
    value_len: u32,
    expire_at_ns: u64,

    comptime {
        std.debug.assert(@sizeOf(KvRecordHeader) == 16);
    }
};

pub const KvWriterGrant = extern struct {
    writer_epoch: u64,
    holder_session_nonce: u64,
    holder_service_id: u32,
    holder_node_id: u16,
    _pad: u16 = 0,
    granted_at_ns: u64,

    comptime {
        std.debug.assert(@sizeOf(KvWriterGrant) == 32);
    }
};

pub const KvWriterRelease = extern struct {
    writer_epoch: u64,
    reason: u32,
    _pad: u32 = 0,

    comptime {
        std.debug.assert(@sizeOf(KvWriterRelease) == 16);
    }
};

/// Prefix of every value stored in the engine (§6.2).
pub const KvValueHeader = extern struct {
    write_index: u64,
    expire_at_ns: u64,

    comptime {
        std.debug.assert(@sizeOf(KvValueHeader) == 16);
    }

    /// Unaligned decode from the front of an engine value.
    pub fn read(bytes: []const u8) KvValueHeader {
        return .{
            .write_index = std.mem.readInt(u64, bytes[0..8], .little),
            .expire_at_ns = std.mem.readInt(u64, bytes[8..16], .little),
        };
    }

    /// Encode into the first 16 bytes of `out`.
    pub fn write(self: KvValueHeader, out: []u8) void {
        std.mem.writeInt(u64, out[0..8], self.write_index, .little);
        std.mem.writeInt(u64, out[8..16], self.expire_at_ns, .little);
    }
};

/// Applied-state metadata persisted by engines (§7.2 `meta` keys).
pub const KvEngineMeta = extern struct {
    format_version: u16 = config_format_version,
    engine_kind: u32,
    store_id: u64,
    applied_index: u64,
    applied_leader_epoch: u64,
    writer_epoch: u64,
};

pub const frame_header_size: usize = @sizeOf(KvFrameHeader);
pub const record_header_size: usize = @sizeOf(KvRecordHeader);

/// Round up to a multiple of 8.
pub inline fn pad8(n: u64) u64 {
    return (n + 7) & ~@as(u64, 7);
}

/// Encoded size of one record (header + key + value + pad8).
pub inline fn recordSize(key_len: usize, value_len: usize) usize {
    return @intCast(pad8(record_header_size + key_len + value_len));
}

// ── Store names (§4.1) ───────────────────────────────────────────────

/// 1–64 bytes of `[A-Za-z0-9._-]`; `.` and `..` are rejected because the name
/// is used as a directory name.
pub fn isValidStoreName(name: []const u8) bool {
    if (name.len == 0 or name.len > max_store_name_len) return false;
    if (std.mem.eql(u8, name, ".") or std.mem.eql(u8, name, "..")) return false;
    for (name) |c| {
        const ok = (c >= 'a' and c <= 'z') or (c >= 'A' and c <= 'Z') or
            (c >= '0' and c <= '9') or c == '.' or c == '_' or c == '-';
        if (!ok) return false;
    }
    return true;
}

/// True if `topic_name` carries the reserved KV prefix.
pub fn hasKvTopicPrefix(topic_name: []const u8) bool {
    return std.mem.startsWith(u8, topic_name, kv_topic_prefix);
}

/// Write `__kv.<store>` into `buf` and return the slice.
pub fn topicNameFor(store: []const u8, buf: []u8) error{ InvalidName, BufferTooSmall }![]const u8 {
    if (!isValidStoreName(store)) return error.InvalidName;
    const n = kv_topic_prefix.len + store.len;
    if (buf.len < n) return error.BufferTooSmall;
    @memcpy(buf[0..kv_topic_prefix.len], kv_topic_prefix);
    @memcpy(buf[kv_topic_prefix.len..n], store);
    return buf[0..n];
}

/// `store_id == topic_id == topicIdOf("__kv.<store>")`.
pub fn storeIdOf(store: []const u8) error{InvalidName}!u64 {
    var buf: [max_topic_name_len]u8 = undefined;
    const name = topicNameFor(store, &buf) catch return error.InvalidName;
    return topic_id.topicIdOf(name);
}

// ── TTL (§11.1) ──────────────────────────────────────────────────────

/// A value is absent when `expire_at_ns != 0 and now_ns >= expire_at_ns`.
pub inline fn isExpired(expire_at_ns: u64, now_ns: u64) bool {
    return expire_at_ns != 0 and now_ns >= expire_at_ns;
}

/// `now + ttl_ms` in ns, saturating; 0 ttl means never expires (returns 0).
pub fn expireAtFor(now_ns: u64, ttl_ms: u64) u64 {
    if (ttl_ms == 0) return 0;
    const add = std.math.mul(u64, ttl_ms, std.time.ns_per_ms) catch return std.math.maxInt(u64);
    return std.math.add(u64, now_ns, add) catch std.math.maxInt(u64);
}

// ── Fencing state machine (§9.1) ─────────────────────────────────────

/// Pure fencing state shared by the applier and the grant tracker.
pub const FenceState = struct {
    /// Epoch of the latest grant not yet released; 0 = no writer.
    current_writer_epoch: u64 = 0,
    /// Highest grant epoch ever seen (next grant uses this + 1).
    last_grant_epoch: u64 = 0,

    pub fn onGrant(self: *FenceState, writer_epoch: u64) void {
        self.current_writer_epoch = writer_epoch;
        if (writer_epoch > self.last_grant_epoch) self.last_grant_epoch = writer_epoch;
    }

    /// Clears the current writer only if `writer_epoch` is the current one.
    pub fn onRelease(self: *FenceState, writer_epoch: u64) void {
        if (writer_epoch == self.current_writer_epoch) self.current_writer_epoch = 0;
    }

    /// Data frames take effect only if epoch is non-zero and current.
    pub fn acceptsData(self: FenceState, writer_epoch: u64) bool {
        return writer_epoch != 0 and writer_epoch == self.current_writer_epoch;
    }
};

// ── Encoders ─────────────────────────────────────────────────────────

pub const EncodeError = error{BufferTooSmall};

fn writeFrameHeader(buf: []u8, h: KvFrameHeader) void {
    std.mem.writeInt(u32, buf[0..4], h.magic, .little);
    buf[4] = h.version;
    buf[5] = @backingInt(h.kind);
    std.mem.writeInt(u16, buf[6..8], h.flags, .little);
    std.mem.writeInt(u32, buf[8..12], h.record_count, .little);
    std.mem.writeInt(u32, buf[12..16], h.body_len, .little);
    std.mem.writeInt(u64, buf[16..24], h.leader_epoch, .little);
    std.mem.writeInt(u64, buf[24..32], h.writer_epoch, .little);
}

fn encodeSimple(buf: []u8, kind: KvFrameKind, leader_epoch: u64, body: []const u8) EncodeError![]const u8 {
    const total = frame_header_size + body.len;
    if (buf.len < total) return error.BufferTooSmall;
    writeFrameHeader(buf, .{
        .kind = kind,
        .record_count = 0,
        .body_len = @intCast(body.len),
        .leader_epoch = leader_epoch,
        .writer_epoch = 0,
    });
    @memcpy(buf[frame_header_size..total], body);
    return buf[0..total];
}

/// Encode a leader-authored `store_header` frame.
pub fn encodeStoreHeader(buf: []u8, leader_epoch: u64, config: KvStoreConfig) EncodeError![]const u8 {
    return encodeSimple(buf, .store_header, leader_epoch, std.mem.asBytes(&config));
}

/// Encode a leader-authored `writer_grant` frame.
pub fn encodeWriterGrant(buf: []u8, leader_epoch: u64, grant: KvWriterGrant) EncodeError![]const u8 {
    return encodeSimple(buf, .writer_grant, leader_epoch, std.mem.asBytes(&grant));
}

/// Encode a leader-authored `writer_release` frame.
pub fn encodeWriterRelease(buf: []u8, leader_epoch: u64, release: KvWriterRelease) EncodeError![]const u8 {
    return encodeSimple(buf, .writer_release, leader_epoch, std.mem.asBytes(&release));
}

/// Builds one DATA frame in a caller-provided buffer (no allocation).
pub const DataFrameBuilder = struct {
    buf: []u8,
    len: usize,
    count: u32,
    writer_epoch: u64,
    leader_epoch: u64,
    max_key_len: u16 = engine_max_key_len,
    max_value_len: u32 = default_max_value_len,

    pub const Error = error{ BatchFull, KeyTooLarge, ValueTooLarge, EmptyKey };

    pub fn init(buf: []u8, writer_epoch: u64, leader_epoch: u64) DataFrameBuilder {
        std.debug.assert(buf.len >= frame_header_size);
        return .{
            .buf = buf,
            .len = frame_header_size,
            .count = 0,
            .writer_epoch = writer_epoch,
            .leader_epoch = leader_epoch,
        };
    }

    /// Set the per-store key/value limits (from the effective `KvStoreConfig`).
    pub fn withLimits(self: DataFrameBuilder, max_key_len: u16, max_value_len: u32) DataFrameBuilder {
        var b = self;
        b.max_key_len = max_key_len;
        b.max_value_len = max_value_len;
        return b;
    }

    /// Discard all records; optionally change epochs via the fields directly.
    pub fn reset(self: *DataFrameBuilder) void {
        self.len = frame_header_size;
        self.count = 0;
    }

    pub fn isEmpty(self: *const DataFrameBuilder) bool {
        return self.count == 0;
    }

    pub fn recordCount(self: *const DataFrameBuilder) u32 {
        return self.count;
    }

    /// Bytes still available for records.
    pub fn remaining(self: *const DataFrameBuilder) usize {
        return self.buf.len - self.len;
    }

    fn append(self: *DataFrameBuilder, op: KvOp, key: []const u8, value: []const u8, expire_at_ns: u64) Error!void {
        if (key.len > self.max_key_len) return error.KeyTooLarge;
        if (value.len > self.max_value_len) return error.ValueTooLarge;
        if (op != .clear and key.len == 0) return error.EmptyKey;
        const rec = recordSize(key.len, value.len);
        if (rec > self.buf.len - self.len) return error.BatchFull;
        const o = self.len;
        const b = self.buf;
        b[o] = @backingInt(op);
        b[o + 1] = 0;
        std.mem.writeInt(u16, b[o + 2 ..][0..2], @intCast(key.len), .little);
        std.mem.writeInt(u32, b[o + 4 ..][0..4], @intCast(value.len), .little);
        std.mem.writeInt(u64, b[o + 8 ..][0..8], expire_at_ns, .little);
        const k = o + record_header_size;
        @memcpy(b[k .. k + key.len], key);
        @memcpy(b[k + key.len ..][0..value.len], value);
        @memset(b[k + key.len + value.len .. o + rec], 0);
        self.len += rec;
        self.count += 1;
    }

    pub fn addPut(self: *DataFrameBuilder, key: []const u8, value: []const u8, expire_at_ns: u64) Error!void {
        return self.append(.put, key, value, expire_at_ns);
    }

    pub fn addDelete(self: *DataFrameBuilder, key: []const u8) Error!void {
        return self.append(.delete, key, &.{}, 0);
    }

    pub fn addClear(self: *DataFrameBuilder) Error!void {
        return self.append(.clear, &.{}, &.{}, 0);
    }

    /// Fill in the header and return the encoded frame (a slice of `buf`).
    /// The builder stays usable; call `reset` to start a new frame.
    pub fn finish(self: *DataFrameBuilder) []const u8 {
        writeFrameHeader(self.buf, .{
            .kind = .data,
            .record_count = self.count,
            .body_len = @intCast(self.len - frame_header_size),
            .leader_epoch = self.leader_epoch,
            .writer_epoch = self.writer_epoch,
        });
        return self.buf[0..self.len];
    }
};

// ── Decoding / validation (§6.1) ─────────────────────────────────────

/// Cheap header decode for the grant tracker: checks length, magic, version,
/// known kind and that `body_len` fits the entry. Does not look at the body.
pub fn peekHeader(entry: []const u8) ?KvFrameHeader {
    if (entry.len < frame_header_size) return null;
    if (std.mem.readInt(u32, entry[0..4], .little) != frame_magic) return null;
    if (entry[4] != frame_version) return null;
    const kind = std.enums.fromInt(KvFrameKind, entry[5]) orelse return null;
    const body_len = std.mem.readInt(u32, entry[12..16], .little);
    if (body_len > entry.len - frame_header_size) return null;
    return .{
        .magic = frame_magic,
        .version = frame_version,
        .kind = kind,
        .flags = std.mem.readInt(u16, entry[6..8], .little),
        .record_count = std.mem.readInt(u32, entry[8..12], .little),
        .body_len = body_len,
        .leader_epoch = std.mem.readInt(u64, entry[16..24], .little),
        .writer_epoch = std.mem.readInt(u64, entry[24..32], .little),
    };
}

/// One decoded record; `key`/`value` are views into the entry.
pub const Record = struct {
    op: KvOp,
    key: []const u8,
    value: []const u8,
    expire_at_ns: u64,
};

/// Iterates the records of a validated data frame.
pub const RecordIterator = struct {
    body: []const u8,
    offset: usize = 0,
    remaining: u32,

    pub fn next(self: *RecordIterator) ?Record {
        if (self.remaining == 0) return null;
        const b = self.body[self.offset..];
        const key_len: usize = std.mem.readInt(u16, b[2..4], .little);
        const value_len: usize = std.mem.readInt(u32, b[4..8], .little);
        const k = record_header_size;
        const rec = Record{
            .op = std.enums.fromInt(KvOp, b[0]).?,
            .key = b[k .. k + key_len],
            .value = b[k + key_len .. k + key_len + value_len],
            .expire_at_ns = std.mem.readInt(u64, b[8..16], .little),
        };
        self.offset += recordSize(key_len, value_len);
        self.remaining -= 1;
        return rec;
    }
};

pub const FramePayload = union(KvFrameKind) {
    store_header: KvStoreConfig,
    data: RecordIterator,
    writer_grant: KvWriterGrant,
    writer_release: KvWriterRelease,
};

/// A validated frame.
pub const FrameView = struct {
    header: KvFrameHeader,
    payload: FramePayload,
};

fn readStruct(comptime T: type, bytes: []const u8) T {
    var v: T = undefined;
    @memcpy(std.mem.asBytes(&v), bytes[0..@sizeOf(T)]);
    return v;
}

/// Validate and decode one topic entry. Any violation yields `error.Malformed`;
/// since every replica sees identical bytes, every replica skips the same frames.
///
/// Rules: magic, version, known kind, `body_len` <= entry bytes after the
/// header (trailing bytes are ignored), exact body sizes for grant/release,
/// at least `@sizeOf(KvStoreConfig)` for store_header, and for data frames
/// `record_count` records that exactly consume the body, with known ops,
/// `key_len <= max_key_len`, `value_len <= max_value_len`, non-empty keys for
/// put/delete, and empty key/value for clear and empty value for delete.
pub fn decodeFrame(entry: []const u8, max_key_len: u32, max_value_len: u32) error{Malformed}!FrameView {
    const h = peekHeader(entry) orelse return error.Malformed;
    const body = entry[frame_header_size..][0..h.body_len];
    switch (h.kind) {
        .store_header => {
            if (body.len < @sizeOf(KvStoreConfig)) return error.Malformed;
            return .{ .header = h, .payload = .{ .store_header = readStruct(KvStoreConfig, body) } };
        },
        .writer_grant => {
            if (body.len != @sizeOf(KvWriterGrant)) return error.Malformed;
            return .{ .header = h, .payload = .{ .writer_grant = readStruct(KvWriterGrant, body) } };
        },
        .writer_release => {
            if (body.len != @sizeOf(KvWriterRelease)) return error.Malformed;
            return .{ .header = h, .payload = .{ .writer_release = readStruct(KvWriterRelease, body) } };
        },
        .data => {
            var off: usize = 0;
            var i: u32 = 0;
            while (i < h.record_count) : (i += 1) {
                if (body.len - off < record_header_size) return error.Malformed;
                const b = body[off..];
                const op = std.enums.fromInt(KvOp, b[0]) orelse return error.Malformed;
                const key_len: usize = std.mem.readInt(u16, b[2..4], .little);
                const value_len: usize = std.mem.readInt(u32, b[4..8], .little);
                if (key_len > max_key_len or value_len > max_value_len) return error.Malformed;
                switch (op) {
                    .put => if (key_len == 0) return error.Malformed,
                    .delete => if (key_len == 0 or value_len != 0) return error.Malformed,
                    .clear => if (key_len != 0 or value_len != 0) return error.Malformed,
                }
                const size = recordSize(key_len, value_len);
                if (size > b.len) return error.Malformed;
                off += size;
            }
            if (off != body.len) return error.Malformed;
            return .{ .header = h, .payload = .{ .data = .{ .body = body, .remaining = h.record_count } } };
        },
    }
}

// ── Tests ────────────────────────────────────────────────────────────

const testing = std.testing;

test "struct sizes and offsets" {
    try testing.expectEqual(@as(usize, 80), @sizeOf(KvStoreConfig));
    try testing.expectEqual(@as(usize, 32), @sizeOf(KvFrameHeader));
    try testing.expectEqual(@as(usize, 16), @sizeOf(KvRecordHeader));
    try testing.expectEqual(@as(usize, 32), @sizeOf(KvWriterGrant));
    try testing.expectEqual(@as(usize, 16), @sizeOf(KvWriterRelease));
    try testing.expectEqual(@as(usize, 16), @sizeOf(KvValueHeader));
    try testing.expectEqual(@as(usize, 40), @sizeOf(KvEngineMeta));
    try testing.expectEqual(@as(usize, 4), @offsetOf(KvFrameHeader, "version"));
    try testing.expectEqual(@as(usize, 5), @offsetOf(KvFrameHeader, "kind"));
    try testing.expectEqual(@as(usize, 8), @offsetOf(KvFrameHeader, "record_count"));
    try testing.expectEqual(@as(usize, 12), @offsetOf(KvFrameHeader, "body_len"));
    try testing.expectEqual(@as(usize, 16), @offsetOf(KvFrameHeader, "leader_epoch"));
    try testing.expectEqual(@as(usize, 24), @offsetOf(KvFrameHeader, "writer_epoch"));
    try testing.expectEqual(@as(usize, 4), @offsetOf(KvRecordHeader, "value_len"));
    try testing.expectEqual(@as(usize, 8), @offsetOf(KvRecordHeader, "expire_at_ns"));
}

test "store names" {
    try testing.expect(isValidStoreName("a"));
    try testing.expect(isValidStoreName("My.store_1-x"));
    const x64: [64]u8 = @splat('x');
    const x65: [65]u8 = @splat('x');
    try testing.expect(isValidStoreName(&x64));
    try testing.expect(!isValidStoreName(""));
    try testing.expect(!isValidStoreName(&x65));
    try testing.expect(!isValidStoreName("a/b"));
    try testing.expect(!isValidStoreName("a b"));
    try testing.expect(!isValidStoreName("é"));
    try testing.expect(!isValidStoreName("."));
    try testing.expect(!isValidStoreName(".."));
    try testing.expect(isValidStoreName("..a"));
}

test "topic name and store id" {
    var buf: [max_topic_name_len]u8 = undefined;
    try testing.expectEqualStrings("__kv.prices", try topicNameFor("prices", &buf));
    try testing.expectError(error.InvalidName, topicNameFor("bad/name", &buf));
    var small: [4]u8 = undefined;
    try testing.expectError(error.BufferTooSmall, topicNameFor("prices", &small));
    try testing.expectEqual(topic_id.topicIdOf("__kv.prices"), try storeIdOf("prices"));
    try testing.expectError(error.InvalidName, storeIdOf(""));
    try testing.expect(hasKvTopicPrefix("__kv.x"));
    try testing.expect(!hasKvTopicPrefix("orders"));
}

test "config eql and validate" {
    const a = KvStoreConfig{};
    var b = a;
    b.map_size_hint = 12345;
    try testing.expect(a.eqlIgnoringMapSize(b));
    b.max_value_len += 1;
    try testing.expect(!a.eqlIgnoringMapSize(b));
    try testing.expectEqualStrings("FAST_DAILY", std.mem.sliceTo(&a.topic_roll_scheme, 0));

    try a.validate(1 << 20);
    const capacity = constants.default_max_frame_length - TopicPublishHeader.encoded_length;
    try a.validate(capacity);
    try testing.expectEqual(@as(u32, 64929), a.max_value_len);
    try testing.expectEqual(capacity, frame_header_size + recordSize(a.max_key_len, a.max_value_len));
    try testing.expectError(error.InvalidValueLen, a.validate(capacity - 1));
    var larger = a;
    larger.max_value_len += 1;
    try testing.expectError(error.InvalidValueLen, larger.validate(capacity));
    larger.max_value_len = 64 * 1024;
    try testing.expectError(error.InvalidValueLen, larger.validate(capacity));
    // Explicit 64 KiB values remain supported when the payload budget permits.
    try larger.validate(@intCast(frame_header_size + recordSize(511, 64 * 1024)));
    var c = a;
    c.max_key_len = 512;
    try testing.expectError(error.InvalidKeyLen, c.validate(1 << 20));
    c.max_key_len = 0;
    try testing.expectError(error.InvalidKeyLen, c.validate(1 << 20));
    c = a;
    c.format_version = 2;
    try testing.expectError(error.UnsupportedVersion, c.validate(1 << 20));
}

test "value header round trip unaligned" {
    var raw: [17]u8 = undefined;
    const h = KvValueHeader{ .write_index = 0x0102030405060708, .expire_at_ns = 99 };
    h.write(raw[1..]);
    const back = KvValueHeader.read(raw[1..]);
    try testing.expectEqual(h, back);
}

test "data frame round trip with unaligned entry" {
    var buf: [512]u8 = undefined;
    var b = DataFrameBuilder.init(&buf, 7, 3);
    try b.addPut("k1", "value-one", 1000);
    try b.addDelete("k2");
    try b.addClear();
    try b.addPut("abc", "", 0);
    const frame = b.finish();
    try testing.expectEqual(@as(usize, 0), frame.len % 8);

    // Copy to an odd offset to prove unaligned decoding.
    var shifted: [513]u8 = undefined;
    @memcpy(shifted[1 .. 1 + frame.len], frame);
    const view = try decodeFrame(shifted[1 .. 1 + frame.len], 511, 65536);
    try testing.expectEqual(KvFrameKind.data, view.header.kind);
    try testing.expectEqual(@as(u32, 4), view.header.record_count);
    try testing.expectEqual(@as(u64, 7), view.header.writer_epoch);
    try testing.expectEqual(@as(u64, 3), view.header.leader_epoch);

    var it = view.payload.data;
    const r1 = it.next().?;
    try testing.expectEqual(KvOp.put, r1.op);
    try testing.expectEqualStrings("k1", r1.key);
    try testing.expectEqualStrings("value-one", r1.value);
    try testing.expectEqual(@as(u64, 1000), r1.expire_at_ns);
    const r2 = it.next().?;
    try testing.expectEqual(KvOp.delete, r2.op);
    try testing.expectEqualStrings("k2", r2.key);
    const r3 = it.next().?;
    try testing.expectEqual(KvOp.clear, r3.op);
    try testing.expectEqual(@as(usize, 0), r3.key.len);
    const r4 = it.next().?;
    try testing.expectEqualStrings("abc", r4.key);
    try testing.expectEqual(@as(usize, 0), r4.value.len);
    try testing.expect(it.next() == null);

    const peek = peekHeader(frame).?;
    try testing.expectEqual(view.header, peek);

    // reset reuses the buffer.
    b.reset();
    try testing.expect(b.isEmpty());
    const empty = b.finish();
    const ev = try decodeFrame(empty, 511, 65536);
    try testing.expectEqual(@as(u32, 0), ev.header.record_count);
}

test "builder limits" {
    var buf: [128]u8 = undefined;
    var b = DataFrameBuilder.init(&buf, 1, 1).withLimits(4, 8);
    try testing.expectError(error.KeyTooLarge, b.addPut("12345", "v", 0));
    try testing.expectError(error.ValueTooLarge, b.addPut("k", "123456789", 0));
    try testing.expectError(error.EmptyKey, b.addPut("", "v", 0));
    try testing.expectError(error.EmptyKey, b.addDelete(""));
    try testing.expect(b.isEmpty());
    // 128 - 32 = 96 bytes; each record is 16 + 1 + 8 -> 32. Three fit.
    try b.addPut("a", "12345678", 0);
    try b.addPut("b", "12345678", 0);
    try b.addPut("c", "12345678", 0);
    try testing.expectError(error.BatchFull, b.addPut("d", "12345678", 0));
    try testing.expectEqual(@as(u32, 3), b.recordCount());
    _ = try decodeFrame(b.finish(), 4, 8);
}

test "control frames round trip" {
    var buf: [128]u8 = undefined;

    const cfg = KvStoreConfig{ .max_value_len = 1234 };
    const f1 = try encodeStoreHeader(&buf, 5, cfg);
    try testing.expectEqual(@as(usize, 112), f1.len);
    const v1 = try decodeFrame(f1, 511, 65536);
    try testing.expectEqual(@as(u64, 5), v1.header.leader_epoch);
    try testing.expectEqual(@as(u64, 0), v1.header.writer_epoch);
    try testing.expect(v1.payload.store_header.eqlIgnoringMapSize(cfg));

    const g = KvWriterGrant{ .writer_epoch = 9, .holder_session_nonce = 0xAB, .holder_service_id = 4, .holder_node_id = 2, .granted_at_ns = 77 };
    const f2 = try encodeWriterGrant(&buf, 5, g);
    const v2 = try decodeFrame(f2, 511, 65536);
    try testing.expectEqual(g, v2.payload.writer_grant);

    const r = KvWriterRelease{ .writer_epoch = 9, .reason = 2 };
    const f3 = try encodeWriterRelease(&buf, 5, r);
    const v3 = try decodeFrame(f3, 511, 65536);
    try testing.expectEqual(r, v3.payload.writer_release);

    var tiny: [40]u8 = undefined;
    try testing.expectError(error.BufferTooSmall, encodeWriterGrant(&tiny, 1, g));
}

fn goodDataFrame(buf: []u8) []const u8 {
    var b = DataFrameBuilder.init(buf, 1, 1);
    b.addPut("key", "value", 5) catch unreachable;
    b.addDelete("key") catch unreachable;
    return b.finish();
}

test "decodeFrame rejects malformed frames" {
    var buf: [256]u8 = undefined;
    const good = goodDataFrame(&buf);
    _ = try decodeFrame(good, 511, 65536);

    var m: [256]u8 = undefined;
    const mal = error.Malformed;

    // Too short / empty.
    try testing.expectError(mal, decodeFrame(&.{}, 511, 65536));
    try testing.expectError(mal, decodeFrame(good[0..31], 511, 65536));

    // Bad magic.
    @memcpy(m[0..good.len], good);
    m[0] ^= 0xFF;
    try testing.expectError(mal, decodeFrame(m[0..good.len], 511, 65536));

    // Bad version.
    @memcpy(m[0..good.len], good);
    m[4] = 2;
    try testing.expectError(mal, decodeFrame(m[0..good.len], 511, 65536));

    // Unknown kind (0 and 9).
    @memcpy(m[0..good.len], good);
    m[5] = 0;
    try testing.expectError(mal, decodeFrame(m[0..good.len], 511, 65536));
    m[5] = 9;
    try testing.expectError(mal, decodeFrame(m[0..good.len], 511, 65536));

    // body_len larger than entry.
    @memcpy(m[0..good.len], good);
    std.mem.writeInt(u32, m[12..16], @intCast(good.len - 32 + 1), .little);
    try testing.expectError(mal, decodeFrame(m[0..good.len], 511, 65536));

    // body_len smaller than the records need (truncated body).
    @memcpy(m[0..good.len], good);
    std.mem.writeInt(u32, m[12..16], @intCast(good.len - 32 - 8), .little);
    try testing.expectError(mal, decodeFrame(m[0..good.len], 511, 65536));

    // Truncated entry (body_len now exceeds the entry).
    try testing.expectError(mal, decodeFrame(good[0 .. good.len - 8], 511, 65536));

    // record_count too large for body.
    @memcpy(m[0..good.len], good);
    std.mem.writeInt(u32, m[8..12], 3, .little);
    try testing.expectError(mal, decodeFrame(m[0..good.len], 511, 65536));

    // record_count too small (leftover body bytes).
    @memcpy(m[0..good.len], good);
    std.mem.writeInt(u32, m[8..12], 1, .little);
    try testing.expectError(mal, decodeFrame(m[0..good.len], 511, 65536));

    // record_count absurdly large must not loop forever.
    @memcpy(m[0..good.len], good);
    std.mem.writeInt(u32, m[8..12], std.math.maxInt(u32), .little);
    try testing.expectError(mal, decodeFrame(m[0..good.len], 511, 65536));

    // Unknown op.
    @memcpy(m[0..good.len], good);
    m[32] = 7;
    try testing.expectError(mal, decodeFrame(m[0..good.len], 511, 65536));

    // key_len above limit (frame is fine otherwise).
    try testing.expectError(mal, decodeFrame(good, 2, 65536));
    // value_len above limit.
    try testing.expectError(mal, decodeFrame(good, 511, 4));

    // key_len pointing past the body.
    @memcpy(m[0..good.len], good);
    std.mem.writeInt(u16, m[34..36], 200, .little);
    try testing.expectError(mal, decodeFrame(m[0..good.len], 511, 65536));

    // value_len huge (within limits passed but beyond body).
    @memcpy(m[0..good.len], good);
    std.mem.writeInt(u32, m[36..40], 60000, .little);
    try testing.expectError(mal, decodeFrame(m[0..good.len], 511, 65536));

    // Put with empty key.
    @memcpy(m[0..good.len], good);
    std.mem.writeInt(u16, m[34..36], 0, .little);
    try testing.expectError(mal, decodeFrame(m[0..good.len], 511, 65536));

    // Delete carrying a value.
    var b = DataFrameBuilder.init(&buf, 1, 1);
    try b.addDelete("k");
    var del = b.finish();
    @memcpy(m[0..del.len], del);
    std.mem.writeInt(u32, m[36..40], 8, .little);
    try testing.expectError(mal, decodeFrame(m[0..del.len], 511, 65536));

    // Clear carrying a key.
    b.reset();
    try b.addClear();
    del = b.finish();
    @memcpy(m[0..del.len], del);
    std.mem.writeInt(u16, m[34..36], 1, .little);
    try testing.expectError(mal, decodeFrame(m[0..del.len], 511, 65536));
}

test "decodeFrame rejects bad control frame bodies" {
    var buf: [160]u8 = undefined;
    const mal = error.Malformed;

    const g = try encodeWriterGrant(&buf, 1, .{ .writer_epoch = 1, .holder_session_nonce = 1, .holder_service_id = 1, .holder_node_id = 1, .granted_at_ns = 1 });
    var m: [160]u8 = undefined;
    @memcpy(m[0..g.len], g);
    std.mem.writeInt(u32, m[12..16], 31, .little);
    try testing.expectError(mal, decodeFrame(m[0..g.len], 511, 65536));
    std.mem.writeInt(u32, m[12..16], 33, .little);
    try testing.expectError(mal, decodeFrame(m[0 .. g.len + 1], 511, 65536));
    m[g.len] = 0;

    const r = try encodeWriterRelease(&buf, 1, .{ .writer_epoch = 1, .reason = 0 });
    @memcpy(m[0..r.len], r);
    std.mem.writeInt(u32, m[12..16], 8, .little);
    try testing.expectError(mal, decodeFrame(m[0..r.len], 511, 65536));

    const h = try encodeStoreHeader(&buf, 1, .{});
    @memcpy(m[0..h.len], h);
    std.mem.writeInt(u32, m[12..16], 79, .little);
    try testing.expectError(mal, decodeFrame(m[0..h.len], 511, 65536));
    // Entry shorter than header body_len.
    try testing.expectError(mal, decodeFrame(h[0 .. h.len - 1], 511, 65536));
}

test "peekHeader" {
    var buf: [64]u8 = undefined;
    const g = try encodeWriterGrant(&buf, 4, .{ .writer_epoch = 2, .holder_session_nonce = 1, .holder_service_id = 1, .holder_node_id = 1, .granted_at_ns = 1 });
    const h = peekHeader(g).?;
    try testing.expectEqual(KvFrameKind.writer_grant, h.kind);
    try testing.expectEqual(@as(u64, 4), h.leader_epoch);
    try testing.expect(peekHeader(g[0..10]) == null);
    try testing.expect(peekHeader("garbage garbage garbage garbage garbage") == null);
}

test "fence state machine" {
    var f = FenceState{};
    // No grant yet: nothing accepted, not even epoch 0.
    try testing.expect(!f.acceptsData(0));
    try testing.expect(!f.acceptsData(1));

    f.onGrant(1);
    try testing.expect(f.acceptsData(1));
    try testing.expect(!f.acceptsData(0));
    try testing.expect(!f.acceptsData(2));
    try testing.expectEqual(@as(u64, 1), f.last_grant_epoch);

    // Release of a non-current epoch is ignored.
    f.onRelease(5);
    try testing.expect(f.acceptsData(1));

    // Release of the current epoch clears it but keeps last_grant_epoch.
    f.onRelease(1);
    try testing.expect(!f.acceptsData(1));
    try testing.expectEqual(@as(u64, 0), f.current_writer_epoch);
    try testing.expectEqual(@as(u64, 1), f.last_grant_epoch);

    // Release with epoch 0 while no writer is a no-op that stays fenced.
    f.onRelease(0);
    try testing.expect(!f.acceptsData(0));

    // New grant supersedes; the old epoch becomes stale.
    f.onGrant(2);
    f.onGrant(3);
    try testing.expect(!f.acceptsData(2));
    try testing.expect(f.acceptsData(3));
    // Late release of superseded epoch does not clear the new holder.
    f.onRelease(2);
    try testing.expect(f.acceptsData(3));
    try testing.expectEqual(@as(u64, 3), f.last_grant_epoch);
}

test "ttl expiry boundary" {
    try testing.expect(!isExpired(0, 1_000_000));
    try testing.expect(!isExpired(100, 99));
    try testing.expect(isExpired(100, 100));
    try testing.expect(isExpired(100, 101));
    try testing.expectEqual(@as(u64, 0), expireAtFor(500, 0));
    try testing.expectEqual(@as(u64, 500 + 2_000_000), expireAtFor(500, 2));
    try testing.expectEqual(std.math.maxInt(u64), expireAtFor(std.math.maxInt(u64) - 1, 5));
}

test "default maximum record fits actual topic payload capacity" {
    const cfg: KvStoreConfig = .{};
    const capacity = constants.default_max_frame_length - TopicPublishHeader.encoded_length;
    var buf: [capacity]u8 = undefined;
    const key: [engine_max_key_len]u8 = @splat('k');
    const value: [default_max_value_len]u8 = @splat('v');
    var builder = DataFrameBuilder.init(&buf, 1, 1);
    try builder.addPut(&key, &value, 0);
    const frame = builder.finish();
    try testing.expectEqual(constants.default_max_frame_length, TopicPublishHeader.encoded_length + frame.len);
    var records = (try decodeFrame(frame, cfg.max_key_len, cfg.max_value_len)).payload.data;
    const record = records.next().?;
    try testing.expectEqualSlices(u8, &key, record.key);
    try testing.expectEqualSlices(u8, &value, record.value);
    try testing.expect(records.next() == null);
}
