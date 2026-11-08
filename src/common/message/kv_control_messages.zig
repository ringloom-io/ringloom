// SPDX-License-Identifier: Apache-2.0
//! Service↔broker KV control messages (docs/kv-store-spec.md §12), templates 16–24.
//!
//! Same family as `topic_control_messages.zig`: the control ring-buffer
//! message format (4-byte `ControlMessageHeader{template_id, body_length}` +
//! extern body). Every KV message is **fixed size** (store names and the
//! reader's store directory are zero-padded fixed buffers) so the broker can
//! relay them verbatim over the admin transport (`KvAdminRequest`/`KvAdminReply`
//! in `src/broker/kv/kv_messages.zig`) without re-encoding.
//!
//! Layout rule: the 4-byte header is followed by 4 bytes of fields so that all
//! following 8-byte fields are naturally aligned (no implicit padding), and
//! every struct size is a multiple of 8. Decoding copies into an aligned local.
//!
//! Shared by the broker and the service runtime.
//!
//! ## Flows
//!
//! * Writer acquire: service → `RegisterKvWriter`; broker → `KvWriterResponse`
//!   (status `ok` = granted, `queued`, `held_by_other`, `config_mismatch`, ...).
//!   A queued writer later receives `KvWriterGranted` (async).
//! * Revocation: broker → `KvWriterRevoked` (writer lost / was superseded).
//! * Failover: after `TopicLeaderChanged`, service → `KvWriterReassert`; broker →
//!   `KvWriterReassertResponse` (status `ok` + current epoch) or `KvWriterRevoked`.
//! * Release: service → `UnregisterKvWriter` (no reply).
//! * Reader: service → `OpenKvReader`; broker → `KvReaderResponse` (local only).
//!
//! Responses are matched by `(store_id, session_nonce)`; a service ignores
//! responses whose nonce is not one of its open writers.

const std = @import("std");
const cm = @import("control_messages.zig");
const kv_format = @import("../kv/kv_format.zig");

const ControlMessageHeader = cm.ControlMessageHeader;
const header_size = cm.header_size;
pub const KvStoreConfig = kv_format.KvStoreConfig;

pub const TEMPLATE_REGISTER_KV_WRITER: u16 = 16;
pub const TEMPLATE_KV_WRITER_RESPONSE: u16 = 17;
pub const TEMPLATE_KV_WRITER_GRANTED: u16 = 18;
pub const TEMPLATE_KV_WRITER_REVOKED: u16 = 19;
pub const TEMPLATE_KV_WRITER_REASSERT: u16 = 20;
pub const TEMPLATE_KV_WRITER_REASSERT_RESPONSE: u16 = 21;
pub const TEMPLATE_UNREGISTER_KV_WRITER: u16 = 22;
pub const TEMPLATE_OPEN_KV_READER: u16 = 23;
pub const TEMPLATE_KV_READER_RESPONSE: u16 = 24;

/// Maximum store name length on the wire (== `kv_format.max_store_name_len`).
pub const kv_name_max: usize = kv_format.max_store_name_len;
/// Fixed size of the store directory buffer in `KvReaderResponse`.
pub const kv_store_dir_max: usize = 512;

/// Result code shared by all KV responses (writer and reader).
pub const KvStatus = enum(u8) {
    /// Writer: lease granted (`writer_epoch` is valid). Reader: store opened.
    ok = 0,
    /// Writer: waiting in the FIFO queue; `KvWriterGranted` follows.
    queued = 1,
    /// Writer: another writer holds the lease and `acquire_mode == fail_if_held`.
    held_by_other = 2,
    /// Writer: the store exists with a different config (ignoring `map_size_hint`).
    /// `config` in the response is the store's effective config.
    config_mismatch = 3,
    /// This session does not (or no longer) hold the lease.
    not_writer = 4,
    /// No topic leader reachable / this broker is not the leader and could not forward.
    leader_unavailable = 5,
    /// Reader: this broker is not KV-enabled (`broker.kv.enabled=false`).
    kv_not_available_locally = 6,
    /// Leader still catching up (barrier / tracker), or the local store has no
    /// published generation yet. Retry.
    not_ready = 7,
    /// Store name is not `[A-Za-z0-9._-]{1,64}`.
    invalid_name = 8,
    /// The `KvStoreConfig` failed validation.
    invalid_config = 9,
    /// Topics are disabled on the broker.
    topics_disabled = 10,
    /// Reader: no such store on this broker (yet).
    unknown_store = 11,
    /// The broker's `broker.kv.max.stores` limit is reached.
    store_limit = 12,
    /// Malformed request.
    bad_request = 13,
};

pub const AcquireMode = enum(u8) {
    fail_if_held = 0,
    wait = 1,
};

/// Reason in `KvWriterRevoked`.
pub const RevokeReason = enum(u8) {
    /// Another session now holds (or is queued ahead for) the lease.
    superseded = 0,
    /// The leader released the lease because the holder looked dead.
    holder_timeout = 1,
    /// The store's lease state was lost and could not be re-established.
    lease_lost = 2,
};

// ── Message bodies ───────────────────────────────────────────────────────────

/// service → broker (→ topic leader). Acquire the lease, creating the store if
/// needed. Re-sent unchanged (same `session_nonce`) to retry; a re-grant to the
/// same nonce is idempotent.
pub const RegisterKvWriterMsg = extern struct {
    header: ControlMessageHeader, // template_id = 16
    local_service_id: i32,
    /// Random per writer open; distinguishes restarts of the same service.
    session_nonce: u64,
    config: KvStoreConfig,
    name_length: u16,
    /// `AcquireMode`.
    acquire_mode: u8,
    flags: u8 = 0,
    _pad: [4]u8 = @splat(0),
    name: [kv_name_max]u8,
};

/// broker → service. Reply to `RegisterKvWriter`.
pub const KvWriterResponseMsg = extern struct {
    header: ControlMessageHeader, // template_id = 17
    /// `KvStatus`.
    status: u8,
    _pad0: [3]u8 = @splat(0),
    store_id: u64,
    session_nonce: u64,
    /// Valid for `ok`.
    writer_epoch: u64,
    /// == `store_id`; the backing topic.
    topic_id: u64,
    leader_epoch: u64,
    leader_node_id: i16,
    _pad1: [6]u8 = @splat(0),
    /// The store's effective config (from the log) when known; otherwise echoes the request.
    config: KvStoreConfig,
};

/// broker → service, asynchronous: a queued writer became the holder.
pub const KvWriterGrantedMsg = extern struct {
    header: ControlMessageHeader, // template_id = 18
    _pad0: [4]u8 = @splat(0),
    store_id: u64,
    session_nonce: u64,
    writer_epoch: u64,
    leader_epoch: u64,
    leader_node_id: i16,
    _pad1: [6]u8 = @splat(0),
};

/// broker → service: the session no longer holds the lease (fenced/superseded).
pub const KvWriterRevokedMsg = extern struct {
    header: ControlMessageHeader, // template_id = 19
    /// `RevokeReason`.
    reason: u8,
    _pad0: [3]u8 = @splat(0),
    store_id: u64,
    session_nonce: u64,
    /// The epoch that was revoked (the one the session believed it held; 0 if none).
    writer_epoch: u64,
    /// Epoch of the superseding grant, 0 if unknown/none.
    current_epoch: u64,
};

/// service → broker → topic leader: after a topic-leader change, confirm the lease (§9.4).
pub const KvWriterReassertMsg = extern struct {
    header: ControlMessageHeader, // template_id = 20
    local_service_id: i32,
    session_nonce: u64,
    /// Epoch the service believes it holds.
    writer_epoch: u64,
    name_length: u16,
    _pad: [6]u8 = @splat(0),
    name: [kv_name_max]u8,
};

/// broker → service. Reply to a reassert that was accepted: `status == ok` with
/// the (possibly new) `writer_epoch`. A rejected reassert is answered with
/// `KvWriterRevoked`; `leader_unavailable`/`not_ready` mean "retry".
pub const KvWriterReassertResponseMsg = extern struct {
    header: ControlMessageHeader, // template_id = 21
    /// `KvStatus`.
    status: u8,
    _pad0: [3]u8 = @splat(0),
    store_id: u64,
    session_nonce: u64,
    writer_epoch: u64,
    leader_epoch: u64,
    leader_node_id: i16,
    _pad1: [6]u8 = @splat(0),
};

/// service → broker → topic leader. Graceful release; no reply.
pub const UnregisterKvWriterMsg = extern struct {
    header: ControlMessageHeader, // template_id = 22
    local_service_id: i32,
    session_nonce: u64,
    writer_epoch: u64,
    name_length: u16,
    _pad: [6]u8 = @splat(0),
    name: [kv_name_max]u8,
};

/// service → local broker (never forwarded).
pub const OpenKvReaderMsg = extern struct {
    header: ControlMessageHeader, // template_id = 23
    local_service_id: i32,
    name_length: u16,
    flags: u16 = 0,
    _pad: [4]u8 = @splat(0),
    name: [kv_name_max]u8,
};

/// broker → service. Reply to `OpenKvReader`.
pub const KvReaderResponseMsg = extern struct {
    header: ControlMessageHeader, // template_id = 24
    /// `KvStatus`.
    status: u8,
    _pad0: [3]u8 = @splat(0),
    store_id: u64,
    /// `kv_format.engine_kind_*`.
    engine_kind: u32,
    store_dir_length: u16,
    _pad1: [2]u8 = @splat(0),
    config: KvStoreConfig,
    /// Absolute store directory (contains `control.shm`), zero padded.
    store_dir: [kv_store_dir_max]u8,
};

comptime {
    std.debug.assert(@sizeOf(RegisterKvWriterMsg) == 4 + 4 + 8 + 80 + 8 + kv_name_max);
    std.debug.assert(@sizeOf(KvWriterResponseMsg) == 8 + 8 * 5 + 8 + 80);
    std.debug.assert(@sizeOf(KvWriterGrantedMsg) == 8 + 8 * 4 + 8);
    std.debug.assert(@sizeOf(KvWriterRevokedMsg) == 8 + 8 * 4);
    std.debug.assert(@sizeOf(KvWriterReassertMsg) == 8 + 8 + 8 + 8 + kv_name_max);
    std.debug.assert(@sizeOf(KvWriterReassertResponseMsg) == 8 + 8 * 4 + 8);
    std.debug.assert(@sizeOf(UnregisterKvWriterMsg) == 8 + 8 + 8 + 8 + kv_name_max);
    std.debug.assert(@sizeOf(OpenKvReaderMsg) == 16 + kv_name_max);
    std.debug.assert(@sizeOf(KvReaderResponseMsg) == 8 + 8 + 8 + 80 + kv_store_dir_max);
}

/// Largest KV message a broker may relay over admin (see `kv_messages.zig`).
pub const max_message_size: usize = @max(
    @sizeOf(RegisterKvWriterMsg),
    @max(@sizeOf(KvWriterResponseMsg), @sizeOf(KvWriterReassertMsg)),
);

// ── Name helpers ─────────────────────────────────────────────────────────────

fn padName(name: []const u8) [kv_name_max]u8 {
    var out: [kv_name_max]u8 = @splat(0);
    const n = @min(name.len, kv_name_max);
    @memcpy(out[0..n], name[0..n]);
    return out;
}

/// The name carried by a message (`name_length` clamped to the buffer).
pub fn nameOf(name: *const [kv_name_max]u8, name_length: u16) []const u8 {
    return name[0..@min(@as(usize, name_length), kv_name_max)];
}

// ── Encode helpers ───────────────────────────────────────────────────────────

fn hdr(comptime T: type, template_id: u16) ControlMessageHeader {
    return .{ .template_id = template_id, .body_length = @intCast(@sizeOf(T) - header_size) };
}

fn put(buf: []u8, comptime T: type, value: T) usize {
    std.debug.assert(buf.len >= @sizeOf(T));
    var v = value;
    @memcpy(buf[0..@sizeOf(T)], std.mem.asBytes(&v));
    return @sizeOf(T);
}

pub fn encodeRegisterKvWriter(
    buf: []u8,
    local_service_id: i32,
    session_nonce: u64,
    config: KvStoreConfig,
    acquire_mode: AcquireMode,
    name: []const u8,
) usize {
    std.debug.assert(name.len <= kv_name_max);
    return put(buf, RegisterKvWriterMsg, .{
        .header = hdr(RegisterKvWriterMsg, TEMPLATE_REGISTER_KV_WRITER),
        .local_service_id = local_service_id,
        .session_nonce = session_nonce,
        .config = config,
        .name_length = @intCast(name.len),
        .acquire_mode = @backingInt(acquire_mode),
        .name = padName(name),
    });
}

pub fn encodeKvWriterResponse(
    buf: []u8,
    status: KvStatus,
    store_id: u64,
    session_nonce: u64,
    writer_epoch: u64,
    leader_epoch: u64,
    leader_node_id: i16,
    config: KvStoreConfig,
) usize {
    return put(buf, KvWriterResponseMsg, .{
        .header = hdr(KvWriterResponseMsg, TEMPLATE_KV_WRITER_RESPONSE),
        .status = @backingInt(status),
        .store_id = store_id,
        .session_nonce = session_nonce,
        .writer_epoch = writer_epoch,
        .topic_id = store_id,
        .leader_epoch = leader_epoch,
        .leader_node_id = leader_node_id,
        .config = config,
    });
}

pub fn encodeKvWriterGranted(
    buf: []u8,
    store_id: u64,
    session_nonce: u64,
    writer_epoch: u64,
    leader_epoch: u64,
    leader_node_id: i16,
) usize {
    return put(buf, KvWriterGrantedMsg, .{
        .header = hdr(KvWriterGrantedMsg, TEMPLATE_KV_WRITER_GRANTED),
        .store_id = store_id,
        .session_nonce = session_nonce,
        .writer_epoch = writer_epoch,
        .leader_epoch = leader_epoch,
        .leader_node_id = leader_node_id,
    });
}

pub fn encodeKvWriterRevoked(
    buf: []u8,
    reason: RevokeReason,
    store_id: u64,
    session_nonce: u64,
    writer_epoch: u64,
    current_epoch: u64,
) usize {
    return put(buf, KvWriterRevokedMsg, .{
        .header = hdr(KvWriterRevokedMsg, TEMPLATE_KV_WRITER_REVOKED),
        .reason = @backingInt(reason),
        .store_id = store_id,
        .session_nonce = session_nonce,
        .writer_epoch = writer_epoch,
        .current_epoch = current_epoch,
    });
}

pub fn encodeKvWriterReassert(
    buf: []u8,
    local_service_id: i32,
    session_nonce: u64,
    writer_epoch: u64,
    name: []const u8,
) usize {
    std.debug.assert(name.len <= kv_name_max);
    return put(buf, KvWriterReassertMsg, .{
        .header = hdr(KvWriterReassertMsg, TEMPLATE_KV_WRITER_REASSERT),
        .local_service_id = local_service_id,
        .session_nonce = session_nonce,
        .writer_epoch = writer_epoch,
        .name_length = @intCast(name.len),
        .name = padName(name),
    });
}

pub fn encodeKvWriterReassertResponse(
    buf: []u8,
    status: KvStatus,
    store_id: u64,
    session_nonce: u64,
    writer_epoch: u64,
    leader_epoch: u64,
    leader_node_id: i16,
) usize {
    return put(buf, KvWriterReassertResponseMsg, .{
        .header = hdr(KvWriterReassertResponseMsg, TEMPLATE_KV_WRITER_REASSERT_RESPONSE),
        .status = @backingInt(status),
        .store_id = store_id,
        .session_nonce = session_nonce,
        .writer_epoch = writer_epoch,
        .leader_epoch = leader_epoch,
        .leader_node_id = leader_node_id,
    });
}

pub fn encodeUnregisterKvWriter(
    buf: []u8,
    local_service_id: i32,
    session_nonce: u64,
    writer_epoch: u64,
    name: []const u8,
) usize {
    std.debug.assert(name.len <= kv_name_max);
    return put(buf, UnregisterKvWriterMsg, .{
        .header = hdr(UnregisterKvWriterMsg, TEMPLATE_UNREGISTER_KV_WRITER),
        .local_service_id = local_service_id,
        .session_nonce = session_nonce,
        .writer_epoch = writer_epoch,
        .name_length = @intCast(name.len),
        .name = padName(name),
    });
}

pub fn encodeOpenKvReader(buf: []u8, local_service_id: i32, name: []const u8) usize {
    std.debug.assert(name.len <= kv_name_max);
    return put(buf, OpenKvReaderMsg, .{
        .header = hdr(OpenKvReaderMsg, TEMPLATE_OPEN_KV_READER),
        .local_service_id = local_service_id,
        .name_length = @intCast(name.len),
        .name = padName(name),
    });
}

/// `store_dir` longer than `kv_store_dir_max` is rejected (returns null).
pub fn encodeKvReaderResponse(
    buf: []u8,
    status: KvStatus,
    store_id: u64,
    engine_kind: u32,
    config: KvStoreConfig,
    store_dir: []const u8,
) ?usize {
    if (store_dir.len > kv_store_dir_max) return null;
    var dir: [kv_store_dir_max]u8 = @splat(0);
    @memcpy(dir[0..store_dir.len], store_dir);
    return put(buf, KvReaderResponseMsg, .{
        .header = hdr(KvReaderResponseMsg, TEMPLATE_KV_READER_RESPONSE),
        .status = @backingInt(status),
        .store_id = store_id,
        .engine_kind = engine_kind,
        .store_dir_length = @intCast(store_dir.len),
        .config = config,
        .store_dir = dir,
    });
}

// ── Decode helpers ───────────────────────────────────────────────────────────

/// Copies a `T` out of `bytes`, or null if too short. Does not check the
/// template id; callers dispatch on it first.
pub fn decode(comptime T: type, bytes: []const u8) ?T {
    if (bytes.len < @sizeOf(T)) return null;
    var out: T = undefined;
    @memcpy(std.mem.asBytes(&out), bytes[0..@sizeOf(T)]);
    return out;
}

pub fn statusOf(raw: u8) ?KvStatus {
    return std.enums.fromInt(KvStatus, raw);
}

pub fn readerStoreDir(m: *const KvReaderResponseMsg) []const u8 {
    return m.store_dir[0..@min(@as(usize, m.store_dir_length), kv_store_dir_max)];
}

// ── Tests ────────────────────────────────────────────────────────────────────

const testing = std.testing;

test "template ids are contiguous after the topic templates" {
    try testing.expectEqual(@as(u16, 16), TEMPLATE_REGISTER_KV_WRITER);
    try testing.expectEqual(@as(u16, 24), TEMPLATE_KV_READER_RESPONSE);
}

test "RegisterKvWriter round-trips" {
    var buf: [max_message_size]u8 = undefined;
    var cfg = KvStoreConfig{};
    cfg.default_ttl_ms = 1234;
    const n = encodeRegisterKvWriter(&buf, 42, 0xDEAD_BEEF, cfg, .wait, "prices.v1");
    try testing.expectEqual(@sizeOf(RegisterKvWriterMsg), n);
    const m = decode(RegisterKvWriterMsg, buf[0..n]).?;
    try testing.expectEqual(TEMPLATE_REGISTER_KV_WRITER, m.header.template_id);
    try testing.expectEqual(@as(u16, @intCast(n - header_size)), m.header.body_length);
    try testing.expectEqual(@as(i32, 42), m.local_service_id);
    try testing.expectEqual(@as(u64, 0xDEAD_BEEF), m.session_nonce);
    try testing.expectEqual(AcquireMode.wait, @as(AcquireMode, @fromBackingInt(@intCast(m.acquire_mode))));
    try testing.expectEqual(@as(u64, 1234), m.config.default_ttl_ms);
    try testing.expectEqualStrings("prices.v1", nameOf(&m.name, m.name_length));
    try testing.expect(decode(RegisterKvWriterMsg, buf[0 .. n - 1]) == null);
}

test "KvWriterResponse round-trips with config" {
    var buf: [max_message_size]u8 = undefined;
    var cfg = KvStoreConfig{};
    cfg.max_value_len = 777;
    const n = encodeKvWriterResponse(&buf, .config_mismatch, 99, 5, 3, 8, 2, cfg);
    const m = decode(KvWriterResponseMsg, buf[0..n]).?;
    try testing.expectEqual(KvStatus.config_mismatch, statusOf(m.status).?);
    try testing.expectEqual(@as(u64, 99), m.store_id);
    try testing.expectEqual(@as(u64, 99), m.topic_id);
    try testing.expectEqual(@as(u64, 5), m.session_nonce);
    try testing.expectEqual(@as(u64, 3), m.writer_epoch);
    try testing.expectEqual(@as(u64, 8), m.leader_epoch);
    try testing.expectEqual(@as(i16, 2), m.leader_node_id);
    try testing.expectEqual(@as(u32, 777), m.config.max_value_len);
    try testing.expect(statusOf(200) == null);
}

test "granted / revoked / reassert response round-trip" {
    var buf: [128]u8 = undefined;
    const n1 = encodeKvWriterGranted(&buf, 1, 2, 3, 4, 5);
    const g = decode(KvWriterGrantedMsg, buf[0..n1]).?;
    try testing.expectEqual(TEMPLATE_KV_WRITER_GRANTED, g.header.template_id);
    try testing.expectEqual(@as(u64, 3), g.writer_epoch);
    try testing.expectEqual(@as(i16, 5), g.leader_node_id);

    const n2 = encodeKvWriterRevoked(&buf, .holder_timeout, 1, 2, 3, 4);
    const r = decode(KvWriterRevokedMsg, buf[0..n2]).?;
    try testing.expectEqual(TEMPLATE_KV_WRITER_REVOKED, r.header.template_id);
    try testing.expectEqual(@as(u8, @backingInt(RevokeReason.holder_timeout)), r.reason);
    try testing.expectEqual(@as(u64, 4), r.current_epoch);

    const n3 = encodeKvWriterReassertResponse(&buf, .ok, 1, 2, 9, 4, 1);
    const a = decode(KvWriterReassertResponseMsg, buf[0..n3]).?;
    try testing.expectEqual(TEMPLATE_KV_WRITER_REASSERT_RESPONSE, a.header.template_id);
    try testing.expectEqual(@as(u64, 9), a.writer_epoch);
}

test "reassert and unregister round-trip" {
    var buf: [max_message_size]u8 = undefined;
    const n = encodeKvWriterReassert(&buf, 7, 11, 12, "s");
    const m = decode(KvWriterReassertMsg, buf[0..n]).?;
    try testing.expectEqual(TEMPLATE_KV_WRITER_REASSERT, m.header.template_id);
    try testing.expectEqual(@as(i32, 7), m.local_service_id);
    try testing.expectEqual(@as(u64, 12), m.writer_epoch);
    try testing.expectEqualStrings("s", nameOf(&m.name, m.name_length));

    const n2 = encodeUnregisterKvWriter(&buf, 7, 11, 12, "a-b_c.d");
    const u = decode(UnregisterKvWriterMsg, buf[0..n2]).?;
    try testing.expectEqual(TEMPLATE_UNREGISTER_KV_WRITER, u.header.template_id);
    try testing.expectEqualStrings("a-b_c.d", nameOf(&u.name, u.name_length));
}

test "OpenKvReader + KvReaderResponse round-trip with fixed dir buffer" {
    var buf: [1024]u8 = undefined;
    const n = encodeOpenKvReader(&buf, 3, "prices");
    const o = decode(OpenKvReaderMsg, buf[0..n]).?;
    try testing.expectEqual(TEMPLATE_OPEN_KV_READER, o.header.template_id);
    try testing.expectEqualStrings("prices", nameOf(&o.name, o.name_length));

    const cfg = KvStoreConfig{};
    const dir = "/dev/shm/kv/g/node-1/prices";
    const n2 = encodeKvReaderResponse(&buf, .ok, 77, kv_format.engine_kind_lmdb, cfg, dir).?;
    const r = decode(KvReaderResponseMsg, buf[0..n2]).?;
    try testing.expectEqual(KvStatus.ok, statusOf(r.status).?);
    try testing.expectEqual(@as(u64, 77), r.store_id);
    try testing.expectEqual(kv_format.engine_kind_lmdb, r.engine_kind);
    try testing.expectEqualStrings(dir, readerStoreDir(&r));

    var too_long: [kv_store_dir_max + 1]u8 = @splat('x');
    try testing.expect(encodeKvReaderResponse(&buf, .ok, 1, 1, cfg, &too_long) == null);
    // Failure responses carry an empty dir.
    const n3 = encodeKvReaderResponse(&buf, .kv_not_available_locally, 77, 0, cfg, "").?;
    const f = decode(KvReaderResponseMsg, buf[0..n3]).?;
    try testing.expectEqual(@as(usize, 0), readerStoreDir(&f).len);
}

test "oversize name_length is clamped when read back" {
    var m = decode(OpenKvReaderMsg, blk: {
        var b: [@sizeOf(OpenKvReaderMsg)]u8 = undefined;
        _ = encodeOpenKvReader(&b, 1, "x");
        break :blk &b;
    }).?;
    m.name_length = 60000;
    try testing.expectEqual(kv_name_max, nameOf(&m.name, m.name_length).len);
}
