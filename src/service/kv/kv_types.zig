// SPDX-License-Identifier: Apache-2.0
//! Public result/option types of the service-side KV API
//! (docs/kv-store-spec.md §11.1, §11.2). Shared by the native Zig API and the
//! C ABI.

const std = @import("std");
const ringloom_common = @import("ringloom_common");
const kv = ringloom_common.kv;

pub const KvStoreConfig = kv.KvStoreConfig;
pub const KvStoreState = kv.KvStoreState;
pub const AckMode = ringloom_common.topics.AckMode;

// ── Clocks ───────────────────────────────────────────────────────────────

/// Clock used by a reader for TTL checks (§11.1).
pub const TtlClock = enum(u8) {
    /// `CLOCK_REALTIME` through the vDSO (default).
    precise = 0,
    /// `CLOCK_REALTIME_COARSE`: cheaper, millisecond resolution.
    coarse = 1,
};

/// Current `CLOCK_REALTIME` / `CLOCK_REALTIME_COARSE` in nanoseconds.
pub fn realtimeNs(clock: TtlClock) u64 {
    var ts: std.os.linux.timespec = undefined;
    const id: std.os.linux.clockid_t = switch (clock) {
        .precise => .REALTIME,
        // CLOCK_REALTIME_COARSE == 5 on Linux.
        .coarse => @fromBackingInt(@intCast(5)),
    };
    if (std.os.linux.clock_gettime(id, &ts) != 0) return 0;
    return @as(u64, @intCast(ts.sec)) * std.time.ns_per_s + @as(u64, @intCast(ts.nsec));
}

// ── Reader ───────────────────────────────────────────────────────────────

pub const KvReaderOptions = struct {
    /// Wait (while retrying the broker request) until the store has a published
    /// generation. 0 = do not wait: sessions return `not_ready` until then.
    wait_ready_timeout_ms: u32 = 0,
    /// Warm the page cache and the engine mapping at open.
    prefault: bool = false,
    ttl_clock: TtlClock = .precise,
    /// Engine reader slots for this process' environment (setup only).
    max_readers: u32 = 1024,
};

/// Snapshot of the control header (§11.1 `status()`).
pub const KvStatus = struct {
    state: KvStoreState,
    /// Generation currently published by the applier (0 = none yet).
    generation: u64,
    applied_index: u64,
    applied_leader_epoch: u64,
    /// `CLOCK_REALTIME` ns of the last apply commit.
    applied_at_ns: u64,
    local_log_tip_index: u64,
    /// `local_log_tip_index - applied_index` (saturating).
    lag_entries: u64,
    replication_live: bool,
    /// Age of the applier heartbeat in ms; `maxInt(u64)` if it never beat.
    heartbeat_age_ms: u64,
    /// Generation switches performed by this process' sessions
    /// (`kv_reader_generation_switches`).
    generation_switches: u64,
};

pub const KvGetResult = union(enum) {
    /// Value copied to `out`; payload is its length.
    found: usize,
    /// Absent or expired.
    missing,
    /// `out` is too small; payload is the required length. Nothing copied.
    buffer_too_small: usize,
    /// No generation published yet (or the engine could not be opened).
    not_ready,
};

pub const KvViewResult = union(enum) {
    /// Borrowed value, valid until `KvReadSession.release()`.
    found: []const u8,
    missing,
    not_ready,
};

pub const KvIterResult = struct {
    pub const Status = enum { done, stopped, not_ready, failed };
    status: Status,
    /// Number of non-expired entries passed to the callback.
    visited: u64 = 0,
};

// ── Writer ───────────────────────────────────────────────────────────────

pub const AcquireMode = ringloom_common.message.kv_control_messages.AcquireMode;

pub const KvWriterState = enum(u8) {
    /// Holds the lease and may publish.
    granted = 0,
    /// Waiting in the leader's FIFO queue (`acquire = .wait`).
    queued = 1,
    /// Fenced/superseded; terminal. Writes return `not_writer`.
    revoked = 2,
    /// Topic leader changed; waiting for the leader to confirm the lease.
    reasserting = 3,
};

pub const KvWriteResult = union(enum) {
    /// Published (or, inside a batch, buffered: payload 0). Payload is the
    /// publisher sequence token for `KvWriter.isAcked`.
    ok: u64,
    back_pressured,
    not_writer,
    leader_unavailable,
    key_too_large,
    value_too_large,
    batch_full,
};

pub const KvPutOpts = struct {
    /// null = store default (`KvStoreConfig.default_ttl_ms`); 0 = never expires.
    ttl_ms: ?u64 = null,
    ack_mode: AckMode = .fire_and_forget,
};

pub const KvWriteOpts = struct {
    ack_mode: AckMode = .fire_and_forget,
};

/// Callback for writer state changes. Runs on the thread that polls the
/// service control ring (control agent or the application's poll thread).
pub const KvStateCallback = *const fn (user_data: ?*anyopaque, state: KvWriterState, writer_epoch: u64) void;

pub const KvWriterOptions = struct {
    acquire: AcquireMode = .fail_if_held,
    on_state: ?KvStateCallback = null,
    on_state_user_data: ?*anyopaque = null,
    /// Time to wait for the broker's first response (the lease may need a
    /// leader round trip).
    open_timeout_ms: u32 = 5000,
    /// Size of the preallocated batch/scratch buffers; must not exceed the
    /// broker's `max_frame_length`.
    max_frame_length: u32 = ringloom_common.platform.constants.default_max_frame_length,
};

pub const OpenError = error{
    InvalidName,
    InvalidConfig,
    /// Control agent runs on its own thread in a mode that cannot wait (unused
    /// for KV; kept for symmetry) or the engine is not running.
    EngineNotRunning,
    SendFailed,
    Timeout,
    /// Reader: `broker.kv.enabled=false` on the local broker.
    KvNotAvailableLocally,
    /// Reader: no such store on this broker.
    UnknownStore,
    /// Reader: store exists but has no published generation (timeout elapsed).
    NotReady,
    /// The reader was built with a different engine than the store.
    EngineMismatch,
    ControlFileInvalid,
    TopicsDisabled,
    StoreLimit,
    /// Writer: another writer holds the lease and `acquire = .fail_if_held`.
    HeldByOther,
    ConfigMismatch,
    LeaderUnavailable,
    BadRequest,
    LeaderPublicationFailed,
    OutOfMemory,
    EngineFailure,
};
