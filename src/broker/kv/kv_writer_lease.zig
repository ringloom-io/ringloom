// SPDX-License-Identifier: Apache-2.0
//! KV writer-lease arbitration on the topic leader (docs/kv-store-spec.md §9.2–§9.4).
//!
//! This module is a **pure state machine**: no I/O, no clocks, no allocation
//! after `LeaseTable.init`. The control loop (see `control/kv_control.zig`)
//! drives it:
//!
//!   1. `submit` — decoded service requests (register / reassert / unregister),
//!      and `serviceGone` for dead services;
//!   2. `step(view, ...)` per store, where `View` is built from the grant
//!      tracker (what the log says) plus local topic state (leader, barrier);
//!   3. execute the emitted `Action`s: leader appends (store header / grant /
//!      release) and `reply`s to services;
//!   4. `onAppendDone` when the topic engine reports the assigned index.
//!
//! ## Invariants
//!
//! * One operation per store at a time (`phase`). Further requests wait in a
//!   bounded FIFO `inbox`.
//! * Decisions are made only from the log as read by the tracker, and only
//!   when the tracker has consumed everything the leader itself appended
//!   (`View.tracked_index >= await_index`) and has caught up to the local tip
//!   with the failover barrier cleared (`View.ready`).
//! * `GRANTED` is replied only after the `writer_grant` append completes, so
//!   the writer's data frames always land at a higher index (§9.2).
//! * Epochs strictly increase: `max(log last_epoch, issued_epoch) + 1`.
//! * Fairness: queued waiters (`acquire_mode = wait`) are served FIFO and take
//!   precedence over new requests whenever the lease is free.
//! * Waiters are leader memory only: after a leader change, `queued` clients
//!   must re-send `RegisterKvWriter` (clients do this on `TopicLeaderChanged`).
//!   Holders are recovered from the log and confirmed by reassert (§9.4).
//!
//! ## Holder liveness
//!
//! Immediate: `serviceGone(party)` (service removed, broker peer lost). Backstop:
//! `View.holder_registered == false` for `holder_grace_ns` (covers a leader that
//! never saw the removal, e.g. the holder died during a failover).

const std = @import("std");
const common = @import("ringloom_common");
const kv = common.kv;
const msgs = common.message.kv_control_messages;
const tracker = @import("kv_grant_tracker.zig");

pub const KvStatus = msgs.KvStatus;
pub const AcquireMode = msgs.AcquireMode;
pub const RevokeReason = msgs.RevokeReason;
pub const KvStoreConfig = kv.KvStoreConfig;
pub const Holder = tracker.Holder;

/// `KvWriterRelease.reason` values.
pub const release_graceful: u32 = 0;
pub const release_holder_died: u32 = 1;

pub const inbox_cap = 16;
pub const waiter_cap = 16;
pub const gone_cap = 8;
pub const outbox_cap = 32;

pub const Config = struct {
    /// Blocked requests older than this are answered `NOT_READY` (client retries).
    request_timeout_ns: i64 = 5 * std.time.ns_per_s,
    /// An append with no completion after this long is treated as failed.
    append_timeout_ns: i64 = 5 * std.time.ns_per_s,
    /// Holder missing from the service registry for this long => released.
    holder_grace_ns: i64 = 5 * std.time.ns_per_s,
    /// Delay before retrying a failed leader append.
    retry_ns: i64 = 100 * std.time.ns_per_ms,
};

/// A service endpoint: broker node + that broker's local service id.
pub const Party = struct {
    node_id: u16,
    service_id: u32,

    pub fn eql(a: Party, b: Party) bool {
        return a.node_id == b.node_id and a.service_id == b.service_id;
    }
};

pub const Requester = struct { party: Party, nonce: u64 };

pub const RequestKind = enum { register, reassert, unregister };

pub const Request = struct {
    kind: RequestKind,
    who: Requester,
    mode: AcquireMode = .fail_if_held,
    /// Reassert/unregister: the epoch the client believes it holds.
    epoch: u64 = 0,
    /// Register: requested config.
    config: KvStoreConfig = .{},
    received_ns: i64 = 0,
};

pub const ReplyKind = enum { writer_response, writer_granted, writer_revoked, reassert_response };

pub const Reply = struct {
    to: Party,
    kind: ReplyKind,
    store_id: u64,
    status: KvStatus = .ok,
    nonce: u64,
    /// Granted/held epoch (`ok`), or the revoked epoch (`writer_revoked`).
    epoch: u64 = 0,
    /// `writer_revoked`: the superseding epoch, 0 if unknown.
    current_epoch: u64 = 0,
    reason: RevokeReason = .superseded,
    /// Effective store config (from the log) when known.
    config: KvStoreConfig = .{},
};

pub const Action = union(enum) {
    /// Create the topic `__kv.<name>` (idempotent) and append `STORE_HEADER`.
    create_store: struct { store_id: u64, corr: u64, config: KvStoreConfig },
    append_grant: struct { store_id: u64, corr: u64, grant: kv.KvWriterGrant },
    append_release: struct { store_id: u64, corr: u64, release: kv.KvWriterRelease },
    reply: Reply,
};

pub const Outbox = struct {
    items: [outbox_cap]Action = undefined,
    len: usize = 0,

    pub fn push(self: *Outbox, a: Action) void {
        std.debug.assert(self.len < outbox_cap);
        self.items[self.len] = a;
        self.len += 1;
    }
    pub fn room(self: *const Outbox, n: usize) bool {
        return outbox_cap - self.len >= n;
    }
    pub fn slice(self: *const Outbox) []const Action {
        return self.items[0..self.len];
    }
    pub fn clear(self: *Outbox) void {
        self.len = 0;
    }
};

/// Everything arbitration needs to know about one store at one instant.
pub const View = struct {
    /// This node is the topic leader, the failover barrier is cleared, and the
    /// grant tracker is caught up to the local tip (or the topic does not
    /// exist yet and this node may create it).
    ready: bool,
    /// The log's first valid `store_header` has been read.
    header_seen: bool = false,
    /// Valid iff `header_seen`.
    config: KvStoreConfig = .{},
    holder: ?Holder = null,
    last_epoch: u64 = 0,
    tracked_index: u64 = 0,
    /// The holder's service is still listed in the broker's service registry.
    holder_registered: bool = true,
};

const Waiter = struct { who: Requester, queued_ns: i64 };

const Phase = union(enum) {
    idle,
    header: struct { corr: u64 },
    grant: struct { corr: u64, grant: kv.KvWriterGrant, who: Requester, reply_kind: ReplyKind, config: KvStoreConfig },
    release: struct { corr: u64, epoch: u64 },
};

const WantRelease = struct { epoch: u64, reason: u32, notify: ?Requester = null };

pub const StoreLease = struct {
    used: bool = false,
    store_id: u64 = 0,
    name_len: u8 = 0,
    name: [msgs.kv_name_max]u8 = undefined,

    inbox: [inbox_cap]Request = undefined,
    inbox_len: u8 = 0,
    waiters: [waiter_cap]Waiter = undefined,
    waiters_len: u8 = 0,
    gone: [gone_cap]Party = undefined,
    gone_len: u8 = 0,

    phase: Phase = .idle,
    phase_started_ns: i64 = 0,
    corr_counter: u32 = 0,
    slot_tag: u32 = 0,
    /// Tracker must have consumed this log index before the next decision.
    await_index: u64 = 0,
    issued_epoch: u64 = 0,
    holder_missing_since_ns: i64 = 0,
    want_release: ?WantRelease = null,
    /// No new append before this time (after a failure).
    retry_at_ns: i64 = 0,

    pub fn storeName(self: *const StoreLease) []const u8 {
        return self.name[0..self.name_len];
    }

    pub fn isIdle(self: *const StoreLease) bool {
        return self.phase == .idle;
    }

    pub fn waiterCount(self: *const StoreLease) u32 {
        return self.waiters_len;
    }

    pub fn inboxCount(self: *const StoreLease) u32 {
        return self.inbox_len;
    }

    fn nextCorr(self: *StoreLease) u64 {
        self.corr_counter +%= 1;
        return (@as(u64, self.slot_tag) << 32) | self.corr_counter;
    }

    // ── Input ────────────────────────────────────────────────────────────

    /// Queue a request. Register/reassert retries from the same session replace
    /// the pending one. Returns false if the inbox is full.
    pub fn submit(self: *StoreLease, req: Request) bool {
        if (req.kind == .unregister) {
            // The session is closing: its still-pending register/reassert
            // requests (e.g. a client-side retry sent before it gave up on the
            // open) must not be granted after it left.
            var i: usize = 0;
            while (i < self.inbox_len) {
                const e = self.inbox[i];
                if (e.kind != .unregister and e.who.nonce == req.who.nonce) self.removeInbox(i) else i += 1;
            }
        } else {
            for (self.inbox[0..self.inbox_len]) |*e| {
                if (e.kind == req.kind and e.who.nonce == req.who.nonce) {
                    e.* = req; // refresh party/config; keeps FIFO position
                    e.received_ns = req.received_ns;
                    return true;
                }
            }
        }
        if (self.inbox_len >= inbox_cap) return false;
        self.inbox[self.inbox_len] = req;
        self.inbox_len += 1;
        return true;
    }

    /// A service is gone (removed or its broker lost).
    pub fn serviceGone(self: *StoreLease, party: Party) void {
        self.removeWaitersOf(party);
        if (self.gone_len < gone_cap) {
            self.gone[self.gone_len] = party;
            self.gone_len += 1;
        } // else: dropped; the registry-based liveness check is the backstop
    }

    fn removeWaitersOf(self: *StoreLease, party: Party) void {
        var i: usize = 0;
        while (i < self.waiters_len) {
            if (self.waiters[i].who.party.eql(party)) self.removeWaiter(i) else i += 1;
        }
    }

    fn removeWaiter(self: *StoreLease, i: usize) void {
        var j = i;
        while (j + 1 < self.waiters_len) : (j += 1) self.waiters[j] = self.waiters[j + 1];
        self.waiters_len -= 1;
    }

    fn findWaiter(self: *StoreLease, nonce: u64) ?usize {
        for (self.waiters[0..self.waiters_len], 0..) |w, i| if (w.who.nonce == nonce) return i;
        return null;
    }

    fn pushWaiter(self: *StoreLease, who: Requester, now_ns: i64) bool {
        if (self.waiters_len >= waiter_cap) return false;
        self.waiters[self.waiters_len] = .{ .who = who, .queued_ns = now_ns };
        self.waiters_len += 1;
        return true;
    }

    fn removeInbox(self: *StoreLease, i: usize) void {
        var j = i;
        while (j + 1 < self.inbox_len) : (j += 1) self.inbox[j] = self.inbox[j + 1];
        self.inbox_len -= 1;
    }

    // ── Replies ──────────────────────────────────────────────────────────

    fn replyTo(self: *StoreLease, out: *Outbox, req: Request, status: KvStatus, epoch: u64, config: KvStoreConfig) void {
        out.push(.{ .reply = .{
            .to = req.who.party,
            .kind = if (req.kind == .reassert) .reassert_response else .writer_response,
            .store_id = self.store_id,
            .status = status,
            .nonce = req.who.nonce,
            .epoch = epoch,
            .config = config,
        } });
    }

    fn revoke(self: *StoreLease, out: *Outbox, to: Requester, reason: RevokeReason, epoch: u64, current: u64) void {
        out.push(.{ .reply = .{
            .to = to.party,
            .kind = .writer_revoked,
            .store_id = self.store_id,
            .nonce = to.nonce,
            .epoch = epoch,
            .current_epoch = current,
            .reason = reason,
        } });
    }

    // ── Append completion ────────────────────────────────────────────────

    /// The topic engine finished (or failed) the append tagged `corr`.
    pub fn onAppendDone(self: *StoreLease, corr: u64, ok: bool, index: u64, now_ns: i64, out: *Outbox) void {
        switch (self.phase) {
            .idle => {},
            .header => |p| {
                if (p.corr != corr) return;
                self.phase = .idle;
                if (ok) self.await_index = @max(self.await_index, index) else self.retry_at_ns = now_ns + 100 * std.time.ns_per_ms;
            },
            .grant => |g| {
                if (g.corr != corr) return;
                self.phase = .idle;
                if (ok) {
                    self.await_index = @max(self.await_index, index);
                    out.push(.{ .reply = .{
                        .to = g.who.party,
                        .kind = g.reply_kind,
                        .store_id = self.store_id,
                        .status = .ok,
                        .nonce = g.who.nonce,
                        .epoch = g.grant.writer_epoch,
                        .config = g.config,
                    } });
                } else {
                    self.retry_at_ns = now_ns + 100 * std.time.ns_per_ms;
                    switch (g.reply_kind) {
                        // Synchronous callers retry; a queued waiter keeps its place.
                        .writer_granted => {
                            if (self.waiters_len < waiter_cap) {
                                var j: usize = self.waiters_len;
                                while (j > 0) : (j -= 1) self.waiters[j] = self.waiters[j - 1];
                                self.waiters[0] = .{ .who = g.who, .queued_ns = now_ns };
                                self.waiters_len += 1;
                            }
                        },
                        .reassert_response => out.push(.{ .reply = .{ .to = g.who.party, .kind = .reassert_response, .store_id = self.store_id, .status = .not_ready, .nonce = g.who.nonce } }),
                        else => out.push(.{ .reply = .{ .to = g.who.party, .kind = .writer_response, .store_id = self.store_id, .status = .not_ready, .nonce = g.who.nonce, .config = g.config } }),
                    }
                }
            },
            .release => |r| {
                if (r.corr != corr) return;
                self.phase = .idle;
                if (ok) {
                    self.await_index = @max(self.await_index, index);
                } else {
                    // Keep wanting it; the holder is still in the log.
                    self.want_release = .{ .epoch = r.epoch, .reason = release_graceful };
                    self.retry_at_ns = now_ns + 100 * std.time.ns_per_ms;
                }
            },
        }
    }

    /// True if `step` could do something right now (the driver steps idle
    /// stores only occasionally, to check holder liveness).
    pub fn needsStep(self: *const StoreLease) bool {
        return self.inbox_len > 0 or self.phase != .idle or self.gone_len > 0 or self.want_release != null or self.waiters_len > 0;
    }

    /// This node stopped being the topic leader: answer everything pending with
    /// `leader_unavailable` and forget leader-memory state (waiters re-register
    /// with the new leader; holders are recovered from the log).
    pub fn abandon(self: *StoreLease, out: *Outbox) void {
        var i: usize = 0;
        while (i < self.inbox_len and out.room(1)) : (i += 1) {
            const r = self.inbox[i];
            if (r.kind != .unregister) self.replyTo(out, r, .leader_unavailable, 0, r.config);
        }
        self.inbox_len = 0;
        self.waiters_len = 0;
        self.gone_len = 0;
        self.phase = .idle;
        self.want_release = null;
        self.await_index = 0;
        self.holder_missing_since_ns = 0;
    }

    // ── Arbitration ──────────────────────────────────────────────────────

    /// Advance the state machine. Emits actions into `out` (caller drains).
    pub fn step(self: *StoreLease, view: View, now_ns: i64, wall_ns: u64, cfg: Config, out: *Outbox) void {
        if (self.phase != .idle) {
            if (now_ns - self.phase_started_ns > cfg.append_timeout_ns) {
                const corr = switch (self.phase) {
                    .header => |p| p.corr,
                    .grant => |p| p.corr,
                    .release => |p| p.corr,
                    .idle => 0,
                };
                self.onAppendDone(corr, false, 0, now_ns, out);
            }
            if (self.phase != .idle) return;
        }
        self.expireInbox(now_ns, cfg, out);
        if (!view.ready) return;
        if (view.tracked_index < self.await_index) return;
        if (now_ns < self.retry_at_ns) return;

        if (!view.header_seen) {
            self.stepNoHeader(now_ns, out);
            return;
        }
        self.stepNormal(view, now_ns, wall_ns, cfg, out);
    }

    fn expireInbox(self: *StoreLease, now_ns: i64, cfg: Config, out: *Outbox) void {
        var i: usize = 0;
        while (i < self.inbox_len) {
            const r = self.inbox[i];
            if (now_ns - r.received_ns > cfg.request_timeout_ns and out.room(2)) {
                if (r.kind != .unregister) self.replyTo(out, r, .not_ready, 0, r.config);
                self.removeInbox(i);
            } else i += 1;
        }
    }

    fn startCreateStore(self: *StoreLease, config: KvStoreConfig, now_ns: i64, out: *Outbox) void {
        const corr = self.nextCorr();
        self.phase = .{ .header = .{ .corr = corr } };
        self.phase_started_ns = now_ns;
        out.push(.{ .create_store = .{ .store_id = self.store_id, .corr = corr, .config = config } });
    }

    /// No `store_header` in the log: create the store for the first register;
    /// reassert/unregister cannot refer to anything that exists.
    fn stepNoHeader(self: *StoreLease, now_ns: i64, out: *Outbox) void {
        var first_register: ?KvStoreConfig = null;
        var i: usize = 0;
        while (i < self.inbox_len) {
            const r = self.inbox[i];
            switch (r.kind) {
                .register => {
                    if (first_register == null) first_register = r.config;
                    i += 1;
                },
                .reassert => {
                    // The store (header and grant) was lost in a failover: the
                    // writer must start over with a full registration.
                    if (!out.room(2)) break;
                    self.revoke(out, r.who, .lease_lost, r.epoch, 0);
                    self.removeInbox(i);
                },
                .unregister => self.removeInbox(i),
            }
        }
        if (first_register) |cfg| {
            if (out.room(2)) self.startCreateStore(cfg, now_ns, out);
        }
    }

    fn startGrant(self: *StoreLease, who: Requester, view: View, reply_kind: ReplyKind, now_ns: i64, wall_ns: u64, out: *Outbox) void {
        const epoch = @max(view.last_epoch, self.issued_epoch) + 1;
        self.issued_epoch = epoch;
        const grant = kv.KvWriterGrant{
            .writer_epoch = epoch,
            .holder_session_nonce = who.nonce,
            .holder_service_id = who.party.service_id,
            .holder_node_id = who.party.node_id,
            .granted_at_ns = wall_ns,
        };
        const corr = self.nextCorr();
        self.phase = .{ .grant = .{ .corr = corr, .grant = grant, .who = who, .reply_kind = reply_kind, .config = view.config } };
        self.phase_started_ns = now_ns;
        out.push(.{ .append_grant = .{ .store_id = self.store_id, .corr = corr, .grant = grant } });
    }

    fn startWantedRelease(self: *StoreLease, view: View, now_ns: i64, out: *Outbox) void {
        const wr = self.want_release orelse return;
        self.want_release = null;
        const h = view.holder orelse return;
        if (h.epoch != wr.epoch) return; // already released / superseded
        const corr = self.nextCorr();
        self.phase = .{ .release = .{ .corr = corr, .epoch = h.epoch } };
        self.phase_started_ns = now_ns;
        out.push(.{ .append_release = .{ .store_id = self.store_id, .corr = corr, .release = .{ .writer_epoch = h.epoch, .reason = wr.reason } } });
        if (wr.notify) |n| self.revoke(out, n, .holder_timeout, h.epoch, 0);
    }

    fn stepNormal(self: *StoreLease, view: View, now_ns: i64, wall_ns: u64, cfg: Config, out: *Outbox) void {
        if (!out.room(6)) return;

        // 1. Dead services reported since the last step.
        var gi: usize = 0;
        while (gi < self.gone_len) : (gi += 1) {
            const p = self.gone[gi];
            self.removeWaitersOf(p);
            if (view.holder) |h| {
                if (h.node_id == p.node_id and h.service_id == p.service_id and self.want_release == null) {
                    self.want_release = .{
                        .epoch = h.epoch,
                        .reason = release_holder_died,
                        .notify = .{ .party = p, .nonce = h.nonce },
                    };
                }
            }
        }
        self.gone_len = 0;

        // 2. Registry-based backstop.
        if (view.holder) |h| {
            if (!view.holder_registered) {
                if (self.holder_missing_since_ns == 0) {
                    self.holder_missing_since_ns = now_ns;
                } else if (now_ns - self.holder_missing_since_ns >= cfg.holder_grace_ns and self.want_release == null) {
                    self.want_release = .{
                        .epoch = h.epoch,
                        .reason = release_holder_died,
                        .notify = .{ .party = .{ .node_id = h.node_id, .service_id = h.service_id }, .nonce = h.nonce },
                    };
                }
            } else self.holder_missing_since_ns = 0;
        } else self.holder_missing_since_ns = 0;

        self.startWantedRelease(view, now_ns, out);
        if (self.phase != .idle) return;

        // 3. Free lease and waiters: FIFO head gets it before any new request.
        if (view.holder == null and self.waiters_len > 0) {
            const w = self.waiters[0];
            self.removeWaiter(0);
            self.startGrant(w.who, view, .writer_granted, now_ns, wall_ns, out);
            return;
        }

        // 4. Requests, oldest first, until one starts an append.
        while (self.inbox_len > 0 and self.phase == .idle and out.room(6)) {
            const r = self.inbox[0];
            self.removeInbox(0);
            self.handle(r, view, now_ns, wall_ns, out);
            self.startWantedRelease(view, now_ns, out);
        }
    }

    fn handle(self: *StoreLease, r: Request, view: View, now_ns: i64, wall_ns: u64, out: *Outbox) void {
        switch (r.kind) {
            .register => self.handleRegister(r, view, now_ns, wall_ns, out),
            .reassert => self.handleReassert(r, view, now_ns, wall_ns, out),
            .unregister => {
                if (self.findWaiter(r.who.nonce)) |i| self.removeWaiter(i);
                if (view.holder) |h| {
                    // Epoch 0: the session closed before it learned any epoch
                    // (its open failed, timed out, or it was still queued), so
                    // a grant to that nonce was issued for a request it has
                    // abandoned. Nonces are never reused, so release it.
                    // A non-zero epoch must match: a delayed unregister cannot
                    // release a later same-nonce reasserted grant.
                    const epoch_ok = r.epoch == 0 or h.epoch == r.epoch;
                    if (h.nonce == r.who.nonce and epoch_ok and self.want_release == null) {
                        self.want_release = .{ .epoch = h.epoch, .reason = release_graceful };
                    }
                }
            },
        }
    }

    fn handleRegister(self: *StoreLease, r: Request, view: View, now_ns: i64, wall_ns: u64, out: *Outbox) void {
        if (!r.config.eqlIgnoringMapSize(view.config)) {
            self.replyTo(out, r, .config_mismatch, 0, view.config);
            return;
        }
        if (view.holder) |h| {
            if (h.nonce == r.who.nonce) { // idempotent reconnect
                self.replyTo(out, r, .ok, h.epoch, view.config);
                return;
            }
            self.queueOrReject(r, view, now_ns, out);
            return;
        }
        // Lease is free.
        if (self.waiters_len > 0) {
            // Waiters were served first in stepNormal; reaching here means a
            // grant is pending behind them.
            self.queueOrReject(r, view, now_ns, out);
            return;
        }
        self.startGrant(r.who, view, .writer_response, now_ns, wall_ns, out);
    }

    fn queueOrReject(self: *StoreLease, r: Request, view: View, now_ns: i64, out: *Outbox) void {
        if (self.findWaiter(r.who.nonce)) |i| {
            self.waiters[i].who = r.who; // refresh party
            self.replyTo(out, r, .queued, 0, view.config);
            return;
        }
        if (r.mode == .wait and self.pushWaiter(r.who, now_ns)) {
            self.replyTo(out, r, .queued, 0, view.config);
        } else {
            self.replyTo(out, r, .held_by_other, 0, view.config);
        }
    }

    fn handleReassert(self: *StoreLease, r: Request, view: View, now_ns: i64, wall_ns: u64, out: *Outbox) void {
        if (view.holder) |h| {
            if (h.nonce == r.who.nonce) {
                // Confirm (the client may be told a different epoch than it had).
                self.replyTo(out, r, .ok, h.epoch, view.config);
            } else {
                self.revoke(out, r.who, .superseded, r.epoch, h.epoch);
            }
            return;
        }
        if (self.waiters_len > 0) {
            self.revoke(out, r.who, .superseded, r.epoch, 0);
            return;
        }
        // Grant lost in the failover and nobody else wants the lease: re-grant
        // with a fresh epoch. The writer learns the new epoch from the response.
        self.startGrant(r.who, view, .reassert_response, now_ns, wall_ns, out);
    }
};

pub const SubmitResult = enum { accepted, store_limit, inbox_full };

/// Preallocated per-store state, keyed by `store_id`.
pub const LeaseTable = struct {
    allocator: std.mem.Allocator,
    slots: []StoreLease,
    used: u32 = 0,
    cfg: Config,

    pub fn init(allocator: std.mem.Allocator, max_stores: u32, cfg: Config) !LeaseTable {
        const slots = try allocator.alloc(StoreLease, max_stores);
        for (slots, 0..) |*s, i| {
            s.* = .{};
            s.slot_tag = @intCast(i + 1);
        }
        return .{ .allocator = allocator, .slots = slots, .cfg = cfg };
    }

    pub fn deinit(self: *LeaseTable) void {
        self.allocator.free(self.slots);
    }

    pub fn find(self: *LeaseTable, store_id: u64) ?*StoreLease {
        for (self.slots) |*s| if (s.used and s.store_id == store_id) return s;
        return null;
    }

    pub fn getOrCreate(self: *LeaseTable, store_id: u64, store_name: []const u8) ?*StoreLease {
        if (self.find(store_id)) |s| return s;
        if (store_name.len > msgs.kv_name_max) return null;
        for (self.slots) |*s| {
            if (s.used) continue;
            const tag = s.slot_tag;
            s.* = .{ .used = true, .store_id = store_id, .name_len = @intCast(store_name.len), .slot_tag = tag };
            @memcpy(s.name[0..store_name.len], store_name);
            self.used += 1;
            return s;
        }
        return null;
    }

    pub fn submit(self: *LeaseTable, store_id: u64, store_name: []const u8, req: Request) SubmitResult {
        const s = self.getOrCreate(store_id, store_name) orelse return .store_limit;
        return if (s.submit(req)) .accepted else .inbox_full;
    }

    /// A service disappeared: fan out to every store.
    pub fn serviceGone(self: *LeaseTable, party: Party) void {
        for (self.slots) |*s| if (s.used) s.serviceGone(party);
    }

    pub fn onAppendDone(self: *LeaseTable, store_id: u64, corr: u64, ok: bool, index: u64, now_ns: i64, out: *Outbox) void {
        if (self.find(store_id)) |s| s.onAppendDone(corr, ok, index, now_ns, out);
    }
};

// ═════════════════════════════════════════════════════════════════════════════
// Tests: lease logic over a simulated log (tracker `StoreTrack` + executor).
// ═════════════════════════════════════════════════════════════════════════════

const testing = std.testing;

const Sim = struct {
    table: LeaseTable,
    track: tracker.StoreTrack = .{},
    store_id: u64 = 42,
    next_index: u64 = 0,
    now: i64 = 1_000_000_000,
    cfg: Config = .{},
    ready: bool = true,
    holder_registered: bool = true,
    /// Fail the next N appends.
    fail_appends: u32 = 0,
    /// Complete appends only when `flush()` is called (to test in-flight state).
    defer_done: bool = false,
    pending: std.ArrayList(Action) = .empty,
    replies: std.ArrayList(Reply) = .empty,
    appended_grants: u32 = 0,
    appended_releases: u32 = 0,
    appended_headers: u32 = 0,
    buf: [256]u8 = undefined,

    fn init() !Sim {
        return .{ .table = try LeaseTable.init(testing.allocator, 4, .{}) };
    }
    fn deinit(self: *Sim) void {
        self.pending.deinit(testing.allocator);
        self.replies.deinit(testing.allocator);
        self.table.deinit();
    }

    fn view(self: *Sim) View {
        return .{
            .ready = self.ready,
            .header_seen = self.track.config != null,
            .config = self.track.config orelse .{},
            .holder = self.track.holder(),
            .last_epoch = self.track.lastEpoch(),
            .tracked_index = self.track.tracked_index,
            .holder_registered = self.holder_registered,
        };
    }

    fn party(node: u16, svc: u32) Party {
        return .{ .node_id = node, .service_id = svc };
    }
    fn who(node: u16, svc: u32, nonce: u64) Requester {
        return .{ .party = party(node, svc), .nonce = nonce };
    }

    fn register(self: *Sim, w: Requester, mode: AcquireMode, cfg: KvStoreConfig) !void {
        const r = self.table.submit(self.store_id, "s", .{ .kind = .register, .who = w, .mode = mode, .config = cfg, .received_ns = self.now });
        try testing.expectEqual(SubmitResult.accepted, r);
    }
    fn reassert(self: *Sim, w: Requester, epoch: u64) !void {
        const r = self.table.submit(self.store_id, "s", .{ .kind = .reassert, .who = w, .epoch = epoch, .received_ns = self.now });
        try testing.expectEqual(SubmitResult.accepted, r);
    }
    fn unregister(self: *Sim, w: Requester, epoch: u64) !void {
        const r = self.table.submit(self.store_id, "s", .{ .kind = .unregister, .who = w, .epoch = epoch, .received_ns = self.now });
        try testing.expectEqual(SubmitResult.accepted, r);
    }

    fn appendEntry(self: *Sim, frame: []const u8) u64 {
        self.next_index += 1;
        self.track.onEntry(self.next_index, frame);
        return self.next_index;
    }

    /// Execute one action as the control loop + topic engine + tracker would.
    fn exec(self: *Sim, a: Action, out: *Outbox) !void {
        switch (a) {
            .reply => |r| try self.replies.append(testing.allocator, r),
            .create_store => |c| {
                if (self.takeFail()) return self.table.onAppendDone(c.store_id, c.corr, false, 0, self.now, out);
                const idx = self.appendEntry(try kv.format.encodeStoreHeader(&self.buf, 1, c.config));
                self.appended_headers += 1;
                self.table.onAppendDone(c.store_id, c.corr, true, idx, self.now, out);
            },
            .append_grant => |g| {
                if (self.takeFail()) return self.table.onAppendDone(g.store_id, g.corr, false, 0, self.now, out);
                const idx = self.appendEntry(try kv.format.encodeWriterGrant(&self.buf, 1, g.grant));
                self.appended_grants += 1;
                self.table.onAppendDone(g.store_id, g.corr, true, idx, self.now, out);
            },
            .append_release => |g| {
                if (self.takeFail()) return self.table.onAppendDone(g.store_id, g.corr, false, 0, self.now, out);
                const idx = self.appendEntry(try kv.format.encodeWriterRelease(&self.buf, 1, g.release));
                self.appended_releases += 1;
                self.table.onAppendDone(g.store_id, g.corr, true, idx, self.now, out);
            },
        }
    }
    fn takeFail(self: *Sim) bool {
        if (self.fail_appends == 0) return false;
        self.fail_appends -= 1;
        return true;
    }

    /// One control-loop pass: step, execute actions (which may complete
    /// appends and emit more actions), repeat until quiescent.
    fn tick(self: *Sim) !void {
        var rounds: u32 = 0;
        while (rounds < 16) : (rounds += 1) {
            var out = Outbox{};
            if (self.table.find(self.store_id)) |s| s.step(self.view(), self.now, 7, self.cfg, &out);
            if (out.len == 0) return;
            var i: usize = 0;
            while (i < out.len) : (i += 1) {
                var more = Outbox{};
                try self.exec(out.items[i], &more);
                // Completion may emit replies; run them too.
                for (more.slice()) |m| try self.exec(m, &more);
            }
        }
    }

    fn advance(self: *Sim, ns: i64) void {
        self.now += ns;
    }

    fn lastReply(self: *Sim) Reply {
        return self.replies.items[self.replies.items.len - 1];
    }
    fn clearReplies(self: *Sim) void {
        self.replies.clearRetainingCapacity();
    }
};

const cfg_a = KvStoreConfig{};

test "acquire: creates the store (header) then grants; GRANTED only after the grant append" {
    var sim = try Sim.init();
    defer sim.deinit();
    const w1 = Sim.who(1, 10, 0x111);
    try sim.register(w1, .fail_if_held, cfg_a);
    try sim.tick();

    try testing.expectEqual(@as(u32, 1), sim.appended_headers);
    try testing.expectEqual(@as(u32, 1), sim.appended_grants);
    try testing.expectEqual(@as(usize, 1), sim.replies.items.len);
    const r = sim.lastReply();
    try testing.expectEqual(ReplyKind.writer_response, r.kind);
    try testing.expectEqual(KvStatus.ok, r.status);
    try testing.expectEqual(@as(u64, 1), r.epoch);
    try testing.expectEqual(@as(u64, 0x111), r.nonce);
    try testing.expectEqual(Sim.party(1, 10), r.to);
    // The log holds [header, grant]; holder recorded with requester identity.
    const h = sim.track.holder().?;
    try testing.expectEqual(@as(u64, 0x111), h.nonce);
    try testing.expectEqual(@as(u32, 10), h.service_id);
    try testing.expectEqual(@as(u16, 1), h.node_id);
    try testing.expectEqual(@as(u64, 7), h.granted_at_ns);
    try testing.expectEqual(@as(u64, 2), sim.next_index);
}

test "grant is not replied while the append is in flight" {
    var sim = try Sim.init();
    defer sim.deinit();
    sim.track.onEntry(1, try kv.format.encodeStoreHeader(&sim.buf, 1, cfg_a));
    sim.next_index = 1;
    try sim.register(Sim.who(1, 1, 5), .fail_if_held, cfg_a);
    var out = Outbox{};
    sim.table.find(sim.store_id).?.step(sim.view(), sim.now, 0, sim.cfg, &out);
    try testing.expectEqual(@as(usize, 1), out.len);
    try testing.expect(out.items[0] == .append_grant);
    // Nothing replied yet, and a further step does nothing (one op at a time).
    var out2 = Outbox{};
    sim.table.find(sim.store_id).?.step(sim.view(), sim.now, 0, sim.cfg, &out2);
    try testing.expectEqual(@as(usize, 0), out2.len);
    // Completion yields the reply.
    var out3 = Outbox{};
    sim.table.onAppendDone(sim.store_id, out.items[0].append_grant.corr, true, 2, sim.now, &out3);
    try testing.expectEqual(@as(usize, 1), out3.len);
    try testing.expectEqual(KvStatus.ok, out3.items[0].reply.status);
}

test "idempotent re-register by the same nonce returns the same epoch without a new grant" {
    var sim = try Sim.init();
    defer sim.deinit();
    const w = Sim.who(1, 10, 0xA);
    try sim.register(w, .fail_if_held, cfg_a);
    try sim.tick();
    try sim.register(Sim.who(1, 10, 0xA), .fail_if_held, cfg_a);
    try sim.tick();
    try testing.expectEqual(@as(u32, 1), sim.appended_grants);
    try testing.expectEqual(@as(usize, 2), sim.replies.items.len);
    try testing.expectEqual(KvStatus.ok, sim.lastReply().status);
    try testing.expectEqual(@as(u64, 1), sim.lastReply().epoch);
}

test "fail_if_held: second writer gets HELD_BY_OTHER" {
    var sim = try Sim.init();
    defer sim.deinit();
    try sim.register(Sim.who(1, 10, 0xA), .fail_if_held, cfg_a);
    try sim.tick();
    sim.clearReplies();
    try sim.register(Sim.who(2, 20, 0xB), .fail_if_held, cfg_a);
    try sim.tick();
    try testing.expectEqual(KvStatus.held_by_other, sim.lastReply().status);
    try testing.expectEqual(@as(u32, 0), sim.table.find(sim.store_id).?.waiterCount());
    try testing.expectEqual(@as(u32, 1), sim.appended_grants);
}

test "config mismatch ignores map_size_hint but not other fields" {
    var sim = try Sim.init();
    defer sim.deinit();
    try sim.register(Sim.who(1, 10, 0xA), .fail_if_held, cfg_a);
    try sim.tick();
    sim.clearReplies();

    var diff_hint = cfg_a;
    diff_hint.map_size_hint = 12345;
    try sim.register(Sim.who(2, 20, 0xB), .fail_if_held, diff_hint);
    try sim.tick();
    // Same config modulo hint: not a mismatch (held by the first writer).
    try testing.expectEqual(KvStatus.held_by_other, sim.lastReply().status);

    var other = cfg_a;
    other.max_value_len = 1234;
    try sim.register(Sim.who(3, 30, 0xC), .wait, other);
    try sim.tick();
    const r = sim.lastReply();
    try testing.expectEqual(KvStatus.config_mismatch, r.status);
    try testing.expectEqual(cfg_a.max_value_len, r.config.max_value_len); // effective config from the log
    try testing.expectEqual(@as(u32, 0), sim.table.find(sim.store_id).?.waiterCount());
}

test "wait queue is FIFO; release grants the next waiter asynchronously" {
    var sim = try Sim.init();
    defer sim.deinit();
    const a = Sim.who(1, 1, 0xA);
    const b = Sim.who(2, 2, 0xB);
    const c = Sim.who(3, 3, 0xC);
    try sim.register(a, .fail_if_held, cfg_a);
    try sim.tick();
    try sim.register(b, .wait, cfg_a);
    try sim.tick();
    try testing.expectEqual(KvStatus.queued, sim.lastReply().status);
    try sim.register(c, .wait, cfg_a);
    try sim.tick();
    try testing.expectEqual(KvStatus.queued, sim.lastReply().status);
    // Retrying a queued registration is idempotent (still queued once).
    try sim.register(b, .wait, cfg_a);
    try sim.tick();
    try testing.expectEqual(KvStatus.queued, sim.lastReply().status);
    try testing.expectEqual(@as(u32, 2), sim.table.find(sim.store_id).?.waiterCount());
    sim.clearReplies();

    // A graceful release by A grants B (not C), with an async GRANTED.
    try sim.unregister(a, 1);
    try sim.tick();
    try testing.expectEqual(@as(u32, 1), sim.appended_releases);
    try testing.expectEqual(@as(u32, 2), sim.appended_grants);
    try testing.expectEqual(@as(usize, 1), sim.replies.items.len);
    var r = sim.lastReply();
    try testing.expectEqual(ReplyKind.writer_granted, r.kind);
    try testing.expectEqual(@as(u64, 0xB), r.nonce);
    try testing.expectEqual(@as(u64, 2), r.epoch);
    try testing.expectEqual(@as(u64, 0xB), sim.track.holder().?.nonce);
    try testing.expectEqual(@as(u32, 1), sim.table.find(sim.store_id).?.waiterCount());

    // Then B releases: C is granted epoch 3.
    sim.clearReplies();
    try sim.unregister(b, 2);
    try sim.tick();
    r = sim.lastReply();
    try testing.expectEqual(ReplyKind.writer_granted, r.kind);
    try testing.expectEqual(@as(u64, 0xC), r.nonce);
    try testing.expectEqual(@as(u64, 3), r.epoch);

    // C releases with nobody waiting: lease is free, then a fresh writer gets epoch 4.
    try sim.unregister(c, 3);
    try sim.tick();
    try testing.expect(sim.track.holder() == null);
    try sim.register(Sim.who(4, 4, 0xD), .fail_if_held, cfg_a);
    try sim.tick();
    try testing.expectEqual(@as(u64, 4), sim.lastReply().epoch);
}

test "a new request cannot jump ahead of queued waiters when the lease frees up" {
    var sim = try Sim.init();
    defer sim.deinit();
    const a = Sim.who(1, 1, 0xA);
    const b = Sim.who(2, 2, 0xB);
    try sim.register(a, .fail_if_held, cfg_a);
    try sim.tick();
    try sim.register(b, .wait, cfg_a);
    try sim.tick();
    sim.clearReplies();
    // Release and a newcomer arrive in the same pass: waiter B wins.
    try sim.unregister(a, 1);
    try sim.register(Sim.who(9, 9, 0xE), .fail_if_held, cfg_a);
    try sim.tick();
    try testing.expectEqual(@as(u64, 0xB), sim.track.holder().?.nonce);
    // The newcomer was told the lease is held.
    var saw_held = false;
    for (sim.replies.items) |r| {
        if (r.nonce == 0xE) {
            try testing.expectEqual(KvStatus.held_by_other, r.status);
            saw_held = true;
        }
    }
    try testing.expect(saw_held);
}

test "unregister of a queued waiter removes it without touching the lease" {
    var sim = try Sim.init();
    defer sim.deinit();
    const a = Sim.who(1, 1, 0xA);
    const b = Sim.who(2, 2, 0xB);
    try sim.register(a, .fail_if_held, cfg_a);
    try sim.tick();
    try sim.register(b, .wait, cfg_a);
    try sim.tick();
    try sim.unregister(b, 0);
    try sim.tick();
    try testing.expectEqual(@as(u32, 0), sim.table.find(sim.store_id).?.waiterCount());
    try testing.expectEqual(@as(u64, 0xA), sim.track.holder().?.nonce);
    try testing.expectEqual(@as(u32, 0), sim.appended_releases);
}

test "holder death (serviceGone) releases, revokes the old holder, and grants the next waiter" {
    var sim = try Sim.init();
    defer sim.deinit();
    const a = Sim.who(1, 1, 0xA);
    const b = Sim.who(2, 2, 0xB);
    try sim.register(a, .fail_if_held, cfg_a);
    try sim.tick();
    try sim.register(b, .wait, cfg_a);
    try sim.tick();
    sim.clearReplies();

    sim.table.serviceGone(a.party);
    try sim.tick();
    try testing.expectEqual(@as(u32, 1), sim.appended_releases);
    try testing.expectEqual(@as(u64, 0xB), sim.track.holder().?.nonce);
    var revoked = false;
    var granted = false;
    for (sim.replies.items) |r| {
        if (r.kind == .writer_revoked) {
            revoked = true;
            try testing.expectEqual(a.party, r.to);
            try testing.expectEqual(RevokeReason.holder_timeout, r.reason);
            try testing.expectEqual(@as(u64, 1), r.epoch);
        }
        if (r.kind == .writer_granted) {
            granted = true;
            try testing.expectEqual(@as(u64, 2), r.epoch);
        }
    }
    try testing.expect(revoked and granted);
}

test "a dead waiter is dropped and never granted" {
    var sim = try Sim.init();
    defer sim.deinit();
    const a = Sim.who(1, 1, 0xA);
    const b = Sim.who(2, 2, 0xB);
    const c = Sim.who(3, 3, 0xC);
    try sim.register(a, .fail_if_held, cfg_a);
    try sim.tick();
    try sim.register(b, .wait, cfg_a);
    try sim.register(c, .wait, cfg_a);
    try sim.tick();
    sim.table.serviceGone(b.party);
    sim.table.serviceGone(a.party);
    try sim.tick();
    try testing.expectEqual(@as(u64, 0xC), sim.track.holder().?.nonce);
}

test "registry backstop: holder missing beyond the grace period is released" {
    var sim = try Sim.init();
    defer sim.deinit();
    try sim.register(Sim.who(1, 1, 0xA), .fail_if_held, cfg_a);
    try sim.tick();
    sim.holder_registered = false;
    try sim.tick(); // starts the clock
    try testing.expect(sim.track.holder() != null);
    sim.advance(sim.cfg.holder_grace_ns - 1);
    try sim.tick();
    try testing.expect(sim.track.holder() != null);
    sim.advance(2);
    try sim.tick();
    try testing.expect(sim.track.holder() == null);
    try testing.expectEqual(@as(u32, 1), sim.appended_releases);

    // A flapping registry (holder reappears) resets the timer.
    try sim.register(Sim.who(2, 2, 0xB), .fail_if_held, cfg_a);
    try sim.tick();
    sim.holder_registered = false;
    try sim.tick();
    sim.advance(sim.cfg.holder_grace_ns - 1);
    sim.holder_registered = true;
    try sim.tick();
    sim.holder_registered = false;
    try sim.tick();
    sim.advance(sim.cfg.holder_grace_ns - 1);
    try sim.tick();
    try testing.expect(sim.track.holder() != null);
}

test "reassert after leader failover: confirm when the log still has the grant" {
    var sim = try Sim.init();
    defer sim.deinit();
    const a = Sim.who(1, 1, 0xA);
    try sim.register(a, .fail_if_held, cfg_a);
    try sim.tick();
    sim.clearReplies();
    // New leader: fresh lease table, same log.
    sim.table.deinit();
    sim.table = try LeaseTable.init(testing.allocator, 4, .{});
    try sim.reassert(a, 1);
    try sim.tick();
    const r = sim.lastReply();
    try testing.expectEqual(ReplyKind.reassert_response, r.kind);
    try testing.expectEqual(KvStatus.ok, r.status);
    try testing.expectEqual(@as(u64, 1), r.epoch);
    try testing.expectEqual(@as(u32, 1), sim.appended_grants); // no new grant
}

test "reassert: grant lost in the failover => new epoch (last+1), reported to the writer" {
    var sim = try Sim.init();
    defer sim.deinit();
    // The surviving log has the header and grant epoch 1 that was released earlier;
    // writer B's epoch-2 grant never replicated.
    sim.track.onEntry(1, try kv.format.encodeStoreHeader(&sim.buf, 1, cfg_a));
    sim.track.onEntry(2, try kv.format.encodeWriterGrant(&sim.buf, 1, .{ .writer_epoch = 1, .holder_session_nonce = 0xA, .holder_service_id = 1, .holder_node_id = 1, .granted_at_ns = 0 }));
    sim.track.onEntry(3, try kv.format.encodeWriterRelease(&sim.buf, 1, .{ .writer_epoch = 1, .reason = 0 }));
    sim.next_index = 3;
    const b = Sim.who(2, 2, 0xB);
    try sim.reassert(b, 2);
    try sim.tick();
    const r = sim.lastReply();
    try testing.expectEqual(ReplyKind.reassert_response, r.kind);
    try testing.expectEqual(KvStatus.ok, r.status);
    try testing.expectEqual(@as(u64, 2), r.epoch);
    try testing.expectEqual(@as(u64, 0xB), sim.track.holder().?.nonce);
}

test "reassert: a different holder exists => KvWriterRevoked(superseded) with the current epoch" {
    var sim = try Sim.init();
    defer sim.deinit();
    const a = Sim.who(1, 1, 0xA);
    try sim.register(a, .fail_if_held, cfg_a);
    try sim.tick();
    sim.clearReplies();
    const stale = Sim.who(5, 5, 0x5);
    try sim.reassert(stale, 7);
    try sim.tick();
    const r = sim.lastReply();
    try testing.expectEqual(ReplyKind.writer_revoked, r.kind);
    try testing.expectEqual(RevokeReason.superseded, r.reason);
    try testing.expectEqual(@as(u64, 7), r.epoch);
    try testing.expectEqual(@as(u64, 1), r.current_epoch);
    try testing.expectEqual(stale.party, r.to);
    try testing.expectEqual(@as(u64, 0xA), sim.track.holder().?.nonce);
}

test "reassert on a store whose header was lost => revoked(lease_lost)" {
    var sim = try Sim.init();
    defer sim.deinit();
    try sim.reassert(Sim.who(1, 1, 0xA), 1);
    try sim.tick();
    const r = sim.lastReply();
    try testing.expectEqual(ReplyKind.writer_revoked, r.kind);
    try testing.expectEqual(RevokeReason.lease_lost, r.reason);
    try testing.expectEqual(@as(u32, 0), sim.appended_headers);
}

test "reassert with queued waiters and a free lease is revoked, waiter wins" {
    var sim = try Sim.init();
    defer sim.deinit();
    sim.track.onEntry(1, try kv.format.encodeStoreHeader(&sim.buf, 1, cfg_a));
    sim.next_index = 1;
    // Waiter present (registered during the failover window), no holder.
    const t = sim.table.getOrCreate(sim.store_id, "s").?;
    try testing.expect(t.pushWaiter(Sim.who(2, 2, 0xB), 0));
    // Make the reassert reach handle() before waiters-first runs: not possible in
    // step(); so assert the waiters-first path grants B and A is then revoked.
    try sim.reassert(Sim.who(1, 1, 0xA), 1);
    try sim.tick();
    try testing.expectEqual(@as(u64, 0xB), sim.track.holder().?.nonce);
    const r = sim.lastReply();
    try testing.expectEqual(ReplyKind.writer_revoked, r.kind);
}

test "not ready (barrier / tracker behind): requests wait, then time out with NOT_READY" {
    var sim = try Sim.init();
    defer sim.deinit();
    sim.ready = false;
    try sim.register(Sim.who(1, 1, 0xA), .fail_if_held, cfg_a);
    try sim.tick();
    try testing.expectEqual(@as(usize, 0), sim.replies.items.len);
    try testing.expectEqual(@as(u32, 0), sim.appended_headers);
    sim.advance(sim.cfg.request_timeout_ns + 1);
    try sim.tick();
    try testing.expectEqual(KvStatus.not_ready, sim.lastReply().status);
    try testing.expectEqual(@as(u32, 0), sim.table.find(sim.store_id).?.inboxCount());

    // Becoming ready processes a fresh request.
    sim.ready = true;
    try sim.register(Sim.who(1, 1, 0xA), .fail_if_held, cfg_a);
    try sim.tick();
    try testing.expectEqual(KvStatus.ok, sim.lastReply().status);
}

test "decisions wait for the tracker to consume the leader's own appends" {
    var sim = try Sim.init();
    defer sim.deinit();
    sim.track.onEntry(1, try kv.format.encodeStoreHeader(&sim.buf, 1, cfg_a));
    sim.next_index = 1;
    const s = sim.table.getOrCreate(sim.store_id, "s").?;
    s.await_index = 5; // our grant at index 5 not yet seen by the tracker
    try sim.register(Sim.who(1, 1, 0xA), .fail_if_held, cfg_a);
    var out = Outbox{};
    s.step(sim.view(), sim.now, 0, sim.cfg, &out);
    try testing.expectEqual(@as(usize, 0), out.len);
    sim.track.tracked_index = 5;
    s.step(sim.view(), sim.now, 0, sim.cfg, &out);
    try testing.expectEqual(@as(usize, 1), out.len);
}

test "append failure: synchronous caller gets NOT_READY and may retry; waiter keeps its place" {
    var sim = try Sim.init();
    defer sim.deinit();
    sim.track.onEntry(1, try kv.format.encodeStoreHeader(&sim.buf, 1, cfg_a));
    sim.next_index = 1;
    sim.fail_appends = 1;
    try sim.register(Sim.who(1, 1, 0xA), .fail_if_held, cfg_a);
    try sim.tick();
    try testing.expectEqual(KvStatus.not_ready, sim.lastReply().status);
    try testing.expect(sim.track.holder() == null);
    // Retry after the back-off.
    sim.advance(sim.cfg.retry_ns + 1);
    try sim.register(Sim.who(1, 1, 0xA), .fail_if_held, cfg_a);
    try sim.tick();
    try testing.expectEqual(KvStatus.ok, sim.lastReply().status);

    // Waiter whose async grant fails is re-queued at the head.
    const b = Sim.who(2, 2, 0xB);
    const c = Sim.who(3, 3, 0xC);
    try sim.register(b, .wait, cfg_a);
    try sim.register(c, .wait, cfg_a);
    try sim.tick();
    sim.fail_appends = 0;
    try sim.unregister(Sim.who(1, 1, 0xA), sim.track.holder().?.epoch);
    // release ok, then the grant append fails once:
    var out = Outbox{};
    sim.table.find(sim.store_id).?.step(sim.view(), sim.now, 0, sim.cfg, &out);
    try sim.exec(out.items[0], &out); // release appended
    out.clear();
    sim.fail_appends = 1;
    sim.table.find(sim.store_id).?.step(sim.view(), sim.now, 0, sim.cfg, &out);
    var more = Outbox{};
    try sim.exec(out.items[0], &more); // grant to B fails
    try testing.expectEqual(@as(u32, 2), sim.table.find(sim.store_id).?.waiterCount());
    sim.advance(sim.cfg.retry_ns + 1);
    try sim.tick();
    try testing.expectEqual(@as(u64, 0xB), sim.track.holder().?.nonce); // B still first
}

test "stuck append is failed after the append timeout" {
    var sim = try Sim.init();
    defer sim.deinit();
    sim.track.onEntry(1, try kv.format.encodeStoreHeader(&sim.buf, 1, cfg_a));
    sim.next_index = 1;
    try sim.register(Sim.who(1, 1, 0xA), .fail_if_held, cfg_a);
    var out = Outbox{};
    const s = sim.table.find(sim.store_id).?;
    s.step(sim.view(), sim.now, 0, sim.cfg, &out); // grant started, never completed
    sim.advance(sim.cfg.append_timeout_ns + 1);
    var out2 = Outbox{};
    s.step(sim.view(), sim.now, 0, sim.cfg, &out2);
    try testing.expect(s.isIdle());
    try testing.expectEqual(@as(usize, 1), out2.len);
    try testing.expectEqual(KvStatus.not_ready, out2.items[0].reply.status);
    // A late completion of the old append is ignored.
    var out3 = Outbox{};
    s.onAppendDone(out.items[0].append_grant.corr, true, 9, sim.now, &out3);
    try testing.expectEqual(@as(usize, 0), out3.len);
}

test "table: store limit, inbox dedupe and overflow" {
    var table = try LeaseTable.init(testing.allocator, 2, .{});
    defer table.deinit();
    const w = Requester{ .party = .{ .node_id = 1, .service_id = 1 }, .nonce = 1 };
    const req = Request{ .kind = .register, .who = w };
    try testing.expectEqual(SubmitResult.accepted, table.submit(1, "a", req));
    try testing.expectEqual(SubmitResult.accepted, table.submit(2, "b", req));
    try testing.expectEqual(SubmitResult.store_limit, table.submit(3, "c", req));
    try testing.expectEqual(SubmitResult.accepted, table.submit(1, "a", req)); // same nonce replaces
    try testing.expectEqual(@as(u32, 1), table.find(1).?.inboxCount());
    var i: u64 = 2;
    while (i <= inbox_cap) : (i += 1) {
        var r = req;
        r.who.nonce = i;
        try testing.expectEqual(SubmitResult.accepted, table.submit(1, "a", r));
    }
    var r2 = req;
    r2.who.nonce = 999;
    try testing.expectEqual(SubmitResult.inbox_full, table.submit(1, "a", r2));
    // Distinct slots have distinct correlation tags.
    try testing.expect(table.find(1).?.slot_tag != table.find(2).?.slot_tag);
}

test "epochs never reuse: issued_epoch covers appends the tracker has not read yet" {
    var sim = try Sim.init();
    defer sim.deinit();
    sim.track.onEntry(1, try kv.format.encodeStoreHeader(&sim.buf, 1, cfg_a));
    sim.next_index = 1;
    const s = sim.table.getOrCreate(sim.store_id, "s").?;
    s.issued_epoch = 6;
    try sim.register(Sim.who(1, 1, 0xA), .fail_if_held, cfg_a);
    var out = Outbox{};
    s.step(sim.view(), sim.now, 0, sim.cfg, &out);
    try testing.expectEqual(@as(u64, 7), out.items[0].append_grant.grant.writer_epoch);
}

test "abandon answers pending requests with leader_unavailable and clears leader memory" {
    var sim = try Sim.init();
    defer sim.deinit();
    try sim.register(Sim.who(1, 1, 0xA), .fail_if_held, cfg_a);
    try sim.tick();
    try sim.register(Sim.who(2, 2, 0xB), .wait, cfg_a);
    try sim.tick();
    try sim.register(Sim.who(3, 3, 0xC), .wait, cfg_a);
    sim.ready = false;
    const s = sim.table.find(sim.store_id).?;
    try testing.expect(s.needsStep());
    var out = Outbox{};
    s.abandon(&out);
    try testing.expectEqual(@as(usize, 1), out.len);
    try testing.expectEqual(KvStatus.leader_unavailable, out.items[0].reply.status);
    try testing.expect(!s.needsStep());
}

test "delayed unregister cannot release a same-nonce reasserted grant" {
    var sim = try Sim.init();
    defer sim.deinit();
    const writer = Sim.who(1, 1, 0xA);
    try sim.register(writer, .fail_if_held, cfg_a);
    try sim.tick();
    try sim.unregister(writer, 1);
    try sim.tick();
    try sim.reassert(writer, 1);
    try sim.tick();
    try testing.expectEqual(@as(u64, 2), sim.track.holder().?.epoch);
    const releases = sim.appended_releases;
    try sim.unregister(writer, 1);
    try sim.tick();
    try testing.expectEqual(releases, sim.appended_releases);
    try testing.expectEqual(@as(u64, 2), sim.track.holder().?.epoch);
    try sim.unregister(writer, 2);
    try sim.tick();
    try testing.expect(sim.track.holder() == null);
    try testing.expectEqual(releases + 1, sim.appended_releases);
}

test "abandoned open: epoch-0 unregister releases a grant the client never saw, so its retry is not HELD_BY_OTHER" {
    // Client open attempt #1 (nonce 0xA) sent a register that was granted, but
    // the client had already failed that attempt (e.g. LEADER_UNAVAILABLE
    // answered an earlier copy) and closed it with epoch 0. Attempt #2 uses a
    // fresh nonce from the same service and must get the lease.
    var sim = try Sim.init();
    defer sim.deinit();
    const stale = Sim.who(1, 10, 0xA);
    try sim.register(stale, .fail_if_held, cfg_a);
    try sim.tick();
    try testing.expectEqual(@as(u64, 0xA), sim.track.holder().?.nonce);
    try sim.unregister(stale, 0);
    try sim.register(Sim.who(1, 10, 0xB), .fail_if_held, cfg_a);
    try sim.tick();
    try sim.tick();
    try testing.expectEqual(@as(u32, 1), sim.appended_releases);
    try testing.expectEqual(KvStatus.ok, sim.lastReply().status);
    try testing.expectEqual(@as(u64, 0xB), sim.lastReply().nonce);
    try testing.expectEqual(@as(u64, 0xB), sim.track.holder().?.nonce);
    try testing.expectEqual(@as(u64, 2), sim.track.holder().?.epoch);
    // Real contention is still enforced against the new holder.
    try sim.register(Sim.who(1, 10, 0xC), .fail_if_held, cfg_a);
    try sim.tick();
    try testing.expectEqual(KvStatus.held_by_other, sim.lastReply().status);
}

test "unregister drops the closing session's pending register before it is granted" {
    var sim = try Sim.init();
    defer sim.deinit();
    sim.track.onEntry(1, try kv.format.encodeStoreHeader(&sim.buf, 1, cfg_a));
    sim.next_index = 1;
    const stale = Sim.who(1, 10, 0xA);
    try sim.register(stale, .fail_if_held, cfg_a);
    try sim.unregister(stale, 0);
    try sim.register(Sim.who(1, 10, 0xB), .fail_if_held, cfg_a);
    try sim.tick();
    try testing.expectEqual(@as(u32, 1), sim.appended_grants);
    try testing.expectEqual(@as(u32, 0), sim.appended_releases);
    try testing.expectEqual(@as(usize, 1), sim.replies.items.len);
    try testing.expectEqual(@as(u64, 0xB), sim.lastReply().nonce);
    try testing.expectEqual(KvStatus.ok, sim.lastReply().status);
}
