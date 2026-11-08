// SPDX-License-Identifier: Apache-2.0
const std = @import("std");
const common = @import("ringloom_common");
const format = common.kv.format;
const types = @import("kv_types.zig");
const Context = @import("kv_context.zig").KvContext;
const Publisher = @import("../topics/topic_publisher.zig").TopicPublisher;

/// One publishing thread per writer. Control updates are serialized with
/// mutations by the owning context. Batches survive back-pressure unchanged.
pub const KvWriter = struct {
    allocator: std.mem.Allocator,
    context: ?*Context,
    next: ?*KvWriter = null,
    name_buf: [64]u8 = undefined,
    name_len: usize,
    nonce: u64,
    config: types.KvStoreConfig,
    options: types.KvWriterOptions,
    publisher: Publisher,
    epoch: u64 = 0,
    writer_state: types.KvWriterState = .queued,
    response_status: ?u8 = null,
    next_retry_ns: i64 = 0,
    buffer: []u8,
    scratch: []u8,
    builder: format.DataFrameBuilder,
    batching: bool = false,
    batch_ack: types.AckMode = .fire_and_forget,

    pub fn name(self: *const KvWriter) []const u8 {
        return self.name_buf[0..self.name_len];
    }
    pub fn state(self: *const KvWriter) types.KvWriterState {
        const ctx = self.context orelse return .revoked;
        ctx.lock();
        defer ctx.unlock();
        return self.writer_state;
    }
    pub fn isAcked(self: *const KvWriter, seq: u64) bool {
        return seq != 0 and self.publisher.isAcked(seq);
    }
    pub fn put(self: *KvWriter, key: []const u8, value: []const u8, opts: types.KvPutOpts) types.KvWriteResult {
        return self.write(.put, key, value, opts.ttl_ms, opts.ack_mode);
    }
    pub fn delete(self: *KvWriter, key: []const u8, opts: types.KvWriteOpts) types.KvWriteResult {
        return self.write(.delete, key, &.{}, null, opts.ack_mode);
    }
    pub fn clear(self: *KvWriter, opts: types.KvWriteOpts) types.KvWriteResult {
        return self.write(.clear, &.{}, &.{}, null, opts.ack_mode);
    }
    fn readiness(self: *const KvWriter) ?types.KvWriteResult {
        return switch (self.writer_state) {
            .granted => if (self.epoch == 0) .not_writer else null,
            .queued, .revoked => .not_writer,
            .reasserting => .leader_unavailable,
        };
    }
    fn add(self: *KvWriter, op: format.KvOp, key: []const u8, value: []const u8, expiry: u64) format.DataFrameBuilder.Error!void {
        switch (op) {
            .put => try self.builder.addPut(key, value, expiry),
            .delete => try self.builder.addDelete(key),
            .clear => try self.builder.addClear(),
        }
    }
    fn write(self: *KvWriter, op: format.KvOp, key: []const u8, value: []const u8, ttl_ms: ?u64, ack: types.AckMode) types.KvWriteResult {
        const ctx = self.context orelse return .not_writer;
        ctx.lock();
        defer ctx.unlock();
        if (self.readiness()) |r| return r;
        const expiry = if (op == .put) format.expireAtFor(types.realtimeNs(.precise), ttl_ms orelse self.config.default_ttl_ms) else 0;
        if (op != .clear and (key.len == 0 or key.len > self.config.max_key_len)) return .key_too_large;
        if (value.len > self.config.max_value_len) return .value_too_large;
        // Validate before auto-flushing, so an oversized record has no effects.
        if (format.frame_header_size + format.recordSize(key.len, value.len) > self.buffer.len) return .batch_full;
        if (!self.batching) self.builder.reset();
        self.add(op, key, value, expiry) catch |err| switch (err) {
            error.BatchFull => {
                const r = self.flush(self.batch_ack);
                switch (r) {
                    .ok => {},
                    else => return r,
                }
                self.add(op, key, value, expiry) catch return .batch_full;
            },
            error.KeyTooLarge, error.EmptyKey => return .key_too_large,
            error.ValueTooLarge => return .value_too_large,
        };
        if (ack == .replicate_once) self.batch_ack = ack;
        return if (self.batching) .{ .ok = 0 } else self.flush(ack);
    }
    pub fn beginBatch(self: *KvWriter) void {
        const ctx = self.context orelse return;
        ctx.lock();
        defer ctx.unlock();
        // Repeated begin must not silently discard buffered mutations.
        if (self.batching) return;
        self.builder.reset();
        self.batch_ack = .fire_and_forget;
        self.batching = true;
    }
    pub fn commitBatch(self: *KvWriter, opts: types.KvWriteOpts) types.KvWriteResult {
        const ctx = self.context orelse return .not_writer;
        ctx.lock();
        defer ctx.unlock();
        if (self.readiness()) |r| return r;
        if (!self.batching) return .{ .ok = 0 };
        const r = self.flush(if (opts.ack_mode == .replicate_once) .replicate_once else self.batch_ack);
        switch (r) {
            .ok => self.batching = false,
            else => {},
        }
        return r;
    }
    fn flush(self: *KvWriter, ack: types.AckMode) types.KvWriteResult {
        if (self.builder.isEmpty()) return .{ .ok = 0 };
        self.builder.writer_epoch = self.epoch;
        self.builder.leader_epoch = self.publisher.leader_epoch;
        const result = self.publisher.publish(self.builder.finish(), 0, ack);
        return switch (result) {
            .ok => blk: {
                self.builder.reset();
                self.batch_ack = .fire_and_forget;
                break :blk .{ .ok = self.publisher.lastAssignedToken() };
            },
            .back_pressured => .back_pressured,
            .not_registered => .not_writer,
            .not_connected, .failed => .leader_unavailable,
        };
    }
    /// Context lock held. Never revive a revoked session; nonce matching is
    /// performed by the dispatcher. Unsent batch records survive reassertion
    /// and receive the confirmed epoch when committed.
    pub fn transition(self: *KvWriter, new_state: types.KvWriterState, epoch: u64) void {
        if (self.writer_state == .revoked) return;
        const changed = self.writer_state != new_state or self.epoch != epoch;
        if (new_state == .revoked) self.builder.reset();
        self.epoch = epoch;
        self.writer_state = new_state;
        if (changed) if (self.options.on_state) |cb| cb(self.options.on_state_user_data, new_state, epoch);
    }
    pub fn close(self: *KvWriter) void {
        if (self.context) |ctx| ctx.closeWriter(self);
        const a = self.allocator;
        a.free(self.buffer);
        a.free(self.scratch);
        a.destroy(self);
    }
};

test "writer validation, batch preservation, fencing and TTL encoding" {
    const a = std.testing.allocator;
    var storage: [256]u8 = undefined;
    var ctx = Context.testContext(a);
    var w = KvWriter{ .allocator = a, .context = &ctx, .name_len = 1, .nonce = 9, .config = .{ .max_key_len = 4, .max_value_len = 64 }, .options = .{}, .publisher = .{ .topic_id = 1, .publisher_id = 9, .leader_node_id = 1, .leader_epoch = 3 }, .buffer = &storage, .scratch = &.{}, .builder = format.DataFrameBuilder.init(&storage, 2, 3), .epoch = 2, .writer_state = .granted };
    w.beginBatch();
    try std.testing.expectEqual(types.KvWriteResult{ .ok = 0 }, w.put("k", "v", .{ .ttl_ms = 0 }));
    try std.testing.expectEqual(types.KvWriteResult.key_too_large, w.put("large", "v", .{}));
    try std.testing.expectEqual(@as(u32, 1), w.builder.recordCount());
    const frame = try format.decodeFrame(w.builder.finish(), 4, 64);
    var it = frame.payload.data;
    try std.testing.expectEqual(@as(u64, 0), it.next().?.expire_at_ns);
    // No publication: failed commit retains records for retry.
    try std.testing.expectEqual(types.KvWriteResult.leader_unavailable, w.commitBatch(.{}));
    try std.testing.expect(w.batching);
    try std.testing.expectEqual(@as(u32, 1), w.builder.recordCount());
    w.transition(.reasserting, 2);
    try std.testing.expectEqual(types.KvWriteResult.leader_unavailable, w.clear(.{}));
    w.transition(.granted, 4);
    try std.testing.expectEqual(@as(u32, 1), w.builder.recordCount());
    w.transition(.revoked, 4);
    w.transition(.granted, 5);
    try std.testing.expectEqual(types.KvWriterState.revoked, w.state());
    try std.testing.expectEqual(types.KvWriteResult.not_writer, w.delete("k", .{}));
}
