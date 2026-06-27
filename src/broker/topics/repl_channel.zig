// SPDX-License-Identifier: Apache-2.0
//! Replication channels implementing ringloom-queue's transport SPI (spec 06).
//!
//! These types are transport-agnostic: an `OutboundChannel` wraps each ringloom
//! repl frame in a `TopicReplEnvelope` and hands it to a `Publisher` vtable (the
//! broker supplies an Aeron-backed publisher; tests supply an in-memory one). An
//! `InboundChannel` is a bounded queue of whole inner frames fed by the receiver
//! loop's repl demux after it strips the envelope.
//!
//! Hard requirement from ringloom-queue: loss must surface as a disconnect, never
//! a silent gap. Backpressure holds the exact frame for retry.

const std = @import("std");
const rq = @import("ringloom_queue");
const topic_data_header = @import("ringloom_common").message.topic_data_header;

const TopicReplEnvelope = topic_data_header.TopicReplEnvelope;
const ReplDirection = topic_data_header.ReplDirection;
const OfferResult = rq.repl.transport.OfferResult;

/// Runtime publisher used by `OutboundChannel`. The broker binds this to its
/// Aeron repl publications; tests bind it to a loopback ring.
pub const Publisher = struct {
    ctx: *anyopaque,
    /// Publish a complete repl frame (envelope + inner) to `target_node`.
    /// Returns an offer result: `>= 0` accepted, negative is an `OfferResult`.
    offerFn: *const fn (ctx: *anyopaque, target_node: u8, frame: []const u8) i64,
    isConnectedFn: *const fn (ctx: *anyopaque, target_node: u8) bool,
    isBackPressuredFn: *const fn (ctx: *anyopaque, target_node: u8) bool,
};

/// Outbound side: builds an enveloped frame and offers it via the publisher.
pub const OutboundChannel = struct {
    publisher: Publisher,
    envelope: TopicReplEnvelope,
    target_node: u8,
    scratch: []u8, // owned: >= TopicReplEnvelope.size + max inner frame
    allocator: std.mem.Allocator,

    pub fn init(
        allocator: std.mem.Allocator,
        publisher: Publisher,
        topic_id: u64,
        leader_epoch: u64,
        direction: ReplDirection,
        source_node: u8,
        target_node: u8,
        max_inner_frame: usize,
    ) !OutboundChannel {
        const scratch = try allocator.alloc(u8, TopicReplEnvelope.encoded_length + max_inner_frame);
        return .{
            .publisher = publisher,
            .envelope = .{
                .direction = @intFromEnum(direction),
                .topic_id = topic_id,
                .leader_epoch = leader_epoch,
                .source_node_id = source_node,
                .target_node_id = target_node,
            },
            .target_node = target_node,
            .scratch = scratch,
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *OutboundChannel) void {
        self.allocator.free(self.scratch);
        self.* = undefined;
    }

    pub fn setEpoch(self: *OutboundChannel, epoch: u64) void {
        self.envelope.leader_epoch = epoch;
    }

    /// ringloom-queue Outbound.offer: copy/consume the frame before returning.
    pub fn offer(self: *OutboundChannel, frame: []const u8) i64 {
        const total = TopicReplEnvelope.encoded_length + frame.len;
        if (total > self.scratch.len) {
            return @intFromEnum(OfferResult.max_position_exceeded);
        }
        const n = self.envelope.encodeWithFrame(self.scratch, frame);
        return self.publisher.offerFn(self.publisher.ctx, self.target_node, self.scratch[0..n]);
    }

    pub fn isConnected(self: *OutboundChannel) bool {
        return self.publisher.isConnectedFn(self.publisher.ctx, self.target_node);
    }

    pub fn isBackPressured(self: *OutboundChannel) bool {
        return self.publisher.isBackPressuredFn(self.publisher.ctx, self.target_node);
    }
};

/// Inbound side: a bounded SPSC ring of whole inner repl frames. The receiver
/// loop's repl demux strips the envelope and calls `pushFrame` with the inner
/// frame; the ringloom Source/Sink pulls them via `poll`/`nextFrame`.
pub const InboundChannel = struct {
    allocator: std.mem.Allocator,
    slots: [][]u8,
    lens: []usize,
    cap: []usize, // slot buffer capacities
    head: usize = 0, // next write slot
    tail: usize = 0, // next read slot
    count: usize = 0,
    readable: usize = 0,
    connected: std.atomic.Value(bool),

    pub fn init(allocator: std.mem.Allocator, capacity_frames: usize, max_frame: usize) !InboundChannel {
        const slots = try allocator.alloc([]u8, capacity_frames);
        errdefer allocator.free(slots);
        var made: usize = 0;
        errdefer for (slots[0..made]) |s| allocator.free(s);
        while (made < capacity_frames) : (made += 1) slots[made] = try allocator.alloc(u8, max_frame);
        const lens = try allocator.alloc(usize, capacity_frames);
        errdefer allocator.free(lens);
        const cap = try allocator.alloc(usize, capacity_frames);
        for (cap, 0..) |*c, i| c.* = slots[i].len;
        return .{
            .allocator = allocator,
            .slots = slots,
            .lens = lens,
            .cap = cap,
            .connected = std.atomic.Value(bool).init(true),
        };
    }

    pub fn deinit(self: *InboundChannel) void {
        for (self.slots) |s| self.allocator.free(s);
        self.allocator.free(self.slots);
        self.allocator.free(self.lens);
        self.allocator.free(self.cap);
        self.* = undefined;
    }

    fn capacity(self: *const InboundChannel) usize {
        return self.slots.len;
    }

    /// Push a whole inner frame (envelope already stripped). Returns false if full
    /// (the caller surfaces this as backpressure/loss → disconnect upstream).
    pub fn pushFrame(self: *InboundChannel, frame: []const u8) bool {
        if (self.count == self.capacity()) return false;
        if (frame.len > self.cap[self.head]) return false;
        @memcpy(self.slots[self.head][0..frame.len], frame);
        self.lens[self.head] = frame.len;
        self.head = (self.head + 1) % self.capacity();
        self.count += 1;
        return true;
    }

    pub fn setConnected(self: *InboundChannel, v: bool) void {
        self.connected.store(v, .release);
    }

    // ── ringloom-queue Inbound contract ──
    pub fn poll(self: *InboundChannel, fragment_limit: u32) u32 {
        if (!self.connected.load(.acquire)) return 0;
        const want = @min(@as(usize, fragment_limit), self.count);
        self.readable = want;
        return @intCast(want);
    }

    pub fn nextFrame(self: *InboundChannel) ?[]const u8 {
        if (self.readable == 0 or self.count == 0) return null;
        const slot = self.tail;
        const len = self.lens[slot];
        self.tail = (self.tail + 1) % self.capacity();
        self.count -= 1;
        self.readable -= 1;
        return self.slots[slot][0..len];
    }

    pub fn isConnected(self: *InboundChannel) bool {
        return self.connected.load(.acquire);
    }
};

comptime {
    rq.repl.transport.AssertOutbound(OutboundChannel);
    rq.repl.transport.AssertInbound(InboundChannel);
}

// ── In-memory test wiring ────────────────────────────────────────────────────
// A `LoopWire` connects an OutboundChannel's publisher to a peer InboundChannel,
// stripping the envelope exactly as the receiver demux would. Used by unit tests
// and the loopback replication parity test.

pub const LoopWire = struct {
    peer_inbound: *InboundChannel,
    connected: bool = true,
    drop_remaining: usize = 0,

    pub fn publisher(self: *LoopWire) Publisher {
        return .{
            .ctx = self,
            .offerFn = offerImpl,
            .isConnectedFn = isConnectedImpl,
            .isBackPressuredFn = isBackPressuredImpl,
        };
    }

    fn offerImpl(ctx: *anyopaque, target_node: u8, frame: []const u8) i64 {
        _ = target_node;
        const self: *LoopWire = @ptrCast(@alignCast(ctx));
        if (!self.connected) return @intFromEnum(OfferResult.not_connected);
        if (self.drop_remaining > 0) {
            self.drop_remaining -= 1;
            self.connected = false;
            self.peer_inbound.setConnected(false);
            return @intFromEnum(OfferResult.not_connected);
        }
        // Strip envelope, route inner frame to the peer inbound (as the demux does).
        if (frame.len < TopicReplEnvelope.encoded_length) return @intFromEnum(OfferResult.closed);
        const env = TopicReplEnvelope.decode(frame) catch
            return @intFromEnum(OfferResult.closed);
        const inner = env.frameSlice(frame);
        if (!self.peer_inbound.pushFrame(inner)) return @intFromEnum(OfferResult.back_pressured);
        return 1;
    }

    fn isConnectedImpl(ctx: *anyopaque, target_node: u8) bool {
        _ = target_node;
        const self: *LoopWire = @ptrCast(@alignCast(ctx));
        return self.connected;
    }

    fn isBackPressuredImpl(ctx: *anyopaque, target_node: u8) bool {
        _ = target_node;
        const self: *LoopWire = @ptrCast(@alignCast(ctx));
        return self.peer_inbound.count == self.peer_inbound.capacity();
    }
};

test "outbound wraps frame in envelope and inbound receives inner bytes" {
    const allocator = std.testing.allocator;
    var inbound = try InboundChannel.init(allocator, 8, 256);
    defer inbound.deinit();
    var wire = LoopWire{ .peer_inbound = &inbound };

    var outbound = try OutboundChannel.init(allocator, wire.publisher(), 0xABCD, 5, .source_to_sink, 1, 2, 256);
    defer outbound.deinit();

    const payload = "hello-repl-frame";
    const r = outbound.offer(payload);
    try std.testing.expect(OfferResult.accepted(r));

    try std.testing.expectEqual(@as(u32, 1), inbound.poll(16));
    const got = inbound.nextFrame().?;
    try std.testing.expectEqualStrings(payload, got);
    try std.testing.expect(inbound.nextFrame() == null);
}

test "dropped frame surfaces as disconnect, not silent gap" {
    const allocator = std.testing.allocator;
    var inbound = try InboundChannel.init(allocator, 8, 256);
    defer inbound.deinit();
    var wire = LoopWire{ .peer_inbound = &inbound, .drop_remaining = 1 };

    var outbound = try OutboundChannel.init(allocator, wire.publisher(), 1, 1, .source_to_sink, 1, 2, 256);
    defer outbound.deinit();

    const r = outbound.offer("frame");
    try std.testing.expect(!OfferResult.accepted(r));
    try std.testing.expect(!outbound.isConnected());
    try std.testing.expect(!inbound.isConnected());
}
