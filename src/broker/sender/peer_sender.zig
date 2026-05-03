//! Per-peer sender connection state for the BRZ broker TCP send path.
//!
//! Each connected peer broker is represented by a PeerSender. This struct
//! holds the TCP connection state, bounded write queue, heartbeat timing,
//! and reconnection backoff state.

const std = @import("std");
const builtin = @import("builtin");
const net = @import("brz_tcp").socket;
const WriteQueue = @import("write_queue.zig").WriteQueue;
const constants = @import("brz_common").platform.constants;
const platform = @import("brz_common").platform;

pub const ConnectionState = enum {
    /// Not yet connected. Waiting for reconnect timer.
    disconnected,
    /// TCP connect in progress (non-blocking).
    connecting,
    /// Connected. Handshake sent, ready to send data.
    connected,
};

/// Per-peer sender state for TCP connections.
pub const PeerSender = struct {
    /// The peer broker's node ID.
    node_id: u8,

    /// TCP socket file descriptor for the outgoing connection.
    socket_fd: std.posix.fd_t,

    /// IP address and port of the peer broker.
    address: net.Address,

    /// Current connection state.
    state: ConnectionState,

    /// Per-peer outbound write queue.
    write_queue: WriteQueue,

    /// Whether the write path is blocked (partial write pending).
    write_blocked: bool,

    /// Byte offset into the current frame for a partial write.
    partial_write_offset: usize,

    /// Monotonic timestamp (ns) of the last data or heartbeat sent.
    last_send_ns: i64,

    /// Reconnection backoff state (ms).
    reconnect_delay_ms: u64,

    /// Monotonic timestamp (ns) of next reconnection attempt.
    next_reconnect_ns: i64,

    // ── io_uring async send state ────────────────────────────────────

    /// Whether an io_uring send is in flight for this peer.
    io_pending: bool,

    /// Number of frames submitted in the current in-flight writev batch.
    in_flight_frames: u32,

    /// Total bytes submitted in the current in-flight writev batch.
    in_flight_bytes: usize,

    /// Generation counter, incremented on each disconnect/reconnect.
    /// Used to ignore stale CQEs from previous connections.
    io_generation: u8,

    /// Per-peer iovec storage for io_uring writev batches.
    /// Must remain valid from SQE submission until CQE harvest.
    send_iovecs: [max_send_batch]std.posix.iovec_const,

    pub const max_send_batch = 64;

    const Self = @This();

    pub fn init(
        node_id: u8,
        address: net.Address,
        allocator: std.mem.Allocator,
    ) !Self {
        return .{
            .node_id = node_id,
            .socket_fd = -1,
            .address = address,
            .state = .disconnected,
            .write_queue = try WriteQueue.init(
                constants.default_peer_write_queue_capacity,
                constants.default_max_frame_length,
                allocator,
            ),
            .write_blocked = false,
            .partial_write_offset = 0,
            .last_send_ns = 0,
            .reconnect_delay_ms = constants.default_reconnect_initial_delay_ms,
            .next_reconnect_ns = 0,
            .io_pending = false,
            .in_flight_frames = 0,
            .in_flight_bytes = 0,
            .io_generation = 0,
            .send_iovecs = undefined,
        };
    }

    /// Reset connection state for a new connection attempt.
    pub fn resetForReconnect(self: *Self) void {
        if (self.socket_fd >= 0) {
            platform.closeFd(self.socket_fd);
            self.socket_fd = -1;
        }
        self.state = .disconnected;
        self.write_blocked = false;
        self.partial_write_offset = 0;
        self.write_queue.clear();
        self.io_generation +%= 1;
        self.io_pending = false;
        self.in_flight_frames = 0;
        self.in_flight_bytes = 0;
    }

    /// Advance the reconnect backoff timer (exponential with cap).
    pub fn advanceBackoff(self: *Self, now_ns: i64) void {
        self.next_reconnect_ns = now_ns +
            @as(i64, @intCast(self.reconnect_delay_ms)) * std.time.ns_per_ms;
        self.reconnect_delay_ms = @min(
            self.reconnect_delay_ms * 2,
            constants.default_reconnect_max_delay_ms,
        );
    }

    /// Reset backoff after successful connection.
    pub fn resetBackoff(self: *Self) void {
        self.reconnect_delay_ms = constants.default_reconnect_initial_delay_ms;
    }

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        if (self.socket_fd >= 0) platform.closeFd(self.socket_fd);
        self.write_queue.deinit(allocator);
    }

    // ── io_uring user_data encoding ──────────────────────────────────

    /// Encode peer identity into io_uring user_data for CQE correlation.
    /// Layout: [0:7]=node_id, [8:15]=generation, [16:31]=frame_count
    pub fn encodeUserData(self: *const Self, frame_count: u16) u64 {
        return @as(u64, self.node_id) |
            (@as(u64, self.io_generation) << 8) |
            (@as(u64, frame_count) << 16);
    }

    /// Decode user_data from a CQE back to node_id, generation, frame_count.
    pub fn decodeUserData(user_data: u64) struct { node_id: u8, generation: u8, frame_count: u16 } {
        return .{
            .node_id = @truncate(user_data),
            .generation = @truncate(user_data >> 8),
            .frame_count = @truncate(user_data >> 16),
        };
    }

    /// Return the number of frames available to submit (excluding in-flight ones).
    pub fn availableToSend(self: *const Self) u32 {
        return self.write_queue.count -| self.in_flight_frames;
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

test "PeerSender init sets correct defaults" {
    const allocator = std.testing.allocator;
    const address = net.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);

    var peer = try PeerSender.init(1, address, allocator);
    defer peer.deinit(allocator);

    try std.testing.expectEqual(@as(u8, 1), peer.node_id);
    try std.testing.expectEqual(ConnectionState.disconnected, peer.state);
    try std.testing.expectEqual(@as(std.posix.fd_t, -1), peer.socket_fd);
    try std.testing.expect(!peer.write_blocked);
    try std.testing.expectEqual(@as(i64, 0), peer.last_send_ns);
    try std.testing.expectEqual(constants.default_reconnect_initial_delay_ms, peer.reconnect_delay_ms);
}

test "PeerSender resetForReconnect clears state" {
    const allocator = std.testing.allocator;
    const address = net.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);

    var peer = try PeerSender.init(1, address, allocator);
    defer peer.deinit(allocator);

    peer.state = .connected;
    peer.write_blocked = true;
    peer.partial_write_offset = 100;

    const frame = [_]u8{0} ** 24;
    try peer.write_queue.enqueue(&frame);

    peer.resetForReconnect();

    try std.testing.expectEqual(ConnectionState.disconnected, peer.state);
    try std.testing.expect(!peer.write_blocked);
    try std.testing.expectEqual(@as(usize, 0), peer.partial_write_offset);
    try std.testing.expect(peer.write_queue.isEmpty());
}

test "PeerSender advanceBackoff doubles delay with cap" {
    const allocator = std.testing.allocator;
    const address = net.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);

    var peer = try PeerSender.init(1, address, allocator);
    defer peer.deinit(allocator);

    try std.testing.expectEqual(constants.default_reconnect_initial_delay_ms, peer.reconnect_delay_ms);

    peer.advanceBackoff(0);
    try std.testing.expectEqual(constants.default_reconnect_initial_delay_ms * 2, peer.reconnect_delay_ms);

    // Advance several times to hit the cap
    peer.advanceBackoff(0);
    peer.advanceBackoff(0);
    peer.advanceBackoff(0);
    peer.advanceBackoff(0);

    try std.testing.expect(peer.reconnect_delay_ms <= constants.default_reconnect_max_delay_ms);
}

test "PeerSender resetBackoff restores initial delay" {
    const allocator = std.testing.allocator;
    const address = net.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);

    var peer = try PeerSender.init(1, address, allocator);
    defer peer.deinit(allocator);

    peer.advanceBackoff(0);
    peer.advanceBackoff(0);

    peer.resetBackoff();
    try std.testing.expectEqual(constants.default_reconnect_initial_delay_ms, peer.reconnect_delay_ms);
}
