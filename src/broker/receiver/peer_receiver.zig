//! Per-peer receiver connection state for the BRZ broker TCP receive path.
//!
//! Each connected peer broker is represented by a PeerReceiver. This struct
//! holds the per-peer TCP connection state, read state machine for TCP stream
//! reassembly, and liveness tracking (heartbeat timeout detection).
//!
//! TCP provides reliable ordered delivery, so there is no receive log buffer,
//! no gap detection, no NAK generation, and no fragment reassembly.

const std = @import("std");
const brz_common = @import("brz_common");
const constants = brz_common.platform.constants;
const Clock = brz_common.platform.clock.Clock;
const frame_parser = brz_common.protocol.frame_parser;
const TcpFrameHeader = frame_parser.TcpFrameHeader;

pub const LivenessState = enum {
    /// Receiving data or heartbeats within the expected interval.
    alive,
    /// No data received for heartbeat_suspect_threshold_ns. May be slow or dropping.
    suspect,
    /// No data received for heartbeat_timeout_ns. Peer is considered dead.
    dead,
};

/// Per-peer read state machine for TCP stream reassembly.
pub const ReadState = struct {
    phase: Phase,
    header_buf: [TcpFrameHeader.size]u8 align(@alignOf(TcpFrameHeader)),
    header_bytes_read: u8,
    payload_buf: []u8,
    payload_bytes_read: u32,
    frame_length: u32,

    const Phase = enum {
        reading_header,
        reading_payload,
    };

    pub fn init(payload_buf: []u8) ReadState {
        return .{
            .phase = .reading_header,
            .header_buf = std.mem.zeroes([TcpFrameHeader.size]u8),
            .header_bytes_read = 0,
            .payload_buf = payload_buf,
            .payload_bytes_read = 0,
            .frame_length = 0,
        };
    }

    pub fn reset(self: *ReadState) void {
        self.phase = .reading_header;
        self.header_bytes_read = 0;
        self.payload_bytes_read = 0;
        self.frame_length = 0;
    }

    pub fn payloadLength(self: *const ReadState) u32 {
        if (self.frame_length < TcpFrameHeader.size) return 0;
        return self.frame_length - TcpFrameHeader.size;
    }
};

/// Per-peer receiver state for TCP connections.
pub const PeerReceiver = struct {
    /// The peer broker's node ID (established during handshake).
    node_id: u8,

    /// TCP socket file descriptor for this incoming connection.
    socket_fd: std.posix.fd_t,

    /// Session epoch from the handshake. Used to detect peer restarts.
    session_epoch: u32,

    /// Read state machine for TCP stream reassembly.
    read_state: ReadState,

    /// Monotonic timestamp (ns) of the last frame received from this peer.
    last_recv_ns: i64,

    /// Current liveness state.
    liveness: LivenessState,

    /// IP address and port of the peer broker (from accepted connection).
    address: std.net.Address,

    /// Whether this peer is actively connected.
    connected: bool,

    const Self = @This();

    pub fn init(
        node_id: u8,
        socket_fd: std.posix.fd_t,
        address: std.net.Address,
        session_epoch: u32,
        payload_buf: []u8,
    ) Self {
        return .{
            .node_id = node_id,
            .socket_fd = socket_fd,
            .session_epoch = session_epoch,
            .read_state = ReadState.init(payload_buf),
            .last_recv_ns = Clock.monotonicNanos(),
            .liveness = .alive,
            .address = address,
            .connected = true,
        };
    }

    /// Update liveness state based on time since last received frame.
    pub fn updateLiveness(self: *Self, now_ns: i64) LivenessState {
        const elapsed_ns = now_ns - self.last_recv_ns;
        const suspect_threshold_ns: i64 = 1500 * std.time.ns_per_ms;
        const dead_threshold_ns: i64 = @as(i64, @intCast(constants.default_heartbeat_timeout_ms_tcp)) * std.time.ns_per_ms;

        if (elapsed_ns >= dead_threshold_ns) {
            self.liveness = .dead;
        } else if (elapsed_ns >= suspect_threshold_ns) {
            self.liveness = .suspect;
        } else {
            self.liveness = .alive;
        }
        return self.liveness;
    }

    /// Reset peer state for reconnection (new handshake from same peer).
    pub fn resetForReconnect(
        self: *Self,
        socket_fd: std.posix.fd_t,
        address: std.net.Address,
        session_epoch: u32,
    ) void {
        if (self.socket_fd >= 0) {
            std.posix.close(self.socket_fd);
        }
        self.socket_fd = socket_fd;
        self.address = address;
        self.session_epoch = session_epoch;
        self.read_state.reset();
        self.last_recv_ns = Clock.monotonicNanos();
        self.liveness = .alive;
        self.connected = true;
    }

    pub fn close(self: *Self) void {
        if (self.socket_fd >= 0) {
            std.posix.close(self.socket_fd);
            self.socket_fd = -1;
        }
        self.connected = false;
        self.liveness = .dead;
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "PeerReceiver init sets correct defaults" {
    var payload_buf: [4096]u8 = undefined;
    const address = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);

    const peer = PeerReceiver.init(1, -1, address, 42, &payload_buf);

    try testing.expectEqual(@as(u8, 1), peer.node_id);
    try testing.expectEqual(@as(std.posix.fd_t, -1), peer.socket_fd);
    try testing.expectEqual(@as(u32, 42), peer.session_epoch);
    try testing.expect(peer.connected);
    try testing.expect(peer.last_recv_ns > 0);
    try testing.expectEqual(LivenessState.alive, peer.liveness);
}

test "PeerReceiver updateLiveness transitions correctly" {
    var payload_buf: [4096]u8 = undefined;
    const address = std.net.Address.initIp4(.{ 127, 0, 0, 1 }, 9001);

    var peer = PeerReceiver.init(1, -1, address, 1, &payload_buf);
    const now: i64 = @intCast(std.time.nanoTimestamp());

    // Recent data → alive
    peer.last_recv_ns = now - 100 * std.time.ns_per_ms;
    try testing.expectEqual(LivenessState.alive, peer.updateLiveness(now));

    // 1600ms without data → suspect
    peer.last_recv_ns = now - 1600 * std.time.ns_per_ms;
    try testing.expectEqual(LivenessState.suspect, peer.updateLiveness(now));

    // 2500ms without data → dead (timeout is 2000ms)
    peer.last_recv_ns = now - 2500 * std.time.ns_per_ms;
    try testing.expectEqual(LivenessState.dead, peer.updateLiveness(now));
}

test "ReadState init and reset" {
    var payload_buf: [4096]u8 = undefined;
    var state = ReadState.init(&payload_buf);

    try testing.expectEqual(ReadState.Phase.reading_header, state.phase);
    try testing.expectEqual(@as(u8, 0), state.header_bytes_read);
    try testing.expectEqual(@as(u32, 0), state.payload_bytes_read);
    try testing.expectEqual(@as(u32, 0), state.frame_length);

    // Simulate partial read progress
    state.phase = .reading_payload;
    state.header_bytes_read = 24;
    state.payload_bytes_read = 100;
    state.frame_length = 500;

    state.reset();

    try testing.expectEqual(ReadState.Phase.reading_header, state.phase);
    try testing.expectEqual(@as(u8, 0), state.header_bytes_read);
    try testing.expectEqual(@as(u32, 0), state.payload_bytes_read);
    try testing.expectEqual(@as(u32, 0), state.frame_length);
}

test "ReadState payloadLength" {
    var payload_buf: [4096]u8 = undefined;
    var state = ReadState.init(&payload_buf);

    state.frame_length = 100;
    try testing.expectEqual(@as(u32, 100 - 24), state.payloadLength());

    state.frame_length = 24;
    try testing.expectEqual(@as(u32, 0), state.payloadLength());

    state.frame_length = 0;
    try testing.expectEqual(@as(u32, 0), state.payloadLength());
}
