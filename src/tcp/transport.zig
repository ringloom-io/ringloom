//! Generic TCP transport implementation.
//!
//! `TcpTransport` is parameterized over an I/O engine backend (io_uring or
//! kqueue). It orchestrates connection management, handshaking, framing,
//! heartbeats, and reconnection for all peer broker connections.

const std = @import("std");
const io_engine_mod = @import("io_engine.zig");
const frame_mod = @import("frame.zig");
const handshake_mod = @import("handshake.zig");
const connection_manager_mod = @import("connection_manager.zig");
const socket_config_mod = @import("socket_config.zig");

pub const ConnectionHandle = io_engine_mod.ConnectionHandle;
pub const Completion = io_engine_mod.Completion;
pub const FrameHeader = frame_mod.FrameHeader;
pub const PeerConnection = connection_manager_mod.PeerConnection;

pub const TransportConfig = struct {
    local_node_id: u8,
    group_name: []const u8,
    peer_count: u8,
    bind_port: u16,
    heartbeat_interval_ms: u64 = 500,
    heartbeat_timeout_ms: u64 = 2000,
    reconnect_initial_delay_ms: u64 = 100,
    reconnect_max_delay_ms: u64 = 1000,
    max_frame_length: u32 = 65_536,
    peer_write_queue_capacity: u32 = 4_096,
    socket_config: socket_config_mod.SocketConfig = .{},
};

/// Generic TCP transport using a compile-time selected I/O engine backend.
pub fn TcpTransportImpl(comptime Engine: type) type {
    io_engine_mod.assertValidEngine(Engine);

    return struct {
        const Self = @This();

        engine: Engine,
        conn_mgr: connection_manager_mod.ConnectionManager,
        config: TransportConfig,
        allocator: std.mem.Allocator,
        running: bool,

        pub fn init(allocator: std.mem.Allocator, config: TransportConfig) !Self {
            var engine = try Engine.init(allocator, config.peer_count);
            errdefer engine.deinit();

            var conn_mgr = try connection_manager_mod.ConnectionManager.init(
                allocator,
                config.local_node_id,
                config.group_name,
                config.peer_count,
            );
            errdefer conn_mgr.deinit();

            return .{
                .engine = engine,
                .conn_mgr = conn_mgr,
                .config = config,
                .allocator = allocator,
                .running = false,
            };
        }

        pub fn deinit(self: *Self) void {
            self.conn_mgr.deinit();
            self.engine.deinit();
        }

        pub fn start(self: *Self) void {
            self.running = true;
        }

        pub fn stop(self: *Self) void {
            self.running = false;
        }

        pub fn isRunning(self: *const Self) bool {
            return self.running;
        }

        pub fn getConnectionManager(self: *Self) *connection_manager_mod.ConnectionManager {
            return &self.conn_mgr;
        }

        pub fn getEngine(self: *Self) *Engine {
            return &self.engine;
        }
    };
}

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

/// A minimal mock I/O engine for unit tests.
const MockEngine = struct {
    peer_count: u8,
    deinited: bool = false,

    pub fn init(_: std.mem.Allocator, peer_count: u8) !MockEngine {
        return .{ .peer_count = peer_count };
    }

    pub fn deinit(self: *MockEngine) void {
        self.deinited = true;
    }

    pub fn submit_accept(_: *MockEngine, _: ConnectionHandle) void {}
    pub fn submit_connect(_: *MockEngine, _: ConnectionHandle, _: std.net.Address) void {}
    pub fn submit_recv(_: *MockEngine, _: ConnectionHandle, _: []u8) void {}
    pub fn submit_send(_: *MockEngine, _: ConnectionHandle, _: []const u8) void {}
    pub fn submit_close(_: *MockEngine, _: ConnectionHandle) void {}

    pub fn harvest(_: *MockEngine, _: []Completion) u32 {
        return 0;
    }
};

test "TcpTransportImpl init/deinit with mock engine" {
    const Transport = TcpTransportImpl(MockEngine);
    var transport = try Transport.init(testing.allocator, .{
        .local_node_id = 1,
        .group_name = "test-cluster",
        .peer_count = 4,
        .bind_port = 9000,
    });
    defer transport.deinit();

    try testing.expect(!transport.isRunning());
    transport.start();
    try testing.expect(transport.isRunning());
    transport.stop();
    try testing.expect(!transport.isRunning());
}

test "TcpTransportImpl provides access to ConnectionManager" {
    const Transport = TcpTransportImpl(MockEngine);
    var transport = try Transport.init(testing.allocator, .{
        .local_node_id = 1,
        .group_name = "test-cluster",
        .peer_count = 4,
        .bind_port = 9000,
    });
    defer transport.deinit();

    const cm = transport.getConnectionManager();
    try testing.expectEqual(@as(u8, 1), cm.local_node_id);
}

test "assertValidEngine accepts MockEngine" {
    io_engine_mod.assertValidEngine(MockEngine);
}
