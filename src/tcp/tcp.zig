//! brz_tcp — TCP transport library for BRZ broker.
//!
//! This is the root module that provides public API and re-exports.
//! The library handles connection management, framing, handshake,
//! and I/O engine abstraction for TCP-based broker communication.

const std = @import("std");
const builtin = @import("builtin");

// ── Public API ────────────────────────────────────────────────────────

pub const io_engine = @import("io_engine.zig");
pub const frame = @import("frame.zig");
pub const handshake = @import("handshake.zig");
pub const socket_config = @import("socket_config.zig");
pub const connection_manager = @import("connection_manager.zig");
pub const transport = @import("transport.zig");

// Re-export key types at root for convenience.
pub const ConnectionHandle = io_engine.ConnectionHandle;
pub const Completion = io_engine.Completion;
pub const FrameHeader = frame.FrameHeader;
pub const HandshakeFrame = handshake.HandshakeFrame;
pub const SocketConfig = socket_config.SocketConfig;
pub const ConnectionState = connection_manager.ConnectionState;
pub const PeerConnection = connection_manager.PeerConnection;
pub const ConnectionManager = connection_manager.ConnectionManager;
pub const FrameReader = frame.FrameReader;
pub const FrameWriter = frame.FrameWriter;
pub const TransportConfig = transport.TransportConfig;

// Backend-specific engine types.
pub const io_uring_engine = @import("io_uring_engine.zig");
pub const kqueue_engine = @import("kqueue_engine.zig");

/// Select the I/O engine type for the current platform.
pub fn DefaultEngine() type {
    return switch (builtin.os.tag) {
        .linux => io_uring_engine.IoUringEngine,
        .macos, .freebsd => kqueue_engine.KqueueEngine,
        else => @compileError("Unsupported platform for brz_tcp"),
    };
}

/// The default transport type using the platform-specific engine.
pub const TcpTransport = transport.TcpTransportImpl(DefaultEngine());

// ── Comptime test discovery ───────────────────────────────────────────

comptime {
    _ = io_engine;
    _ = frame;
    _ = handshake;
    _ = socket_config;
    _ = connection_manager;
    _ = transport;
    // Backend engines — only reference them to pull in their tests.
    _ = io_uring_engine;
    _ = kqueue_engine;
}
