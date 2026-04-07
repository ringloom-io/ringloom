//! Core types for the brz_tcp I/O engine abstraction.
//!
//! Every platform backend (io_uring, kqueue) implements a common interface
//! using these types. The broker programs against this interface; the
//! concrete backend is injected at compile time.

const std = @import("std");

/// Opaque handle to a TCP connection within the I/O engine.
/// Indexes into the engine's internal connection table.
pub const ConnectionHandle = enum(u16) {
    invalid = std.math.maxInt(u16),
    _,

    pub fn toIndex(self: ConnectionHandle) u16 {
        return @intFromEnum(self);
    }

    pub fn fromIndex(index: u16) ConnectionHandle {
        return @enumFromInt(index);
    }
};

/// Result of a single completed I/O operation.
pub const Completion = struct {
    handle: ConnectionHandle,
    op: OpType,
    result: Result,

    pub const OpType = enum(u8) {
        accept,
        connect,
        recv,
        send,
        close,
    };

    pub const Result = union(enum) {
        /// Successful operation — `bytes` is the number of bytes transferred
        /// (for recv/send) or 0 (for accept/connect/close).
        ok: struct { bytes: u32 },
        /// Operation failed with a kernel error code.
        err: std.posix.E,
        /// Peer closed the connection (recv returned 0).
        eof: void,
    };
};

/// Pack a connection handle and operation type into a 64-bit user_data field.
pub fn encodeUserData(handle: ConnectionHandle, op: Completion.OpType) u64 {
    return @as(u64, @intFromEnum(handle)) |
        (@as(u64, @intFromEnum(op)) << 16);
}

/// Unpack a 64-bit user_data field into a connection handle and operation type.
pub fn decodeUserData(user_data: u64) struct { handle: ConnectionHandle, op: Completion.OpType } {
    return .{
        .handle = @enumFromInt(@as(u16, @truncate(user_data))),
        .op = @enumFromInt(@as(u8, @truncate(user_data >> 16))),
    };
}

/// Compile-time check that an engine type satisfies the IoEngine interface.
pub fn assertValidEngine(comptime E: type) void {
    const required = .{
        "init", "deinit", "submit_accept", "submit_connect",
        "submit_recv", "submit_send", "submit_close", "harvest",
    };
    inline for (required) |name| {
        if (!@hasDecl(E, name))
            @compileError("IoEngine missing required method: " ++ name);
    }
}

// ── Tests ─────────────────────────────────────────────────────────────

test "ConnectionHandle roundtrip" {
    const handle = ConnectionHandle.fromIndex(42);
    try std.testing.expectEqual(@as(u16, 42), handle.toIndex());
}

test "ConnectionHandle invalid sentinel" {
    try std.testing.expectEqual(@as(u16, std.math.maxInt(u16)), ConnectionHandle.invalid.toIndex());
}

test "encodeUserData/decodeUserData roundtrip" {
    const handle = ConnectionHandle.fromIndex(123);
    const op = Completion.OpType.recv;
    const encoded = encodeUserData(handle, op);
    const decoded = decodeUserData(encoded);
    try std.testing.expectEqual(handle, decoded.handle);
    try std.testing.expectEqual(op, decoded.op);
}

test "encodeUserData with all op types" {
    const ops = [_]Completion.OpType{ .accept, .connect, .recv, .send, .close };
    for (ops) |op| {
        const handle = ConnectionHandle.fromIndex(7);
        const decoded = decodeUserData(encodeUserData(handle, op));
        try std.testing.expectEqual(handle, decoded.handle);
        try std.testing.expectEqual(op, decoded.op);
    }
}
