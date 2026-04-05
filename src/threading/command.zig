//! Self-dispatching command for inter-event-loop communication.
//!
//! A Command carries its own handler function pointer and a reference to
//! the data it operates on. The receiving event loop calls the handler
//! without needing a switch statement or type inspection.
//!
//! The handler function knows how to interpret the data pointer and apply
//! the command to the owning event loop's state.

const std = @import("std");

/// A self-dispatching command. The handler function knows how to execute
/// itself on the target event loop.
pub const Command = struct {
    /// Function to execute when this command is processed.
    /// `context` is the owning event loop's context pointer.
    /// `self` is this Command struct (to access `data`).
    handler: *const fn (context: *anyopaque, self: *const Command) void,

    /// Pointer to command-specific data. The handler function casts this to
    /// the appropriate type. The memory must remain valid until the command
    /// is processed (typically stack-allocated — the ring buffer copies bytes).
    data: ?*anyopaque,
};

comptime {
    // Command should be exactly 16 bytes (two pointers) on 64-bit systems.
    std.debug.assert(@sizeOf(Command) == 16);
}

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "Command is 16 bytes" {
    // Given / When / Then
    try testing.expectEqual(@as(usize, 16), @sizeOf(Command));
}

test "Command handler is called with context and data" {
    // Given
    const TestCtx = struct {
        value: u32 = 0,
    };
    var ctx = TestCtx{};
    var payload: u32 = 42;

    const cmd = Command{
        .handler = struct {
            fn handle(context: *anyopaque, cmd_ptr: *const Command) void {
                const c: *TestCtx = @ptrCast(@alignCast(context));
                const val: *const u32 = @ptrCast(@alignCast(cmd_ptr.data.?));
                c.value = val.*;
            }
        }.handle,
        .data = @ptrCast(&payload),
    };

    // When
    cmd.handler(@ptrCast(&ctx), &cmd);

    // Then
    try testing.expectEqual(@as(u32, 42), ctx.value);
}

test "Command with null data" {
    // Given
    const TestCtx = struct {
        called: bool = false,
    };
    var ctx = TestCtx{};

    const cmd = Command{
        .handler = struct {
            fn handle(context: *anyopaque, _: *const Command) void {
                const c: *TestCtx = @ptrCast(@alignCast(context));
                c.called = true;
            }
        }.handle,
        .data = null,
    };

    // When
    cmd.handler(@ptrCast(&ctx), &cmd);

    // Then
    try testing.expect(ctx.called);
}
