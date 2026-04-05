//! Status Message (SM) encoding, decoding, and scheduling.
//!
//! Status Messages are the mechanism by which the receiver communicates its
//! capacity back to the sender. They carry the receiver's consumption position
//! and advertised window, enabling the sender to update its send_limit.

const std = @import("std");
const constants = @import("../platform/constants.zig");
const ReceiverFlowControl = @import("receiver_flow_control.zig").ReceiverFlowControl;

// ── SM Flags ──────────────────────────────────────────────────────────

pub const sm_flags = struct {
    /// No special flags — normal periodic or eager SM.
    pub const none: u8 = 0x00;

    /// Request the sender to re-send its SETUP frame. Used when the
    /// receiver detects loss of the initial connection state (e.g.,
    /// after a restart or after detecting a gap at position 0).
    pub const send_setup: u8 = 0x01;
};

// ── Status Message Flyweight ──────────────────────────────────────────

/// Flyweight overlay for a Status Message frame. Zero-copy — reads
/// and writes go directly to the underlying buffer bytes.
pub const StatusMessageFlyweight = struct {
    buffer: [*]u8,

    const frame_length_offset = 0;
    const version_offset = 4;
    const flags_offset = 5;
    const frame_type_offset = 6;
    const node_id_offset = 8;
    const consumption_position_offset = 12;
    const receiver_window_offset = 20;

    pub const encoded_length: usize = 28;

    pub fn wrap(buffer: [*]u8) StatusMessageFlyweight {
        return .{ .buffer = buffer };
    }

    // ── Getters ──

    pub inline fn frameLength(self: StatusMessageFlyweight) i32 {
        return readLittleI32(self.buffer + frame_length_offset);
    }

    pub inline fn flags(self: StatusMessageFlyweight) u8 {
        return self.buffer[flags_offset];
    }

    pub inline fn nodeId(self: StatusMessageFlyweight) u8 {
        return self.buffer[node_id_offset];
    }

    pub inline fn consumptionPosition(self: StatusMessageFlyweight) i64 {
        return readLittleI64(self.buffer + consumption_position_offset);
    }

    pub inline fn receiverWindow(self: StatusMessageFlyweight) i32 {
        return readLittleI32(self.buffer + receiver_window_offset);
    }

    // ── Setters ──

    pub fn encode(
        self: StatusMessageFlyweight,
        node_id: u8,
        consumption_pos: i64,
        recv_window: i32,
        sm_flag: u8,
    ) void {
        writeLittleI32(self.buffer + frame_length_offset, @intCast(encoded_length));
        self.buffer[version_offset] = constants.frame_header_version;
        self.buffer[flags_offset] = sm_flag;
        writeLittleU16(self.buffer + frame_type_offset, constants.frame_type_sm);
        self.buffer[node_id_offset] = node_id;
        self.buffer[node_id_offset + 1] = 0; // reserved
        writeLittleU16(self.buffer + node_id_offset + 2, 0); // reserved
        writeLittleI64(self.buffer + consumption_position_offset, consumption_pos);
        writeLittleI32(self.buffer + receiver_window_offset, recv_window);
        writeLittleI32(self.buffer + receiver_window_offset + 4, 0); // reserved
    }

    // ── Little-endian helpers ──

    inline fn readLittleI32(ptr: [*]const u8) i32 {
        return @bitCast(std.mem.readInt(u32, ptr[0..4], .little));
    }

    inline fn readLittleI64(ptr: [*]const u8) i64 {
        return @bitCast(std.mem.readInt(u64, ptr[0..8], .little));
    }

    inline fn writeLittleI32(ptr: [*]u8, val: i32) void {
        std.mem.writeInt(u32, ptr[0..4], @bitCast(val), .little);
    }

    inline fn writeLittleI64(ptr: [*]u8, val: i64) void {
        std.mem.writeInt(u64, ptr[0..8], @bitCast(val), .little);
    }

    inline fn writeLittleU16(ptr: [*]u8, val: u16) void {
        std.mem.writeInt(u16, ptr[0..2], val, .little);
    }
};

// ── Status Message Scheduler ──────────────────────────────────────────

/// Status Message timing controller. Called once per receiver event
/// loop iteration for each peer.
pub const StatusMessageScheduler = struct {
    /// Pre-allocated SM encode buffer (one per peer, avoids allocation).
    sm_buffer: [StatusMessageFlyweight.encoded_length]u8 = undefined,

    const Self = @This();

    /// Check all SM triggers and send if any fire.
    /// Returns 1 if an SM was sent, 0 otherwise.
    pub fn maybeSendStatusMessage(
        self: *Self,
        fc: *ReceiverFlowControl,
        peer_node_id: u8,
        local_node_id: u8,
        now_ns: i64,
        sendFn: *const fn (buf: []const u8, peer_node_id: u8) void,
    ) u32 {
        // Trigger 1: Eager — consumption advanced significantly
        const advance = fc.consumption_position - fc.last_sm_consumption_position;
        const threshold = @divFloor(@as(i64, fc.last_sm_receiver_window), 4);
        const eager = advance >= threshold and threshold > 0;

        // Trigger 2: Periodic — timeout expired
        const periodic = (now_ns - fc.last_sm_sent_ns) >= constants.sm_timeout_ns;

        if (!eager and !periodic) return 0;

        // Calculate current window and encode
        const window = fc.calculateReceiverWindow();
        const flyweight = StatusMessageFlyweight.wrap(&self.sm_buffer);
        flyweight.encode(
            local_node_id,
            fc.consumption_position,
            window,
            sm_flags.none,
        );

        // Send
        sendFn(&self.sm_buffer, peer_node_id);

        // Update tracking state
        fc.last_sm_consumption_position = fc.consumption_position;
        fc.last_sm_receiver_window = window;
        fc.last_sm_sent_ns = now_ns;

        return 1;
    }

    /// Send an SM immediately (e.g., on SETUP received or loss detected).
    pub fn sendImmediate(
        self: *Self,
        fc: *ReceiverFlowControl,
        peer_node_id: u8,
        local_node_id: u8,
        now_ns: i64,
        sm_flag: u8,
        sendFn: *const fn (buf: []const u8, peer_node_id: u8) void,
    ) void {
        const window = fc.calculateReceiverWindow();
        const flyweight = StatusMessageFlyweight.wrap(&self.sm_buffer);
        flyweight.encode(
            local_node_id,
            fc.consumption_position,
            window,
            sm_flag,
        );

        sendFn(&self.sm_buffer, peer_node_id);

        fc.last_sm_consumption_position = fc.consumption_position;
        fc.last_sm_receiver_window = window;
        fc.last_sm_sent_ns = now_ns;
    }
};
