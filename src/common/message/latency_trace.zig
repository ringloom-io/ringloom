//! Helpers for benchmark latency tracing embedded in message payloads.
//!
//! The layout is intentionally compact so it fits inside the 32-byte benchmark
//! payload used by the smallest remote latency case:
//!
//!   0..7   send timestamp set by ping service
//!   8      phase flag (0=warmup, 1=measured)
//!   9..12  magic marker ("RING")
//!   13..20 broker A sender dequeue timestamp
//!   21..28 broker B receiver ingress timestamp

const std = @import("std");

pub const warmup_phase: u8 = 0;
pub const measured_phase: u8 = 1;

pub const min_basic_len: usize = 9;
pub const min_trace_len: usize = 29;

const send_ts_offset = 0;
const phase_offset = 8;
const magic_offset = 9;
const sender_dequeue_offset = 13;
const receiver_ingress_offset = 21;
const magic: u32 = 0x474E4952; // "RING" when written little-endian

pub const StageTrace = struct {
    send_ts_ns: u64,
    sender_dequeue_ns: u64,
    receiver_ingress_ns: u64,
};

pub fn embedSend(payload: []u8, phase: u8, send_ts_ns: u64) void {
    if (payload.len < min_basic_len) return;

    std.mem.writeInt(u64, payload[send_ts_offset .. send_ts_offset + 8], send_ts_ns, .little);
    payload[phase_offset] = phase;

    if (payload.len < min_trace_len) return;

    std.mem.writeInt(u32, payload[magic_offset .. magic_offset + 4], magic, .little);
    std.mem.writeInt(u64, payload[sender_dequeue_offset .. sender_dequeue_offset + 8], 0, .little);
    std.mem.writeInt(u64, payload[receiver_ingress_offset .. receiver_ingress_offset + 8], 0, .little);
}

pub fn isMeasured(payload: []const u8) bool {
    return payload.len >= min_basic_len and payload[phase_offset] == measured_phase;
}

pub fn readSendTimestamp(payload: []const u8) ?u64 {
    if (payload.len < min_basic_len) return null;
    return std.mem.readInt(u64, payload[send_ts_offset .. send_ts_offset + 8], .little);
}

pub fn stampSenderDequeue(payload: []u8, timestamp_ns: u64) void {
    if (!hasTraceMarker(payload)) return;
    std.mem.writeInt(u64, payload[sender_dequeue_offset .. sender_dequeue_offset + 8], timestamp_ns, .little);
}

pub fn stampReceiverIngress(payload: []u8, timestamp_ns: u64) void {
    if (!hasTraceMarker(payload)) return;
    std.mem.writeInt(u64, payload[receiver_ingress_offset .. receiver_ingress_offset + 8], timestamp_ns, .little);
}

pub fn readStageTrace(payload: []const u8) ?StageTrace {
    if (!hasTraceMarker(payload)) return null;

    const send_ts_ns = std.mem.readInt(u64, payload[send_ts_offset .. send_ts_offset + 8], .little);
    const sender_dequeue_ns = std.mem.readInt(u64, payload[sender_dequeue_offset .. sender_dequeue_offset + 8], .little);
    const receiver_ingress_ns = std.mem.readInt(u64, payload[receiver_ingress_offset .. receiver_ingress_offset + 8], .little);

    if (send_ts_ns == 0 or sender_dequeue_ns == 0 or receiver_ingress_ns == 0) return null;
    if (sender_dequeue_ns < send_ts_ns or receiver_ingress_ns < sender_dequeue_ns) return null;

    return .{
        .send_ts_ns = send_ts_ns,
        .sender_dequeue_ns = sender_dequeue_ns,
        .receiver_ingress_ns = receiver_ingress_ns,
    };
}

fn hasTraceMarker(payload: []const u8) bool {
    return payload.len >= min_trace_len and
        std.mem.readInt(u32, payload[magic_offset .. magic_offset + 4], .little) == magic;
}

test "embedSend writes basic timestamp and phase" {
    var payload: [12]u8 = [_]u8{0xAA} ** 12;

    embedSend(&payload, measured_phase, 1234);

    try std.testing.expectEqual(@as(u64, 1234), readSendTimestamp(&payload).?);
    try std.testing.expect(isMeasured(&payload));
}

test "embedSend resets trace timestamps for retried sends" {
    var payload: [32]u8 = [_]u8{0xAA} ** 32;

    embedSend(&payload, measured_phase, 100);
    stampSenderDequeue(&payload, 200);
    stampReceiverIngress(&payload, 300);
    embedSend(&payload, measured_phase, 400);

    try std.testing.expectEqual(@as(u64, 400), readSendTimestamp(&payload).?);
    try std.testing.expect(readStageTrace(&payload) == null);
}

test "stage trace is readable after sender and receiver stamps" {
    var payload: [32]u8 = [_]u8{0xAA} ** 32;

    embedSend(&payload, measured_phase, 100);
    stampSenderDequeue(&payload, 250);
    stampReceiverIngress(&payload, 400);

    const trace = readStageTrace(&payload).?;
    try std.testing.expectEqual(@as(u64, 100), trace.send_ts_ns);
    try std.testing.expectEqual(@as(u64, 250), trace.sender_dequeue_ns);
    try std.testing.expectEqual(@as(u64, 400), trace.receiver_ingress_ns);
}
