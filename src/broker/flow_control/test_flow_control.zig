//! Unit and integration tests for the flow control subsystem.

const std = @import("std");
const testing = std.testing;
const SenderFlowControl = @import("sender_flow_control.zig").SenderFlowControl;
const receiver_fc = @import("receiver_flow_control.zig");
const ReceiverFlowControl = receiver_fc.ReceiverFlowControl;
const StatusMessageFlyweight = @import("status_message.zig").StatusMessageFlyweight;
const StatusMessageScheduler = @import("status_message.zig").StatusMessageScheduler;
const constants = @import("brz_common").platform.constants;
const ReceiveLogBuffer = @import("brz_common").memory.receive_log.ReceiveLogBuffer;
const counters_mod = @import("counters.zig");
const validateFramePosition = counters_mod.validateFramePosition;

// ═══════════════════════════════════════════════════════════════════════
// 10.1 — Sender Flow Control Unit Tests
// ═══════════════════════════════════════════════════════════════════════

test "canSend returns false when window is exhausted" {
    // Given
    var fc = SenderFlowControl{};
    fc.send_position = 1000;
    fc.send_limit = 1500;

    // When / Then
    try testing.expect(fc.canSend(500)); // exactly at limit
    try testing.expect(!fc.canSend(501)); // one byte over
    try testing.expect(!fc.canSend(1000)); // well over
}

test "canSend returns false before first SM" {
    // Given — fresh state, no SM received yet
    var fc = SenderFlowControl{};

    // When / Then
    try testing.expect(!fc.canSend(1)); // can't send anything
    try testing.expectEqual(@as(i64, 0), fc.send_limit);
}

test "onStatusMessage advances send_limit" {
    // Given
    var fc = SenderFlowControl{};
    fc.send_position = 0;

    // When — first SM arrives
    fc.onStatusMessage(0, 2_097_152, 1000);

    // Then
    try testing.expectEqual(@as(i64, 2_097_152), fc.send_limit);
    try testing.expectEqual(@as(i64, 0), fc.consumption_position);
    try testing.expectEqual(@as(i32, 2_097_152), fc.receiver_window);
    try testing.expect(fc.canSend(1_000_000));
}

test "onStatusMessage never decreases send_limit (monotonicity)" {
    // Given
    var fc = SenderFlowControl{};
    fc.onStatusMessage(1000, 5000, 100);
    try testing.expectEqual(@as(i64, 6000), fc.send_limit);

    // When — stale SM arrives with lower limit
    fc.onStatusMessage(500, 2000, 200);

    // Then — send_limit unchanged
    try testing.expectEqual(@as(i64, 6000), fc.send_limit);
}

test "onStatusMessage advances send_limit with progressing consumption" {
    // Given
    var fc = SenderFlowControl{};
    fc.onStatusMessage(0, 5000, 100);
    try testing.expectEqual(@as(i64, 5000), fc.send_limit);

    // When — receiver consumed 3000 bytes, window still 5000
    fc.onStatusMessage(3000, 5000, 200);

    // Then — send_limit = 3000 + 5000 = 8000
    try testing.expectEqual(@as(i64, 8000), fc.send_limit);
    try testing.expectEqual(@as(i64, 3000), fc.consumption_position);
}

test "onFrameSent advances send_position" {
    // Given
    var fc = SenderFlowControl{};
    fc.send_limit = 10000;

    // When
    fc.onFrameSent(1500);
    fc.onFrameSent(2000);

    // Then
    try testing.expectEqual(@as(i64, 3500), fc.send_position);
    try testing.expect(fc.canSend(6500)); // exactly remaining
    try testing.expect(!fc.canSend(6501));
}

test "remainingWindow returns correct value" {
    // Given
    var fc = SenderFlowControl{};
    fc.send_position = 3000;
    fc.send_limit = 10000;

    // Then
    try testing.expectEqual(@as(i64, 7000), fc.remainingWindow());
}

test "isStale returns false when no SM has ever been received" {
    // Given
    var fc = SenderFlowControl{};

    // Then — never received an SM, so not "stale" — just unconnected
    try testing.expect(!fc.isStale(1_000_000_000, 5_000_000_000));
}

test "isStale returns true when SM is overdue" {
    // Given
    var fc = SenderFlowControl{};
    fc.last_sm_received_ns = 1_000_000_000; // 1 second

    // When — 7 seconds later with a 5 second timeout
    const now_ns: i64 = 7_000_000_000;
    const timeout_ns: i64 = 5_000_000_000;

    // Then
    try testing.expect(fc.isStale(now_ns, timeout_ns));
}

test "reset clears all state" {
    // Given
    var fc = SenderFlowControl{};
    fc.send_position = 5000;
    fc.send_limit = 10000;
    fc.consumption_position = 3000;
    fc.receiver_window = 7000;
    fc.last_sm_received_ns = 999;
    fc.consecutive_flow_control_failures = 42;

    // When
    fc.reset();

    // Then
    try testing.expectEqual(@as(i64, 0), fc.send_position);
    try testing.expectEqual(@as(i64, 0), fc.send_limit);
    try testing.expectEqual(@as(i64, 0), fc.consumption_position);
    try testing.expectEqual(@as(i32, 0), fc.receiver_window);
    try testing.expectEqual(@as(i64, 0), fc.last_sm_received_ns);
    try testing.expectEqual(@as(u64, 0), fc.consecutive_flow_control_failures);
}

// ═══════════════════════════════════════════════════════════════════════
// 10.2 — Receiver Flow Control Unit Tests
// ═══════════════════════════════════════════════════════════════════════

test "calculateReceiverWindow returns half capacity when buffer is empty" {
    // Given
    var log = try ReceiveLogBuffer.allocate(4 * 1024 * 1024);
    defer log.close();
    var fc = ReceiverFlowControl.init(&log);

    // When
    const window = fc.calculateReceiverWindow();

    // Then — empty buffer: available = capacity, capped at capacity/2
    try testing.expectEqual(@as(i32, 2 * 1024 * 1024), window);
}

test "calculateReceiverWindow shrinks as data accumulates" {
    // Given
    const capacity: i64 = 4 * 1024 * 1024;
    var log = try ReceiveLogBuffer.allocate(@intCast(capacity));
    defer log.close();
    var fc = ReceiverFlowControl.init(&log);

    // Simulate: 3MB of data sitting in the buffer (tail advanced,
    // consumption has not)
    const buffered: i64 = 3 * 1024 * 1024;
    log.storeTailPosition(buffered);
    fc.consumption_position = 0;

    // When
    const window = fc.calculateReceiverWindow();

    // Then — available = 4MB - 3MB = 1MB, < cap/2, so window = 1MB
    try testing.expectEqual(@as(i32, 1 * 1024 * 1024), window);
}

test "calculateReceiverWindow returns zero when buffer is full" {
    // Given
    const capacity: i64 = 4 * 1024 * 1024;
    var log = try ReceiveLogBuffer.allocate(@intCast(capacity));
    defer log.close();
    var fc = ReceiverFlowControl.init(&log);

    // Simulate: entire buffer is occupied
    log.storeTailPosition(capacity);
    fc.consumption_position = 0;

    // When
    const window = fc.calculateReceiverWindow();

    // Then
    try testing.expectEqual(@as(i32, 0), window);
}

test "calculateReceiverWindow grows as data is consumed" {
    // Given
    const capacity: i64 = 4 * 1024 * 1024;
    var log = try ReceiveLogBuffer.allocate(@intCast(capacity));
    defer log.close();
    var fc = ReceiverFlowControl.init(&log);

    // Buffer was full, now consumed 2MB
    log.storeTailPosition(capacity);
    fc.consumption_position = 2 * 1024 * 1024;

    // When
    const window = fc.calculateReceiverWindow();

    // Then — available = 4MB - (4MB - 2MB) = 2MB, equals cap/2
    try testing.expectEqual(@as(i32, 2 * 1024 * 1024), window);
}

test "initialWindow is half capacity" {
    // Given
    var log = try ReceiveLogBuffer.allocate(4 * 1024 * 1024);
    defer log.close();
    var fc = ReceiverFlowControl.init(&log);

    // Then
    try testing.expectEqual(@as(i32, 2 * 1024 * 1024), fc.initialWindow());
}

// ═══════════════════════════════════════════════════════════════════════
// 10.3 — Status Message Flyweight Unit Tests
// ═══════════════════════════════════════════════════════════════════════

test "StatusMessageFlyweight round-trips all fields" {
    // Given
    var buf: [StatusMessageFlyweight.encoded_length]u8 = undefined;
    const sm = StatusMessageFlyweight.wrap(&buf);

    // When
    sm.encode(
        7, // node_id
        1_500_000, // consumption_position
        2_097_152, // receiver_window
        0x01, // flags (send_setup)
    );

    // Then
    try testing.expectEqual(@as(i32, 28), sm.frameLength());
    try testing.expectEqual(@as(u8, 0x01), sm.flags());
    try testing.expectEqual(@as(u8, 7), sm.nodeId());
    try testing.expectEqual(@as(i64, 1_500_000), sm.consumptionPosition());
    try testing.expectEqual(@as(i32, 2_097_152), sm.receiverWindow());
}

test "StatusMessageFlyweight encodes correct frame type" {
    // Given
    var buf: [StatusMessageFlyweight.encoded_length]u8 = undefined;
    const sm = StatusMessageFlyweight.wrap(&buf);

    // When
    sm.encode(1, 0, 1000, 0);

    // Then — frame_type at offset 6, little-endian u16
    const frame_type = std.mem.readInt(u16, buf[6..8], .little);
    try testing.expectEqual(constants.frame_type_sm, @as(u8, @intCast(frame_type)));
}

// ═══════════════════════════════════════════════════════════════════════
// 10.4 — Status Message Timing Unit Tests
// ═══════════════════════════════════════════════════════════════════════

test "eager SM fires when consumption advances by window/4" {
    // Given
    var log = try ReceiveLogBuffer.allocate(4 * 1024 * 1024);
    defer log.close();
    var fc = ReceiverFlowControl.init(&log);
    var scheduler = StatusMessageScheduler{};

    // Initial SM sent: window = 2MB, consumption = 0
    fc.last_sm_consumption_position = 0;
    fc.last_sm_receiver_window = 2 * 1024 * 1024;
    fc.last_sm_sent_ns = 0;

    // Consumption advances by exactly window/4 = 512KB
    fc.consumption_position = 512 * 1024;

    const sendFn = struct {
        fn send(_: []const u8, _: u8) void {}
    }.send;

    // When
    const work = scheduler.maybeSendStatusMessage(
        &fc,
        1,
        0,
        1_000_000, // 1ms — well within periodic timeout
        sendFn,
    );

    // Then — eager SM should have fired
    try testing.expectEqual(@as(u32, 1), work);
    try testing.expectEqual(@as(i64, 512 * 1024), fc.last_sm_consumption_position);
}

test "periodic SM fires after timeout even without consumption advance" {
    // Given
    var log = try ReceiveLogBuffer.allocate(4 * 1024 * 1024);
    defer log.close();
    var fc = ReceiverFlowControl.init(&log);
    var scheduler = StatusMessageScheduler{};

    fc.last_sm_consumption_position = 0;
    fc.last_sm_receiver_window = 2 * 1024 * 1024;
    fc.last_sm_sent_ns = 0;
    fc.consumption_position = 0; // no advance

    const sendFn = struct {
        fn send(_: []const u8, _: u8) void {}
    }.send;

    // When — 300ms later (> 200ms timeout)
    const now_ns: i64 = 300 * std.time.ns_per_ms;
    const work = scheduler.maybeSendStatusMessage(&fc, 1, 0, now_ns, sendFn);

    // Then — periodic SM should have fired
    try testing.expectEqual(@as(u32, 1), work);
}

test "no SM sent when neither eager nor periodic threshold met" {
    // Given
    var log = try ReceiveLogBuffer.allocate(4 * 1024 * 1024);
    defer log.close();
    var fc = ReceiverFlowControl.init(&log);
    var scheduler = StatusMessageScheduler{};

    fc.last_sm_consumption_position = 0;
    fc.last_sm_receiver_window = 2 * 1024 * 1024;
    fc.last_sm_sent_ns = 0;
    fc.consumption_position = 100; // tiny advance, well below window/4

    const sendFn = struct {
        fn send(_: []const u8, _: u8) void {}
    }.send;

    // When — 50ms later (< 200ms timeout)
    const now_ns: i64 = 50 * std.time.ns_per_ms;
    const work = scheduler.maybeSendStatusMessage(&fc, 1, 0, now_ns, sendFn);

    // Then — no SM
    try testing.expectEqual(@as(u32, 0), work);
}

// ═══════════════════════════════════════════════════════════════════════
// 10.5 — Integration Tests
// ═══════════════════════════════════════════════════════════════════════

test "full flow control cycle: send → consume → SM → send more" {
    // Given — sender with initial window from first SM
    var sender_fc = SenderFlowControl{};
    sender_fc.onStatusMessage(0, 2_097_152, 100); // 2MB window

    // Simulate sending 1.5MB
    sender_fc.onFrameSent(1_500_000);
    try testing.expectEqual(@as(i64, 1_500_000), sender_fc.send_position);
    try testing.expect(sender_fc.canSend(500_000)); // 597,152 remaining

    // Simulate sending remaining window
    sender_fc.onFrameSent(597_152);
    try testing.expect(!sender_fc.canSend(1)); // window exhausted

    // Receiver consumed 1MB, sends new SM
    sender_fc.onStatusMessage(1_000_000, 2_097_152, 200);
    // new send_limit = 1_000_000 + 2_097_152 = 3_097_152
    try testing.expectEqual(@as(i64, 3_097_152), sender_fc.send_limit);

    // Sender can now send again
    const remaining = sender_fc.send_limit - sender_fc.send_position;
    try testing.expect(remaining > 0);
    try testing.expect(sender_fc.canSend(1));
}

test "back-pressure: zero window stops sender" {
    // Given
    var sender_fc = SenderFlowControl{};
    sender_fc.onStatusMessage(0, 1_000_000, 100);

    // Send up to limit
    sender_fc.onFrameSent(1_000_000);
    try testing.expect(!sender_fc.canSend(1));

    // Receiver sends SM with zero window (buffer full)
    sender_fc.onStatusMessage(0, 0, 200);
    // send_limit = max(1_000_000, 0 + 0) = 1_000_000 (monotonicity)
    try testing.expect(!sender_fc.canSend(1)); // still blocked

    // Receiver consumes everything, sends SM with full window
    sender_fc.onStatusMessage(1_000_000, 2_097_152, 300);
    // send_limit = 1_000_000 + 2_097_152 = 3_097_152
    try testing.expect(sender_fc.canSend(1)); // unblocked
}

test "receiver restart detected via send_setup flag" {
    // Given — established connection
    var sender_fc = SenderFlowControl{};
    sender_fc.onStatusMessage(0, 2_097_152, 100);
    sender_fc.onFrameSent(1_000_000);
    try testing.expectEqual(@as(i64, 1_000_000), sender_fc.send_position);

    // When — receiver restarts, sends SM with consumption=0
    // The proposed_limit = 0 + 2_097_152 = 2_097_152 <= current 2_097_152
    // → send_limit doesn't advance (monotonicity protects us)
    sender_fc.onStatusMessage(0, 2_097_152, 200);
    try testing.expectEqual(@as(i64, 2_097_152), sender_fc.send_limit);

    // Recovery: reset (triggered by send_setup flag handling)
    sender_fc.reset();
    try testing.expectEqual(@as(i64, 0), sender_fc.send_position);
    try testing.expectEqual(@as(i64, 0), sender_fc.send_limit);

    // New initial SM after SETUP
    sender_fc.onStatusMessage(0, 2_097_152, 300);
    try testing.expectEqual(@as(i64, 2_097_152), sender_fc.send_limit);
    try testing.expect(sender_fc.canSend(1_000_000));
}

test "validateFramePosition detects under-run" {
    // Given
    var log = try ReceiveLogBuffer.allocate(4 * 1024 * 1024);
    defer log.close();
    var fc = ReceiverFlowControl.init(&log);
    fc.consumption_position = 10000;

    // When — frame entirely behind consumption position
    const result = validateFramePosition(&fc, 0, 5000);

    // Then
    try testing.expectEqual(counters_mod.FramePositionValidity.under_run, result);
}

test "validateFramePosition detects over-run" {
    // Given
    var log = try ReceiveLogBuffer.allocate(4 * 1024 * 1024);
    defer log.close();
    var fc = ReceiverFlowControl.init(&log);
    fc.consumption_position = 0;

    // When — frame beyond receivable range
    const result = validateFramePosition(&fc, 5 * 1024 * 1024, 1000);

    // Then
    try testing.expectEqual(counters_mod.FramePositionValidity.over_run, result);
}

test "validateFramePosition accepts valid frame" {
    // Given
    var log = try ReceiveLogBuffer.allocate(4 * 1024 * 1024);
    defer log.close();
    var fc = ReceiverFlowControl.init(&log);
    fc.consumption_position = 0;

    // When — frame within valid range
    const result = validateFramePosition(&fc, 1000, 500);

    // Then
    try testing.expectEqual(counters_mod.FramePositionValidity.valid, result);
}
