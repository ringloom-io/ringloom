// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const ringloom_service = @import("ringloom_service");

const ServiceClient = ringloom_service.ServiceClient;

pub const ProtocolVersion: u8 = 1;
pub const max_bulk_orders = 4;

pub const TemplateId = enum(u16) {
    new_order = 1001,
    cancel_order = 1002,
    risk_check_request = 1101,
    risk_accepted = 1102,
    order_rejected = 1103,
    fill = 1201,
    execution_report = 1301,
    portfolio_snapshot_request = 1401,
    portfolio_snapshot = 1402,
    bulk_order_batch = 1501,
};

pub const Stage = enum(u8) {
    simulator = 1,
    gateway = 2,
    risk = 3,
    matching = 4,
    execution = 5,
    portfolio = 6,
};

pub const Symbol = enum(u16) {
    aapl = 1,
    msft = 2,
    nvda = 3,
    zig = 4,
};

pub const Side = enum(u8) {
    buy = 1,
    sell = 2,
};

pub const TimeInForce = enum(u8) {
    day = 1,
    ioc = 2,
};

pub const RejectReason = enum(u16) {
    unknown_symbol = 1,
    zero_quantity = 2,
    bad_price = 3,
    malformed = 4,
    unsupported_version = 5,
    system_busy = 6,
    risk_credit = 7,
    risk_symbol_limit = 8,
    unsupported_message = 9,
};

pub const ExecutionStatus = enum(u8) {
    filled = 1,
    rejected = 2,
};

pub const Envelope = extern struct {
    correlation_id: i64,
    created_ns: u64,
    stage_ns: u64,
    payload_len: u16,
    version: u8,
    flags: u8,
    source_stage: Stage,
    reserved: [3]u8,
};

pub const EnvelopeFields = struct {
    correlation_id: i64,
    created_ns: u64,
    stage_ns: u64,
    payload_len: u16 = 0,
    flags: u8 = 0,
    source_stage: Stage,
};

pub const NewOrder = extern struct {
    account_id: u32,
    order_id: u64,
    symbol: Symbol,
    side: Side,
    quantity: u32,
    price_nanos: i64,
    tif: TimeInForce,
};

pub const CancelOrder = extern struct {
    account_id: u32,
    order_id: u64,
    cancel_order_id: u64,
    symbol: Symbol,
};

pub const RiskCheckRequest = extern struct {
    gateway_sequence: u64,
    order: NewOrder,
};

pub const RiskAccepted = extern struct {
    gateway_sequence: u64,
    order: NewOrder,
    accepted_notional_nanos: i64,
};

pub const OrderRejected = extern struct {
    account_id: u32,
    order_id: u64,
    symbol: Symbol,
    reason: RejectReason,
    stage: Stage,
};

pub const Fill = extern struct {
    account_id: u32,
    order_id: u64,
    symbol: Symbol,
    side: Side,
    quantity: u32,
    price_nanos: i64,
    gateway_sequence: u64,
};

pub const ExecutionReport = extern struct {
    account_id: u32,
    order_id: u64,
    symbol: Symbol,
    side: Side,
    quantity: u32,
    price_nanos: i64,
    status: ExecutionStatus,
};

pub const PortfolioSnapshotRequest = extern struct {
    account_id: u32,
};

pub const PortfolioSnapshot = extern struct {
    account_id: u32,
    positions: [max_bulk_orders]i64,
    updates_applied: u64,
};

pub const BulkOrderBatch = extern struct {
    batch_id: u64,
    count: u16,
    orders: [max_bulk_orders]NewOrder,
};

pub const Error = error{
    BufferTooSmall,
    InvalidEnvelope,
    InvalidPayload,
    UnknownTemplate,
};

pub const ClaimedMessage = struct {
    claim: ServiceClient.SendClaim,
    payload: []u8,
    committed: bool = false,

    pub fn body(self: *ClaimedMessage) []u8 {
        return self.payload[@sizeOf(Envelope)..];
    }

    pub fn commit(self: *ClaimedMessage) void {
        self.claim.commit();
        self.committed = true;
    }

    pub fn abortUnlessCommitted(self: *ClaimedMessage) void {
        if (!self.committed) {
            self.claim.abort();
        }
    }
};

pub fn templateFromMsgType(msg_type_id: i32) Error!TemplateId {
    if (msg_type_id < 0 or msg_type_id > std.math.maxInt(u16)) return error.UnknownTemplate;
    return switch (@as(u16, @intCast(msg_type_id))) {
        @intFromEnum(TemplateId.new_order) => .new_order,
        @intFromEnum(TemplateId.cancel_order) => .cancel_order,
        @intFromEnum(TemplateId.risk_check_request) => .risk_check_request,
        @intFromEnum(TemplateId.risk_accepted) => .risk_accepted,
        @intFromEnum(TemplateId.order_rejected) => .order_rejected,
        @intFromEnum(TemplateId.fill) => .fill,
        @intFromEnum(TemplateId.execution_report) => .execution_report,
        @intFromEnum(TemplateId.portfolio_snapshot_request) => .portfolio_snapshot_request,
        @intFromEnum(TemplateId.portfolio_snapshot) => .portfolio_snapshot,
        @intFromEnum(TemplateId.bulk_order_batch) => .bulk_order_batch,
        else => error.UnknownTemplate,
    };
}

pub fn claimMessage(
    client: *ServiceClient,
    template_id: TemplateId,
    body_size: usize,
) ServiceClient.SendError!ClaimedMessage {
    const claim = try client.tryClaim(@intFromEnum(template_id), @sizeOf(Envelope) + body_size);
    const payload = claim.payload;
    return .{
        .claim = claim,
        .payload = payload,
    };
}

pub fn writeEnvelope(dest: []u8, fields: EnvelopeFields) Error!void {
    if (dest.len < @sizeOf(Envelope)) return error.BufferTooSmall;
    const envelope = Envelope{
        .correlation_id = fields.correlation_id,
        .created_ns = fields.created_ns,
        .stage_ns = fields.stage_ns,
        .payload_len = fields.payload_len,
        .version = ProtocolVersion,
        .flags = fields.flags,
        .source_stage = fields.source_stage,
        .reserved = .{ 0, 0, 0 },
    };
    @memcpy(dest[0..@sizeOf(Envelope)], std.mem.asBytes(&envelope));
}

pub fn writeBody(comptime T: type, dest: []u8, body: T) Error!void {
    if (dest.len < @sizeOf(T)) return error.BufferTooSmall;
    @memcpy(dest[0..@sizeOf(T)], std.mem.asBytes(&body));
}

pub fn fillMessage(
    comptime T: type,
    dest: []u8,
    fields: EnvelopeFields,
    body: T,
) Error![]u8 {
    const total_len = @sizeOf(Envelope) + @sizeOf(T);
    if (dest.len < total_len) return error.BufferTooSmall;
    var envelope_fields = fields;
    envelope_fields.payload_len = @sizeOf(T);
    try writeEnvelope(dest, envelope_fields);
    try writeBody(T, dest[@sizeOf(Envelope)..][0..@sizeOf(T)], body);
    return dest[0..total_len];
}

pub fn decodeEnvelope(src: []const u8) Error!*const Envelope {
    if (src.len < @sizeOf(Envelope)) return error.InvalidEnvelope;
    const envelope: *const Envelope = @ptrCast(@alignCast(src.ptr));
    if (envelope.version != ProtocolVersion) return error.InvalidEnvelope;
    if (src.len < @sizeOf(Envelope) + envelope.payload_len) return error.InvalidPayload;
    return envelope;
}

pub fn payloadAs(comptime T: type, src: []const u8) Error!*const T {
    const envelope = try decodeEnvelope(src);
    if (envelope.payload_len < @sizeOf(T)) return error.InvalidPayload;
    const body_start = @sizeOf(Envelope);
    return @ptrCast(@alignCast(src[body_start..].ptr));
}

pub fn copyPayloadAs(comptime T: type, src: []const u8) Error!T {
    const envelope = try decodeEnvelope(src);
    if (envelope.payload_len < @sizeOf(T)) return error.InvalidPayload;
    var value: T = undefined;
    @memcpy(std.mem.asBytes(&value), src[@sizeOf(Envelope)..][0..@sizeOf(T)]);
    return value;
}

pub fn sendClaimed(
    comptime T: type,
    client: *ServiceClient,
    template_id: TemplateId,
    fields: EnvelopeFields,
    body: T,
) ServiceClient.SendError!void {
    var msg = try claimMessage(client, template_id, @sizeOf(T));
    errdefer msg.abortUnlessCommitted();
    _ = fillMessage(T, msg.payload, fields, body) catch return error.MessageTooLong;
    msg.commit();
}

pub fn messageCapacity() usize {
    return @sizeOf(Envelope) + @sizeOf(BulkOrderBatch);
}

comptime {
    std.debug.assert(@sizeOf(Envelope) == 32);
    std.debug.assert(@alignOf(NewOrder) <= 8);
    std.debug.assert(@alignOf(BulkOrderBatch) <= 8);
}

test "templateFromMsgType decodes sample template IDs" {
    try std.testing.expectEqual(TemplateId.new_order, try templateFromMsgType(1001));
    try std.testing.expectEqual(TemplateId.execution_report, try templateFromMsgType(1301));
    try std.testing.expectError(error.UnknownTemplate, templateFromMsgType(99));
}

test "envelope and new order round trip without allocation" {
    var buf: [messageCapacity()]u8 align(8) = undefined;
    const order = NewOrder{
        .account_id = 1001,
        .order_id = 7,
        .symbol = .aapl,
        .side = .buy,
        .quantity = 10,
        .price_nanos = 185_000_000_000,
        .tif = .day,
    };
    const encoded = try fillMessage(NewOrder, &buf, .{
        .correlation_id = 7,
        .created_ns = 11,
        .stage_ns = 12,
        .source_stage = .simulator,
    }, order);

    const envelope = try decodeEnvelope(encoded);
    try std.testing.expectEqual(@as(i64, 7), envelope.correlation_id);
    try std.testing.expectEqual(@as(u16, @sizeOf(NewOrder)), envelope.payload_len);
    const decoded = try payloadAs(NewOrder, encoded);
    try std.testing.expectEqual(order.order_id, decoded.order_id);
    try std.testing.expectEqual(order.symbol, decoded.symbol);
}
