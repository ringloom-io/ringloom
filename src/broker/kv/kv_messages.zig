// SPDX-License-Identifier: Apache-2.0
//! Broker-side KV messages (docs/kv-store-spec.md §12).
//!
//! * Re-exports the service↔broker control messages from `ringloom_common`
//!   (templates 16–24), the same way `topics/topic_messages.zig` does.
//! * Defines the **admin-plane** envelopes (broker↔broker, over the admin UDP
//!   stream) used to forward writer-lease traffic:
//!
//!   - `KvAdminRequest` (admin template 19): local broker → topic leader. Wraps a
//!     service request (`RegisterKvWriter`, `KvWriterReassert`,
//!     `UnregisterKvWriter`) verbatim, plus the originating node, so the leader
//!     can address replies. The service id is inside the wrapped message
//!     (`local_service_id`).
//!   - `KvAdminReply` (admin template 20): topic leader → originating broker.
//!     Wraps a broker→service message (`KvWriterResponse`, `KvWriterGranted`,
//!     `KvWriterRevoked`, `KvWriterReassertResponse`) verbatim plus the
//!     destination service id; the receiving broker writes `bytes` to that
//!     service's control buffer unchanged.
//!
//! Admin templates 1–5, 9–11 are cluster admin, 12–18 topics; 19–20 are KV.
//! `OpenKvReader` is never forwarded.

const std = @import("std");
const common = @import("ringloom_common");
pub const msgs = common.message.kv_control_messages;

pub const TEMPLATE_KV_REQUEST: u16 = 19;
pub const TEMPLATE_KV_REPLY: u16 = 20;

/// Max wrapped service message size.
pub const kv_admin_bytes_max: usize = msgs.max_message_size;

pub const KvAdminRequestBody = extern struct {
    origin_node_id: u8,
    _pad0: u8 = 0,
    /// Length of `bytes` used.
    len: u16,
    _pad1: [4]u8 = @splat(0),
    bytes: [kv_admin_bytes_max]u8,

    pub fn payload(self: *const KvAdminRequestBody) []const u8 {
        return self.bytes[0..@min(@as(usize, self.len), kv_admin_bytes_max)];
    }
};

pub const KvAdminReplyBody = extern struct {
    dest_node_id: u8,
    _pad0: u8 = 0,
    len: u16,
    /// Service on `dest_node_id` that receives `bytes`.
    dest_service_id: i32 align(1),
    bytes: [kv_admin_bytes_max]u8,

    pub fn payload(self: *const KvAdminReplyBody) []const u8 {
        return self.bytes[0..@min(@as(usize, self.len), kv_admin_bytes_max)];
    }
};

pub fn makeRequest(origin_node_id: u8, service_msg: []const u8) ?KvAdminRequestBody {
    if (service_msg.len > kv_admin_bytes_max) return null;
    var b = KvAdminRequestBody{ .origin_node_id = origin_node_id, .len = @intCast(service_msg.len), .bytes = @splat(0) };
    @memcpy(b.bytes[0..service_msg.len], service_msg);
    return b;
}

pub fn makeReply(dest_node_id: u8, dest_service_id: i32, service_msg: []const u8) ?KvAdminReplyBody {
    if (service_msg.len > kv_admin_bytes_max) return null;
    var b = KvAdminReplyBody{ .dest_node_id = dest_node_id, .len = @intCast(service_msg.len), .dest_service_id = dest_service_id, .bytes = @splat(0) };
    @memcpy(b.bytes[0..service_msg.len], service_msg);
    return b;
}

/// Which service request a wrapped/received control message is.
pub fn templateOf(bytes: []const u8) ?u16 {
    if (bytes.len < 2) return null;
    return std.mem.readInt(u16, bytes[0..2], .little);
}

const testing = std.testing;

test "admin envelopes wrap service messages verbatim" {
    var buf: [msgs.max_message_size]u8 = undefined;
    const n = msgs.encodeRegisterKvWriter(&buf, 9, 77, .{}, .wait, "prices");
    const req = makeRequest(3, buf[0..n]).?;
    try testing.expectEqual(@as(u8, 3), req.origin_node_id);
    try testing.expectEqualSlices(u8, buf[0..n], req.payload());
    try testing.expectEqual(msgs.TEMPLATE_REGISTER_KV_WRITER, templateOf(req.payload()).?);
    const m = msgs.decode(msgs.RegisterKvWriterMsg, req.payload()).?;
    try testing.expectEqual(@as(i32, 9), m.local_service_id);

    var rb: [msgs.max_message_size]u8 = undefined;
    const rn = msgs.encodeKvWriterResponse(&rb, .queued, 1, 77, 0, 4, 2, .{});
    const rep = makeReply(5, 9, rb[0..rn]).?;
    try testing.expectEqual(@as(i32, 9), rep.dest_service_id);
    const r = msgs.decode(msgs.KvWriterResponseMsg, rep.payload()).?;
    try testing.expectEqual(msgs.KvStatus.queued, msgs.statusOf(r.status).?);

    var big: [kv_admin_bytes_max + 1]u8 = undefined;
    try testing.expect(makeRequest(1, &big) == null);
    try testing.expect(makeReply(1, 1, &big) == null);
}
