//! ServiceClient — client-side proxy for sending messages to a named service.
//!
//! Encapsulates service discovery, instance tracking, and load balancing.
//! Automatically routes via same-host direct path or cross-host broker-routed
//! path depending on the target instance's node_id.

const std = @import("std");
const IpcProducer = @import("../ipc/ipc_producer.zig").IpcProducer;
const ServiceInstance = @import("service_instance.zig").ServiceInstance;
const load_balancer = @import("load_balancer.zig");
const message_header = @import("../message/message_header.zig");
const memory = @import("../memory.zig");
const constants = @import("../memory/constants.zig");
const RingBuffer = @import("../concurrent/ring_buffer.zig").RingBuffer;

const BrokerMetadataFile = memory.BrokerMetadataFile;
const MessageHeader = message_header.MessageHeader;

pub const ServiceClient = struct {
    service_name: []const u8,
    instances: std.ArrayListUnmanaged(ServiceInstance),
    allocator: std.mem.Allocator,
    balancer: load_balancer.ClientLoadBalancer,

    /// IPC context — set during BrzEngine initialization.
    broker_meta: ?*BrokerMetadataFile,
    local_node_id: i16,
    local_service_id: i32,

    const Self = @This();

    pub const SendError = error{
        NoAvailableInstance,
        ProducerNotInitialized,
        SendBufferFull,
        NoLeaderAvailable,
    } || RingBuffer.WriteError;

    pub fn init(
        allocator: std.mem.Allocator,
        service_name: []const u8,
        broker_meta: ?*BrokerMetadataFile,
        local_node_id: i16,
        local_service_id: i32,
    ) Self {
        return .{
            .service_name = service_name,
            .instances = .{},
            .allocator = allocator,
            .balancer = .{ .round_robin = .{} },
            .broker_meta = broker_meta,
            .local_node_id = local_node_id,
            .local_service_id = local_service_id,
        };
    }

    /// Send a message to one instance of this service, selected by the
    /// load balancer.
    pub fn send(self: *Self, payload: []const u8) SendError!void {
        const instance = self.balancer.next(self.instances.items) orelse
            return error.NoAvailableInstance;

        if (instance.node_id == self.local_node_id) {
            // Same-host direct path.
            const producer = instance.ipc_producer orelse
                return error.ProducerNotInitialized;
            try producer.write(constants.application_msg_type_id, payload);
        } else {
            // Cross-host routed path.
            try self.sendToRemoteService(
                instance.node_id,
                instance.service_id,
                payload,
            );
        }
    }

    /// Send a message to a specific instance (bypasses load balancer).
    pub fn sendTo(self: *Self, target_service_id: i32, payload: []const u8) SendError!void {
        const instance = self.findInstance(target_service_id) orelse
            return error.NoAvailableInstance;

        if (instance.node_id == self.local_node_id) {
            const producer = instance.ipc_producer orelse
                return error.ProducerNotInitialized;
            try producer.write(constants.application_msg_type_id, payload);
        } else {
            try self.sendToRemoteService(
                instance.node_id,
                instance.service_id,
                payload,
            );
        }
    }

    /// Send to the leader instance only.
    pub fn sendToLeader(self: *Self, payload: []const u8) SendError!void {
        for (self.instances.items) |*inst| {
            if (inst.is_leader) {
                if (inst.node_id == self.local_node_id) {
                    const producer = inst.ipc_producer orelse
                        return error.ProducerNotInitialized;
                    try producer.write(constants.application_msg_type_id, payload);
                } else {
                    try self.sendToRemoteService(
                        inst.node_id,
                        inst.service_id,
                        payload,
                    );
                }
                return;
            }
        }
        return error.NoLeaderAvailable;
    }

    // ── Instance Management ───────────────────────────────────────────

    pub fn addInstance(self: *Self, instance: ServiceInstance) !void {
        try self.instances.append(self.allocator, instance);
    }

    pub fn removeInstance(self: *Self, service_id: i32) void {
        var i: usize = 0;
        while (i < self.instances.items.len) {
            if (self.instances.items[i].service_id == service_id) {
                _ = self.instances.swapRemove(i);
                return;
            }
            i += 1;
        }
    }

    pub fn updateLeader(self: *Self, leader_service_id: i32) void {
        for (self.instances.items) |*inst| {
            inst.is_leader = (inst.service_id == leader_service_id);
        }
    }

    pub fn instanceCount(self: *const Self) usize {
        return self.instances.items.len;
    }

    fn findInstance(self: *const Self, service_id: i32) ?*ServiceInstance {
        for (self.instances.items) |*inst| {
            if (inst.service_id == service_id) return inst;
        }
        return null;
    }

    pub fn deinit(self: *Self) void {
        self.instances.deinit(self.allocator);
    }

    // ── Internal: Cross-Host Send ─────────────────────────────────────

    fn sendToRemoteService(
        self: *Self,
        target_node_id: i16,
        target_service_id: i32,
        payload: []const u8,
    ) SendError!void {
        const broker = self.broker_meta orelse return error.SendBufferFull;
        const send_buf = broker.getSendBuffer();

        // We need at least ring_buffer_alignment alignment for RingBuffer.init.
        // The send buffer from BrokerMetadataFile is part of an aligned mmap region.
        var send_rb = RingBuffer.init(
            @alignCast(send_buf),
            false,
            null,
            null,
        ) catch return error.SendBufferFull;

        const total_len = MessageHeader.encoded_length + payload.len;
        var claim = send_rb.tryClaim(constants.application_msg_type_id, total_len) orelse
            return error.SendBufferFull;

        // Write the BRZ message header into the claimed region.
        MessageHeader.encode(claim.buffer[0..MessageHeader.encoded_length], .{
            .source_node_id = self.local_node_id,
            .source_service_id = @intCast(self.local_service_id),
            .target_node_id = target_node_id,
            .target_service_id = @intCast(target_service_id),
            .template_id = 0,
            .correlation_id = 0,
            .flags = 0,
        });

        // Copy the application payload immediately after the header.
        @memcpy(claim.buffer[MessageHeader.encoded_length..][0..payload.len], payload);

        // Commit — makes the message visible to the broker's sender event loop.
        claim.commit();
    }
};
