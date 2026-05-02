//! ControlAgent — polls the service's control ring buffer (Channel 2) for
//! messages from the broker, and periodically writes heartbeats.

const std = @import("std");
const brz_common = @import("brz_common");
const platform = brz_common.platform;
const memory = brz_common.memory;
const ring_buffer = brz_common.concurrent.ring_buffer;
const constants = brz_common.memory.constants;
const control_encoding = brz_common.message.control_encoding;
const ServiceClientRegistry = @import("service_client_registry.zig").ServiceClientRegistry;

const RingBuffer = ring_buffer.RingBuffer;
const ServiceMetadataFile = memory.ServiceMetadataFile;
const BrokerMetadataFile = memory.BrokerMetadataFile;
const Clock = platform.Clock;

/// Template IDs for control messages (matches the encoding module).
pub const TemplateId = struct {
    pub const register_service: u16 = 1;
    pub const registration_response: u16 = 2;
    pub const subscribe_to_service_updates: u16 = 3;
    pub const service_instances: u16 = 4;
    pub const unregister_service: u16 = 5;
    pub const leader_changed: u16 = 6;
};

/// Heartbeat write interval in nanoseconds.
const heartbeat_write_interval_ns: i64 =
    constants.service_heartbeat_write_interval_ms * std.time.ns_per_ms;

pub const ControlAgent = struct {
    control_rb: RingBuffer,
    service_meta: *ServiceMetadataFile,
    broker_meta: *BrokerMetadataFile,
    service_registry: *ServiceClientRegistry,
    last_heartbeat_ns: i64,

    const Self = @This();

    pub fn init(
        service_meta: *ServiceMetadataFile,
        broker_meta: *BrokerMetadataFile,
        service_registry: *ServiceClientRegistry,
    ) !Self {
        return .{
            .control_rb = try RingBuffer.init(
                @alignCast(service_meta.getControlBuffer()),
                false,
                null,
                null,
            ),
            .service_meta = service_meta,
            .broker_meta = broker_meta,
            .service_registry = service_registry,
            .last_heartbeat_ns = Clock.monotonicNanos(),
        };
    }

    /// Duty-cycle function. Returns total work count.
    pub fn doWork(self: *Self) u32 {
        var work_count: u32 = 0;

        // 1. Poll control ring buffer for broker messages.
        work_count += self.pollControlMessages();

        // 2. Write heartbeat if interval has elapsed.
        if (self.shouldWriteHeartbeat()) {
            self.service_meta.storeHeartbeat(Clock.epochMillis());
            self.last_heartbeat_ns = Clock.monotonicNanos();
            work_count += 1;
        }

        return work_count;
    }

    /// EventLoop-compatible function pointer.
    pub fn doWorkFn(ctx: *anyopaque) u32 {
        const self: *Self = @ptrCast(@alignCast(ctx));
        return self.doWork();
    }

    /// No-op close function for EventLoop compatibility.
    pub fn onCloseFn(_: *anyopaque) void {}

    fn pollControlMessages(self: *Self) u32 {
        // Set file-level context so the bare function pointer handler can
        // reach this agent instance. Safe because ControlAgent runs on a
        // single dedicated thread.
        dispatch_agent_ptr = self;
        return self.control_rb.read(&dispatchWrapper, constants.control_read_limit);
    }

    /// Dispatch a single control message based on its template ID.
    pub fn dispatchControlMessage(self: *Self, payload: []const u8) void {
        const template_id = control_encoding.readTemplateId(payload);

        switch (template_id) {
            TemplateId.registration_response => {
                // Late registration response — log and ignore.
            },

            TemplateId.service_instances => {
                // Broker sends current set of instances for a subscribed service.
                const inst = control_encoding.decodeServiceInstance(payload);
                self.service_registry.addOrUpdateInstance(.{
                    .service_id = inst.service_id,
                    .service_name = inst.service_name,
                    .node_id = inst.node_id,
                    .is_leader = inst.is_leader,
                });
            },

            TemplateId.leader_changed => {
                const leader_info = control_encoding.decodeLeaderChanged(payload);
                self.service_registry.updateLeader(
                    leader_info.service_name,
                    leader_info.leader_service_id,
                );
            },

            else => {
                // Unknown template ID — ignore for forward compatibility.
            },
        }
    }

    fn shouldWriteHeartbeat(self: *const Self) bool {
        const now = Clock.monotonicNanos();
        return (now - self.last_heartbeat_ns) >= heartbeat_write_interval_ns;
    }
};

// File-level state for the dispatch handler — set before each read() call.
// Safe: ControlAgent runs on a single dedicated thread.
var dispatch_agent_ptr: ?*ControlAgent = null;

fn dispatchWrapper(_: i32, payload: []const u8) void {
    if (dispatch_agent_ptr) |agent| {
        agent.dispatchControlMessage(payload);
    }
}

// ── Free Functions for Registration ───────────────────────────────────

/// Writes a RegisterService message into the broker's control ring buffer.
pub fn registerWithBroker(
    broker_meta: *BrokerMetadataFile,
    service_id: i32,
    service_name: []const u8,
    leader_election_enabled: bool,
) !void {
    var control_rb = try RingBuffer.init(
        @alignCast(broker_meta.getControlBuffer()),
        false,
        null,
        null,
    );

    var msg_buf: [256]u8 = undefined;
    const msg_len = control_encoding.encodeRegisterService(&msg_buf, .{
        .service_id = service_id,
        .service_name = service_name,
        .leader_election_enabled = leader_election_enabled,
    });

    try control_rb.write(constants.control_msg_type_id, msg_buf[0..msg_len]);
}

/// Writes an UnregisterService message into the broker's control ring buffer.
pub fn unregisterFromBroker(
    broker_meta: *BrokerMetadataFile,
    service_id: i32,
) !void {
    var control_rb = try RingBuffer.init(
        @alignCast(broker_meta.getControlBuffer()),
        false,
        null,
        null,
    );

    var msg_buf: [256]u8 = undefined;
    const msg_len = control_encoding.encodeUnregisterService(&msg_buf, .{
        .service_id = service_id,
    });

    try control_rb.write(constants.control_msg_type_id, msg_buf[0..msg_len]);
}

/// Sends a SubscribeToServiceUpdates request to the broker.
pub fn subscribeToServiceUpdates(
    broker_meta: *BrokerMetadataFile,
    local_service_id: i32,
    remote_service_name: []const u8,
) !void {
    var control_rb = try RingBuffer.init(
        @alignCast(broker_meta.getControlBuffer()),
        false,
        null,
        null,
    );

    var msg_buf: [256]u8 = undefined;
    const msg_len = control_encoding.encodeSubscribeToServiceUpdates(&msg_buf, .{
        .subscriber_service_id = local_service_id,
        .target_service_name = remote_service_name,
    });

    try control_rb.write(constants.control_msg_type_id, msg_buf[0..msg_len]);
}

/// Blocks until the broker sends a RegistrationResponse, or until timeout.
pub fn waitForRegistrationResponse(
    service_meta: *ServiceMetadataFile,
    timeout_ms: u64,
) !control_encoding.RegistrationResponseData {
    var control_rb = try RingBuffer.init(
        @alignCast(service_meta.getControlBuffer()),
        false,
        null,
        null,
    );

    var response: ?control_encoding.RegistrationResponseData = null;
    const deadline_ns: i64 = Clock.monotonicNanos() + @as(i64, @intCast(timeout_ms * std.time.ns_per_ms));

    while (response == null) {
        // Use file-level state to pass data from the handler.
        wait_response_ptr = &response;
        _ = control_rb.read(&waitResponseHandler, 1);

        if (response != null) break;

        if (Clock.monotonicNanos() >= deadline_ns) {
            return error.RegistrationTimeout;
        }

        platform.sleepNanos(1 * std.time.ns_per_ms);
    }

    const resp = response.?;
    if (!resp.success) return error.RegistrationRejected;
    return resp;
}

// File-level state for the waitForRegistrationResponse handler.
var wait_response_ptr: ?*?control_encoding.RegistrationResponseData = null;

fn waitResponseHandler(_: i32, payload: []const u8) void {
    const template_id = control_encoding.readTemplateId(payload);
    if (template_id == TemplateId.registration_response) {
        if (wait_response_ptr) |ptr| {
            ptr.* = control_encoding.decodeRegistrationResponse(payload);
        }
    }
}
