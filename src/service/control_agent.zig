//! ControlAgent — polls the service's control ring buffer (Channel 2) for
//! messages from the broker, and periodically writes heartbeats.

const std = @import("std");
const ringloom_common = @import("ringloom_common");
const platform = ringloom_common.platform;
const memory = ringloom_common.memory;
const ring_buffer = ringloom_common.concurrent.ring_buffer;
const constants = ringloom_common.memory.constants;
const control_encoding = ringloom_common.message.control_encoding;
const ServiceClientRegistry = @import("service_client_registry.zig").ServiceClientRegistry;

const RingBuffer = ring_buffer.RingBuffer;
const ServiceMetadataFile = memory.ServiceMetadataFile;
const BrokerMetadataFile = memory.BrokerMetadataFile;
const Clock = platform.Clock;
const ServiceCounters = ringloom_common.monitoring.ServiceCounters;
const topic_messages = ringloom_common.message.topic_control_messages;
const TopicPublisherRegistry = @import("topics/topic_publisher_registry.zig").TopicPublisherRegistry;
const TopicPublisher = @import("topics/topic_publisher.zig").TopicPublisher;

/// Template IDs for control messages (matches the encoding module).
pub const TemplateId = struct {
    pub const register_service: u16 = 1;
    pub const registration_response: u16 = 2;
    pub const subscribe_to_service_updates: u16 = 3;
    pub const service_instances: u16 = 4;
    pub const unregister_service: u16 = 5;
    pub const leader_changed: u16 = 6;
    // Topic control-plane messages (spec 03); bodies use the shared 4-byte
    // header from ringloom_common.message.topic_control_messages.
    pub const topic_publication_response: u16 = 8;
    pub const topic_subscription_response: u16 = 10;
    pub const topic_leader_changed: u16 = 13;
    pub const topic_ack_feedback: u16 = 15;
};

/// Heartbeat write interval in nanoseconds.
const heartbeat_write_interval_ns: i64 =
    constants.service_heartbeat_write_interval_ms * std.time.ns_per_ms;

pub const ControlAgent = struct {
    control_rb: RingBuffer,
    service_meta: *ServiceMetadataFile,
    broker_meta: *BrokerMetadataFile,
    service_registry: *ServiceClientRegistry,
    service_counters: ?*ServiceCounters,
    topic_publishers: ?*TopicPublisherRegistry,
    last_heartbeat_ns: i64,

    const Self = @This();

    pub fn init(
        service_meta: *ServiceMetadataFile,
        broker_meta: *BrokerMetadataFile,
        service_registry: *ServiceClientRegistry,
        service_counters: ?*ServiceCounters,
        topic_publishers: ?*TopicPublisherRegistry,
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
            .service_counters = service_counters,
            .topic_publishers = topic_publishers,
            .last_heartbeat_ns = Clock.monotonicNanos(),
        };
    }

    /// Duty-cycle function. Returns total work count.
    pub fn doWork(self: *Self) u32 {
        var work_count: u32 = 0;

        // 1. Poll control ring buffer for broker messages.
        const control_messages = self.pollControlMessages(constants.control_read_limit);
        if (control_messages > 0) {
            self.addCounter(.control_messages_received, control_messages);
        }
        work_count += control_messages;

        // 2. Write heartbeat if interval has elapsed.
        if (self.writeHeartbeatIfDue()) {
            work_count += 1;
        }

        return work_count;
    }

    /// Poll control messages from an application-owned thread and keep the
    /// service heartbeat fresh. Returns only the number of messages processed.
    pub fn poll(self: *Self, limit: u32) u32 {
        const messages_read = self.pollControlMessages(limit);
        if (messages_read > 0) {
            self.addCounter(.control_messages_received, messages_read);
        }
        _ = self.writeHeartbeatIfDue();
        return messages_read;
    }

    /// EventLoop-compatible function pointer.
    pub fn doWorkFn(ctx: *anyopaque) u32 {
        const self: *Self = @ptrCast(@alignCast(ctx));
        return self.doWork();
    }

    /// No-op close function for EventLoop compatibility.
    pub fn onCloseFn(_: *anyopaque) void {}

    pub fn pollControlMessages(self: *Self, limit: u32) u32 {
        // Set file-level context so the bare function pointer handler can
        // reach this agent instance. Safe because ControlAgent runs on a
        // single dedicated thread.
        dispatch_agent_ptr = self;
        defer dispatch_agent_ptr = null;
        return self.control_rb.read(&dispatchWrapper, limit);
    }

    /// Dispatch a single control message based on its template ID.
    pub fn dispatchControlMessage(self: *Self, payload: []const u8) void {
        const template_id = control_encoding.readTemplateId(payload);

        switch (template_id) {
            TemplateId.registration_response => {
                // Late registration response — log and ignore.
            },

            TemplateId.service_instances => {
                // Broker sends the complete current set of instances for a
                // subscribed service, including empty snapshots.
                const snapshot = control_encoding.decodeServiceInstances(payload);
                self.service_registry.replaceInstances(
                    snapshot.service_name,
                    snapshot.entries,
                );
            },

            TemplateId.leader_changed => {
                const leader_info = control_encoding.decodeLeaderChanged(payload);
                self.service_registry.updateLeader(
                    leader_info.service_name,
                    leader_info.leader_service_id,
                );
            },

            TemplateId.topic_publication_response => {
                // Registration response for a pending registerTopicPublication.
                const m = topic_messages.decode(topic_messages.TopicPublicationResponseMsg, payload) orelse return;
                if (pending_publication_response) |ptr| {
                    ptr.* = .{
                        .topic_id = m.topic_id,
                        .leader_node_id = m.leader_node_id,
                        .leader_epoch = m.leader_epoch,
                        .status = m.status,
                    };
                }
            },

            TemplateId.topic_subscription_response => {
                // Registration response for a pending subscribeTopic.
                const m = topic_messages.decode(topic_messages.TopicSubscriptionResponseMsg, payload) orelse return;
                if (pending_subscription_response) |ptr| {
                    ptr.* = .{
                        .topic_id = m.topic_id,
                        .status = m.status,
                        .start_index = m.start_index,
                        .queue_dir = topic_messages.subscriptionResponseQueueDir(payload),
                        .geometry = m.geometry,
                    };
                }
            },

            TemplateId.topic_leader_changed => {
                // Producer must re-target the new topic leader (spec 08).
                const m = topic_messages.decode(topic_messages.TopicLeaderChangedMsg, payload) orelse return;
                if (self.topic_publishers) |reg| {
                    if (reg.get(m.topic_id)) |publisher| {
                        publisher.applyLeaderChanged(@intCast(m.leader_node_id), m.leader_epoch);
                    }
                }
            },

            TemplateId.topic_ack_feedback => {
                // Throttled replicated HWM/count for replicate_once completion.
                const m = topic_messages.decode(topic_messages.TopicAckFeedbackMsg, payload) orelse return;
                if (self.topic_publishers) |reg| {
                    if (reg.get(m.topic_id)) |publisher| {
                        publisher.applyAckFeedback(m.leader_epoch, m.replicated_hwm, m.replicated_count);
                    }
                }
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

    fn writeHeartbeatIfDue(self: *Self) bool {
        if (!self.shouldWriteHeartbeat()) return false;
        self.service_meta.storeHeartbeat(Clock.epochMillis());
        self.last_heartbeat_ns = Clock.monotonicNanos();
        self.incrementCounter(.heartbeats_sent);
        return true;
    }

    fn incrementCounter(self: *const Self, counter: ringloom_common.monitoring.ServiceCounter) void {
        if (self.service_counters) |counters| counters.increment(counter);
    }

    fn addCounter(self: *const Self, counter: ringloom_common.monitoring.ServiceCounter, delta: u32) void {
        if (self.service_counters) |counters| counters.add(counter, @intCast(delta));
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

// ── Topic registration (spec 03 templates 7–12) ───────────────────────

/// Pending response data for registerTopicPublication (template 8).
pub const PendingPublicationResponse = struct {
    topic_id: u64,
    leader_node_id: i16,
    leader_epoch: u64,
    status: u8,
};

/// Pending response data for subscribeTopic (template 10). `queue_dir` borrows
/// into the control ring buffer memory for the duration of the wait loop; the
/// caller copies it before returning.
pub const PendingSubscriptionResponse = struct {
    topic_id: u64,
    status: u8,
    start_index: u64,
    queue_dir: []const u8,
    geometry: ringloom_common.topics.TopicConfig,
};

var pending_publication_response: ?*?PendingPublicationResponse = null;
var pending_subscription_response: ?*?PendingSubscriptionResponse = null;

/// Writes a RegisterTopicPublication (template 7) into the broker control RB.
pub fn registerTopicPublication(
    broker_meta: *BrokerMetadataFile,
    service_id: i32,
    config: ringloom_common.topics.TopicConfig,
    topic_name: []const u8,
) !void {
    var control_rb = try RingBuffer.init(
        @alignCast(broker_meta.getControlBuffer()),
        false,
        null,
        null,
    );
    var msg_buf: [512]u8 = undefined;
    const msg_len = topic_messages.encodeRegisterTopicPublication(
        &msg_buf,
        service_id,
        config,
        topic_name,
    );
    try control_rb.write(constants.control_msg_type_id, msg_buf[0..msg_len]);
}

/// Writes a SubscribeTopic (template 9) into the broker control RB.
pub fn subscribeTopic(
    broker_meta: *BrokerMetadataFile,
    service_id: i32,
    start_position: topic_messages.StartPosition,
    topic_name: []const u8,
) !void {
    var control_rb = try RingBuffer.init(
        @alignCast(broker_meta.getControlBuffer()),
        false,
        null,
        null,
    );
    var msg_buf: [512]u8 = undefined;
    const msg_len = topic_messages.encodeSubscribeTopic(
        &msg_buf,
        service_id,
        start_position,
        topic_name,
    );
    try control_rb.write(constants.control_msg_type_id, msg_buf[0..msg_len]);
}

/// Writes an UnregisterTopicPublication (template 11) into the broker control RB.
pub fn unregisterTopicPublication(
    broker_meta: *BrokerMetadataFile,
    service_id: i32,
    topic_id: u64,
) !void {
    var control_rb = try RingBuffer.init(
        @alignCast(broker_meta.getControlBuffer()),
        false,
        null,
        null,
    );
    var msg_buf: [128]u8 = undefined;
    const msg_len = topic_messages.encodeUnregisterTopicPublication(&msg_buf, service_id, topic_id);
    try control_rb.write(constants.control_msg_type_id, msg_buf[0..msg_len]);
}

/// Writes an UnsubscribeTopic (template 12) into the broker control RB.
pub fn unsubscribeTopic(
    broker_meta: *BrokerMetadataFile,
    service_id: i32,
    topic_id: u64,
) !void {
    var control_rb = try RingBuffer.init(
        @alignCast(broker_meta.getControlBuffer()),
        false,
        null,
        null,
    );
    var msg_buf: [128]u8 = undefined;
    const msg_len = topic_messages.encodeUnsubscribeTopic(&msg_buf, service_id, topic_id);
    try control_rb.write(constants.control_msg_type_id, msg_buf[0..msg_len]);
}

/// Blocks until the broker sends a TopicPublicationResponse (template 8), or
/// until timeout. Spin-polls the service control RB, which also progresses the
/// heartbeat. The caller must copy any needed fields before the next poll.
pub fn waitForTopicPublicationResponse(
    service_meta: *ServiceMetadataFile,
    timeout_ms: u64,
) !PendingPublicationResponse {
    var control_rb = try RingBuffer.init(
        @alignCast(service_meta.getControlBuffer()),
        false,
        null,
        null,
    );
    var response: ?PendingPublicationResponse = null;
    const deadline_ns: i64 = Clock.monotonicNanos() + @as(i64, @intCast(timeout_ms * std.time.ns_per_ms));

    while (response == null) {
        pending_publication_response = &response;
        _ = control_rb.read(&publicationResponseHandler, 1);
        if (response != null) break;
        if (Clock.monotonicNanos() >= deadline_ns) {
            pending_publication_response = null;
            return error.RegistrationTimeout;
        }
        platform.sleepNanos(1 * std.time.ns_per_ms);
    }
    pending_publication_response = null;
    return response.?;
}

/// Blocks until the broker sends a TopicSubscriptionResponse (template 10).
pub fn waitForTopicSubscriptionResponse(
    service_meta: *ServiceMetadataFile,
    timeout_ms: u64,
) !PendingSubscriptionResponse {
    var control_rb = try RingBuffer.init(
        @alignCast(service_meta.getControlBuffer()),
        false,
        null,
        null,
    );
    var response: ?PendingSubscriptionResponse = null;
    const deadline_ns: i64 = Clock.monotonicNanos() + @as(i64, @intCast(timeout_ms * std.time.ns_per_ms));

    while (response == null) {
        pending_subscription_response = &response;
        _ = control_rb.read(&subscriptionResponseHandler, 1);
        if (response != null) break;
        if (Clock.monotonicNanos() >= deadline_ns) {
            pending_subscription_response = null;
            return error.RegistrationTimeout;
        }
        platform.sleepNanos(1 * std.time.ns_per_ms);
    }
    pending_subscription_response = null;
    return response.?;
}

fn publicationResponseHandler(_: i32, payload: []const u8) void {
    const template_id = control_encoding.readTemplateId(payload);
    if (template_id == TemplateId.topic_publication_response) {
        const m = topic_messages.decode(topic_messages.TopicPublicationResponseMsg, payload) orelse return;
        if (pending_publication_response) |ptr| {
            ptr.* = .{
                .topic_id = m.topic_id,
                .leader_node_id = m.leader_node_id,
                .leader_epoch = m.leader_epoch,
                .status = m.status,
            };
        }
    }
}

fn subscriptionResponseHandler(_: i32, payload: []const u8) void {
    const template_id = control_encoding.readTemplateId(payload);
    if (template_id == TemplateId.topic_subscription_response) {
        const m = topic_messages.decode(topic_messages.TopicSubscriptionResponseMsg, payload) orelse return;
        if (pending_subscription_response) |ptr| {
            ptr.* = .{
                .topic_id = m.topic_id,
                .status = m.status,
                .start_index = m.start_index,
                .queue_dir = topic_messages.subscriptionResponseQueueDir(payload),
                .geometry = m.geometry,
            };
        }
    }
}
