//! Well-known system counter IDs for the BRZ broker.
//!
//! Every counter has a fixed, well-known integer ID. This enum is the
//! single source of truth for counter identities.

pub const SystemCounter = enum(u8) {
    // ── Traffic counters ────────────────────────────────────────
    bytes_sent = 0,
    bytes_received = 1,
    messages_routed_local = 2,
    messages_routed_remote = 3,

    // ── Reliability counters ────────────────────────────────────
    naks_sent = 4,
    naks_received = 5,
    retransmits_sent = 6,

    // ── Status messages ─────────────────────────────────────────
    status_messages_sent = 7,
    status_messages_received = 8,

    // ── Heartbeats ──────────────────────────────────────────────
    heartbeats_sent = 9,
    heartbeats_received = 10,

    // ── Service lifecycle ───────────────────────────────────────
    services_registered = 11,
    services_removed = 12,

    // ── Back-pressure ───────────────────────────────────────────
    send_rb_back_pressure = 13,
    service_back_pressure = 14,

    // ── Error counters ──────────────────────────────────────────
    unknown_service_drops = 15,
    invalid_packets = 16,

    // ── Performance: max cycle time per event loop (nanoseconds) ──
    control_loop_cycle_time_max = 17,
    sender_cycle_time_max = 18,
    receiver_cycle_time_max = 19,

    // ── Flow control ────────────────────────────────────────────
    flow_control_under_runs = 20,
    flow_control_over_runs = 21,

    /// Total number of well-known counters.
    pub const count: usize = 22;

    /// Human-readable label for each counter.
    pub fn label(self: SystemCounter) []const u8 {
        return switch (self) {
            .bytes_sent => "bytes-sent",
            .bytes_received => "bytes-received",
            .messages_routed_local => "messages-routed-local",
            .messages_routed_remote => "messages-routed-remote",
            .naks_sent => "naks-sent",
            .naks_received => "naks-received",
            .retransmits_sent => "retransmits-sent",
            .status_messages_sent => "status-messages-sent",
            .status_messages_received => "status-messages-received",
            .heartbeats_sent => "heartbeats-sent",
            .heartbeats_received => "heartbeats-received",
            .services_registered => "services-registered",
            .services_removed => "services-removed",
            .send_rb_back_pressure => "send-rb-back-pressure",
            .service_back_pressure => "service-back-pressure",
            .unknown_service_drops => "unknown-service-drops",
            .invalid_packets => "invalid-packets",
            .control_loop_cycle_time_max => "control-loop-cycle-time-max-ns",
            .sender_cycle_time_max => "sender-cycle-time-max-ns",
            .receiver_cycle_time_max => "receiver-cycle-time-max-ns",
            .flow_control_under_runs => "flow-control-under-runs",
            .flow_control_over_runs => "flow-control-over-runs",
        };
    }
};
