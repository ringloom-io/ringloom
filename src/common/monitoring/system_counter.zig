//! Well-known system counter IDs for the RingLoom broker.
//!
//! Every counter has a fixed, well-known integer ID. This enum is the
//! single source of truth for counter identities.

pub const SystemCounter = enum(u8) {
    // ── Traffic counters ────────────────────────────────────────
    bytes_sent = 0,
    bytes_received = 1,
    messages_routed_local = 2,
    messages_routed_remote = 3,

    // ── UDP session counters ────────────────────────────────────
    udp_peers_configured = 4,
    udp_endpoint_errors = 5,
    udp_setup_failures = 6,
    udp_setup_retries = 7,

    // ── Heartbeats ──────────────────────────────────────────────
    heartbeats_sent = 8,
    heartbeats_received = 9,
    heartbeat_timeouts = 10,

    // ── Service lifecycle ───────────────────────────────────────
    services_registered = 11,
    services_removed = 12,

    // ── Back-pressure ───────────────────────────────────────────
    send_rb_back_pressure = 13,
    service_back_pressure = 14,

    // ── Drop counters ───────────────────────────────────────────
    unknown_service_drops = 15,
    service_full_drops = 16,
    peer_queue_overflow_drops = 17,
    peer_not_connected_drops = 18,
    invalid_frames = 19,

    // ── Performance: max cycle time per event loop (nanoseconds) ──
    control_loop_cycle_time_max = 20,
    sender_cycle_time_max = 21,
    receiver_cycle_time_max = 22,

    // ── Flow control counters ───────────────────────────────────
    fc_updates_sent = 23,
    fc_updates_received = 24,
    fc_pressure_events = 25,
    fc_recovery_events = 26,
    fc_client_backpressure = 27,
    fc_client_spin_timeouts = 28,
    fc_slot_allocations = 29,
    fc_slot_reclamations = 30,
    fc_peer_congestion_events = 31,
    fc_peer_disconnected_sends_avoided = 32,

    /// Total number of well-known counters.
    pub const count: usize = 33;

    /// Human-readable label for each counter.
    pub fn label(self: SystemCounter) []const u8 {
        return switch (self) {
            .bytes_sent => "bytes-sent",
            .bytes_received => "bytes-received",
            .messages_routed_local => "messages-routed-local",
            .messages_routed_remote => "messages-routed-remote",
            .udp_peers_configured => "udp-peers-configured",
            .udp_endpoint_errors => "udp-endpoint-errors",
            .udp_setup_failures => "udp-setup-failures",
            .udp_setup_retries => "udp-setup-retries",
            .heartbeats_sent => "heartbeats-sent",
            .heartbeats_received => "heartbeats-received",
            .heartbeat_timeouts => "heartbeat-timeouts",
            .services_registered => "services-registered",
            .services_removed => "services-removed",
            .send_rb_back_pressure => "send-rb-back-pressure",
            .service_back_pressure => "service-back-pressure",
            .unknown_service_drops => "unknown-service-drops",
            .service_full_drops => "service-full-drops",
            .peer_queue_overflow_drops => "peer-queue-overflow-drops",
            .peer_not_connected_drops => "peer-not-connected-drops",
            .invalid_frames => "invalid-frames",
            .control_loop_cycle_time_max => "control-loop-cycle-time-max-ns",
            .sender_cycle_time_max => "sender-cycle-time-max-ns",
            .receiver_cycle_time_max => "receiver-cycle-time-max-ns",
            .fc_updates_sent => "fc-updates-sent",
            .fc_updates_received => "fc-updates-received",
            .fc_pressure_events => "fc-pressure-events",
            .fc_recovery_events => "fc-recovery-events",
            .fc_client_backpressure => "fc-client-backpressure",
            .fc_client_spin_timeouts => "fc-client-spin-timeouts",
            .fc_slot_allocations => "fc-slot-allocations",
            .fc_slot_reclamations => "fc-slot-reclamations",
            .fc_peer_congestion_events => "fc-peer-congestion-events",
            .fc_peer_disconnected_sends_avoided => "fc-peer-disconnected-sends-avoided",
        };
    }
};
