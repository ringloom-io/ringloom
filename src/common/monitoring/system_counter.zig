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

    // ── Reserved v1 transport counter IDs ───────────────────────
    reserved_v1_transport_4 = 4,
    reserved_v1_transport_5 = 5,
    reserved_v1_transport_6 = 6,
    reserved_v1_transport_7 = 7,

    // ── Heartbeats ──────────────────────────────────────────────
    heartbeats_sent = 8,
    heartbeats_received = 9,
    heartbeat_timeouts = 10,

    // ── Service lifecycle ───────────────────────────────────────
    services_registered = 11,
    services_removed = 12,

    // ── Back-pressure ───────────────────────────────────────────
    reserved_v1_transport_13 = 13,
    service_back_pressure = 14,

    // ── Drop counters ───────────────────────────────────────────
    unknown_service_drops = 15,
    service_full_drops = 16,
    reserved_v1_transport_17 = 17,
    reserved_v1_transport_18 = 18,
    invalid_frames = 19,

    // ── Performance: max cycle time per event loop (nanoseconds) ──
    control_loop_cycle_time_max = 20,
    reserved_v1_transport_21 = 21,
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
    reserved_v1_transport_31 = 31,
    reserved_v1_transport_32 = 32,

    /// Total number of well-known counters.
    pub const count: usize = 33;

    /// Human-readable label for each counter.
    pub fn label(self: SystemCounter) []const u8 {
        return switch (self) {
            .bytes_sent => "bytes-sent",
            .bytes_received => "bytes-received",
            .messages_routed_local => "messages-routed-local",
            .messages_routed_remote => "messages-routed-remote",
            .reserved_v1_transport_4 => "reserved-v1-transport-4",
            .reserved_v1_transport_5 => "reserved-v1-transport-5",
            .reserved_v1_transport_6 => "reserved-v1-transport-6",
            .reserved_v1_transport_7 => "reserved-v1-transport-7",
            .heartbeats_sent => "heartbeats-sent",
            .heartbeats_received => "heartbeats-received",
            .heartbeat_timeouts => "heartbeat-timeouts",
            .services_registered => "services-registered",
            .services_removed => "services-removed",
            .reserved_v1_transport_13 => "reserved-v1-transport-13",
            .service_back_pressure => "service-back-pressure",
            .unknown_service_drops => "unknown-service-drops",
            .service_full_drops => "service-full-drops",
            .reserved_v1_transport_17 => "reserved-v1-transport-17",
            .reserved_v1_transport_18 => "reserved-v1-transport-18",
            .invalid_frames => "invalid-frames",
            .control_loop_cycle_time_max => "control-loop-cycle-time-max-ns",
            .reserved_v1_transport_21 => "reserved-v1-transport-21",
            .receiver_cycle_time_max => "receiver-cycle-time-max-ns",
            .fc_updates_sent => "fc-updates-sent",
            .fc_updates_received => "fc-updates-received",
            .fc_pressure_events => "fc-pressure-events",
            .fc_recovery_events => "fc-recovery-events",
            .fc_client_backpressure => "fc-client-backpressure",
            .fc_client_spin_timeouts => "fc-client-spin-timeouts",
            .fc_slot_allocations => "fc-slot-allocations",
            .fc_slot_reclamations => "fc-slot-reclamations",
            .reserved_v1_transport_31 => "reserved-v1-transport-31",
            .reserved_v1_transport_32 => "reserved-v1-transport-32",
        };
    }
};
