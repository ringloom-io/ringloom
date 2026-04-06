// src/config/service_config.zig

const std = @import("std");

pub const ServiceConfig = struct {
    /// Service name (e.g. "pricing", "order-gateway").
    service_name: []const u8,

    /// Control ring buffer capacity. Power of 2.
    control_buffer_size: u32 = 65_536,

    /// Application messages ring buffer capacity. Power of 2.
    messages_buffer_size: u32 = 1_048_576,

    /// Whether the ring buffer uses kernel-level blocking when full.
    blocking_mode: bool = false,

    /// Service heartbeat timeout in milliseconds.
    heartbeat_timeout_ms: u32 = 10_000,

    /// Idle strategy for the service's message consumer thread.
    idle_strategy_name: []const u8 = "backoff",

    /// Whether this service participates in per-service leader election.
    leader_election_enabled: bool = false,

    /// Storage path override (defaults to broker's storage path).
    storage_path: ?[]const u8 = null,

    /// Group name override (defaults to broker's group name).
    group_name: ?[]const u8 = null,
};
