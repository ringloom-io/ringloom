// src/config/broker_config.zig

const std = @import("std");

pub const ThreadingMode = enum {
    dedicated,
    shared_network,
    shared,

    pub fn fromString(s: []const u8) ?ThreadingMode {
        const map = std.StaticStringMap(ThreadingMode).initComptime(.{
            .{ "DEDICATED", .dedicated },
            .{ "dedicated", .dedicated },
            .{ "SHARED_NETWORK", .shared_network },
            .{ "shared_network", .shared_network },
            .{ "SHARED", .shared },
            .{ "shared", .shared },
        });
        return map.get(s);
    }
};

pub const IdleStrategyName = enum {
    busy_spin,
    yielding,
    sleeping,
    backoff,
    blocking,

    pub fn fromString(s: []const u8) ?IdleStrategyName {
        const map = std.StaticStringMap(IdleStrategyName).initComptime(.{
            .{ "busy_spin", .busy_spin },
            .{ "yielding", .yielding },
            .{ "sleeping", .sleeping },
            .{ "backoff", .backoff },
            .{ "blocking", .blocking },
        });
        return map.get(s);
    }
};

pub const PeerEndpoint = struct {
    node_id: u8,
    host: []const u8,
    port: u16,
};

pub const BrokerConfig = struct {
    // ── Required ────────────────────────────────────────────────
    node_id: u8,
    local_host: []const u8,
    local_port: u16,

    // ── Peers ───────────────────────────────────────────────────
    peer_endpoints: []const PeerEndpoint,

    // ── Naming & storage ────────────────────────────────────────
    group_name: []const u8 = "brz",
    storage_path: []const u8 = "/dev/shm",

    // ── Buffer sizes (bytes — all must be power of 2 unless noted) ──
    control_buffer_size: u32 = 65_536,
    messages_buffer_size: u32 = 1_048_576,

    // ── TCP transport ───────────────────────────────────────────
    tcp_send_buffer_size: u32 = 262_144,
    tcp_recv_buffer_size: u32 = 262_144,
    max_frame_length: u32 = 65_536,
    peer_write_queue_capacity: u32 = 4_096,
    heartbeat_interval_ms: u64 = 500,
    heartbeat_timeout_ms: u64 = 2_000,
    reconnect_initial_delay_ms: u64 = 100,
    reconnect_max_delay_ms: u64 = 1_000,

    // ── Threading ───────────────────────────────────────────────
    threading_mode: ThreadingMode = .dedicated,
    idle_strategy_name: IdleStrategyName = .backoff,
    sender_cpu_affinity: ?u32 = null,
    receiver_cpu_affinity: ?u32 = null,

    // ── Monitoring ──────────────────────────────────────────────
    counter_values_buffer_size: u32 = 65_536,
    error_log_buffer_size: u32 = 262_144,

    // ── Limits ──────────────────────────────────────────────────
    max_services: u16 = 256,
    max_peers: u8 = 16,

    // ── Flow Control ────────────────────────────────────────────
    fc_enabled: bool = false,
    fc_max_entries: u32 = 256,
    fc_low_watermark_pct: u8 = 25,
    fc_high_watermark_pct: u8 = 50,
    fc_refresh_interval_ms: u32 = 200,
    fc_normal_refresh_interval_ms: u32 = 2000,
    fc_check_interval_ms: u32 = 1,
    fc_slot_reuse_delay_ms: u32 = 10_000,
    fc_peer_send_counters_enabled: bool = false,
    fc_peer_send_counters_max_peers: u32 = 32,

    // ── io_uring (Linux only) ───────────────────────────────────
    io_uring_queue_depth: u32 = 256,
    io_uring_sqpoll: bool = false,
    io_uring_registered_buffers: u32 = 64,

    // ── Computed (set during validation, not from file) ─────────
    max_counter_id: u32 = 0,
    counter_metadata_buffer_size: u32 = 0,
    single_node_cluster: bool = true,

    /// Returns the maximum message length for the send ring buffer.
    pub fn maxMessageLength(self: *const BrokerConfig) u32 {
        return self.messages_buffer_size / 8;
    }

    /// Returns the maximum number of peers that can be active.
    pub fn maxActivePeers(self: *const BrokerConfig) u8 {
        return self.max_peers;
    }


};
