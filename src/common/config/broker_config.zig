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

pub const TransportKind = enum {
    udp,

    pub fn fromString(s: []const u8) ?TransportKind {
        const map = std.StaticStringMap(TransportKind).initComptime(.{
            .{ "udp", .udp },
            .{ "UDP", .udp },
        });
        return map.get(s);
    }
};

pub const TransportEngine = enum {
    posix,
    prefer_af_xdp,
    require_af_xdp,

    pub fn fromString(s: []const u8) ?TransportEngine {
        const map = std.StaticStringMap(TransportEngine).initComptime(.{
            .{ "posix", .posix },
            .{ "POSIX", .posix },
            .{ "prefer_af_xdp", .prefer_af_xdp },
            .{ "PREFER_AF_XDP", .prefer_af_xdp },
            .{ "require_af_xdp", .require_af_xdp },
            .{ "REQUIRE_AF_XDP", .require_af_xdp },
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
    group_name: []const u8 = "ringloom",
    storage_path: []const u8 = "/dev/shm",

    // ── Buffer sizes (bytes — all must be power of 2 unless noted) ──
    control_buffer_size: u32 = 65_536,
    messages_buffer_size: u32 = 1_048_576,

    // ── V2 UDP transport ─────────────────────────────────────────
    transport: TransportKind = .udp,
    transport_engine: TransportEngine = .posix,
    udp_mtu: u16 = 1408,
    udp_term_length: u32 = 64 * 1024,
    udp_receiver_window_length: u32 = 32 * 1024,
    udp_heartbeat_interval_ms: u64 = 500,
    udp_session_timeout_ms: u64 = 2_000,
    udp_nak_initial_delay_us: u32 = 50,
    udp_nak_retry_delay_us: u32 = 250,

    // ── Per-destination send buffers ─────────────────────────────
    send_buffers_max_entries: u32 = 256,
    send_buffers_default_size: u32 = 1_048_576,
    send_buffers_max_total_bytes: u64 = 256 * 1_048_576,
    send_buffers_idle_timeout_ms: u64 = 60_000,
    send_buffers_drain_timeout_ms: u64 = 5_000,

    // ── Optional AF_XDP transport engine ─────────────────────────
    af_xdp_interface: ?[]const u8 = null,
    af_xdp_ports: []const u16 = &.{},
    af_xdp_rx_queue: u32 = 0,
    af_xdp_umem_frame_count: u32 = 4096,
    af_xdp_umem_frame_size: u32 = 2048,

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

    benchmark_latency_tracing_enabled: bool = false,

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
