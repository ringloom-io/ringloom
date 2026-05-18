// src/config/broker_config.zig

const std = @import("std");

pub const aeron_term_min_length: u32 = 64 * 1024;
pub const aeron_default_mtu_length: u32 = 1408;
pub const aeron_network_publication_max_messages_per_send_max: u32 = 16;
pub const max_aeron_directory_length: usize = 512;

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
    group_name: []const u8 = "ringloom",
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

    // ── Aeron transport ─────────────────────────────────────────
    /// Empty means derive a broker-owned directory from storage_path, group, and node_id.
    aeron_directory: []const u8 = "",
    aeron_ipc_term_length: u32 = 1_048_576,
    aeron_udp_term_length: u32 = 16_777_216,
    aeron_ipc_mtu_length: u32 = aeron_default_mtu_length,
    aeron_mtu_length: u32 = aeron_default_mtu_length,
    aeron_sparse_files: bool = true,
    aeron_delete_directory_on_start: bool = true,
    aeron_delete_directory_on_shutdown: bool = false,
    aeron_publication_linger_timeout_ns: u64 = 5 * std.time.ns_per_s,
    aeron_client_liveness_timeout_ns: u64 = 10 * std.time.ns_per_s,
    aeron_network_publication_max_messages_per_send: u32 = aeron_network_publication_max_messages_per_send_max,
    aeron_ingress_stream_base: i32 = 10_000,
    aeron_admin_stream_base: i32 = 20_000,
    aeron_data_stream_base: i32 = 30_000,
    aeron_threading_mode: ThreadingMode = .dedicated,

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
    benchmark_latency_tracing_enabled: bool = false,

    // ── Computed (set during validation, not from file) ─────────
    max_counter_id: u32 = 0,
    counter_metadata_buffer_size: u32 = 0,
    single_node_cluster: bool = true,

    /// Returns the maximum number of peers that can be active.
    pub fn maxActivePeers(self: *const BrokerConfig) u8 {
        return self.max_peers;
    }

    pub fn buildAeronDirectory(self: *const BrokerConfig, buffer: []u8) std.fmt.BufPrintError![:0]u8 {
        if (self.aeron_directory.len > 0) {
            return std.fmt.bufPrintZ(buffer, "{s}", .{self.aeron_directory});
        }

        return std.fmt.bufPrintZ(
            buffer,
            "{s}/ringloom-aeron-{s}-node-{d}",
            .{ self.storage_path, self.group_name, self.node_id },
        );
    }
};
