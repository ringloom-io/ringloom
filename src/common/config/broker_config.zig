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
    recv_log_buffer_size: u32 = 4_194_304,
    retransmit_buffer_size: u32 = 4_194_304,
    mtu_length: u32 = 1_408,

    // ── Threading ───────────────────────────────────────────────
    threading_mode: ThreadingMode = .dedicated,
    idle_strategy_name: IdleStrategyName = .backoff,

    // ── Monitoring ──────────────────────────────────────────────
    counter_values_buffer_size: u32 = 65_536,
    error_log_buffer_size: u32 = 262_144,

    // ── Limits ──────────────────────────────────────────────────
    max_services: u16 = 256,
    max_peers: u8 = 16,

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
