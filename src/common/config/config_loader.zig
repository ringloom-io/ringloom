// src/config/config_loader.zig

const std = @import("std");
const BrokerConfig = @import("broker_config.zig").BrokerConfig;
const PeerEndpoint = @import("broker_config.zig").PeerEndpoint;
const ThreadingMode = @import("broker_config.zig").ThreadingMode;
const IdleStrategyName = @import("broker_config.zig").IdleStrategyName;
const TransportKind = @import("broker_config.zig").TransportKind;
const TransportEngine = @import("broker_config.zig").TransportEngine;

pub const ConfigError = error{
    FileNotFound,
    IoError,
    MissingRequiredProperty,
    InvalidValue,
    InvalidPeerFormat,
    InvalidNodeId,
    InvalidPort,
    BufferSizeNotPowerOfTwo,
    BufferSizeTooSmall,
    NodeIdConflict,
};

pub const ConfigLoader = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    environ_map: ?*const std.process.Environ.Map,

    pub fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        environ_map: ?*const std.process.Environ.Map,
    ) ConfigLoader {
        return .{
            .allocator = allocator,
            .io = io,
            .environ_map = environ_map,
        };
    }

    pub fn initForTesting(allocator: std.mem.Allocator) ConfigLoader {
        return init(allocator, std.testing.io, null);
    }

    /// Load configuration from a file. Tries RINGLOOM_CONFIG_FILE env var first,
    /// falls back to broker.properties in the current directory.
    pub fn load(self: *const ConfigLoader) ConfigError!BrokerConfig {
        const path = getEnvVar(self.environ_map, "RINGLOOM_CONFIG_FILE") orelse "broker.properties";
        return self.loadFromFile(path);
    }

    /// Load configuration from a specific file path.
    pub fn loadFromFile(self: *const ConfigLoader, path: []const u8) ConfigError!BrokerConfig {
        const io = self.io;
        const file = std.Io.Dir.cwd().openFile(io, path, .{}) catch |err| switch (err) {
            error.FileNotFound => return ConfigError.FileNotFound,
            else => return ConfigError.IoError,
        };
        defer file.close(io);

        var reader_buf: [4096]u8 = undefined;
        var reader = file.reader(io, &reader_buf);
        const content = reader.interface.allocRemaining(self.allocator, .limited(1024 * 1024)) catch
            return ConfigError.IoError;
        defer self.allocator.free(content);

        return self.parseAndBuild(content);
    }

    /// Parse properties from a string and build a validated BrokerConfig.
    pub fn parseAndBuild(self: *const ConfigLoader, content: []const u8) ConfigError!BrokerConfig {
        var props = parseProperties(self.allocator, content) catch
            return ConfigError.IoError;
        defer props.deinit();

        // Apply environment variable overrides.
        applyEnvOverrides(&props, self.allocator, self.environ_map) catch
            return ConfigError.IoError;

        var config = BrokerConfig{
            .node_id = undefined,
            .local_host = undefined,
            .local_port = undefined,
            .peer_endpoints = &[_]PeerEndpoint{},
        };

        // ── Required fields ─────────────────────────────────────
        const node_id_str = props.get("broker.node.id") orelse
            return ConfigError.MissingRequiredProperty;
        config.node_id = std.fmt.parseInt(u8, node_id_str, 10) catch
            return ConfigError.InvalidValue;

        const local_hp = props.get("broker.udp.local.host.port") orelse
            props.get("broker.local.host.port") orelse
            return ConfigError.MissingRequiredProperty;
        const colon_pos = std.mem.lastIndexOfScalar(u8, local_hp, ':') orelse
            return ConfigError.InvalidValue;
        config.local_host = self.allocator.dupe(u8, local_hp[0..colon_pos]) catch
            return ConfigError.IoError;
        config.local_port = std.fmt.parseInt(u16, local_hp[colon_pos + 1 ..], 10) catch
            return ConfigError.InvalidValue;

        // ── Peers ───────────────────────────────────────────────
        if (props.get("broker.udp.member.host.ports") orelse props.get("broker.member.host.ports")) |peer_str| {
            config.peer_endpoints = parsePeerEndpoints(self.allocator, peer_str) catch |err| switch (err) {
                error.InvalidPeerFormat => return ConfigError.InvalidPeerFormat,
                error.InvalidNodeId => return ConfigError.InvalidNodeId,
                error.InvalidPort => return ConfigError.InvalidPort,
                else => return ConfigError.IoError,
            };
        }

        // ── Optional fields ─────────────────────────────────────
        if (props.get("broker.group.name")) |v|
            config.group_name = self.allocator.dupe(u8, v) catch return ConfigError.IoError;
        if (props.get("broker.storage.path")) |v|
            config.storage_path = self.allocator.dupe(u8, v) catch return ConfigError.IoError;

        if (props.get("broker.control.buffer.size")) |v|
            config.control_buffer_size = std.fmt.parseInt(u32, v, 10) catch
                return ConfigError.InvalidValue;
        if (props.get("broker.messages.buffer.size")) |v|
            config.messages_buffer_size = std.fmt.parseInt(u32, v, 10) catch
                return ConfigError.InvalidValue;
        if (props.get("broker.transport")) |v|
            config.transport = TransportKind.fromString(v) orelse return ConfigError.InvalidValue;
        if (props.get("broker.transport.engine")) |v|
            config.transport_engine = TransportEngine.fromString(v) orelse return ConfigError.InvalidValue;
        if (props.get("broker.udp.mtu")) |v|
            config.udp_mtu = std.fmt.parseInt(u16, v, 10) catch return ConfigError.InvalidValue;
        if (props.get("broker.udp.term.length")) |v|
            config.udp_term_length = std.fmt.parseInt(u32, v, 10) catch return ConfigError.InvalidValue;
        if (props.get("broker.udp.receiver.window.length")) |v|
            config.udp_receiver_window_length = std.fmt.parseInt(u32, v, 10) catch return ConfigError.InvalidValue;
        if (props.get("broker.udp.heartbeat.interval.ms")) |v|
            config.udp_heartbeat_interval_ms = std.fmt.parseInt(u64, v, 10) catch return ConfigError.InvalidValue;
        if (props.get("broker.udp.session.timeout.ms")) |v|
            config.udp_session_timeout_ms = std.fmt.parseInt(u64, v, 10) catch return ConfigError.InvalidValue;
        if (props.get("broker.udp.nak.initial.delay.us")) |v|
            config.udp_nak_initial_delay_us = std.fmt.parseInt(u32, v, 10) catch return ConfigError.InvalidValue;
        if (props.get("broker.udp.nak.retry.delay.us")) |v|
            config.udp_nak_retry_delay_us = std.fmt.parseInt(u32, v, 10) catch return ConfigError.InvalidValue;
        if (props.get("broker.send.buffers.max.entries")) |v|
            config.send_buffers_max_entries = std.fmt.parseInt(u32, v, 10) catch return ConfigError.InvalidValue;
        if (props.get("broker.send.buffers.default.size")) |v|
            config.send_buffers_default_size = std.fmt.parseInt(u32, v, 10) catch return ConfigError.InvalidValue;
        if (props.get("broker.send.buffers.max.total.bytes")) |v|
            config.send_buffers_max_total_bytes = std.fmt.parseInt(u64, v, 10) catch return ConfigError.InvalidValue;
        if (props.get("broker.send.buffers.idle.timeout.ms")) |v|
            config.send_buffers_idle_timeout_ms = std.fmt.parseInt(u64, v, 10) catch return ConfigError.InvalidValue;
        if (props.get("broker.send.buffers.drain.timeout.ms")) |v|
            config.send_buffers_drain_timeout_ms = std.fmt.parseInt(u64, v, 10) catch return ConfigError.InvalidValue;
        if (props.get("broker.af_xdp.interface")) |v|
            config.af_xdp_interface = self.allocator.dupe(u8, v) catch return ConfigError.IoError;
        if (props.get("broker.af_xdp.ports")) |v|
            config.af_xdp_ports = parsePortList(self.allocator, v) catch return ConfigError.InvalidValue;
        if (props.get("broker.af_xdp.rx.queue")) |v|
            config.af_xdp_rx_queue = std.fmt.parseInt(u32, v, 10) catch return ConfigError.InvalidValue;
        if (props.get("broker.af_xdp.umem.frame.count")) |v|
            config.af_xdp_umem_frame_count = std.fmt.parseInt(u32, v, 10) catch return ConfigError.InvalidValue;
        if (props.get("broker.af_xdp.umem.frame.size")) |v|
            config.af_xdp_umem_frame_size = std.fmt.parseInt(u32, v, 10) catch return ConfigError.InvalidValue;

        if (props.get("broker.threading.mode")) |v|
            config.threading_mode = ThreadingMode.fromString(v) orelse
                return ConfigError.InvalidValue;
        if (props.get("broker.idle.strategy")) |v|
            config.idle_strategy_name = IdleStrategyName.fromString(v) orelse
                return ConfigError.InvalidValue;
        if (props.get("broker.sender.cpu.affinity")) |v|
            config.sender_cpu_affinity = std.fmt.parseInt(u32, v, 10) catch
                return ConfigError.InvalidValue;
        if (props.get("broker.receiver.cpu.affinity")) |v|
            config.receiver_cpu_affinity = std.fmt.parseInt(u32, v, 10) catch
                return ConfigError.InvalidValue;

        if (props.get("broker.counter.values.buffer.size")) |v|
            config.counter_values_buffer_size = std.fmt.parseInt(u32, v, 10) catch
                return ConfigError.InvalidValue;
        if (props.get("broker.error.log.buffer.size")) |v|
            config.error_log_buffer_size = std.fmt.parseInt(u32, v, 10) catch
                return ConfigError.InvalidValue;

        if (props.get("broker.max.services")) |v|
            config.max_services = std.fmt.parseInt(u16, v, 10) catch
                return ConfigError.InvalidValue;
        if (props.get("broker.max.peers")) |v|
            config.max_peers = std.fmt.parseInt(u8, v, 10) catch
                return ConfigError.InvalidValue;

        if (props.get("broker.benchmark.latency.tracing.enabled")) |v|
            config.benchmark_latency_tracing_enabled = std.mem.eql(u8, v, "true");

        // ── Validate and compute derived fields ─────────────────
        try validate(&config);

        return config;
    }
};

fn parsePeerEndpoints(
    allocator: std.mem.Allocator,
    value: []const u8,
) ![]PeerEndpoint {
    if (value.len == 0) return &[_]PeerEndpoint{};

    // Count commas to pre-allocate.
    var count: usize = 1;
    for (value) |c| {
        if (c == ',') count += 1;
    }

    const endpoints = try allocator.alloc(PeerEndpoint, count);
    var idx: usize = 0;

    var iter = std.mem.splitScalar(u8, value, ',');
    while (iter.next()) |entry_raw| {
        const entry = std.mem.trim(u8, entry_raw, &std.ascii.whitespace);
        if (entry.len == 0) continue;

        // Parse "nodeId@host:port"
        const at_pos = std.mem.indexOfScalar(u8, entry, '@') orelse
            return error.InvalidPeerFormat;
        const node_id_str = entry[0..at_pos];
        const host_port = entry[at_pos + 1 ..];

        const colon_pos = std.mem.lastIndexOfScalar(u8, host_port, ':') orelse
            return error.InvalidPeerFormat;

        const host = host_port[0..colon_pos];
        const port_str = host_port[colon_pos + 1 ..];

        const node_id = std.fmt.parseInt(u8, node_id_str, 10) catch
            return error.InvalidNodeId;
        const port = std.fmt.parseInt(u16, port_str, 10) catch
            return error.InvalidPort;

        // Dupe the host string into the arena so it outlives the file buffer.
        const host_owned = try allocator.dupe(u8, host);

        endpoints[idx] = .{
            .node_id = node_id,
            .host = host_owned,
            .port = port,
        };
        idx += 1;
    }

    return endpoints[0..idx];
}

fn parsePortList(allocator: std.mem.Allocator, value: []const u8) ![]const u16 {
    if (value.len == 0) return &[_]u16{};

    var count: usize = 1;
    for (value) |c| {
        if (c == ',') count += 1;
    }

    const ports = try allocator.alloc(u16, count);
    errdefer allocator.free(ports);
    var idx: usize = 0;
    var iter = std.mem.splitScalar(u8, value, ',');
    while (iter.next()) |entry_raw| {
        const entry = std.mem.trim(u8, entry_raw, &std.ascii.whitespace);
        if (entry.len == 0) continue;
        ports[idx] = std.fmt.parseInt(u16, entry, 10) catch return error.InvalidPort;
        idx += 1;
    }
    return ports[0..idx];
}

fn parseProperties(
    allocator: std.mem.Allocator,
    content: []const u8,
) !std.StringHashMap([]const u8) {
    var map = std.StringHashMap([]const u8).init(allocator);

    var line_iter = std.mem.splitScalar(u8, content, '\n');
    while (line_iter.next()) |raw_line| {
        // Strip trailing \r for Windows-style line endings.
        const line = std.mem.trimEnd(u8, raw_line, &[_]u8{'\r'});
        const trimmed = std.mem.trim(u8, line, &std.ascii.whitespace);

        // Skip empty lines and comments.
        if (trimmed.len == 0) continue;
        if (trimmed[0] == '#' or trimmed[0] == '!') continue;

        // Find the separator: first '=' or ':'
        const sep_pos = blk: {
            for (trimmed, 0..) |c, i| {
                if (c == '=' or c == ':') break :blk i;
            }
            continue; // No separator — skip malformed line.
        };

        const key = std.mem.trim(
            u8,
            trimmed[0..sep_pos],
            &std.ascii.whitespace,
        );
        const value = std.mem.trim(
            u8,
            trimmed[sep_pos + 1 ..],
            &std.ascii.whitespace,
        );

        if (key.len > 0) {
            try map.put(key, value);
        }
    }

    return map;
}

fn applyEnvOverrides(
    props: *std.StringHashMap([]const u8),
    allocator: std.mem.Allocator,
    environ_map: ?*const std.process.Environ.Map,
) !void {
    const keys = [_][]const u8{
        "broker.node.id",
        "broker.udp.local.host.port",
        "broker.udp.member.host.ports",
        "broker.group.name",
        "broker.storage.path",
        "broker.control.buffer.size",
        "broker.messages.buffer.size",
        "broker.transport",
        "broker.transport.engine",
        "broker.udp.mtu",
        "broker.udp.term.length",
        "broker.udp.receiver.window.length",
        "broker.udp.heartbeat.interval.ms",
        "broker.udp.session.timeout.ms",
        "broker.udp.nak.initial.delay.us",
        "broker.udp.nak.retry.delay.us",
        "broker.send.buffers.max.entries",
        "broker.send.buffers.default.size",
        "broker.send.buffers.max.total.bytes",
        "broker.send.buffers.idle.timeout.ms",
        "broker.send.buffers.drain.timeout.ms",
        "broker.af_xdp.interface",
        "broker.af_xdp.ports",
        "broker.af_xdp.rx.queue",
        "broker.af_xdp.umem.frame.count",
        "broker.af_xdp.umem.frame.size",
        "broker.threading.mode",
        "broker.idle.strategy",
        "broker.counter.values.buffer.size",
        "broker.error.log.buffer.size",
        "broker.max.services",
        "broker.max.peers",
        "broker.sender.cpu.affinity",
        "broker.receiver.cpu.affinity",
        "broker.benchmark.latency.tracing.enabled",
    };

    var env_name_buf: [256]u8 = undefined;

    for (keys) |key| {
        // Build env var name: "RINGLOOM_" + key.replace('.', '_').toUpperCase()
        var len: usize = 9;
        @memcpy(env_name_buf[0..9], "RINGLOOM_");

        for (key) |c| {
            env_name_buf[len] = if (c == '.') '_' else std.ascii.toUpper(c);
            len += 1;
        }

        const env_name: [:0]const u8 = blk: {
            env_name_buf[len] = 0;
            break :blk env_name_buf[0..len :0];
        };
        if (getEnvVar(environ_map, env_name)) |env_value| {
            const owned = try allocator.dupe(u8, env_value);
            try props.put(key, owned);
        }
    }
}

fn getEnvVar(environ_map: ?*const std.process.Environ.Map, name: []const u8) ?[]const u8 {
    const map = environ_map orelse return null;
    return map.get(name);
}

fn validate(config: *BrokerConfig) ConfigError!void {
    // ── Node ID must not conflict with a peer ───────────────────
    for (config.peer_endpoints) |peer| {
        if (peer.node_id == config.node_id) {
            return ConfigError.NodeIdConflict;
        }
    }

    // ── Buffer sizes must be power of 2 (auto-align) ────────────
    config.control_buffer_size = alignToPowerOfTwo(config.control_buffer_size);
    config.messages_buffer_size = alignToPowerOfTwo(config.messages_buffer_size);
    config.counter_values_buffer_size = alignToPowerOfTwo(config.counter_values_buffer_size);

    // ── Minimum buffer sizes ────────────────────────────────────
    if (config.control_buffer_size < 4096)
        return ConfigError.BufferSizeTooSmall;
    if (config.messages_buffer_size < 4096)
        return ConfigError.BufferSizeTooSmall;

    // ── UDP transport validation ────────────────────────────────
    const data_header_len = 64;
    if (config.udp_mtu < data_header_len + 64 or config.udp_mtu > 65_507)
        return ConfigError.InvalidValue;
    if (config.udp_term_length < 4096 or !std.math.isPowerOfTwo(config.udp_term_length))
        return ConfigError.InvalidValue;
    if (config.udp_receiver_window_length == 0 or
        config.udp_receiver_window_length > config.udp_term_length / 2)
    {
        return ConfigError.InvalidValue;
    }
    config.udp_receiver_window_length = alignToPowerOfTwo(config.udp_receiver_window_length);
    if (config.udp_receiver_window_length > config.udp_term_length / 2)
        return ConfigError.InvalidValue;
    if (config.udp_heartbeat_interval_ms == 0 or config.udp_session_timeout_ms <= config.udp_heartbeat_interval_ms)
        return ConfigError.InvalidValue;
    if (config.udp_nak_initial_delay_us == 0 or config.udp_nak_retry_delay_us == 0)
        return ConfigError.InvalidValue;

    config.send_buffers_default_size = alignToPowerOfTwo(config.send_buffers_default_size);
    if (config.send_buffers_max_entries == 0 or config.send_buffers_default_size < 4096)
        return ConfigError.InvalidValue;
    const total_send_budget = @as(u64, config.send_buffers_max_entries) *
        @as(u64, config.send_buffers_default_size);
    if (total_send_budget > config.send_buffers_max_total_bytes)
        return ConfigError.InvalidValue;

    if (config.transport_engine == .require_af_xdp and !containsPort(config.af_xdp_ports, config.local_port)) {
        return ConfigError.InvalidValue;
    }
    if (config.af_xdp_umem_frame_count == 0 or config.af_xdp_umem_frame_size < config.udp_mtu)
        return ConfigError.InvalidValue;

    // ── Compute derived fields ──────────────────────────────────
    config.max_counter_id = config.counter_values_buffer_size / 128;
    config.counter_metadata_buffer_size = config.max_counter_id * 256;
    config.single_node_cluster = config.peer_endpoints.len == 0;
}

fn containsPort(ports: []const u16, port: u16) bool {
    for (ports) |candidate| {
        if (candidate == port) return true;
    }
    return false;
}

fn alignToPowerOfTwo(value: u32) u32 {
    if (value == 0) return 1;
    if (std.math.isPowerOfTwo(value)) return value;

    var v = value;
    v -= 1;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    return v +| 1; // Saturating add to avoid overflow on u32 max.
}

// ── Tests ─────────────────────────────────────────────────────────────

test "load valid config from properties string" {
    // Given
    const content =
        \\# Test configuration
        \\broker.node.id=1
        \\broker.transport=udp
        \\broker.udp.local.host.port=10.0.0.1:9000
        \\broker.udp.member.host.ports=2@10.0.0.2:9000,3@10.0.0.3:9000
        \\broker.group.name=test-group
        \\broker.control.buffer.size=131072
        \\broker.threading.mode=SHARED
    ;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const loader = ConfigLoader.initForTesting(allocator);

    // When
    const config = try loader.parseAndBuild(content);

    // Then
    try std.testing.expectEqual(@as(u8, 1), config.node_id);
    try std.testing.expectEqualStrings("10.0.0.1", config.local_host);
    try std.testing.expectEqual(@as(u16, 9000), config.local_port);
    try std.testing.expectEqual(@as(usize, 2), config.peer_endpoints.len);
    try std.testing.expectEqual(@as(u8, 2), config.peer_endpoints[0].node_id);
    try std.testing.expectEqualStrings("10.0.0.2", config.peer_endpoints[0].host);
    try std.testing.expectEqual(@as(u16, 9000), config.peer_endpoints[0].port);
    try std.testing.expectEqualStrings("test-group", config.group_name);
    try std.testing.expectEqual(@as(u32, 131072), config.control_buffer_size);
    try std.testing.expectEqual(ThreadingMode.shared, config.threading_mode);
    try std.testing.expect(!config.single_node_cluster);
}

test "default values are applied for omitted properties" {
    // Given
    const content =
        \\broker.node.id=0
        \\broker.local.host.port=0.0.0.0:9000
    ;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const loader = ConfigLoader.initForTesting(arena.allocator());

    // When
    const config = try loader.parseAndBuild(content);

    // Then
    try std.testing.expectEqualStrings("ringloom", config.group_name);
    try std.testing.expectEqualStrings("/dev/shm", config.storage_path);
    try std.testing.expectEqual(@as(u32, 65_536), config.control_buffer_size);
    try std.testing.expectEqual(@as(u32, 1_048_576), config.messages_buffer_size);
    try std.testing.expectEqual(TransportKind.udp, config.transport);
    try std.testing.expectEqual(TransportEngine.posix, config.transport_engine);
    try std.testing.expectEqual(@as(u16, 1408), config.udp_mtu);
    try std.testing.expectEqual(@as(u32, 64 * 1024), config.udp_term_length);
    try std.testing.expectEqual(@as(u32, 32 * 1024), config.udp_receiver_window_length);
    try std.testing.expect(config.single_node_cluster);
}

test "v2 udp transport properties are parsed" {
    const content =
        \\broker.node.id=1
        \\broker.udp.local.host.port=127.0.0.1:9000
        \\broker.transport=udp
        \\broker.transport.engine=prefer_af_xdp
        \\broker.udp.mtu=1200
        \\broker.udp.term.length=131072
        \\broker.udp.receiver.window.length=65536
        \\broker.udp.heartbeat.interval.ms=100
        \\broker.udp.session.timeout.ms=1000
        \\broker.udp.nak.initial.delay.us=25
        \\broker.udp.nak.retry.delay.us=100
        \\broker.send.buffers.max.entries=8
        \\broker.send.buffers.default.size=65536
        \\broker.send.buffers.max.total.bytes=524288
        \\broker.af_xdp.interface=eth0
        \\broker.af_xdp.ports=9000,9001
    ;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const loader = ConfigLoader.initForTesting(arena.allocator());

    const config = try loader.parseAndBuild(content);

    try std.testing.expectEqual(TransportEngine.prefer_af_xdp, config.transport_engine);
    try std.testing.expectEqual(@as(u16, 1200), config.udp_mtu);
    try std.testing.expectEqual(@as(u32, 131072), config.udp_term_length);
    try std.testing.expectEqual(@as(u32, 65536), config.udp_receiver_window_length);
    try std.testing.expectEqual(@as(u64, 100), config.udp_heartbeat_interval_ms);
    try std.testing.expectEqual(@as(u32, 25), config.udp_nak_initial_delay_us);
    try std.testing.expectEqual(@as(u32, 8), config.send_buffers_max_entries);
    try std.testing.expectEqualStrings("eth0", config.af_xdp_interface.?);
    try std.testing.expectEqual(@as(usize, 2), config.af_xdp_ports.len);
}

test "windows-style line endings are handled" {
    // Given
    const content = "broker.node.id=5\r\nbroker.local.host.port=127.0.0.1:8080\r\n";

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const loader = ConfigLoader.initForTesting(arena.allocator());

    // When
    const config = try loader.parseAndBuild(content);

    // Then
    try std.testing.expectEqual(@as(u8, 5), config.node_id);
    try std.testing.expectEqual(@as(u16, 8080), config.local_port);
}

test "comments and blank lines are skipped" {
    // Given
    const content =
        \\# This is a comment
        \\! This is also a comment
        \\
        \\broker.node.id=3
        \\
        \\# Another comment
        \\broker.local.host.port=10.0.0.3:9000
    ;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const loader = ConfigLoader.initForTesting(arena.allocator());

    // When
    const config = try loader.parseAndBuild(content);

    // Then
    try std.testing.expectEqual(@as(u8, 3), config.node_id);
}

test "colon separator is accepted" {
    // Given
    const content =
        \\broker.node.id: 7
        \\broker.local.host.port: 10.0.0.7:9000
    ;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const loader = ConfigLoader.initForTesting(arena.allocator());

    // When
    const config = try loader.parseAndBuild(content);

    // Then
    try std.testing.expectEqual(@as(u8, 7), config.node_id);
}

test "missing broker.node.id returns MissingRequiredProperty" {
    // Given
    const content =
        \\broker.local.host.port=10.0.0.1:9000
    ;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const loader = ConfigLoader.initForTesting(arena.allocator());

    // When
    const result = loader.parseAndBuild(content);

    // Then
    try std.testing.expectError(ConfigError.MissingRequiredProperty, result);
}

test "missing broker.local.host.port returns MissingRequiredProperty" {
    // Given
    const content =
        \\broker.node.id=1
    ;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const loader = ConfigLoader.initForTesting(arena.allocator());

    // When
    const result = loader.parseAndBuild(content);

    // Then
    try std.testing.expectError(ConfigError.MissingRequiredProperty, result);
}

test "non-power-of-2 buffer size is auto-aligned" {
    // Given — 100_000 is not a power of 2, should round up to 131_072
    const content =
        \\broker.node.id=1
        \\broker.local.host.port=10.0.0.1:9000
        \\broker.control.buffer.size=100000
    ;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const loader = ConfigLoader.initForTesting(arena.allocator());

    // When
    const config = try loader.parseAndBuild(content);

    // Then
    try std.testing.expectEqual(@as(u32, 131_072), config.control_buffer_size);
    try std.testing.expect(std.math.isPowerOfTwo(config.control_buffer_size));
}

test "buffer size too small returns BufferSizeTooSmall" {
    // Given
    const content =
        \\broker.node.id=1
        \\broker.local.host.port=10.0.0.1:9000
        \\broker.control.buffer.size=256
    ;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const loader = ConfigLoader.initForTesting(arena.allocator());

    // When
    const result = loader.parseAndBuild(content);

    // Then
    try std.testing.expectError(ConfigError.BufferSizeTooSmall, result);
}

test "node ID conflicting with a peer returns NodeIdConflict" {
    // Given
    const content =
        \\broker.node.id=2
        \\broker.local.host.port=10.0.0.1:9000
        \\broker.member.host.ports=2@10.0.0.2:9000
    ;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const loader = ConfigLoader.initForTesting(arena.allocator());

    // When
    const result = loader.parseAndBuild(content);

    // Then
    try std.testing.expectError(ConfigError.NodeIdConflict, result);
}

test "invalid peer format returns InvalidPeerFormat" {
    // Given — missing @ separator
    const content =
        \\broker.node.id=1
        \\broker.local.host.port=10.0.0.1:9000
        \\broker.member.host.ports=10.0.0.2:9000
    ;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const loader = ConfigLoader.initForTesting(arena.allocator());

    // When
    const result = loader.parseAndBuild(content);

    // Then
    try std.testing.expectError(ConfigError.InvalidPeerFormat, result);
}

test "udp mtu out of range returns InvalidValue" {
    // Given
    const content =
        \\broker.node.id=1
        \\broker.local.host.port=10.0.0.1:9000
        \\broker.udp.mtu=100
    ;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const loader = ConfigLoader.initForTesting(arena.allocator());

    // When
    const result = loader.parseAndBuild(content);

    // Then
    try std.testing.expectError(ConfigError.InvalidValue, result);
}

test "computed fields are set after validation" {
    // Given
    const content =
        \\broker.node.id=1
        \\broker.local.host.port=10.0.0.1:9000
        \\broker.counter.values.buffer.size=65536
    ;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const loader = ConfigLoader.initForTesting(arena.allocator());

    // When
    const config = try loader.parseAndBuild(content);

    // Then — 65536 / 128 = 512 max counters
    try std.testing.expectEqual(@as(u32, 512), config.max_counter_id);
    // 512 * 256 = 131072 bytes metadata
    try std.testing.expectEqual(@as(u32, 131_072), config.counter_metadata_buffer_size);
}

test "alignToPowerOfTwo edge cases" {
    try std.testing.expectEqual(@as(u32, 1), alignToPowerOfTwo(0));
    try std.testing.expectEqual(@as(u32, 1), alignToPowerOfTwo(1));
    try std.testing.expectEqual(@as(u32, 2), alignToPowerOfTwo(2));
    try std.testing.expectEqual(@as(u32, 4), alignToPowerOfTwo(3));
    try std.testing.expectEqual(@as(u32, 4), alignToPowerOfTwo(4));
    try std.testing.expectEqual(@as(u32, 8), alignToPowerOfTwo(5));
    try std.testing.expectEqual(@as(u32, 65536), alignToPowerOfTwo(65536));
    try std.testing.expectEqual(@as(u32, 131072), alignToPowerOfTwo(100000));
}
