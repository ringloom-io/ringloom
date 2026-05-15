//! Configuration file generator for RingLoom end-to-end tests.
//!
//! `ConfigGen` produces `.properties` files consumed by the broker and
//! service runtimes.  Each generated file is written to the harness's
//! `config/` directory and its path is returned so that it can be passed
//! as a command-line argument when spawning child processes.
//!
//! ## Generated file format
//!
//! Broker properties (`broker_<node_id>.properties`):
//! ```text
//! broker.node.id=1
//! broker.transport=udp
//! broker.udp.local.host.port=127.0.0.1:19001
//! broker.udp.member.host.ports=2@10.0.0.2:19002,3@10.0.0.3:19003
//! broker.group.name=ringloom-test
//! broker.storage.path=/tmp/ringloom-e2e-…/storage
//! broker.control.buffer.size=65536
//! broker.messages.buffer.size=1048576
//! broker.threading.mode=dedicated
//! broker.idle.strategy=backoff
//! ```
//!
//! Service properties (`service_<name>.properties`):
//! ```text
//! ringloom.service.name=my-service
//! ringloom.group.name=ringloom-test
//! ringloom.storage.path=/tmp/ringloom-e2e-…/storage
//! ringloom.broker.node.id=1
//! ringloom.service.leader_election.enabled=false
//! ```

const std = @import("std");
const mem = std.mem;
const Allocator = std.mem.Allocator;

const harness = @import("harness.zig");
const test_io = @import("io.zig");
const BrokerSpec = harness.BrokerSpec;
const ServiceSpec = harness.ServiceSpec;
const PeerSpec = harness.PeerSpec;

/// Generates broker and service configuration property files for tests.
pub const ConfigGen = struct {
    allocator: Allocator,

    /// Creates a new `ConfigGen` bound to the given allocator.
    pub fn init(allocator: Allocator) ConfigGen {
        return .{ .allocator = allocator };
    }

    /// Writes a broker properties file to `<dir_path>/broker_<node_id>.properties`.
    ///
    /// Returns the full path of the generated file (heap-allocated, caller-owned).
    pub fn writeBrokerConfig(
        self: *const ConfigGen,
        dir_path: []const u8,
        spec: BrokerSpec,
        storage_path: []const u8,
    ) ![]const u8 {
        const file_name = try std.fmt.allocPrint(
            self.allocator,
            "{s}/broker_{d}.properties",
            .{ dir_path, spec.node_id },
        );
        errdefer self.allocator.free(file_name);

        const content = try self.formatBrokerProperties(spec, storage_path);
        defer self.allocator.free(content);

        try test_io.writeFile(file_name, content);

        return file_name;
    }

    /// Writes a service properties file to `<dir_path>/service_<name>.properties`.
    ///
    /// Returns the full path of the generated file (heap-allocated, caller-owned).
    pub fn writeServiceConfig(
        self: *const ConfigGen,
        dir_path: []const u8,
        spec: ServiceSpec,
        storage_path: []const u8,
    ) ![]const u8 {
        const file_name = try std.fmt.allocPrint(
            self.allocator,
            "{s}/service_{s}.properties",
            .{ dir_path, spec.service_name },
        );
        errdefer self.allocator.free(file_name);

        const content = try self.formatServiceProperties(spec, storage_path);
        defer self.allocator.free(content);

        try test_io.writeFile(file_name, content);

        return file_name;
    }

    // ── Internal formatting ──────────────────────────────────────

    fn formatBrokerProperties(
        self: *const ConfigGen,
        spec: BrokerSpec,
        storage_path: []const u8,
    ) ![]const u8 {
        // Build the peer endpoints string: "node_id@host:port,…"
        const peers_str = try self.formatPeerEndpoints(spec.peers);
        defer self.allocator.free(peers_str);

        // Start with the required properties.
        var buf: std.ArrayList(u8) = .empty;
        defer buf.deinit(self.allocator);
        var writer: std.Io.Writer.Allocating = .fromArrayList(self.allocator, &buf);
        defer buf = writer.toArrayList();

        try writer.writer.print("# Auto-generated broker config for node {d}\n", .{spec.node_id});
        try writer.writer.print("broker.node.id={d}\n", .{spec.node_id});
        try writer.writer.print("broker.transport={s}\n", .{spec.transport});
        try writer.writer.print("broker.transport.engine={s}\n", .{spec.transport_engine});
        try writer.writer.print("broker.udp.local.host.port={s}:{d}\n", .{ spec.host, spec.port });

        if (peers_str.len > 0) {
            try writer.writer.print("broker.udp.member.host.ports={s}\n", .{peers_str});
        }

        try writer.writer.print("broker.group.name={s}\n", .{spec.group_name});
        try writer.writer.print("broker.storage.path={s}\n", .{storage_path});
        try writer.writer.print("broker.control.buffer.size={d}\n", .{spec.control_buffer_size});
        try writer.writer.print("broker.messages.buffer.size={d}\n", .{spec.messages_buffer_size});
        try writer.writer.print("broker.threading.mode={s}\n", .{spec.threading_mode});
        try writer.writer.print("broker.idle.strategy={s}\n", .{spec.idle_strategy});

        if (spec.sender_cpu_affinity) |core| {
            try writer.writer.print("broker.sender.cpu.affinity={d}\n", .{core});
        }
        if (spec.receiver_cpu_affinity) |core| {
            try writer.writer.print("broker.receiver.cpu.affinity={d}\n", .{core});
        }
        try writer.writer.print("broker.udp.mtu={d}\n", .{spec.udp_mtu});
        try writer.writer.print("broker.udp.term.length={d}\n", .{spec.udp_term_length});
        try writer.writer.print("broker.udp.receiver.window.length={d}\n", .{spec.udp_receiver_window_length});
        try writer.writer.print("broker.udp.heartbeat.interval.ms={d}\n", .{spec.udp_heartbeat_interval_ms});
        try writer.writer.print("broker.udp.session.timeout.ms={d}\n", .{spec.udp_session_timeout_ms});
        try writer.writer.print("broker.udp.nak.initial.delay.us={d}\n", .{spec.udp_nak_initial_delay_us});
        try writer.writer.print("broker.udp.nak.retry.delay.us={d}\n", .{spec.udp_nak_retry_delay_us});
        try writer.writer.print("broker.send.buffers.max.entries={d}\n", .{spec.send_buffers_max_entries});
        try writer.writer.print("broker.send.buffers.default.size={d}\n", .{spec.send_buffers_default_size});
        try writer.writer.print("broker.send.buffers.max.total.bytes={d}\n", .{spec.send_buffers_max_total_bytes});
        if (spec.af_xdp_interface) |interface| {
            try writer.writer.print("broker.af_xdp.interface={s}\n", .{interface});
        }
        if (spec.af_xdp_ports.len > 0) {
            try writer.writer.writeAll("broker.af_xdp.ports=");
            for (spec.af_xdp_ports, 0..) |port, i| {
                if (i > 0) try writer.writer.writeByte(',');
                try writer.writer.print("{d}", .{port});
            }
            try writer.writer.writeByte('\n');
        }
        try writer.writer.print("broker.af_xdp.rx.queue={d}\n", .{spec.af_xdp_rx_queue});
        if (spec.benchmark_latency_tracing_enabled) {
            try writer.writer.writeAll("broker.benchmark.latency.tracing.enabled=true\n");
        }

        return self.allocator.dupe(u8, writer.written());
    }

    fn formatServiceProperties(
        self: *const ConfigGen,
        spec: ServiceSpec,
        storage_path: []const u8,
    ) ![]const u8 {
        var buf: std.ArrayList(u8) = .empty;
        defer buf.deinit(self.allocator);
        var writer: std.Io.Writer.Allocating = .fromArrayList(self.allocator, &buf);
        defer buf = writer.toArrayList();

        try writer.writer.print("# Auto-generated service config for {s}\n", .{spec.service_name});
        try writer.writer.print("ringloom.service.name={s}\n", .{spec.service_name});
        try writer.writer.print("ringloom.group.name={s}\n", .{spec.group_name});
        try writer.writer.print("ringloom.storage.path={s}\n", .{storage_path});
        try writer.writer.print("ringloom.broker.node.id={d}\n", .{spec.broker_node_id});

        const le_str = if (spec.leader_election_enabled) "true" else "false";
        try writer.writer.print("ringloom.service.leader_election.enabled={s}\n", .{le_str});

        return self.allocator.dupe(u8, writer.written());
    }

    fn formatPeerEndpoints(self: *const ConfigGen, peers: []const PeerSpec) ![]const u8 {
        if (peers.len == 0) {
            return self.allocator.dupe(u8, "");
        }

        var buf: std.ArrayList(u8) = .empty;
        defer buf.deinit(self.allocator);
        var writer: std.Io.Writer.Allocating = .fromArrayList(self.allocator, &buf);
        defer buf = writer.toArrayList();

        for (peers, 0..) |peer, i| {
            if (i > 0) {
                try writer.writer.writeByte(',');
            }
            try writer.writer.print("{d}@{s}:{d}", .{ peer.node_id, peer.host, peer.port });
        }

        return self.allocator.dupe(u8, writer.written());
    }
};

// ── Test helpers ─────────────────────────────────────────────────────

fn readFileContent(allocator: Allocator, path: []const u8) ![]const u8 {
    return test_io.readFileAlloc(allocator, path, 1024 * 1024);
}

// ── Tests ────────────────────────────────────────────────────────────

test "writeBrokerConfig generates valid properties file" {
    // Given
    const allocator = std.testing.allocator;

    const tmp_dir = "/tmp/ringloom-test-config-gen-broker";
    try test_io.createDirPath(tmp_dir);
    defer test_io.deleteTree(tmp_dir) catch {};

    const gen = ConfigGen.init(allocator);
    const spec = BrokerSpec{};

    // When
    const path = try gen.writeBrokerConfig(tmp_dir, spec, "/tmp/storage");
    defer allocator.free(path);

    // Then — the file must exist and contain expected keys.
    const content = try readFileContent(allocator, path);
    defer allocator.free(content);

    try std.testing.expect(mem.indexOf(u8, content, "broker.node.id=1") != null);
    try std.testing.expect(mem.indexOf(u8, content, "broker.transport=udp") != null);
    try std.testing.expect(mem.indexOf(u8, content, "broker.transport.engine=posix") != null);
    try std.testing.expect(mem.indexOf(u8, content, "broker.udp.local.host.port=127.0.0.1:19001") != null);
    try std.testing.expect(mem.indexOf(u8, content, "broker.group.name=ringloom-test") != null);
    try std.testing.expect(mem.indexOf(u8, content, "broker.storage.path=/tmp/storage") != null);
    try std.testing.expect(mem.indexOf(u8, content, "broker.control.buffer.size=65536") != null);
    try std.testing.expect(mem.indexOf(u8, content, "broker.messages.buffer.size=1048576") != null);
    try std.testing.expect(mem.indexOf(u8, content, "broker.threading.mode=dedicated") != null);
    try std.testing.expect(mem.indexOf(u8, content, "broker.idle.strategy=backoff") != null);
}

test "writeBrokerConfig file is named broker_<node_id>.properties" {
    // Given
    const allocator = std.testing.allocator;

    const tmp_dir = "/tmp/ringloom-test-config-gen-name";
    try test_io.createDirPath(tmp_dir);
    defer test_io.deleteTree(tmp_dir) catch {};

    const gen = ConfigGen.init(allocator);
    const spec = BrokerSpec{ .node_id = 7 };

    // When
    const path = try gen.writeBrokerConfig(tmp_dir, spec, "/tmp/s");
    defer allocator.free(path);

    // Then
    try std.testing.expect(mem.endsWith(u8, path, "/broker_7.properties"));
}

test "writeBrokerConfig includes peer endpoints" {
    // Given
    const allocator = std.testing.allocator;

    const tmp_dir = "/tmp/ringloom-test-config-gen-peers";
    try test_io.createDirPath(tmp_dir);
    defer test_io.deleteTree(tmp_dir) catch {};

    const gen = ConfigGen.init(allocator);
    const peers = [_]PeerSpec{
        .{ .node_id = 2, .host = "10.0.0.2", .port = 19002 },
        .{ .node_id = 3, .host = "10.0.0.3", .port = 19003 },
    };
    const spec = BrokerSpec{
        .node_id = 1,
        .peers = &peers,
    };

    // When
    const path = try gen.writeBrokerConfig(tmp_dir, spec, "/tmp/storage");
    defer allocator.free(path);

    // Then
    const content = try readFileContent(allocator, path);
    defer allocator.free(content);

    try std.testing.expect(mem.indexOf(u8, content, "broker.udp.member.host.ports=2@10.0.0.2:19002,3@10.0.0.3:19003") != null);
}

test "writeBrokerConfig omits peer line when no peers" {
    // Given
    const allocator = std.testing.allocator;

    const tmp_dir = "/tmp/ringloom-test-config-gen-nopeers";
    try test_io.createDirPath(tmp_dir);
    defer test_io.deleteTree(tmp_dir) catch {};

    const gen = ConfigGen.init(allocator);
    const spec = BrokerSpec{ .peers = &.{} };

    // When
    const path = try gen.writeBrokerConfig(tmp_dir, spec, "/tmp/s");
    defer allocator.free(path);

    // Then — the peer line should not appear.
    const content = try readFileContent(allocator, path);
    defer allocator.free(content);

    try std.testing.expect(mem.indexOf(u8, content, "broker.udp.member.host.ports") == null);
}

test "writeBrokerConfig respects custom spec values" {
    // Given
    const allocator = std.testing.allocator;

    const tmp_dir = "/tmp/ringloom-test-config-gen-custom";
    try test_io.createDirPath(tmp_dir);
    defer test_io.deleteTree(tmp_dir) catch {};

    const gen = ConfigGen.init(allocator);
    const spec = BrokerSpec{
        .node_id = 5,
        .host = "192.168.1.10",
        .port = 25000,
        .group_name = "custom-group",
        .threading_mode = "shared",
        .idle_strategy = "sleeping",
        .control_buffer_size = 131_072,
        .messages_buffer_size = 2_097_152,
    };

    // When
    const path = try gen.writeBrokerConfig(tmp_dir, spec, "/mnt/fast");
    defer allocator.free(path);

    // Then
    const content = try readFileContent(allocator, path);
    defer allocator.free(content);

    try std.testing.expect(mem.indexOf(u8, content, "broker.node.id=5") != null);
    try std.testing.expect(mem.indexOf(u8, content, "broker.udp.local.host.port=192.168.1.10:25000") != null);
    try std.testing.expect(mem.indexOf(u8, content, "broker.group.name=custom-group") != null);
    try std.testing.expect(mem.indexOf(u8, content, "broker.storage.path=/mnt/fast") != null);
    try std.testing.expect(mem.indexOf(u8, content, "broker.control.buffer.size=131072") != null);
    try std.testing.expect(mem.indexOf(u8, content, "broker.messages.buffer.size=2097152") != null);
    try std.testing.expect(mem.indexOf(u8, content, "broker.threading.mode=shared") != null);
    try std.testing.expect(mem.indexOf(u8, content, "broker.idle.strategy=sleeping") != null);
}

test "writeServiceConfig generates valid properties file" {
    // Given
    const allocator = std.testing.allocator;

    const tmp_dir = "/tmp/ringloom-test-config-gen-svc";
    try test_io.createDirPath(tmp_dir);
    defer test_io.deleteTree(tmp_dir) catch {};

    const gen = ConfigGen.init(allocator);
    const spec = ServiceSpec{
        .executable_name = "my-svc",
        .service_name = "my-service",
    };

    // When
    const path = try gen.writeServiceConfig(tmp_dir, spec, "/tmp/storage");
    defer allocator.free(path);

    // Then
    const content = try readFileContent(allocator, path);
    defer allocator.free(content);

    try std.testing.expect(mem.indexOf(u8, content, "ringloom.service.name=my-service") != null);
    try std.testing.expect(mem.indexOf(u8, content, "ringloom.group.name=ringloom-test") != null);
    try std.testing.expect(mem.indexOf(u8, content, "ringloom.storage.path=/tmp/storage") != null);
    try std.testing.expect(mem.indexOf(u8, content, "ringloom.broker.node.id=1") != null);
    try std.testing.expect(mem.indexOf(u8, content, "ringloom.service.leader_election.enabled=false") != null);
}

test "writeServiceConfig file is named service_<name>.properties" {
    // Given
    const allocator = std.testing.allocator;

    const tmp_dir = "/tmp/ringloom-test-config-gen-svcname";
    try test_io.createDirPath(tmp_dir);
    defer test_io.deleteTree(tmp_dir) catch {};

    const gen = ConfigGen.init(allocator);
    const spec = ServiceSpec{
        .executable_name = "svc-exe",
        .service_name = "heartbeat-sender",
    };

    // When
    const path = try gen.writeServiceConfig(tmp_dir, spec, "/tmp/s");
    defer allocator.free(path);

    // Then
    try std.testing.expect(mem.endsWith(u8, path, "/service_heartbeat-sender.properties"));
}

test "writeServiceConfig with leader election enabled" {
    // Given
    const allocator = std.testing.allocator;

    const tmp_dir = "/tmp/ringloom-test-config-gen-svcle";
    try test_io.createDirPath(tmp_dir);
    defer test_io.deleteTree(tmp_dir) catch {};

    const gen = ConfigGen.init(allocator);
    const spec = ServiceSpec{
        .executable_name = "leader-svc",
        .service_name = "leader-service",
        .leader_election_enabled = true,
    };

    // When
    const path = try gen.writeServiceConfig(tmp_dir, spec, "/tmp/storage");
    defer allocator.free(path);

    // Then
    const content = try readFileContent(allocator, path);
    defer allocator.free(content);

    try std.testing.expect(mem.indexOf(u8, content, "ringloom.service.leader_election.enabled=true") != null);
}

test "writeServiceConfig with custom broker node and group" {
    // Given
    const allocator = std.testing.allocator;

    const tmp_dir = "/tmp/ringloom-test-config-gen-svccustom";
    try test_io.createDirPath(tmp_dir);
    defer test_io.deleteTree(tmp_dir) catch {};

    const gen = ConfigGen.init(allocator);
    const spec = ServiceSpec{
        .executable_name = "custom-svc",
        .service_name = "custom-service",
        .broker_node_id = 3,
        .group_name = "prod-group",
    };

    // When
    const path = try gen.writeServiceConfig(tmp_dir, spec, "/opt/shm");
    defer allocator.free(path);

    // Then
    const content = try readFileContent(allocator, path);
    defer allocator.free(content);

    try std.testing.expect(mem.indexOf(u8, content, "ringloom.broker.node.id=3") != null);
    try std.testing.expect(mem.indexOf(u8, content, "ringloom.group.name=prod-group") != null);
    try std.testing.expect(mem.indexOf(u8, content, "ringloom.storage.path=/opt/shm") != null);
}

test "formatPeerEndpoints with empty peers returns empty string" {
    // Given
    const allocator = std.testing.allocator;
    const gen = ConfigGen.init(allocator);

    // When
    const result = try gen.formatPeerEndpoints(&.{});
    defer allocator.free(result);

    // Then
    try std.testing.expectEqualStrings("", result);
}

test "formatPeerEndpoints with single peer" {
    // Given
    const allocator = std.testing.allocator;
    const gen = ConfigGen.init(allocator);
    const peers = [_]PeerSpec{
        .{ .node_id = 2, .host = "10.0.0.2", .port = 9000 },
    };

    // When
    const result = try gen.formatPeerEndpoints(&peers);
    defer allocator.free(result);

    // Then
    try std.testing.expectEqualStrings("2@10.0.0.2:9000", result);
}

test "formatPeerEndpoints with multiple peers" {
    // Given
    const allocator = std.testing.allocator;
    const gen = ConfigGen.init(allocator);
    const peers = [_]PeerSpec{
        .{ .node_id = 2, .host = "10.0.0.2", .port = 9000 },
        .{ .node_id = 3, .host = "10.0.0.3", .port = 9001 },
        .{ .node_id = 4, .host = "10.0.0.4", .port = 9002 },
    };

    // When
    const result = try gen.formatPeerEndpoints(&peers);
    defer allocator.free(result);

    // Then
    try std.testing.expectEqualStrings("2@10.0.0.2:9000,3@10.0.0.3:9001,4@10.0.0.4:9002", result);
}
