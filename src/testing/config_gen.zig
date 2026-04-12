//! Configuration file generator for BRZ end-to-end tests.
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
//! broker.local.host.port=127.0.0.1:19001
//! broker.member.host.ports=2@10.0.0.2:19002,3@10.0.0.3:19003
//! broker.group.name=brz-test
//! broker.storage.path=/tmp/brz-e2e-…/storage
//! broker.control.buffer.size=65536
//! broker.messages.buffer.size=1048576
//! broker.threading.mode=dedicated
//! broker.idle.strategy=backoff
//! ```
//!
//! Service properties (`service_<name>.properties`):
//! ```text
//! brz.service.name=my-service
//! brz.group.name=brz-test
//! brz.storage.path=/tmp/brz-e2e-…/storage
//! brz.broker.node.id=1
//! brz.service.leader_election.enabled=false
//! ```

const std = @import("std");
const fs = std.fs;
const mem = std.mem;
const Allocator = std.mem.Allocator;

const harness = @import("harness.zig");
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

        const file = try fs.cwd().createFile(file_name, .{ .truncate = true });
        defer file.close();
        try file.writeAll(content);

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

        const file = try fs.cwd().createFile(file_name, .{ .truncate = true });
        defer file.close();
        try file.writeAll(content);

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

        const writer = buf.writer(self.allocator);

        try writer.print("# Auto-generated broker config for node {d}\n", .{spec.node_id});
        try writer.print("broker.node.id={d}\n", .{spec.node_id});
        try writer.print("broker.local.host.port={s}:{d}\n", .{ spec.host, spec.port });

        if (peers_str.len > 0) {
            try writer.print("broker.member.host.ports={s}\n", .{peers_str});
        }

        try writer.print("broker.group.name={s}\n", .{spec.group_name});
        try writer.print("broker.storage.path={s}\n", .{storage_path});
        try writer.print("broker.control.buffer.size={d}\n", .{spec.control_buffer_size});
        try writer.print("broker.messages.buffer.size={d}\n", .{spec.messages_buffer_size});
        try writer.print("broker.threading.mode={s}\n", .{spec.threading_mode});
        try writer.print("broker.idle.strategy={s}\n", .{spec.idle_strategy});

        if (spec.sender_cpu_affinity) |core| {
            try writer.print("broker.sender.cpu.affinity={d}\n", .{core});
        }
        if (spec.receiver_cpu_affinity) |core| {
            try writer.print("broker.receiver.cpu.affinity={d}\n", .{core});
        }

        return self.allocator.dupe(u8, buf.items);
    }

    fn formatServiceProperties(
        self: *const ConfigGen,
        spec: ServiceSpec,
        storage_path: []const u8,
    ) ![]const u8 {
        var buf: std.ArrayList(u8) = .empty;
        defer buf.deinit(self.allocator);

        const writer = buf.writer(self.allocator);

        try writer.print("# Auto-generated service config for {s}\n", .{spec.service_name});
        try writer.print("brz.service.name={s}\n", .{spec.service_name});
        try writer.print("brz.group.name={s}\n", .{spec.group_name});
        try writer.print("brz.storage.path={s}\n", .{storage_path});
        try writer.print("brz.broker.node.id={d}\n", .{spec.broker_node_id});

        const le_str = if (spec.leader_election_enabled) "true" else "false";
        try writer.print("brz.service.leader_election.enabled={s}\n", .{le_str});

        return self.allocator.dupe(u8, buf.items);
    }

    fn formatPeerEndpoints(self: *const ConfigGen, peers: []const PeerSpec) ![]const u8 {
        if (peers.len == 0) {
            return self.allocator.dupe(u8, "");
        }

        var buf: std.ArrayList(u8) = .empty;
        defer buf.deinit(self.allocator);

        const writer = buf.writer(self.allocator);

        for (peers, 0..) |peer, i| {
            if (i > 0) {
                try writer.writeByte(',');
            }
            try writer.print("{d}@{s}:{d}", .{ peer.node_id, peer.host, peer.port });
        }

        return self.allocator.dupe(u8, buf.items);
    }
};

// ── Test helpers ─────────────────────────────────────────────────────

fn readFileContent(allocator: Allocator, path: []const u8) ![]const u8 {
    const file = try fs.cwd().openFile(path, .{});
    defer file.close();
    return file.readToEndAlloc(allocator, 1024 * 1024);
}

// ── Tests ────────────────────────────────────────────────────────────

test "writeBrokerConfig generates valid properties file" {
    // Given
    const allocator = std.testing.allocator;

    const tmp_dir = "/tmp/brz-test-config-gen-broker";
    try fs.cwd().makePath(tmp_dir);
    defer fs.cwd().deleteTree(tmp_dir) catch {};

    const gen = ConfigGen.init(allocator);
    const spec = BrokerSpec{};

    // When
    const path = try gen.writeBrokerConfig(tmp_dir, spec, "/tmp/storage");
    defer allocator.free(path);

    // Then — the file must exist and contain expected keys.
    const content = try readFileContent(allocator, path);
    defer allocator.free(content);

    try std.testing.expect(mem.indexOf(u8, content, "broker.node.id=1") != null);
    try std.testing.expect(mem.indexOf(u8, content, "broker.local.host.port=127.0.0.1:19001") != null);
    try std.testing.expect(mem.indexOf(u8, content, "broker.group.name=brz-test") != null);
    try std.testing.expect(mem.indexOf(u8, content, "broker.storage.path=/tmp/storage") != null);
    try std.testing.expect(mem.indexOf(u8, content, "broker.control.buffer.size=65536") != null);
    try std.testing.expect(mem.indexOf(u8, content, "broker.messages.buffer.size=1048576") != null);
    try std.testing.expect(mem.indexOf(u8, content, "broker.threading.mode=dedicated") != null);
    try std.testing.expect(mem.indexOf(u8, content, "broker.idle.strategy=backoff") != null);
}

test "writeBrokerConfig file is named broker_<node_id>.properties" {
    // Given
    const allocator = std.testing.allocator;

    const tmp_dir = "/tmp/brz-test-config-gen-name";
    try fs.cwd().makePath(tmp_dir);
    defer fs.cwd().deleteTree(tmp_dir) catch {};

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

    const tmp_dir = "/tmp/brz-test-config-gen-peers";
    try fs.cwd().makePath(tmp_dir);
    defer fs.cwd().deleteTree(tmp_dir) catch {};

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

    try std.testing.expect(mem.indexOf(u8, content, "broker.member.host.ports=2@10.0.0.2:19002,3@10.0.0.3:19003") != null);
}

test "writeBrokerConfig omits peer line when no peers" {
    // Given
    const allocator = std.testing.allocator;

    const tmp_dir = "/tmp/brz-test-config-gen-nopeers";
    try fs.cwd().makePath(tmp_dir);
    defer fs.cwd().deleteTree(tmp_dir) catch {};

    const gen = ConfigGen.init(allocator);
    const spec = BrokerSpec{ .peers = &.{} };

    // When
    const path = try gen.writeBrokerConfig(tmp_dir, spec, "/tmp/s");
    defer allocator.free(path);

    // Then — the peer line should not appear.
    const content = try readFileContent(allocator, path);
    defer allocator.free(content);

    try std.testing.expect(mem.indexOf(u8, content, "broker.member.host.ports") == null);
}

test "writeBrokerConfig respects custom spec values" {
    // Given
    const allocator = std.testing.allocator;

    const tmp_dir = "/tmp/brz-test-config-gen-custom";
    try fs.cwd().makePath(tmp_dir);
    defer fs.cwd().deleteTree(tmp_dir) catch {};

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
    try std.testing.expect(mem.indexOf(u8, content, "broker.local.host.port=192.168.1.10:25000") != null);
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

    const tmp_dir = "/tmp/brz-test-config-gen-svc";
    try fs.cwd().makePath(tmp_dir);
    defer fs.cwd().deleteTree(tmp_dir) catch {};

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

    try std.testing.expect(mem.indexOf(u8, content, "brz.service.name=my-service") != null);
    try std.testing.expect(mem.indexOf(u8, content, "brz.group.name=brz-test") != null);
    try std.testing.expect(mem.indexOf(u8, content, "brz.storage.path=/tmp/storage") != null);
    try std.testing.expect(mem.indexOf(u8, content, "brz.broker.node.id=1") != null);
    try std.testing.expect(mem.indexOf(u8, content, "brz.service.leader_election.enabled=false") != null);
}

test "writeServiceConfig file is named service_<name>.properties" {
    // Given
    const allocator = std.testing.allocator;

    const tmp_dir = "/tmp/brz-test-config-gen-svcname";
    try fs.cwd().makePath(tmp_dir);
    defer fs.cwd().deleteTree(tmp_dir) catch {};

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

    const tmp_dir = "/tmp/brz-test-config-gen-svcle";
    try fs.cwd().makePath(tmp_dir);
    defer fs.cwd().deleteTree(tmp_dir) catch {};

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

    try std.testing.expect(mem.indexOf(u8, content, "brz.service.leader_election.enabled=true") != null);
}

test "writeServiceConfig with custom broker node and group" {
    // Given
    const allocator = std.testing.allocator;

    const tmp_dir = "/tmp/brz-test-config-gen-svccustom";
    try fs.cwd().makePath(tmp_dir);
    defer fs.cwd().deleteTree(tmp_dir) catch {};

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

    try std.testing.expect(mem.indexOf(u8, content, "brz.broker.node.id=3") != null);
    try std.testing.expect(mem.indexOf(u8, content, "brz.group.name=prod-group") != null);
    try std.testing.expect(mem.indexOf(u8, content, "brz.storage.path=/opt/shm") != null);
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
