const std = @import("std");
const brz_common = @import("brz_common");

const BrokerConfig = brz_common.config.broker_config.BrokerConfig;
const PeerEndpoint = brz_common.config.broker_config.PeerEndpoint;
const config_loader_mod = brz_common.config.config_loader;
const ConfigLoader = config_loader_mod.ConfigLoader;
const ConfigError = config_loader_mod.ConfigError;

/// Factory responsible for loading broker configuration and constructing
/// a process-level broker application wrapper.
///
/// This file intentionally focuses on configuration resolution and
/// application bootstrap orchestration. The actual broker runtime and
/// application lifecycle types are expected to live elsewhere.
///
/// Current responsibilities:
/// - resolve config path from explicit argument or default loader behavior
/// - load and validate `BrokerConfig`
/// - expose a small bootstrap result that higher layers can use
///
/// Future responsibilities:
/// - construct `BrokerApplication` once that type exists
/// - wire logging/monitoring initialization
/// - map startup failures to process exit codes
pub const BrokerApplicationFactory = struct {
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return .{
            .allocator = allocator,
        };
    }

    /// Resolve and load broker configuration.
    ///
    /// Resolution order:
    /// 1. explicit `config_path`, if provided
    /// 2. environment/default behavior implemented by `ConfigLoader.load()`
    pub fn loadConfig(self: *const Self, config_path: ?[]const u8) ConfigError!BrokerConfig {
        var loader = ConfigLoader.init(self.allocator);

        if (config_path) |path| {
            return loader.loadFromFile(path);
        }

        return loader.load();
    }

    /// Create a bootstrap result containing the loaded broker configuration.
    ///
    /// This is a transitional API that gives the executable/application layer
    /// a stable place to obtain validated configuration before a full
    /// `BrokerApplication` type is introduced.
    pub fn create(self: *const Self, config_path: ?[]const u8) ConfigError!BrokerApplicationBootstrap {
        const config = try self.loadConfig(config_path);
        return .{
            .allocator = self.allocator,
            .config = config,
        };
    }
};

/// Transitional bootstrap container returned by `BrokerApplicationFactory.create()`.
///
/// This keeps the factory useful immediately, even before a dedicated
/// `BrokerApplication` runtime wrapper is implemented.
pub const BrokerApplicationBootstrap = struct {
    allocator: std.mem.Allocator,
    config: BrokerConfig,

    const Self = @This();

    pub fn deinit(self: *Self) void {
        freeBrokerConfig(self.allocator, &self.config);
        self.* = undefined;
    }
};

/// Free heap-owned fields inside `BrokerConfig`.
///
/// The current config loader duplicates several strings and allocates the
/// peer endpoint slice. This helper centralizes cleanup so the eventual
/// application layer can safely own a loaded config.
pub fn freeBrokerConfig(allocator: std.mem.Allocator, config: *BrokerConfig) void {
    if (config.local_host.len > 0) {
        allocator.free(config.local_host);
    }

    if (config.group_name.len > 0 and !std.mem.eql(u8, config.group_name, "brz")) {
        allocator.free(config.group_name);
    }

    if (config.storage_path.len > 0 and !std.mem.eql(u8, config.storage_path, "/dev/shm")) {
        allocator.free(config.storage_path);
    }

    if (config.peer_endpoints.len > 0) {
        for (config.peer_endpoints) |peer| {
            if (peer.host.len > 0) {
                allocator.free(peer.host);
            }
        }
        allocator.free(config.peer_endpoints);
    }

    config.* = undefined;
}

test "factory loads config from explicit path" {
    // Given
    const testing = std.testing;
    var tmp = testing.tmpDir(.{});
    defer tmp.cleanup();

    const config_text =
        \\broker.node.id=1
        \\broker.local.host.port=127.0.0.1:19001
        \\broker.group.name=test-group
        \\broker.storage.path=/tmp/brz-test
    ;

    try tmp.dir.writeFile(testing.io, .{
        .sub_path = "broker.properties",
        .data = config_text,
    });

    // Get absolute path to the temp directory so loadFromFile can find the file.
    const tmp_abs = try tmp.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(tmp_abs);

    var factory = BrokerApplicationFactory.init(testing.allocator);

    const file_path = try std.fmt.allocPrint(
        testing.allocator,
        "{s}/{s}",
        .{ tmp_abs, "broker.properties" },
    );
    defer testing.allocator.free(file_path);

    var bootstrap = try factory.create(file_path);
    defer bootstrap.deinit();

    // Then
    try testing.expectEqual(@as(u8, 1), bootstrap.config.node_id);
    try testing.expectEqualStrings("127.0.0.1", bootstrap.config.local_host);
    try testing.expectEqual(@as(u16, 19001), bootstrap.config.local_port);
    try testing.expectEqualStrings("test-group", bootstrap.config.group_name);
    try testing.expectEqualStrings("/tmp/brz-test", bootstrap.config.storage_path);
}

test "freeBrokerConfig releases owned fields" {
    // Given
    const testing = std.testing;

    var peers = try testing.allocator.alloc(PeerEndpoint, 1);
    peers[0] = .{
        .node_id = 2,
        .host = try testing.allocator.dupe(u8, "127.0.0.1"),
        .port = 19002,
    };

    var config = BrokerConfig{
        .node_id = 1,
        .local_host = try testing.allocator.dupe(u8, "127.0.0.1"),
        .local_port = 19001,
        .peer_endpoints = peers,
        .group_name = try testing.allocator.dupe(u8, "custom-group"),
        .storage_path = try testing.allocator.dupe(u8, "/tmp/custom-storage"),
    };

    // When
    freeBrokerConfig(testing.allocator, &config);

    // Then
    try testing.expect(true);
}
