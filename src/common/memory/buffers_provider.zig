//! Buffers Provider — per-service metadata file cache.
//!
//! Manages mappings to other services' metadata files. The broker uses it
//! to access each registered service's control and messages ring buffers.
//! Services use it to access the broker's and other services' files.

const std = @import("std");
const ServiceMetadataFile = @import("service_metadata.zig").ServiceMetadataFile;
const BrokerMetadataFile = @import("broker_metadata.zig").BrokerMetadataFile;
const constants = @import("constants.zig");
const platform = @import("../platform.zig");

pub const BuffersProvider = struct {
    service_file: ServiceMetadataFile,
    service_id: i32,
    service_name: []const u8,

    const Self = @This();

    // ── Construction ──────────────────────────────────────────────────

    /// Get or create a BuffersProvider for the given service.
    /// If a mapping already exists in the cache, returns it.
    /// Otherwise, opens the service's metadata file and caches the result.
    pub fn getInstance(
        allocator: std.mem.Allocator,
        service_id: i32,
        service_name: []const u8,
        storage_path: []const u8,
        group: []const u8,
    ) !*BuffersProvider {
        if (cache.get(service_id)) |existing| {
            return existing;
        }

        const provider = try allocator.create(BuffersProvider);
        errdefer allocator.destroy(provider);

        provider.* = BuffersProvider{
            .service_file = try ServiceMetadataFile.open(
                storage_path,
                group,
                service_name,
                service_id,
            ),
            .service_id = service_id,
            .service_name = service_name,
        };

        try cache.put(service_id, provider);
        return provider;
    }

    /// Get the cached instance for a service, or null if not cached.
    pub fn getCached(service_id: i32) ?*BuffersProvider {
        return cache.get(service_id);
    }

    // ── Buffer Accessors ──────────────────────────────────────────────

    /// Returns the control ring buffer region for this service.
    pub fn getControlBuffer(self: *const BuffersProvider) []u8 {
        return self.service_file.getControlBuffer();
    }

    /// Returns the messages ring buffer region for this service.
    pub fn getMessagesBuffer(self: *const BuffersProvider) []u8 {
        return self.service_file.getMessagesBuffer();
    }

    // ── Health Checks ─────────────────────────────────────────────────

    /// Read the service's last heartbeat timestamp (atomic).
    pub fn readHeartbeat(self: *const BuffersProvider) i64 {
        return self.service_file.loadHeartbeat();
    }

    /// Check if the service is healthy (heartbeat within timeout).
    pub fn isHealthy(self: *const BuffersProvider) bool {
        const now_ms = platform.epochMillis();
        const last_heartbeat = self.readHeartbeat();
        return (now_ms - last_heartbeat) <= constants.default_svc_heartbeat_timeout_ms;
    }

    /// Check if the service's owning process is still alive.
    pub fn isProcessAlive(self: *const BuffersProvider) bool {
        return self.service_file.isProcessAlive();
    }

    /// Returns whether the service's ring buffers use blocking mode.
    pub fn isBlocking(self: *const BuffersProvider) bool {
        return self.service_file.isBlocking();
    }

    // ── Broker nextServiceId Access ───────────────────────────────────

    /// Increment the broker's nextServiceId counter and return the new value.
    pub fn incrementAndGetNextServiceId(broker_file: *BrokerMetadataFile) i32 {
        return broker_file.incrementAndGetNextServiceId();
    }

    // ── Cleanup ───────────────────────────────────────────────────────

    /// Close this provider's mapping and remove it from the cache.
    pub fn close(self: *BuffersProvider, allocator: std.mem.Allocator) void {
        _ = cache.remove(self.service_id);
        self.service_file.close();
        allocator.destroy(self);
    }

    /// Close all cached providers. Called at shutdown.
    pub fn closeAll(allocator: std.mem.Allocator) void {
        var iter = cache.valueIterator();
        while (iter.next()) |provider_ptr| {
            provider_ptr.*.service_file.close();
            allocator.destroy(provider_ptr.*);
        }
        cache.clearAndFree();
    }
};

/// Module-level cache of service_id → BuffersProvider instances.
var cache: std.AutoHashMap(i32, *BuffersProvider) = std.AutoHashMap(i32, *BuffersProvider).init(std.heap.page_allocator);

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "BuffersProvider cache returns same instance" {
    // Create a service metadata file first.
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realpathAlloc(testing.allocator, ".");
    defer testing.allocator.free(storage_path);
    try tmp_dir.dir.makePath("test-group/services");

    var svc_file = try ServiceMetadataFile.create(.{
        .storage_path = storage_path,
        .group = "test-group",
        .service_name = "test-svc",
        .service_id = 1,
        .node_id = 0,
    });
    // Don't close yet — keep the file on disk for BuffersProvider to open.
    svc_file.close();

    // Get via BuffersProvider.
    const provider1 = try BuffersProvider.getInstance(
        testing.allocator,
        1,
        "test-svc",
        storage_path,
        "test-group",
    );

    // Getting again should return the same pointer.
    const provider2 = try BuffersProvider.getInstance(
        testing.allocator,
        1,
        "test-svc",
        storage_path,
        "test-group",
    );

    try testing.expect(provider1 == provider2);

    // Cleanup.
    BuffersProvider.closeAll(testing.allocator);
}
