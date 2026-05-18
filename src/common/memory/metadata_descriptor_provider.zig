//! Metadata Descriptor Provider — singleton managing the broker's own metadata file.
//!
//! Exactly one instance exists per broker process. Manages the broker's
//! metadata file lifecycle, heartbeat updates, service ID assignment,
//! and service scanning.

const std = @import("std");
const BrokerMetadataFile = @import("broker_metadata.zig").BrokerMetadataFile;
const ServiceScanner = @import("service_scanner.zig").ServiceScanner;
const ScanResult = @import("service_scanner.zig").ScanResult;
const constants = @import("constants.zig");
const platform = @import("../platform.zig");

/// Module-level mutable state — initialized once at startup, reset on close.
var instance: ?MetadataDescriptorProvider = null;

pub const MetadataDescriptorProvider = struct {
    broker_file: BrokerMetadataFile,
    scanner: ServiceScanner,
    allocator: std.mem.Allocator,

    // ── Singleton Access ──────────────────────────────────────────────

    pub const InitOptions = struct {
        allocator: std.mem.Allocator,
        storage_path: []const u8,
        group: []const u8,
        node_id: i16,
        control_buffer_length: usize = constants.default_control_buffer_length,
    };

    /// Initialize the singleton. Must be called exactly once before any
    /// other access. Creates the broker metadata file and the service scanner.
    pub fn init(opts: InitOptions) !void {
        if (instance != null) return error.AlreadyInitialized;

        const broker_file = try BrokerMetadataFile.create(
            opts.storage_path,
            opts.group,
            opts.node_id,
            opts.control_buffer_length,
            0,
        );

        instance = MetadataDescriptorProvider{
            .broker_file = broker_file,
            .scanner = ServiceScanner.init(opts.allocator, opts.storage_path, opts.group),
            .allocator = opts.allocator,
        };
    }

    /// Get the singleton instance. Panics if not initialized.
    pub fn getInstance() *MetadataDescriptorProvider {
        return &(instance orelse @panic("MetadataDescriptorProvider not initialized"));
    }

    // ── Accessors ─────────────────────────────────────────────────────

    /// Returns the control ring buffer region (services → broker).
    pub fn getControlBuffer(self: *MetadataDescriptorProvider) []u8 {
        return self.broker_file.getControlBuffer();
    }

    pub fn getAeronDiscovery(self: *MetadataDescriptorProvider) *const @import("broker_metadata.zig").BrokerAeronDiscovery {
        return self.broker_file.getAeronDiscovery();
    }

    /// Update the broker's heartbeat timestamp.
    pub fn updateHeartbeat(self: *MetadataDescriptorProvider) void {
        self.broker_file.storeHeartbeat(platform.Clock.epochMillis());
    }

    /// Read the broker's current heartbeat timestamp.
    pub fn readHeartbeat(self: *MetadataDescriptorProvider) i64 {
        return self.broker_file.loadHeartbeat();
    }

    /// Assign the next service ID (atomic increment).
    pub fn assignNextServiceId(self: *MetadataDescriptorProvider) i32 {
        return self.broker_file.incrementAndGetNextServiceId();
    }

    /// Set the next service ID counter (used after scanning).
    pub fn setNextServiceId(self: *MetadataDescriptorProvider, value: i32) void {
        self.broker_file.storeNextServiceId(value);
    }

    /// Scan for existing live services on disk.
    pub fn scanServices(self: *MetadataDescriptorProvider) !ScanResult {
        return self.scanner.scan();
    }

    // ── Cleanup ───────────────────────────────────────────────────────

    /// Unmap the broker's metadata file and reset the singleton.
    pub fn close(self: *MetadataDescriptorProvider) void {
        self.broker_file.close();
        instance = null;
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "MetadataDescriptorProvider singleton lifecycle" {
    // Ensure clean state.
    if (instance != null) {
        MetadataDescriptorProvider.getInstance().close();
    }

    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(storage_path);
    try tmp_dir.dir.createDirPath(testing.io, "test-group/services");

    try MetadataDescriptorProvider.init(.{
        .allocator = testing.allocator,
        .storage_path = storage_path,
        .group = "test-group",
        .node_id = 1,
        .control_buffer_length = 4096,
    });

    const provider = MetadataDescriptorProvider.getInstance();

    // Verify buffer access (includes ring buffer trailer).
    const trailer = constants.ring_buffer_trailer_length;
    try testing.expectEqual(@as(usize, 4096 + trailer), provider.getControlBuffer().len);
    try testing.expectEqual(constants.metadata_version, provider.broker_file.header.metadata_version);

    // Verify heartbeat.
    provider.updateHeartbeat();
    try testing.expect(provider.readHeartbeat() > 0);

    // Verify service ID assignment.
    const id1 = provider.assignNextServiceId();
    const id2 = provider.assignNextServiceId();
    try testing.expectEqual(@as(i32, 2), id1);
    try testing.expectEqual(@as(i32, 3), id2);

    // Double init should fail.
    const result = MetadataDescriptorProvider.init(.{
        .allocator = testing.allocator,
        .storage_path = storage_path,
        .group = "test-group",
        .node_id = 1,
    });
    try testing.expectError(error.AlreadyInitialized, result);

    // Cleanup.
    provider.close();
}
