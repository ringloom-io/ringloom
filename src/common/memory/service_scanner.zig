//! Service file discovery scanner for the RingLoom broker.
//!
//! On broker startup, scans for existing service metadata files to recover
//! state after a broker restart. Live services that were registered before
//! the broker crashed are re-discovered without requiring re-registration.

const std = @import("std");
const constants = @import("constants.zig");
const ServiceMetadataFile = @import("service_metadata.zig").ServiceMetadataFile;
const BrokerMetadataFile = @import("broker_metadata.zig").BrokerMetadataFile;
const platform = @import("../platform.zig");

/// Information about a discovered live service.
pub const ServiceInstance = struct {
    service_id: i32,
    service_name: []const u8, // Allocated — caller must free.
    node_id: i16,
    metadata_file: ServiceMetadataFile,
};

/// Result of scanning the services directory.
pub const ScanResult = struct {
    /// All services whose PID is still alive and heartbeat is fresh.
    service_instances: []ServiceInstance,

    /// The next service ID to assign (max found ID + 1).
    next_service_id: i32,

    /// Paths of stale files (dead PID or expired heartbeat) for cleanup.
    stale_file_paths: [][]const u8,

    pub fn deinit(self: *ScanResult, allocator: std.mem.Allocator) void {
        for (self.service_instances) |*inst| {
            allocator.free(inst.service_name);
            inst.metadata_file.close();
        }
        allocator.free(self.service_instances);
        for (self.stale_file_paths) |path| {
            allocator.free(path);
        }
        allocator.free(self.stale_file_paths);
    }
};

pub const ServiceScanner = struct {
    storage_path: []const u8,
    group: []const u8,
    allocator: std.mem.Allocator,

    pub fn init(
        allocator: std.mem.Allocator,
        storage_path: []const u8,
        group: []const u8,
    ) ServiceScanner {
        return .{
            .storage_path = storage_path,
            .group = group,
            .allocator = allocator,
        };
    }

    /// Scan the services directory for `.dat` files.
    /// Returns live services and the next service ID to assign.
    pub fn scan(self: *const ServiceScanner) !ScanResult {
        const allocator = self.allocator;

        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const dir_path = std.fmt.bufPrint(&path_buf, "{s}/{s}/services", .{
            self.storage_path,
            self.group,
        }) catch return error.PathTooLong;

        var dir = std.fs.openDirAbsolute(dir_path, .{ .iterate = true }) catch |err| switch (err) {
            error.FileNotFound => return ScanResult{
                .service_instances = try allocator.alloc(ServiceInstance, 0),
                .next_service_id = 1,
                .stale_file_paths = try allocator.alloc([]const u8, 0),
            },
            else => return err,
        };
        defer dir.close();

        var live_list: std.ArrayList(ServiceInstance) = .empty;
        errdefer {
            for (live_list.items) |*inst| {
                allocator.free(inst.service_name);
                inst.metadata_file.close();
            }
            live_list.deinit(allocator);
        }

        var stale_list: std.ArrayList([]const u8) = .empty;
        errdefer {
            for (stale_list.items) |path| {
                allocator.free(path);
            }
            stale_list.deinit(allocator);
        }

        var max_service_id: i32 = 0;
        const now_ms = platform.Clock.epochMillis();

        var iter = dir.iterate();
        while (try iter.next()) |entry| {
            if (!std.mem.endsWith(u8, entry.name, ".dat")) continue;

            // Skip broker metadata files (broker_0.dat, broker_1.dat, etc.).
            if (BrokerMetadataFile.isBrokerMetadataFile(entry.name)) continue;

            // Parse service name, node ID, and service ID from filename.
            const parsed = parseFileName(entry.name) orelse continue;

            // Try to open the metadata file.
            var metadata_file = ServiceMetadataFile.open(
                self.storage_path,
                self.group,
                parsed.name,
                parsed.id,
                parsed.node_id,
            ) catch continue;

            // Check if the owning process is alive and heartbeat is fresh.
            const heartbeat = metadata_file.loadHeartbeat();
            const heartbeat_timeout: i64 = metadata_file.header.heartbeat_timeout_ms;
            const is_alive = metadata_file.isProcessAlive();
            const is_fresh = (now_ms - heartbeat) < heartbeat_timeout;

            if (is_alive and is_fresh) {
                if (parsed.id > max_service_id) {
                    max_service_id = parsed.id;
                }
                try live_list.append(allocator, .{
                    .service_id = parsed.id,
                    .service_name = try allocator.dupe(u8, parsed.name),
                    .node_id = metadata_file.header.node_id,
                    .metadata_file = metadata_file,
                });
            } else {
                // Stale — record the path for potential cleanup.
                var full_path_buf: [std.fs.max_path_bytes]u8 = undefined;
                const full_path = std.fmt.bufPrint(&full_path_buf, "{s}/{s}", .{
                    dir_path,
                    entry.name,
                }) catch continue;
                try stale_list.append(allocator, try allocator.dupe(u8, full_path));

                // Close the mapping for the stale file.
                metadata_file.close();
            }
        }

        return ScanResult{
            .service_instances = try live_list.toOwnedSlice(allocator),
            .next_service_id = max_service_id + 1,
            .stale_file_paths = try stale_list.toOwnedSlice(allocator),
        };
    }

    pub const ParsedFileName = struct {
        name: []const u8,
        node_id: i16,
        id: i32,
    };

    /// Parse "<name>_node<node_id>_<id>.dat" into name, node id, and service id.
    /// Legacy "<name>_<id>.dat" files are still accepted with node_id=0 so
    /// stale files from older versions can be scanned and cleaned up.
    pub fn parseFileName(file_name: []const u8) ?ParsedFileName {
        // Must end with ".dat"
        if (!std.mem.endsWith(u8, file_name, ".dat")) return null;
        if (file_name.len <= 4) return null;

        // Strip ".dat" suffix.
        const without_ext = file_name[0 .. file_name.len - 4];

        // Find the last underscore before the service id.
        const last_underscore = std.mem.lastIndexOfScalar(u8, without_ext, '_') orelse return null;
        if (last_underscore == 0) return null;

        const name_and_node = without_ext[0..last_underscore];
        const id_str = without_ext[last_underscore + 1 ..];

        const id = std.fmt.parseInt(i32, id_str, 10) catch return null;

        const node_marker = "_node";
        const node_marker_index = std.mem.lastIndexOf(u8, name_and_node, node_marker);
        if (node_marker_index) |idx| {
            const node_str = name_and_node[idx + node_marker.len ..];
            if (node_str.len > 0) {
                const node_id = std.fmt.parseInt(i16, node_str, 10) catch {
                    return .{
                        .name = name_and_node,
                        .node_id = 0,
                        .id = id,
                    };
                };
                if (idx == 0) return null;
                return .{
                    .name = name_and_node[0..idx],
                    .node_id = node_id,
                    .id = id,
                };
            }
        }

        return .{
            .name = name_and_node,
            .node_id = 0,
            .id = id,
        };
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "parse valid node-scoped service filename" {
    const parsed = ServiceScanner.parseFileName("pricing_node2_3.dat");
    try testing.expect(parsed != null);
    try testing.expectEqualStrings("pricing", parsed.?.name);
    try testing.expectEqual(@as(i16, 2), parsed.?.node_id);
    try testing.expectEqual(@as(i32, 3), parsed.?.id);
}

test "parse filename with underscores in name" {
    const parsed = ServiceScanner.parseFileName("order_service_node4_12.dat");
    try testing.expect(parsed != null);
    try testing.expectEqualStrings("order_service", parsed.?.name);
    try testing.expectEqual(@as(i16, 4), parsed.?.node_id);
    try testing.expectEqual(@as(i32, 12), parsed.?.id);
}

test "parse legacy service filename" {
    const parsed = ServiceScanner.parseFileName("order_service_12.dat");
    try testing.expect(parsed != null);
    try testing.expectEqualStrings("order_service", parsed.?.name);
    try testing.expectEqual(@as(i16, 0), parsed.?.node_id);
    try testing.expectEqual(@as(i32, 12), parsed.?.id);
}

test "reject filename without underscore" {
    const parsed = ServiceScanner.parseFileName("invalid.dat");
    try testing.expect(parsed == null);
}

test "reject filename with non-numeric id" {
    const parsed = ServiceScanner.parseFileName("service_abc.dat");
    try testing.expect(parsed == null);
}

test "reject filename without .dat extension" {
    const parsed = ServiceScanner.parseFileName("service_1.txt");
    try testing.expect(parsed == null);
}

test "reject empty underscore prefix" {
    const parsed = ServiceScanner.parseFileName("_1.dat");
    try testing.expect(parsed == null);
}
