//! Service Registry — central in-memory data structure tracking all known
//! service instances (local and remote) and all subscriptions.
//!
//! The registry is owned by the control loop and accessed only from the
//! control thread — no locking required.

const std = @import("std");
const BuffersProvider = @import("brz_common").memory.buffers_provider.BuffersProvider;

/// A single service instance, local or remote.
pub const ServiceInstance = struct {
    service_id: i32,
    node_id: u8,
    service_name: []const u8,
    leader_election_enabled: bool,
    is_local: bool,
};

/// Composite key for the instances map: (serviceId, nodeId).
pub const InstanceKey = struct {
    service_id: i32,
    node_id: u8,
};

pub const ServiceRegistry = struct {
    /// All known instances, keyed by (serviceId, nodeId).
    /// This is the source of truth for service discovery.
    instances: std.AutoHashMap(InstanceKey, ServiceInstance),

    /// Index: serviceName → list of InstanceKeys.
    /// Maintained in sync with `instances`. Used for fast lookup by name
    /// (needed for discovery notifications and leader election).
    name_index: std.StringHashMap(std.ArrayList(InstanceKey)),

    /// Subscriptions: serviceName → set of subscriberServiceIds.
    subscriptions: std.StringHashMap(std.AutoHashMap(i32, void)),

    /// Local service ID → BuffersProvider mapping.
    /// Only populated for services on this broker's node.
    local_buffers: std.AutoHashMap(i32, *BuffersProvider),

    /// Service leader tracking: serviceName → leader serviceId.
    service_leaders: std.StringHashMap(i32),

    /// Allocator for dynamic structures.
    allocator: std.mem.Allocator,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator) Self {
        return .{
            .instances = std.AutoHashMap(InstanceKey, ServiceInstance).init(allocator),
            .name_index = std.StringHashMap(std.ArrayList(InstanceKey)).init(allocator),
            .subscriptions = std.StringHashMap(std.AutoHashMap(i32, void)).init(allocator),
            .local_buffers = std.AutoHashMap(i32, *BuffersProvider).init(allocator),
            .service_leaders = std.StringHashMap(i32).init(allocator),
            .allocator = allocator,
        };
    }

    pub fn deinit(self: *Self) void {
        // Clean up name_index ArrayLists.
        var name_iter = self.name_index.valueIterator();
        while (name_iter.next()) |list| {
            list.deinit(self.allocator);
        }
        self.name_index.deinit();

        // Clean up subscription sets.
        var sub_iter = self.subscriptions.valueIterator();
        while (sub_iter.next()) |set| {
            set.deinit();
        }
        self.subscriptions.deinit();

        self.instances.deinit();
        self.local_buffers.deinit();
        self.service_leaders.deinit();
    }

    // ── Registration ─────────────────────────────────────────────

    pub fn register(self: *Self, instance: ServiceInstance) !void {
        const key = InstanceKey{
            .service_id = instance.service_id,
            .node_id = instance.node_id,
        };

        // Reject duplicate registrations.
        if (self.instances.contains(key)) {
            return error.AlreadyRegistered;
        }

        try self.instances.put(key, instance);

        // Update the name index.
        const gop = try self.name_index.getOrPut(instance.service_name);
        if (!gop.found_existing) {
            gop.value_ptr.* = .empty;
        }
        try gop.value_ptr.append(self.allocator, key);
    }

    /// Remove a service instance. Returns the removed instance, or null
    /// if no matching instance was found.
    pub fn remove(self: *Self, service_id: i32, node_id: u8) ?ServiceInstance {
        const key = InstanceKey{ .service_id = service_id, .node_id = node_id };
        const kv = self.instances.fetchRemove(key) orelse return null;
        const removed = kv.value;

        // Remove from name index.
        if (self.name_index.getPtr(removed.service_name)) |list| {
            for (list.items, 0..) |entry, i| {
                if (entry.service_id == service_id and entry.node_id == node_id) {
                    _ = list.orderedRemove(i);
                    break;
                }
            }
        }

        return removed;
    }

    // ── Queries ──────────────────────────────────────────────────

    /// Returns all instances of a service name as InstanceKeys.
    /// Use `getInstancesByNameBuf` for the allocation-free variant.
    pub fn getInstancesByName(self: *Self, name: []const u8) []const InstanceKey {
        const keys_list = self.name_index.get(name) orelse return &.{};
        return keys_list.items;
    }

    /// Returns instances matching a name, filling a caller-provided buffer.
    /// This is the allocation-free variant used on the hot path.
    pub fn getInstancesByNameBuf(
        self: *Self,
        name: []const u8,
        out: []ServiceInstance,
    ) u32 {
        const keys_list = self.name_index.get(name) orelse return 0;
        var count: u32 = 0;
        for (keys_list.items) |key| {
            if (count >= out.len) break;
            if (self.instances.get(key)) |inst| {
                out[count] = inst;
                count += 1;
            }
        }
        return count;
    }

    /// Returns the set of subscriber service IDs for a service name.
    pub fn getSubscribers(self: *Self, name: []const u8) ?*const std.AutoHashMap(i32, void) {
        return self.subscriptions.getPtr(name);
    }

    pub fn getLocalBuffers(self: *Self, service_id: i32) ?*BuffersProvider {
        return self.local_buffers.get(service_id);
    }

    pub fn getLeader(self: *Self, service_name: []const u8) ?i32 {
        return self.service_leaders.get(service_name);
    }

    pub fn localServiceCount(self: *const Self) u32 {
        return @intCast(self.local_buffers.count());
    }

    // ── Mutations ────────────────────────────────────────────────

    pub fn setLocalBuffers(self: *Self, service_id: i32, buffers: *BuffersProvider) void {
        self.local_buffers.put(service_id, buffers) catch {};
    }

    pub fn removeLocalBuffers(self: *Self, service_id: i32) void {
        _ = self.local_buffers.remove(service_id);
    }

    pub fn addSubscription(self: *Self, service_name: []const u8, subscriber_id: i32) !void {
        const gop = try self.subscriptions.getOrPut(service_name);
        if (!gop.found_existing) {
            gop.value_ptr.* = std.AutoHashMap(i32, void).init(self.allocator);
        }
        try gop.value_ptr.put(subscriber_id, {});
    }

    pub fn removeSubscription(self: *Self, service_name: []const u8, subscriber_id: i32) void {
        if (self.subscriptions.getPtr(service_name)) |set| {
            _ = set.remove(subscriber_id);
        }
    }

    pub fn setLeader(self: *Self, service_name: []const u8, leader_service_id: i32) void {
        self.service_leaders.put(service_name, leader_service_id) catch {};
    }

    pub fn removeLeader(self: *Self, service_name: []const u8) void {
        _ = self.service_leaders.remove(service_name);
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "register service and query by name" {
    // Given
    var registry = ServiceRegistry.init(testing.allocator);
    defer registry.deinit();

    // When
    try registry.register(.{
        .service_id = 1,
        .node_id = 1,
        .service_name = "my-service",
        .leader_election_enabled = false,
        .is_local = true,
    });

    // Then
    var buf: [256]ServiceInstance = undefined;
    const count = registry.getInstancesByNameBuf("my-service", &buf);
    try testing.expectEqual(@as(u32, 1), count);
    try testing.expectEqual(@as(i32, 1), buf[0].service_id);
}

test "remove service removes from name index" {
    // Given
    var registry = ServiceRegistry.init(testing.allocator);
    defer registry.deinit();

    try registry.register(.{
        .service_id = 1,
        .node_id = 1,
        .service_name = "my-service",
        .leader_election_enabled = false,
        .is_local = true,
    });

    // When
    const removed = registry.remove(1, 1);

    // Then
    try testing.expect(removed != null);
    try testing.expectEqualStrings("my-service", removed.?.service_name);

    var buf: [256]ServiceInstance = undefined;
    const count = registry.getInstancesByNameBuf("my-service", &buf);
    try testing.expectEqual(@as(u32, 0), count);
}

test "duplicate registration returns error" {
    // Given
    var registry = ServiceRegistry.init(testing.allocator);
    defer registry.deinit();

    try registry.register(.{
        .service_id = 1,
        .node_id = 1,
        .service_name = "my-service",
        .leader_election_enabled = false,
        .is_local = true,
    });

    // When / Then
    try testing.expectError(error.AlreadyRegistered, registry.register(.{
        .service_id = 1,
        .node_id = 1,
        .service_name = "my-service",
        .leader_election_enabled = false,
        .is_local = true,
    }));
}

test "same serviceId on different nodes is allowed" {
    // Given
    var registry = ServiceRegistry.init(testing.allocator);
    defer registry.deinit();

    // When (same service_id, different node_id)
    try registry.register(.{
        .service_id = 1,
        .node_id = 1,
        .service_name = "my-service",
        .leader_election_enabled = false,
        .is_local = true,
    });
    try registry.register(.{
        .service_id = 1,
        .node_id = 2,
        .service_name = "my-service",
        .leader_election_enabled = false,
        .is_local = false,
    });

    // Then
    var buf: [256]ServiceInstance = undefined;
    const count = registry.getInstancesByNameBuf("my-service", &buf);
    try testing.expectEqual(@as(u32, 2), count);
}

test "addSubscription and getSubscribers" {
    // Given
    var registry = ServiceRegistry.init(testing.allocator);
    defer registry.deinit();

    // When
    try registry.addSubscription("service-b", 1);
    try registry.addSubscription("service-b", 5);
    try registry.addSubscription("service-c", 1);

    // Then
    const subs_b = registry.getSubscribers("service-b");
    try testing.expect(subs_b != null);
    try testing.expectEqual(@as(u32, 2), subs_b.?.count());

    const subs_c = registry.getSubscribers("service-c");
    try testing.expect(subs_c != null);
    try testing.expectEqual(@as(u32, 1), subs_c.?.count());

    const subs_d = registry.getSubscribers("service-d");
    try testing.expect(subs_d == null);
}

test "removeSubscription removes only the target" {
    // Given
    var registry = ServiceRegistry.init(testing.allocator);
    defer registry.deinit();

    try registry.addSubscription("service-b", 1);
    try registry.addSubscription("service-b", 5);

    // When
    registry.removeSubscription("service-b", 1);

    // Then
    const subs = registry.getSubscribers("service-b");
    try testing.expect(subs != null);
    try testing.expectEqual(@as(u32, 1), subs.?.count());
    try testing.expect(subs.?.contains(5));
}

test "setLeader and getLeader" {
    // Given
    var registry = ServiceRegistry.init(testing.allocator);
    defer registry.deinit();

    // When
    registry.setLeader("my-service", 42);

    // Then
    try testing.expectEqual(@as(i32, 42), registry.getLeader("my-service").?);
}

test "removeLeader clears leader" {
    // Given
    var registry = ServiceRegistry.init(testing.allocator);
    defer registry.deinit();

    registry.setLeader("my-service", 42);

    // When
    registry.removeLeader("my-service");

    // Then
    try testing.expect(registry.getLeader("my-service") == null);
}

test "localServiceCount tracks local buffers" {
    // Given
    var registry = ServiceRegistry.init(testing.allocator);
    defer registry.deinit();

    // When / Then
    try testing.expectEqual(@as(u32, 0), registry.localServiceCount());
}
