//! Service Registry — central in-memory data structure tracking all known
//! service instances (local and remote) and all subscriptions.
//!
//! The registry is owned by the control loop and accessed only from the
//! control thread — no locking required.

const std = @import("std");
const ringloom_common = @import("ringloom_common");
const BuffersProvider = ringloom_common.memory.buffers_provider.BuffersProvider;
const PressureState = ringloom_common.memory.PressureState;

/// A single service instance, local or remote.
pub const ServiceInstance = struct {
    service_id: i32,
    node_id: u8,
    service_name: []const u8,
    leader_election_enabled: bool,
    is_local: bool,
    fc_slot_id: i32 = -1,
    fc_slot_generation: u16 = 0,
    messages_buffer_capacity: u32 = 0,
    fc_pressure_state: PressureState = .unknown,
    last_fc_remaining: u32 = 0,
    last_fc_broadcast_ns: i64 = 0,
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
        // Free owned instance names.
        var inst_iter = self.instances.valueIterator();
        while (inst_iter.next()) |inst| {
            self.allocator.free(@constCast(inst.service_name));
        }
        self.instances.deinit();

        // Free owned name_index keys and lists.
        var name_iter = self.name_index.iterator();
        while (name_iter.next()) |entry| {
            entry.value_ptr.deinit(self.allocator);
            self.allocator.free(@constCast(entry.key_ptr.*));
        }
        self.name_index.deinit();

        // Free owned subscription keys and sets.
        var sub_iter = self.subscriptions.iterator();
        while (sub_iter.next()) |entry| {
            entry.value_ptr.deinit();
            self.allocator.free(@constCast(entry.key_ptr.*));
        }
        self.subscriptions.deinit();

        // Free owned leader keys.
        var leader_iter = self.service_leaders.keyIterator();
        while (leader_iter.next()) |key_ptr| {
            self.allocator.free(@constCast(key_ptr.*));
        }
        self.service_leaders.deinit();

        self.local_buffers.deinit();
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

        // Own the service name — the caller's slice may point to transient
        // ring-buffer memory that gets reused after the callback returns.
        const owned_name = try self.allocator.dupe(u8, instance.service_name);

        var owned_instance = instance;
        owned_instance.service_name = owned_name;

        self.instances.put(key, owned_instance) catch |err| {
            self.allocator.free(owned_name);
            return err;
        };

        // instances now owns owned_name. Update name index with a separately
        // owned key so that removing one instance doesn't invalidate the key
        // while other instances of the same name remain.
        const needs_new_key = !self.name_index.contains(owned_name);
        const owned_key: ?[]u8 = if (needs_new_key)
            self.allocator.dupe(u8, instance.service_name) catch return error.OutOfMemory
        else
            null;

        const gop = self.name_index.getOrPut(if (owned_key) |k| k else owned_name) catch {
            if (owned_key) |k| self.allocator.free(k);
            return error.OutOfMemory;
        };
        if (!gop.found_existing) {
            gop.value_ptr.* = .empty;
        }
        gop.value_ptr.append(self.allocator, key) catch return error.OutOfMemory;
    }

    /// Remove a service instance. Returns the removed instance, or null
    /// if no matching instance was found.
    /// The caller must free the returned instance's `service_name` with
    /// `self.allocator.free()` after use, since the registry owns it.
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
            // If the list is now empty, remove the name_index entry and free
            // the owned key.
            if (list.items.len == 0) {
                list.deinit(self.allocator);
                const entry = self.name_index.fetchRemove(removed.service_name).?;
                self.allocator.free(@constCast(entry.key));
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

    pub fn getInstancePtr(self: *Self, service_id: i32, node_id: u8) ?*ServiceInstance {
        return self.instances.getPtr(.{ .service_id = service_id, .node_id = node_id });
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

    pub fn setFlowControlSlot(
        self: *Self,
        service_id: i32,
        node_id: u8,
        slot_id: i32,
        generation: u16,
        capacity: u32,
    ) void {
        const inst = self.getInstancePtr(service_id, node_id) orelse return;
        inst.fc_slot_id = slot_id;
        inst.fc_slot_generation = generation;
        if (capacity > 0) inst.messages_buffer_capacity = capacity;
    }

    pub fn updateFlowControlState(
        self: *Self,
        service_id: i32,
        node_id: u8,
        remaining: u32,
        capacity: u32,
        pressure_state: PressureState,
        broadcast_ns: i64,
    ) void {
        const inst = self.getInstancePtr(service_id, node_id) orelse return;
        if (capacity > 0) inst.messages_buffer_capacity = capacity;
        inst.last_fc_remaining = remaining;
        inst.fc_pressure_state = pressure_state;
        inst.last_fc_broadcast_ns = broadcast_ns;
    }

    pub fn addSubscription(self: *Self, service_name: []const u8, subscriber_id: i32) !void {
        const needs_new_key = !self.subscriptions.contains(service_name);
        const owned_key: ?[]u8 = if (needs_new_key)
            try self.allocator.dupe(u8, service_name)
        else
            null;

        const gop = self.subscriptions.getOrPut(if (owned_key) |k| k else service_name) catch |err| {
            if (owned_key) |k| self.allocator.free(k);
            return err;
        };
        if (!gop.found_existing) {
            gop.value_ptr.* = std.AutoHashMap(i32, void).init(self.allocator);
        }
        try gop.value_ptr.put(subscriber_id, {});
    }

    pub fn removeSubscription(self: *Self, service_name: []const u8, subscriber_id: i32) void {
        const set = self.subscriptions.getPtr(service_name) orelse return;
        _ = set.remove(subscriber_id);
        if (set.count() == 0) {
            set.deinit();
            if (self.subscriptions.fetchRemove(service_name)) |entry| {
                self.allocator.free(@constCast(entry.key));
            }
        }
    }

    pub fn setLeader(self: *Self, service_name: []const u8, leader_service_id: i32) void {
        const gop = self.service_leaders.getOrPut(service_name) catch return;
        if (!gop.found_existing) {
            gop.key_ptr.* = self.allocator.dupe(u8, service_name) catch return;
        }
        gop.value_ptr.* = leader_service_id;
    }

    pub fn removeLeader(self: *Self, service_name: []const u8) void {
        if (self.service_leaders.fetchRemove(service_name)) |entry| {
            self.allocator.free(@constCast(entry.key));
        }
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
    // Free the owned name returned by remove().
    testing.allocator.free(@constCast(removed.?.service_name));

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
