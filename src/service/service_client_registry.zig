//! ServiceClientRegistry — manages all ServiceClient instances for a service process.
//!
//! Provides lookup by service name and handles instance updates from the broker.

const std = @import("std");
const ServiceClient = @import("service_client.zig").ServiceClient;
const ServiceInstance = @import("service_instance.zig").ServiceInstance;
const IpcProducer = @import("ipc/ipc_producer.zig").IpcProducer;
const ringloom_common = @import("ringloom_common");
const memory = ringloom_common.memory;
const control_encoding = ringloom_common.message.control_encoding;

const BuffersProvider = memory.BuffersProvider;
const BrokerMetadataFile = memory.BrokerMetadataFile;
const ServiceCounters = ringloom_common.monitoring.ServiceCounters;

pub const ServiceClientRegistry = struct {
    clients: std.StringHashMap(*ServiceClient),
    allocator: std.mem.Allocator,
    broker_meta: ?*BrokerMetadataFile,
    local_node_id: i16,
    local_service_id: i32,
    storage_path: []const u8,
    group: []const u8,
    service_counters: ?*ServiceCounters,

    const Self = @This();

    pub fn init(
        allocator: std.mem.Allocator,
        broker_meta: ?*BrokerMetadataFile,
        local_node_id: i16,
        local_service_id: i32,
        storage_path: []const u8,
        group: []const u8,
        service_counters: ?*ServiceCounters,
    ) Self {
        return .{
            .clients = std.StringHashMap(*ServiceClient).init(allocator),
            .allocator = allocator,
            .broker_meta = broker_meta,
            .local_node_id = local_node_id,
            .local_service_id = local_service_id,
            .storage_path = storage_path,
            .group = group,
            .service_counters = service_counters,
        };
    }

    /// Get or create a ServiceClient for the named service.
    pub fn getOrCreate(self: *Self, service_name: []const u8) !*ServiceClient {
        if (self.clients.get(service_name)) |existing| {
            return existing;
        }

        const owned_service_name = try self.allocator.dupe(u8, service_name);
        errdefer self.allocator.free(owned_service_name);

        const client = try self.allocator.create(ServiceClient);
        errdefer self.allocator.destroy(client);

        client.* = ServiceClient.init(
            self.allocator,
            owned_service_name,
            self.broker_meta,
            self.local_node_id,
            self.local_service_id,
            self.service_counters,
        );
        client.owns_service_name = true;

        try self.clients.put(owned_service_name, client);
        return client;
    }

    /// Called when the broker sends a ServiceInstances update.
    pub fn addOrUpdateInstance(self: *Self, instance_data: struct {
        service_id: i32,
        service_name: []const u8,
        node_id: i16,
        is_leader: bool,
    }) void {
        const client = self.clients.get(instance_data.service_name) orelse return;
        self.addOrUpdateClientInstance(client, .{
            .service_id = instance_data.service_id,
            .node_id = instance_data.node_id,
            .is_leader = if (instance_data.is_leader) 1 else 0,
        });
    }

    /// Called when the broker sends the complete instance snapshot for a service.
    pub fn replaceInstances(
        self: *Self,
        service_name: []const u8,
        entries: []const control_encoding.ServiceInstanceEntry,
    ) void {
        const client = self.clients.get(service_name) orelse return;

        client.lockInstances();
        defer client.unlockInstances();

        var i: usize = 0;
        while (i < client.instances.items.len) {
            const current = client.instances.items[i];
            if (!snapshotContains(entries, current.node_id, current.service_id)) {
                const removed = client.instances.swapRemove(i);
                if (removed.ipc_producer) |producer| {
                    self.allocator.destroy(producer);
                }
                client.emitLifecycle(.{
                    .event_type = .unavailable,
                    .service_name = removed.service_name,
                    .service_id = removed.service_id,
                    .node_id = removed.node_id,
                    .is_leader = removed.is_leader,
                });
                continue;
            }
            i += 1;
        }

        for (entries) |entry| {
            self.addOrUpdateClientInstanceLocked(client, entry);
        }
    }

    fn addOrUpdateClientInstance(
        self: *Self,
        client: *ServiceClient,
        entry: control_encoding.ServiceInstanceEntry,
    ) void {
        client.lockInstances();
        defer client.unlockInstances();
        self.addOrUpdateClientInstanceLocked(client, entry);
    }

    fn addOrUpdateClientInstanceLocked(
        self: *Self,
        client: *ServiceClient,
        entry: control_encoding.ServiceInstanceEntry,
    ) void {
        const is_leader = entry.is_leader != 0;

        // Check if this instance already exists — update in place if so.
        if (client.findInstance(entry.node_id, entry.service_id)) |existing| {
            const was_leader = existing.is_leader;
            existing.node_id = entry.node_id;
            existing.is_leader = is_leader;
            existing.fc_slot_id = entry.fc_slot_id;
            existing.fc_slot_generation = entry.fc_slot_generation;
            existing.messages_buffer_capacity = entry.messages_buffer_capacity;
            if (was_leader != existing.is_leader) {
                client.emitInstanceAvailable(existing.*);
            }
            return;
        }

        // For local instances, open the target service's metadata file
        // and create an IpcProducer for direct shared-memory writes.
        const ipc_producer = self.createIpcProducer(entry.service_id, entry.node_id, client.service_name);

        const instance = ServiceInstance{
            .service_id = entry.service_id,
            .service_name = client.service_name,
            .node_id = entry.node_id,
            .is_leader = is_leader,
            .ipc_producer = ipc_producer,
            .fc_slot_id = entry.fc_slot_id,
            .fc_slot_generation = entry.fc_slot_generation,
            .messages_buffer_capacity = entry.messages_buffer_capacity,
        };
        client.instances.append(self.allocator, instance) catch {
            if (ipc_producer) |producer| self.allocator.destroy(producer);
            return;
        };
        client.emitInstanceAvailable(instance);
    }

    fn createIpcProducer(
        self: *Self,
        service_id: i32,
        node_id: i16,
        service_name: []const u8,
    ) ?*IpcProducer {
        if (node_id != self.local_node_id) return null;

        if (BuffersProvider.getInstance(
            self.allocator,
            service_id,
            node_id,
            service_name,
            self.storage_path,
            self.group,
        )) |provider| {
            const producer = self.allocator.create(IpcProducer) catch return null;
            producer.* = IpcProducer.init(
                @alignCast(provider.getMessagesBuffer()),
            ) catch {
                self.allocator.destroy(producer);
                return null;
            };
            return producer;
        } else |_| {}

        return null;
    }

    fn snapshotContains(
        entries: []const control_encoding.ServiceInstanceEntry,
        node_id: i16,
        service_id: i32,
    ) bool {
        for (entries) |entry| {
            if (entry.service_id == service_id and entry.node_id == node_id) return true;
        }
        return false;
    }

    /// Called when the broker notifies that a service instance was removed.
    pub fn removeInstance(self: *Self, service_name: []const u8, service_id: i32) void {
        if (self.clients.get(service_name)) |client| {
            client.removeInstance(self.local_node_id, service_id);
        }
    }

    /// Called when the broker notifies that leadership changed.
    pub fn updateLeader(self: *Self, service_name: []const u8, leader_service_id: i32) void {
        if (self.clients.get(service_name)) |client| {
            client.updateLeader(leader_service_id);
        }
    }

    pub fn deinit(self: *Self) void {
        var iter = self.clients.valueIterator();
        while (iter.next()) |client_ptr| {
            client_ptr.*.deinit();
            self.allocator.destroy(client_ptr.*);
        }
        self.clients.deinit();
    }
};
