//! ServiceClientRegistry — manages all ServiceClient instances for a service process.
//!
//! Provides lookup by service name and handles instance updates from the broker.

const std = @import("std");
const ServiceClient = @import("service_client.zig").ServiceClient;
const ServiceInstance = @import("service_instance.zig").ServiceInstance;
const IpcProducer = @import("ipc/ipc_producer.zig").IpcProducer;
const ringloom_common = @import("ringloom_common");
const memory = ringloom_common.memory;

const BuffersProvider = memory.BuffersProvider;
const BrokerMetadataFile = memory.BrokerMetadataFile;

pub const ServiceClientRegistry = struct {
    clients: std.StringHashMap(*ServiceClient),
    allocator: std.mem.Allocator,
    broker_meta: ?*BrokerMetadataFile,
    local_node_id: i16,
    local_service_id: i32,
    storage_path: []const u8,
    group: []const u8,

    const Self = @This();

    pub fn init(
        allocator: std.mem.Allocator,
        broker_meta: ?*BrokerMetadataFile,
        local_node_id: i16,
        local_service_id: i32,
        storage_path: []const u8,
        group: []const u8,
    ) Self {
        return .{
            .clients = std.StringHashMap(*ServiceClient).init(allocator),
            .allocator = allocator,
            .broker_meta = broker_meta,
            .local_node_id = local_node_id,
            .local_service_id = local_service_id,
            .storage_path = storage_path,
            .group = group,
        };
    }

    /// Get or create a ServiceClient for the named service.
    pub fn getOrCreate(self: *Self, service_name: []const u8) !*ServiceClient {
        if (self.clients.get(service_name)) |existing| {
            return existing;
        }

        const client = try self.allocator.create(ServiceClient);
        client.* = ServiceClient.init(
            self.allocator,
            service_name,
            self.broker_meta,
            self.local_node_id,
            self.local_service_id,
        );

        try self.clients.put(service_name, client);
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

        // Check if this instance already exists — update in place if so.
        if (client.findInstance(instance_data.service_id)) |existing| {
            existing.is_leader = instance_data.is_leader;
            return;
        }

        // For local instances, open the target service's metadata file
        // and create an IpcProducer for direct shared-memory writes.
        var ipc_producer: ?*IpcProducer = null;
        if (instance_data.node_id == self.local_node_id) {
            if (BuffersProvider.getInstance(
                self.allocator,
                instance_data.service_id,
                instance_data.service_name,
                self.storage_path,
                self.group,
            )) |provider| {
                const producer = self.allocator.create(IpcProducer) catch return;
                producer.* = IpcProducer.init(
                    @alignCast(provider.getMessagesBuffer()),
                ) catch {
                    self.allocator.destroy(producer);
                    return;
                };
                ipc_producer = producer;
            } else |_| {}
        }

        client.addInstance(.{
            .service_id = instance_data.service_id,
            .service_name = instance_data.service_name,
            .node_id = instance_data.node_id,
            .is_leader = instance_data.is_leader,
            .ipc_producer = ipc_producer,
        }) catch {};
    }

    /// Called when the broker notifies that a service instance was removed.
    pub fn removeInstance(self: *Self, service_name: []const u8, service_id: i32) void {
        if (self.clients.get(service_name)) |client| {
            client.removeInstance(service_id);
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
