//! ServiceInstance — represents one registered instance of a named service.
//!
//! Each instance may be on the local host (with an IpcProducer for direct
//! shared-memory writes) or a remote host (messages routed via the broker).

const IpcProducer = @import("ipc/ipc_producer.zig").IpcProducer;

pub const ServiceInstance = struct {
    /// Unique ID assigned by the broker at registration time.
    service_id: i32,

    /// The service's logical name (e.g. "pricing", "risk-engine").
    service_name: []const u8,

    /// The host/node this instance runs on.
    node_id: i16,

    /// Whether this instance is the elected leader for its service name.
    is_leader: bool = false,

    /// Non-null only for instances on the local host.
    /// Points to an IpcProducer that writes directly into this instance's
    /// messages ring buffer.
    ipc_producer: ?*IpcProducer = null,

    /// Returns true if this instance is on the local host.
    pub fn isLocal(self: *const ServiceInstance, local_node_id: i16) bool {
        return self.node_id == local_node_id;
    }
};
