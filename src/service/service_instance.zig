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

    // ── Flow Control Fields ──────────────────────────────────────

    /// Index into the flow control counters region.
    /// -1 = not assigned (local instances use direct ring buffer access).
    fc_slot_id: i32 = -1,

    /// Generation counter for the assigned FC slot (detect stale references).
    fc_slot_generation: u16 = 0,

    /// Total capacity of this instance's messages ring buffer (bytes).
    /// Used to interpret remaining-bytes as a ratio.
    messages_buffer_capacity: u32 = 0,

    /// Returns true if this instance is on the local host.
    pub fn isLocal(self: *const ServiceInstance, local_node_id: i16) bool {
        return self.node_id == local_node_id;
    }

    /// Returns true if this instance has an assigned FC slot.
    pub fn hasFcSlot(self: *const ServiceInstance) bool {
        return self.fc_slot_id >= 0;
    }
};
