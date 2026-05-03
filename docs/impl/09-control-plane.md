# 09 — Control Plane

> **Depends on:** [02 — Memory Layout & Shared Memory](02-memory-layout-and-shared-memory.md) (metadata files, `BuffersProvider`), [03 — Concurrent Data Structures](03-concurrent-data-structures.md) (MPSC ring buffer, atomic counters), [08 — Service IPC](08-service-ipc.md) (service ↔ broker message path, control ring buffers)
>
> **Depended on by:** [10 — Threading Model](10-threading-model.md) (control loop is Thread 1), [11 — Cluster Management](11-cluster-management.md) (leader election, state sync build on the service registry and subscriber notification mechanisms defined here)

The broker control plane is the nerve center of the RingLoom broker. It handles service
registration, deregistration, discovery (subscriptions), heartbeat-based health
checking, service leader election, and inter-event-loop command dispatch. All of this
runs on a single dedicated thread (Thread 1 in the threading model) in a tight
duty-cycle event loop.

All code targets **Zig 0.14.x** stable.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Control Message Protocol](#2-control-message-protocol)
3. [Control Loop Implementation](#3-control-loop-implementation)
4. [Service Registration](#4-service-registration)
5. [Service Deregistration](#5-service-deregistration)
6. [Service Discovery](#6-service-discovery)
7. [Service Registry](#7-service-registry)
8. [Heartbeat Checking](#8-heartbeat-checking)
9. [Service Leader Election](#9-service-leader-election)
10. [Inter-Event-Loop Commands](#10-inter-event-loop-commands)
11. [Counters & Observability](#11-counters--observability)
12. [Testing](#12-testing)
13. [File Structure](#13-file-structure)

---

## 1. Overview

The broker control loop runs on a dedicated thread (Thread 1) and performs the
following duties on every iteration:

1. **Drains the inter-event-loop command queue** — processes commands from the sender
   and receiver threads (peer connected/disconnected, admin messages forwarded from
   the receiver, send errors).
2. **Polls the broker's control ring buffer** — reads messages written by local services
   (registration requests, subscription requests, unregister requests).
3. **Checks service heartbeats** (rate-limited to every 3 seconds) — reads heartbeat
   timestamps from each locally registered service's metadata file and removes any
   service that has been silent for more than 10 seconds.
4. **Delegates to the cluster manager** (rate-limited) — drives leader election, state
   synchronization, and broker-to-broker heartbeat logic.
5. **Updates monitoring counters** — writes current service count, subscription count,
   and other control-plane metrics to the counters buffer.

The control loop never allocates on the hot path. All data structures are pre-allocated
at startup. Message encoding and decoding use `packed struct` flyweight overlays
directly on ring buffer memory — no intermediate copies, no serialization frameworks.

```
┌─────────────────────────────────────────────────────────────────┐
│                  Control Loop Duty Cycle                        │
│                                                                 │
│  ┌──────────────┐   ┌──────────────────┐   ┌────────────────┐  │
│  │ Command Queue │──►│ Control Ring Buf  │──►│ Periodic Tasks │  │
│  │  (1 per cycle)│   │ (up to 10 msgs)  │   │ (rate-limited) │  │
│  └──────────────┘   └──────────────────┘   └────────────────┘  │
│                                                     │           │
│                                              ┌──────┴───────┐   │
│                                              │  Heartbeat   │   │
│                                              │  (every 3s)  │   │
│                                              ├──────────────┤   │
│                                              │  Cluster Mgr │   │
│                                              │  (every 1s)  │   │
│                                              └──────────────┘   │
│                                                                 │
│  return work_count → idle strategy                              │
└─────────────────────────────────────────────────────────────────┘
```

---

## 2. Control Message Protocol

All control messages flow through MPSC ring buffers. Services write to the **broker's**
control ring buffer. The broker writes responses to each **service's** control ring
buffer.

### 2.1 Template IDs

Every control message starts with a 4-byte header containing a `template_id` that
identifies the message type and a `body_length` that gives the size of everything
after the header.

| `template_id` | Message | Direction | Description |
|:-:|---|---|---|
| 1 | `RegisterService` | Service → Broker | Service announces itself after creating its metadata file |
| 2 | `RegistrationResponse` | Broker → Service | Broker confirms registration, provides `nodeId` and leader status |
| 3 | `SubscribeToServiceUpdates` | Service → Broker | Service wants notifications when instances of a named service change |
| 4 | `ServiceInstances` | Broker → Service | Complete list of currently known instances of a service |
| 5 | `UnregisterService` | Service → Broker | Service is shutting down gracefully |
| 6 | `LeaderChanged` | Broker → Service | The leader for a service name has changed |

Template IDs 1, 3, and 5 are **inbound** (written by services, read by the broker).
Template IDs 2, 4, and 6 are **outbound** (written by the broker, read by services).

### 2.2 Common Header

```zig
// src/control/control_messages.zig

/// 4-byte header prefixed to every control message.
/// The ring buffer's own record header (8 bytes) wraps this — the control
/// message header is the first thing inside the record payload.
pub const ControlMessageHeader = packed struct {
    /// Identifies the message type (see template_id table above).
    template_id: u16,

    /// Length of the message body AFTER this header, in bytes.
    /// Total message size = @sizeOf(ControlMessageHeader) + body_length.
    body_length: u16,
};

pub const header_size = @sizeOf(ControlMessageHeader); // 4
```

### 2.3 Message Definitions

Each message is a `packed struct` that can be overlaid directly onto ring buffer
memory. Variable-length fields (service names) are appended after the fixed struct
and referenced via a length field.

```zig
// ── RegisterService (templateId = 1) ─────────────────────────────────
// Direction: Service → Broker
// Sent after the service creates its metadata file.

pub const RegisterServiceMsg = packed struct {
    header: ControlMessageHeader,       // template_id = 1
    service_id: i32,                    // Assigned by the service from broker's nextServiceId
    node_id: i16,                       // 0 on send (broker fills in the real value)
    leader_election_enabled: u8,        // 1 = enabled, 0 = disabled
    service_name_length: u8,            // Length of the service name that follows
    // Followed by `service_name_length` bytes of UTF-8 service name.
};

// ── RegistrationResponse (templateId = 2) ────────────────────────────
// Direction: Broker → Service
// Written to the service's control ring buffer.

pub const RegistrationResponseMsg = packed struct {
    header: ControlMessageHeader,       // template_id = 2
    service_id: i32,                    // Confirmed service ID
    node_id: i16,                       // Broker's node ID (so the service knows its own nodeId)
    is_leader: u8,                      // 1 = this instance is the leader, 0 = not
    _padding: u8 = 0,
};

// ── SubscribeToServiceUpdates (templateId = 3) ───────────────────────
// Direction: Service → Broker
// The service wants to be notified when instances of a named service change.

pub const SubscribeMsg = packed struct {
    header: ControlMessageHeader,       // template_id = 3
    local_service_id: i32,              // The subscribing service's own ID
    service_name_length: u16,           // Length of the target service name
    _padding: u16 = 0,
    // Followed by `service_name_length` bytes of UTF-8 target service name.
};

// ── ServiceInstances (templateId = 4) ────────────────────────────────
// Direction: Broker → Service
// Contains the COMPLETE current set of instances for a service name.
// Not a delta — the receiver replaces its entire instance list.

pub const ServiceInstancesMsg = packed struct {
    header: ControlMessageHeader,       // template_id = 4
    subscriber_service_id: i32,         // The subscribing service's ID (for routing)
    instance_count: u16,                // Number of ServiceInstanceEntry structs that follow
    service_name_length: u16,           // Length of the service name
    // Followed by:
    //   1. `service_name_length` bytes of UTF-8 service name
    //   2. `instance_count` × @sizeOf(ServiceInstanceEntry) bytes of instance data
};

/// One entry inside a ServiceInstances message.
pub const ServiceInstanceEntry = packed struct {
    service_id: i32,
    node_id: i16,
    is_leader: u8,
    _padding: u8 = 0,
};

// ── UnregisterService (templateId = 5) ───────────────────────────────
// Direction: Service → Broker
// Sent when a service shuts down gracefully.

pub const UnregisterServiceMsg = packed struct {
    header: ControlMessageHeader,       // template_id = 5
    service_id: i32,                    // The service being unregistered
    node_id: i16,                       // 0 on send (broker uses local_node_id)
    _padding: i16 = 0,
};

// ── LeaderChanged (templateId = 6) ───────────────────────────────────
// Direction: Broker → Service
// Sent to all local subscribers of a service name when the leader changes.

pub const LeaderChangedMsg = packed struct {
    header: ControlMessageHeader,       // template_id = 6
    leader_service_id: i32,             // The new leader's service ID
    leader_node_id: i16,                // The new leader's node ID
    service_name_length: u16,           // Length of the service name
    // Followed by `service_name_length` bytes of UTF-8 service name.
};
```

### 2.4 Message Encoding Helpers

Encoding builds a message into a caller-provided buffer. No allocation.

```zig
// src/control/control_messages.zig (continued)

const std = @import("std");

/// Encode a RegisterService message into `buf`. Returns the total encoded length.
pub fn encodeRegisterService(
    buf: []u8,
    service_id: i32,
    leader_election_enabled: bool,
    service_name: []const u8,
) u16 {
    const fixed_len = @sizeOf(RegisterServiceMsg);
    const total_len: u16 = @intCast(fixed_len + service_name.len);
    std.debug.assert(total_len <= buf.len);

    const msg: *RegisterServiceMsg = @ptrCast(@alignCast(buf.ptr));
    msg.* = .{
        .header = .{
            .template_id = 1,
            .body_length = total_len - header_size,
        },
        .service_id = service_id,
        .node_id = 0, // broker fills this in
        .leader_election_enabled = if (leader_election_enabled) 1 else 0,
        .service_name_length = @intCast(service_name.len),
    };

    @memcpy(buf[fixed_len..][0..service_name.len], service_name);
    return total_len;
}

/// Encode a RegistrationResponse message into `buf`. Returns the total encoded length.
pub fn encodeRegistrationResponse(
    buf: []u8,
    service_id: i32,
    node_id: i16,
    is_leader: bool,
) u16 {
    const total_len: u16 = @sizeOf(RegistrationResponseMsg);
    std.debug.assert(total_len <= buf.len);

    const msg: *RegistrationResponseMsg = @ptrCast(@alignCast(buf.ptr));
    msg.* = .{
        .header = .{
            .template_id = 2,
            .body_length = total_len - header_size,
        },
        .service_id = service_id,
        .node_id = node_id,
        .is_leader = if (is_leader) 1 else 0,
    };
    return total_len;
}

/// Encode a SubscribeToServiceUpdates message into `buf`. Returns the total encoded length.
pub fn encodeSubscribe(
    buf: []u8,
    local_service_id: i32,
    service_name: []const u8,
) u16 {
    const fixed_len = @sizeOf(SubscribeMsg);
    const total_len: u16 = @intCast(fixed_len + service_name.len);
    std.debug.assert(total_len <= buf.len);

    const msg: *SubscribeMsg = @ptrCast(@alignCast(buf.ptr));
    msg.* = .{
        .header = .{
            .template_id = 3,
            .body_length = total_len - header_size,
        },
        .local_service_id = local_service_id,
        .service_name_length = @intCast(service_name.len),
    };

    @memcpy(buf[fixed_len..][0..service_name.len], service_name);
    return total_len;
}

/// Encode a ServiceInstances message into `buf`. Returns the total encoded length.
/// `instances` is the full set of currently known instances for `service_name`.
pub fn encodeServiceInstances(
    buf: []u8,
    subscriber_service_id: i32,
    service_name: []const u8,
    instances: []const ServiceInstanceEntry,
) u16 {
    const fixed_len = @sizeOf(ServiceInstancesMsg);
    const name_end = fixed_len + service_name.len;
    const entries_size = instances.len * @sizeOf(ServiceInstanceEntry);
    const total_len: u16 = @intCast(name_end + entries_size);
    std.debug.assert(total_len <= buf.len);

    const msg: *ServiceInstancesMsg = @ptrCast(@alignCast(buf.ptr));
    msg.* = .{
        .header = .{
            .template_id = 4,
            .body_length = total_len - header_size,
        },
        .subscriber_service_id = subscriber_service_id,
        .instance_count = @intCast(instances.len),
        .service_name_length = @intCast(service_name.len),
    };

    @memcpy(buf[fixed_len..][0..service_name.len], service_name);

    if (instances.len > 0) {
        const entries_dst = buf[name_end..][0..entries_size];
        const entries_src: [*]const u8 = @ptrCast(instances.ptr);
        @memcpy(entries_dst, entries_src[0..entries_size]);
    }

    return total_len;
}

/// Encode an UnregisterService message into `buf`. Returns the total encoded length.
pub fn encodeUnregisterService(buf: []u8, service_id: i32) u16 {
    const total_len: u16 = @sizeOf(UnregisterServiceMsg);
    std.debug.assert(total_len <= buf.len);

    const msg: *UnregisterServiceMsg = @ptrCast(@alignCast(buf.ptr));
    msg.* = .{
        .header = .{
            .template_id = 5,
            .body_length = total_len - header_size,
        },
        .service_id = service_id,
        .node_id = 0,
    };
    return total_len;
}

/// Encode a LeaderChanged message into `buf`. Returns the total encoded length.
pub fn encodeLeaderChanged(
    buf: []u8,
    leader_service_id: i32,
    leader_node_id: i16,
    service_name: []const u8,
) u16 {
    const fixed_len = @sizeOf(LeaderChangedMsg);
    const total_len: u16 = @intCast(fixed_len + service_name.len);
    std.debug.assert(total_len <= buf.len);

    const msg: *LeaderChangedMsg = @ptrCast(@alignCast(buf.ptr));
    msg.* = .{
        .header = .{
            .template_id = 6,
            .body_length = total_len - header_size,
        },
        .leader_service_id = leader_service_id,
        .leader_node_id = leader_node_id,
        .service_name_length = @intCast(service_name.len),
    };

    @memcpy(buf[fixed_len..][0..service_name.len], service_name);
    return total_len;
}
```

### 2.5 Message Decoding

Decoding is zero-copy: cast the ring buffer payload pointer to the appropriate `packed
struct` pointer and read fields directly. For variable-length tails (service names),
slice into the payload using the length field:

```zig
/// Extract the service name from a RegisterService payload.
pub fn decodeRegisterServiceName(payload: []const u8) []const u8 {
    const msg: *const RegisterServiceMsg = @ptrCast(@alignCast(payload.ptr));
    const offset = @sizeOf(RegisterServiceMsg);
    return payload[offset..][0..msg.service_name_length];
}

/// Extract the service name from a Subscribe payload.
pub fn decodeSubscribeServiceName(payload: []const u8) []const u8 {
    const msg: *const SubscribeMsg = @ptrCast(@alignCast(payload.ptr));
    const offset = @sizeOf(SubscribeMsg);
    return payload[offset..][0..msg.service_name_length];
}

/// Extract the service name from a LeaderChanged payload.
pub fn decodeLeaderChangedServiceName(payload: []const u8) []const u8 {
    const msg: *const LeaderChangedMsg = @ptrCast(@alignCast(payload.ptr));
    const offset = @sizeOf(LeaderChangedMsg);
    return payload[offset..][0..msg.service_name_length];
}

/// Extract the service name and instance entries from a ServiceInstances payload.
pub fn decodeServiceInstances(payload: []const u8) struct {
    service_name: []const u8,
    entries: []const ServiceInstanceEntry,
} {
    const msg: *const ServiceInstancesMsg = @ptrCast(@alignCast(payload.ptr));
    const name_offset = @sizeOf(ServiceInstancesMsg);
    const name = payload[name_offset..][0..msg.service_name_length];
    const entries_offset = name_offset + msg.service_name_length;
    const entries_size = msg.instance_count * @sizeOf(ServiceInstanceEntry);
    const entries_ptr: [*]const ServiceInstanceEntry = @ptrCast(
        @alignCast(payload[entries_offset..].ptr),
    );
    return .{
        .service_name = name,
        .entries = entries_ptr[0..msg.instance_count],
    };
}
```

### 2.6 Size Verification

All message structs must have deterministic sizes at compile time. Add `comptime`
assertions:

```zig
comptime {
    std.debug.assert(@sizeOf(ControlMessageHeader) == 4);
    std.debug.assert(@sizeOf(RegisterServiceMsg) == 12);
    std.debug.assert(@sizeOf(RegistrationResponseMsg) == 12);
    std.debug.assert(@sizeOf(SubscribeMsg) == 12);
    std.debug.assert(@sizeOf(ServiceInstancesMsg) == 12);
    std.debug.assert(@sizeOf(ServiceInstanceEntry) == 8);
    std.debug.assert(@sizeOf(UnregisterServiceMsg) == 12);
    std.debug.assert(@sizeOf(LeaderChangedMsg) == 12);
}
```

---

## 3. Control Loop Implementation

### 3.1 ControlLoop Struct

The `ControlLoop` is the main state container for Thread 1. It owns the service
registry, has references to the broker's control ring buffer and command queue, and
holds timing state for rate-limited periodic tasks.

```zig
// src/control/control_loop.zig

const std = @import("std");
const platform = @import("../platform.zig");
const constants = platform.constants;
const RingBuffer = @import("../concurrent/ring_buffer.zig").RingBuffer;
const ServiceRegistry = @import("service_registry.zig").ServiceRegistry;
const ServiceHeartbeatChecker = @import("service_heartbeat_checker.zig").ServiceHeartbeatChecker;
const ServiceLeaderElection = @import("service_leader_election.zig").ServiceLeaderElection;
const BuffersProvider = @import("../memory/buffers_provider.zig").BuffersProvider;
const CommandQueue = @import("../concurrent/command_queue.zig").CommandQueue;
const ClusterManager = @import("../cluster/cluster_manager.zig").ClusterManager;
const CountersManager = @import("../counters/counters_manager.zig").CountersManager;
const msg = @import("control_messages.zig");
const log = std.log.scoped(.control_loop);

pub const ControlLoop = struct {
    // ── Core dependencies ────────────────────────────────────────
    /// The broker's control ring buffer. Services write here; we read.
    control_rb: *RingBuffer,

    /// Inter-event-loop command queue. Sender/receiver threads post
    /// commands here; we drain them.
    cmd_queue: *CommandQueue,

    /// Cluster manager — drives leader election, state sync, broker heartbeats.
    cluster_manager: *ClusterManager,

    /// Counters buffer for monitoring metrics.
    counters: *CountersManager,

    // ── Owned state ──────────────────────────────────────────────
    /// All known service instances (local and remote).
    service_registry: ServiceRegistry,

    /// Heartbeat checker — stateless, operates on the registry.
    heartbeat_checker: ServiceHeartbeatChecker,

    /// Leader election evaluator.
    leader_election: ServiceLeaderElection,

    // ── Configuration ────────────────────────────────────────────
    /// This broker's node ID.
    local_node_id: u8,

    /// Storage path for metadata files (e.g. "/dev/shm").
    storage_path: []const u8,

    /// Group name (e.g. "ringloom-default").
    group: []const u8,

    // ── Timing ───────────────────────────────────────────────────
    /// Next time (monotonic ns) to run the periodic-task block.
    next_timeout_check_ns: i64,

    /// Next time (monotonic ns) to check service heartbeats.
    next_heartbeat_check_ns: i64,

    // ── Scratch buffer for message encoding ──────────────────────
    /// Pre-allocated buffer for encoding outbound control messages.
    /// 4096 bytes is more than enough for any single control message
    /// (the largest is ServiceInstances, which caps at ~256 instances
    /// × 8 bytes + fixed header + service name ≈ 2100 bytes).
    encode_buf: [4096]u8 = undefined,

    /// Allocator used for BuffersProvider instances (page allocator).
    allocator: std.mem.Allocator,

    const Self = @This();

    // ── Timing constants (imported from platform/constants.zig) ──
    const COMMAND_DRAIN_LIMIT: u32 = constants.command_drain_limit;
    const CONTROL_READ_LIMIT: u32 = constants.control_read_limit;
    const TIMEOUT_CHECK_INTERVAL_NS: i64 = constants.control_loop_timeout_check_interval_ns;
    const HEARTBEAT_CHECK_INTERVAL_NS: i64 = constants.service_heartbeat_check_interval_ms * std.time.ns_per_ms;
    const CONTROL_MSG_TYPE: i32 = 1; // ring buffer msg_type_id for control messages

    // ─────────────────────────────────────────────────────────────
    // Construction
    // ─────────────────────────────────────────────────────────────

    pub const InitOptions = struct {
        control_rb: *RingBuffer,
        cmd_queue: *CommandQueue,
        cluster_manager: *ClusterManager,
        counters: *CountersManager,
        local_node_id: u8,
        storage_path: []const u8,
        group: []const u8,
        allocator: std.mem.Allocator,
    };

    pub fn init(opts: InitOptions) Self {
        return .{
            .control_rb = opts.control_rb,
            .cmd_queue = opts.cmd_queue,
            .cluster_manager = opts.cluster_manager,
            .counters = opts.counters,
            .service_registry = ServiceRegistry.init(opts.allocator),
            .heartbeat_checker = ServiceHeartbeatChecker.init(),
            .leader_election = ServiceLeaderElection.init(),
            .local_node_id = opts.local_node_id,
            .storage_path = opts.storage_path,
            .group = opts.group,
            .next_timeout_check_ns = 0,
            .next_heartbeat_check_ns = 0,
            .allocator = opts.allocator,
        };
    }

    // ─────────────────────────────────────────────────────────────
    // EventLoop interface
    // ─────────────────────────────────────────────────────────────

    /// Called by the ThreadRunner on every iteration of the event loop.
    /// Returns the number of work items processed. If zero, the idle
    /// strategy will engage.
    pub fn doWork(self: *Self) u32 {
        var work_count: u32 = 0;
        const now_ns = platform.Clock.monotonicNanos();

        // 1. Drain inter-event-loop commands (max 1 per cycle to limit jitter)
        work_count += self.cmd_queue.drain(self, dispatchCommand, COMMAND_DRAIN_LIMIT);

        // 2. Poll broker's control ring buffer for service messages
        work_count += self.control_rb.read(self, onControlMessage, CONTROL_READ_LIMIT);

        // 3. Periodic tasks — rate-limited to every ~1 second
        if (now_ns > self.next_timeout_check_ns) {

            // 3a. Heartbeat checking — every 3 seconds
            if (now_ns > self.next_heartbeat_check_ns) {
                self.checkServiceHeartbeats(now_ns);
                self.next_heartbeat_check_ns = now_ns + HEARTBEAT_CHECK_INTERVAL_NS;
            }

            // 3b. Cluster protocol tasks (leader election, state sync, broker heartbeats)
            self.cluster_manager.doWork(now_ns);

            self.next_timeout_check_ns = now_ns + TIMEOUT_CHECK_INTERVAL_NS;
        }

        // 4. Update monitoring counters
        self.updateCounters();

        return work_count;
    }

    /// Called once when the event loop is shutting down.
    pub fn onClose(self: *Self) void {
        log.info("control loop shutting down, closing {} local service mappings", .{
            self.service_registry.localServiceCount(),
        });
        self.service_registry.deinit();
    }

    // ─────────────────────────────────────────────────────────────
    // Control message dispatch
    // ─────────────────────────────────────────────────────────────

    /// Ring buffer read callback. Called for each record in the broker's
    /// control ring buffer.
    fn onControlMessage(self: *Self, _msg_type_id: i32, payload: []const u8) void {
        _ = _msg_type_id;

        if (payload.len < msg.header_size) {
            log.warn("control message too short: {} bytes", .{payload.len});
            return;
        }

        const header: *const msg.ControlMessageHeader = @ptrCast(@alignCast(payload.ptr));

        switch (header.template_id) {
            1 => self.handleRegisterService(payload),
            3 => self.handleSubscribeToServiceUpdates(payload),
            5 => self.handleUnregisterService(payload),
            else => {
                log.warn("unknown control template_id: {}", .{header.template_id});
            },
        }
    }

    // ─────────────────────────────────────────────────────────────
    // Handlers (see Sections 4, 5, 6 below)
    // ─────────────────────────────────────────────────────────────

    fn handleRegisterService(self: *Self, payload: []const u8) void {
        // Full implementation in Section 4.
        _ = self;
        _ = payload;
    }

    fn handleSubscribeToServiceUpdates(self: *Self, payload: []const u8) void {
        // Full implementation in Section 6.
        _ = self;
        _ = payload;
    }

    fn handleUnregisterService(self: *Self, payload: []const u8) void {
        // Full implementation in Section 5.
        _ = self;
        _ = payload;
    }

    fn dispatchCommand(self: *Self, cmd: *CommandQueue.Command) void {
        // Full implementation in Section 10.
        _ = self;
        _ = cmd;
    }

    fn checkServiceHeartbeats(self: *Self, now_ns: i64) void {
        // Full implementation in Section 8.
        _ = self;
        _ = now_ns;
    }

    fn updateCounters(self: *Self) void {
        // Full implementation in Section 11.
        _ = self;
    }
};
```

### 3.2 Integration with ThreadRunner

The control loop implements the `EventLoop` interface from the platform layer
(see [01 — Platform Abstraction](01-platform-abstraction.md), Section 5). The broker's
startup code creates a `ThreadRunner` that drives it:

```zig
// In broker startup (e.g. broker_application.zig)

const platform = @import("platform.zig");

var control_loop = ControlLoop.init(.{
    .control_rb = &broker_metadata.getControlBuffer(),
    .cmd_queue = &control_cmd_queue,
    .cluster_manager = &cluster_manager,
    .counters = &counters_manager,
    .local_node_id = config.node_id,
    .storage_path = config.storage_path,
    .group = config.group,
    .allocator = std.heap.page_allocator,
});

const event_loop = platform.EventLoop{
    .context = &control_loop,
    .doWorkFn = @ptrCast(&ControlLoop.doWork),
    .onCloseFn = @ptrCast(&ControlLoop.onClose),
};

var runner = platform.ThreadRunner.init(
    "ringloom-control-loop",
    event_loop,
    platform.IdleStrategy{ .backoff = .{} },
);
runner.start();
```

### 3.3 Design Rationale

**Why drain commands first?** Commands from other threads may add or remove peers,
which affects how the control loop processes subsequent control messages (e.g., a
`ServiceAdded` command from the receiver thread might need to notify subscribers
before the next control message is processed).

**Why limit to 1 command per cycle?** Processing too many commands in one cycle
could starve control message processing and increase worst-case latency for
registration responses.

**Why rate-limit periodic tasks?** Heartbeat checking reads memory-mapped heartbeat
timestamps from every local service. At 100+ services, doing this every cycle
(potentially millions of times per second under busy-spin) would pollute the L1 cache.
Every 3 seconds is more than sufficient — the heartbeat timeout is 10 seconds, so
checking at 3-second intervals gives at least 3 opportunities to detect a dead service
before the timeout expires.

---

## 4. Service Registration

When a service starts, it:
1. Creates its own metadata file.
2. Writes a `RegisterService` message to the broker's control ring buffer.
3. Waits for a `RegistrationResponse` on its own control ring buffer.

The broker handles step 2 here.

### 4.1 HandleRegisterService

```zig
// src/control/control_loop.zig (continued)

fn handleRegisterService(self: *Self, payload: []const u8) void {
    if (payload.len < @sizeOf(msg.RegisterServiceMsg)) {
        log.warn("RegisterService message too short: {} bytes", .{payload.len});
        return;
    }

    const register_msg: *const msg.RegisterServiceMsg = @ptrCast(@alignCast(payload.ptr));
    const service_name = msg.decodeRegisterServiceName(payload);

    log.info("registering service: name={s}, id={}, leader_election={}", .{
        service_name,
        register_msg.service_id,
        register_msg.leader_election_enabled != 0,
    });

    // 1. Register in ServiceRegistry
    self.service_registry.register(.{
        .service_id = register_msg.service_id,
        .node_id = self.local_node_id,
        .service_name = service_name,
        .leader_election_enabled = register_msg.leader_election_enabled != 0,
        .is_local = true,
    }) catch |err| {
        log.err("failed to register service {s} (id={}): {}", .{
            service_name,
            register_msg.service_id,
            err,
        });
        return;
    };

    // 2. Open the service's metadata file and create a BuffersProvider
    const buffers = BuffersProvider.getInstance(
        self.allocator,
        register_msg.service_id,
        service_name,
        self.storage_path,
        self.group,
    ) catch |err| {
        log.err("failed to open metadata file for service {s} (id={}): {}", .{
            service_name,
            register_msg.service_id,
            err,
        });
        // Undo the registration.
        self.service_registry.remove(register_msg.service_id, self.local_node_id);
        return;
    };

    // 3. Associate the BuffersProvider with the service in the registry
    self.service_registry.setLocalBuffers(register_msg.service_id, buffers);

    // 4. Evaluate service leader (if leader election is enabled for this service)
    var is_leader = false;
    if (register_msg.leader_election_enabled != 0) {
        if (self.cluster_manager.isClusterLeader()) {
            is_leader = self.leader_election.evaluate(
                &self.service_registry,
                service_name,
                self.local_node_id,
                register_msg.service_id,
            );
        }
    }

    // 5. Send RegistrationResponse to the service's control ring buffer
    self.sendRegistrationResponse(buffers, register_msg.service_id, is_leader);

    // 6. Broadcast ServiceAdded to peer brokers (via cluster manager)
    self.cluster_manager.broadcastServiceAdded(
        register_msg.service_id,
        service_name,
        register_msg.leader_election_enabled != 0,
    );

    // 7. Notify all local subscribers watching this service name
    self.notifySubscribers(service_name);
}
```

### 4.2 SendRegistrationResponse

The broker writes a `RegistrationResponse` to the service's control ring buffer.
The service is polling this buffer, waiting for the response.

```zig
fn sendRegistrationResponse(
    self: *Self,
    buffers: *BuffersProvider,
    service_id: i32,
    is_leader: bool,
) void {
    const len = msg.encodeRegistrationResponse(
        &self.encode_buf,
        service_id,
        @intCast(self.local_node_id),
        is_leader,
    );

    var control_rb = RingBuffer.init(buffers.getControlBuffer());
    control_rb.write(CONTROL_MSG_TYPE, self.encode_buf[0..len]) catch |err| {
        // The service's control ring buffer is full. This should not happen
        // during registration because the service hasn't started processing
        // other control messages yet. Log and move on — the service will
        // eventually time out waiting for the response.
        log.err("failed to write RegistrationResponse to service {}: {}", .{
            service_id,
            err,
        });
    };
}
```

### 4.3 Registration Sequence Diagram

```
   Service                          Broker Control Loop
      │                                    │
      │  1. Create metadata file           │
      │  2. Assign serviceId (atomic       │
      │     increment from broker file)    │
      │  3. Map broker's metadata file     │
      │                                    │
      │  RegisterService(templateId=1)     │
      ├───────────────────────────────────►│
      │  [service_id, service_name,        │  4. Register in ServiceRegistry
      │   leader_election_enabled]         │  5. Map service's metadata file (BuffersProvider)
      │                                    │  6. Evaluate leader (if enabled + cluster leader)
      │                                    │  7. Broadcast ServiceAdded to peer brokers
      │                                    │  8. Notify local subscribers
      │                                    │
      │  RegistrationResponse(templateId=2)│
      │◄───────────────────────────────────┤
      │  [service_id, node_id, is_leader]  │
      │                                    │
      │  9. Start heartbeat writes (~1/s)  │
      │  10. Subscribe to dependencies     │
      │                                    │
```

### 4.4 Error Handling

| Failure | Handling |
|---|---|
| Service name exceeds 255 bytes | Rejected by `encodeRegisterService` (compile-time `u8` length field) |
| Metadata file does not exist | `BuffersProvider.getInstance` returns error; registration is rolled back |
| Service already registered (duplicate ID) | `ServiceRegistry.register` returns error; broker logs and ignores |
| Service's control ring buffer full | `RegistrationResponse` write fails; service times out and retries or aborts |

---

## 5. Service Deregistration

Deregistration can happen in two ways:

1. **Graceful:** The service sends an `UnregisterService` message before shutting down.
2. **Forced:** The heartbeat checker detects that a service's heartbeat is stale
   (Section 8).

Both paths converge on the same `handleServiceRemoved` method.

### 5.1 HandleUnregisterService

```zig
fn handleUnregisterService(self: *Self, payload: []const u8) void {
    if (payload.len < @sizeOf(msg.UnregisterServiceMsg)) {
        log.warn("UnregisterService message too short: {} bytes", .{payload.len});
        return;
    }

    const unregister_msg: *const msg.UnregisterServiceMsg = @ptrCast(
        @alignCast(payload.ptr),
    );

    log.info("unregistering service: id={}", .{unregister_msg.service_id});
    self.handleServiceRemoved(unregister_msg.service_id);
}
```

### 5.2 HandleServiceRemoved

This is the shared path for both graceful and forced deregistration. It performs
all cleanup and notifications.

```zig
fn handleServiceRemoved(self: *Self, service_id: i32) void {
    // 1. Remove from ServiceRegistry. Returns the instance if it existed.
    const removed = self.service_registry.remove(service_id, self.local_node_id) orelse {
        log.warn("attempted to remove unknown service: id={}", .{service_id});
        return;
    };

    log.info("service removed: name={s}, id={}", .{
        removed.service_name,
        service_id,
    });

    // 2. Close the BuffersProvider (unmaps the service's metadata file).
    if (self.service_registry.getLocalBuffers(service_id)) |buffers| {
        buffers.close(self.allocator);
    }
    self.service_registry.removeLocalBuffers(service_id);

    // 3. Broadcast ServiceRemoved to peer brokers.
    self.cluster_manager.broadcastServiceRemoved(service_id, removed.service_name);

    // 4. Notify all local subscribers watching this service name.
    //    They'll receive an updated ServiceInstances list (possibly empty).
    self.notifySubscribers(removed.service_name);

    // 5. Re-evaluate service leader if this service had leader election enabled.
    if (removed.leader_election_enabled) {
        if (self.cluster_manager.isClusterLeader()) {
            const result = self.leader_election.evaluate(
                &self.service_registry,
                removed.service_name,
                self.local_node_id,
                -1, // no specific local candidate — re-evaluate globally
            );
            _ = result;
        }
    }
}
```

### 5.3 Cleanup Order

The cleanup order matters:

1. **Remove from registry first** — so subsequent subscriber notifications don't
   include the dead service.
2. **Close BuffersProvider** — unmaps the metadata file. After this, the memory
   region is no longer accessible.
3. **Broadcast to peers** — peers remove the service from their registries and
   notify their local subscribers.
4. **Notify local subscribers** — they receive the updated (reduced) instance list.
5. **Re-evaluate leader last** — leader evaluation depends on the current registry
   state, which must already reflect the removal.

---

## 6. Service Discovery

Services discover other services by subscribing to a service name. The broker sends
the complete current instance list immediately, and then again whenever the instance
set changes (registration, removal, leader change, cluster state update).

### 6.1 HandleSubscribeToServiceUpdates

```zig
fn handleSubscribeToServiceUpdates(self: *Self, payload: []const u8) void {
    if (payload.len < @sizeOf(msg.SubscribeMsg)) {
        log.warn("SubscribeToServiceUpdates message too short: {} bytes", .{payload.len});
        return;
    }

    const subscribe_msg: *const msg.SubscribeMsg = @ptrCast(@alignCast(payload.ptr));
    const service_name = msg.decodeSubscribeServiceName(payload);

    log.info("subscription: service {} subscribing to '{s}'", .{
        subscribe_msg.local_service_id,
        service_name,
    });

    // 1. Register the subscription in the registry.
    self.service_registry.addSubscription(service_name, subscribe_msg.local_service_id) catch |err| {
        log.err("failed to register subscription for service {}: {}", .{
            subscribe_msg.local_service_id,
            err,
        });
        return;
    };

    // 2. Immediately send the current instance list.
    //    Even if there are zero instances, the service needs to know.
    self.sendServiceInstances(subscribe_msg.local_service_id, service_name);
}
```

### 6.2 SendServiceInstances

Sends the complete current instance list for a service name to a single subscriber.

```zig
fn sendServiceInstances(self: *Self, subscriber_id: i32, service_name: []const u8) void {
    // Look up the subscriber's BuffersProvider to find its control ring buffer.
    const subscriber_buffers = self.service_registry.getLocalBuffers(subscriber_id) orelse {
        log.warn("subscriber {} has no local buffers — cannot send ServiceInstances", .{
            subscriber_id,
        });
        return;
    };

    // Collect all instances of this service name.
    const instances = self.service_registry.getInstancesByName(service_name);

    // Build ServiceInstanceEntry array on the stack (max 256 services).
    var entries: [constants.default_max_services]msg.ServiceInstanceEntry = undefined;
    const count = @min(instances.len, constants.default_max_services);
    for (instances[0..count], 0..) |inst, i| {
        const is_leader_id = self.service_registry.getLeader(service_name);
        entries[i] = .{
            .service_id = inst.service_id,
            .node_id = @intCast(inst.node_id),
            .is_leader = if (is_leader_id != null and is_leader_id.? == inst.service_id) 1 else 0,
        };
    }

    // Encode the ServiceInstances message.
    const len = msg.encodeServiceInstances(
        &self.encode_buf,
        subscriber_id,
        service_name,
        entries[0..count],
    );

    // Write to the subscriber's control ring buffer.
    var control_rb = RingBuffer.init(subscriber_buffers.getControlBuffer());
    control_rb.write(CONTROL_MSG_TYPE, self.encode_buf[0..len]) catch {
        log.warn("subscriber {} control ring buffer full — dropping ServiceInstances", .{
            subscriber_id,
        });
    };
}
```

### 6.3 NotifySubscribers

Called whenever the instance set for a service name changes. Sends the updated
complete list to **all** local subscribers of that name.

```zig
fn notifySubscribers(self: *Self, service_name: []const u8) void {
    const subscriber_ids = self.service_registry.getSubscribers(service_name) orelse return;

    for (subscriber_ids.items()) |subscriber_id| {
        self.sendServiceInstances(subscriber_id, service_name);
    }
}
```

### 6.4 Discovery Sequence Diagram

```
   Service A                          Broker Control Loop
      │                                      │
      │  SubscribeToServiceUpdates            │
      ├──────────────────────────────────────►│
      │  (localServiceId=1,                   │  1. Register subscription:
      │   serviceName="service-b")            │     "service-b" → {1}
      │                                       │
      │  ServiceInstances(templateId=4)       │  2. Send current instances
      │◄──────────────────────────────────────┤     (may be empty if no
      │  [{serviceId=4, nodeId=1},            │      "service-b" exists yet)
      │   {serviceId=7, nodeId=2}]            │
      │                                       │
      │     ... later, "service-b" instance   │
      │         registers on a peer broker,   │
      │         cluster state update arrives   │
      │                                       │
      │  ServiceInstances(templateId=4)       │  3. notifySubscribers("service-b")
      │◄──────────────────────────────────────┤     sends updated list to service 1
      │  [{serviceId=4, nodeId=1},            │
      │   {serviceId=7, nodeId=2},            │
      │   {serviceId=9, nodeId=2}]            │
      │                                       │
```

### 6.5 Key Property: Complete Replacement

The `ServiceInstances` message always contains the **complete** set of instances. The
receiving service replaces its entire instance list — it never applies deltas. This
simplifies the protocol enormously: no ordering issues, no missed deltas, no
reconciliation logic. The cost is slightly larger messages, but with a maximum of ~256
services × 8 bytes per entry = ~2 KB, this is negligible.

---

## 7. Service Registry

The `ServiceRegistry` is the central in-memory data structure tracking all known
service instances (local and remote) and all subscriptions.

### 7.1 Data Model

```zig
// src/control/service_registry.zig

const std = @import("std");
const BuffersProvider = @import("../memory/buffers_provider.zig").BuffersProvider;

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
```

### 7.2 ServiceRegistry Struct

```zig
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
            list.deinit();
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
            gop.value_ptr.* = std.ArrayList(InstanceKey).init(self.allocator);
        }
        try gop.value_ptr.append(key);
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

    /// Returns all instances of a service name. Caller must NOT retain
    /// the returned slice beyond the current duty cycle (the backing
    /// ArrayList may be modified on the next cycle).
    pub fn getInstancesByName(self: *Self, name: []const u8) []const ServiceInstance {
        // TODO: This returns InstanceKeys; map them to ServiceInstances.
        // For the implementation, maintain a parallel slice or iterate
        // instances. The pattern below is a simplified sketch.
        const keys = self.name_index.get(name) orelse return &.{};
        // In practice, allocate a bounded stack buffer and fill it:
        _ = keys;
        return &.{}; // placeholder — see Section 7.3 for real implementation
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

    /// Iterator over all local service instances (used by heartbeat checker).
    pub fn localServices(self: *Self) LocalServiceIterator {
        return .{
            .buffers_iter = self.local_buffers.iterator(),
            .instances = &self.instances,
            .local_node_id = undefined, // set by caller
        };
    }
};
```

### 7.3 Allocation-Free Instance Lookups

The `getInstancesByNameBuf` method fills a caller-provided stack buffer. This avoids
allocation on every subscriber notification. The caller (control loop) has a
pre-allocated `[constants.default_max_services]ServiceInstance` array.

```zig
// In ControlLoop:

fn sendServiceInstances(self: *Self, subscriber_id: i32, service_name: []const u8) void {
    var instance_buf: [constants.default_max_services]ServiceInstance = undefined;
    const count = self.service_registry.getInstancesByNameBuf(service_name, &instance_buf);

    // Build entries from instance_buf[0..count]...
}
```

### 7.4 BiInt Key Design

The `InstanceKey` struct combines `service_id` (i32) and `node_id` (u8) into a
composite key. Zig's `std.AutoHashMap` automatically generates a hash function for
this struct. An instance is globally unique within the cluster by this pair — the
same `service_id` value on two different nodes represents two different instances.

---

## 8. Heartbeat Checking

Services write their current epoch milliseconds to the `HEARTBEAT_TIME_OFFSET` (byte
256) in their metadata file approximately once per second. The broker reads this value
periodically to detect dead services.

### 8.1 ServiceHeartbeatChecker

The heartbeat checker is stateless — it reads the registry and heartbeat timestamps,
and returns a list of service IDs to remove.

```zig
// src/control/service_heartbeat_checker.zig

const std = @import("std");
const platform = @import("../platform.zig");
const constants = platform.constants;
const ServiceRegistry = @import("service_registry.zig").ServiceRegistry;
const BuffersProvider = @import("../memory/buffers_provider.zig").BuffersProvider;
const log = std.log.scoped(.heartbeat_checker);

pub const ServiceHeartbeatChecker = struct {
    /// Pre-allocated buffer for service IDs to remove.
    /// Avoids allocation during the check.
    to_remove: [constants.default_max_services]i32 = undefined,

    const Self = @This();

    pub fn init() Self {
        return .{};
    }

    /// Check all local services for heartbeat timeout.
    /// Returns a slice of service IDs that should be removed.
    pub fn check(
        self: *Self,
        registry: *ServiceRegistry,
        now_ns: i64,
    ) []const i32 {
        const now_ms = @divFloor(now_ns, std.time.ns_per_ms);
        var remove_count: u32 = 0;

        var buffers_iter = registry.local_buffers.iterator();
        while (buffers_iter.next()) |entry| {
            const service_id = entry.key_ptr.*;
            const buffers: *BuffersProvider = entry.value_ptr.*;

            const last_heartbeat = buffers.readHeartbeat();
            const elapsed = now_ms - last_heartbeat;

            if (elapsed > constants.service_heartbeat_timeout_ms) {
                log.info("heartbeat timeout: service {} (elapsed={}ms, timeout={}ms)", .{
                    service_id,
                    elapsed,
                    constants.service_heartbeat_timeout_ms,
                });

                if (remove_count < self.to_remove.len) {
                    self.to_remove[remove_count] = service_id;
                    remove_count += 1;
                }
            }
        }

        return self.to_remove[0..remove_count];
    }
};
```

### 8.2 Integration with Control Loop

```zig
// In ControlLoop:

fn checkServiceHeartbeats(self: *Self, now_ns: i64) void {
    const dead_services = self.heartbeat_checker.check(&self.service_registry, now_ns);

    for (dead_services) |service_id| {
        self.handleServiceRemoved(service_id);
    }
}
```

### 8.3 Heartbeat Read — Memory Ordering

The service writes its heartbeat timestamp with a **release** store:

```zig
// Service side:
@atomicStore(i64, heartbeat_ptr, epoch_ms, .release);
```

The broker reads it with an **acquire** load:

```zig
// Broker side (BuffersProvider.readHeartbeat → ServiceMetadataFile.loadHeartbeat):
return @atomicLoad(i64, heartbeat_ptr, .acquire);
```

This acquire/release pair ensures the broker never reads a stale or partially-written
heartbeat value, even though the service and broker are in separate processes sharing
the same memory-mapped page.

### 8.4 Timing Constants

| Parameter | Value | Rationale |
|---|---|---|
| Heartbeat write interval | ~1 second | Frequent enough to detect failures quickly, infrequent enough to be negligible overhead |
| Heartbeat check interval | 3 seconds | 3 checks within the 10-second timeout window |
| Heartbeat timeout | 10 seconds | Generous enough to tolerate GC pauses, short enough for responsive failure detection |

### 8.5 Two-Phase Collection

The heartbeat checker collects dead service IDs into a buffer first, then the control
loop removes them in a separate pass. This avoids mutating the registry's `local_buffers`
map while iterating it.

---

## 9. Service Leader Election

Service leader election is an optional per-service feature. When enabled, the **broker
cluster leader** designates one instance of the service as the leader across the entire
cluster. The rule is simple: **lowest `serviceId` wins** (effectively first-registered
wins, since service IDs are monotonically incremented).

### 9.1 Election Rule

```
Leader = instance with min(serviceId) across all nodes for a given serviceName
```

### 9.2 When Evaluation Runs

| Event | Trigger |
|---|---|
| Service registered with `leader_election_enabled = true` | Re-evaluate for that service name |
| Service removed (heartbeat timeout or graceful) with leader election enabled | Re-evaluate for that service name |
| Broker cluster leader changed | `reEvaluateAllLeaders()` for all services with leader election |
| Cluster state snapshot received from peer | Re-evaluate affected service names |

### 9.3 ServiceLeaderElection

```zig
// src/control/service_leader_election.zig

const std = @import("std");
const ServiceRegistry = @import("service_registry.zig").ServiceRegistry;
const msg = @import("control_messages.zig");
const constants = @import("../platform.zig").constants;
const log = std.log.scoped(.leader_election);

pub const ServiceLeaderElection = struct {
    const Self = @This();

    pub fn init() Self {
        return .{};
    }

    /// Evaluate the leader for a service name. Returns true if the
    /// designated leader is the `local_candidate_id` on `local_node_id`.
    ///
    /// Side effects:
    ///   - Updates `registry.service_leaders` if the leader changed.
    ///
    /// The caller is responsible for broadcasting LeaderChanged and
    /// ServiceLeaderDesignated messages after this returns.
    pub fn evaluate(
        self: *Self,
        registry: *ServiceRegistry,
        service_name: []const u8,
        local_node_id: u8,
        local_candidate_id: i32,
    ) bool {
        _ = self;
        _ = local_candidate_id;

        // Collect all instances of this service name.
        var instance_buf: [constants.default_max_services]ServiceRegistry.ServiceInstance = undefined;
        const count = registry.getInstancesByNameBuf(service_name, &instance_buf);

        if (count == 0) {
            // No instances — clear the leader.
            _ = registry.service_leaders.remove(service_name);
            return false;
        }

        // Find the instance with the lowest serviceId.
        var leader = instance_buf[0];
        for (instance_buf[1..count]) |inst| {
            if (inst.service_id < leader.service_id) {
                leader = inst;
            }
        }

        // Check if the leader changed.
        const prev_leader = registry.getLeader(service_name);
        const leader_changed = (prev_leader == null or prev_leader.? != leader.service_id);

        if (leader_changed) {
            log.info("leader elected: service_name={s}, leader_id={}, leader_node={}", .{
                service_name,
                leader.service_id,
                leader.node_id,
            });
            registry.setLeader(service_name, leader.service_id);
        }

        return leader.node_id == local_node_id;
    }

    /// Re-evaluate leaders for ALL services that have leader election
    /// enabled. Called when the broker cluster leader changes or a full
    /// cluster state snapshot is received.
    pub fn reEvaluateAll(
        self: *Self,
        registry: *ServiceRegistry,
        local_node_id: u8,
        notify_fn: *const fn (service_name: []const u8, leader_id: i32, leader_node: u8) void,
    ) void {
        var name_iter = registry.name_index.iterator();
        while (name_iter.next()) |entry| {
            const service_name = entry.key_ptr.*;
            // Check if any instance of this service has leader election enabled.
            const keys = entry.value_ptr.*;
            var has_election = false;
            for (keys.items) |key| {
                if (registry.instances.get(key)) |inst| {
                    if (inst.leader_election_enabled) {
                        has_election = true;
                        break;
                    }
                }
            }

            if (has_election) {
                _ = self.evaluate(registry, service_name, local_node_id, -1);

                // Notify via callback (broadcasts to peers and local services).
                if (registry.getLeader(service_name)) |leader_id| {
                    const leader_key = ServiceRegistry.InstanceKey{
                        .service_id = leader_id,
                        .node_id = undefined, // look up
                    };
                    _ = leader_key;
                    // In practice, look up the leader's node_id from the registry
                    // and call notify_fn.
                    _ = notify_fn;
                }
            }
        }
    }
};
```

### 9.4 Leader Designation Flow

When the cluster leader determines a new service leader:

```
   Broker Leader                   Peer Broker                    Local Services
       │                               │                               │
       │  1. evaluate("service-a")     │                               │
       │     → leader = {id=3, node=1} │                               │
       │                               │                               │
       │  ServiceLeaderDesignated      │                               │
       ├──────────────────────────────►│                               │
       │  (admin msg, templateId=8)    │  2. Update registry leader    │
       │                               │                               │
       │                               │  LeaderChanged(templateId=6)  │
       │                               ├──────────────────────────────►│
       │                               │                               │
       │  LeaderChanged(templateId=6)  │                               │
       ├──────────────────────────────►│  (to local instances of       │
       │  (to local instances on       │   "service-a" on peer node)   │
       │   leader's node)              │                               │
       │                               │                               │
```

### 9.5 Leader Changed Notification

```zig
// In ControlLoop:

fn sendLeaderChangedToLocalSubscribers(
    self: *Self,
    leader_service_id: i32,
    leader_node_id: u8,
    service_name: []const u8,
) void {
    const subscriber_set = self.service_registry.getSubscribers(service_name) orelse return;

    const len = msg.encodeLeaderChanged(
        &self.encode_buf,
        leader_service_id,
        @intCast(leader_node_id),
        service_name,
    );

    var sub_iter = subscriber_set.keyIterator();
    while (sub_iter.next()) |subscriber_id_ptr| {
        const subscriber_id = subscriber_id_ptr.*;
        const buffers = self.service_registry.getLocalBuffers(subscriber_id) orelse continue;
        var control_rb = RingBuffer.init(buffers.getControlBuffer());
        control_rb.write(CONTROL_MSG_TYPE, self.encode_buf[0..len]) catch {
            log.warn("subscriber {} control buffer full — dropping LeaderChanged", .{
                subscriber_id,
            });
        };
    }
}
```

### 9.6 Split-Brain Recovery

When the broker cluster leader changes (e.g., old leader went down, new leader
elected):

1. The **new** cluster leader calls `reEvaluateAllLeaders()`.
2. This iterates all services with `leader_election_enabled = true`.
3. For each, re-evaluates based on the currently known global state.
4. Broadcasts `ServiceLeaderDesignated` to all peers for any changed leaders.
5. Peers forward `LeaderChanged` to their local services.

Since the election rule is deterministic (lowest `serviceId` wins) and the cluster
state eventually converges (via state snapshots), split-brain resolves automatically
once connectivity is restored.

### 9.7 Managed by Cluster Leader Only

Only the broker cluster leader performs leader designations. Non-leader brokers
track the leader state they receive from the cluster leader but never initiate
leader changes. This prevents conflicting designations during network partitions.

```zig
// Guard in handleRegisterService:
if (register_msg.leader_election_enabled != 0) {
    if (self.cluster_manager.isClusterLeader()) {
        is_leader = self.leader_election.evaluate(...);
    }
    // If we're NOT the cluster leader, skip evaluation.
    // The cluster leader will evaluate when it receives
    // the ServiceAdded broadcast.
}
```

---

## 10. Inter-Event-Loop Commands

The control loop receives commands from the sender and receiver threads via an MPSC
command queue. Commands are the only mechanism for cross-thread communication — event
loops never share mutable state directly.

### 10.1 Command Types

| Source | Command | Description |
|---|---|---|
| Receiver → Control | `PeerConnected` | Peer TCP connection established — a peer broker connected |
| Receiver → Control | `PeerDisconnected` | Peer TCP connection lost — a peer broker disconnected |
| Receiver → Control | `AdminMessageReceived` | Cluster protocol message (forwarded from receiver) |
| Sender → Control | `SendError` | A send to a peer broker failed |

### 10.2 Command Struct

Commands use self-dispatching function pointers.
Each command knows how to execute itself on the target event loop.

```zig
// src/concurrent/command_queue.zig

pub const Command = struct {
    /// The handler function. Called on the target event loop's thread.
    /// `context` is the event loop's state (e.g. *ControlLoop).
    /// `self` is this Command instance.
    handler: *const fn (context: *anyopaque, self: *Command) void,

    /// Payload data. Interpretation depends on the handler.
    data: [56]u8 = undefined, // enough for any command payload, cache-line sized
};
```

### 10.3 Dispatch in Control Loop

```zig
fn dispatchCommand(self: *Self, cmd: *CommandQueue.Command) void {
    // The command's handler already knows what to do.
    cmd.handler(@ptrCast(self), cmd);
}
```

### 10.4 Example: PeerConnected Command

```zig
fn createPeerConnectedCommand(node_id: u8) CommandQueue.Command {
    var cmd = CommandQueue.Command{
        .handler = handlePeerConnected,
    };
    // Store the node_id in the data payload.
    cmd.data[0] = node_id;
    return cmd;
}

fn handlePeerConnected(context: *anyopaque, cmd: *CommandQueue.Command) void {
    const self: *ControlLoop = @ptrCast(@alignCast(context));
    const node_id = cmd.data[0];

    log.info("peer broker connected: nodeId={}", .{node_id});

    // Trigger leader election.
    self.cluster_manager.onPeerConnected(node_id);
}
```

### 10.5 Command Queue Capacity

The command queue is itself an MPSC ring buffer (see [03 — Concurrent Data
Structures](03-concurrent-data-structures.md)). It is sized to hold at least 256
commands. Since commands are drained at most 1 per duty cycle, and duty cycles run
at high frequency (microseconds to low milliseconds), the queue is unlikely to fill.
If it does, the sender/receiver thread that attempted to enqueue will spin-retry on the
next iteration of its own duty cycle.

---

## 11. Counters & Observability

The control loop updates monitoring counters on every duty cycle. These are stored in
a shared counters buffer (memory-mapped or heap-allocated) that external monitoring
tools can read.

### 11.1 Control Plane Counters

| Counter | Description |
|---|---|
| `services.registered` | Total currently registered local services |
| `services.registered.total` | Cumulative registrations since broker start |
| `services.removed` | Cumulative removals (heartbeat timeout + graceful) |
| `services.heartbeat.timeouts` | Cumulative heartbeat timeout events |
| `subscriptions.active` | Total active subscription entries |
| `control.messages.received` | Cumulative control messages processed |
| `control.commands.processed` | Cumulative inter-event-loop commands processed |
| `leader.elections.evaluated` | Cumulative service leader evaluations |

### 11.2 UpdateCounters

```zig
fn updateCounters(self: *Self) void {
    self.counters.set(.services_registered, self.service_registry.localServiceCount());
    // Other counters are incremented in-line where events occur.
}
```

---

## 12. Testing

### 12.1 Unit Test: Control Message Encode/Decode Roundtrip

```zig
const std = @import("std");
const testing = std.testing;
const msg = @import("control_messages.zig");

test "RegisterService encode/decode roundtrip" {
    // Given
    var buf: [256]u8 = undefined;
    const service_name = "pricing-service";
    const service_id: i32 = 42;

    // When
    const len = msg.encodeRegisterService(&buf, service_id, true, service_name);

    // Then
    const header: *const msg.ControlMessageHeader = @ptrCast(@alignCast(&buf));
    try testing.expectEqual(@as(u16, 1), header.template_id);

    const register: *const msg.RegisterServiceMsg = @ptrCast(@alignCast(&buf));
    try testing.expectEqual(service_id, register.service_id);
    try testing.expectEqual(@as(u8, 1), register.leader_election_enabled);
    try testing.expectEqual(@as(u8, service_name.len), register.service_name_length);

    const decoded_name = msg.decodeRegisterServiceName(buf[0..len]);
    try testing.expectEqualStrings(service_name, decoded_name);
}

test "RegistrationResponse encode/decode roundtrip" {
    // Given
    var buf: [256]u8 = undefined;

    // When
    const len = msg.encodeRegistrationResponse(&buf, 42, 1, true);

    // Then
    const response: *const msg.RegistrationResponseMsg = @ptrCast(@alignCast(&buf));
    try testing.expectEqual(@as(u16, 2), response.header.template_id);
    try testing.expectEqual(@as(i32, 42), response.service_id);
    try testing.expectEqual(@as(i16, 1), response.node_id);
    try testing.expectEqual(@as(u8, 1), response.is_leader);
    try testing.expectEqual(@as(u16, len), @sizeOf(msg.RegistrationResponseMsg));
}

test "ServiceInstances encode/decode roundtrip" {
    // Given
    var buf: [4096]u8 = undefined;
    const service_name = "order-service";
    const entries = [_]msg.ServiceInstanceEntry{
        .{ .service_id = 3, .node_id = 1, .is_leader = 1 },
        .{ .service_id = 7, .node_id = 2, .is_leader = 0 },
        .{ .service_id = 12, .node_id = 1, .is_leader = 0 },
    };

    // When
    const len = msg.encodeServiceInstances(&buf, 1, service_name, &entries);

    // Then
    const decoded = msg.decodeServiceInstances(buf[0..len]);
    try testing.expectEqualStrings(service_name, decoded.service_name);
    try testing.expectEqual(@as(usize, 3), decoded.entries.len);
    try testing.expectEqual(@as(i32, 3), decoded.entries[0].service_id);
    try testing.expectEqual(@as(i32, 7), decoded.entries[1].service_id);
    try testing.expectEqual(@as(i32, 12), decoded.entries[2].service_id);
    try testing.expectEqual(@as(u8, 1), decoded.entries[0].is_leader);
    try testing.expectEqual(@as(u8, 0), decoded.entries[1].is_leader);
}

test "LeaderChanged encode/decode roundtrip" {
    // Given
    var buf: [256]u8 = undefined;
    const service_name = "gateway";

    // When
    const len = msg.encodeLeaderChanged(&buf, 5, 2, service_name);

    // Then
    const leader_msg: *const msg.LeaderChangedMsg = @ptrCast(@alignCast(&buf));
    try testing.expectEqual(@as(u16, 6), leader_msg.header.template_id);
    try testing.expectEqual(@as(i32, 5), leader_msg.leader_service_id);
    try testing.expectEqual(@as(i16, 2), leader_msg.leader_node_id);

    const decoded_name = msg.decodeLeaderChangedServiceName(buf[0..len]);
    try testing.expectEqualStrings(service_name, decoded_name);
}
```

### 12.2 Unit Test: Message Struct Sizes

```zig
test "control message struct sizes are deterministic" {
    // Given / When / Then
    try testing.expectEqual(@as(usize, 4), @sizeOf(msg.ControlMessageHeader));
    try testing.expectEqual(@as(usize, 12), @sizeOf(msg.RegisterServiceMsg));
    try testing.expectEqual(@as(usize, 12), @sizeOf(msg.RegistrationResponseMsg));
    try testing.expectEqual(@as(usize, 12), @sizeOf(msg.SubscribeMsg));
    try testing.expectEqual(@as(usize, 12), @sizeOf(msg.ServiceInstancesMsg));
    try testing.expectEqual(@as(usize, 8), @sizeOf(msg.ServiceInstanceEntry));
    try testing.expectEqual(@as(usize, 12), @sizeOf(msg.UnregisterServiceMsg));
    try testing.expectEqual(@as(usize, 12), @sizeOf(msg.LeaderChangedMsg));
}
```

### 12.3 Unit Test: Service Registry — Register and Remove

```zig
const ServiceRegistry = @import("service_registry.zig").ServiceRegistry;

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
    var buf: [256]ServiceRegistry.ServiceInstance = undefined;
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

    var buf: [256]ServiceRegistry.ServiceInstance = undefined;
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
    var buf: [256]ServiceRegistry.ServiceInstance = undefined;
    const count = registry.getInstancesByNameBuf("my-service", &buf);
    try testing.expectEqual(@as(u32, 2), count);
}
```

### 12.4 Unit Test: Subscription and Notification

```zig
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
```

### 12.5 Unit Test: Leader Election — Lowest ServiceId Wins

```zig
const ServiceLeaderElection = @import("service_leader_election.zig").ServiceLeaderElection;

test "leader election selects lowest serviceId" {
    // Given
    var registry = ServiceRegistry.init(testing.allocator);
    defer registry.deinit();

    try registry.register(.{ .service_id = 10, .node_id = 1, .service_name = "svc", .leader_election_enabled = true, .is_local = true });
    try registry.register(.{ .service_id = 3, .node_id = 2, .service_name = "svc", .leader_election_enabled = true, .is_local = false });
    try registry.register(.{ .service_id = 7, .node_id = 1, .service_name = "svc", .leader_election_enabled = true, .is_local = true });

    var election = ServiceLeaderElection.init();

    // When
    _ = election.evaluate(&registry, "svc", 1, 10);

    // Then
    const leader = registry.getLeader("svc");
    try testing.expect(leader != null);
    try testing.expectEqual(@as(i32, 3), leader.?);
}

test "leader election with single instance" {
    // Given
    var registry = ServiceRegistry.init(testing.allocator);
    defer registry.deinit();

    try registry.register(.{ .service_id = 5, .node_id = 1, .service_name = "svc", .leader_election_enabled = true, .is_local = true });

    var election = ServiceLeaderElection.init();

    // When
    const is_local_leader = election.evaluate(&registry, "svc", 1, 5);

    // Then
    try testing.expect(is_local_leader);
    try testing.expectEqual(@as(i32, 5), registry.getLeader("svc").?);
}

test "leader election with no instances clears leader" {
    // Given
    var registry = ServiceRegistry.init(testing.allocator);
    defer registry.deinit();

    registry.setLeader("svc", 5); // pre-existing leader

    var election = ServiceLeaderElection.init();

    // When
    const is_local_leader = election.evaluate(&registry, "svc", 1, -1);

    // Then
    try testing.expect(!is_local_leader);
    try testing.expect(registry.getLeader("svc") == null);
}
```

### 12.6 Unit Test: Heartbeat Timeout Triggers Removal

```zig
test "heartbeat timeout returns service for removal" {
    // This test requires a mock BuffersProvider or a real metadata file
    // with a stale heartbeat. Sketch:

    // Given
    // - Create a service metadata file with heartbeat set to (now - 15 seconds)
    // - Register the service in a ServiceRegistry with a real BuffersProvider
    // - Create a ServiceHeartbeatChecker

    // When
    // const dead = checker.check(&registry, now_ns);

    // Then
    // try testing.expectEqual(@as(usize, 1), dead.len);
    // try testing.expectEqual(service_id, dead[0]);
}
```

### 12.7 Integration Test: Full Registration Flow

```zig
test "full registration flow: register → response → subscribe → instances" {
    // This test requires:
    // 1. A broker metadata file with a control ring buffer
    // 2. A service metadata file with a control ring buffer
    // 3. A ControlLoop wired up with real ring buffers
    //
    // Steps:
    // 1. Write RegisterService to the broker's control ring buffer
    // 2. Call controlLoop.doWork() to process it
    // 3. Read the service's control ring buffer for RegistrationResponse
    // 4. Verify the response contains the correct serviceId and nodeId
    // 5. Write SubscribeToServiceUpdates to the broker's control ring buffer
    // 6. Call controlLoop.doWork() again
    // 7. Read the service's control ring buffer for ServiceInstances
    // 8. Verify the instances list contains the registered service
    //
    // This is the most important integration test — it validates the
    // entire control plane end-to-end without network involvement.
}
```

### 12.8 Integration Test: Service Removed → Subscribers Notified

```zig
test "service removal notifies subscribers with updated instance list" {
    // Steps:
    // 1. Register service A (id=1, name="svc-a")
    // 2. Register service B (id=2, name="svc-b")
    // 3. Service B subscribes to "svc-a"
    // 4. doWork() — service B receives ServiceInstances with [id=1]
    // 5. Simulate heartbeat timeout for service A (set stale heartbeat)
    // 6. doWork() with heartbeat check interval elapsed
    // 7. Read service B's control ring buffer
    // 8. Verify ServiceInstances with empty list is received
}
```

---

## 13. File Structure

```
src/
  control/
    control_loop.zig               # Main duty-cycle event loop (Section 3)
    control_messages.zig           # Message structs, encode/decode (Section 2)
    service_registry.zig           # ServiceRegistry data structure (Section 7)
    service_heartbeat_checker.zig  # Heartbeat timeout detection (Section 8)
    service_leader_election.zig    # Leader evaluation logic (Section 9)
  memory/
    buffers_provider.zig           # Per-service metadata file cache (doc 02)
  concurrent/
    ring_buffer.zig                # MPSC ring buffer (doc 03)
    command_queue.zig              # Inter-event-loop command queue (Section 10)
  cluster/
    cluster_manager.zig            # Cluster state, leader election (doc 11)
  counters/
    counters_manager.zig           # Monitoring counters (Section 11)
```

### Module Dependencies

```
                    ┌──────────────────────┐
                    │    control_loop.zig   │
                    │    (Section 3)        │
                    └──────┬───────────────┘
                           │
          ┌────────────────┼────────────────────────┐
          │                │                        │
          ▼                ▼                        ▼
┌─────────────────┐ ┌──────────────┐ ┌──────────────────────────┐
│ control_messages │ │  service_    │ │ service_heartbeat_checker │
│   .zig          │ │  registry    │ │ .zig                      │
│ (encode/decode) │ │  .zig        │ │ (check)                   │
└─────────────────┘ └──────┬───────┘ └──────────────────────────┘
                           │
                           ▼
                   ┌──────────────────────┐
                   │ service_leader_      │
                   │ election.zig         │
                   │ (evaluate)           │
                   └──────────────────────┘
                           │
              ┌────────────┴────────────┐
              ▼                         ▼
      ┌──────────────┐         ┌──────────────┐
      │ buffers_      │         │ ring_buffer  │
      │ provider.zig  │         │ .zig         │
      │ (doc 02)      │         │ (doc 03)     │
      └──────────────┘         └──────────────┘
```

---

## Summary

The control plane is the coordination hub of the broker. It runs entirely on a single
thread, avoiding all locking and cross-thread synchronization for its owned data
structures. Communication with other threads happens exclusively through command
queues. The key design choices are:

| Decision | Rationale |
|---|---|
| Single-threaded control loop | No locks needed for ServiceRegistry, subscriptions, leader state |
| Pre-allocated encode buffer | Zero allocation on hot path |
| `packed struct` flyweight messages | Zero-copy encode/decode directly on ring buffer memory |
| Complete instance lists (no deltas) | Simplifies protocol; no ordering or missed-delta issues |
| Two-phase heartbeat collection | Avoids mutating iterator during scan |
| Leader election on cluster leader only | Prevents split-brain leader conflicts |
| Rate-limited periodic tasks | Avoids cache pollution from metadata file reads |
| Self-dispatching commands | Commands carry their own handler — no central switch statement |

---

*Previous: [08 — Service IPC](08-service-ipc.md) · Next: [10 — Threading Model](10-threading-model.md)*
