# 08 — Service ↔ Broker IPC

> **Depends on:** [02 — Memory Layout & Shared Memory](02-memory-layout-and-shared-memory.md) (metadata files, buffer regions, `BuffersProvider`, `ServiceMetadataFile`, `BrokerMetadataFile`),
> [03 — Concurrent Data Structures](03-concurrent-data-structures.md) (MPSC ring buffer, `RingBuffer.init`, `tryClaim`, `read`, `write`)
>
> **Depended on by:** [09 — Control Plane](09-control-plane.md) (registration, discovery, heartbeats ride on the channels defined here),
> [10 — Threading Model](10-threading-model.md) (event loops wrap the agents defined here)

This document specifies the Service ↔ Broker IPC subsystem — the four shared-memory
channels that connect every service to its local broker, the same-host direct path that
bypasses the broker entirely, and the cross-host routed path that hands messages off to
the broker's TCP transport layer.

All code targets **Zig 0.16.x** stable.

---

## Table of Contents

1. [Overview](#1-overview)
2. [IPC Architecture — Four Shared-Memory Channels](#2-ipc-architecture--four-shared-memory-channels)
3. [Service Startup (Client Side)](#3-service-startup-client-side)
4. [Same-Host Message Path (Zero Broker Involvement)](#4-same-host-message-path-zero-broker-involvement)
5. [Cross-Host Message Path (Broker-Routed)](#5-cross-host-message-path-broker-routed)
6. [Service Discovery (Client Side)](#6-service-discovery-client-side)
7. [ServiceClient — Client-Side Proxy](#7-serviceclient--client-side-proxy)
8. [ServiceInstance](#8-serviceinstance)
9. [RingLoomEngine — Main Entry Point](#9-ringloomengine--main-entry-point)
10. [MessageConsumer](#10-messageconsumer)
11. [ControlAgent](#11-controlagent)
12. [Message Header — Routing Fields](#12-message-header--routing-fields)
13. [Message Fragmentation and Reassembly](#13-message-fragmentation-and-reassembly)
14. [Testing](#14-testing)
15. [File Structure](#15-file-structure)

---

## 1. Overview

There are two IPC paths:

1. **Same-host direct path** — Service A writes directly into Service B's messages ring
   buffer via the memory-mapped region in Service B's metadata file. The broker is not
   involved at all. This is the lowest-latency path (~100 ns), zero-copy when using the
   `tryClaim` API, and the common case for co-located services.

2. **Cross-host routed path** — Service A writes into the broker's send ring buffer
   (via the broker's metadata file). The broker's sender event loop drains this buffer
   and transmits the message over TCP to the remote broker. The remote broker writes the
   message into the target service's messages ring buffer on that host.

Both paths use the MPSC ring buffer defined in doc 03, backed by memory-mapped files
defined in doc 02. No kernel involvement occurs on the hot path — producers and
consumers operate on the same physical pages via `mmap`.

```
                        Same Host                              Cross Host
                   ┌─────────────────┐                   ┌─────────────────┐
                   │                 │                   │                 │
  Service A ──────►│  Service B's    │   Service A ─────►│  Broker's Send  │
  (IpcProducer)    │  Messages RB    │   (IpcProducer)   │  Ring Buffer    │
                   │  (direct write) │                   │  (routed write) │
                   │                 │                   │                 │
                   └────────┬────────┘                   └────────┬────────┘
                            │                                     │
                            ▼                                     ▼
                     Service B reads                       Broker drains →
                     (MessageConsumer)                     TCP to remote host
```

---

## 2. IPC Architecture — Four Shared-Memory Channels

Service ↔ Broker communication uses four shared-memory channels. Each channel is a
region within either the broker's or the service's metadata file, interpreted as an
MPSC ring buffer.

```
┌─────────────────────────────────────────────────────────────────────────┐
│                                                                         │
│  Channel 1: Service → Broker Control                                    │
│  ─────────────────────────────────                                      │
│  Location:   Broker's metadata file, control buffer region              │
│  Direction:  Service writes, broker reads (MPSC)                        │
│  Messages:   RegisterService, SubscribeToServiceUpdates,                │
│              UnregisterService, heartbeat control                       │
│                                                                         │
│  Channel 2: Broker → Service Control                                    │
│  ─────────────────────────────────                                      │
│  Location:   Service's metadata file, control buffer region             │
│  Direction:  Broker writes, service reads (MPSC — but single writer     │
│              in practice since only the local broker writes here)        │
│  Messages:   RegistrationResponse, ServiceInstances,                    │
│              LeaderChanged, ServiceRemoved                              │
│                                                                         │
│  Channel 3: Service → Broker Send                                       │
│  ──────────────────────────────                                         │
│  Location:   Broker's metadata file, send buffer region                 │
│  Direction:  Service writes, broker reads (MPSC)                        │
│  Messages:   Cross-host outbound application messages                   │
│              (routed by targetNodeId in the RingLoom message header)         │
│                                                                         │
│  Channel 4: Producers → Service Messages                                │
│  ────────────────────────────────────                                   │
│  Location:   Service's metadata file, messages buffer region            │
│  Direction:  Any producer writes, service reads (MPSC)                  │
│  Producers:  Local services (same-host direct IPC),                     │
│              local broker (delivering cross-host inbound messages)       │
│  Messages:   Application messages                                       │
│                                                                         │
└─────────────────────────────────────────────────────────────────────────┘
```

Channel layout in memory, with references to doc 02 structures:

| Channel | Backing File | Buffer Accessor | Default Size |
|---------|-------------|-----------------|-------------|
| Service → Broker Control | `broker_0.dat` | `BrokerMetadataFile.getControlBuffer()` | 64 KiB |
| Broker → Service Control | `<name>_node<node_id>_<id>.dat` | `ServiceMetadataFile.getControlBuffer()` | 64 KiB |
| Service → Broker Send | `broker_0.dat` | `BrokerMetadataFile.getSendBuffer()` | 1 MiB |
| Producers → Service Messages | `<name>_node<node_id>_<id>.dat` | `ServiceMetadataFile.getMessagesBuffer()` | 1 MiB |

All four buffers are MPSC ring buffers initialized with `RingBuffer.init(buffer_slice)`
from doc 03.

---

## 3. Service Startup (Client Side)

When a service starts, it executes the following sequence:

1. Open the broker's metadata file.
2. Allocate a unique `service_id` from the broker's atomic counter.
3. Create the service's own metadata file.
4. Register with the broker via Channel 1.
5. Wait for a registration response via Channel 2.
6. Start the heartbeat writer.
7. Start the message consumer thread.
8. Start the control agent thread.

### 3.1 Create Service Metadata File

```zig
// src/service/ringloom_engine.zig

const std = @import("std");
const platform = @import("../platform.zig");
const memory = @import("../memory.zig");
const ring_buffer = @import("../concurrent/ring_buffer.zig");
const constants = @import("../memory/constants.zig");

const BrokerMetadataFile = memory.BrokerMetadataFile;
const ServiceMetadataFile = memory.ServiceMetadataFile;
const BuffersProvider = memory.BuffersProvider;
const RingBuffer = ring_buffer.RingBuffer;
const Clock = platform.Clock;

pub const ServiceConfig = struct {
    storage_path: []const u8 = constants.default_storage_path,
    group: []const u8 = "default",
    service_name: []const u8,
    blocking_mode: bool = false,
    heartbeat_timeout_ms: i32 = @intCast(constants.default_heartbeat_timeout_ms),
    control_buffer_length: usize = constants.default_control_buffer_length,
    messages_buffer_length: usize = constants.default_messages_buffer_length,
    leader_election_enabled: bool = false,
};

/// Opens the broker's metadata file, allocates a unique service_id,
/// and creates this service's metadata file on /dev/shm.
fn createServiceMetadata(config: ServiceConfig) !struct {
    broker_meta: *BrokerMetadataFile,
    service_meta: *ServiceMetadataFile,
    service_id: i32,
    node_id: i16,
} {
    // 1. Open the broker's metadata file (must already exist).
    //    The broker creates this at startup; services connect to it.
    const broker_meta = try BrokerMetadataFile.open(
        config.storage_path,
        config.group,
    );

    // 2. Atomically allocate a unique service_id.
    //    This is a fetchAdd(1) on the broker metadata file's next_service_id
    //    field. Multiple services may call this concurrently — the atomic
    //    guarantees uniqueness.
    const service_id = broker_meta.incrementAndGetNextServiceId();

    // 3. Read the node_id from the broker's header.
    //    All services on this host share the same node_id.
    const node_id = broker_meta.header.node_id;

    // 4. Create the service's metadata file.
    //    Path: <storage_path>/<group>/services/<name>_node<node_id>_<id>.dat
    //
    //    File layout (from doc 02):
    //      [512-byte header]
    //      [384-byte blocking trailer, if blocking_mode]
    //      [control ring buffer, control_buffer_length bytes]
    //      [messages ring buffer, messages_buffer_length bytes]
    const service_meta = try ServiceMetadataFile.create(.{
        .storage_path = config.storage_path,
        .group = config.group,
        .service_name = config.service_name,
        .service_id = service_id,
        .node_id = node_id,
        .blocking_mode = config.blocking_mode,
        .heartbeat_timeout_ms = config.heartbeat_timeout_ms,
        .control_buffer_length = config.control_buffer_length,
        .messages_buffer_length = config.messages_buffer_length,
    });

    return .{
        .broker_meta = broker_meta,
        .service_meta = service_meta,
        .service_id = service_id,
        .node_id = node_id,
    };
}
```

### 3.2 Register with Broker

After creating the metadata file, the service writes a `RegisterService` control
message into the broker's control ring buffer (Channel 1).

```zig
// src/service/control_agent.zig

const control_encoding = @import("../message/control_encoding.zig");

/// Template IDs for control messages (matches SBE schema).
const TemplateId = struct {
    const register_service: u16 = 1;
    const registration_response: u16 = 2;
    const subscribe_to_service_updates: u16 = 3;
    const service_instances: u16 = 4;
    const unregister_service: u16 = 5;
    const leader_changed: u16 = 6;
};

/// Writes a RegisterService message into the broker's control ring buffer.
fn registerWithBroker(
    broker_meta: *BrokerMetadataFile,
    service_id: i32,
    service_name: []const u8,
    leader_election_enabled: bool,
) !void {
    // Initialize a ring buffer over the broker's control buffer region.
    var control_rb = RingBuffer.init(broker_meta.getControlBuffer());

    // Encode the RegisterService message into a stack-local buffer.
    // The message is small (service_id + name + flags) — well under 256 bytes.
    var msg_buf: [256]u8 = undefined;
    const msg_len = control_encoding.encodeRegisterService(&msg_buf, .{
        .service_id = service_id,
        .service_name = service_name,
        .leader_election_enabled = leader_election_enabled,
    });

    // Write into the broker's control ring buffer (MPSC — may contend
    // with other services registering concurrently).
    try control_rb.write(constants.control_msg_type_id, msg_buf[0..msg_len]);
}
```

### 3.3 Wait for Registration Response

After sending the registration request, the service polls its own control ring buffer
(Channel 2) for the broker's `RegistrationResponse` message.

```zig
// src/service/control_agent.zig

const RegistrationResponse = struct {
    service_id: i32,
    node_id: i16,
    success: bool,
};

/// Blocks until the broker sends a RegistrationResponse, or until timeout.
fn waitForRegistrationResponse(
    service_meta: *ServiceMetadataFile,
    timeout_ms: u64,
) !RegistrationResponse {
    var control_rb = RingBuffer.init(service_meta.getControlBuffer());

    var response: ?RegistrationResponse = null;
    const deadline_ns = Clock.monotonicNanos() + (timeout_ms * std.time.ns_per_ms);

    while (response == null) {
        // Poll for one message at a time.
        _ = control_rb.read(struct {
            resp: *?RegistrationResponse,

            pub fn onMessage(self: *@This(), _: i32, payload: []const u8) void {
                const template_id = control_encoding.readTemplateId(payload);
                if (template_id == TemplateId.registration_response) {
                    self.resp.* = control_encoding.decodeRegistrationResponse(payload);
                }
            }
        }{ .resp = &response }, 1);

        if (response != null) break;

        // Check deadline.
        if (Clock.monotonicNanos() >= deadline_ns) {
            return error.RegistrationTimeout;
        }

        // Yield briefly before retrying. The registration path is a cold path —
        // a short sleep is acceptable here.
        std.time.sleep(1 * std.time.ns_per_ms);
    }

    const resp = response.?;
    if (!resp.success) return error.RegistrationRejected;
    return resp;
}
```

### 3.4 Start Heartbeat Writer

The service periodically writes the current wall-clock time into its metadata file's
`heartbeat_time_ms` field. The broker's heartbeat checker reads this field to detect
dead services.

```zig
// src/service/control_agent.zig

/// Heartbeat write interval — how often the service updates its timestamp.
const heartbeat_write_interval_ns: u64 =
    constants.service_heartbeat_write_interval_ms * std.time.ns_per_ms;

/// Writes the current epoch time into the service's heartbeat field.
/// Called periodically from the control agent's duty cycle.
fn writeHeartbeat(service_meta: *ServiceMetadataFile) void {
    service_meta.storeHeartbeat(Clock.epochMillis());
}

/// Checks whether enough time has elapsed since the last heartbeat write.
fn shouldWriteHeartbeat(last_heartbeat_ns: *u64) bool {
    const now = Clock.monotonicNanos();
    if (now - last_heartbeat_ns.* >= heartbeat_write_interval_ns) {
        last_heartbeat_ns.* = now;
        return true;
    }
    return false;
}
```

The heartbeat is an `@atomicStore` with `.release` ordering on the service side and an
`@atomicLoad` with `.acquire` ordering on the broker side (see doc 02, §4.4). On
x86-64 this compiles to plain `mov` instructions with a compiler fence — zero overhead.

---

## 4. Same-Host Message Path (Zero Broker Involvement)

When `targetNodeId == sourceNodeId`, the sending service writes directly into the
target service's messages ring buffer. The broker is not involved. This is the fastest
path:

- **No broker hop** — the message does not traverse Channel 3 or any broker thread.
- **No serialization beyond the application payload** — the RingLoom message header is
  written directly into the ring buffer record.
- **Sub-microsecond latency** — measured at ~100 ns for small messages on modern
  hardware.
- **Zero-copy with `tryClaim`** — the producer claims a slot, gets a pointer into
  shared memory, writes directly, and commits. No intermediate buffer.

```
Service A                                  Service B
┌──────────┐                              ┌──────────┐
│          │  1. Look up ServiceInstance   │          │
│          │  2. Get IpcProducer           │          │
│          │  3. tryClaim / write          │          │
│          │ ────────────────────────────► │ Messages │
│          │    (direct shared memory)     │ Ring Buf │
│          │                              │          │
│          │  No broker involvement!       │          │
└──────────┘                              └──────────┘
```

### 4.1 IpcProducer

The `IpcProducer` wraps a ring buffer backed by a target service's messages buffer
region. It is the `RingLoomProducer` implementation for same-host IPC.

```zig
// src/ipc/ipc_producer.zig

const std = @import("std");
const RingBuffer = @import("../concurrent/ring_buffer.zig").RingBuffer;
const constants = @import("../memory/constants.zig");

pub const IpcProducer = struct {
    ring_buffer: *RingBuffer,

    const Self = @This();

    /// Initialize an IpcProducer over a target service's messages buffer.
    pub fn init(messages_buffer: []u8) Self {
        return .{
            .ring_buffer = RingBuffer.init(messages_buffer),
        };
    }

    /// Write a complete message into the target service's ring buffer.
    /// The message is copied into the ring buffer as a single record.
    ///
    /// Returns error.InsufficientCapacity if the ring buffer is full.
    /// On the hot path, callers should check the return value and apply
    /// back-pressure (retry, drop, or yield) rather than propagating the error.
    pub fn write(self: *Self, msg_type: i32, payload: []const u8) !void {
        return self.ring_buffer.write(msg_type, payload);
    }

    /// Claim a contiguous region in the ring buffer for zero-copy writing.
    ///
    /// Returns a `MemoryClaim` with a pointer directly into shared memory.
    /// The caller writes the payload into the claimed region, then calls
    /// `claim.commit()` to make it visible to the consumer.
    ///
    /// Returns null if insufficient space is available.
    pub fn tryClaim(self: *Self, length: usize) ?MemoryClaim {
        return self.ring_buffer.tryClaim(length);
    }

    /// Returns the ring buffer's current remaining capacity in bytes.
    /// Useful for back-pressure decisions.
    pub fn remainingCapacity(self: *const Self) i64 {
        return self.ring_buffer.remainingCapacity();
    }
};

/// A claimed region in the ring buffer. The caller writes directly into
/// the shared memory, then commits (or aborts) the claim.
///
/// Implements an RAII-like pattern: if `commit` is not called, the
/// destructor aborts the claim.
pub const MemoryClaim = struct {
    buffer: []u8,
    ring_buffer: *RingBuffer,
    offset: usize,
    length: usize,
    committed: bool = false,

    /// Returns a writable slice into the claimed ring buffer region.
    /// The caller writes the payload directly here — no intermediate copy.
    pub fn writableSlice(self: *MemoryClaim) []u8 {
        return self.buffer[self.offset..][0..self.length];
    }

    /// Commit the claim, making the record visible to the consumer.
    /// After this call, the consumer can read the message.
    pub fn commit(self: *MemoryClaim) void {
        self.ring_buffer.commitClaim(self.offset, self.length);
        self.committed = true;
    }

    /// Abort the claim without committing. The ring buffer slot is released
    /// but the consumer never sees the message.
    pub fn abort(self: *MemoryClaim) void {
        self.ring_buffer.abortClaim(self.offset, self.length);
        self.committed = true; // prevent double-free in deinit
    }
};
```

### 4.2 IpcConsumer

The `IpcConsumer` wraps a ring buffer and provides the polling API. Each service has
one `IpcConsumer` on its messages ring buffer, driven by the `MessageConsumer` agent.

```zig
// src/ipc/ipc_consumer.zig

const std = @import("std");
const RingBuffer = @import("../concurrent/ring_buffer.zig").RingBuffer;

/// Callback signature for processing messages read from the ring buffer.
pub const MessageHandler = *const fn (msg_type: i32, buffer: []const u8) void;

pub const IpcConsumer = struct {
    ring_buffer: *RingBuffer,

    const Self = @This();

    /// Initialize an IpcConsumer over this service's messages buffer.
    pub fn init(messages_buffer: []u8) Self {
        return .{
            .ring_buffer = RingBuffer.init(messages_buffer),
        };
    }

    /// Poll the ring buffer for available messages.
    ///
    /// Calls `handler` for each message, up to `limit` messages per poll.
    /// Returns the number of messages processed (the "work count" for the
    /// duty-cycle event loop).
    ///
    /// This is the single-consumer side of the MPSC ring buffer. Only one
    /// thread may call `poll` at a time.
    pub fn poll(self: *Self, handler: MessageHandler, limit: u32) u32 {
        return self.ring_buffer.read(handler, limit);
    }
};
```

### 4.3 Same-Host Send Flow

Putting it together — sending a message to a service on the same host:

```zig
// Illustrative usage — the ServiceClient (§7) encapsulates this.

fn sendToLocalService(
    target_service_id: i32,
    msg_type: i32,
    payload: []const u8,
) !void {
    // 1. Look up the target service's BuffersProvider from the cache.
    //    The cache is populated when the broker sends ServiceInstances
    //    updates (§6). Each entry holds an open mmap to the target
    //    service's metadata file.
    const target = BuffersProvider.getCached(target_service_id) orelse
        return error.ServiceNotFound;

    // 2. Create an IpcProducer over the target's messages buffer.
    //    In practice, this is cached on the ServiceInstance (§8).
    var producer = IpcProducer.init(target.getMessagesBuffer());

    // 3. Write directly into the target service's ring buffer.
    //    This is a single CAS on the tail position + memcpy into the
    //    shared memory region. No broker, no syscall, no copy beyond
    //    the ring buffer write itself.
    try producer.write(msg_type, payload);
}
```

---

## 5. Cross-Host Message Path (Broker-Routed)

When `targetNodeId != sourceNodeId`, the message must traverse the network. The sending
service writes the message into the broker's send ring buffer (Channel 3). The broker's
sender event loop (doc 05) drains this buffer, looks up the target node's TCP connection,
and transmits the message. On the remote host, the receiving broker delivers the
message into the target service's messages ring buffer.

```
 Source Host                                          Destination Host
┌───────────────────────────────────────┐   ┌───────────────────────────────────────┐
│                                       │   │                                       │
│  Service A                            │   │                            Service B  │
│     │                                 │   │                                │      │
│     │ 1. write to broker's send RB    │   │  4. write to Service B's       │      │
│     ▼                                 │   │     messages RB                ▲      │
│  ┌──────────────────┐                 │   │  ┌──────────────────┐          │      │
│  │ Broker Send RB   │                 │   │  │ Broker (routing) │──────────┘      │
│  │ (Channel 3)      │                 │   │  │                  │                 │
│  └────────┬─────────┘                 │   │  └────────▲─────────┘                 │
│           │                           │   │           │                            │
│           │ 2. Broker sender drains   │   │           │ 3. Remote broker receives  │
│           ▼                           │   │           │                            │
│  ┌──────────────────┐   TCP frame     │   │  ┌──────────────────┐                 │
│  │ Sender Event     │ ══════════════════════►│ Receiver Event   │                 │
│  │ Loop (doc 05)    │   (ringloom_tcp)    │   │  │ Loop (doc 06)    │                 │
│  └──────────────────┘                 │   │  └──────────────────┘                 │
│                                       │   │                                       │
└───────────────────────────────────────┘   └───────────────────────────────────────┘
```

### 5.1 Writing to the Broker's Send Ring Buffer

The sending service builds a message with a RingLoom message header (§12) containing
routing fields, then writes it into the broker's send ring buffer.

```zig
// src/service/ringloom_engine.zig

const message_header = @import("../message/message_header.zig");
const MessageHeader = message_header.MessageHeader;

/// Sends a message to a service on a remote host via the broker.
fn sendToRemoteService(
    broker_meta: *BrokerMetadataFile,
    source_node_id: i16,
    source_service_id: i32,
    target_node_id: i16,
    target_service_id: i32,
    payload: []const u8,
) !void {
    // 1. Initialize a ring buffer over the broker's send buffer region.
    var send_rb = RingBuffer.init(broker_meta.getSendBuffer());

    // 2. Use tryClaim for zero-copy header + payload writing.
    const total_len = MessageHeader.encoded_length + payload.len;
    var claim = send_rb.tryClaim(total_len) orelse return error.SendBufferFull;

    // 3. Write the RingLoom message header into the claimed region.
    //    The header contains all routing information the broker needs
    //    to forward this message to the correct remote host and service.
    var writable = claim.writableSlice();
    MessageHeader.encode(writable[0..MessageHeader.encoded_length], .{
        .source_node_id = source_node_id,
        .source_service_id = @intCast(source_service_id),
        .target_node_id = target_node_id,
        .target_service_id = @intCast(target_service_id),
        .template_id = 0, // raw application message
        .correlation_id = 0,
        .flags = 0,
    });

    // 4. Copy the application payload immediately after the header.
    @memcpy(writable[MessageHeader.encoded_length..][0..payload.len], payload);

    // 5. Commit — makes the message visible to the broker's sender event loop.
    claim.commit();
}
```

### 5.2 Broker-Side Routing

On the broker side, the `MessageRoutingConsumer` (doc 05/10) reads from the send ring
buffer and routes based on `targetNodeId`:

```zig
// src/broker/routing/message_routing_consumer.zig (sketch — full impl in doc 10)

fn onSendBufferMessage(msg_type: i32, payload: []const u8) void {
    // Decode the routing header (flyweight — no allocation).
    const header = MessageHeader.decode(payload[0..MessageHeader.encoded_length]);
    const body = payload[MessageHeader.encoded_length..];

    if (header.target_node_id == local_node_id) {
        // Message is for a service on this host — deliver locally.
        // This shouldn't normally happen (the service should use the direct
        // path), but handle it for correctness.
        deliverToLocalService(header.target_service_id, msg_type, body);
    } else {
        // Message is for a remote host — forward via TCP.
        // The MessageRoutingPublisher looks up the target node's TCP connection
        // (via ringloom_tcp) and transmits the message.
        routing_publisher.sendToRemoteNode(header.target_node_id, payload);
    }
}
```

### 5.3 Remote Broker Delivery

On the destination host, the broker's receiver event loop receives the TCP frame
(doc 06), strips the broker-to-broker transport header, preserves the logical
template ID as the service-visible ring-buffer message type, and writes the
application payload into the target service's messages ring buffer:

```zig
// src/broker/receiver/message_router.zig (sketch)

fn onRemoteMessageReceived(header: *const TcpFrameHeader, payload: []const u8) void {
    // Look up the target service's BuffersProvider.
    const target = BuffersProvider.getCached(header.target_service_id) orelse {
        // Service not registered locally — drop the message.
        // (Could log a warning on the cold path.)
        return;
    };

    const msg_type_id: i32 = if (header.template_id == 0)
        constants.application_msg_type_id
    else
        @intCast(header.template_id);

    // Write only the application payload into the target service's messages
    // ring buffer. The TCP frame header remains broker-internal.
    var messages_rb = RingBuffer.init(target.getMessagesBuffer());
    messages_rb.write(msg_type_id, payload) catch {
        // Ring buffer full — the target service is a slow consumer.
        // Back-pressure propagates via the flow control layer (doc 07).
    };
}
```

---

## 6. Service Discovery (Client Side)

Services discover other services through the broker. The flow is:

1. The service sends a `SubscribeToServiceUpdates` message to the broker (Channel 1).
2. The broker responds with a `ServiceInstances` message containing all known
   instances of the requested service (Channel 2).
3. The broker pushes incremental updates whenever instances are added or removed.

### 6.1 Subscribe Request

```zig
// src/service/control_agent.zig

/// Sends a subscription request to the broker for a named service.
/// The broker will respond with current instances and push future updates.
fn subscribeToServiceUpdates(
    broker_meta: *BrokerMetadataFile,
    local_service_id: i32,
    remote_service_name: []const u8,
) !void {
    var control_rb = RingBuffer.init(broker_meta.getControlBuffer());

    var msg_buf: [256]u8 = undefined;
    const msg_len = control_encoding.encodeSubscribeToServiceUpdates(&msg_buf, .{
        .subscriber_service_id = local_service_id,
        .target_service_name = remote_service_name,
    });

    try control_rb.write(constants.control_msg_type_id, msg_buf[0..msg_len]);
}
```

### 6.2 Handling ServiceInstances Response

When the broker sends a `ServiceInstances` message, the control agent decodes it and
updates the `ServiceClientRegistry`:

```zig
// src/service/control_agent.zig

fn handleServiceInstances(
    service_registry: *ServiceClientRegistry,
    payload: []const u8,
) void {
    const instances = control_encoding.decodeServiceInstances(payload);

    // Update the registry with the new set of instances.
    // For each instance, if it's on the local node, open a BuffersProvider
    // and create an IpcProducer for direct IPC.
    for (instances) |inst| {
        service_registry.addOrUpdateInstance(.{
            .service_id = inst.service_id,
            .service_name = inst.service_name,
            .node_id = inst.node_id,
            .is_leader = inst.is_leader,
        });
    }
}
```

### 6.3 Handling Instance Changes

The broker pushes incremental updates as `ServiceInstances` messages when services
register, unregister, or when leadership changes. The control agent processes these
the same way — the `ServiceClientRegistry` reconciles the full set.

---

## 7. ServiceClient — Client-Side Proxy

The `ServiceClient` is the application-facing API for sending messages to a named
service. It encapsulates service discovery, instance tracking, and load balancing.

```zig
// src/service/service_client.zig

const std = @import("std");
const IpcProducer = @import("../ipc/ipc_producer.zig").IpcProducer;
const ServiceInstance = @import("service_instance.zig").ServiceInstance;
const load_balancer = @import("load_balancer.zig");
const message_header = @import("../message/message_header.zig");
const memory = @import("../memory.zig");
const constants = @import("../memory/constants.zig");

const BrokerMetadataFile = memory.BrokerMetadataFile;
const MessageHeader = message_header.MessageHeader;

pub const ServiceClient = struct {
    service_name: []const u8,
    instances: std.ArrayList(ServiceInstance),
    balancer: load_balancer.ClientLoadBalancer,

    /// IPC context — set during RingLoomEngine initialization.
    broker_meta: *BrokerMetadataFile,
    local_node_id: i16,
    local_service_id: i32,

    const Self = @This();

    pub fn init(
        allocator: std.mem.Allocator,
        service_name: []const u8,
        broker_meta: *BrokerMetadataFile,
        local_node_id: i16,
        local_service_id: i32,
    ) Self {
        return .{
            .service_name = service_name,
            .instances = std.ArrayList(ServiceInstance).init(allocator),
            .balancer = load_balancer.ClientLoadBalancer{ .round_robin = .{} },
            .broker_meta = broker_meta,
            .local_node_id = local_node_id,
            .local_service_id = local_service_id,
        };
    }

    /// Send a message to one instance of this service, selected by the
    /// load balancer.
    ///
    /// Automatically routes via the same-host direct path or the cross-host
    /// broker-routed path depending on the selected instance's node_id.
    pub fn send(self: *Self, payload: []const u8) !void {
        const instance = self.balancer.next(self.instances.items) orelse
            return error.NoAvailableInstance;

        if (instance.node_id == self.local_node_id) {
            // ── Same-host direct path ──
            // Write directly into the target service's messages ring buffer.
            // No broker involvement.
            const producer = instance.ipc_producer orelse
                return error.ProducerNotInitialized;
            try producer.write(constants.application_msg_type_id, payload);
        } else {
            // ── Cross-host routed path ──
            // Write into the broker's send ring buffer with routing header.
            try sendToRemoteService(
                self.broker_meta,
                self.local_node_id,
                self.local_service_id,
                instance.node_id,
                @intCast(instance.service_id),
                payload,
            );
        }
    }

    /// Send a message to a specific instance (bypasses load balancer).
    pub fn sendTo(self: *Self, target_service_id: i32, payload: []const u8) !void {
        const instance = self.findInstance(target_service_id) orelse
            return error.ServiceNotFound;

        if (instance.node_id == self.local_node_id) {
            const producer = instance.ipc_producer orelse
                return error.ProducerNotInitialized;
            try producer.write(constants.application_msg_type_id, payload);
        } else {
            try sendToRemoteService(
                self.broker_meta,
                self.local_node_id,
                self.local_service_id,
                instance.node_id,
                @intCast(instance.service_id),
                payload,
            );
        }
    }

    /// Send to the leader instance only. Returns error if no leader is known.
    pub fn sendToLeader(self: *Self, payload: []const u8) !void {
        for (self.instances.items) |*inst| {
            if (inst.is_leader) {
                if (inst.node_id == self.local_node_id) {
                    const producer = inst.ipc_producer orelse
                        return error.ProducerNotInitialized;
                    try producer.write(constants.application_msg_type_id, payload);
                } else {
                    try sendToRemoteService(
                        self.broker_meta,
                        self.local_node_id,
                        self.local_service_id,
                        inst.node_id,
                        @intCast(inst.service_id),
                        payload,
                    );
                }
                return;
            }
        }
        return error.NoLeaderAvailable;
    }

    // ── Instance Management ───────────────────────────────────────────

    pub fn addInstance(self: *Self, instance: ServiceInstance) !void {
        try self.instances.append(instance);
    }

    pub fn removeInstance(self: *Self, service_id: i32) void {
        var i: usize = 0;
        while (i < self.instances.items.len) {
            if (self.instances.items[i].service_id == service_id) {
                _ = self.instances.swapRemove(i);
                return;
            }
            i += 1;
        }
    }

    pub fn updateLeader(self: *Self, leader_service_id: i32) void {
        for (self.instances.items) |*inst| {
            inst.is_leader = (inst.service_id == leader_service_id);
        }
    }

    pub fn instanceCount(self: *const Self) usize {
        return self.instances.items.len;
    }

    fn findInstance(self: *const Self, service_id: i32) ?*ServiceInstance {
        for (self.instances.items) |*inst| {
            if (inst.service_id == service_id) return inst;
        }
        return null;
    }

    pub fn deinit(self: *Self) void {
        self.instances.deinit();
    }
};
```

### 7.1 Load Balancing

```zig
// src/service/load_balancer.zig

const std = @import("std");
const ServiceInstance = @import("service_instance.zig").ServiceInstance;

pub const ClientLoadBalancer = union(enum) {
    round_robin: RoundRobinBalancer,
    // Future strategies:
    // random: RandomBalancer,
    // leader_only: LeaderOnlyBalancer,
    // weighted: WeightedBalancer,

    pub fn next(self: *ClientLoadBalancer, instances: []ServiceInstance) ?*ServiceInstance {
        return switch (self.*) {
            .round_robin => |*rb| rb.next(instances),
        };
    }
};

pub const RoundRobinBalancer = struct {
    index: usize = 0,

    /// Returns the next instance in round-robin order.
    /// Uses wrapping arithmetic to avoid modulo on every call.
    pub fn next(self: *RoundRobinBalancer, instances: []ServiceInstance) ?*ServiceInstance {
        if (instances.len == 0) return null;
        const instance = &instances[self.index % instances.len];
        self.index +%= 1;
        return instance;
    }

    /// Reset the round-robin counter (e.g. after instance list changes).
    pub fn reset(self: *RoundRobinBalancer) void {
        self.index = 0;
    }
};
```

The `RoundRobinBalancer` uses wrapping addition (`+%=`) to avoid modulo overflow
concerns. The modulo in `next` is cheap because the instance count is small (typically
< 16).

### 7.2 ServiceClientRegistry

The `ServiceClientRegistry` manages all `ServiceClient` instances for a given service
process. It provides lookup by service name and handles instance updates from the
broker.

```zig
// src/service/service_client_registry.zig

const std = @import("std");
const ServiceClient = @import("service_client.zig").ServiceClient;
const ServiceInstance = @import("service_instance.zig").ServiceInstance;
const IpcProducer = @import("../ipc/ipc_producer.zig").IpcProducer;
const memory = @import("../memory.zig");

const BuffersProvider = memory.BuffersProvider;
const BrokerMetadataFile = memory.BrokerMetadataFile;

pub const ServiceClientRegistry = struct {
    clients: std.StringHashMap(*ServiceClient),
    allocator: std.mem.Allocator,
    broker_meta: *BrokerMetadataFile,
    local_node_id: i16,
    local_service_id: i32,

    const Self = @This();

    pub fn init(
        allocator: std.mem.Allocator,
        broker_meta: *BrokerMetadataFile,
        local_node_id: i16,
        local_service_id: i32,
    ) Self {
        return .{
            .clients = std.StringHashMap(*ServiceClient).init(allocator),
            .allocator = allocator,
            .broker_meta = broker_meta,
            .local_node_id = local_node_id,
            .local_service_id = local_service_id,
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
    /// Adds a new instance to the appropriate ServiceClient.
    pub fn addOrUpdateInstance(self: *Self, instance_data: struct {
        service_id: i32,
        service_name: []const u8,
        node_id: i16,
        is_leader: bool,
    }) void {
        const client = self.clients.get(instance_data.service_name) orelse return;

        // For local instances, open the target service's metadata file
        // and create an IpcProducer for direct writes.
        var ipc_producer: ?*IpcProducer = null;
        if (instance_data.node_id == self.local_node_id) {
            if (BuffersProvider.getCached(instance_data.service_id)) |provider| {
                // Allocate the IpcProducer — this is a cold-path operation.
                const producer = self.allocator.create(IpcProducer) catch return;
                producer.* = IpcProducer.init(provider.getMessagesBuffer());
                ipc_producer = producer;
            }
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

    /// Called when the broker notifies that leadership changed for a service.
    pub fn updateLeader(self: *Self, service_name: []const u8, leader_service_id: i32) void {
        if (self.clients.get(service_name)) |client| {
            client.updateLeader(leader_service_id);
        }
    }

    pub fn deinit(self: *Self) void {
        var iter = self.clients.valueIterator();
        while (iter.next()) |client| {
            client.*.deinit();
            self.allocator.destroy(client.*);
        }
        self.clients.deinit();
    }
};
```

---

## 8. ServiceInstance

A `ServiceInstance` represents one registered instance of a named service, either on
the local host or a remote host.

```zig
// src/service/service_instance.zig

const IpcProducer = @import("../ipc/ipc_producer.zig").IpcProducer;

pub const ServiceInstance = struct {
    /// Unique ID assigned by the broker at registration time.
    service_id: i32,

    /// The service's logical name (e.g. "pricing", "risk-engine").
    service_name: []const u8,

    /// The host/node this instance runs on. Combined with service_id,
    /// forms a globally unique address.
    node_id: i16,

    /// Whether this instance is the elected leader for its service name.
    /// Only meaningful when leader election is enabled for this service.
    is_leader: bool = false,

    /// Non-null only for instances on the local host.
    /// Points to an IpcProducer that writes directly into this instance's
    /// messages ring buffer. For remote instances, this is null — messages
    /// are routed via the broker's send ring buffer instead.
    ipc_producer: ?*IpcProducer = null,

    /// Returns true if this instance is on the local host.
    pub fn isLocal(self: *const ServiceInstance, local_node_id: i16) bool {
        return self.node_id == local_node_id;
    }
};
```

---

## 9. RingLoomEngine — Main Entry Point

The `RingLoomEngine` is the service's main entry point. It orchestrates the startup
sequence, owns the metadata files and agent threads, and provides the application-facing
API for creating clients and registering message handlers.

```zig
// src/service/ringloom_engine.zig

const std = @import("std");
const platform = @import("../platform.zig");
const memory = @import("../memory.zig");
const ring_buffer = @import("../concurrent/ring_buffer.zig");
const constants = @import("../memory/constants.zig");
const MessageConsumer = @import("message_consumer.zig").MessageConsumer;
const ControlAgent = @import("control_agent.zig").ControlAgent;
const ServiceClient = @import("service_client.zig").ServiceClient;
const ServiceClientRegistry = @import("service_client_registry.zig").ServiceClientRegistry;

const BrokerMetadataFile = memory.BrokerMetadataFile;
const ServiceMetadataFile = memory.ServiceMetadataFile;
const ThreadRunner = platform.ThreadRunner;
const EventLoop = platform.EventLoop;
const Clock = platform.Clock;

pub const RingLoomEngine = struct {
    config: ServiceConfig,
    allocator: std.mem.Allocator,

    // ── Metadata files ────────────────────────────────────────────────
    service_meta: *ServiceMetadataFile,
    broker_meta: *BrokerMetadataFile,

    // ── Identity ──────────────────────────────────────────────────────
    service_id: i32,
    node_id: i16,

    // ── Service discovery ─────────────────────────────────────────────
    service_registry: ServiceClientRegistry,

    // ── Agent threads ─────────────────────────────────────────────────
    message_consumer_runner: ThreadRunner,
    control_agent_runner: ThreadRunner,

    // ── State ─────────────────────────────────────────────────────────
    running: platform.AtomicBool,

    const Self = @This();

    /// Start the RingLoomEngine: create metadata, register with broker,
    /// start heartbeat, launch agent threads.
    pub fn start(allocator: std.mem.Allocator, config: ServiceConfig) !*Self {
        var engine = try allocator.create(Self);
        errdefer allocator.destroy(engine);

        // ── Step 1–4: Create metadata and register ────────────────────
        const meta = try createServiceMetadata(config);
        engine.service_meta = meta.service_meta;
        engine.broker_meta = meta.broker_meta;
        engine.service_id = meta.service_id;
        engine.node_id = meta.node_id;
        engine.config = config;
        engine.allocator = allocator;
        engine.running = platform.AtomicBool.init(true);

        // Register with the broker.
        try registerWithBroker(
            meta.broker_meta,
            meta.service_id,
            config.service_name,
            config.leader_election_enabled,
        );

        // Wait for registration response.
        _ = try waitForRegistrationResponse(meta.service_meta, 5000);

        // Write initial heartbeat.
        meta.service_meta.storeHeartbeat(Clock.epochMillis());

        // ── Step 5: Initialize service registry ───────────────────────
        engine.service_registry = ServiceClientRegistry.init(
            allocator,
            meta.broker_meta,
            meta.node_id,
            meta.service_id,
        );

        // ── Step 6: Start message consumer thread ─────────────────────
        //    Polls the service's messages ring buffer (Channel 4).
        const message_consumer = try allocator.create(MessageConsumer);
        message_consumer.* = MessageConsumer.init(meta.service_meta.getMessagesBuffer());

        engine.message_consumer_runner = ThreadRunner.init(
            "message-consumer",
            EventLoop{
                .context = message_consumer,
                .doWorkFn = MessageConsumer.doWorkFn,
                .onCloseFn = null,
            },
            .{ .blocking = {} },
        );
        try engine.message_consumer_runner.start();

        // ── Step 7: Start control agent thread ────────────────────────
        //    Polls the service's control ring buffer (Channel 2).
        //    Writes heartbeats periodically.
        const control_agent = try allocator.create(ControlAgent);
        control_agent.* = ControlAgent.init(
            meta.service_meta,
            meta.broker_meta,
            &engine.service_registry,
        );

        engine.control_agent_runner = ThreadRunner.init(
            "control-agent",
            EventLoop{
                .context = control_agent,
                .doWorkFn = ControlAgent.doWorkFn,
                .onCloseFn = null,
            },
            .{ .backoff = .{} },
        );
        try engine.control_agent_runner.start();

        return engine;
    }

    /// Graceful shutdown.
    pub fn stop(self: *Self) void {
        self.running.store(false);

        // 1. Stop the message consumer first — no new messages will be
        //    dispatched after this returns.
        self.message_consumer_runner.stopAndJoin();

        // 2. Stop the control agent.
        self.control_agent_runner.stopAndJoin();

        // 3. Send UnregisterService to the broker.
        unregisterFromBroker(self.broker_meta, self.service_id) catch {};

        // 4. Close metadata files (unmaps shared memory).
        self.service_meta.close();
        self.broker_meta.close();

        // 5. Clean up the service registry.
        self.service_registry.deinit();
    }

    // ── Application API ───────────────────────────────────────────────

    /// Create a client proxy for the named service.
    /// Sends a SubscribeToServiceUpdates request to the broker.
    /// Returns a ServiceClient that can be used to send messages.
    pub fn createClient(self: *Self, service_name: []const u8) !*ServiceClient {
        const client = try self.service_registry.getOrCreate(service_name);

        // Subscribe to updates from the broker for this service.
        try subscribeToServiceUpdates(
            self.broker_meta,
            self.service_id,
            service_name,
        );

        return client;
    }

    /// Register a message handler that will be called for incoming messages.
    pub fn setMessageHandler(
        self: *Self,
        handler: *const fn (msg_type: i32, payload: []const u8) void,
    ) void {
        const consumer: *MessageConsumer = @ptrCast(@alignCast(
            self.message_consumer_runner.event_loop.context,
        ));
        consumer.setHandler(handler);
    }
};
```

### 9.1 Shutdown Sequence

The shutdown order is critical:

1. **Stop `message_consumer_runner`** — halts the messages ring buffer polling thread.
   After this returns, no new application messages will be dispatched. This must happen
   first to avoid dispatching messages into a partially-torn-down engine.

2. **Stop `control_agent_runner`** — halts the control ring buffer polling thread and
   the heartbeat writer. After this, no more control messages are processed.

3. **Send `UnregisterService`** — a best-effort write to the broker's control ring
   buffer. If the write fails (e.g. broker is already dead), we ignore the error.

4. **Close metadata files** — calls `munmap` on the shared memory regions. After this,
   the service's metadata file remains on disk but the broker will detect the dead PID
   on the next heartbeat check.

5. **Clean up `ServiceClientRegistry`** — deallocates all `ServiceClient` instances and
   their associated `IpcProducer` objects.

---

## 10. MessageConsumer

The `MessageConsumer` is an agent that polls the service's messages ring buffer
(Channel 4) on a dedicated thread. It delegates each message to the application's
registered handler.

```zig
// src/service/message_consumer.zig

const std = @import("std");
const RingBuffer = @import("../concurrent/ring_buffer.zig").RingBuffer;
const constants = @import("../memory/constants.zig");

/// Default number of messages to read per poll cycle.
const read_limit: u32 = 256;

pub const MessageConsumer = struct {
    ring_buffer: *RingBuffer,
    handler: ?*const fn (msg_type: i32, payload: []const u8) void,

    const Self = @This();

    pub fn init(messages_buffer: []u8) Self {
        return .{
            .ring_buffer = RingBuffer.init(messages_buffer),
            .handler = null,
        };
    }

    pub fn setHandler(self: *Self, handler: *const fn (msg_type: i32, payload: []const u8) void) void {
        self.handler = handler;
    }

    /// Duty-cycle function. Called by the ThreadRunner's event loop.
    /// Returns the number of messages processed (work count).
    ///
    /// If work count > 0, the idle strategy resets and the loop runs again
    /// immediately. If work count == 0, the idle strategy engages
    /// (spin → yield → park, depending on configuration).
    pub fn doWork(self: *Self) u32 {
        const h = self.handler orelse return 0;
        return self.ring_buffer.read(h, read_limit);
    }

    /// EventLoop-compatible function pointer (casts context to *Self).
    pub fn doWorkFn(ctx: *anyopaque) u32 {
        const self: *Self = @ptrCast(@alignCast(ctx));
        return self.doWork();
    }
};
```

The `MessageConsumer` is intentionally simple — it is a thin adapter between the ring
buffer's `read` function and the `EventLoop` interface. All message dispatch logic
(template ID routing, typed deserialization) happens in the handler, not here.

---

## 11. ControlAgent

The `ControlAgent` polls the service's control ring buffer (Channel 2) for messages
from the broker, and periodically writes heartbeats.

```zig
// src/service/control_agent.zig

const std = @import("std");
const platform = @import("../platform.zig");
const memory = @import("../memory.zig");
const ring_buffer = @import("../concurrent/ring_buffer.zig");
const constants = @import("../memory/constants.zig");
const control_encoding = @import("../message/control_encoding.zig");
const ServiceClientRegistry = @import("service_client_registry.zig").ServiceClientRegistry;

const RingBuffer = ring_buffer.RingBuffer;
const ServiceMetadataFile = memory.ServiceMetadataFile;
const BrokerMetadataFile = memory.BrokerMetadataFile;
const Clock = platform.Clock;

pub const ControlAgent = struct {
    control_rb: *RingBuffer,
    service_meta: *ServiceMetadataFile,
    broker_meta: *BrokerMetadataFile,
    service_registry: *ServiceClientRegistry,
    last_heartbeat_ns: u64,

    const Self = @This();

    pub fn init(
        service_meta: *ServiceMetadataFile,
        broker_meta: *BrokerMetadataFile,
        service_registry: *ServiceClientRegistry,
    ) Self {
        return .{
            .control_rb = RingBuffer.init(service_meta.getControlBuffer()),
            .service_meta = service_meta,
            .broker_meta = broker_meta,
            .service_registry = service_registry,
            .last_heartbeat_ns = Clock.monotonicNanos(),
        };
    }

    /// Duty-cycle function. Returns total work count.
    pub fn doWork(self: *Self) u32 {
        var work_count: u32 = 0;

        // 1. Poll control ring buffer for broker messages.
        work_count += self.pollControlMessages();

        // 2. Write heartbeat if interval has elapsed.
        if (self.shouldWriteHeartbeat()) {
            self.service_meta.storeHeartbeat(Clock.epochMillis());
            self.last_heartbeat_ns = Clock.monotonicNanos();
            work_count += 1;
        }

        return work_count;
    }

    /// EventLoop-compatible function pointer.
    pub fn doWorkFn(ctx: *anyopaque) u32 {
        const self: *Self = @ptrCast(@alignCast(ctx));
        return self.doWork();
    }

    fn pollControlMessages(self: *Self) u32 {
        // Read up to CONTROL_READ_LIMIT messages per cycle.
        return self.control_rb.read(struct {
            agent: *Self,

            pub fn onMessage(handler: *@This(), _: i32, payload: []const u8) void {
                handler.agent.dispatchControlMessage(payload);
            }
        }{ .agent = self }, constants.control_read_limit);
    }

    fn dispatchControlMessage(self: *Self, payload: []const u8) void {
        const template_id = control_encoding.readTemplateId(payload);

        switch (template_id) {
            TemplateId.registration_response => {
                // Late registration response — can happen if the initial
                // wait timed out and the broker was slow. Log and ignore.
            },

            TemplateId.service_instances => {
                // Broker is sending us the current set of instances for
                // a service we subscribed to.
                handleServiceInstances(self.service_registry, payload);
            },

            TemplateId.leader_changed => {
                // Leadership changed for a service we're subscribed to.
                const leader_info = control_encoding.decodeLeaderChanged(payload);
                self.service_registry.updateLeader(
                    leader_info.service_name,
                    leader_info.leader_service_id,
                );
            },

            else => {
                // Unknown template ID — ignore. Forward compatibility:
                // newer brokers may send messages this version doesn't
                // understand.
            },
        }
    }

    fn shouldWriteHeartbeat(self: *const Self) bool {
        const now = Clock.monotonicNanos();
        return (now - self.last_heartbeat_ns) >=
            constants.service_heartbeat_write_interval_ms * std.time.ns_per_ms;
    }
};
```

### 11.1 Threading Model Summary

Each service (RingLoomEngine) runs two agent threads:

| Thread Name | Agent | Ring Buffer | Direction | Idle Strategy |
|------------|-------|-------------|-----------|---------------|
| `message-consumer` | `MessageConsumer` | Service's messages RB (Channel 4) | Producers → Service | `blocking` (kernel park via futex/ulock when no messages) |
| `control-agent` | `ControlAgent` | Service's control RB (Channel 2) | Broker → Service | `backoff` (spin → yield → sleep — control messages are infrequent) |

The `message-consumer` thread uses the blocking idle strategy because application
messages are latency-sensitive and the ring buffer supports kernel-level
wake (doc 01, §6). The `control-agent` thread uses backoff because control messages
are infrequent and sub-millisecond latency is not required.

---

## 12. Message Header — Routing Fields

Every application message written to a ring buffer carries a RingLoom message header as a
prefix. This header contains the routing information needed by the broker (for cross-host
messages) and by the service (for dispatching).

```zig
// src/message/message_header.zig

const std = @import("std");

/// RingLoom message header — fixed-size prefix on every application message.
///
/// Layout (28 bytes total):
///   +0:   correlation_id       (i64)
///   +8:   source_node_id       (i16)
///   +10:  source_service_id    (i16)
///   +12:  target_node_id       (i16)
///   +14:  target_service_id    (i16)
///   +16:  template_id          (u16)
///   +18:  flags                (u8)
///   +19:  reserved             (1 byte)
///   +20:  payload_length       (i32)
///   +24:  reserved             (4 bytes, alignment padding)
///
/// Total: 28 bytes, aligned to 4 bytes.
pub const MessageHeader = extern struct {
    correlation_id: i64,
    source_node_id: i16,
    source_service_id: i16,
    target_node_id: i16,
    target_service_id: i16,
    template_id: u16,
    flags: u8,
    _reserved0: u8 = 0,
    payload_length: i32,
    _reserved1: [4]u8 = [_]u8{0} ** 4,

    pub const encoded_length: usize = @sizeOf(MessageHeader);

    comptime {
        std.debug.assert(@sizeOf(MessageHeader) == 28);
    }

    /// Flyweight encode — writes the header directly into the buffer.
    pub fn encode(dest: *[encoded_length]u8, fields: struct {
        source_node_id: i16,
        source_service_id: i16,
        target_node_id: i16,
        target_service_id: i16,
        template_id: u16,
        correlation_id: i64 = 0,
        flags: u8 = 0,
    }) void {
        const header: *MessageHeader = @ptrCast(@alignCast(dest));
        header.* = .{
            .correlation_id = fields.correlation_id,
            .source_node_id = fields.source_node_id,
            .source_service_id = fields.source_service_id,
            .target_node_id = fields.target_node_id,
            .target_service_id = fields.target_service_id,
            .template_id = fields.template_id,
            .flags = fields.flags,
            .payload_length = 0, // set by caller after writing payload
        };
    }

    /// Flyweight decode — overlays a read-only header onto the buffer.
    pub fn decode(src: *const [encoded_length]u8) *const MessageHeader {
        return @ptrCast(@alignCast(src));
    }
};
```

### 12.1 Routing Decisions

The broker uses the header fields to make routing decisions:

| Condition | Action |
|-----------|--------|
| `targetNodeId == localNodeId` | Deliver to local service via `ServiceMetadataFile.getMessagesBuffer()` |
| `targetNodeId != localNodeId` | Forward via TCP to the remote broker |
| `targetServiceId == 0` | Message is for the broker itself (control/admin) |
| `flags & FLAG_ADMIN != 0` | Broker-to-broker admin message (cluster protocol) |

---

## 13. Message Fragmentation and Reassembly

Messages larger than a single ring buffer record are fragmented by a
`MessageFragmentingProducer` and reassembled by a `MessageAssembler` on the consumer
side. This is transparent to the application.

### 13.1 Fragmentation (Producer Side)

```zig
// src/message/message_fragmenting_producer.zig

const std = @import("std");
const IpcProducer = @import("../ipc/ipc_producer.zig").IpcProducer;
const message_header = @import("message_header.zig");
const constants = @import("../memory/constants.zig");

pub const MessageFragmentingProducer = struct {
    producer: *IpcProducer,
    max_payload_per_fragment: usize,

    const Self = @This();

    pub fn init(producer: *IpcProducer, max_message_length: usize) Self {
        // Reserve space for the ring buffer record header and RingLoom message header.
        const overhead = constants.ring_buffer_record_header_length +
            message_header.MessageHeader.encoded_length;
        return .{
            .producer = producer,
            .max_payload_per_fragment = max_message_length - overhead,
        };
    }

    /// Send a message, automatically fragmenting if it exceeds the
    /// maximum payload size per ring buffer record.
    pub fn send(self: *Self, msg_type: i32, payload: []const u8) !void {
        if (payload.len <= self.max_payload_per_fragment) {
            // Fits in a single record — no fragmentation needed.
            return self.producer.write(msg_type, payload);
        }

        // Fragment the message.
        var offset: usize = 0;
        var fragment_index: u32 = 0;
        const total_fragments = (payload.len + self.max_payload_per_fragment - 1) /
            self.max_payload_per_fragment;

        while (offset < payload.len) {
            const remaining = payload.len - offset;
            const chunk_len = @min(remaining, self.max_payload_per_fragment);

            // Set fragment flags:
            //   First fragment:  FLAG_BEGIN
            //   Last fragment:   FLAG_END
            //   Middle fragment: 0x00
            //   Unfragmented:    FLAG_BEGIN | FLAG_END (handled above)
            var flags: u8 = 0;
            if (fragment_index == 0) flags |= constants.flag_begin;
            if (fragment_index == total_fragments - 1) flags |= constants.flag_end;

            try self.sendFragment(msg_type, payload[offset..][0..chunk_len], flags);

            offset += chunk_len;
            fragment_index += 1;
        }
    }

    fn sendFragment(self: *Self, msg_type: i32, chunk: []const u8, flags: u8) !void {
        _ = msg_type;
        _ = flags;
        // Write chunk with fragment header into the ring buffer.
        // (Full implementation deferred to the message encoding module.)
        try self.producer.write(constants.application_msg_type_id, chunk);
    }
};
```

### 13.2 Reassembly (Consumer Side)

```zig
// src/message/message_assembler.zig

const std = @import("std");
const constants = @import("../memory/constants.zig");

/// Reassembles fragmented messages on the consumer side.
///
/// Uses a pre-allocated growable buffer. When the first fragment (FLAG_BEGIN)
/// arrives, the assembler starts accumulating. When the last fragment (FLAG_END)
/// arrives, the complete message is dispatched to the application handler.
pub const MessageAssembler = struct {
    buffer: []u8,
    buffer_len: usize,
    capacity: usize,
    in_progress: bool,

    const Self = @This();

    pub fn init(allocator: std.mem.Allocator, max_message_size: usize) !Self {
        const buf = try allocator.alloc(u8, max_message_size);
        return .{
            .buffer = buf,
            .buffer_len = 0,
            .capacity = max_message_size,
            .in_progress = false,
        };
    }

    /// Process a fragment. Returns the complete message when all fragments
    /// have been received, or null if more fragments are expected.
    pub fn onFragment(self: *Self, flags: u8, data: []const u8) ?[]const u8 {
        const is_begin = (flags & constants.flag_begin) != 0;
        const is_end = (flags & constants.flag_end) != 0;

        if (is_begin and is_end) {
            // Unfragmented message — return directly.
            return data;
        }

        if (is_begin) {
            // Start of a new fragmented message.
            self.buffer_len = 0;
            self.in_progress = true;
        }

        if (!self.in_progress) return null;

        // Append this fragment's data to the assembly buffer.
        if (self.buffer_len + data.len > self.capacity) {
            // Message exceeds maximum size — discard.
            self.in_progress = false;
            self.buffer_len = 0;
            return null;
        }

        @memcpy(self.buffer[self.buffer_len..][0..data.len], data);
        self.buffer_len += data.len;

        if (is_end) {
            // All fragments received — return the complete message.
            self.in_progress = false;
            return self.buffer[0..self.buffer_len];
        }

        return null; // More fragments expected.
    }

    pub fn deinit(self: *Self, allocator: std.mem.Allocator) void {
        allocator.free(self.buffer);
    }
};
```

---

## 14. Testing

### 14.1 Unit Test: IpcProducer Write + IpcConsumer Poll Roundtrip

```zig
// src/ipc/ipc_test.zig

const std = @import("std");
const testing = std.testing;
const IpcProducer = @import("ipc_producer.zig").IpcProducer;
const IpcConsumer = @import("ipc_consumer.zig").IpcConsumer;
const RingBuffer = @import("../concurrent/ring_buffer.zig").RingBuffer;
const constants = @import("../memory/constants.zig");

test "IpcProducer write and IpcConsumer poll roundtrip" {
    // Given: a shared buffer simulating a service's messages ring buffer.
    const capacity = 4096;
    var backing: [capacity + constants.ring_buffer_trailer_length]u8 = undefined;
    @memset(&backing, 0);

    var producer = IpcProducer.init(&backing);
    var consumer = IpcConsumer.init(&backing);

    // When: a message is written via the producer.
    const payload = "hello from service-a";
    try producer.write(42, payload);

    // Then: the consumer reads it back with the correct type and content.
    var received_type: i32 = 0;
    var received_payload: []const u8 = &.{};

    const count = consumer.poll(struct {
        pub fn handler(msg_type: i32, buffer: []const u8) void {
            received_type = msg_type;
            received_payload = buffer;
        }
    }.handler, 10);

    try testing.expectEqual(@as(u32, 1), count);
    try testing.expectEqual(@as(i32, 42), received_type);
    try testing.expectEqualStrings(payload, received_payload);
}

test "IpcProducer write returns error when buffer is full" {
    // Given: a minimal ring buffer.
    const capacity = 256;
    var backing: [capacity + constants.ring_buffer_trailer_length]u8 = undefined;
    @memset(&backing, 0);

    var producer = IpcProducer.init(&backing);

    // When: we write until the buffer is full.
    var large_payload: [200]u8 = undefined;
    @memset(&large_payload, 0xAA);

    // First write should succeed.
    try producer.write(1, &large_payload);

    // Then: subsequent write should fail — buffer is full.
    const result = producer.write(1, &large_payload);
    try testing.expectError(error.InsufficientCapacity, result);
}
```

### 14.2 Unit Test: ServiceClient Round-Robin Load Balancing

```zig
// src/service/service_client_test.zig

const std = @import("std");
const testing = std.testing;
const ServiceClient = @import("service_client.zig").ServiceClient;
const ServiceInstance = @import("service_instance.zig").ServiceInstance;

test "round-robin load balancer cycles through instances" {
    // Given: a ServiceClient with 3 instances.
    var client = ServiceClient.init(
        testing.allocator,
        "test-service",
        undefined, // broker_meta (not needed for this test)
        1,         // local_node_id
        100,       // local_service_id
    );
    defer client.deinit();

    try client.addInstance(.{ .service_id = 1, .service_name = "test-service", .node_id = 1 });
    try client.addInstance(.{ .service_id = 2, .service_name = "test-service", .node_id = 1 });
    try client.addInstance(.{ .service_id = 3, .service_name = "test-service", .node_id = 1 });

    // When/Then: next() cycles through instances in order.
    const first = client.balancer.next(client.instances.items);
    try testing.expect(first != null);
    try testing.expectEqual(@as(i32, 1), first.?.service_id);

    const second = client.balancer.next(client.instances.items);
    try testing.expectEqual(@as(i32, 2), second.?.service_id);

    const third = client.balancer.next(client.instances.items);
    try testing.expectEqual(@as(i32, 3), third.?.service_id);

    // Wraps around.
    const fourth = client.balancer.next(client.instances.items);
    try testing.expectEqual(@as(i32, 1), fourth.?.service_id);
}

test "round-robin returns null for empty instance list" {
    // Given: a ServiceClient with no instances.
    var client = ServiceClient.init(
        testing.allocator,
        "empty-service",
        undefined,
        1,
        100,
    );
    defer client.deinit();

    // When/Then: next() returns null.
    const result = client.balancer.next(client.instances.items);
    try testing.expect(result == null);
}

test "ServiceClient updateLeader sets correct instance" {
    // Given: a ServiceClient with 3 instances, none are leader.
    var client = ServiceClient.init(
        testing.allocator,
        "leader-test",
        undefined,
        1,
        100,
    );
    defer client.deinit();

    try client.addInstance(.{ .service_id = 1, .service_name = "leader-test", .node_id = 1 });
    try client.addInstance(.{ .service_id = 2, .service_name = "leader-test", .node_id = 1 });
    try client.addInstance(.{ .service_id = 3, .service_name = "leader-test", .node_id = 2 });

    // When: leader is updated to instance 2.
    client.updateLeader(2);

    // Then: only instance 2 is leader.
    try testing.expect(!client.instances.items[0].is_leader);
    try testing.expect(client.instances.items[1].is_leader);
    try testing.expect(!client.instances.items[2].is_leader);
}
```

### 14.3 Integration Test: Two Services on Same Host (Direct IPC)

```zig
// src/ipc/ipc_integration_test.zig

const std = @import("std");
const testing = std.testing;
const memory = @import("../memory.zig");
const IpcProducer = @import("ipc_producer.zig").IpcProducer;
const IpcConsumer = @import("ipc_consumer.zig").IpcConsumer;
const constants = @import("../memory/constants.zig");

test "two services exchange messages via direct IPC" {
    // Given: two service metadata files on /dev/shm (or test tmpdir).
    //        Service A will write to Service B's messages ring buffer.

    const service_b = try memory.ServiceMetadataFile.create(.{
        .storage_path = "/tmp/ringloom-test",
        .group = "test",
        .service_name = "service-b",
        .service_id = 2,
        .node_id = 1,
        .control_buffer_length = 4096,
        .messages_buffer_length = 16384,
    });
    defer service_b.close();

    // Service A creates a producer pointing at Service B's messages buffer.
    var producer = IpcProducer.init(service_b.getMessagesBuffer());

    // Service B creates a consumer on its own messages buffer.
    var consumer = IpcConsumer.init(service_b.getMessagesBuffer());

    // When: Service A sends a message.
    const payload = "cross-service IPC message";
    try producer.write(constants.application_msg_type_id, payload);

    // Then: Service B receives it.
    var received: bool = false;
    _ = consumer.poll(struct {
        pub fn handler(_: i32, buffer: []const u8) void {
            testing.expectEqualStrings("cross-service IPC message", buffer) catch {};
            received = true;
        }
    }.handler, 10);

    try testing.expect(received);
}
```

### 14.4 Integration Test: Service Registration Flow

```zig
// src/service/registration_integration_test.zig

const std = @import("std");
const testing = std.testing;
const memory = @import("../memory.zig");
const ring_buffer = @import("../concurrent/ring_buffer.zig");
const control_encoding = @import("../message/control_encoding.zig");
const constants = @import("../memory/constants.zig");

test "service registration: register → response → subscribe → instances" {
    // Given: a broker metadata file.
    const broker_meta = try memory.BrokerMetadataFile.create(.{
        .storage_path = "/tmp/ringloom-test",
        .group = "test",
        .node_id = 1,
    });
    defer broker_meta.close();

    // When: a service registers.
    const service_id = broker_meta.incrementAndGetNextServiceId();
    try testing.expectEqual(@as(i32, 1), service_id);

    // The service writes a RegisterService message to the broker's control RB.
    var control_rb = ring_buffer.RingBuffer.init(broker_meta.getControlBuffer());
    var msg_buf: [256]u8 = undefined;
    const msg_len = control_encoding.encodeRegisterService(&msg_buf, .{
        .service_id = service_id,
        .service_name = "test-service",
        .leader_election_enabled = false,
    });
    try control_rb.write(constants.control_msg_type_id, msg_buf[0..msg_len]);

    // Then: the broker can read the registration message.
    var decoded_service_id: i32 = 0;
    const count = control_rb.read(struct {
        pub fn handler(_: i32, payload: []const u8) void {
            const reg = control_encoding.decodeRegisterService(payload);
            decoded_service_id = reg.service_id;
        }
    }.handler, 1);

    try testing.expectEqual(@as(u32, 1), count);
    try testing.expectEqual(service_id, decoded_service_id);
}
```

### 14.5 Integration Test: Cross-Host Message Path

```zig
// src/service/cross_host_integration_test.zig

const std = @import("std");
const testing = std.testing;
const memory = @import("../memory.zig");
const ring_buffer = @import("../concurrent/ring_buffer.zig");
const message_header = @import("../message/message_header.zig");
const constants = @import("../memory/constants.zig");

const MessageHeader = message_header.MessageHeader;

test "cross-host message written to send RB contains routing header" {
    // Given: a broker metadata file with a send ring buffer.
    const broker_meta = try memory.BrokerMetadataFile.create(.{
        .storage_path = "/tmp/ringloom-test",
        .group = "test",
        .node_id = 1,
    });
    defer broker_meta.close();

    // When: a service writes a cross-host message.
    var send_rb = ring_buffer.RingBuffer.init(broker_meta.getSendBuffer());
    const payload = "remote-bound payload";
    const total_len = MessageHeader.encoded_length + payload.len;

    var msg_buf: [512]u8 = undefined;
    MessageHeader.encode(msg_buf[0..MessageHeader.encoded_length], .{
        .source_node_id = 1,
        .source_service_id = 5,
        .target_node_id = 2,
        .target_service_id = 10,
        .template_id = 0,
    });
    @memcpy(msg_buf[MessageHeader.encoded_length..][0..payload.len], payload);

    try send_rb.write(constants.application_msg_type_id, msg_buf[0..total_len]);

    // Then: the broker reads the message and can extract routing info.
    var decoded_header: ?*const MessageHeader = null;
    var decoded_payload: []const u8 = &.{};

    _ = send_rb.read(struct {
        pub fn handler(_: i32, buffer: []const u8) void {
            decoded_header = MessageHeader.decode(
                buffer[0..MessageHeader.encoded_length],
            );
            decoded_payload = buffer[MessageHeader.encoded_length..];
        }
    }.handler, 1);

    try testing.expect(decoded_header != null);
    try testing.expectEqual(@as(i16, 1), decoded_header.?.source_node_id);
    try testing.expectEqual(@as(i16, 2), decoded_header.?.target_node_id);
    try testing.expectEqual(@as(i16, 10), decoded_header.?.target_service_id);
    try testing.expectEqualStrings(payload, decoded_payload);
}
```

### 14.6 Unit Test: Message Assembler

```zig
// src/message/message_assembler_test.zig

const std = @import("std");
const testing = std.testing;
const MessageAssembler = @import("message_assembler.zig").MessageAssembler;
const constants = @import("../memory/constants.zig");

test "unfragmented message passes through directly" {
    // Given: an assembler.
    var assembler = try MessageAssembler.init(testing.allocator, 4096);
    defer assembler.deinit(testing.allocator);

    // When: an unfragmented message arrives (BEGIN | END).
    const flags = constants.flag_begin | constants.flag_end;
    const result = assembler.onFragment(flags, "complete message");

    // Then: it's returned immediately without copying.
    try testing.expect(result != null);
    try testing.expectEqualStrings("complete message", result.?);
}

test "fragmented message is reassembled" {
    // Given: an assembler.
    var assembler = try MessageAssembler.init(testing.allocator, 4096);
    defer assembler.deinit(testing.allocator);

    // When: three fragments arrive.
    const result1 = assembler.onFragment(constants.flag_begin, "hello ");
    try testing.expect(result1 == null); // not complete yet

    const result2 = assembler.onFragment(0, "beautiful ");
    try testing.expect(result2 == null); // not complete yet

    const result3 = assembler.onFragment(constants.flag_end, "world");
    try testing.expect(result3 != null); // complete!

    // Then: the reassembled message is correct.
    try testing.expectEqualStrings("hello beautiful world", result3.?);
}

test "oversized fragment is discarded" {
    // Given: an assembler with a tiny capacity.
    var assembler = try MessageAssembler.init(testing.allocator, 16);
    defer assembler.deinit(testing.allocator);

    // When: fragments exceed capacity.
    _ = assembler.onFragment(constants.flag_begin, "12345678901234567");

    // Then: the fragment is discarded.
    const result = assembler.onFragment(constants.flag_end, "overflow");
    try testing.expect(result == null);
}
```

---

## 15. File Structure

```
src/
  ipc/
    ipc_producer.zig               # IpcProducer — writes to target's messages RB
    ipc_consumer.zig               # IpcConsumer — polls own messages RB
    ipc_test.zig                   # Unit tests: write + poll roundtrip
    ipc_integration_test.zig       # Integration: two services, direct IPC
  service/
    ringloom_engine.zig                 # RingLoomEngine — main entry point, start/stop
    message_consumer.zig           # MessageConsumer — messages RB polling agent
    control_agent.zig              # ControlAgent — control RB polling + heartbeat
    service_client.zig             # ServiceClient — client proxy with load balancing
    service_client_registry.zig    # ServiceClientRegistry — manages all ServiceClients
    service_instance.zig           # ServiceInstance — record for one service instance
    load_balancer.zig              # ClientLoadBalancer — round-robin (extensible)
    service_client_test.zig        # Unit tests: load balancing, leader tracking
    registration_integration_test.zig   # Integration: registration flow
    cross_host_integration_test.zig     # Integration: cross-host send RB routing
  message/
    message_header.zig             # MessageHeader — RingLoom routing header (28 bytes)
    message_fragmenting_producer.zig    # Fragments large messages across records
    message_assembler.zig          # Reassembles fragments on consumer side
    control_encoding.zig           # Encode/decode control messages (register, etc.)
    message_assembler_test.zig     # Unit tests: fragmentation + reassembly
```

### Module Dependency Graph

```
ringloom_engine.zig
  ├── message_consumer.zig ─── ipc_consumer.zig ─── ring_buffer.zig (doc 03)
  ├── control_agent.zig ─────── ring_buffer.zig (doc 03)
  │                           └── control_encoding.zig
  ├── service_client_registry.zig
  │     └── service_client.zig
  │           ├── ipc_producer.zig ─── ring_buffer.zig (doc 03)
  │           ├── load_balancer.zig
  │           └── service_instance.zig
  ├── message_header.zig
  ├── broker_metadata.zig (doc 02)
  └── service_metadata.zig (doc 02)
```

---

## Appendix: Memory Ordering Quick Reference

All atomic operations in the IPC subsystem follow the ordering specified in doc 02.
The table below covers the IPC-specific fields:

| Field | Writer | Reader | Ordering |
|-------|--------|--------|----------|
| Ring buffer `tail_position` | Producer (CAS) | Consumer | `acq_rel` / `acquire` |
| Ring buffer `head_position` | Consumer | Producer | `release` / `acquire` |
| Record header `length` | Producer (commit) | Consumer | `release` / `acquire` |
| `heartbeat_time_ms` | Service (periodic) | Broker (health check) | `release` / `acquire` |
| `writer_wait_state` (blocking) | Producer (park) | Consumer (wake) | `release` / `acquire` + futex |
| `reader_wait_state` (blocking) | Consumer (park) | Producer (wake) | `release` / `acquire` + futex |

On x86-64, `acquire` loads and `release` stores compile to plain `mov` instructions
plus a compiler fence (x86-64's TSO provides acquire/release semantics natively). The
CAS on `tail_position` compiles to `lock cmpxchg`. On ARM64, acquire loads emit
`ldapr` and release stores emit `stlr`.

---

*Previous: [06 — Receive Path](06-receive-path.md)*
*Next: [09 — Control Plane](09-control-plane.md)*
