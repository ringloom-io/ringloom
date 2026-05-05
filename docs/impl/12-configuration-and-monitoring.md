# 12 — Configuration & Monitoring

> **Prerequisites:** All previous documents — this is the final implementation step.
> In particular: [01 — Platform Abstraction](01-platform-abstraction.md) (constants,
> clocks), [02 — Memory Layout & Shared Memory](02-memory-layout-and-shared-memory.md)
> (metadata files, buffer regions), [03 — Concurrent Data Structures](03-concurrent-data-structures.md)
> (`CountersManager`, `ErrorLog`, `ErrorState`),
> [10 — Threading Model](10-threading-model.md) (event loops, idle strategies, threading
> modes), [11 — Cluster Management](11-cluster-management.md) (peer endpoints, node IDs).

This document ties together the configuration, monitoring, and error handling subsystems.
Configuration is loaded once at startup and threaded through every component as immutable
state. Counters and the error log (defined in doc 03) are wired into the live system with
well-known counter IDs, cycle-time tracking, and a monitoring snapshot interface for
external tools. Hot-path error handling follows the zero-allocation, no-exception patterns
established throughout the codebase.

All code targets **Zig 0.16.x** stable.

---

## Table of Contents

1.  [Overview](#1-overview)
2.  [Configuration](#2-configuration)
    1.  [Broker Configuration](#21-broker-configuration)
    2.  [BrokerConfig Struct](#22-brokerconfig-struct)
    3.  [PeerEndpoint](#23-peerendpoint)
    4.  [Config Loading](#24-config-loading)
    5.  [Properties File Parser](#25-properties-file-parser)
    6.  [Environment Variable Override](#26-environment-variable-override)
    7.  [Buffer Sizing Validation](#27-buffer-sizing-validation)
    8.  [Service Configuration](#28-service-configuration)
    9.  [ThreadingMode & IdleStrategy Selection](#29-threadingmode--idlestrategy-selection)
3.  [System Counters](#3-system-counters)
    1.  [Counter IDs](#31-counter-ids)
    2.  [Counter Wiring](#32-counter-wiring)
    3.  [Cycle Time Tracking](#33-cycle-time-tracking)
    4.  [Counter Snapshot](#34-counter-snapshot)
    5.  [Current Implementation Review](#35-current-implementation-review)
    6.  [Required Counter Coverage](#36-required-counter-coverage)
    7.  [Metadata Counter Regions](#37-metadata-counter-regions)
    8.  [Derived Ring Gauges](#38-derived-ring-gauges)
4.  [Error Log Wiring](#4-error-log-wiring)
    1.  [Error Categories](#41-error-categories)
    2.  [Error Recording Patterns](#42-error-recording-patterns)
    3.  [Error Log Iteration](#43-error-log-iteration)
5.  [Thread-Local Error State Wiring](#5-thread-local-error-state-wiring)
6.  [Hot-Path Error Handling Patterns](#6-hot-path-error-handling-patterns)
    1.  [Pattern 1: Return Value + Counter](#61-pattern-1-return-value--counter)
    2.  [Pattern 2: Fallthrough + Counter + Error Log](#62-pattern-2-fallthrough--counter--error-log)
    3.  [Pattern 3: Thread-Local State for Callbacks](#63-pattern-3-thread-local-state-for-callbacks)
    4.  [Anti-Patterns (Never Do This)](#64-anti-patterns-never-do-this)
7.  [Monitoring Interface](#7-monitoring-interface)
    1.  [Shared-Memory Monitoring (Primary)](#71-shared-memory-monitoring-primary)
    2.  [MonitoringSnapshot](#72-monitoringsnapshot)
    3.  [Periodic Stderr Dump (Optional)](#73-periodic-stderr-dump-optional)
    4.  [External Monitoring Tool](#74-external-monitoring-tool)
    5.  [Prometheus Observability Process](#75-prometheus-observability-process)
8.  [Broker Startup Wiring — Putting It All Together](#8-broker-startup-wiring--putting-it-all-together)
9.  [Constants Reference](#9-constants-reference)
    1.  [Buffer Size Constants](#91-buffer-size-constants)
    2.  [Protocol Constants](#92-protocol-constants)
    3.  [Timing Constants](#93-timing-constants)
    4.  [Memory Ordering Summary](#94-memory-ordering-summary)
    5.  [Default Configuration Values](#95-default-configuration-values)
10. [Testing](#10-testing)
    1.  [Config Loading Tests](#101-config-loading-tests)
    2.  [Config Validation Tests](#102-config-validation-tests)
    3.  [System Counter Tests](#103-system-counter-tests)
    4.  [Error Log Integration Tests](#104-error-log-integration-tests)
    5.  [Thread-Local Error State Tests](#105-thread-local-error-state-tests)
    6.  [Multi-Threaded Counter Tests](#106-multi-threaded-counter-tests)
    7.  [Monitoring Snapshot Tests](#107-monitoring-snapshot-tests)
    8.  [Testing Tips](#108-testing-tips)
11. [File Structure](#11-file-structure)
12. [Implementation Steps](#12-implementation-steps)

---

## 1. Overview

This document covers five subsystems that span the entire broker:

| Subsystem | Hot Path? | Allocates? | Purpose |
|---|---|---|---|
| **Configuration** | No — startup only | Yes (arena allocator) | Load, parse, validate, and distribute settings |
| **System counters** | Yes — every message | No (atomic add to pre-mapped memory) | Track bytes, messages, back-pressure events, cycle times |
| **Error log** | Rare — only on errors | No (writes into pre-allocated buffer) | Deduplicated error observations with timestamps |
| **Thread-local error state** | Rare — only on errors | No (stack-local fixed buffer) | Rich per-thread diagnostic context |
| **Monitoring interface** | No — external reader | Read-only (mmap of existing buffers) | Expose counters + error log to external tools |

The key architectural invariant: **configuration is immutable after startup.** Every
component receives a `*const BrokerConfig` (or `*const ServiceConfig`) and never
modifies it. This eliminates the need for synchronization on the config path and makes
the hot path unconditionally safe.

---

## 2. Configuration

### 2.1 Broker Configuration

Configuration is loaded from a Java-style properties file at startup. The file path is
determined by:

1. The `RINGLOOM_CONFIG_FILE` environment variable, if set.
2. The default path: `broker.properties` in the current working directory.

All properties follow a `broker.` prefix convention. Required properties must be present
or startup fails immediately with a descriptive error.

| Property | Type | Default | Required | Description |
|---|---|---|---|---|
| `broker.node.id` | `u8` | — | **Yes** | Unique node ID for this broker (0–255) |
| `broker.local.host.port` | `host:port` | — | **Yes** | This broker's TCP listen address |
| `broker.member.host.ports` | `id@host:port,...` | (empty) | No | Comma-separated peer list: `1@10.0.0.2:9100,2@10.0.0.3:9100` |
| `broker.group.name` | `[]const u8` | `"ringloom"` | No | Group name (directory prefix under storage path) |
| `broker.storage.path` | `[]const u8` | `/dev/shm` | No | Base path for metadata files |
| `broker.control.buffer.size` | `u32` | `65536` | No | Control ring buffer capacity (bytes, power of 2) |
| `broker.messages.buffer.size` | `u32` | `1048576` | No | Send ring buffer capacity (bytes, power of 2) |
| `broker.peer.write.queue.capacity` | `u32` | `8192` | No | Per-peer outbound write queue capacity (frames) |
| `broker.max.frame.length` | `u32` | `1048576` | No | Maximum TCP frame length (bytes, default 1 MB) |
| `broker.threading.mode` | enum | `DEDICATED` | No | `DEDICATED`, `SHARED_NETWORK`, or `SHARED` |
| `broker.idle.strategy` | enum | `backoff` | No | `busy_spin`, `yielding`, `sleeping`, `backoff`, `blocking` |
| `broker.counter.values.buffer.size` | `u32` | `65536` | No | Counter values buffer (bytes, power of 2) |
| `broker.error.log.buffer.size` | `u32` | `262144` | No | Error log buffer (bytes) |
| `broker.max.services` | `u16` | `256` | No | Maximum concurrent services |
| `broker.max.peers` | `u8` | `16` | No | Maximum peer brokers |
| `broker.tcp.sndbuf.size` | `u32` | `262144` | No | TCP SO_SNDBUF size (bytes, default 256 KB) |
| `broker.tcp.rcvbuf.size` | `u32` | `262144` | No | TCP SO_RCVBUF size (bytes, default 256 KB) |
| `broker.tcp.listen.backlog` | `u32` | `128` | No | TCP listen backlog |
| `broker.heartbeat.interval.ms` | `u32` | `500` | No | Heartbeat send interval (ms) |
| `broker.heartbeat.timeout.ms` | `u32` | `2000` | No | Heartbeat receive timeout (ms) |
| `broker.reconnect.base.delay.ms` | `u32` | `100` | No | Reconnect initial backoff (ms) |
| `broker.reconnect.max.delay.ms` | `u32` | `1000` | No | Reconnect max backoff (ms) |
| `broker.io.uring.queue.depth` | `u32` | `256` | No | io_uring SQ depth (Linux only) |
| `broker.io.uring.cq.depth` | `u32` | `1024` | No | io_uring CQ depth (Linux only) |
| `broker.io.uring.sqpoll` | `bool` | `false` | No | Enable io_uring SQPOLL mode (Linux only) |
| `broker.io.uring.single.issuer` | `bool` | `true` | No | Enable SINGLE_ISSUER setup when supported |
| `broker.io.uring.coop.taskrun` | `bool` | `true` | No | Enable COOP_TASKRUN setup when supported |
| `broker.io.uring.registered.buffers` | `u32` | `64` | No | Number of registered io_uring buffers (Linux only) |
| `broker.io.uring.sender.enabled` | `bool` | `false` | No | Enable optional sender `writev` io_uring path |
| `broker.io.uring.sender.cqe.batch.size` | `u32` | `64` | No | Sender io_uring CQEs copied per poll |
| `broker.io.uring.receiver.enabled` | `bool` | `false` | No | Enable optional receiver multishot accept/recv path |
| `broker.io.uring.receiver.cqe.batch.size` | `u32` | `256` | No | Receiver io_uring CQEs copied per poll |
| `broker.io.uring.recv.buffer.size` | `u32` | `16384` | No | Receiver provided-buffer size |
| `broker.io.uring.recv.buffer.count` | `u32` | `256` | No | Receiver provided-buffer count |
| `broker.sender.writev.batch.size` | `u32` | `64` | No | Maximum frames per synchronous sender `writev` syscall |
| `broker.sender.write.budget.per.peer` | `u32` | `256` | No | Maximum sender frames flushed per peer per duty cycle |

### 2.2 BrokerConfig Struct

```zig
// src/config/broker_config.zig

const std = @import("std");
const constants = @import("../platform/constants.zig");

pub const ThreadingMode = enum {
    dedicated,
    shared_network,
    shared,

    pub fn fromString(s: []const u8) ?ThreadingMode {
        const map = std.StaticStringMap(ThreadingMode).initComptime(.{
            .{ "DEDICATED", .dedicated },
            .{ "dedicated", .dedicated },
            .{ "SHARED_NETWORK", .shared_network },
            .{ "shared_network", .shared_network },
            .{ "SHARED", .shared },
            .{ "shared", .shared },
        });
        return map.get(s);
    }
};

pub const IdleStrategyName = enum {
    busy_spin,
    yielding,
    sleeping,
    backoff,
    blocking,

    pub fn fromString(s: []const u8) ?IdleStrategyName {
        const map = std.StaticStringMap(IdleStrategyName).initComptime(.{
            .{ "busy_spin", .busy_spin },
            .{ "yielding", .yielding },
            .{ "sleeping", .sleeping },
            .{ "backoff", .backoff },
            .{ "blocking", .blocking },
        });
        return map.get(s);
    }
};

pub const PeerEndpoint = struct {
    node_id: u8,
    host: []const u8,
    port: u16,
};

pub const BrokerConfig = struct {
    // ── Required ────────────────────────────────────────────────
    node_id: u8,
    local_host: []const u8,
    local_port: u16,

    // ── Peers ───────────────────────────────────────────────────
    peer_endpoints: []const PeerEndpoint,

    // ── Naming & storage ────────────────────────────────────────
    group_name: []const u8 = "ringloom",
    storage_path: []const u8 = "/dev/shm",

    // ── Buffer sizes (bytes — all must be power of 2 unless noted) ──
    control_buffer_size: u32 = 65_536,           // 64 KB
    messages_buffer_size: u32 = 1_048_576,       // 1 MB

    // ── TCP transport ──────────────────────────────────────────
    peer_write_queue_capacity: u32 = 8_192,      // frames
    max_frame_length: u32 = 1_048_576,           // 1 MB
    tcp_sndbuf_size: u32 = 262_144,              // 256 KB
    tcp_rcvbuf_size: u32 = 262_144,              // 256 KB
    tcp_listen_backlog: u32 = 128,
    heartbeat_interval_ms: u32 = 500,
    heartbeat_timeout_ms: u32 = 2_000,
    reconnect_base_delay_ms: u32 = 100,
    reconnect_max_delay_ms: u32 = 1_000,

    // ── Threading ───────────────────────────────────────────────
    threading_mode: ThreadingMode = .dedicated,
    idle_strategy_name: IdleStrategyName = .backoff,

    // ── Monitoring ──────────────────────────────────────────────
    counter_values_buffer_size: u32 = 65_536,    // 64 KB
    error_log_buffer_size: u32 = 262_144,        // 256 KB

    // ── Limits ──────────────────────────────────────────────────
    max_services: u16 = 256,
    max_peers: u8 = 16,

    // ── io_uring (Linux only) ───────────────────────────────────
    io_uring_queue_depth: u32 = 256,
    io_uring_sqpoll: bool = false,
    io_uring_registered_buffers: u32 = 64,

    // ── Computed (set during validation, not from file) ─────────
    /// Total counter values buffer in bytes = counter_values_buffer_size.
    /// Max counter ID = counter_values_buffer_size / 128 - 1.
    max_counter_id: u32 = 0,

    /// Counter metadata buffer size = max_counter_id * 256.
    counter_metadata_buffer_size: u32 = 0,

    /// Derived: is this a single-node cluster? (no peers)
    single_node_cluster: bool = true,

    /// Returns the maximum message length for the send ring buffer.
    pub fn maxMessageLength(self: *const BrokerConfig) u32 {
        return self.messages_buffer_size / 8;
    }

    /// Returns the maximum number of peers that can be active.
    pub fn maxActivePeers(self: *const BrokerConfig) u8 {
        return self.max_peers;
    }
};
```

### 2.3 PeerEndpoint

Peers are specified as a comma-separated list of `nodeId@host:port` triplets:

```
broker.member.host.ports=1@10.0.0.2:9000,2@10.0.0.3:9000,3@10.0.0.4:9000
```

Parsing:

```zig
// src/config/config_loader.zig  (peer parsing section)

fn parsePeerEndpoints(
    allocator: std.mem.Allocator,
    value: []const u8,
) ![]PeerEndpoint {
    if (value.len == 0) return &[_]PeerEndpoint{};

    // Count commas to pre-allocate.
    var count: usize = 1;
    for (value) |c| {
        if (c == ',') count += 1;
    }

    var endpoints = try allocator.alloc(PeerEndpoint, count);
    var idx: usize = 0;

    var iter = std.mem.splitScalar(u8, value, ',');
    while (iter.next()) |entry_raw| {
        const entry = std.mem.trim(u8, entry_raw, &std.ascii.whitespace);
        if (entry.len == 0) continue;

        // Parse "nodeId@host:port"
        const at_pos = std.mem.indexOfScalar(u8, entry, '@') orelse
            return error.InvalidPeerFormat;
        const node_id_str = entry[0..at_pos];
        const host_port = entry[at_pos + 1 ..];

        const colon_pos = std.mem.lastIndexOfScalar(u8, host_port, ':') orelse
            return error.InvalidPeerFormat;

        const host = host_port[0..colon_pos];
        const port_str = host_port[colon_pos + 1 ..];

        const node_id = std.fmt.parseInt(u8, node_id_str, 10) catch
            return error.InvalidNodeId;
        const port = std.fmt.parseInt(u16, port_str, 10) catch
            return error.InvalidPort;

        // Dupe the host string into the arena so it outlives the file buffer.
        const host_owned = try allocator.dupe(u8, host);

        endpoints[idx] = .{
            .node_id = node_id,
            .host = host_owned,
            .port = port,
        };
        idx += 1;
    }

    return endpoints[0..idx];
}
```

### 2.4 Config Loading

```zig
// src/config/config_loader.zig

const std = @import("std");
const BrokerConfig = @import("broker_config.zig").BrokerConfig;
const PeerEndpoint = @import("broker_config.zig").PeerEndpoint;
const ThreadingMode = @import("broker_config.zig").ThreadingMode;
const IdleStrategyName = @import("broker_config.zig").IdleStrategyName;
const constants = @import("../platform/constants.zig");

pub const ConfigError = error{
    FileNotFound,
    IoError,
    MissingRequiredProperty,
    InvalidValue,
    InvalidPeerFormat,
    InvalidNodeId,
    InvalidPort,
    BufferSizeNotPowerOfTwo,
    BufferSizeTooSmall,
    NodeIdConflict,
};

pub const ConfigLoader = struct {
    allocator: std.mem.Allocator,

    pub fn init(allocator: std.mem.Allocator) ConfigLoader {
        return .{ .allocator = allocator };
    }

    /// Load configuration from a file. Tries `RINGLOOM_CONFIG_FILE` env var first,
    /// falls back to `broker.properties` in the current directory.
    pub fn load(self: *const ConfigLoader) !BrokerConfig {
        const path = std.posix.getenv("RINGLOOM_CONFIG_FILE") orelse "broker.properties";
        return self.loadFromFile(path);
    }

    /// Load configuration from a specific file path.
    pub fn loadFromFile(self: *const ConfigLoader, path: []const u8) !BrokerConfig {
        const file = std.fs.cwd().openFile(path, .{}) catch |err| switch (err) {
            error.FileNotFound => return ConfigError.FileNotFound,
            else => return ConfigError.IoError,
        };
        defer file.close();

        const content = file.readToEndAlloc(self.allocator, 1024 * 1024) catch
            return ConfigError.IoError;
        defer self.allocator.free(content);

        return self.parseAndBuild(content);
    }

    /// Parse properties from a string and build a validated BrokerConfig.
    pub fn parseAndBuild(self: *const ConfigLoader, content: []const u8) !BrokerConfig {
        var props = parseProperties(content, self.allocator) catch
            return ConfigError.IoError;
        defer props.deinit();

        var config = BrokerConfig{
            .node_id = undefined,
            .local_host = undefined,
            .local_port = undefined,
            .peer_endpoints = &[_]PeerEndpoint{},
        };

        // ── Required fields ─────────────────────────────────────

        const node_id_str = props.get("broker.node.id") orelse
            return ConfigError.MissingRequiredProperty;
        config.node_id = std.fmt.parseInt(u8, node_id_str, 10) catch
            return ConfigError.InvalidValue;

        const local_hp = props.get("broker.local.host.port") orelse
            return ConfigError.MissingRequiredProperty;
        const colon_pos = std.mem.lastIndexOfScalar(u8, local_hp, ':') orelse
            return ConfigError.InvalidValue;
        config.local_host = try self.allocator.dupe(u8, local_hp[0..colon_pos]);
        config.local_port = std.fmt.parseInt(u16, local_hp[colon_pos + 1 ..], 10) catch
            return ConfigError.InvalidValue;

        // ── Peers ───────────────────────────────────────────────

        if (props.get("broker.member.host.ports")) |peer_str| {
            config.peer_endpoints = try parsePeerEndpoints(self.allocator, peer_str);
        }

        // ── Optional fields ─────────────────────────────────────

        if (props.get("broker.group.name")) |v|
            config.group_name = try self.allocator.dupe(u8, v);
        if (props.get("broker.storage.path")) |v|
            config.storage_path = try self.allocator.dupe(u8, v);

        if (props.get("broker.control.buffer.size")) |v|
            config.control_buffer_size = std.fmt.parseInt(u32, v, 10) catch
                return ConfigError.InvalidValue;
        if (props.get("broker.messages.buffer.size")) |v|
            config.messages_buffer_size = std.fmt.parseInt(u32, v, 10) catch
                return ConfigError.InvalidValue;
        if (props.get("broker.peer.write.queue.capacity")) |v|
            config.peer_write_queue_capacity = std.fmt.parseInt(u32, v, 10) catch
                return ConfigError.InvalidValue;
        if (props.get("broker.max.frame.length")) |v|
            config.max_frame_length = std.fmt.parseInt(u32, v, 10) catch
                return ConfigError.InvalidValue;
        if (props.get("broker.tcp.sndbuf.size")) |v|
            config.tcp_sndbuf_size = std.fmt.parseInt(u32, v, 10) catch
                return ConfigError.InvalidValue;
        if (props.get("broker.tcp.rcvbuf.size")) |v|
            config.tcp_rcvbuf_size = std.fmt.parseInt(u32, v, 10) catch
                return ConfigError.InvalidValue;
        if (props.get("broker.tcp.listen.backlog")) |v|
            config.tcp_listen_backlog = std.fmt.parseInt(u32, v, 10) catch
                return ConfigError.InvalidValue;
        if (props.get("broker.heartbeat.interval.ms")) |v|
            config.heartbeat_interval_ms = std.fmt.parseInt(u32, v, 10) catch
                return ConfigError.InvalidValue;
        if (props.get("broker.heartbeat.timeout.ms")) |v|
            config.heartbeat_timeout_ms = std.fmt.parseInt(u32, v, 10) catch
                return ConfigError.InvalidValue;
        if (props.get("broker.reconnect.base.delay.ms")) |v|
            config.reconnect_base_delay_ms = std.fmt.parseInt(u32, v, 10) catch
                return ConfigError.InvalidValue;
        if (props.get("broker.reconnect.max.delay.ms")) |v|
            config.reconnect_max_delay_ms = std.fmt.parseInt(u32, v, 10) catch
                return ConfigError.InvalidValue;

        if (props.get("broker.threading.mode")) |v|
            config.threading_mode = ThreadingMode.fromString(v) orelse
                return ConfigError.InvalidValue;
        if (props.get("broker.idle.strategy")) |v|
            config.idle_strategy_name = IdleStrategyName.fromString(v) orelse
                return ConfigError.InvalidValue;

        if (props.get("broker.counter.values.buffer.size")) |v|
            config.counter_values_buffer_size = std.fmt.parseInt(u32, v, 10) catch
                return ConfigError.InvalidValue;
        if (props.get("broker.error.log.buffer.size")) |v|
            config.error_log_buffer_size = std.fmt.parseInt(u32, v, 10) catch
                return ConfigError.InvalidValue;

        if (props.get("broker.max.services")) |v|
            config.max_services = std.fmt.parseInt(u16, v, 10) catch
                return ConfigError.InvalidValue;
        if (props.get("broker.max.peers")) |v|
            config.max_peers = std.fmt.parseInt(u8, v, 10) catch
                return ConfigError.InvalidValue;

        if (props.get("broker.io.uring.queue.depth")) |v|
            config.io_uring_queue_depth = std.fmt.parseInt(u32, v, 10) catch
                return ConfigError.InvalidValue;
        if (props.get("broker.io.uring.sqpoll")) |v|
            config.io_uring_sqpoll = std.mem.eql(u8, v, "true");
        if (props.get("broker.io.uring.registered.buffers")) |v|
            config.io_uring_registered_buffers = std.fmt.parseInt(u32, v, 10) catch
                return ConfigError.InvalidValue;

        // ── Validate and compute derived fields ─────────────────

        try validate(&config);

        return config;
    }
};
```

### 2.5 Properties File Parser

The format is intentionally minimal — the same format used by Java `.properties` files:

- Lines starting with `#` or `!` are comments.
- Empty lines are skipped.
- Key-value pairs are separated by `=` or `:`.
- Leading and trailing whitespace on both key and value is trimmed.
- No multiline values, no escape sequences, no Unicode escapes.

```zig
// src/config/config_loader.zig  (properties parser section)

fn parseProperties(
    content: []const u8,
    allocator: std.mem.Allocator,
) !std.StringHashMap([]const u8) {
    var map = std.StringHashMap([]const u8).init(allocator);

    var line_iter = std.mem.splitScalar(u8, content, '\n');
    while (line_iter.next()) |raw_line| {
        // Strip trailing \r for Windows-style line endings.
        const line = std.mem.trimRight(u8, raw_line, &[_]u8{'\r'});
        const trimmed = std.mem.trim(u8, line, &std.ascii.whitespace);

        // Skip empty lines and comments.
        if (trimmed.len == 0) continue;
        if (trimmed[0] == '#' or trimmed[0] == '!') continue;

        // Find the separator: first '=' or ':'
        const sep_pos = blk: {
            for (trimmed, 0..) |c, i| {
                if (c == '=' or c == ':') break :blk i;
            }
            continue; // No separator — skip malformed line.
        };

        const key = std.mem.trim(
            u8,
            trimmed[0..sep_pos],
            &std.ascii.whitespace,
        );
        const value = std.mem.trim(
            u8,
            trimmed[sep_pos + 1 ..],
            &std.ascii.whitespace,
        );

        if (key.len > 0) {
            try map.put(key, value);
        }
    }

    return map;
}
```

### 2.6 Environment Variable Override

Every property can be overridden by an environment variable. The mapping is:

1. Replace all `.` with `_`.
2. Convert to uppercase.
3. Prefix with `RINGLOOM_`.

Example: `broker.node.id` → `RINGLOOM_BROKER_NODE_ID`.

Environment variables take precedence over file values:

```zig
// src/config/config_loader.zig  (env override section)

fn applyEnvOverrides(
    props: *std.StringHashMap([]const u8),
    allocator: std.mem.Allocator,
) !void {
    // List of all known property keys.
    const keys = [_][]const u8{
        "broker.node.id",
        "broker.local.host.port",
        "broker.member.host.ports",
        "broker.group.name",
        "broker.storage.path",
        "broker.control.buffer.size",
        "broker.messages.buffer.size",
        "broker.peer.write.queue.capacity",
        "broker.max.frame.length",
        "broker.tcp.sndbuf.size",
        "broker.tcp.rcvbuf.size",
        "broker.tcp.listen.backlog",
        "broker.heartbeat.interval.ms",
        "broker.heartbeat.timeout.ms",
        "broker.reconnect.base.delay.ms",
        "broker.reconnect.max.delay.ms",
        "broker.threading.mode",
        "broker.idle.strategy",
        "broker.counter.values.buffer.size",
        "broker.error.log.buffer.size",
        "broker.max.services",
        "broker.max.peers",
        "broker.io.uring.queue.depth",
        "broker.io.uring.sqpoll",
        "broker.io.uring.registered.buffers",
    };

    var env_name_buf: [256]u8 = undefined;

    for (keys) |key| {
        // Build env var name: "RINGLOOM_" + key.replace('.', '_').toUpperCase()
        var len: usize = 4; // "RINGLOOM_"
        @memcpy(env_name_buf[0..4], "RINGLOOM_");

        for (key) |c| {
            env_name_buf[len] = if (c == '.') '_' else std.ascii.toUpper(c);
            len += 1;
        }

        const env_name = env_name_buf[0..len];
        if (std.posix.getenv(env_name)) |env_value| {
            const owned = try allocator.dupe(u8, env_value);
            try props.put(key, owned);
        }
    }
}
```

### 2.7 Buffer Sizing Validation

All ring buffer sizes must be powers of two (Agrona / MPSC ring buffer requirement from
doc 03). The error log buffer size does not need to be a power of two either (it is a
linear append-only buffer).

Validation runs after all properties are loaded and env overrides are applied:

```zig
// src/config/config_loader.zig  (validation section)

fn validate(config: *BrokerConfig) !void {
    // ── Required field presence ─────────────────────────────────
    // node_id, local_host, and local_port were already checked during parsing.
    // If we reached here, they are set.

    // ── Node ID must not conflict with a peer ───────────────────
    for (config.peer_endpoints) |peer| {
        if (peer.node_id == config.node_id) {
            return ConfigError.NodeIdConflict;
        }
    }

    // ── Buffer sizes must be power of 2 ─────────────────────────
    const po2_fields = [_]*u32{
        &config.control_buffer_size,
        &config.messages_buffer_size,
        &config.counter_values_buffer_size,
    };

    for (po2_fields) |field_ptr| {
        if (!constants.isPowerOfTwo(field_ptr.*)) {
            // Auto-align up to next power of 2.
            field_ptr.* = alignToPowerOfTwo(field_ptr.*);
        }
    }

    // ── Minimum buffer sizes ────────────────────────────────────
    if (config.control_buffer_size < 4096)
        return ConfigError.BufferSizeTooSmall;
    if (config.messages_buffer_size < 4096)
        return ConfigError.BufferSizeTooSmall;

    // ── Max frame length bounds ─────────────────────────────────
    if (config.max_frame_length < 1024 or config.max_frame_length > 16 * 1024 * 1024)
        return ConfigError.InvalidValue;

    // ── Heartbeat sanity (timeout must exceed interval) ─────────
    if (config.heartbeat_timeout_ms <= config.heartbeat_interval_ms)
        return ConfigError.InvalidValue;

    // ── Compute derived fields ──────────────────────────────────
    config.max_counter_id = config.counter_values_buffer_size / 128;
    config.counter_metadata_buffer_size = config.max_counter_id * 256;
    config.single_node_cluster = config.peer_endpoints.len == 0;
}

fn alignToPowerOfTwo(value: u32) u32 {
    if (constants.isPowerOfTwo(value)) return value;
    if (value == 0) return 1;

    var v = value;
    v -= 1;
    v |= v >> 1;
    v |= v >> 2;
    v |= v >> 4;
    v |= v >> 8;
    v |= v >> 16;
    return v +| 1; // Saturating add to avoid overflow on u32 max.
}
```

### 2.8 Service Configuration

Services also have configuration, loaded from a separate file or supplied programmatically
via the `RingLoomEngine` builder. Service config uses the same properties format with a
`ringloom.service.` prefix:

```zig
// src/config/service_config.zig

const std = @import("std");
const constants = @import("../platform/constants.zig");

pub const ServiceConfig = struct {
    /// Service name (e.g. "pricing", "order-gateway").
    service_name: []const u8,

    /// Control ring buffer capacity. Power of 2.
    control_buffer_size: u32 = 65_536,

    /// Application messages ring buffer capacity. Power of 2.
    messages_buffer_size: u32 = 1_048_576,

    /// Whether the ring buffer uses kernel-level blocking when full.
    /// When true, producers park via futex/ulock instead of spinning.
    blocking_mode: bool = false,

    /// Service heartbeat timeout. If the broker doesn't see a heartbeat
    /// from this service within this window, the service is considered dead.
    heartbeat_timeout_ms: u32 = 10_000,

    /// Idle strategy for the service's message consumer thread.
    idle_strategy_name: []const u8 = "backoff",

    /// Whether this service participates in per-service leader election.
    leader_election_enabled: bool = false,

    /// Storage path override (defaults to broker's storage path).
    storage_path: ?[]const u8 = null,

    /// Group name override (defaults to broker's group name).
    group_name: ?[]const u8 = null,
};
```

Service configuration properties file example:

```
# service.properties
ringloom.service.name=pricing
ringloom.service.control.buffer.size=65536
ringloom.service.messages.buffer.size=1048576
ringloom.service.blocking.mode=false
ringloom.service.heartbeat.timeout.ms=10000
ringloom.service.idle.strategy=backoff
ringloom.service.leader.election.enabled=false
```

### 2.9 ThreadingMode & IdleStrategy Selection

Configuration maps string names to the enum values used by the threading model (doc 10).
The `BrokerConfig.threading_mode` and `BrokerConfig.idle_strategy_name` fields are parsed
during config loading and used during startup wiring:

```zig
// src/config/broker_config.zig  (idle strategy factory section)

const platform = @import("../platform.zig");

/// Create the configured idle strategy. Called once at startup for each thread.
pub fn createIdleStrategy(name: IdleStrategyName) platform.IdleStrategy {
    return switch (name) {
        .busy_spin => platform.IdleStrategy.busySpin(),
        .yielding => platform.IdleStrategy.yielding(),
        .sleeping => platform.IdleStrategy.sleeping(1_000), // 1µs
        .backoff => platform.IdleStrategy.backoff(.{
            .max_spins = 10,
            .max_yields = 20,
        }),
        .blocking => platform.IdleStrategy.blocking(),
    };
}
```

---

## 3. System Counters

Doc 03 defined the `CountersManager` data structure — cache-line-padded atomic `i64`
values backed by shared memory. This section defines the **well-known counter IDs** used
by the broker and the wiring that connects counters to every subsystem.

### 3.1 Counter IDs

Every counter has a fixed, well-known integer ID. This enum is the single source of
truth for counter identities:

```zig
// src/monitoring/system_counter.zig

pub const SystemCounter = enum(u8) {
    // ── Traffic counters ────────────────────────────────────────
    bytes_sent = 0,
    bytes_received = 1,
    messages_routed_local = 2,
    messages_routed_remote = 3,

    // ── TCP connection counters ─────────────────────────────────
    tcp_connections_accepted = 4,
    tcp_connection_errors = 5,
    tcp_handshake_failures = 6,
    tcp_reconnect_attempts = 7,

    // ── Heartbeats ──────────────────────────────────────────────
    heartbeats_sent = 8,
    heartbeats_received = 9,
    heartbeat_timeouts = 10,

    // ── Service lifecycle ───────────────────────────────────────
    services_registered = 11,
    services_removed = 12,

    // ── Back-pressure ───────────────────────────────────────────
    send_rb_back_pressure = 13,
    service_full_drops = 14,
    peer_queue_overflow_drops = 15,
    peer_not_connected_drops = 16,

    // ── Error counters ──────────────────────────────────────────
    unknown_service_drops = 17,
    invalid_frames = 18,

    // ── Performance: max cycle time per event loop (nanoseconds) ──
    control_loop_cycle_time_max = 19,
    sender_cycle_time_max = 20,
    receiver_cycle_time_max = 21,

    /// Total number of well-known counters.
    pub const count: usize = 22;

    /// Human-readable label for each counter.
    pub fn label(self: SystemCounter) []const u8 {
        return switch (self) {
            .bytes_sent => "bytes-sent",
            .bytes_received => "bytes-received",
            .messages_routed_local => "messages-routed-local",
            .messages_routed_remote => "messages-routed-remote",
            .tcp_connections_accepted => "tcp-connections-accepted",
            .tcp_connection_errors => "tcp-connection-errors",
            .tcp_handshake_failures => "tcp-handshake-failures",
            .tcp_reconnect_attempts => "tcp-reconnect-attempts",
            .heartbeats_sent => "heartbeats-sent",
            .heartbeats_received => "heartbeats-received",
            .heartbeat_timeouts => "heartbeat-timeouts",
            .services_registered => "services-registered",
            .services_removed => "services-removed",
            .send_rb_back_pressure => "send-rb-back-pressure",
            .service_full_drops => "service-full-drops",
            .peer_queue_overflow_drops => "peer-queue-overflow-drops",
            .peer_not_connected_drops => "peer-not-connected-drops",
            .unknown_service_drops => "unknown-service-drops",
            .invalid_frames => "invalid-frames",
            .control_loop_cycle_time_max => "control-loop-cycle-time-max-ns",
            .sender_cycle_time_max => "sender-cycle-time-max-ns",
            .receiver_cycle_time_max => "receiver-cycle-time-max-ns",
        };
    }
};
```

### 3.2 Counter Wiring

At startup, all well-known counters are pre-allocated. A `SystemCounters` struct holds
the allocated counter IDs and a pointer to the `CountersManager`, providing typed
increment/get methods:

```zig
// src/monitoring/system_counters.zig

const std = @import("std");
const CountersManager = @import("../concurrent/counters.zig").CountersManager;
const SystemCounter = @import("system_counter.zig").SystemCounter;

pub const SystemCounters = struct {
    counters: *CountersManager,
    ids: [SystemCounter.count]usize,

    pub fn init(counters: *CountersManager) !SystemCounters {
        var self = SystemCounters{
            .counters = counters,
            .ids = undefined,
        };

        // Allocate all well-known counters at startup.
        inline for (0..SystemCounter.count) |i| {
            const sc: SystemCounter = @enumFromInt(i);
            self.ids[i] = counters.allocate(i, sc.label()) orelse
                return error.CounterAllocationFailed;
        }

        return self;
    }

    /// Atomically increment a counter by 1.
    pub inline fn increment(self: *const SystemCounters, counter: SystemCounter) void {
        self.counters.increment(self.ids[@intFromEnum(counter)]);
    }

    /// Atomically add `delta` to a counter.
    pub inline fn add(self: *const SystemCounters, counter: SystemCounter, delta: i64) void {
        self.counters.add(self.ids[@intFromEnum(counter)], delta);
    }

    /// Atomically set a counter to an absolute value.
    pub inline fn set(self: *const SystemCounters, counter: SystemCounter, value: i64) void {
        self.counters.set(self.ids[@intFromEnum(counter)], value);
    }

    /// Read the current value of a counter (atomic load).
    pub inline fn get(self: *const SystemCounters, counter: SystemCounter) i64 {
        return self.counters.get(self.ids[@intFromEnum(counter)]);
    }

    /// Conditionally update a counter to the maximum of current and new value.
    /// Used for cycle-time tracking. Not atomic-max (acceptable for monitoring).
    pub inline fn updateMax(self: *const SystemCounters, counter: SystemCounter, value: i64) void {
        const current = self.get(counter);
        if (value > current) {
            self.set(counter, value);
        }
    }
};
```

**Wiring into event loops.** Each event loop receives a `*const SystemCounters` at
construction time. The counters are injected as a field, not imported as a global:

```zig
// Usage within the sender event loop (doc 05):
fn sendFrame(self: *SenderEventLoop, peer: *PeerState, frame: []const u8) void {
    self.network_io.sendTo(peer.address, frame) catch |err| {
        _ = self.error_log.record("sendTo failed", clock.epochMs());
        return;
    };
    self.counters.add(.bytes_sent, @intCast(frame.len));
}

// Usage within the routing agent (doc 08):
fn routeToLocalService(
    self: *MessageRouter,
    service_id: u16,
    payload: []const u8,
) void {
    const instance = self.service_registry.lookup(service_id) orelse {
        self.counters.increment(.unknown_service_drops);
        return;
    };

    instance.messages_rb.write(MSG_TYPE_APP, payload) catch {
        self.counters.increment(.service_back_pressure);
        return;
    };

    self.counters.increment(.messages_routed_local);
}
```

### 3.3 Cycle Time Tracking

Each event loop tracks its maximum duty-cycle duration. This is the elapsed wall-clock
time for a single `doWork()` call. High values indicate either too much work per cycle
or a blocking operation that shouldn't be on the hot path.

```zig
// src/monitoring/cycle_time.zig

const clock = @import("../platform/clock.zig");
const SystemCounters = @import("system_counters.zig").SystemCounters;
const SystemCounter = @import("system_counter.zig").SystemCounter;

pub const CycleTimeTracker = struct {
    counter: SystemCounter,
    counters: *const SystemCounters,
    reset_interval_ns: i64,
    last_reset_ns: i64,

    pub fn init(
        counter: SystemCounter,
        counters: *const SystemCounters,
        reset_interval_ns: i64,
    ) CycleTimeTracker {
        return .{
            .counter = counter,
            .counters = counters,
            .reset_interval_ns = reset_interval_ns,
            .last_reset_ns = clock.monotonicNanos(),
        };
    }

    /// Call at the start of each duty cycle. Returns the start timestamp.
    pub inline fn start(_: *const CycleTimeTracker) i64 {
        return clock.monotonicNanos();
    }

    /// Call at the end of each duty cycle with the start timestamp.
    /// Updates the max cycle time counter.
    pub inline fn stop(self: *CycleTimeTracker, start_ns: i64) void {
        const now = clock.monotonicNanos();
        const elapsed = now - start_ns;

        self.counters.updateMax(self.counter, elapsed);

        // Periodically reset the max so stale spikes don't persist forever.
        if (now - self.last_reset_ns > self.reset_interval_ns) {
            self.counters.set(self.counter, elapsed);
            self.last_reset_ns = now;
        }
    }
};
```

Usage in an event loop:

```zig
// Inside ControlLoop.doWork (from doc 10):
fn doWork(ctx: *anyopaque) u32 {
    const self: *ControlLoop = @ptrCast(@alignCast(ctx));
    const cycle_start = self.cycle_tracker.start();

    var work_count: u32 = 0;
    work_count += self.cmd_queue.drain(constants.command_drain_limit);
    work_count += self.control_rb.read(self.onControlMessage, constants.control_read_limit);
    work_count += self.checkServiceHeartbeats();

    self.cycle_tracker.stop(cycle_start);
    return work_count;
}
```

### 3.4 Counter Snapshot

For monitoring tools that want a point-in-time view of all counters without holding
references to the live atomic memory:

```zig
// src/monitoring/counter_snapshot.zig

const SystemCounter = @import("system_counter.zig").SystemCounter;
const SystemCounters = @import("system_counters.zig").SystemCounters;

pub const CounterValue = struct {
    id: u8,
    label: []const u8,
    value: i64,
};

pub const CounterSnapshot = struct {
    values: [SystemCounter.count]CounterValue,
    timestamp_ms: i64,

    pub fn take(counters: *const SystemCounters, timestamp_ms: i64) CounterSnapshot {
        var snapshot: CounterSnapshot = .{
            .values = undefined,
            .timestamp_ms = timestamp_ms,
        };

        inline for (0..SystemCounter.count) |i| {
            const sc: SystemCounter = @enumFromInt(i);
            snapshot.values[i] = .{
                .id = @intCast(i),
                .label = sc.label(),
                .value = counters.get(sc),
            };
        }

        return snapshot;
    }
};
```

### 3.5 Current Implementation Review

The intended architecture is that every broker and every service metadata file is
self-describing and contains all counters needed by an external observer. The current
implementation is only partially there:

| Area | Current state | Gap |
|---|---|---|
| Broker generic counters | `BrokerApplication` and `BrokerRuntime` allocate `CountersManager` value/metadata buffers with the process allocator. Sender and receiver loops allocate ad-hoc labels such as `bytes_sent`, `recv_frames_routed`, and `peer_queue_overflow_drops`. | These counters are not inside the broker metadata mmap, so `ringloom-stat` and future observers cannot read them after opening `broker_<node>.dat`. |
| Broker metadata layout | `BrokerMetadataFile` contains header, control ring, send ring, optional flow-control region, and optional per-peer send-counter region. | No generic counter values region, counter metadata region, or error-log region is appended to the broker metadata file. Header fields only expose flow-control and per-peer region lengths. |
| Typed counter registry | `src/common/monitoring/system_counter.zig` defines 33 well-known counters including flow-control counters. | Broker event loops mostly bypass `SystemCounters` and allocate their own IDs, so labels/IDs are not consistent across control, sender, receiver, docs, and external tooling. |
| Control loop counters | `ControlLoop.updateCounters()` is currently a no-op. | Active service count, cumulative registrations/removals, heartbeat timeouts, control messages, subscription count, and flow-control update counters are not consistently recorded. |
| Service metadata | `ServiceMetadataFile` contains header, optional blocking trailer, control ring, and message ring. | No service-owned counters are present. Service-side sends, receives, handler outcomes, backpressure, and flow-control decisions are invisible unless applications print their own summaries. |
| Service runtime | `RingLoomEngine`, `ServiceClient`, `IpcProducer`, `MessageConsumer`, and `ControlAgent` return errors or update flow-control shared state but do not own a service `CountersManager`. | Important producer/consumer/client metrics cannot be scraped from service metadata. |
| Ring occupancy | Ring buffers expose producer/consumer positions and capacities. | Occupancy can be derived by observers, but it is not documented as a first-class monitoring surface. Optional high-water counters are missing. |
| External tool | `ringloom-stat` scans broker/service metadata and prints process/header state, flow-control byte counts, and peer-send region sizes. | It does not print generic counters or error-log entries because those regions are not in metadata files yet. |

The rest of this section defines the target coverage. The implementation should treat
metadata-resident counters as the source of truth and use heap-backed counters only in
tests or temporary standalone components.

### 3.6 Required Counter Coverage

Counters are divided into **broker**, **service runtime**, and **shared derived gauges**.
Every counter label should use Prometheus-safe snake case in metadata. The exporter may
add a `ringloom_` prefix, but the stored labels should already be stable and readable.

#### Broker counters

| Counter | Type | Recording point |
|---|---|---|
| `broker_bytes_sent_total` | counter | Bytes successfully handed to TCP, including frame header. |
| `broker_bytes_received_total` | counter | Bytes accepted from TCP, including frame header. |
| `broker_frames_sent_total` | counter | Application/admin/heartbeat TCP frames sent. |
| `broker_frames_received_total` | counter | Complete TCP frames received before classification. |
| `broker_messages_routed_local_total` | counter | Application frames written to a local service messages ring. |
| `broker_messages_routed_remote_total` | counter | Service-originated frames dequeued from the broker send ring for remote peers. |
| `broker_admin_messages_sent_total` | counter | Cluster/service-discovery admin frames sent. |
| `broker_admin_messages_received_total` | counter | Admin frames decoded and dispatched. |
| `broker_heartbeats_sent_total` | counter | Broker-to-broker heartbeat frames sent. |
| `broker_heartbeats_received_total` | counter | Broker-to-broker heartbeat frames received. |
| `broker_tcp_connections_accepted_total` | counter | Incoming peer TCP connections accepted. |
| `broker_tcp_connections_established_total` | counter | Outgoing or incoming peer connections that complete handshake. |
| `broker_tcp_connections_closed_total` | counter | Peer connections closed for any reason. |
| `broker_tcp_connection_errors_total` | counter | Connect/read/write/socket errors. |
| `broker_tcp_reconnect_attempts_total` | counter | Sender reconnect attempts. |
| `broker_handshake_failures_total` | counter | Peer handshake validation failures. |
| `broker_invalid_frames_total` | counter | Malformed frames or frames rejected by node/header validation. |
| `broker_malformed_messages_dropped_total` | counter | Send-ring records too short to contain a frame header. |
| `broker_unknown_peer_drops_total` | counter | Frames dropped because target/source peer is unknown. |
| `broker_peer_not_connected_drops_total` | counter | Frames dropped because target peer is disconnected. |
| `broker_peer_queue_overflow_drops_total` | counter | Frames dropped because a per-peer write queue overflowed. |
| `broker_unknown_service_drops_total` | counter | Frames dropped because target service ID is unknown. |
| `broker_service_full_drops_total` | counter | Frames dropped because target service messages ring is full. |
| `broker_send_ring_full_total` | counter | Service writes/claims to the broker send ring that failed with `BufferFull`. |
| `broker_service_control_ring_full_total` | counter | Broker writes to a service control ring that failed with `BufferFull`. |
| `broker_services_registered_current` | gauge | Active local service count. |
| `broker_services_registered_total` | counter | Successful local service registrations. |
| `broker_services_removed_total` | counter | Graceful unregisters plus heartbeat removals. |
| `broker_service_heartbeat_timeouts_total` | counter | Services removed by heartbeat checker. |
| `broker_control_messages_received_total` | counter | Valid service-to-broker control messages read. |
| `broker_control_messages_invalid_total` | counter | Malformed or unknown control messages. |
| `broker_subscriptions_current` | gauge | Active service discovery subscription entries. |
| `broker_leader_elections_total` | counter | Service leader election evaluations. |
| `broker_control_loop_cycle_time_max_ns` | gauge | Rolling max control-loop duty-cycle time. |
| `broker_sender_cycle_time_max_ns` | gauge | Rolling max sender-loop duty-cycle time. |
| `broker_receiver_cycle_time_max_ns` | gauge | Rolling max receiver-loop duty-cycle time. |

#### Flow-control and per-peer counters

The existing flow-control and per-peer regions should remain as compact structured state,
but aggregate events should also be mirrored as generic counters so all observability
clients can consume them without understanding every specialized region.

| Counter | Type | Recording point |
|---|---|---|
| `broker_fc_updates_sent_total` | counter | Remaining-capacity updates or snapshots sent to peers. |
| `broker_fc_updates_received_total` | counter | Remaining-capacity updates or snapshots received from peers. |
| `broker_fc_pressure_events_total` | counter | Local service transitions into pressured state. |
| `broker_fc_recovery_events_total` | counter | Local service transitions back to normal state. |
| `broker_fc_slot_allocations_total` | counter | Flow-control slots allocated for remote services. |
| `broker_fc_slot_reclamations_total` | counter | Flow-control slots reclaimed. |
| `service_fc_client_backpressure_total` | counter | Service sends blocked by target-buffer or send-buffer flow control. |
| `service_fc_client_spin_timeouts_total` | counter | Service spin strategy timed out. |
| `service_fc_peer_congestion_total` | counter | Service sends blocked by per-peer pending threshold. |
| `service_fc_peer_disconnected_avoided_total` | counter | Service sends avoided because peer was known disconnected. |

The structured flow-control entries remain exported as gauges:

| Field | Prometheus interpretation |
|---|---|
| `remaining_bytes` | `ringloom_flow_control_remaining_bytes` gauge labeled by `source_node`, `target_node`, `service_id`, `slot`. |
| `capacity` | `ringloom_flow_control_capacity_bytes` gauge. |
| `pressure_state` | `ringloom_flow_control_pressure_state` gauge with 0=unknown, 1=normal, 2=pressured. |
| `last_update_ns` | Used to emit `ringloom_flow_control_update_age_seconds`. |

Per-peer send entries remain exported as gauges/counters:

| Field | Prometheus interpretation |
|---|---|
| `ring_bytes_pending` | `ringloom_broker_peer_ring_pending_bytes` gauge. |
| `queue_bytes_pending` | `ringloom_broker_peer_queue_pending_bytes` gauge. |
| `queue_capacity` | `ringloom_broker_peer_queue_capacity_bytes` gauge. |
| `connection_state` | `ringloom_broker_peer_connected` gauge, 1 connected, 0 disconnected. |
| `total_bytes_sent` | `ringloom_broker_peer_bytes_sent_total` counter. |
| `total_bytes_dropped` | `ringloom_broker_peer_bytes_dropped_total` counter. |
| `last_update_ns` | Used to emit `ringloom_broker_peer_counter_update_age_seconds`. |

#### Service runtime counters

Each service metadata file must contain a service-local `CountersManager` initialized by
`RingLoomEngine.start`. These counters describe what the service runtime did, independent
of application-domain counters.

| Counter | Type | Recording point |
|---|---|---|
| `service_messages_sent_total` | counter | Successful sends through `ServiceClient.send`, `sendTo`, `sendToLeader`, or zero-copy commit. |
| `service_messages_sent_local_total` | counter | Successful same-host IPC sends. |
| `service_messages_sent_remote_total` | counter | Successful writes/claims into the broker send ring. |
| `service_messages_received_total` | counter | Messages read from this service's messages ring. |
| `service_bytes_sent_total` | counter | Application payload bytes sent by this service. |
| `service_bytes_received_total` | counter | Application payload bytes delivered to handlers. |
| `service_send_failures_total` | counter | Send attempts that returned an error. |
| `service_send_buffer_full_total` | counter | Local target ring or broker send ring returned `BufferFull`. |
| `service_message_too_long_total` | counter | Send rejected because payload exceeded ring or frame limit. |
| `service_no_available_instance_total` | counter | Send failed because service discovery had no usable target. |
| `service_no_leader_available_total` | counter | `sendToLeader` failed because no leader was known. |
| `service_no_producer_total` | counter | Target instance existed but IPC producer was not initialized. |
| `service_claims_total` | counter | Successful zero-copy claims. |
| `service_claim_commits_total` | counter | Zero-copy claims committed. |
| `service_claim_aborts_total` | counter | Zero-copy claims aborted or dropped before commit. |
| `service_handler_invocations_total` | counter | Application handler calls. |
| `service_handler_errors_total` | counter | Handler errors reported via service runtime APIs or thread-local error state. |
| `service_handler_cycle_time_max_ns` | gauge | Rolling max handler invocation duration when handler timing is enabled. |
| `service_message_consumer_cycle_time_max_ns` | gauge | Rolling max message-consumer duty-cycle time. |
| `service_control_agent_cycle_time_max_ns` | gauge | Rolling max control-agent duty-cycle time. |
| `service_control_messages_received_total` | counter | Broker-to-service control messages processed. |
| `service_registry_instances_current` | gauge | Current number of known target instances in the service registry. |
| `service_registry_updates_total` | counter | Service discovery updates applied. |
| `service_heartbeats_sent_total` | counter | Heartbeat timestamp writes or explicit heartbeat control messages. |

Application-specific counters may also be allocated from the service metadata counter
region. They should use an application prefix (`orders_generated_total`,
`risk_rejected_total`, etc.) and must not reuse runtime labels.

### 3.7 Metadata Counter Regions

Both metadata file types must append a generic monitoring tail after their hot-path ring
buffers. Readers discover the tail from fixed header fields; if the fields are zero, the
file is treated as an older metadata version without generic counters.

```
Broker Metadata File:
┌──────────────────────────────────────────────┐
│ Metadata Header (512 B)                      │
├──────────────────────────────────────────────┤
│ Control Ring Buffer + trailer                │
├──────────────────────────────────────────────┤
│ Send Ring Buffer + trailer                   │
├──────────────────────────────────────────────┤
│ Flow-Control Region (optional, broker only)  │
├──────────────────────────────────────────────┤
│ Per-Peer Send Counter Region (optional)      │
├──────────────────────────────────────────────┤
│ Counter Values Buffer (128 B per counter)    │
├──────────────────────────────────────────────┤
│ Counter Metadata Buffer (256 B per counter)  │
├──────────────────────────────────────────────┤
│ Error Log Buffer                             │
└──────────────────────────────────────────────┘

Service Metadata File:
┌──────────────────────────────────────────────┐
│ Metadata Header (512 B)                      │
├──────────────────────────────────────────────┤
│ Blocking Trailer (optional)                  │
├──────────────────────────────────────────────┤
│ Control Ring Buffer + trailer                │
├──────────────────────────────────────────────┤
│ Messages Ring Buffer + trailer               │
├──────────────────────────────────────────────┤
│ Counter Values Buffer (128 B per counter)    │
├──────────────────────────────────────────────┤
│ Counter Metadata Buffer (256 B per counter)  │
├──────────────────────────────────────────────┤
│ Error Log Buffer                             │
└──────────────────────────────────────────────┘
```

Required header additions, stored in the reserved 512-byte metadata header space:

| Field | Type | Applies to | Meaning |
|---|---|---|---|
| `metadata_version` | `u16` | broker, service | Incremented when monitoring tail fields are present. |
| `flags` | `u16` | broker, service | Bitset: counters present, error log present, flow-control present, per-peer present. |
| `counter_values_buffer_length` | `u32` | broker, service | Byte length of counter values region. Zero means absent. |
| `counter_metadata_buffer_length` | `u32` | broker, service | Byte length of counter metadata region. |
| `error_log_buffer_length` | `u32` | broker, service | Byte length of error log region. Zero means absent. |
| `monitoring_tail_offset` | `u64` | broker, service | Absolute byte offset of counter values region. |
| `monitoring_tail_length` | `u64` | broker, service | Total bytes from counter values through error log. |

The writer computes all offsets at startup and never changes them afterward. The observer
validates every offset/length against file size before reading. Counter values remain
cache-line padded and are updated with atomic operations; counter metadata is written once
during startup before the counter is used.

### 3.8 Derived Ring Gauges

Ring buffer occupancy should not require hot-path counter increments. The observer can
derive gauges from each ring buffer's trailer:

```
used_bytes = producer_position - consumer_position
free_bytes = capacity - used_bytes
usage_ratio = used_bytes / capacity
```

Expose these for:

| Ring | Metric labels |
|---|---|
| Broker control ring | `owner_type="broker"`, `ring="control"` |
| Broker send ring | `owner_type="broker"`, `ring="send"` |
| Service control ring | `owner_type="service"`, `ring="control"` |
| Service messages ring | `owner_type="service"`, `ring="messages"` |

The runtime should additionally maintain optional high-water gauges when cheap to update:
`*_ring_usage_high_water_bytes` and `*_ring_full_total`. High-water updates may use the
same non-atomic max pattern as cycle-time tracking because they are advisory monitoring
data; `BufferFull` events must remain exact counters.

---

## 4. Error Log Wiring

The `ErrorLog` data structure from doc 03 is a flat, append-only buffer with deduplication.
This section defines how it is wired into the broker and what categories of errors are
recorded.

### 4.1 Error Categories

Errors fall into three categories based on severity and frequency:

| Category | Example | Recording Strategy |
|---|---|---|
| **Transient** | Ring buffer full, send buffer back-pressure | Counter increment only — no error log (too frequent) |
| **Operational** | Unknown target service, invalid frame, protocol error | Counter + error log (deduplicated — one entry per unique error message) |
| **Fatal** | mmap failure, io_uring setup failure, out of memory | Log to stderr + error log + immediate shutdown |

The error log is **not for hot-path events**. Incrementing a counter is ~1 ns (single
atomic add). Recording to the error log involves scanning for duplicates (~100 ns for
10 entries) — acceptable for infrequent errors but not for every message.

### 4.2 Error Recording Patterns

```zig
// Pattern: operational error — counter + error log
fn handleInvalidPacket(
    self: *ReceiverEventLoop,
    addr: std.net.Address,
    frame: []const u8,
) void {
    self.counters.increment(.invalid_packets);

    // Format the error description into a stack-local buffer (no allocation).
    var buf: [256]u8 = undefined;
    const desc = std.fmt.bufPrint(&buf, "invalid packet from {}: len={d}, header bytes={x:0>2}{x:0>2}", .{
        addr,
        frame.len,
        if (frame.len > 0) frame[0] else 0,
        if (frame.len > 1) frame[1] else 0,
    }) catch "invalid packet (description too long)";

    _ = self.error_log.record(desc, clock.epochMs());
}

// Pattern: unknown service — counter + error log
fn handleUnknownService(
    self: *MessageRouter,
    target_service_id: u16,
    source_node_id: u8,
) void {
    self.counters.increment(.unknown_service_drops);

    var buf: [128]u8 = undefined;
    const desc = std.fmt.bufPrint(&buf, "unknown target service_id={d} from node={d}", .{
        target_service_id,
        source_node_id,
    }) catch "unknown service (description too long)";

    _ = self.error_log.record(desc, clock.epochMs());
}
```

### 4.3 Error Log Iteration

The error log supports `forEach` iteration for monitoring tools. The iterator is
lock-free and read-only — it can be called from a monitoring thread without affecting
the broker's event loops:

```zig
fn dumpErrorLog(error_log: *const ErrorLog, writer: anytype) !void {
    error_log.forEach(struct {
        fn print(entry: ErrorLog.Entry) void {
            std.log.warn(
                "[{d}x since {d}, last {d}] {s}",
                .{
                    entry.observation_count,
                    entry.first_observation_timestamp,
                    entry.last_observation_timestamp,
                    entry.description,
                },
            );
        }
    }.print);
}
```

---

## 5. Thread-Local Error State Wiring

Doc 03 defined `ErrorState` as a per-thread supplementary diagnostic channel. It is used
in contexts where the error must traverse a function-pointer callback boundary (e.g.,
`MessageHandler`) where Zig's error return mechanism is not available.

The wiring is minimal — every module that needs it imports `error_state.zig`:

```zig
const error_state = @import("../concurrent/error_state.zig");

// Inside a MessageHandler callback:
fn onControlMessage(msg_type_id: i32, payload: []const u8) void {
    if (msg_type_id == REGISTER_SERVICE) {
        const result = processRegistration(payload);
        if (!result) {
            // The handler can't return an error — it's behind a fn pointer.
            // The error details are in thread-local state for the caller to check.
            const msg = error_state.err_state.message() orelse "unknown error";
            _ = error_log.record(msg, clock.epochMs());
        }
    }
}

fn processRegistration(payload: []const u8) bool {
    if (payload.len < @sizeOf(RegisterServiceDecoder)) {
        error_state.err_state.setFmt(-1, "registration payload too short: {d} bytes", .{payload.len});
        return false;
    }
    // ... process registration ...
    return true;
}
```

**Important:** Thread-local error state is cleared at the **start** of each duty cycle,
not at the end. This ensures stale errors from a previous cycle don't leak:

```zig
fn doWork(ctx: *anyopaque) u32 {
    const self: *ControlLoop = @ptrCast(@alignCast(ctx));

    // Clear thread-local error state at the start of each cycle.
    error_state.err_state.clear();

    var work_count: u32 = 0;
    work_count += self.cmd_queue.drain(constants.command_drain_limit);
    work_count += self.control_rb.read(self.onControlMessage, constants.control_read_limit);
    // ...
    return work_count;
}
```

---

## 6. Hot-Path Error Handling Patterns

The hot path — message routing, ring buffer reads/writes, TCP send/receive — must
**never allocate**, **never throw**, and **never log synchronously**. Every error on the
hot path is handled through one of three patterns.

### 6.1 Pattern 1: Return Value + Counter

The most common pattern. The callee returns an error or a sentinel value; the caller
increments a counter and continues.

```zig
// Ring buffer write failure
fn writeToService(
    service: *ServiceInstance,
    msg_type: i32,
    payload: []const u8,
    counters: *const SystemCounters,
) bool {
    service.messages_rb.write(msg_type, payload) catch {
        counters.increment(.service_back_pressure);
        return false; // Message lost — tracked by counter.
    };
    return true;
}

// tryClaim failure (zero-copy path)
fn tryClaim(
    rb: *RingBuffer,
    length: usize,
    counters: *const SystemCounters,
    bp_counter: SystemCounter,
) ?RingBuffer.Claim {
    return rb.tryClaim(@intCast(length)) catch {
        counters.increment(bp_counter);
        return null;
    };
}
```

### 6.2 Pattern 2: Fallthrough + Counter + Error Log

For errors that are infrequent but operationally significant. The counter tracks
frequency; the error log captures context for debugging.

```zig
// Unknown service during routing
fn routeInboundMessage(
    self: *MessageRouter,
    header: *const MessageHeader,
    payload: []const u8,
) void {
    const target_id = header.target_service_id;
    const instance = self.service_registry.lookup(target_id) orelse {
        self.counters.increment(.unknown_service_drops);

        var buf: [128]u8 = undefined;
        const desc = std.fmt.bufPrint(
            &buf,
            "drop: unknown service_id={d} from node={d}/svc={d}",
            .{ target_id, header.source_node_id, header.source_service_id },
        ) catch "drop: unknown service";

        _ = self.error_log.record(desc, clock.epochMs());
        return;
    };

    instance.messages_rb.write(MSG_TYPE_APP, payload) catch {
        self.counters.increment(.service_back_pressure);
        return;
    };

    self.counters.increment(.messages_routed_local);
}
```

### 6.3 Pattern 3: Thread-Local State for Callbacks

When an error occurs deep inside a `fn(*const u8) void` callback chain where Zig error
returns are not available:

```zig
// Inside a ring buffer MessageHandler callback:
fn onServiceMessage(msg_type_id: i32, payload: []const u8) void {
    if (payload.len < min_message_length) {
        error_state.err_state.setFmt(
            -1,
            "message too short: type={d} len={d}",
            .{ msg_type_id, payload.len },
        );
        return; // Caller checks error_state.err_state.isSet().
    }
    // ... process message ...
}
```

### 6.4 Anti-Patterns (Never Do This)

These patterns are **bugs** if they appear on the hot path:

```zig
// ❌ NEVER: allocate on error
fn handleError(desc: []const u8) void {
    const msg = std.fmt.allocPrint(allocator, "error: {s}", .{desc}); // ALLOCATION!
    std.log.err("{s}", .{msg});
    allocator.free(msg);
}

// ❌ NEVER: format to stderr on hot path
fn routeMessage(payload: []const u8) void {
    // ...
    std.log.warn("back pressure on service {d}", .{id}); // FILE I/O!
    // ...
}

// ❌ NEVER: panic on recoverable error
fn tryClaim(rb: *RingBuffer, len: usize) RingBuffer.Claim {
    return rb.tryClaim(@intCast(len)) catch @panic("buffer full"); // CRASH!
}

// ❌ NEVER: use mutex-protected logging
fn onPacket(frame: []const u8) void {
    mutex.lock();         // CONTENTION!
    log.append(frame);
    mutex.unlock();
}
```

**Summary of hot-path error handling rules:**

| Allowed | Not Allowed |
|---|---|
| Atomic counter increment (~1 ns) | Any allocation (`alloc`, `allocPrint`) |
| Return error / sentinel value | `std.log.*` (synchronous file I/O) |
| Set thread-local `ErrorState` | `@panic` on recoverable conditions |
| `error_log.record()` for rare errors | Mutex/lock acquisition |
| Stack-local `bufPrint` for error descriptions | Heap-allocated error strings |

---

## 7. Monitoring Interface

### 7.1 Shared-Memory Monitoring (Primary)

The primary monitoring mechanism requires **zero broker overhead**: an external process
`mmap`s the same counter values and error log buffers and reads them directly.

The counter values buffer and counter metadata buffer are stored in the broker's metadata
file (see doc 02). Their offsets are derived from the broker metadata header:

```
Broker Metadata File:
┌────────────────────────────┐  ← offset 0
│  Metadata Header (512 B)   │
├────────────────────────────┤  ← offset 512
│  Control Ring Buffer       │
├────────────────────────────┤  ← offset 512 + control_buf_size + trailer
│  Send Ring Buffer          │
├────────────────────────────┤  ← offset ... + send_buf_size + trailer
│  Counter Values Buffer     │  ← 128 bytes per counter, cache-line padded
├────────────────────────────┤
│  Counter Metadata Buffer   │  ← 256 bytes per counter
├────────────────────────────┤
│  Error Log Buffer          │  ← linear append-only
└────────────────────────────┘
```

An external monitoring tool only needs to:

1. Open the broker's `.dat` file.
2. `mmap` it read-only.
3. Read the header to find buffer offsets and sizes.
4. Iterate counter values and error log entries using the same `forEach` logic from
   doc 03.

### 7.2 MonitoringSnapshot

For tools that prefer a copied-out point-in-time snapshot (e.g., for JSON serialization
or network export), the `MonitoringSnapshot` struct captures everything:

```zig
// src/monitoring/monitoring.zig

const std = @import("std");
const clock = @import("../platform/clock.zig");
const CountersManager = @import("../concurrent/counters.zig").CountersManager;
const ErrorLog = @import("../concurrent/error_log.zig").ErrorLog;
const SystemCounter = @import("system_counter.zig").SystemCounter;
const SystemCounters = @import("system_counters.zig").SystemCounters;

pub const CounterEntry = struct {
    id: u8,
    label: []const u8,
    value: i64,
};

pub const ErrorEntry = struct {
    observation_count: i32,
    first_observation_timestamp: i64,
    last_observation_timestamp: i64,
    description: []const u8,
};

pub const MonitoringSnapshot = struct {
    node_id: u8,
    timestamp_ms: i64,
    counters: [SystemCounter.count]CounterEntry,
    error_count: usize,
    errors: [max_snapshot_errors]ErrorEntry,

    const max_snapshot_errors: usize = 64;

    pub fn take(
        node_id: u8,
        sys_counters: *const SystemCounters,
        error_log: *const ErrorLog,
    ) MonitoringSnapshot {
        var snapshot = MonitoringSnapshot{
            .node_id = node_id,
            .timestamp_ms = clock.epochMs(),
            .counters = undefined,
            .error_count = 0,
            .errors = undefined,
        };

        // Capture all system counters.
        inline for (0..SystemCounter.count) |i| {
            const sc: SystemCounter = @enumFromInt(i);
            snapshot.counters[i] = .{
                .id = @intCast(i),
                .label = sc.label(),
                .value = sys_counters.get(sc),
            };
        }

        // Capture error log entries (up to max_snapshot_errors).
        error_log.forEach(struct {
            fn collect(entry: ErrorLog.Entry) void {
                // This callback captures into the snapshot via threadlocal.
                // In practice, use the context-passing pattern below.
                _ = entry;
            }
        }.collect);

        // A more practical approach: iterate with an index.
        snapshot.collectErrors(error_log);

        return snapshot;
    }

    fn collectErrors(self: *MonitoringSnapshot, error_log: *const ErrorLog) void {
        var offset: usize = 0;
        while (offset < error_log.buffer.len and self.error_count < max_snapshot_errors) {
            const entry_length_ptr: *const i32 = @ptrCast(@alignCast(error_log.buffer.ptr + offset));
            const entry_length = @atomicLoad(i32, entry_length_ptr, .acquire);
            if (entry_length <= 0) break;

            const base = offset;
            const obs_ptr: *const i32 = @ptrCast(@alignCast(error_log.buffer.ptr + base + 4));
            const last_ts_ptr: *const i64 = @ptrCast(@alignCast(error_log.buffer.ptr + base + 8));
            const first_ts_ptr: *const i64 = @ptrCast(@alignCast(error_log.buffer.ptr + base + 16));

            const desc_len: usize = @intCast(entry_length - 24);

            self.errors[self.error_count] = .{
                .observation_count = @atomicLoad(i32, obs_ptr, .acquire),
                .first_observation_timestamp = first_ts_ptr.*,
                .last_observation_timestamp = @atomicLoad(i64, last_ts_ptr, .acquire),
                .description = error_log.buffer[base + 24 ..][0..desc_len],
            };
            self.error_count += 1;

            offset += std.mem.alignForward(usize, @intCast(entry_length), 4);
        }
    }

    /// Format the snapshot as human-readable text to a writer.
    pub fn dump(self: *const MonitoringSnapshot, writer: anytype) !void {
        try writer.print("=== RingLoom Broker Node {d} — Monitoring Snapshot ===\n", .{self.node_id});
        try writer.print("Timestamp: {d} ms\n\n", .{self.timestamp_ms});

        try writer.print("--- Counters ---\n", .{});
        for (self.counters) |c| {
            if (c.value != 0) {
                try writer.print("  [{d:>2}] {s:<40} {d}\n", .{ c.id, c.label, c.value });
            }
        }

        try writer.print("\n--- Error Log ({d} entries) ---\n", .{self.error_count});
        for (self.errors[0..self.error_count]) |e| {
            try writer.print(
                "  [{d}x] {s}\n        first={d}  last={d}\n",
                .{ e.observation_count, e.description, e.first_observation_timestamp, e.last_observation_timestamp },
            );
        }
    }
};
```

### 7.3 Periodic Stderr Dump (Optional)

For development and debugging, the broker can periodically dump a monitoring snapshot to
stderr. This is controlled by an environment variable and runs on the control loop thread
at a low frequency (default: every 10 seconds).

```zig
// src/monitoring/periodic_dump.zig

const std = @import("std");
const clock = @import("../platform/clock.zig");
const MonitoringSnapshot = @import("monitoring.zig").MonitoringSnapshot;
const SystemCounters = @import("system_counters.zig").SystemCounters;
const ErrorLog = @import("../concurrent/error_log.zig").ErrorLog;

pub const PeriodicMonitoringDump = struct {
    enabled: bool,
    interval_ns: i64,
    next_dump_ns: i64,
    node_id: u8,
    counters: *const SystemCounters,
    error_log: *const ErrorLog,

    const default_interval_ns: i64 = 10 * std.time.ns_per_s;

    pub fn init(
        node_id: u8,
        counters: *const SystemCounters,
        error_log: *const ErrorLog,
    ) PeriodicMonitoringDump {
        const enabled = std.posix.getenv("RINGLOOM_MONITORING_DUMP") != null;
        return .{
            .enabled = enabled,
            .interval_ns = default_interval_ns,
            .next_dump_ns = clock.monotonicNanos() + default_interval_ns,
            .node_id = node_id,
            .counters = counters,
            .error_log = error_log,
        };
    }

    /// Called once per control loop duty cycle. Returns 1 if a dump was written, 0 otherwise.
    pub fn doWork(self: *PeriodicMonitoringDump) u32 {
        if (!self.enabled) return 0;

        const now = clock.monotonicNanos();
        if (now < self.next_dump_ns) return 0;

        self.next_dump_ns = now + self.interval_ns;

        const snapshot = MonitoringSnapshot.take(self.node_id, self.counters, self.error_log);
        snapshot.dump(std.io.getStdErr().writer()) catch {};

        return 1;
    }
};
```

### 7.4 External Monitoring Tool

A standalone `ringloom-stat` tool reads the broker's metadata file and prints counters and
error log entries. It is a separate binary that `mmap`s the broker's `.dat` file
read-only:

```zig
// tools/ringloom_stat.zig

const std = @import("std");
const constants = @import("../src/platform/constants.zig");
const CountersManager = @import("../src/concurrent/counters.zig").CountersManager;
const ErrorLog = @import("../src/concurrent/error_log.zig").ErrorLog;

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    const path = if (args.len > 1) args[1] else "/dev/shm/ringloom/services/broker_0.dat";

    // Open and mmap the file read-only.
    const file = try std.fs.cwd().openFile(path, .{ .mode = .read_only });
    defer file.close();

    const stat = try file.stat();
    const mapped = try std.posix.mmap(
        null,
        stat.size,
        std.posix.PROT.READ,
        .{ .TYPE = .SHARED },
        file.handle,
        0,
    );
    defer std.posix.munmap(mapped);

    // Read header to find buffer offsets.
    const header: *const BrokerMetadataHeader = @ptrCast(@alignCast(mapped.ptr));
    const control_buf_size: usize = @intCast(header.control_buffer_length);
    const send_buf_size: usize = @intCast(header.messages_buffer_length);

    // Calculate offsets (must match broker's layout computation).
    const control_end = constants.metadata_header_length + control_buf_size + constants.ring_buffer_trailer_length;
    const send_end = control_end + send_buf_size + constants.ring_buffer_trailer_length;

    // Counter values buffer starts at send_end.
    const counter_values_offset = send_end;
    const counter_values_size: usize = @intCast(header.counter_values_buffer_size);
    const counter_metadata_offset = counter_values_offset + counter_values_size;
    const counter_metadata_size = (counter_values_size / 128) * 256;
    const error_log_offset = counter_metadata_offset + counter_metadata_size;
    const error_log_size: usize = @intCast(header.error_log_buffer_size);

    // Create managers over the mapped memory.
    const values_buf = mapped[counter_values_offset..][0..counter_values_size];
    const meta_buf = mapped[counter_metadata_offset..][0..counter_metadata_size];
    const error_buf = mapped[error_log_offset..][0..error_log_size];

    var counters = CountersManager.init(
        @alignCast(values_buf),
        meta_buf,
    );

    const error_log = ErrorLog.init(error_buf);

    const stdout = std.io.getStdOut().writer();

    // Print counters.
    try stdout.print("=== RingLoom Counters ({s}) ===\n", .{path});
    counters.forEach(struct {
        fn print(id: usize, type_id: i32, label_text: []const u8, value: i64) void {
            _ = type_id;
            std.io.getStdOut().writer().print(
                "  [{d:>3}] {s:<40} {d}\n",
                .{ id, label_text, value },
            ) catch {};
        }
    }.print);

    // Print error log.
    try stdout.print("\n=== RingLoom Error Log ===\n", .{});
    error_log.forEach(struct {
        fn print(entry: ErrorLog.Entry) void {
            std.io.getStdOut().writer().print(
                "  [{d}x] {s}  (first: {d}, last: {d})\n",
                .{ entry.observation_count, entry.description, entry.first_observation_timestamp, entry.last_observation_timestamp },
            ) catch {};
        }
    }.print);
}

const BrokerMetadataHeader = extern struct {
    control_buffer_length: i32,
    messages_buffer_length: i32,
    service_id: i32,
    node_id: i16,
    _padding0: i16,
    pid: i64,
    start_timestamp_ms: i64,
    _reserved: [224]u8,
    heartbeat_time_ms: i64, // offset 256
    _padding1: [24]u8,
    next_service_id: i32, // offset 288
    _padding2: [212]u8,
    // Extended fields (added in doc 12):
    counter_values_buffer_size: i32, // offset 512 - or wherever the header stores this
    error_log_buffer_size: i32,
};
```

### 7.5 Prometheus Observability Process

The production monitoring path is a separate executable, tentatively named
`ringloom-observability`, described in [Observability](../observability.md). It does not
run inside broker or service processes. It scans the metadata storage directory, maps each
broker and service metadata file read-only, converts metadata-resident counters and
derived ring-buffer gauges into Prometheus text format, and serves them from `/metrics`
using Zig's standard-library HTTP server.

Required behavior:

| Requirement | Design |
|---|---|
| Input | `--storage-path`, `--group`, `--listen`, `--refresh-ms`, optional `--broker-node-id`, optional service-name filters. |
| Discovery | Periodically rescan `<storage_path>/<group>/services` for `broker_<node>.dat` and service metadata files. |
| Safety | Validate metadata version, file size, offsets, lengths, and power-of-two ring capacities before reading any region. Skip invalid files and increment exporter self-counters. |
| Export model | Emit broker counters, service counters, flow-control entries, per-peer send entries, process liveness gauges, heartbeat age gauges, and ring occupancy gauges. |
| Hot-path cost | Zero broker/service work beyond existing atomic counter writes. Exporter reads are acquire loads from read-only mmaps. |
| Compatibility | Files with no monitoring tail still produce liveness/header/ring gauges where possible, but generic counters are absent. |

`ringloom-stat` remains a human CLI inspection tool. `ringloom-observability` is the
long-running scrape endpoint for Prometheus and should share metadata parsing helpers with
`ringloom-stat` where practical.

---

## 8. Broker Startup Wiring — Putting It All Together

This section shows how configuration, counters, error log, and monitoring are wired into
the broker startup sequence defined in doc 10 and doc 11.

```zig
// src/broker.zig  (startup section — extends doc 10's Broker.create)

const std = @import("std");
const config_loader = @import("config/config_loader.zig");
const BrokerConfig = @import("config/broker_config.zig").BrokerConfig;
const CountersManager = @import("concurrent/counters.zig").CountersManager;
const ErrorLog = @import("concurrent/error_log.zig").ErrorLog;
const SystemCounters = @import("monitoring/system_counters.zig").SystemCounters;
const PeriodicMonitoringDump = @import("monitoring/periodic_dump.zig").PeriodicMonitoringDump;
const constants = @import("platform/constants.zig");
const clock = @import("platform/clock.zig");

pub const Broker = struct {
    config: BrokerConfig,
    allocator: std.mem.Allocator,

    // Monitoring subsystem
    counters_manager: CountersManager,
    system_counters: SystemCounters,
    error_log: ErrorLog,
    monitoring_dump: PeriodicMonitoringDump,

    // ... other fields from doc 10 and doc 11 ...

    pub fn create(allocator: std.mem.Allocator) !*Broker {
        // ── Step 1: Load configuration ──────────────────────────
        const loader = config_loader.ConfigLoader.init(allocator);
        const config = try loader.load();

        std.log.info("RingLoom Broker starting: node_id={d}, bind={s}:{d}, peers={d}", .{
            config.node_id,
            config.local_host,
            config.local_port,
            config.peer_endpoints.len,
        });

        // ── Step 2: Create metadata file and mmap ───────────────
        // (From doc 02 — creates the broker's .dat file with all regions.)
        // The metadata file now includes counter values, counter metadata,
        // and error log buffers after the ring buffers.
        const total_file_size = constants.metadata_header_length +
            config.control_buffer_size + constants.ring_buffer_trailer_length +
            config.messages_buffer_size + constants.ring_buffer_trailer_length +
            config.counter_values_buffer_size +
            config.counter_metadata_buffer_size +
            config.error_log_buffer_size;

        // ... mmap the file (see doc 02) ...
        // Assume `mapped` is the mmap'd slice.
        const mapped = try createAndMapBrokerFile(allocator, &config, total_file_size);

        // ── Step 3: Slice out buffer regions ────────────────────
        var offset: usize = constants.metadata_header_length;

        const control_rb_slice = mapped[offset..][0 .. config.control_buffer_size + constants.ring_buffer_trailer_length];
        offset += config.control_buffer_size + constants.ring_buffer_trailer_length;

        const send_rb_slice = mapped[offset..][0 .. config.messages_buffer_size + constants.ring_buffer_trailer_length];
        offset += config.messages_buffer_size + constants.ring_buffer_trailer_length;

        const counter_values_slice = mapped[offset..][0..config.counter_values_buffer_size];
        offset += config.counter_values_buffer_size;

        const counter_metadata_slice = mapped[offset..][0..config.counter_metadata_buffer_size];
        offset += config.counter_metadata_buffer_size;

        const error_log_slice = mapped[offset..][0..config.error_log_buffer_size];
        offset += config.error_log_buffer_size;

        // ── Step 4: Initialize monitoring subsystem ─────────────
        var counters_manager = CountersManager.init(
            @alignCast(counter_values_slice),
            counter_metadata_slice,
        );
        var system_counters = try SystemCounters.init(&counters_manager);
        const error_log = ErrorLog.init(error_log_slice);

        // ── Step 5: Initialize monitoring dump (optional) ───────
        const monitoring_dump = PeriodicMonitoringDump.init(
            config.node_id,
            &system_counters,
            &error_log,
        );

        // ── Step 6: Create ring buffers (from doc 03) ───────────
        // ... same as before, using control_rb_slice and send_rb_slice ...

        // ── Step 7: Create event loops (from doc 10) ────────────
        // Pass &system_counters and &error_log to every event loop.
        // ...

        // ── Step 8: Create cluster manager (from doc 11) ────────
        // ...

        std.log.info("RingLoom Broker initialized: counters={d}, error_log={d}KB", .{
            SystemCounters.SystemCounter.count,
            config.error_log_buffer_size / 1024,
        });

        // ... allocate and return Broker struct ...
        const broker = try allocator.create(Broker);
        broker.* = .{
            .config = config,
            .allocator = allocator,
            .counters_manager = counters_manager,
            .system_counters = system_counters,
            .error_log = error_log,
            .monitoring_dump = monitoring_dump,
            // ... other fields ...
        };

        return broker;
    }
};
```

**Startup sequence summary:**

```
1. Load config        → BrokerConfig (immutable after this point)
2. Create metadata file → mmap'd memory region
3. Slice regions      → control RB, send RB, counter values, counter metadata, error log
4. Init CountersManager → operates over counter values + metadata slices
5. Init SystemCounters  → allocates all 22 well-known counters
6. Init ErrorLog       → operates over error log slice
7. Init ring buffers   → control + send (from doc 03)
8. Init event loops    → inject counters + error_log (from doc 10)
9. Init cluster manager → inject counters (from doc 11)
10. Start threads      → based on threading_mode (from doc 10)
```

---

## 9. Constants Reference

This section consolidates every constant used across the entire broker implementation.
All values are defined in `src/platform/constants.zig` (doc 01) and
`src/memory/constants.zig` (doc 02). They are reproduced here as a complete reference.

### 9.1 Buffer Size Constants

| Constant | Value | Unit | Defined In | Description |
|---|---|---|---|---|
| `cache_line_length` | `64` | bytes | doc 01 | Hardware cache line size (x86-64, ARM64) |
| `cache_line_pad` | `128` | bytes | doc 01 | Two cache lines — prevents false sharing between adjacent atomics |
| `page_size` | `4096` | bytes | doc 01 | OS memory page size for mmap alignment |
| `metadata_header_length` | `512` | bytes | doc 01 | Metadata header at the start of every `.dat` file |
| `ring_buffer_trailer_length` | `768` | bytes | doc 01 | 6 × 128-byte padded slots: begin_pad, tail, head_cache, head, correlation, heartbeat |
| `ring_buffer_record_header_length` | `8` | bytes | doc 01 | `i32 length` + `i32 msg_type_id` |
| `ring_buffer_alignment` | `8` | bytes | doc 01 | Record alignment within ring buffers |
| `recv_log_metadata_length` | `256` | bytes | doc 01 | (Reserved — no longer used with TCP transport) |
| `frame_header_length` | `24` | bytes | doc 01 | On-wire TCP message frame header |
| `blocking_trailer_length` | `384` | bytes | doc 02 | 3 × 128-byte padded slots for blocking mode (writer wait, reader wait, timeout) |
| `counter_value_length` | `128` | bytes | doc 03 | Per-counter value slot (i64 + 120 bytes padding) |
| `counter_metadata_length` | `256` | bytes | doc 03 | Per-counter metadata slot (state + type_id + label_len + label) |
| `entry_header_length` | `24` | bytes | doc 03 | Error log entry header (length + obs_count + last_ts + first_ts) |
| `entry_alignment` | `4` | bytes | doc 03 | Error log entry alignment |
| `max_error_message_length` | `8192` | bytes | doc 03 | Thread-local error state message buffer |

### 9.2 Protocol Constants

| Constant | Value | Description |
|---|---|---|
| `protocol_version` | `1` | TCP wire protocol version |
| `handshake_magic` | `0x474E4952` | "RING" — TCP handshake magic bytes |
| `handshake_length` | `24` | bytes | TCP handshake frame length |
| `padding_msg_type_id` | `-1` | Sentinel msg_type_id for padding records in ring buffers |
| `flag_admin` | `0x20` | Admin / cluster management message |
| `direction_send` | `0` | Handshake direction: sender connecting |
| `direction_recv` | `1` | Handshake direction: receiver connecting |
| `broker_service_id` | `0` | Broker is always service ID 0 |
| `broker_service_name` | `"broker"` | Broker service name |
| `heartbeat_template_id` | `0xFFFF` | Template ID used for heartbeat frames |

### 9.3 Timing Constants

| Constant | Value | Unit | Description |
|---|---|---|---|
| `heartbeat_interval_ms` | `500` | ms | Sender emits heartbeat frames to idle peers |
| `heartbeat_timeout_ms` | `2000` | ms | Receiver marks peer suspect if no data within this window |
| `peer_liveness_timeout_ms` | `5000` | ms | Peer declared dead if no data within this window |
| `reconnect_base_delay_ms` | `100` | ms | Initial reconnect backoff delay |
| `reconnect_max_delay_ms` | `1000` | ms | Maximum reconnect backoff delay |
| `service_heartbeat_write_interval_ms` | `1000` | ms | Services write heartbeat timestamps at this rate |
| `service_heartbeat_check_interval_ms` | `3000` | ms | Broker scans for stale service heartbeats at this rate |
| `service_heartbeat_timeout_ms` | `10000` | ms | Service declared dead if no heartbeat within this window |
| `control_loop_timeout_check_interval_ns` | `1s` | ns | Control loop checks for timed-out services |
| `broker_heartbeat_interval_ns` | `1s` | ns | Broker-to-broker admin heartbeat interval |
| `election_window_ns` | `5s` | ns | Time window to collect election responses before declaring a winner |
| `command_drain_limit` | `1` | count | Max commands drained from inter-thread queue per duty cycle |
| `control_read_limit` | `10` | count | Max control messages read per duty cycle |
| `write_budget_per_peer` | `16` | count | Max TCP frames written per peer per duty cycle |
| `read_budget_per_peer` | `16` | count | Max TCP frames read per peer per duty cycle |
| `send_batch_limit` | `64` | count | Max messages read from send ring buffer per duty cycle |

### 9.4 Memory Ordering Summary

This table summarizes the atomic memory ordering used at every synchronization point in
the broker. The ordering chosen is the **minimum** required for correctness.

| Operation | Ordering | Reason |
|---|---|---|
| Ring buffer tail CAS (producer claim) | `.acq_rel` | Acquire: see prior consumer head advance. Release: subsequent payload writes happen-after the claim. |
| Ring buffer record length commit | `.release` | Consumer must see payload bytes before seeing the positive length. |
| Ring buffer record length read (consumer) | `.acquire` | Pairs with producer's release store — guarantees payload visibility. |
| Ring buffer head position store (consumer) | `.release` | Producers reading head (to check free space) must see it after consumed records are zeroed. |
| Ring buffer head position load (producer) | `.acquire` | Pairs with consumer's release store. |
| Ring buffer head cache store (producer) | `.monotonic` | Head cache is producer-local optimization — no cross-thread guarantee needed. |
| Counter value increment | `.monotonic` | Counters are advisory. Exact ordering vs. other operations is not required. |
| Counter state CAS (allocate) | `.acq_rel` | Must see prior state and metadata writes from a previous owner. |
| Counter state store (free/reclaim) | `.release` | Readers doing forEach must see value=0 before state=RECLAIMED. |
| Error log entry length commit | `.release` | Readers must see description bytes before seeing the positive length. |
| Error log observation count increment | `.monotonic` | Advisory counter — exact ordering not needed. |
| Error log last timestamp store | `.release` | External readers should see a consistent timestamp. |
| Metadata heartbeat_time_ms store | `.release` | Reader (broker) must see timestamp after the value is meaningful. |
| Metadata heartbeat_time_ms load | `.acquire` | Pairs with service's release store. |
| Metadata next_service_id fetchAdd | `.acq_rel` | Each service must get a unique ID — no duplicates. |

### 9.5 Default Configuration Values

These are the defaults compiled into `BrokerConfig` and `ServiceConfig`. They match the
values in the properties table (§2.1) and the constants defined in doc 01.

```zig
// Complete default configuration (for reference)

pub const defaults = struct {
    // Broker
    pub const group_name = "ringloom";
    pub const storage_path = "/dev/shm";
    pub const control_buffer_size: u32 = 65_536;           // 64 KB
    pub const messages_buffer_size: u32 = 1_048_576;       // 1 MB
    pub const peer_write_queue_capacity: u32 = 8_192;      // frames
    pub const max_frame_length: u32 = 1_048_576;           // 1 MB
    pub const tcp_sndbuf_size: u32 = 262_144;              // 256 KB
    pub const tcp_rcvbuf_size: u32 = 262_144;              // 256 KB
    pub const tcp_listen_backlog: u32 = 128;
    pub const heartbeat_interval_ms: u32 = 500;
    pub const heartbeat_timeout_ms: u32 = 2_000;
    pub const reconnect_base_delay_ms: u32 = 100;
    pub const reconnect_max_delay_ms: u32 = 1_000;
    pub const threading_mode = "DEDICATED";
    pub const idle_strategy = "backoff";
    pub const counter_values_buffer_size: u32 = 65_536;    // 64 KB → 512 counters
    pub const error_log_buffer_size: u32 = 262_144;        // 256 KB
    pub const max_services: u16 = 256;
    pub const max_peers: u8 = 16;
    pub const io_uring_queue_depth: u32 = 256;
    pub const io_uring_sqpoll = false;
    pub const io_uring_registered_buffers: u32 = 64;

    // Service
    pub const service_control_buffer_size: u32 = 65_536;   // 64 KB
    pub const service_messages_buffer_size: u32 = 1_048_576; // 1 MB
    pub const service_blocking_mode = false;
    pub const service_heartbeat_timeout_ms: u32 = 10_000;
    pub const service_idle_strategy = "backoff";
    pub const service_leader_election_enabled = false;

    // Computed
    pub const max_counter_id: u32 = counter_values_buffer_size / 128; // 512
    pub const counter_metadata_buffer_size: u32 = max_counter_id * 256; // 128 KB
};
```

---

## 10. Testing

### 10.1 Config Loading Tests

```zig
// src/config/config_loader_test.zig

const std = @import("std");
const testing = std.testing;
const ConfigLoader = @import("config_loader.zig").ConfigLoader;
const BrokerConfig = @import("broker_config.zig").BrokerConfig;

test "load valid config from properties string" {
    // Given
    const content =
        \\# Test configuration
        \\broker.node.id=1
        \\broker.local.host.port=10.0.0.1:9000
        \\broker.member.host.ports=2@10.0.0.2:9000,3@10.0.0.3:9000
        \\broker.group.name=test-group
        \\broker.control.buffer.size=131072
        \\broker.threading.mode=SHARED
    ;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const allocator = arena.allocator();

    const loader = ConfigLoader.init(allocator);

    // When
    const config = try loader.parseAndBuild(content);

    // Then
    try testing.expectEqual(@as(u8, 1), config.node_id);
    try testing.expectEqualStrings("10.0.0.1", config.local_host);
    try testing.expectEqual(@as(u16, 9000), config.local_port);
    try testing.expectEqual(@as(usize, 2), config.peer_endpoints.len);
    try testing.expectEqual(@as(u8, 2), config.peer_endpoints[0].node_id);
    try testing.expectEqualStrings("10.0.0.2", config.peer_endpoints[0].host);
    try testing.expectEqual(@as(u16, 9000), config.peer_endpoints[0].port);
    try testing.expectEqualStrings("test-group", config.group_name);
    try testing.expectEqual(@as(u32, 131072), config.control_buffer_size);
    try testing.expectEqual(BrokerConfig.ThreadingMode.shared, config.threading_mode);
    try testing.expect(!config.single_node_cluster);
}

test "default values are applied for omitted properties" {
    // Given
    const content =
        \\broker.node.id=0
        \\broker.local.host.port=0.0.0.0:9000
    ;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const loader = ConfigLoader.init(arena.allocator());

    // When
    const config = try loader.parseAndBuild(content);

    // Then
    try testing.expectEqualStrings("ringloom", config.group_name);
    try testing.expectEqualStrings("/dev/shm", config.storage_path);
    try testing.expectEqual(@as(u32, 65_536), config.control_buffer_size);
    try testing.expectEqual(@as(u32, 1_048_576), config.messages_buffer_size);
    try testing.expect(config.single_node_cluster);
}

test "windows-style line endings are handled" {
    // Given
    const content = "broker.node.id=5\r\nbroker.local.host.port=127.0.0.1:8080\r\n";

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const loader = ConfigLoader.init(arena.allocator());

    // When
    const config = try loader.parseAndBuild(content);

    // Then
    try testing.expectEqual(@as(u8, 5), config.node_id);
    try testing.expectEqual(@as(u16, 8080), config.local_port);
}

test "comments and blank lines are skipped" {
    // Given
    const content =
        \\# This is a comment
        \\! This is also a comment
        \\
        \\broker.node.id=3
        \\
        \\# Another comment
        \\broker.local.host.port=10.0.0.3:9000
    ;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const loader = ConfigLoader.init(arena.allocator());

    // When
    const config = try loader.parseAndBuild(content);

    // Then
    try testing.expectEqual(@as(u8, 3), config.node_id);
}

test "colon separator is accepted" {
    // Given
    const content =
        \\broker.node.id: 7
        \\broker.local.host.port: 10.0.0.7:9000
    ;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const loader = ConfigLoader.init(arena.allocator());

    // When
    const config = try loader.parseAndBuild(content);

    // Then
    try testing.expectEqual(@as(u8, 7), config.node_id);
}
```

### 10.2 Config Validation Tests

```zig
// src/config/config_validation_test.zig

const std = @import("std");
const testing = std.testing;
const ConfigLoader = @import("config_loader.zig").ConfigLoader;
const ConfigError = @import("config_loader.zig").ConfigError;

test "missing broker.node.id returns MissingRequiredProperty" {
    // Given
    const content =
        \\broker.local.host.port=10.0.0.1:9000
    ;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const loader = ConfigLoader.init(arena.allocator());

    // When
    const result = loader.parseAndBuild(content);

    // Then
    try testing.expectError(ConfigError.MissingRequiredProperty, result);
}

test "missing broker.local.host.port returns MissingRequiredProperty" {
    // Given
    const content =
        \\broker.node.id=1
    ;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const loader = ConfigLoader.init(arena.allocator());

    // When
    const result = loader.parseAndBuild(content);

    // Then
    try testing.expectError(ConfigError.MissingRequiredProperty, result);
}

test "non-power-of-2 buffer size is auto-aligned" {
    // Given — 100_000 is not a power of 2, should round up to 131_072
    const content =
        \\broker.node.id=1
        \\broker.local.host.port=10.0.0.1:9000
        \\broker.control.buffer.size=100000
    ;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const loader = ConfigLoader.init(arena.allocator());

    // When
    const config = try loader.parseAndBuild(content);

    // Then
    try testing.expectEqual(@as(u32, 131_072), config.control_buffer_size);
    try testing.expect(std.math.isPowerOfTwo(config.control_buffer_size));
}

test "buffer size too small returns BufferSizeTooSmall" {
    // Given
    const content =
        \\broker.node.id=1
        \\broker.local.host.port=10.0.0.1:9000
        \\broker.control.buffer.size=256
    ;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const loader = ConfigLoader.init(arena.allocator());

    // When
    const result = loader.parseAndBuild(content);

    // Then
    try testing.expectError(ConfigError.BufferSizeTooSmall, result);
}

test "node ID conflicting with a peer returns NodeIdConflict" {
    // Given
    const content =
        \\broker.node.id=2
        \\broker.local.host.port=10.0.0.1:9000
        \\broker.member.host.ports=2@10.0.0.2:9000
    ;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const loader = ConfigLoader.init(arena.allocator());

    // When
    const result = loader.parseAndBuild(content);

    // Then
    try testing.expectError(ConfigError.NodeIdConflict, result);
}

test "invalid peer format returns InvalidPeerFormat" {
    // Given — missing @ separator
    const content =
        \\broker.node.id=1
        \\broker.local.host.port=10.0.0.1:9000
        \\broker.member.host.ports=10.0.0.2:9000
    ;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const loader = ConfigLoader.init(arena.allocator());

    // When
    const result = loader.parseAndBuild(content);

    // Then
    try testing.expectError(ConfigError.InvalidPeerFormat, result);
}

test "MTU out of range returns InvalidValue" {
    // Given
    const content =
        \\broker.node.id=1
        \\broker.local.host.port=10.0.0.1:9000
        \\broker.mtu.length=100
    ;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const loader = ConfigLoader.init(arena.allocator());

    // When
    const result = loader.parseAndBuild(content);

    // Then
    try testing.expectError(ConfigError.InvalidValue, result);
}

test "computed fields are set after validation" {
    // Given
    const content =
        \\broker.node.id=1
        \\broker.local.host.port=10.0.0.1:9000
        \\broker.counter.values.buffer.size=65536
    ;

    var arena = std.heap.ArenaAllocator.init(std.heap.page_allocator);
    defer arena.deinit();
    const loader = ConfigLoader.init(arena.allocator());

    // When
    const config = try loader.parseAndBuild(content);

    // Then — 65536 / 128 = 512 max counters
    try testing.expectEqual(@as(u32, 512), config.max_counter_id);
    // 512 * 256 = 131072 bytes metadata
    try testing.expectEqual(@as(u32, 131_072), config.counter_metadata_buffer_size);
}
```

### 10.3 System Counter Tests

```zig
// src/monitoring/system_counters_test.zig

const std = @import("std");
const testing = std.testing;
const CountersManager = @import("../concurrent/counters.zig").CountersManager;
const SystemCounters = @import("system_counters.zig").SystemCounters;
const SystemCounter = @import("system_counter.zig").SystemCounter;

fn createTestBuffers() struct { values: []align(128) u8, metadata: []u8 } {
    // 64 counters × 128 bytes = 8192 bytes for values
    const values = std.heap.page_allocator.alignedAlloc(u8, 128, 8192) catch @panic("alloc");
    @memset(values, 0);
    // 64 counters × 256 bytes = 16384 bytes for metadata
    const metadata = std.heap.page_allocator.alloc(u8, 16384) catch @panic("alloc");
    @memset(metadata, 0);
    return .{ .values = values, .metadata = metadata };
}

test "SystemCounters init allocates all well-known counters" {
    // Given
    const bufs = createTestBuffers();
    defer std.heap.page_allocator.free(bufs.values);
    defer std.heap.page_allocator.free(bufs.metadata);

    var manager = CountersManager.init(bufs.values, bufs.metadata);

    // When
    const counters = try SystemCounters.init(&manager);

    // Then — all 22 counters should be allocated with value 0
    inline for (0..SystemCounter.count) |i| {
        const sc: SystemCounter = @enumFromInt(i);
        try testing.expectEqual(@as(i64, 0), counters.get(sc));
    }
}

test "increment and get" {
    // Given
    const bufs = createTestBuffers();
    defer std.heap.page_allocator.free(bufs.values);
    defer std.heap.page_allocator.free(bufs.metadata);

    var manager = CountersManager.init(bufs.values, bufs.metadata);
    const counters = try SystemCounters.init(&manager);

    // When
    counters.increment(.bytes_sent);
    counters.increment(.bytes_sent);
    counters.increment(.bytes_sent);

    // Then
    try testing.expectEqual(@as(i64, 3), counters.get(.bytes_sent));
}

test "add delta" {
    // Given
    const bufs = createTestBuffers();
    defer std.heap.page_allocator.free(bufs.values);
    defer std.heap.page_allocator.free(bufs.metadata);

    var manager = CountersManager.init(bufs.values, bufs.metadata);
    const counters = try SystemCounters.init(&manager);

    // When
    counters.add(.bytes_sent, 1500);
    counters.add(.bytes_sent, 2048);

    // Then
    try testing.expectEqual(@as(i64, 3548), counters.get(.bytes_sent));
}

test "set absolute value" {
    // Given
    const bufs = createTestBuffers();
    defer std.heap.page_allocator.free(bufs.values);
    defer std.heap.page_allocator.free(bufs.metadata);

    var manager = CountersManager.init(bufs.values, bufs.metadata);
    const counters = try SystemCounters.init(&manager);

    // When
    counters.set(.control_loop_cycle_time_max, 42_000);

    // Then
    try testing.expectEqual(@as(i64, 42_000), counters.get(.control_loop_cycle_time_max));
}

test "updateMax only updates when new value is larger" {
    // Given
    const bufs = createTestBuffers();
    defer std.heap.page_allocator.free(bufs.values);
    defer std.heap.page_allocator.free(bufs.metadata);

    var manager = CountersManager.init(bufs.values, bufs.metadata);
    const counters = try SystemCounters.init(&manager);

    // When
    counters.updateMax(.sender_cycle_time_max, 100);
    counters.updateMax(.sender_cycle_time_max, 50);   // Should not update (50 < 100).
    counters.updateMax(.sender_cycle_time_max, 200);

    // Then
    try testing.expectEqual(@as(i64, 200), counters.get(.sender_cycle_time_max));
}

test "SystemCounter.label returns non-empty strings for all counters" {
    // Given / When / Then
    inline for (0..SystemCounter.count) |i| {
        const sc: SystemCounter = @enumFromInt(i);
        try testing.expect(sc.label().len > 0);
    }
}
```

### 10.4 Error Log Integration Tests

```zig
// src/monitoring/error_log_integration_test.zig

const std = @import("std");
const testing = std.testing;
const ErrorLog = @import("../concurrent/error_log.zig").ErrorLog;

test "record and iterate with formatted error descriptions" {
    // Given
    var buffer: [4096]u8 = undefined;
    @memset(&buffer, 0);
    var log = ErrorLog.init(&buffer);

    // When — record a formatted error
    var desc_buf: [128]u8 = undefined;
    const desc = std.fmt.bufPrint(&desc_buf, "unknown service_id={d} from node={d}", .{
        @as(u16, 42),
        @as(u8, 3),
    }) catch unreachable;
    _ = log.record(desc, 1000);

    // Then
    var found = false;
    log.forEach(struct {
        fn check(entry: ErrorLog.Entry) void {
            _ = entry;
        }
    }.check);

    // Manual iteration to check content.
    const entry_length_ptr: *const i32 = @ptrCast(@alignCast(buffer[0..4]));
    const entry_length = entry_length_ptr.*;
    try testing.expect(entry_length > 0);

    const desc_len: usize = @intCast(entry_length - 24);
    const recorded_desc = buffer[24..][0..desc_len];
    try testing.expectEqualStrings("unknown service_id=42 from node=3", recorded_desc);
    found = true;
    try testing.expect(found);
}

test "same error description deduplicates with observation count" {
    // Given
    var buffer: [4096]u8 = undefined;
    @memset(&buffer, 0);
    var log = ErrorLog.init(&buffer);

    const desc = "send ring buffer full";

    // When — record the same error 5 times
    _ = log.record(desc, 1000);
    _ = log.record(desc, 2000);
    _ = log.record(desc, 3000);
    _ = log.record(desc, 4000);
    _ = log.record(desc, 5000);

    // Then — should be one entry with observation_count=5
    var entry_count: usize = 0;
    log.forEach(struct {
        fn check(entry: ErrorLog.Entry) void {
            _ = entry;
        }
    }.check);

    // Check observation count manually.
    const obs_ptr: *const i32 = @ptrCast(@alignCast(buffer[4..8]));
    try testing.expectEqual(@as(i32, 5), @atomicLoad(i32, obs_ptr, .acquire));

    // Check timestamps.
    const first_ts_ptr: *const i64 = @ptrCast(@alignCast(buffer[16..24]));
    const last_ts_ptr: *const i64 = @ptrCast(@alignCast(buffer[8..16]));
    try testing.expectEqual(@as(i64, 1000), first_ts_ptr.*);
    try testing.expectEqual(@as(i64, 5000), @atomicLoad(i64, last_ts_ptr, .acquire));

    // Only one entry should exist (verify no second entry).
    const first_len: usize = @intCast(@as(*const i32, @ptrCast(@alignCast(buffer[0..4]))).*);
    const next_offset = std.mem.alignForward(usize, first_len, 4);
    const next_len_ptr: *const i32 = @ptrCast(@alignCast(buffer[next_offset..][0..4]));
    try testing.expectEqual(@as(i32, 0), next_len_ptr.*); // No second entry.
    entry_count = 1;
    try testing.expectEqual(@as(usize, 1), entry_count);
}

test "error log full returns false for new errors" {
    // Given — tiny buffer that can hold only one entry
    var buffer: [64]u8 = undefined;
    @memset(&buffer, 0);
    var log = ErrorLog.init(&buffer);

    // When — first entry fits
    const first_result = log.record("error one", 1000);
    // Second entry (different description) won't fit
    const second_result = log.record("error two which is a different and longer error", 2000);

    // Then
    try testing.expect(first_result);
    try testing.expect(!second_result);

    // But recording the first error again (dedup) should still work
    const dedup_result = log.record("error one", 3000);
    try testing.expect(dedup_result);
}
```

### 10.5 Thread-Local Error State Tests

```zig
// src/monitoring/error_state_test.zig

const std = @import("std");
const testing = std.testing;
const error_state = @import("../concurrent/error_state.zig");
const ErrorState = error_state.ErrorState;

test "set and read error" {
    // Given
    var state = ErrorState{};

    // When
    state.set(-1, "something went wrong");

    // Then
    try testing.expectEqual(@as(i32, -1), state.errcode);
    try testing.expect(state.isSet());
    try testing.expectEqualStrings("something went wrong", state.message().?);
}

test "clear resets error" {
    // Given
    var state = ErrorState{};
    state.set(-1, "error");

    // When
    state.clear();

    // Then
    try testing.expectEqual(@as(i32, 0), state.errcode);
    try testing.expect(!state.isSet());
    try testing.expectEqual(@as(?[]const u8, null), state.message());
}

test "setFmt formats error message" {
    // Given
    var state = ErrorState{};

    // When
    state.setFmt(-2, "failed for service_id={d} node={d}", .{ @as(u16, 42), @as(u8, 7) });

    // Then
    try testing.expectEqual(@as(i32, -2), state.errcode);
    try testing.expectEqualStrings("failed for service_id=42 node=7", state.message().?);
}

test "truncation on very long message" {
    // Given
    var state = ErrorState{};
    const long_msg = "x" ** (error_state.max_error_message_length + 100);

    // When
    state.set(-1, long_msg);

    // Then — should be truncated to max_error_message_length
    try testing.expectEqual(error_state.max_error_message_length, state.msg_len);
    try testing.expect(state.isSet());
}

test "threadlocal err_state is independent per-thread" {
    // Given — the default threadlocal state
    error_state.err_state.clear();
    try testing.expect(!error_state.err_state.isSet());

    // When — set an error on this thread
    error_state.err_state.set(-1, "main thread error");

    // Then — another thread should have its own clean state
    const thread = try std.Thread.spawn(.{}, struct {
        fn run() void {
            // This thread's err_state should be clean.
            std.debug.assert(!error_state.err_state.isSet());
            // Set something to verify independence.
            error_state.err_state.set(-2, "worker thread error");
            std.debug.assert(error_state.err_state.errcode == -2);
        }
    }.run, .{});
    thread.join();

    // Main thread's state should be unaffected.
    try testing.expectEqual(@as(i32, -1), error_state.err_state.errcode);
    try testing.expectEqualStrings("main thread error", error_state.err_state.message().?);
}
```

### 10.6 Multi-Threaded Counter Tests

```zig
// src/monitoring/counter_stress_test.zig

const std = @import("std");
const testing = std.testing;
const CountersManager = @import("../concurrent/counters.zig").CountersManager;
const SystemCounters = @import("system_counters.zig").SystemCounters;
const SystemCounter = @import("system_counter.zig").SystemCounter;

test "concurrent increments from multiple threads converge to correct sum" {
    // Given
    const num_threads = 4;
    const increments_per_thread = 100_000;

    const values = try std.heap.page_allocator.alignedAlloc(u8, 128, 8192);
    defer std.heap.page_allocator.free(values);
    @memset(values, 0);

    const metadata = try std.heap.page_allocator.alloc(u8, 16384);
    defer std.heap.page_allocator.free(metadata);
    @memset(metadata, 0);

    var manager = CountersManager.init(values, metadata);
    const counters = try SystemCounters.init(&manager);

    // When — spawn N threads, each incrementing bytes_sent M times
    var threads: [num_threads]std.Thread = undefined;
    for (&threads) |*t| {
        t.* = try std.Thread.spawn(.{}, struct {
            fn run(c: *const SystemCounters) void {
                for (0..increments_per_thread) |_| {
                    c.increment(.bytes_sent);
                }
            }
        }.run, .{&counters});
    }

    for (&threads) |*t| {
        t.join();
    }

    // Then — total should be num_threads × increments_per_thread
    const expected: i64 = num_threads * increments_per_thread;
    try testing.expectEqual(expected, counters.get(.bytes_sent));
}

test "concurrent add from multiple threads" {
    // Given
    const num_threads = 4;
    const adds_per_thread = 50_000;
    const delta: i64 = 100;

    const values = try std.heap.page_allocator.alignedAlloc(u8, 128, 8192);
    defer std.heap.page_allocator.free(values);
    @memset(values, 0);

    const metadata = try std.heap.page_allocator.alloc(u8, 16384);
    defer std.heap.page_allocator.free(metadata);
    @memset(metadata, 0);

    var manager = CountersManager.init(values, metadata);
    const counters = try SystemCounters.init(&manager);

    // When
    var threads: [num_threads]std.Thread = undefined;
    for (&threads) |*t| {
        t.* = try std.Thread.spawn(.{}, struct {
            fn run(c: *const SystemCounters) void {
                for (0..adds_per_thread) |_| {
                    c.add(.bytes_received, delta);
                }
            }
        }.run, .{&counters});
    }

    for (&threads) |*t| {
        t.join();
    }

    // Then
    const expected: i64 = num_threads * adds_per_thread * delta;
    try testing.expectEqual(expected, counters.get(.bytes_received));
}
```

### 10.7 Monitoring Snapshot Tests

```zig
// src/monitoring/monitoring_test.zig

const std = @import("std");
const testing = std.testing;
const CountersManager = @import("../concurrent/counters.zig").CountersManager;
const ErrorLog = @import("../concurrent/error_log.zig").ErrorLog;
const SystemCounters = @import("system_counters.zig").SystemCounters;
const SystemCounter = @import("system_counter.zig").SystemCounter;
const MonitoringSnapshot = @import("monitoring.zig").MonitoringSnapshot;

test "snapshot captures current counter values" {
    // Given
    const values = try std.heap.page_allocator.alignedAlloc(u8, 128, 8192);
    defer std.heap.page_allocator.free(values);
    @memset(values, 0);

    const metadata = try std.heap.page_allocator.alloc(u8, 16384);
    defer std.heap.page_allocator.free(metadata);
    @memset(metadata, 0);

    var manager = CountersManager.init(values, metadata);
    const counters = try SystemCounters.init(&manager);

    counters.add(.bytes_sent, 42_000);
    counters.increment(.messages_routed_local);

    var error_buf: [4096]u8 = undefined;
    @memset(&error_buf, 0);
    const error_log = ErrorLog.init(&error_buf);

    // When
    const snapshot = MonitoringSnapshot.take(1, &counters, &error_log);

    // Then
    try testing.expectEqual(@as(u8, 1), snapshot.node_id);
    try testing.expectEqual(@as(i64, 42_000), snapshot.counters[@intFromEnum(SystemCounter.bytes_sent)].value);
    try testing.expectEqual(@as(i64, 1), snapshot.counters[@intFromEnum(SystemCounter.messages_routed_local)].value);
}

test "snapshot captures error log entries" {
    // Given
    const values = try std.heap.page_allocator.alignedAlloc(u8, 128, 8192);
    defer std.heap.page_allocator.free(values);
    @memset(values, 0);

    const metadata = try std.heap.page_allocator.alloc(u8, 16384);
    defer std.heap.page_allocator.free(metadata);
    @memset(metadata, 0);

    var manager = CountersManager.init(values, metadata);
    const counters = try SystemCounters.init(&manager);

    var error_buf: [4096]u8 = undefined;
    @memset(&error_buf, 0);
    var error_log = ErrorLog.init(&error_buf);
    _ = error_log.record("test error one", 1000);
    _ = error_log.record("test error two", 2000);

    // When
    const snapshot = MonitoringSnapshot.take(0, &counters, &error_log);

    // Then
    try testing.expectEqual(@as(usize, 2), snapshot.error_count);
    try testing.expectEqualStrings("test error one", snapshot.errors[0].description);
    try testing.expectEqualStrings("test error two", snapshot.errors[1].description);
}

test "snapshot dump produces non-empty output" {
    // Given
    const values = try std.heap.page_allocator.alignedAlloc(u8, 128, 8192);
    defer std.heap.page_allocator.free(values);
    @memset(values, 0);

    const metadata = try std.heap.page_allocator.alloc(u8, 16384);
    defer std.heap.page_allocator.free(metadata);
    @memset(metadata, 0);

    var manager = CountersManager.init(values, metadata);
    const counters = try SystemCounters.init(&manager);
    counters.increment(.bytes_sent);

    var error_buf: [4096]u8 = undefined;
    @memset(&error_buf, 0);
    const error_log = ErrorLog.init(&error_buf);

    const snapshot = MonitoringSnapshot.take(1, &counters, &error_log);

    // When
    var output_buf: [8192]u8 = undefined;
    var fbs = std.io.fixedBufferStream(&output_buf);
    try snapshot.dump(fbs.writer());

    // Then
    const output = fbs.getWritten();
    try testing.expect(output.len > 0);
    try testing.expect(std.mem.indexOf(u8, output, "bytes-sent") != null);
    try testing.expect(std.mem.indexOf(u8, output, "Node 1") != null);
}
```

### 10.8 Testing Tips

- **Arena allocator for config tests.** Use `std.heap.ArenaAllocator` so all config
  string allocations are freed in one shot via `arena.deinit()`. This avoids tracking
  individual allocations in test code.

- **Aligned buffers for counter tests.** Counter values require 128-byte alignment
  (cache-line pad). Use `std.heap.page_allocator.alignedAlloc(u8, 128, size)` to get
  properly aligned memory.

- **Zero-initialize test buffers.** Always `@memset(buffer, 0)` before creating a
  `CountersManager` or `ErrorLog` — the initial state depends on zero-filled memory
  (counter state `UNUSED = 0`, error log entry length `0 = empty`).

- **Thread join before assertions.** In multi-threaded counter tests, always `join()` all
  threads before reading counter values. Without the join, the values are racy.

- **No mocking needed.** The `SystemCounters` struct takes a `*CountersManager` pointer
  — pass a stack-local or heap-allocated instance. No need for mock frameworks.

- **Error log buffer size in tests.** Use a small buffer (4 KB) for unit tests so the
  "log full" behavior can be tested without allocating 256 KB per test.

- **Test `alignToPowerOfTwo` edge cases.** Zero → 1, 1 → 1, max u32 power of 2
  (2^31 = 2_147_483_648) → stays the same, just above a power of 2 → next one.

---

## 11. File Structure

```
src/
  config/
    broker_config.zig             # BrokerConfig struct, ThreadingMode, IdleStrategyName, PeerEndpoint
    service_config.zig            # ServiceConfig struct
    config_loader.zig             # ConfigLoader: load(), parseAndBuild(), parseProperties(), validate()
  monitoring/
    system_counter.zig            # SystemCounter enum(u8) — well-known counter IDs + labels
    system_counters.zig           # SystemCounters — typed wrapper around CountersManager
    cycle_time.zig                # CycleTimeTracker — per-event-loop max cycle time tracking
    counter_snapshot.zig          # CounterSnapshot — point-in-time counter read
    monitoring.zig                # MonitoringSnapshot — counters + error log snapshot + dump()
    periodic_dump.zig             # PeriodicMonitoringDump — optional stderr dump on control loop
  concurrent/
    counters.zig                  # CountersManager (defined in doc 03 — referenced here)
    error_log.zig                 # ErrorLog (defined in doc 03 — referenced here)
    error_state.zig               # ErrorState + threadlocal (defined in doc 03 — referenced here)
tools/
  ringloom_stat.zig                   # Standalone monitoring tool — reads broker .dat file
```

**Dependency graph (within this step's new modules):**

```
broker_config.zig                ◄── config_loader.zig (parsing + validation)
       │
       ▼
service_config.zig               ◄── standalone, no internal deps

system_counter.zig               ◄── system_counters.zig (counter ID enum)
       │
       ▼
system_counters.zig              ◄── cycle_time.zig, counter_snapshot.zig, monitoring.zig
       │
       ├── concurrent/counters.zig (CountersManager from doc 03)
       │
       ▼
monitoring.zig                   ◄── periodic_dump.zig
       │
       ├── concurrent/error_log.zig (ErrorLog from doc 03)
       │
       ▼
periodic_dump.zig                ◄── broker.zig (wired into control loop)
```

**Cross-module dependencies:**

| This module | Depends on |
|---|---|
| `config_loader.zig` | `broker_config.zig`, `platform/constants.zig` (doc 01) |
| `system_counters.zig` | `concurrent/counters.zig` (doc 03), `system_counter.zig` |
| `cycle_time.zig` | `platform/clock.zig` (doc 01), `system_counters.zig` |
| `monitoring.zig` | `system_counters.zig`, `concurrent/error_log.zig` (doc 03), `platform/clock.zig` (doc 01) |
| `periodic_dump.zig` | `monitoring.zig`, `system_counters.zig`, `platform/clock.zig` (doc 01) |
| `broker.zig` (startup) | All of the above + docs 02–11 |

---

## 12. Implementation Steps

1.  **Create `src/config/broker_config.zig`.** Define `BrokerConfig`, `PeerEndpoint`,
    `ThreadingMode`, `IdleStrategyName` structs and enums. Add `fromString()` methods
    for enum parsing. No external dependencies.

2.  **Create `src/config/service_config.zig`.** Define `ServiceConfig` struct. No
    external dependencies.

3.  **Create `src/config/config_loader.zig`.** Implement `ConfigLoader` with `load()`,
    `loadFromFile()`, `parseAndBuild()`. Implement `parseProperties()` (key=value
    parser), `parsePeerEndpoints()` (nodeId@host:port parser), `applyEnvOverrides()`,
    `validate()`, and `alignToPowerOfTwo()`. Write unit tests for all parsing and
    validation paths.

4.  **Create `src/monitoring/system_counter.zig`.** Define the broker and service
    runtime counter IDs and stable Prometheus-safe labels described in
    [Required Counter Coverage](#36-required-counter-coverage). No external
    dependencies.

5.  **Create `src/monitoring/system_counters.zig`.** Implement `SystemCounters` wrapping
    `CountersManager` from doc 03. Pre-allocate all well-known counters in `init()`.
    Provide `increment()`, `add()`, `set()`, `get()`, `updateMax()`.

6.  **Create `src/monitoring/cycle_time.zig`.** Implement `CycleTimeTracker` with
    `start()`, `stop()`, and periodic reset. Depends on `clock.zig` (doc 01) and
    `SystemCounters`.

7.  **Create `src/monitoring/counter_snapshot.zig`.** Implement `CounterSnapshot.take()`
    that reads all counter values into a stack-local struct.

8.  **Create `src/monitoring/monitoring.zig`.** Implement `MonitoringSnapshot` with
    `take()` (counters + error log) and `dump()` (human-readable formatter).

9.  **Create `src/monitoring/periodic_dump.zig`.** Implement `PeriodicMonitoringDump`
    with `doWork()` that periodically dumps a snapshot to stderr. Controlled by
    `RINGLOOM_MONITORING_DUMP` env var.

10. **Move counter buffers into broker metadata.** Extend `BrokerMetadataFile` so the
    generic counter values, counter metadata, and error-log regions are appended after
    broker ring buffers, flow-control state, and per-peer send counters. Add header fields
    for monitoring offsets and lengths and keep zero values backward-compatible.

11. **Add counter buffers to service metadata.** Extend `ServiceMetadataFile` so every
    service owns counter values, counter metadata, and error-log regions after its control
    and messages rings. Initialize a service `CountersManager` during `RingLoomEngine.start`.

12. **Wire counters into broker event loops.** Extend `ControlLoop` (doc 09/10),
    `SenderEventLoop` (doc 05), and `ReceiverEventLoop` (doc 06) to accept
    `*const SystemCounters` and `*ErrorLog` pointers. Add counter increments at every
    relevant point (bytes sent/received, messages routed, back-pressure events,
    heartbeats).

13. **Wire counters into service runtime.** Update `ServiceClient`, `IpcProducer`,
    `MessageConsumer`, and `ControlAgent` to record send, receive, flow-control,
    backpressure, registry, control-message, and heartbeat counters in the service metadata
    counter region.

14. **Wire cycle time tracking into event loops.** Add `CycleTimeTracker` to each event
    loop's `doWork()`: call `start()` at the top, `stop()` at the bottom.

15. **Wire `PeriodicMonitoringDump` into control loop.** Add
    `monitoring_dump.doWork()` as the last step in the control loop's `doWork()`.

16. **Update broker startup (`src/broker.zig`).** Load config via `ConfigLoader`,
    compute total file size including counter + error log regions, allocate and mmap
    the file, slice out regions, initialize `CountersManager`, `SystemCounters`,
    `ErrorLog`, `PeriodicMonitoringDump`, and inject them into all event loops.

17. **Update `tools/ringloom_stat.zig`.** Keep it as the human inspection tool, but teach it
    to read both broker and service counter regions, error logs, ring occupancy, flow-control
    entries, and per-peer send entries.

18. **Create `tools/ringloom_observability.zig`.** Implement the Prometheus exporter
    described in [Observability](../observability.md) and add `zig build observability`
    plus `zig build run-observability` steps.

19. **Write integration tests.** Multi-threaded counter stress test (4 threads × 100K
    increments). Monitoring snapshot test. Config round-trip test (write properties
    file, load it, verify all fields). Error log deduplication test. Metadata-tail offset
    tests for broker and service files. Exporter scrape-format tests for broker and service
    counters.

---

*Previous: [11 — Cluster Management](11-cluster-management.md)*
·
*Start: [00 — Overview & Index](00-overview.md)*
