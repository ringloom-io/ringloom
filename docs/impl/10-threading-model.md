# 10 — Threading Model

> **Prerequisites:** [01 — Platform Abstraction](01-platform-abstraction.md) (threads,
> idle strategies, `EventLoop`, `ThreadRunner`, `ProcessSynchronizer`),
> [03 — Concurrent Data Structures](03-concurrent-data-structures.md) (MPSC ring buffer),
> [05 — Send Path](05-send-path.md) (sender event loop),
> [06 — Receive Path](06-receive-path.md) (receiver event loop),
> [09 — Control Plane](09-control-plane.md) (control loop).

This document ties together the control loop, sender event loop, and receiver event loop
into a coherent multi-threaded architecture. It covers the event loop interface (already
introduced in doc 01, now refined for the broker), the event loop runner that drives each
thread, idle strategies and their trade-offs, inter-event-loop command passing, threading
modes, shutdown sequencing, thread naming, and optional CPU affinity.

All primitives described here build on the `EventLoop`, `ThreadRunner`, and
`IdleStrategy` types from [01 — Platform Abstraction](01-platform-abstraction.md). This
document extends them with broker-specific wiring and introduces the command queue
mechanism that connects the three event loops without shared mutable state.

---

## Table of Contents

1. [Overview](#1-overview)
2. [Event Loop Interface (Recap & Extension)](#2-event-loop-interface-recap--extension)
3. [Event Loop Runner](#3-event-loop-runner)
4. [Idle Strategies](#4-idle-strategies)
   1. [BusySpin](#41-busyspin-lowest-latency-highest-cpu)
   2. [Yielding](#42-yielding-low-latency)
   3. [Sleeping](#43-sleeping-balanced)
   4. [Backoff](#44-backoff-production-default)
   5. [Blocking](#45-blocking-lowest-cpu-higher-latency)
   6. [Choosing a Strategy](#46-choosing-a-strategy)
5. [Inter-Event-Loop Communication](#5-inter-event-loop-communication)
   1. [Command Struct](#51-command-struct)
   2. [Command Queue](#52-command-queue)
   3. [Command Flow](#53-command-flow)
   4. [Self-Dispatching Commands](#54-self-dispatching-commands)
   5. [Enqueuing a Command (Producer Side)](#55-enqueuing-a-command-producer-side)
   6. [Draining Commands (Consumer Side)](#56-draining-commands-consumer-side)
6. [Threading Modes](#6-threading-modes)
   1. [DEDICATED Mode](#61-dedicated-mode-3-threads)
   2. [SHARED_NETWORK Mode](#62-shared_network-mode-2-threads)
   3. [SHARED Mode](#63-shared-mode-1-thread)
   4. [Mode Selection](#64-mode-selection)
7. [Broker Lifecycle](#7-broker-lifecycle)
   1. [Startup Sequence](#71-startup-sequence)
   2. [Shutdown Sequence](#72-shutdown-sequence)
   3. [Signal Handling](#73-signal-handling)
8. [Thread Naming](#8-thread-naming)
9. [CPU Affinity (Optional)](#9-cpu-affinity-optional)
10. [Service-Side Threading](#10-service-side-threading)
11. [Putting It All Together](#11-putting-it-all-together)
12. [Testing](#12-testing)
13. [File Structure](#13-file-structure)
14. [Implementation Steps](#14-implementation-steps)

---

## 1. Overview

The broker runs **3 threads** in DEDICATED mode (the default and recommended
configuration for production):

```
┌─────────────────────────────────────────────────────────────────┐
│                        Broker Process                           │
│                                                                 │
│  Thread 1: brz-control                                          │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  • Drain inter-loop command queue (1 per cycle)           │  │
│  │  • Poll broker's control ring buffer                      │  │
│  │    – Service registrations / deregistrations              │  │
│  │    – Subscription requests                                │  │
│  │  • Heartbeat checking (every 3s)                          │  │
│  │  • Cluster management, leader election                    │  │
│  │  • Service discovery notifications                        │  │
│  │  • Scheduled tasks, counter updates                       │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  Thread 2: brz-sender                                           │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  • Drain inter-loop command queue (1 per cycle)           │  │
│  │  • Drain send ring buffer (outbound cross-host messages)  │  │
│  │  • TCP write via io_uring (Linux) / kqueue (macOS)        │  │
│  │  • Per-peer write queues + fair round-robin               │  │
│  │  • Send heartbeats (every 500ms per idle peer)            │  │
│  │  • Connection management + reconnection                   │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
│  Thread 3: brz-receiver                                         │
│  ┌───────────────────────────────────────────────────────────┐  │
│  │  • Drain inter-loop command queue (1 per cycle)           │  │
│  │  • TCP read via io_uring (Linux) / kqueue (macOS)         │  │
│  │  • TCP accept + handshake validation                      │  │
│  │  • Route messages to target service ring buffers          │  │
│  │  • Heartbeat timeout detection                            │  │
│  │  • Always-read model (drop on full, never pause)          │  │
│  │  • Handle admin messages (cluster protocol)               │  │
│  └───────────────────────────────────────────────────────────┘  │
│                                                                 │
└─────────────────────────────────────────────────────────────────┘
```

Each **service** process (`BrzEngine`) runs 2 threads:

```
┌─────────────────────────────────────────┐
│           Service Process               │
│                                         │
│  Thread 1: brz-svc-ctrl                 │
│  ┌───────────────────────────────────┐  │
│  │  • Poll service's control RB      │  │
│  │  • Handle registration responses  │  │
│  │  • Write heartbeats               │  │
│  └───────────────────────────────────┘  │
│                                         │
│  Thread 2: brz-svc-msg                  │
│  ┌───────────────────────────────────┐  │
│  │  • Poll service's messages RB     │  │
│  │  • Dispatch to application        │  │
│  │    handlers                       │  │
│  └───────────────────────────────────┘  │
│                                         │
└─────────────────────────────────────────┘
```

**Total thread count** for a host with 1 broker + N services: `3 + 2N` (in DEDICATED
mode). Every thread runs a tight duty-cycle event loop driven by an idle strategy. No
thread ever blocks on a mutex. No thread ever allocates on the hot path.

---

## 2. Event Loop Interface (Recap & Extension)

Document [01 — Platform Abstraction](01-platform-abstraction.md) introduced the
`EventLoop` interface. Here's the definition again for reference, unchanged:

```zig
// src/platform/thread.zig (defined in doc 01)

pub const EventLoop = struct {
    /// Pointer to the concrete event loop implementation.
    context: *anyopaque,

    /// Perform one duty cycle. Returns the number of work items processed.
    /// 0 = idle (triggers idle strategy), >0 = did work (loop again immediately).
    doWorkFn: *const fn (context: *anyopaque) u32,

    /// Called once after the event loop exits. Used for resource cleanup.
    onCloseFn: *const fn (context: *anyopaque) void,

    pub fn doWork(self: EventLoop) u32 {
        return self.doWorkFn(self.context);
    }

    pub fn onClose(self: EventLoop) void {
        self.onCloseFn(self.context);
    }
};
```

Every event loop in the broker implements this interface by providing a `doWork` and
`onClose` function. The contract:

- **`doWork()`** runs exactly one duty cycle. It polls all work sources (command queue,
  ring buffer, timers, I/O completions) and returns the total number of items processed.
  This function must never block. It must never allocate. It should complete as fast as
  possible.

- **`onClose()`** runs once, after the `running` flag goes false and the last `doWork()`
  returns. This is the place to flush pending writes, close sockets, unmap files, and
  release any resources owned by this event loop.

The three broker event loops are:

| Event Loop | Struct | File |
|---|---|---|
| Control Loop | `ControlLoop` | `src/control/control_loop.zig` |
| Sender | `SenderEventLoop` | `src/send/sender_event_loop.zig` |
| Receiver | `ReceiverEventLoop` | `src/recv/receiver_event_loop.zig` |

Each struct stores its own state and exposes the `EventLoop` interface through function
pointers:

```zig
// src/control/control_loop.zig

pub const ControlLoop = struct {
    // ... all control loop state ...
    cmd_queue: *CommandQueue,
    control_rb: *RingBuffer,
    service_registry: *ServiceRegistry,
    cluster_manager: *ClusterManager,
    counters: *CountersManager,
    next_heartbeat_check_ns: i64,
    next_timeout_check_ns: i64,

    /// Return an EventLoop interface backed by this ControlLoop.
    pub fn eventLoop(self: *ControlLoop) EventLoop {
        return .{
            .context = @ptrCast(self),
            .doWorkFn = doWorkFn,
            .onCloseFn = onCloseFn,
        };
    }

    fn doWorkFn(ctx: *anyopaque) u32 {
        const self: *ControlLoop = @ptrCast(@alignCast(ctx));
        return self.doWork();
    }

    fn onCloseFn(ctx: *anyopaque) void {
        const self: *ControlLoop = @ptrCast(@alignCast(ctx));
        self.onClose();
    }

    fn doWork(self: *ControlLoop) u32 {
        var work_count: u32 = 0;
        const now_ns = platform.Clock.monotonicNanos();

        // 1. Drain inter-loop commands (limit: 1 per cycle).
        work_count += self.cmd_queue.drain(constants.command_drain_limit);

        // 2. Poll broker's control ring buffer.
        work_count += self.control_rb.read(self.onControlMessage, constants.control_read_limit);

        // 3. Periodic tasks (rate-limited).
        if (now_ns >= self.next_timeout_check_ns) {
            if (now_ns >= self.next_heartbeat_check_ns) {
                self.checkServiceHeartbeats(now_ns);
                self.next_heartbeat_check_ns = now_ns +
                    constants.service_heartbeat_check_interval_ms * std.time.ns_per_ms;
            }
            self.cluster_manager.doWork(now_ns);
            self.next_timeout_check_ns = now_ns + constants.control_loop_timeout_check_interval_ns;
        }

        // 4. Update counters.
        self.updateCounters();

        return work_count;
    }

    fn onClose(self: *ControlLoop) void {
        self.counters.flush();
        // No sockets to close — control loop is IPC only.
    }

    fn onControlMessage(msg_type_id: i32, payload: []const u8) void {
        // Dispatch by template ID — see doc 09.
        _ = msg_type_id;
        _ = payload;
    }

    fn checkServiceHeartbeats(self: *ControlLoop, now_ns: i64) void {
        _ = self;
        _ = now_ns;
        // See doc 09 for full implementation.
    }

    fn updateCounters(self: *ControlLoop) void {
        _ = self;
    }
};
```

The sender and receiver event loops follow the same pattern. Their `doWork()` functions
are detailed in [05 — Send Path](05-send-path.md) and
[06 — Receive Path](06-receive-path.md) respectively. The key point for this document is
that all three implement `EventLoop` identically.

---

## 3. Event Loop Runner

The `ThreadRunner` from doc 01 is used directly. No wrapper is needed. Here's how the
broker creates and manages its three runners:

```zig
// src/threading/broker_threads.zig

const std = @import("std");
const platform = @import("../platform.zig");
const ThreadRunner = platform.ThreadRunner;
const EventLoop = platform.EventLoop;
const IdleStrategy = platform.IdleStrategy;

pub const BrokerThreads = struct {
    control_runner: ThreadRunner,
    sender_runner: ThreadRunner,
    receiver_runner: ThreadRunner,
    mode: ThreadingMode,

    pub fn init(
        control_loop: EventLoop,
        sender_loop: EventLoop,
        receiver_loop: EventLoop,
        control_idle: IdleStrategy,
        sender_idle: IdleStrategy,
        receiver_idle: IdleStrategy,
    ) BrokerThreads {
        return .{
            .control_runner = ThreadRunner.init("brz-control", control_loop, control_idle),
            .sender_runner = ThreadRunner.init("brz-sender", sender_loop, sender_idle),
            .receiver_runner = ThreadRunner.init("brz-receiver", receiver_loop, receiver_idle),
            .mode = .dedicated,
        };
    }

    /// Start all threads. Order: receiver first, then sender, then control.
    /// Receiver starts first so it's ready to process incoming packets by the
    /// time the sender or control loop triggers outbound traffic.
    pub fn start(self: *BrokerThreads) !void {
        try self.receiver_runner.start();
        try self.sender_runner.start();
        try self.control_runner.start();
    }

    /// Graceful shutdown. Order: receiver, sender, control (reverse of hot-path
    /// dependency — stop accepting new work before stopping producers).
    pub fn shutdown(self: *BrokerThreads) void {
        // 1. Stop receiver first — no new incoming messages.
        self.receiver_runner.stop();
        self.receiver_runner.join();

        // 2. Stop sender — flush remaining sends, then exit.
        self.sender_runner.stop();
        self.sender_runner.join();

        // 3. Stop control loop last — it may need to process final deregistrations.
        self.control_runner.stop();
        self.control_runner.join();
    }

    pub fn isRunning(self: *const BrokerThreads) bool {
        return self.control_runner.running.load();
    }
};
```

The `ThreadRunner.threadMain` function (from doc 01) does the actual work:

```
spawn thread
  → setThreadName(role_name)
  → while (running.load(.acquire)):
      work_count = event_loop.doWork()
      idle_strategy.idle(work_count)
  → event_loop.onClose()
  → thread exits
```

This is the core execution model for every thread in the system. The idle strategy is the
only variable — everything else is the same tight loop.

---

## 4. Idle Strategies

The idle strategy determines the CPU usage vs. latency trade-off when `doWork()` returns
zero. All strategies are defined in the `IdleStrategy` tagged union from doc 01. This
section provides deeper implementation detail and guidance on choosing the right strategy.

### 4.1 BusySpin (Lowest Latency, Highest CPU)

```zig
// Part of IdleStrategy union — .busy_spin variant

// When work_count == 0:
std.atomic.spinLoopHint();
// Compiles to:
//   x86_64: PAUSE instruction (reduces pipeline stalls in spin loops)
//   aarch64: YIELD instruction (similar hint to the CPU)
```

**Latency:** Sub-microsecond wake-up. The thread never leaves the CPU core.

**CPU cost:** 100% of one core per thread. A 3-thread DEDICATED broker burns 3 full
cores.

**When to use:** Benchmark-only, or if you have dedicated cores with nothing else on
them and sub-microsecond latency is mandatory.

### 4.2 Yielding (Low Latency)

```zig
// When work_count == 0:
std.Thread.yield() catch {};
// Calls sched_yield() on POSIX, SwitchToThread() on Windows.
```

**Latency:** ~1–10µs. The thread gives up its timeslice but stays in the run queue.

**CPU cost:** High but not 100% — the OS can schedule other threads on the same core
between yields.

**When to use:** When you want low latency but other work needs CPU time on the same
cores.

### 4.3 Sleeping (Balanced)

```zig
// When work_count == 0:
std.time.sleep(1_000); // 1µs = 1,000ns
```

**Latency:** ~1–50µs depending on OS timer resolution and scheduling.

**CPU cost:** Low. The thread is descheduled for at least 1µs.

**When to use:** Development, low-throughput deployments, or when CPU conservation
matters more than tail latency.

### 4.4 Backoff (Production Default)

The backoff strategy progresses through three phases. When work arrives, it resets to the
first phase immediately.

```zig
// src/platform/thread.zig (defined in doc 01, shown here for completeness)

pub const BackoffState = struct {
    spins: u32 = 0,
    yields: u32 = 0,

    const max_spins: u32 = 10;
    const max_yields: u32 = 20;

    pub fn step(self: *BackoffState) void {
        if (self.spins < max_spins) {
            self.spins += 1;
            std.atomic.spinLoopHint();
        } else if (self.yields < max_yields) {
            self.yields += 1;
            std.Thread.yield() catch {};
        } else {
            std.time.sleep(1_000); // 1µs
        }
    }

    pub fn reset(self: *BackoffState) void {
        self.spins = 0;
        self.yields = 0;
    }
};
```

**Phase transitions (when idle):**

```
work_count > 0 at any point → reset to spinning
          ┌──────┐
          │      ▼
    ┌──────────┐   10 spins   ┌──────────┐   20 yields   ┌──────────┐
    │ Spinning │─────────────►│ Yielding │───────────────►│ Sleeping │
    │  (PAUSE) │              │ (yield)  │                │  (1µs)   │
    └──────────┘              └──────────┘                └──────────┘
          ▲                        ▲                           │
          │                        │                           │
          └────────────────────────┴───── stays here ──────────┘
```

**Latency:** ~0 during spinning phase, ~1–10µs during yielding, ~1–50µs during sleeping.
Sustained idle settles at 1µs sleep granularity.

**CPU cost:** Adapts. Hot bursts use near-zero latency spinning. Quiet periods converge
to sleeping. This is the best general-purpose choice.

**When to use:** Production default. Works well for mixed workloads where message arrival
rates vary.

### 4.5 Blocking (Lowest CPU, Higher Latency)

The blocking strategy uses the platform's `ProcessSynchronizer` to park the thread in the
kernel until explicitly woken. This is the same mechanism used by the blocking ring buffer
extension (doc 03, §1.11).

```zig
// Part of IdleStrategy union — .blocking variant
// The function pointer calls ProcessSynchronizer.wait() internally.

// When work_count == 0:
_ = synchronizer.wait(wait_state_ptr, 0, BLOCK_TIMEOUT_NS);
// Linux:   futex(wait_state_ptr, FUTEX_WAIT, 0, timeout)
// macOS:   __ulock_wait(UL_COMPARE_AND_WAIT, wait_state_ptr, 0, timeout_us)
// Windows: WaitOnAddress(wait_state_ptr, &expected, sizeof(i32), timeout_ms)
```

The producer side (whichever event loop or ring buffer wants to wake this thread) calls:

```zig
synchronizer.wake(wait_state_ptr);
```

**Latency:** ~10–100µs (kernel wake-up path). Worst case depends on OS scheduler.

**CPU cost:** Near zero when idle. The thread is fully descheduled.

**When to use:** When the broker is one of many processes on the host and CPU must be
shared. Also useful for service-side threads that process infrequent messages.

### 4.6 Choosing a Strategy

| Scenario | Control Loop | Sender | Receiver |
|---|---|---|---|
| **Ultra-low latency** (dedicated cores) | `backoff` | `busy_spin` | `busy_spin` |
| **Production default** | `backoff` | `backoff` | `backoff` |
| **Shared host** (conserve CPU) | `blocking` | `backoff` | `backoff` |
| **Testing / development** | `sleeping` | `sleeping` | `sleeping` |

The idle strategy is configured per-thread:

```
# broker.properties
broker.control.idle_strategy = backoff
broker.sender.idle_strategy = backoff
broker.receiver.idle_strategy = backoff
```

---

## 5. Inter-Event-Loop Communication

Event loops never share mutable state directly. No pointer to a `ServiceRegistry` is
read by the sender while the control loop writes to it. Instead, event loops communicate
through **command queues** — small MPSC ring buffers carrying self-dispatching command
structs.

This is the single most important threading invariant in the system:

> **Every mutable data structure is owned by exactly one event loop. Other event loops
> interact with it only by sending commands.**

### 5.1 Command Struct

A command carries its own handler function pointer and a reference to the data it
operates on. The receiving event loop calls the handler without needing a switch
statement or type inspection.

```zig
// src/threading/command.zig

/// A self-dispatching command. The handler function knows how to interpret
/// the data pointer and apply the command to the owning event loop's state.
pub const Command = struct {
    /// Function to execute when this command is processed.
    /// `loop_ctx` is the owning event loop's context pointer.
    /// `self` is this Command struct (to access `data`).
    handler: *const fn (loop_ctx: *anyopaque, cmd: *const Command) void,

    /// Pointer to command-specific data. The handler function casts this to
    /// the appropriate type. The memory must remain valid until the command
    /// is processed (typically heap-allocated or from a pool).
    data: ?*anyopaque,
};
```

The `Command` struct is exactly 16 bytes (two pointers) on 64-bit systems. It fits in a
single ring buffer record with minimal overhead.

### 5.2 Command Queue

The command queue is a thin wrapper around the MPSC ring buffer from doc 03, specialized
for `Command` structs.

```zig
// src/threading/command_queue.zig

const std = @import("std");
const RingBuffer = @import("../concurrent/ring_buffer.zig").RingBuffer;
const Command = @import("command.zig").Command;

/// A command queue backed by an MPSC ring buffer.
/// Multiple producers (any event loop) can enqueue commands.
/// A single consumer (the owning event loop) drains them.
pub const CommandQueue = struct {
    ring_buffer: *RingBuffer,

    /// The context pointer for the owning event loop.
    /// Passed as the first argument to every command handler.
    loop_context: *anyopaque,

    const command_msg_type_id: i32 = 1;

    pub fn init(ring_buffer: *RingBuffer, loop_context: *anyopaque) CommandQueue {
        return .{
            .ring_buffer = ring_buffer,
            .loop_context = loop_context,
        };
    }

    /// Enqueue a command. Called from any thread.
    /// Returns error if the ring buffer is full (back-pressure).
    pub fn enqueue(self: *CommandQueue, cmd: Command) !void {
        const bytes = std.mem.asBytes(&cmd);
        try self.ring_buffer.write(command_msg_type_id, bytes);
    }

    /// Drain up to `limit` commands, executing each handler inline.
    /// Returns the number of commands processed.
    /// Called only from the owning event loop's thread.
    pub fn drain(self: *CommandQueue, limit: u32) u32 {
        const ctx = self;
        return self.ring_buffer.read(struct {
            fn handle(_: i32, payload: []const u8) void {
                // This closure needs the CommandQueue pointer.
                // We use a technique: store it in a threadlocal.
                const queue = current_draining_queue orelse unreachable;
                const cmd: *const Command = @ptrCast(@alignCast(payload.ptr));
                cmd.handler(queue.loop_context, cmd);
            }
        }.handle, limit);
        // Note: the actual implementation avoids the threadlocal by using
        // tryClaim/read patterns or by storing the context on the stack.
        // The above is simplified for clarity.
        _ = ctx;
    }
};

// Thread-local used during drain. Set before read(), cleared after.
threadlocal var current_draining_queue: ?*CommandQueue = null;
```

In practice, the drain implementation is cleaner. Because `RingBuffer.read` takes a
function pointer (not a closure with captures), we use a pattern where the
`CommandQueue` itself stores the loop context and the read handler accesses it through
a known, thread-safe mechanism. Here's the practical version:

```zig
    /// Drain up to `limit` commands. Each command's handler is invoked inline
    /// with the owning event loop's context.
    pub fn drain(self: *CommandQueue, limit: u32) u32 {
        // Set the threadlocal so the static handler function can find us.
        current_draining_queue = self;
        defer current_draining_queue = null;

        return self.ring_buffer.read(dispatchCommand, limit);
    }

    fn dispatchCommand(_: i32, payload: []const u8) void {
        const queue = current_draining_queue.?;
        const cmd: *const Command = @ptrCast(@alignCast(payload.ptr));
        cmd.handler(queue.loop_context, cmd);
    }
```

This is safe because drain is only ever called from one thread (the consumer thread).
The threadlocal is set and cleared within the same call frame.

**Buffer sizing:** Command queues are small. Most carry at most a few commands per duty
cycle. A 4 KiB or 8 KiB ring buffer is sufficient. Each `Command` is 16 bytes +
8 bytes record header = 24 bytes, so an 8 KiB buffer holds ~340 commands — far more
than will ever be enqueued between drain cycles.

```zig
const command_queue_buffer_length = 8 * 1024; // 8 KiB — generous for command traffic
```

### 5.3 Command Flow

The three event loops communicate through four command queues:

```
┌──────────────┐    sender_cmd_queue     ┌──────────────┐
│              │────────────────────────►│              │
│ Control Loop │    receiver_cmd_queue   │    Sender    │
│              │─────────────┐          │              │
│              │             │          └──────────────┘
│              │             │                 │
│              │◄────────────┼─────────────────┘
│              │  control    │          sender → control
│              │  _cmd_queue │          (send errors, peer unreachable)
└──────────────┘             │
       ▲                     ▼
       │              ┌──────────────┐
       │              │              │
       └──────────────│   Receiver   │
        control       │              │
        _cmd_queue    └──────────────┘
        (peer connected, admin msgs)
```

| Source | Destination | Queue | Example Commands |
|---|---|---|---|
| Control Loop | Sender | `sender_cmd_queue` | Add peer endpoint, remove peer endpoint, update flow control parameters |
| Control Loop | Receiver | `receiver_cmd_queue` | Add peer, remove peer, update service routing table |
| Receiver | Control Loop | `control_cmd_queue` | Peer connected (SETUP received), peer disconnected, admin message received |
| Sender | Control Loop | `control_cmd_queue` | Send error, peer unreachable, connection timeout |

Note that the control loop's command queue (`control_cmd_queue`) has **two producers**
(sender and receiver). This is why we use an MPSC ring buffer — it handles multiple
concurrent writers safely.

The sender and receiver command queues each have a **single producer** (the control
loop), so they could use an SPSC ring buffer for slightly less overhead. However, using
the same MPSC ring buffer implementation everywhere keeps the code simpler, and the
overhead difference is negligible on the command path (which is cold relative to the
message path).

### 5.4 Self-Dispatching Commands

Each command type is a struct containing its data, plus a static handler function. The
handler knows how to cast the data pointer and apply the operation to the target event
loop's state.

Example: adding a peer endpoint to the sender:

```zig
// src/control/commands.zig

const std = @import("std");
const Command = @import("../threading/command.zig").Command;
const SenderEventLoop = @import("../send/sender_event_loop.zig").SenderEventLoop;
const net = std.net;

/// Command data for adding a peer endpoint to the sender.
pub const AddPeerEndpointCmd = struct {
    node_id: u16,
    address: net.Address,

    /// Create a Command that dispatches to SenderEventLoop.addPeerEndpoint().
    pub fn toCommand(self: *AddPeerEndpointCmd) Command {
        return .{
            .handler = handleAddPeerEndpoint,
            .data = @ptrCast(self),
        };
    }

    fn handleAddPeerEndpoint(loop_ctx: *anyopaque, cmd: *const Command) void {
        const sender: *SenderEventLoop = @ptrCast(@alignCast(loop_ctx));
        const data: *const AddPeerEndpointCmd = @ptrCast(@alignCast(cmd.data.?));
        sender.addPeerEndpoint(data.node_id, data.address);
    }
};

/// Command data for notifying the control loop about a new TCP peer connection.
pub const PeerConnectedCmd = struct {
    node_id: u16,
    session_epoch: u64,

    pub fn toCommand(self: *PeerConnectedCmd) Command {
        return .{
            .handler = handlePeerConnected,
            .data = @ptrCast(self),
        };
    }

    fn handlePeerConnected(loop_ctx: *anyopaque, cmd: *const Command) void {
        const control: *@import("control_loop.zig").ControlLoop = @ptrCast(@alignCast(loop_ctx));
        const data: *const PeerConnectedCmd = @ptrCast(@alignCast(cmd.data.?));
        control.onPeerConnected(data.node_id, data.session_epoch);
    }
};
```

The critical property: **the receiving event loop never inspects a type tag or does a
switch on the command type.** It just calls `cmd.handler(loop_ctx, cmd)`. This makes
adding new command types trivial — define the struct, write the handler, done.

### 5.5 Enqueuing a Command (Producer Side)

When the control loop needs to tell the sender about a new peer, it builds the command
data and enqueues it:

```zig
// Inside ControlLoop, after a service registers and we discover it needs
// cross-host routing to a new peer:

fn notifySenderOfNewPeer(self: *ControlLoop, node_id: u16, address: net.Address) void {
    // Command data is stack-allocated. The ring buffer copies the bytes,
    // so the data doesn't need to outlive this function.
    var cmd_data = AddPeerEndpointCmd{
        .node_id = node_id,
        .address = address,
    };
    var cmd = cmd_data.toCommand();
    self.sender_cmd_queue.enqueue(cmd) catch {
        // Command queue full — this is extremely unlikely with an 8 KiB buffer.
        // Log the error and increment a counter. The control loop will retry
        // on the next cycle.
        self.counters.increment(.cmd_queue_full_events);
    };
}
```

**Lifetime note:** The ring buffer's `write()` method copies the command bytes into the
ring buffer. The original `cmd_data` on the stack can go out of scope immediately. On the
consumer side, the `read()` callback receives a pointer into the ring buffer's memory,
which is valid for the duration of the callback.

### 5.6 Draining Commands (Consumer Side)

Each event loop drains its command queue at the start of every duty cycle:

```zig
// Inside SenderEventLoop.doWork():

fn doWork(self: *SenderEventLoop) u32 {
    var work_count: u32 = 0;

    // 1. Process inter-loop commands first — they may change our state
    //    (add/remove peers) before we process message traffic.
    work_count += self.cmd_queue.drain(constants.command_drain_limit);

    // 2. Drain send ring buffer ...
    work_count += self.drainSendBuffer();

    // 3. I/O completions, heartbeats, retransmissions ...
    work_count += self.processIoCompletions();
    work_count += self.checkHeartbeats();
    work_count += self.processRetransmissions();

    return work_count;
}
```

The `command_drain_limit` constant (from doc 01: `constants.command_drain_limit = 1`)
limits how many commands are processed per duty cycle. A limit of 1 means each cycle
processes at most one command, keeping the duty cycle short and predictable. For most
deployments this is fine — commands are rare (peer add/remove, config changes). If
command throughput becomes a bottleneck (unlikely), increase the limit.

---

## 6. Threading Modes

The broker supports three threading modes that trade thread count for simplicity. The
event loop implementations are identical in all modes — only the threading and wiring
differ.

### 6.1 DEDICATED Mode (3 Threads)

The default. Each event loop gets its own thread and its own idle strategy. This provides
the lowest latency because each loop can burn its own core.

```zig
// src/threading/threading_mode.zig

pub const ThreadingMode = enum {
    /// 3 threads: Control + Sender + Receiver.
    dedicated,

    /// 2 threads: Control on one, Sender + Receiver combined on another.
    shared_network,

    /// 1 thread: All event loops on a single thread.
    shared,
};
```

Startup in DEDICATED mode is handled by `BrokerThreads.start()` as shown in §3.

### 6.2 SHARED_NETWORK Mode (2 Threads)

The sender and receiver duty cycles are combined into a single composite event loop. The
control loop remains on its own thread.

```zig
// src/threading/composite_event_loop.zig

const platform = @import("../platform.zig");
const EventLoop = platform.EventLoop;

/// Combines two event loops into one. doWork() calls both, summing work counts.
pub const CompositeEventLoop = struct {
    first: EventLoop,
    second: EventLoop,

    pub fn eventLoop(self: *CompositeEventLoop) EventLoop {
        return .{
            .context = @ptrCast(self),
            .doWorkFn = doWorkFn,
            .onCloseFn = onCloseFn,
        };
    }

    fn doWorkFn(ctx: *anyopaque) u32 {
        const self: *CompositeEventLoop = @ptrCast(@alignCast(ctx));
        var work: u32 = 0;
        work += self.first.doWork();
        work += self.second.doWork();
        return work;
    }

    fn onCloseFn(ctx: *anyopaque) void {
        const self: *CompositeEventLoop = @ptrCast(@alignCast(ctx));
        self.first.onClose();
        self.second.onClose();
    }
};
```

Usage:

```zig
fn startSharedNetwork(broker: *Broker) !void {
    // Combine sender + receiver into one event loop.
    broker.network_composite = CompositeEventLoop{
        .first = broker.sender_loop.eventLoop(),
        .second = broker.receiver_loop.eventLoop(),
    };

    broker.control_runner = ThreadRunner.init(
        "brz-control",
        broker.control_loop.eventLoop(),
        broker.config.control_idle_strategy,
    );
    broker.network_runner = ThreadRunner.init(
        "brz-network",
        broker.network_composite.eventLoop(),
        broker.config.network_idle_strategy,
    );

    try broker.control_runner.start();
    try broker.network_runner.start();
}
```

**Trade-off:** Half the threads, but sender and receiver contend for the same core. This
mode is useful when you have limited CPU but still want the control loop isolated from
network I/O.

### 6.3 SHARED Mode (1 Thread)

All three event loops run in sequence on a single thread. This is the simplest mode and
is primarily useful for testing.

```zig
fn startShared(broker: *Broker) !void {
    // Combine all three into one event loop.
    broker.inner_composite = CompositeEventLoop{
        .first = broker.sender_loop.eventLoop(),
        .second = broker.receiver_loop.eventLoop(),
    };
    broker.outer_composite = CompositeEventLoop{
        .first = broker.control_loop.eventLoop(),
        .second = broker.inner_composite.eventLoop(),
    };

    broker.shared_runner = ThreadRunner.init(
        "brz-broker",
        broker.outer_composite.eventLoop(),
        broker.config.shared_idle_strategy,
    );

    try broker.shared_runner.start();
}
```

**In SHARED mode, command queues can be bypassed.** Since all event loops run on the same
thread, there is no concurrency. Commands can be delivered as direct function calls:

```zig
fn notifySenderOfNewPeerDirect(sender: *SenderEventLoop, node_id: u16, address: net.Address) void {
    // No command queue — call directly. Safe because we're on the same thread.
    sender.addPeerEndpoint(node_id, address);
}
```

However, for simplicity the initial implementation should keep command queues active in
all modes. The queues work correctly in single-threaded mode (the MPSC ring buffer
degrades gracefully to SPSC with one producer). Direct-call optimization can be added
later if profiling shows the command queue overhead matters.

### 6.4 Mode Selection

```
# broker.properties
broker.threading.mode = dedicated    # dedicated | shared_network | shared
```

The mode is read at startup and determines which `start*()` function is called:

```zig
pub fn startBroker(broker: *Broker) !void {
    switch (broker.config.threading_mode) {
        .dedicated => try startDedicated(broker),
        .shared_network => try startSharedNetwork(broker),
        .shared => try startShared(broker),
    }
}

fn startDedicated(broker: *Broker) !void {
    try broker.threads.start();
}
```

---

## 7. Broker Lifecycle

### 7.1 Startup Sequence

```
1. Load configuration (broker.properties)
2. Create/open broker metadata file
3. Scan for existing services (PID-based liveness check)
4. Initialize counters manager + error log
5. Create ring buffers (control RB, send RB) over mapped memory
6. Initialize service registry (populate from scan results)
7. Create command queue ring buffers (small, heap-allocated)
8. Create event loop instances:
   a. ControlLoop — owns control RB, service registry, cluster manager
   b. SenderEventLoop — owns send RB, outgoing TCP connections (via brz_tcp), per-peer write queues
   c. ReceiverEventLoop — owns TCP listener, incoming TCP connections (via brz_tcp), routing table
9. Wire command queues between event loops
10. Select threading mode, create ThreadRunners
11. Start threads (order depends on mode — see §3)
12. Write initial heartbeat
13. Broker is ready — log "Broker started on node {node_id}"
```

### 7.2 Shutdown Sequence

Shutdown must be orderly to avoid dropped messages and resource leaks:

```zig
pub fn shutdown(broker: *Broker) void {
    // 1. Signal all event loops to stop.
    //    Order matters: receiver first, then sender, then control.
    //    This drains in-flight messages before closing the control plane.

    switch (broker.config.threading_mode) {
        .dedicated => {
            // Stop receiver first — no new incoming messages.
            broker.threads.receiver_runner.stop();
            broker.threads.receiver_runner.join();

            // Stop sender — flush remaining sends.
            broker.threads.sender_runner.stop();
            broker.threads.sender_runner.join();

            // Stop control loop last.
            broker.threads.control_runner.stop();
            broker.threads.control_runner.join();
        },
        .shared_network => {
            broker.network_runner.stopAndJoin();
            broker.control_runner.stopAndJoin();
        },
        .shared => {
            broker.shared_runner.stopAndJoin();
        },
    }

    // 2. Close all resources (after all threads have exited).
    //    onClose() has already been called by each ThreadRunner.
    //    Now close shared resources.
    broker.counters.flush();
    broker.broker_metadata_file.close();

    // 3. Log final state.
    log.info("Broker shut down cleanly on node {d}", .{broker.config.node_id});
}
```

**Why receiver stops first:** The receiver is the entry point for all external data. By
stopping it first, we guarantee no new messages arrive after the sender has flushed. If
we stopped the sender first, the receiver might still deliver messages that would need
to be forwarded — but the sender would already be gone.

**Why control stops last:** The control loop handles deregistration events that may be
triggered during shutdown. It needs to run long enough to process any final cleanup
commands from the sender and receiver.

### 7.3 Signal Handling

The broker installs a signal handler for `SIGINT` and `SIGTERM` that sets the running
flag to false:

```zig
// src/main.zig

const std = @import("std");
const Broker = @import("broker.zig").Broker;

var global_broker: ?*Broker = null;

pub fn main() !void {
    var broker = try Broker.create();
    global_broker = &broker;

    // Install signal handler.
    const act = std.posix.Sigaction{
        .handler = .{ .handler = handleSignal },
        .mask = std.posix.empty_sigset,
        .flags = 0,
    };
    try std.posix.sigaction(std.posix.SIG.INT, &act, null);
    try std.posix.sigaction(std.posix.SIG.TERM, &act, null);

    try broker.start();

    // Block until shutdown.
    broker.awaitShutdown();
    broker.shutdown();
}

fn handleSignal(sig: c_int) callconv(.C) void {
    _ = sig;
    if (global_broker) |b| {
        b.threads.receiver_runner.stop();
        b.threads.sender_runner.stop();
        b.threads.control_runner.stop();
    }
}
```

The `awaitShutdown()` function blocks the main thread until all runners have stopped:

```zig
pub fn awaitShutdown(self: *Broker) void {
    // Spin-wait on the control runner's running flag.
    // (It's the last to be stopped.)
    while (self.threads.control_runner.running.load()) {
        std.time.sleep(100 * std.time.ns_per_ms);
    }
}
```

---

## 8. Thread Naming

Each thread is named for visibility in `ps`, `top`, `htop`, `perf`, and profilers. The
naming implementation is in doc 01 (`setThreadName`). Here are the conventions:

| Thread | Name | Max Length (Linux) |
|---|---|---|
| Broker control loop | `brz-control` | 12 chars ✓ |
| Broker sender | `brz-sender` | 10 chars ✓ |
| Broker receiver | `brz-receiver` | 12 chars ✓ |
| Broker combined network | `brz-network` | 11 chars ✓ |
| Broker shared (single thread) | `brz-broker` | 10 chars ✓ |
| Service control agent | `brz-svc-ctrl` | 13 chars ✓ |
| Service message consumer | `brz-svc-msg` | 11 chars ✓ |

All names are ≤15 characters to fit within the Linux `pthread_setname_np` limit.

Verification in `htop` or `ps`:

```
$ ps -eo pid,lwp,comm | grep brz
  1234  1234 brz-broker       # main thread (blocked in awaitShutdown)
  1234  1235 brz-control
  1234  1236 brz-sender
  1234  1237 brz-receiver
```

---

## 9. CPU Affinity (Optional)

For ultra-low latency deployments, each thread can be pinned to a specific CPU core. This
eliminates cross-core migration, improves cache locality, and makes performance more
predictable.

The `setThreadAffinity` function from doc 01 handles the platform details. Here we show
how it integrates with configuration:

```
# broker.properties — optional CPU affinity
broker.control.cpu_affinity = 1
broker.sender.cpu_affinity = 2
broker.receiver.cpu_affinity = 3
```

Applied during thread startup:

```zig
// Inside ThreadRunner.threadMain():

fn threadMain(self: *Self) void {
    platform.thread.setThreadName(self.name);

    // Apply CPU affinity if configured.
    if (self.cpu_affinity) |core_id| {
        platform.thread.setThreadAffinity(core_id) catch |err| {
            // Non-fatal — log and continue without affinity.
            log.warn("Failed to set CPU affinity to core {d} for {s}: {}", .{
                core_id,
                self.name,
                err,
            });
        };
    }

    while (self.running.load()) {
        const work_count = self.event_loop.doWork();
        self.idle_strategy.idle(work_count);
    }

    self.event_loop.onClose();
}
```

To support this, extend `ThreadRunner` with an optional affinity field:

```zig
pub const ThreadRunner = struct {
    name: []const u8,
    event_loop: EventLoop,
    idle_strategy: IdleStrategy,
    running: AtomicBool,
    thread: ?std.Thread = null,
    cpu_affinity: ?u32 = null,     // Optional CPU core to pin to.

    // ... rest unchanged ...
};
```

**Guidelines for core assignment:**

- Reserve core 0 for the OS and interrupts.
- Pin the receiver to a core on the same NUMA node as the NIC (check with `lstopo` or
  `lscpu`).
- Pin the sender to an adjacent core (same L3 cache).
- The control loop has the lowest frequency — it can share a core or use a non-dedicated
  core.
- On hyperthreaded CPUs, avoid pinning two busy threads to sibling hyperthreads on the
  same physical core.

---

## 10. Service-Side Threading

Each service (`BrzEngine`) runs its own pair of threads. These use the same `EventLoop` +
`ThreadRunner` infrastructure as the broker.

```zig
// src/engine.zig (simplified)

pub const BrzEngine = struct {
    control_agent: ControlAgent,
    message_consumer: MessageConsumerAgent,
    control_runner: ThreadRunner,
    message_runner: ThreadRunner,

    pub fn start(self: *BrzEngine) !void {
        self.control_runner = ThreadRunner.init(
            "brz-svc-ctrl",
            self.control_agent.eventLoop(),
            self.config.control_idle_strategy,
        );
        self.message_runner = ThreadRunner.init(
            "brz-svc-msg",
            self.message_consumer.eventLoop(),
            self.config.message_idle_strategy,
        );

        try self.control_runner.start();
        try self.message_runner.start();
    }

    pub fn stop(self: *BrzEngine) void {
        // Stop message consumer first (no new messages dispatched to app).
        self.message_runner.stopAndJoin();
        // Stop control agent (flushes final heartbeat).
        self.control_runner.stopAndJoin();
    }
};
```

The service control agent's duty cycle:

```zig
fn doWork(self: *ControlAgent) u32 {
    var work_count: u32 = 0;
    const now_ms = platform.Clock.epochMillis();

    // 1. Poll service's control ring buffer (registration responses, discovery).
    work_count += self.control_rb.read(self.onControlMessage, constants.control_read_limit);

    // 2. Write heartbeat (every 1s).
    if (now_ms >= self.next_heartbeat_ms) {
        self.metadata.storeHeartbeat(now_ms);
        self.next_heartbeat_ms = now_ms + constants.service_heartbeat_write_interval_ms;
    }

    return work_count;
}
```

The service message consumer's duty cycle:

```zig
fn doWork(self: *MessageConsumerAgent) u32 {
    // Poll service's messages ring buffer, dispatch to application handler.
    return self.messages_rb.read(self.onMessage, constants.message_read_limit);
}
```

---

## 11. Putting It All Together

Here's the complete wiring for a broker in DEDICATED mode:

```zig
// src/broker.zig (simplified — shows the threading wiring)

const std = @import("std");
const platform = @import("platform.zig");
const RingBuffer = @import("concurrent/ring_buffer.zig").RingBuffer;
const CommandQueue = @import("threading/command_queue.zig").CommandQueue;
const BrokerThreads = @import("threading/broker_threads.zig").BrokerThreads;
const ControlLoop = @import("control/control_loop.zig").ControlLoop;
const SenderEventLoop = @import("send/sender_event_loop.zig").SenderEventLoop;
const ReceiverEventLoop = @import("recv/receiver_event_loop.zig").ReceiverEventLoop;
const CompositeEventLoop = @import("threading/composite_event_loop.zig").CompositeEventLoop;
const ThreadingMode = @import("threading/threading_mode.zig").ThreadingMode;

pub const Broker = struct {
    // Event loops (own their state).
    control_loop: ControlLoop,
    sender_loop: SenderEventLoop,
    receiver_loop: ReceiverEventLoop,

    // Command queues (inter-loop communication).
    control_cmd_queue: CommandQueue,
    sender_cmd_queue: CommandQueue,
    receiver_cmd_queue: CommandQueue,

    // Command queue backing buffers (heap-allocated, small).
    control_cmd_buffer: []align(platform.constants.cache_line_pad) u8,
    sender_cmd_buffer: []align(platform.constants.cache_line_pad) u8,
    receiver_cmd_buffer: []align(platform.constants.cache_line_pad) u8,

    // Command queue ring buffers.
    control_cmd_rb: RingBuffer,
    sender_cmd_rb: RingBuffer,
    receiver_cmd_rb: RingBuffer,

    // Threading.
    threads: BrokerThreads,
    network_composite: CompositeEventLoop,    // Used in SHARED_NETWORK mode.
    inner_composite: CompositeEventLoop,      // Used in SHARED mode.
    outer_composite: CompositeEventLoop,      // Used in SHARED mode.
    network_runner: platform.ThreadRunner,    // Used in SHARED_NETWORK mode.
    shared_runner: platform.ThreadRunner,     // Used in SHARED mode.

    // Config.
    config: BrokerConfig,

    // Shared resources.
    broker_metadata_file: BrokerMetadataFile,
    counters: CountersManager,
    allocator: std.mem.Allocator,

    pub fn create(allocator: std.mem.Allocator, config: BrokerConfig) !Broker {
        // 1. Allocate command queue buffers.
        const cmd_buf_len = RingBuffer.calculateRequiredSize(8 * 1024);
        const ctrl_buf = try allocator.alignedAlloc(u8, platform.constants.cache_line_pad, cmd_buf_len);
        const send_buf = try allocator.alignedAlloc(u8, platform.constants.cache_line_pad, cmd_buf_len);
        const recv_buf = try allocator.alignedAlloc(u8, platform.constants.cache_line_pad, cmd_buf_len);

        // 2. Initialize command queue ring buffers over the allocated memory.
        var ctrl_rb = try RingBuffer.init(ctrl_buf, false, null);
        var send_rb = try RingBuffer.init(send_buf, false, null);
        var recv_rb = try RingBuffer.init(recv_buf, false, null);

        // 3. Create event loops (details omitted — see docs 05, 06, 09).
        var control_loop = try ControlLoop.create(config, allocator);
        var sender_loop = try SenderEventLoop.create(config, allocator);
        var receiver_loop = try ReceiverEventLoop.create(config, allocator);

        // 4. Wire command queues.
        var broker: Broker = .{
            .control_loop = control_loop,
            .sender_loop = sender_loop,
            .receiver_loop = receiver_loop,

            .control_cmd_rb = ctrl_rb,
            .sender_cmd_rb = send_rb,
            .receiver_cmd_rb = recv_rb,

            .control_cmd_queue = CommandQueue.init(&ctrl_rb, @ptrCast(&control_loop)),
            .sender_cmd_queue = CommandQueue.init(&send_rb, @ptrCast(&sender_loop)),
            .receiver_cmd_queue = CommandQueue.init(&recv_rb, @ptrCast(&receiver_loop)),

            .control_cmd_buffer = ctrl_buf,
            .sender_cmd_buffer = send_buf,
            .receiver_cmd_buffer = recv_buf,

            .threads = undefined, // Set below.
            .network_composite = undefined,
            .inner_composite = undefined,
            .outer_composite = undefined,
            .network_runner = undefined,
            .shared_runner = undefined,

            .config = config,
            .broker_metadata_file = undefined, // Opened elsewhere.
            .counters = undefined,             // Initialized elsewhere.
            .allocator = allocator,
        };

        // 5. Inject command queues into event loops so they can enqueue commands
        //    to other loops and drain their own queue.
        broker.control_loop.cmd_queue = &broker.control_cmd_queue;
        broker.control_loop.sender_cmd_queue = &broker.sender_cmd_queue;
        broker.control_loop.receiver_cmd_queue = &broker.receiver_cmd_queue;

        broker.sender_loop.cmd_queue = &broker.sender_cmd_queue;
        broker.sender_loop.control_cmd_queue = &broker.control_cmd_queue;

        broker.receiver_loop.cmd_queue = &broker.receiver_cmd_queue;
        broker.receiver_loop.control_cmd_queue = &broker.control_cmd_queue;

        return broker;
    }

    pub fn start(self: *Broker) !void {
        switch (self.config.threading_mode) {
            .dedicated => {
                self.threads = BrokerThreads.init(
                    self.control_loop.eventLoop(),
                    self.sender_loop.eventLoop(),
                    self.receiver_loop.eventLoop(),
                    self.config.control_idle_strategy,
                    self.config.sender_idle_strategy,
                    self.config.receiver_idle_strategy,
                );
                try self.threads.start();
            },
            .shared_network => {
                self.network_composite = CompositeEventLoop{
                    .first = self.sender_loop.eventLoop(),
                    .second = self.receiver_loop.eventLoop(),
                };
                self.threads.control_runner = platform.ThreadRunner.init(
                    "brz-control",
                    self.control_loop.eventLoop(),
                    self.config.control_idle_strategy,
                );
                self.network_runner = platform.ThreadRunner.init(
                    "brz-network",
                    self.network_composite.eventLoop(),
                    self.config.sender_idle_strategy,
                );
                try self.threads.control_runner.start();
                try self.network_runner.start();
            },
            .shared => {
                self.inner_composite = CompositeEventLoop{
                    .first = self.sender_loop.eventLoop(),
                    .second = self.receiver_loop.eventLoop(),
                };
                self.outer_composite = CompositeEventLoop{
                    .first = self.control_loop.eventLoop(),
                    .second = self.inner_composite.eventLoop(),
                };
                self.shared_runner = platform.ThreadRunner.init(
                    "brz-broker",
                    self.outer_composite.eventLoop(),
                    self.config.shared_idle_strategy,
                );
                try self.shared_runner.start();
            },
        }
    }

    pub fn shutdown(self: *Broker) void {
        switch (self.config.threading_mode) {
            .dedicated => self.threads.shutdown(),
            .shared_network => {
                self.network_runner.stopAndJoin();
                self.threads.control_runner.stopAndJoin();
            },
            .shared => {
                self.shared_runner.stopAndJoin();
            },
        }

        // Free command queue buffers.
        self.allocator.free(self.control_cmd_buffer);
        self.allocator.free(self.sender_cmd_buffer);
        self.allocator.free(self.receiver_cmd_buffer);
    }

    pub fn awaitShutdown(self: *Broker) void {
        switch (self.config.threading_mode) {
            .dedicated => {
                while (self.threads.isRunning()) {
                    std.time.sleep(100 * std.time.ns_per_ms);
                }
            },
            .shared_network => {
                while (self.threads.control_runner.running.load() or
                    self.network_runner.running.load())
                {
                    std.time.sleep(100 * std.time.ns_per_ms);
                }
            },
            .shared => {
                while (self.shared_runner.running.load()) {
                    std.time.sleep(100 * std.time.ns_per_ms);
                }
            },
        }
    }
};
```

---

## 12. Testing

### 12.1 Unit Tests: Idle Strategy

```zig
// src/threading/tests/idle_strategy_test.zig

const std = @import("std");
const testing = std.testing;
const IdleStrategy = @import("../platform.zig").IdleStrategy;

test "backoff: spin phase increments spin count" {
    // Given
    var strategy = IdleStrategy{ .backoff = .{} };

    // When — idle with no work 3 times.
    strategy.idle(0);
    strategy.idle(0);
    strategy.idle(0);

    // Then — should be in spinning phase, spin count = 3.
    try testing.expectEqual(@as(u32, 3), strategy.backoff.spins);
    try testing.expectEqual(@as(u32, 0), strategy.backoff.yields);
}

test "backoff: transitions from spinning to yielding after max_spins" {
    // Given
    var strategy = IdleStrategy{ .backoff = .{} };

    // When — idle 11 times (max_spins = 10, so 11th enters yielding).
    var i: u32 = 0;
    while (i < 11) : (i += 1) {
        strategy.idle(0);
    }

    // Then — should have transitioned to yielding phase.
    try testing.expectEqual(@as(u32, 10), strategy.backoff.spins);
    try testing.expectEqual(@as(u32, 1), strategy.backoff.yields);
}

test "backoff: transitions from yielding to sleeping after max_yields" {
    // Given
    var strategy = IdleStrategy{ .backoff = .{} };

    // When — exhaust spins, then exhaust yields, then one more idle.
    var i: u32 = 0;
    while (i < 10 + 20 + 1) : (i += 1) {
        strategy.idle(0);
    }

    // Then — should be in sleeping phase (spins maxed, yields maxed).
    try testing.expectEqual(@as(u32, 10), strategy.backoff.spins);
    try testing.expectEqual(@as(u32, 20), strategy.backoff.yields);
}

test "backoff: resets on work" {
    // Given — advance to yielding phase.
    var strategy = IdleStrategy{ .backoff = .{} };
    var i: u32 = 0;
    while (i < 15) : (i += 1) {
        strategy.idle(0);
    }
    try testing.expect(strategy.backoff.yields > 0);

    // When — do work.
    strategy.idle(1);

    // Then — back to spinning, counts reset.
    try testing.expectEqual(@as(u32, 0), strategy.backoff.spins);
    try testing.expectEqual(@as(u32, 0), strategy.backoff.yields);
}
```

### 12.2 Unit Tests: Command Queue

```zig
// src/threading/tests/command_queue_test.zig

const std = @import("std");
const testing = std.testing;
const RingBuffer = @import("../../concurrent/ring_buffer.zig").RingBuffer;
const CommandQueue = @import("../command_queue.zig").CommandQueue;
const Command = @import("../command.zig").Command;

test "enqueue and drain single command" {
    // Given — a command queue backed by a small ring buffer.
    const buf_len = RingBuffer.calculateRequiredSize(1024);
    var buffer: [buf_len]u8 align(128) = undefined;
    var rb = try RingBuffer.init(&buffer, false, null);

    const TestCtx = struct {
        value: u32 = 0,
    };
    var ctx = TestCtx{};

    var queue = CommandQueue.init(&rb, @ptrCast(&ctx));

    // When — enqueue a command that sets ctx.value to 42.
    const cmd = Command{
        .handler = struct {
            fn handle(loop_ctx: *anyopaque, _: *const Command) void {
                const c: *TestCtx = @ptrCast(@alignCast(loop_ctx));
                c.value = 42;
            }
        }.handle,
        .data = null,
    };
    try queue.enqueue(cmd);

    // Then — drain executes the handler.
    const drained = queue.drain(10);
    try testing.expectEqual(@as(u32, 1), drained);
    try testing.expectEqual(@as(u32, 42), ctx.value);
}

test "drain returns 0 when queue is empty" {
    // Given
    const buf_len = RingBuffer.calculateRequiredSize(1024);
    var buffer: [buf_len]u8 align(128) = undefined;
    var rb = try RingBuffer.init(&buffer, false, null);
    var dummy: u32 = 0;
    var queue = CommandQueue.init(&rb, @ptrCast(&dummy));

    // When / Then
    const drained = queue.drain(10);
    try testing.expectEqual(@as(u32, 0), drained);
}

test "enqueue multiple commands and drain with limit" {
    // Given
    const buf_len = RingBuffer.calculateRequiredSize(4096);
    var buffer: [buf_len]u8 align(128) = undefined;
    var rb = try RingBuffer.init(&buffer, false, null);

    const TestCtx = struct {
        count: u32 = 0,
    };
    var ctx = TestCtx{};
    var queue = CommandQueue.init(&rb, @ptrCast(&ctx));

    // When — enqueue 5 commands.
    var i: u32 = 0;
    while (i < 5) : (i += 1) {
        const cmd = Command{
            .handler = struct {
                fn handle(loop_ctx: *anyopaque, _: *const Command) void {
                    const c: *TestCtx = @ptrCast(@alignCast(loop_ctx));
                    c.count += 1;
                }
            }.handle,
            .data = null,
        };
        try queue.enqueue(cmd);
    }

    // Then — drain with limit 2 processes only 2.
    var drained = queue.drain(2);
    try testing.expectEqual(@as(u32, 2), drained);
    try testing.expectEqual(@as(u32, 2), ctx.count);

    // And the remaining 3 are still there.
    drained = queue.drain(10);
    try testing.expectEqual(@as(u32, 3), drained);
    try testing.expectEqual(@as(u32, 5), ctx.count);
}
```

### 12.3 Unit Tests: Event Loop Runner

```zig
// src/threading/tests/thread_runner_test.zig

const std = @import("std");
const testing = std.testing;
const platform = @import("../../platform.zig");
const ThreadRunner = platform.ThreadRunner;
const EventLoop = platform.EventLoop;

test "ThreadRunner start and stop" {
    // Given — a trivial event loop that counts doWork calls.
    const TestLoop = struct {
        call_count: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

        fn doWork(ctx: *anyopaque) u32 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            _ = self.call_count.fetchAdd(1, .monotonic);
            return 1;
        }

        fn onClose(_: *anyopaque) void {}
    };

    var loop = TestLoop{};
    var runner = ThreadRunner.init(
        "test-runner",
        .{
            .context = @ptrCast(&loop),
            .doWorkFn = TestLoop.doWork,
            .onCloseFn = TestLoop.onClose,
        },
        .{ .backoff = .{} },
    );

    // When
    try runner.start();
    std.time.sleep(5 * std.time.ns_per_ms);
    runner.stopAndJoin();

    // Then
    try testing.expect(loop.call_count.load(.monotonic) > 0);
}

test "ThreadRunner onClose is called after stop" {
    // Given
    const TestLoop = struct {
        closed: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

        fn doWork(_: *anyopaque) u32 {
            return 0;
        }

        fn onClose(ctx: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            _ = self.closed.fetchAdd(1, .monotonic);
        }
    };

    var loop = TestLoop{};
    var runner = ThreadRunner.init(
        "test-close",
        .{
            .context = @ptrCast(&loop),
            .doWorkFn = TestLoop.doWork,
            .onCloseFn = TestLoop.onClose,
        },
        .sleeping,
    );

    // When
    try runner.start();
    std.time.sleep(2 * std.time.ns_per_ms);
    runner.stopAndJoin();

    // Then — onClose was called exactly once.
    try testing.expectEqual(@as(u32, 1), loop.closed.load(.monotonic));
}
```

### 12.4 Unit Tests: Composite Event Loop

```zig
// src/threading/tests/composite_event_loop_test.zig

const std = @import("std");
const testing = std.testing;
const CompositeEventLoop = @import("../composite_event_loop.zig").CompositeEventLoop;
const platform = @import("../../platform.zig");
const EventLoop = platform.EventLoop;

test "CompositeEventLoop calls both doWork functions" {
    // Given
    const Counter = struct {
        value: u32 = 0,

        fn doWork(ctx: *anyopaque) u32 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.value += 1;
            return 1;
        }

        fn onClose(_: *anyopaque) void {}
    };

    var first = Counter{};
    var second = Counter{};

    var composite = CompositeEventLoop{
        .first = .{
            .context = @ptrCast(&first),
            .doWorkFn = Counter.doWork,
            .onCloseFn = Counter.onClose,
        },
        .second = .{
            .context = @ptrCast(&second),
            .doWorkFn = Counter.doWork,
            .onCloseFn = Counter.onClose,
        },
    };

    // When
    const el = composite.eventLoop();
    const work = el.doWork();

    // Then — both were called, work count is sum.
    try testing.expectEqual(@as(u32, 2), work);
    try testing.expectEqual(@as(u32, 1), first.value);
    try testing.expectEqual(@as(u32, 1), second.value);
}

test "CompositeEventLoop onClose calls both" {
    // Given
    const Closer = struct {
        closed: bool = false,

        fn doWork(_: *anyopaque) u32 {
            return 0;
        }

        fn onClose(ctx: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            self.closed = true;
        }
    };

    var first = Closer{};
    var second = Closer{};

    var composite = CompositeEventLoop{
        .first = .{
            .context = @ptrCast(&first),
            .doWorkFn = Closer.doWork,
            .onCloseFn = Closer.onClose,
        },
        .second = .{
            .context = @ptrCast(&second),
            .doWorkFn = Closer.doWork,
            .onCloseFn = Closer.onClose,
        },
    };

    // When
    const el = composite.eventLoop();
    el.onClose();

    // Then
    try testing.expect(first.closed);
    try testing.expect(second.closed);
}
```

### 12.5 Integration Tests: Threading Modes

```zig
// src/threading/tests/threading_mode_test.zig

const std = @import("std");
const testing = std.testing;
const platform = @import("../../platform.zig");
const ThreadRunner = platform.ThreadRunner;
const EventLoop = platform.EventLoop;
const RingBuffer = @import("../../concurrent/ring_buffer.zig").RingBuffer;
const CommandQueue = @import("../command_queue.zig").CommandQueue;
const Command = @import("../command.zig").Command;

test "DEDICATED mode: 3 threads running, commands flowing" {
    // Given — three event loops that drain their command queues and count work.
    const LoopState = struct {
        cmd_queue: *CommandQueue,
        work_done: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

        fn doWork(ctx: *anyopaque) u32 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            const drained = self.cmd_queue.drain(10);
            if (drained > 0) {
                _ = self.work_done.fetchAdd(drained, .monotonic);
            }
            return drained;
        }

        fn onClose(_: *anyopaque) void {}
    };

    // Allocate 3 command queue ring buffers.
    const buf_len = RingBuffer.calculateRequiredSize(4096);
    var buf1: [buf_len]u8 align(128) = undefined;
    var buf2: [buf_len]u8 align(128) = undefined;
    var buf3: [buf_len]u8 align(128) = undefined;

    var rb1 = try RingBuffer.init(&buf1, false, null);
    var rb2 = try RingBuffer.init(&buf2, false, null);
    var rb3 = try RingBuffer.init(&buf3, false, null);

    var state1 = LoopState{ .cmd_queue = undefined };
    var state2 = LoopState{ .cmd_queue = undefined };
    var state3 = LoopState{ .cmd_queue = undefined };

    var q1 = CommandQueue.init(&rb1, @ptrCast(&state1));
    var q2 = CommandQueue.init(&rb2, @ptrCast(&state2));
    var q3 = CommandQueue.init(&rb3, @ptrCast(&state3));

    state1.cmd_queue = &q1;
    state2.cmd_queue = &q2;
    state3.cmd_queue = &q3;

    // When — start 3 runners.
    var r1 = ThreadRunner.init("test-ctrl", EventLoop{
        .context = @ptrCast(&state1),
        .doWorkFn = LoopState.doWork,
        .onCloseFn = LoopState.onClose,
    }, .sleeping);

    var r2 = ThreadRunner.init("test-send", EventLoop{
        .context = @ptrCast(&state2),
        .doWorkFn = LoopState.doWork,
        .onCloseFn = LoopState.onClose,
    }, .sleeping);

    var r3 = ThreadRunner.init("test-recv", EventLoop{
        .context = @ptrCast(&state3),
        .doWorkFn = LoopState.doWork,
        .onCloseFn = LoopState.onClose,
    }, .sleeping);

    try r1.start();
    try r2.start();
    try r3.start();

    // Send a command to each loop.
    const noop_cmd = Command{
        .handler = struct {
            fn handle(_: *anyopaque, _: *const Command) void {}
        }.handle,
        .data = null,
    };
    try q1.enqueue(noop_cmd);
    try q2.enqueue(noop_cmd);
    try q3.enqueue(noop_cmd);

    // Wait for processing.
    std.time.sleep(10 * std.time.ns_per_ms);

    // Then — all three processed their commands.
    r1.stopAndJoin();
    r2.stopAndJoin();
    r3.stopAndJoin();

    try testing.expect(state1.work_done.load(.monotonic) >= 1);
    try testing.expect(state2.work_done.load(.monotonic) >= 1);
    try testing.expect(state3.work_done.load(.monotonic) >= 1);
}

test "SHARED mode: single thread processes all event loops" {
    // Given
    const Counter = struct {
        value: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

        fn doWork(ctx: *anyopaque) u32 {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            _ = self.value.fetchAdd(1, .monotonic);
            return 1;
        }

        fn onClose(_: *anyopaque) void {}
    };

    var c1 = Counter{};
    var c2 = Counter{};
    var c3 = Counter{};

    const CompositeEventLoop = @import("../composite_event_loop.zig").CompositeEventLoop;

    var inner = CompositeEventLoop{
        .first = .{
            .context = @ptrCast(&c2),
            .doWorkFn = Counter.doWork,
            .onCloseFn = Counter.onClose,
        },
        .second = .{
            .context = @ptrCast(&c3),
            .doWorkFn = Counter.doWork,
            .onCloseFn = Counter.onClose,
        },
    };

    var outer = CompositeEventLoop{
        .first = .{
            .context = @ptrCast(&c1),
            .doWorkFn = Counter.doWork,
            .onCloseFn = Counter.onClose,
        },
        .second = inner.eventLoop(),
    };

    // When — run on a single thread.
    var runner = ThreadRunner.init(
        "test-shared",
        outer.eventLoop(),
        .sleeping,
    );
    try runner.start();
    std.time.sleep(10 * std.time.ns_per_ms);
    runner.stopAndJoin();

    // Then — all three counters were incremented.
    try testing.expect(c1.value.load(.monotonic) > 0);
    try testing.expect(c2.value.load(.monotonic) > 0);
    try testing.expect(c3.value.load(.monotonic) > 0);
}

test "shutdown sequence: no hang, onClose called for all loops" {
    // Given — event loops that track onClose calls.
    const CloseTracker = struct {
        closed: std.atomic.Value(u32) = std.atomic.Value(u32).init(0),

        fn doWork(_: *anyopaque) u32 {
            return 0;
        }

        fn onClose(ctx: *anyopaque) void {
            const self: *@This() = @ptrCast(@alignCast(ctx));
            _ = self.closed.fetchAdd(1, .monotonic);
        }
    };

    var t1 = CloseTracker{};
    var t2 = CloseTracker{};
    var t3 = CloseTracker{};

    var r1 = ThreadRunner.init("t1", EventLoop{
        .context = @ptrCast(&t1),
        .doWorkFn = CloseTracker.doWork,
        .onCloseFn = CloseTracker.onClose,
    }, .sleeping);

    var r2 = ThreadRunner.init("t2", EventLoop{
        .context = @ptrCast(&t2),
        .doWorkFn = CloseTracker.doWork,
        .onCloseFn = CloseTracker.onClose,
    }, .sleeping);

    var r3 = ThreadRunner.init("t3", EventLoop{
        .context = @ptrCast(&t3),
        .doWorkFn = CloseTracker.doWork,
        .onCloseFn = CloseTracker.onClose,
    }, .sleeping);

    // When — start all, then shut down in order.
    try r1.start();
    try r2.start();
    try r3.start();
    std.time.sleep(2 * std.time.ns_per_ms);

    r3.stopAndJoin(); // Receiver.
    r2.stopAndJoin(); // Sender.
    r1.stopAndJoin(); // Control.

    // Then — all onClose handlers were invoked exactly once.
    try testing.expectEqual(@as(u32, 1), t1.closed.load(.monotonic));
    try testing.expectEqual(@as(u32, 1), t2.closed.load(.monotonic));
    try testing.expectEqual(@as(u32, 1), t3.closed.load(.monotonic));
}
```

### 12.6 Testing Tips

- **Use `.sleeping` idle strategy in tests.** Busy-spin and yielding burn CPU
  unnecessarily in test suites and can cause CI flakiness.

- **Use short sleep durations** (2–10ms) to let threads run a few cycles. Don't sleep
  for seconds — the event loop processes work in microseconds.

- **Use `std.atomic.Value` for shared test state.** Counters and flags accessed from both
  the test thread and the event loop thread must be atomic.

- **Test command queues independently** before testing them inside event loops. Isolate
  the ring buffer behavior from the threading behavior.

- **SHARED mode is the easiest to debug.** If a threading integration test fails, try
  reproducing it in SHARED mode first — everything runs sequentially on one thread,
  eliminating race conditions.

---

## 13. File Structure

```
src/
  threading/
    command.zig              # Command struct definition
    command_queue.zig        # CommandQueue (MPSC ring buffer wrapper)
    composite_event_loop.zig # CompositeEventLoop for SHARED/SHARED_NETWORK modes
    threading_mode.zig       # ThreadingMode enum (dedicated/shared_network/shared)
    broker_threads.zig       # BrokerThreads — owns 3 ThreadRunners, start/shutdown
    tests/
      idle_strategy_test.zig
      command_queue_test.zig
      thread_runner_test.zig
      composite_event_loop_test.zig
      threading_mode_test.zig
  platform/
    thread.zig               # EventLoop, ThreadRunner, IdleStrategy (from doc 01)
```

---

## 14. Implementation Steps

1. **Create `src/threading/command.zig`.** Define the `Command` struct (handler function
   pointer + data pointer). This is 16 bytes, no dependencies.

2. **Create `src/threading/command_queue.zig`.** Implement `CommandQueue` wrapping a
   `RingBuffer` from doc 03. Implement `enqueue()` (writes command bytes) and `drain()`
   (reads and dispatches via threadlocal). Write unit tests.

3. **Create `src/threading/composite_event_loop.zig`.** Implement `CompositeEventLoop`
   that wraps two `EventLoop` instances and sums their work counts. Write unit tests.

4. **Create `src/threading/threading_mode.zig`.** Define the `ThreadingMode` enum:
   `dedicated`, `shared_network`, `shared`.

5. **Create `src/threading/broker_threads.zig`.** Implement `BrokerThreads` that owns
   three `ThreadRunner` instances. Implement `init()`, `start()`, `shutdown()`,
   `isRunning()`.

6. **Wire command queues into event loops.** Extend `ControlLoop`, `SenderEventLoop`, and
   `ReceiverEventLoop` to accept command queue pointers. Add `cmd_queue.drain(limit)` as
   the first step in each `doWork()`.

7. **Define concrete command types.** Create command structs for each inter-loop command
   (add peer, remove peer, peer connected, etc.) with self-dispatching handlers. These
   go in `src/control/commands.zig`, `src/send/commands.zig`, `src/recv/commands.zig`.

8. **Implement broker startup wiring.** In `src/broker.zig`, allocate command queue
   buffers, create ring buffers, initialize event loops, inject command queue references,
   select threading mode, and start.

9. **Implement shutdown sequence.** Stop receiver → sender → control, join all threads,
   free command queue buffers.

10. **Add signal handling.** Install `SIGINT`/`SIGTERM` handlers that set running flags
    to false. Implement `awaitShutdown()`.

11. **Add CPU affinity support.** Extend `ThreadRunner` with an optional `cpu_affinity`
    field. Apply it in `threadMain()` after `setThreadName()`.

12. **Write integration tests.** DEDICATED mode with 3 threads and command flow.
    SHARED mode with single thread. Shutdown sequence with onClose verification.

---

*Previous: [09 — Control Plane](09-control-plane.md) · Next: [11 — Cluster Management](11-cluster-management.md)*