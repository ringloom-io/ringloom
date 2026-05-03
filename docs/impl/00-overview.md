# RingLoom Broker Implementation Guide — Overview & Index

This document series describes how to build **RingLoom**, a high-performance IPC broker in
**Zig**, from the ground up. Each document covers a self-contained layer of the system,
ordered so that later documents build only on concepts and code introduced earlier.

---

## Project Overview

RingLoom is a high-performance IPC framework designed for low-latency, high-throughput
communication between services on the same host and across hosts.

- **Same-host communication** uses shared-memory ring buffers. Services write messages
  directly into memory-mapped regions; the broker (or peer service) reads them with no
  kernel involvement on the hot path.
- **Cross-host communication** uses TCP connections between brokers. Each broker pair
  communicates over a pair of unidirectional TCP connections (one for sending, one for
  receiving). The TCP transport lives in a separate library (`ringloom_tcp`) with pluggable
  I/O backends: `io_uring` on Linux, `kqueue` on macOS, and future optional kernel-bypass
  backends (TCPDirect, F-Stack).
- **A broker process runs on every host.** It coordinates message routing between local
  services, manages service discovery and registration, and participates in cluster-wide
  leader election and state synchronization.

The architecture is deliberately simple: one TCP connection pair per peer (no multiplexed
stream IDs), no command-and-control (CnC) file, no log rotation, no pub/sub abstraction,
and no custom reliable transport protocol. TCP provides reliability, ordering, and flow
control natively. Services talk to the broker; the broker talks to other brokers. That's it.

---

## Key Architectural Choices

### TCP

- TCP handles reliability, ordering, segmentation, and flow control in the kernel.
- The wire protocol is reduced to **length-prefixed message framing** with a 24-byte header.
- No multiplexing: each peer connection is a simple unidirectional stream. No stream IDs,
  no interleaving, no reordering.

### Completion-Based I/O via `ringloom_tcp` Library

All TCP I/O goes through the `ringloom_tcp` library, which provides a platform-abstracted,
completion-based I/O engine:

| Platform | Backend | API |
|----------|---------|-----|
| **Linux** | `io_uring` | `IORING_OP_SEND`, `IORING_OP_RECV`, `IORING_OP_ACCEPT`, batched completions |
| **macOS** | `kqueue` | `EVFILT_READ`/`EVFILT_WRITE`, edge-triggered, non-blocking sockets |
| **Future** | TCPDirect | User-space TCP over Onload-capable NICs (compile-time flag) |
| **Future** | F-Stack | FreeBSD TCP stack on DPDK (compile-time flag) |

The key benefits:
1. **Fewer syscalls.** io_uring batches multiple submissions per `io_uring_enter` call.
2. **No context switch per I/O.** Kernel drains submission queue in batch.
3. **Unified I/O model.** TCP reads, writes, accepts, and connects all flow through the
   same completion ring.
4. **Pluggable backends.** The common `IoEngine` interface allows kernel-bypass backends
   to be added without changing broker code.

### Always-Read Back-Pressure Model

The receiver event loop **always reads** from all peer TCP connections. It never pauses
socket reads. If a target service's ring buffer is full, the message is dropped and a
counter is incremented. This prevents head-of-line blocking where one slow service blocks
admin, heartbeat, and other service traffic from the same peer.

---

## Implementation Order

The documents are ordered so that each phase depends only on previous phases.

| Phase | Document | Description |
|:-----:|----------|-------------|
| 1 | [`01-platform-abstraction.md`](01-platform-abstraction.md) | OS abstractions: `mmap`, clocks, threads, atomics, process synchronization (`futex`, `ulock`, `WaitOnAddress`) |
| 2 | [`02-memory-layout-and-shared-memory.md`](02-memory-layout-and-shared-memory.md) | Metadata files, shared memory regions, memory-mapped I/O, file layout constants |
| 3 | [`03-concurrent-data-structures.md`](03-concurrent-data-structures.md) | MPSC ring buffer, atomic counters, error log, trailer layout |
| 4 | [`04-tcp-transport-library.md`](04-tcp-transport-library.md) | `ringloom_tcp` library: I/O engine interface, io_uring/kqueue backends, connection management, message framing |
| 5 | [`05-send-path.md`](05-send-path.md) | Send ring buffer, sender event loop, per-peer write queues, TCP write mechanics |
| 6 | [`06-receive-path.md`](06-receive-path.md) | TCP framing, receiver event loop, message routing, connection acceptance |
| 7 | [`08-service-ipc.md`](08-service-ipc.md) | Service ↔ broker IPC, same-host direct path, cross-host routed path |
| 8 | [`09-control-plane.md`](09-control-plane.md) | Control messages, service registration, discovery, heartbeats |
| 9 | [`10-threading-model.md`](10-threading-model.md) | Event loop architecture, idle strategies, inter-loop command passing |
| 10 | [`11-cluster-management.md`](11-cluster-management.md) | Leader election, state synchronization, admin messages between brokers |
| 11 | [`12-configuration-and-monitoring.md`](12-configuration-and-monitoring.md) | Configuration loading, monitoring counters, error handling patterns |
| 12 | [`13-library-split-and-packaging.md`](13-library-split-and-packaging.md) | Library boundaries: `ringloom_common`, `ringloom_tcp`, `ringloom_broker`, `ringloom_service` |
| 13 | [`14-broker-executable-and-startup.md`](14-broker-executable-and-startup.md) | Broker main, startup sequence, signal handling |
| 14 | [`15-end-to-end-and-performance-testing.md`](15-end-to-end-and-performance-testing.md) | End-to-end test framework, multi-broker test scenarios, performance validation |
| 15 | [`16-service-c-abi-and-java-bindings.md`](16-service-c-abi-and-java-bindings.md) | Service shared library packaging, C ABI, zero-copy Java FFM bindings, Java integration tests |

**Note:** The previous `07-flow-control.md` has been removed. Back-pressure is now covered
in the send path (05) and receive path (06) documents, since TCP delegates flow control to
the kernel and the broker handles application-level back-pressure through message dropping.

---

## Design Principles

These invariants hold throughout the entire codebase. Violating any of them on the hot
path is a bug.

1. **Zero-copy on hot path (shared memory IPC).**
   Services write directly into shared memory ring buffers. The broker reads from those
   same mapped pages. No intermediate copies, no serialization boundaries on the local
   path.

2. **Lock-free everywhere (atomics, CAS, no mutexes).**
   All shared data structures use atomic operations — loads, stores, compare-and-swap.
   No `pthread_mutex`, no `std.Thread.Mutex`, no OS-level locks on any path that
   touches message data.

3. **Single-writer principle (every mutable location has one writer).**
   Each field in shared memory has exactly one writer. The tail counter is written only
   by producers. The head counter is written only by the consumer. This eliminates
   contention and makes reasoning about correctness tractable.

4. **Allocation-free hot path (pre-allocated buffers, flyweight patterns).**
   All buffers are allocated at startup. Message encoding and decoding use flyweight
   overlays on existing memory — no `allocator.alloc()` in the message path.

5. **Position-based progress tracking (monotonic `i64` positions).**
   Producers and consumers communicate progress through monotonically increasing byte
   positions. A position never decreases. The current offset into the ring buffer is
   `position & (capacity - 1)`.

6. **Duty-cycle event loops (tight loops returning work counts driving idle strategies).**
   Each event loop iteration calls a set of duty-cycle functions. Each function returns
   a work count (number of items processed). If the total work count is zero, the idle
   strategy engages (spin → yield → park). If work was done, the loop runs again
   immediately.

7. **Completion-based I/O via `ringloom_tcp` (io_uring on Linux, kqueue on macOS).**
   TCP reads, writes, accepts, and connects all flow through the platform's async I/O
   engine. On Linux, `io_uring` batches submissions and completions for maximum
   throughput. On macOS, `kqueue` provides edge-triggered event notification.

---

## Zig-Specific Notes

Zig is chosen for its explicit control over memory, lack of hidden behavior, and
first-class support for the low-level patterns this project requires.

- **`comptime` for configuration.** Buffer sizes, alignment requirements, header sizes,
  and capacity masks are computed at compile time. A buffer size that isn't a power of
  two is a compile error, not a runtime check.

- **No hidden allocations.** Zig never allocates behind your back. Every allocation goes
  through an explicit `Allocator` interface, making it trivial to verify that the hot
  path is allocation-free.

- **`packed struct` for wire format mapping.** Protocol headers and metadata structures
  are defined as `packed struct` and overlaid directly onto memory-mapped regions or
  network buffers — the Zig equivalent of SBE flyweights.

- **Atomic builtins map to hardware instructions.** `@atomicLoad`, `@atomicStore`,
  `@cmpxchgStrong`, and `@fence` compile to the exact x86/ARM instructions needed.
  No abstraction layer, no function call overhead.

- **C ABI compatibility for platform syscalls.** Zig can call `mmap`, `futex`,
  `io_uring_setup`, and any other libc or direct syscall without bindings generators or
  FFI overhead. `std.os.linux` exposes raw syscall wrappers.

- **Cross-compilation from a single build.** `zig build -Dtarget=x86_64-linux` works on
  macOS. CI can produce Linux, macOS, and Windows binaries from one machine.

- **Explicit memory management via allocator interface.** Startup code uses
  `std.heap.page_allocator` or an arena. Hot-path code uses pre-allocated slices. The
  allocator interface makes this boundary visible and enforceable.

- **Target Zig 0.14.x stable.** All code targets the latest 0.14.x stable release.
  Avoid `master`-only features; pin the version in `build.zig.zon`.

---

## Glossary of Key Terms

| Term | Definition |
|------|------------|
| **MPSC** | Multi-Producer, Single-Consumer. A ring buffer that supports multiple concurrent writers but exactly one reader. Producers coordinate via CAS on the tail position. |
| **IPC** | Inter-Process Communication. Data exchange between OS processes, here via shared memory (same host) or TCP (cross host). |
| **CAS** | Compare-And-Swap. An atomic CPU instruction that writes a new value only if the current value matches an expected value. The foundation of lock-free algorithms. |
| **SHM** | Shared Memory. Memory-mapped regions (typically on `tmpfs` / `/dev/shm`) accessible by multiple processes. |
| **SQE** | Submission Queue Entry. An `io_uring` structure describing an I/O operation to submit to the kernel. |
| **CQE** | Completion Queue Entry. An `io_uring` structure describing the result of a completed I/O operation. |
| **Flyweight** | A zero-copy accessor pattern. Instead of deserializing data into a struct, a flyweight overlays a typed view directly on the underlying buffer. Reads and writes go straight to the buffer bytes. |
| **Duty Cycle** | One iteration of an event loop that polls all registered work sources and returns a total work count. If no work was done, the idle strategy decides whether to spin, yield, or park. |
| **Idle Strategy** | A pluggable policy that decides what to do when an event loop iteration finds no work. Common strategies: busy-spin, yield, sleep, or kernel-level blocking (futex/ulock). |
| **Position** | A monotonically increasing `i64` byte offset representing progress through a ring buffer or log. The actual index is `position & (capacity - 1)`. Positions never wrap or decrease. |
| **Ring Buffer Trailer** | A fixed-size region at the end of a ring buffer's backing memory containing atomic counters (head position, tail position, correlation IDs) and cache-line padding. |
| **Blocking Mode** | A ring buffer operating mode where the producer parks (via `futex`/`ulock`/`WaitOnAddress`) when the buffer is full, and the consumer wakes it after advancing the head. Adds extra cache lines to the trailer for wait state. |
| **nodeId** | A unique integer identifier for a broker (host) within the cluster. Assigned in configuration. Used for message routing (`targetNodeId`, `sourceNodeId`). |
| **serviceId** | A unique integer identifier for a service instance on a given host. Assigned by the broker at registration time. Combined with `nodeId` to form a globally unique address. |
| **templateId** | An integer identifying the type of a message (analogous to an SBE template ID). Used by message handlers to dispatch incoming messages to the correct decoder and handler function. |
| **session_epoch** | A monotonically increasing counter included in the TCP connection handshake. Incremented on each broker restart. Used to distinguish stale connections from current ones. |

---
