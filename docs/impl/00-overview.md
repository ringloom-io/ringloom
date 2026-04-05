# BRZ Broker Implementation Guide — Overview & Index

This document series describes how to build **BRZ**, a high-performance IPC broker in
**Zig**, from the ground up. Each document covers a self-contained layer of the system,
ordered so that later documents build only on concepts and code introduced earlier.

---

## Project Overview

BRZ is a high-performance IPC framework designed for low-latency, high-throughput
communication between services on the same host and across hosts.

- **Same-host communication** uses shared-memory ring buffers. Services write messages
  directly into memory-mapped regions; the broker (or peer service) reads them with no
  kernel involvement on the hot path.
- **Cross-host communication** uses a custom UDP wire protocol. Each broker maintains
  UDP sockets to its peers and handles fragmentation, retransmission, and flow control
  in userspace.
- **A broker process runs on every host.** It coordinates message routing between local
  services, manages service discovery and registration, and participates in cluster-wide
  leader election and state synchronization.

The architecture is inspired by [Aeron](https://github.com/real-logic/aeron) but is
**significantly simplified**: one stream per peer (no multiplexed stream IDs), no
command-and-control (CnC) file, no log rotation, and no pub/sub abstraction. Services
talk to the broker; the broker talks to other brokers. That's it.

---

## Key Architectural Change: io_uring

On Linux, this implementation replaces the traditional `epoll` + `sendmsg`/`recvmmsg`
approach with **io_uring** for all I/O polling and UDP socket operations.

### Why io_uring?

| Traditional (`epoll` + syscalls)            | io_uring                                        |
|---------------------------------------------|-------------------------------------------------|
| One syscall per `sendmsg`/`recvmsg` call    | Batch N submissions in a single `io_uring_enter` |
| Context switch on every I/O operation       | Kernel processes submissions without context switches (shared ring) |
| Separate readiness notification (`epoll`) and I/O (`sendmsg`) | Unified: submit I/O, get completions — one mechanism |
| No kernel-side polling                      | `SQPOLL` mode: kernel thread polls the submission queue, zero syscalls in steady state |

The key benefits for a broker workload:

1. **Fewer syscalls.** A broker sending 10 packets to different peers can submit all 10
   as SQEs and call `io_uring_enter` once (or zero times with SQPOLL). With `sendmsg`,
   that's 10 syscalls.
2. **No context switch per I/O.** The kernel drains the submission queue in batch,
   avoiding per-operation user↔kernel transitions.
3. **Kernel-side polling (SQPOLL).** In high-throughput scenarios, a kernel thread polls
   the SQ continuously — the broker never calls `io_uring_enter` at all.
4. **Natural batching.** The SQ/CQ ring structure aligns perfectly with the broker's
   duty-cycle event loop: submit everything accumulated this cycle, then drain
   completions.

### Other Platforms

- **macOS** — `kqueue` with `EVFILT_READ`/`EVFILT_WRITE` kevent filters and standard
  `sendmsg`/`recvmsg` calls.
- **Windows** — IOCP (`CreateIoCompletionPort`, `WSASendTo`, `WSARecvFrom` with
  overlapped I/O).

The io_uring integration is detailed in
[`04-udp-transport-and-io-uring.md`](04-udp-transport-and-io-uring.md).

---

## Implementation Order

The documents are ordered so that each phase depends only on previous phases.

| Phase | Document | Description |
|:-----:|----------|-------------|
| 1 | [`01-platform-abstraction.md`](01-platform-abstraction.md) | OS abstractions: `mmap`, clocks, threads, atomics, process synchronization (`futex`, `ulock`, `WaitOnAddress`) |
| 2 | [`02-memory-layout-and-shared-memory.md`](02-memory-layout-and-shared-memory.md) | Metadata files, shared memory regions, memory-mapped I/O, file layout constants |
| 3 | [`03-concurrent-data-structures.md`](03-concurrent-data-structures.md) | MPSC ring buffer, atomic counters, error log, trailer layout |
| 4 | [`04-udp-transport-and-io-uring.md`](04-udp-transport-and-io-uring.md) | UDP wire protocol, `io_uring` integration, socket management, platform I/O backends |
| 5 | [`05-send-path.md`](05-send-path.md) | Send ring buffer, sender event loop, message fragmentation, retransmit buffer |
| 6 | [`06-receive-path.md`](06-receive-path.md) | Receive log buffer, packet insertion, loss detection, fragment reassembly |
| 7 | [`07-flow-control.md`](07-flow-control.md) | Window-based flow control, status messages, back-pressure signaling |
| 8 | [`08-service-ipc.md`](08-service-ipc.md) | Service ↔ broker IPC, same-host direct path, cross-host routed path |
| 9 | [`09-control-plane.md`](09-control-plane.md) | Control messages, service registration, discovery, heartbeats |
| 10 | [`10-threading-model.md`](10-threading-model.md) | Event loop architecture, idle strategies, inter-loop command passing |
| 11 | [`11-cluster-management.md`](11-cluster-management.md) | Leader election, state synchronization, admin messages between brokers |
| 12 | [`12-configuration-and-monitoring.md`](12-configuration-and-monitoring.md) | Configuration loading, monitoring counters, error handling patterns |

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

7. **io_uring for all network I/O on Linux (batched submissions, zero-copy where
   possible).**
   UDP sends, receives, and timeout management all flow through the `io_uring`
   submission/completion rings. On other platforms, equivalent batching strategies are
   used where the OS supports them.

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
| **IPC** | Inter-Process Communication. Data exchange between OS processes, here via shared memory (same host) or UDP (cross host). |
| **CAS** | Compare-And-Swap. An atomic CPU instruction that writes a new value only if the current value matches an expected value. The foundation of lock-free algorithms. |
| **SHM** | Shared Memory. Memory-mapped regions (typically on `tmpfs` / `/dev/shm`) accessible by multiple processes. |
| **MTU** | Maximum Transmission Unit. The largest packet size a network link can carry without fragmentation. Typically 1500 bytes for Ethernet; the broker fragments messages larger than one MTU. |
| **NAK** | Negative Acknowledgement. A signal from a receiver to a sender indicating that a specific packet (or range) was not received and should be retransmitted. |
| **SM (Status Message)** | A control message sent from receiver to sender carrying the receiver's current window position, consumption position, and receiver window length. Used for flow control. |
| **SQE** | Submission Queue Entry. An `io_uring` structure describing an I/O operation to submit to the kernel. |
| **CQE** | Completion Queue Entry. An `io_uring` structure describing the result of a completed I/O operation. |
| **Flyweight** | A zero-copy accessor pattern. Instead of deserializing data into a struct, a flyweight overlays a typed view directly on the underlying buffer. Reads and writes go straight to the buffer bytes. |
| **Duty Cycle** | One iteration of an event loop that polls all registered work sources and returns a total work count. If no work was done, the idle strategy decides whether to spin, yield, or park. |
| **Idle Strategy** | A pluggable policy that decides what to do when an event loop iteration finds no work. Common strategies: busy-spin, yield, sleep, or kernel-level blocking (futex/ulock). |
| **Position** | A monotonically increasing `i64` byte offset representing progress through a ring buffer or log. The actual index is `position & (capacity - 1)`. Positions never wrap or decrease. |
| **Term** | A logical generation counter for a connection or stream. Incremented on reconnection. Used to distinguish stale packets from a previous session. |
| **Ring Buffer Trailer** | A fixed-size region at the end of a ring buffer's backing memory containing atomic counters (head position, tail position, correlation IDs) and cache-line padding. |
| **Blocking Mode** | A ring buffer operating mode where the producer parks (via `futex`/`ulock`/`WaitOnAddress`) when the buffer is full, and the consumer wakes it after advancing the head. Adds extra cache lines to the trailer for wait state. |
| **nodeId** | A unique integer identifier for a broker (host) within the cluster. Assigned in configuration. Used for message routing (`targetNodeId`, `sourceNodeId`). |
| **serviceId** | A unique integer identifier for a service instance on a given host. Assigned by the broker at registration time. Combined with `nodeId` to form a globally unique address. |
| **templateId** | An integer identifying the type of a message (analogous to an SBE template ID). Used by message handlers to dispatch incoming messages to the correct decoder and handler function. |

---

*Next: [01 — Platform Abstraction](01-platform-abstraction.md)*