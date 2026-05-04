# High-Performance io_uring Networking Architecture (Zig 0.16)

## Executive Summary

This document outlines the optimal architecture for achieving lowest-latency, highest-throughput host-to-host messaging using Linux's io_uring, implemented in Zig 0.16 using its `std.os.linux.IoUring` abstraction. It covers the latest kernel features (up to 6.12+), including ring-provided buffers, zero-copy send, multishot accept/recv, send/recv bundles, and recommended ring setup flags. The reference architecture is informed by Jens Axboe's `proxy.c` reference implementation in liburing.

---

## 1. Key io_uring Features for Networking

### Tier 1: Foundational (Must-Have)

| Feature | Kernel | What It Does |
|---------|--------|-------------|
| **Ring-Provided Buffer Rings** | 5.19+ | Shared-memory ring for buffer handoff — zero syscalls to replenish buffers |
| **Multishot Accept (direct)** | 5.19+ | One SQE → unlimited accepts, each into a private (direct) file descriptor |
| **Multishot Recv** | 6.0+ | One SQE → unlimited receives, auto-selects buffers from the ring |
| **Zero-Copy Send** | 6.0+ | Sends data directly from userspace buffers via DMA — no `memcpy` into kernel |
| **Fixed Files (Direct Descriptors)** | 5.12+ | Avoids `fget()/fput()` atomic refcounting on the shared fd table |
| **FAST_POLL** | 5.7+ | Kernel arms poll internally instead of offloading blocked ops to worker threads (automatic) |

### Tier 2: Latency Killers (Highly Recommended)

| Feature | Kernel | What It Does |
|---------|--------|-------------|
| **`IORING_SETUP_SQPOLL`** | 5.11+ | Dedicated kernel thread polls SQ — zero syscalls for submissions |
| **`IORING_SETUP_SINGLE_ISSUER`** | 6.0+ | Kernel skips all locking on SQ since only one thread submits |
| **`IORING_SETUP_DEFER_TASKRUN`** | 6.1+ | Eliminates inter-processor interrupts (IPIs) for completion notification |
| **`IORING_SETUP_COOP_TASKRUN`** | 5.19+ | Avoids heavy signal-based completion notifications |
| **`IOSQE_CQE_SKIP_SUCCESS`** | 5.17+ | Suppress CQEs for intermediate linked ops — reduces CQ ring traffic |

### Tier 3: Bleeding Edge (Maximum Throughput)

| Feature | Kernel | What It Does |
|---------|--------|-------------|
| **Send/Recv Bundles** | 6.10+ | Single SQE sends/receives across *multiple* buffers — reduces stack traversals |
| **Incremental Buffer Consumption** (`IOU_PBUF_RING_INC`) | 6.12+ | Partial buffer consumption — completions continue where the previous left off |
| **`IORING_RECVSEND_POLL_FIRST`** | 5.19+ | Skip initial recv attempt, go straight to poll (for typically-idle sockets) |

---

## 2. How Each Feature Reduces Latency

```
Traditional TCP recv path (epoll):
  epoll_wait()          ~1-2μs syscall overhead
  → recv()             ~1-2μs syscall overhead
  → copy_to_user()     ~0.5μs per 4KB
  → rearm epoll         ~1μs
  TOTAL per message:    ~4-6μs

io_uring optimized path:
  SQPOLL                 0μs    (no submission syscall)
  DEFER_TASKRUN          0μs    (no IPI for completion)
  FAST_POLL              0μs    (no async worker thread)
  Multishot recv         0μs    (no resubmission)
  Buffer ring            0μs    (no buffer provisioning syscall)
  Fixed files            0μs    (no fd refcount atomics)
  Batch CQ processing    amortized across N completions
  TOTAL per message:     ~0.5-1.5μs (kernel processing only)

Zero-copy send saves:
  Eliminates memcpy     ~0.3μs per 4KB → 0μs
  Fixed buffer variant   eliminates page pinning (~0.2μs) → 0μs
```

---

## 3. Feature Deep-Dives

### 3.1 `IORING_SETUP_SQPOLL` — Submission Queue Kernel Polling

Creates a dedicated kernel thread that continuously polls the submission queue for new entries, eliminating the need for `io_uring_enter()` system calls entirely.

- **Zero syscall submission path.** The application writes SQEs to the shared-memory ring and the kernel thread picks them up — no context switch needed.
- The kernel thread spins for `sq_thread_idle` milliseconds before sleeping. If it sleeps, the `IORING_SQ_NEED_WAKEUP` flag is set and the application must call `io_uring_enter()` once to wake it.
- Combined with `IORING_SETUP_SQ_AFF`, the poller thread can be pinned to a specific CPU core.
- Since Linux 5.11, fixed file registration is no longer mandatory (`IORING_FEAT_SQPOLL_NONFIXED`).
- Since Linux 5.13, no special privileges are required.
- `IORING_SETUP_ATTACH_WQ` allows multiple rings to share the same polling thread.

### 3.2 `IORING_SETUP_SINGLE_ISSUER`

Hints to the kernel that only one task/thread will submit requests (enforced with `-EEXIST`).

- Enables internal lock-free optimizations — the kernel skips atomic operations and locking on the SQ ring.
- When combined with `SQPOLL`, the polling task is considered the single issuer.
- **Available since:** Linux 6.0.
- **Best practice:** Always set this flag for single-threaded event loops. Free performance.

### 3.3 `IORING_SETUP_DEFER_TASKRUN`

Defers completion processing until the application explicitly requests it via `io_uring_enter()` with `IORING_ENTER_GETEVENTS`.

- Eliminates inter-processor interrupts (IPIs) that the kernel normally sends to notify of completions (1–5μs each).
- Gives full control over *when* completions are processed, enabling optimal batching.
- Requires `IORING_SETUP_SINGLE_ISSUER`.
- **Available since:** Linux 6.1.
- **Note:** Requires `IORING_ENTER_EXT_ARG` on every `io_uring_enter` call (see Zig compatibility section).

### 3.4 Ring-Provided Buffer Rings (`IORING_REGISTER_PBUF_RING`)

A shared-memory ring between userspace and kernel for buffer provisioning. Replaces the old `io_uring_prep_provide_buffers` SQE-based approach.

**Old vs New Provided Buffers:**

| Aspect | Old (`provide_buffers` SQE) | New (Buffer Ring) |
|--------|---------------------------|-------------------|
| Replenishment | Requires SQE submission | Userspace writes to shared memory — **no syscall** |
| Kernel version | 5.7+ | **5.19+** |
| Contention | Each provide is a kernel operation | Lock-free ring |
| Bundle support | No | Yes (6.10+) |
| Incremental consumption | No | Yes via `IOU_PBUF_RING_INC` (6.12+) |

**How it works:**

The ring is a `struct io_uring_buf_ring` with a `tail` index. The application writes buffer entries (`addr`, `len`, `bid`) and advances the tail atomically. The kernel consumes from the head. Ring must be page-aligned, power-of-2 in size (max 32768 entries).

**Key properties:**
- Zero-overhead buffer provisioning — simple memory write + tail update
- Required by multishot recv and bundle recv
- Buffer ID returned in CQE `flags` via `IORING_CQE_F_BUFFER`
- Buffer IDs are logical handles — you can recycle the same ID with a different address each time

### 3.5 Multishot Accept (`io_uring_prep_multishot_accept`)

A single accept SQE continuously produces CQEs for each incoming connection.

- **One SQE → N accepts.** Eliminates resubmission overhead entirely.
- The `_direct` variant allocates io_uring-private file descriptors, avoiding the shared file table lock.
- Check `IORING_CQE_F_MORE` in each CQE — if absent, multishot has terminated, resubmit.
- Don't pass `addr`/`addrlen` with multishot — data can be overwritten by the next connection before processing.
- `SOCK_CLOEXEC` is **not supported** with direct variants (returns `-EINVAL`).
- **Available since:** Linux 5.19.

### 3.6 Multishot Recv (`io_uring_prep_recv_multishot`)

A single recv SQE continuously posts CQEs as data arrives, automatically selecting buffers from a provided buffer group.

**Requirements:**
- `len` must be **0** (buffer comes from the provided buffer ring)
- `IOSQE_BUFFER_SELECT` must be set
- `MSG_WAITALL` must **NOT** be set
- A buffer group must be registered

**Advantages:**
- **One SQE → N receives** for the lifetime of a connection
- `IORING_RECVSEND_POLL_FIRST` ioprio flag skips the initial (likely empty) recv attempt
- `IORING_RECVSEND_BUNDLE` flag (6.10+) fills *multiple* buffers in a single recv
- `IORING_CQE_F_SOCK_NONEMPTY` in CQE flags tells you more data is available immediately
- **Available since:** Linux 6.0.

### 3.7 Zero-Copy Send (`io_uring_prep_send_zc`)

Sends data directly from userspace buffers without copying into kernel socket buffers.

**The Two-CQE Model (critical to understand):**

```
SQE submitted
  → CQE #1: send result (success/failure, byte count)
      flags: IORING_CQE_F_MORE (if notification coming)
  → CQE #2: notification CQE
      flags: IORING_CQE_F_NOTIF (buffer is now safe to reuse)
```

The application **must not modify or free the source buffer** until the notification CQE arrives.

**Variants:**
- `send_zc` — basic zero-copy send
- `send_zc_fixed` — uses pre-registered buffers (eliminates `get_user_pages()` per send)
- `sendmsg_zc` — zero-copy sendmsg

**`IORING_SEND_ZC_REPORT_USAGE` flag:** Check if kernel actually achieved zero-copy or fell back to copying. On the notification CQE: `res == 0` → true ZC; `res == IORING_NOTIF_USAGE_ZC_COPIED` → fallback copy.

**Caveats:**
- `ulimit -l` must be sufficient for pinned pages (otherwise `-ENOMEM`)
- When using `IOSQE_IO_LINK`, you **must** set `MSG_WAITALL`
- For small messages (< ~1KB), regular send may be faster due to page pinning overhead
- **Available since:** Linux 6.0.

### 3.8 Send/Recv Bundles (6.10+)

#### Recv Bundles (`IORING_RECVSEND_BUNDLE`)

Set in SQE `ioprio`. Instead of consuming a single buffer, the kernel fills **as many contiguous buffers as possible** from available socket data.

- `res` = total bytes across all buffers consumed
- CQE flags contain the **starting buffer ID** — walk contiguously from there
- Requires ring-type provided buffers
- Works with both single-shot and multishot recv
- The first bundle recv may be conservative; subsequent ones become more aggressive

#### Send Bundles

A single SQE sends from **multiple contiguous buffers** in a provided buffer ring.

- Eliminates redundant networking stack round-trips — one stack traversal sends everything
- Guarantees ordering — buffers consumed contiguously from the ring
- Requires `IORING_FEAT_SEND_BUF_SELECT` feature flag (check via `params.features`)
- Currently `len` must be 0

### 3.9 Fixed/Registered Buffers (`io_uring_register_buffers`)

Pre-registers `iovec` arrays with the kernel. The kernel maps pages **once at registration time**, avoiding `get_user_pages_fast()` on every operation.

**Fixed buffers vs. provided buffer rings are complementary:**

| Aspect | Fixed Buffers | Provided Buffer Rings |
|--------|--------------|----------------------|
| Purpose | Avoid per-I/O page pinning | Dynamic buffer selection at completion time |
| Selection | App specifies index at **submission** | Kernel selects at **completion** |
| Use case | Known I/O targets (pre-planned sends) | Unknown I/O timing (network recv) |
| Kernel version | 5.1+ | 5.19+ |

### 3.10 SQE Linking and Batching

- `IOSQE_IO_LINK` — chain SQEs for sequential execution (e.g., `shutdown → close`)
- `IOSQE_IO_HARDLINK` — continue chain even on failure
- `IOSQE_CQE_SKIP_SUCCESS` — suppress intermediate CQEs to reduce CQ ring traffic
- `IORING_SETUP_SUBMIT_ALL` — process entire batch even if one SQE errors

---

## 4. Optimal Architecture

### 4.1 Ring Topology: Thread-per-Ring with `SINGLE_ISSUER`

```
┌──────────────────────────────────────────────────────────┐
│  ACCEPTOR THREAD (pinned to core 0)                      │
│  ┌─────────────────────────────┐                         │
│  │ Ring 0 (SINGLE_ISSUER)      │                         │
│  │ • multishot_accept_direct   │                         │
│  │ • on accept → dispatch      │                         │
│  └─────────────────────────────┘                         │
│                                                          │
│  WORKER THREADS (pinned to cores 1..N)                   │
│  ┌─────────────────────────────┐  ┌──────────────────┐   │
│  │ Ring 1 (SINGLE_ISSUER)      │  │ Ring 2 ...       │   │
│  │ • multishot_recv + bundles  │  │                  │   │
│  │ • send_zc / send_bundle    │  │                  │   │
│  │ • own buffer rings (rx+tx) │  │                  │   │
│  │ • own fixed file table     │  │                  │   │
│  └─────────────────────────────┘  └──────────────────┘   │
│                                                          │
│  Cross-ring: msg_ring for FD passing & signaling         │
└──────────────────────────────────────────────────────────┘
```

**Why thread-per-ring wins over shared ring:**
- `SINGLE_ISSUER` enables kernel-internal lock-free optimizations
- `DEFER_TASKRUN` only works with `SINGLE_ISSUER`
- Zero contention between connections
- Each ring's event loop is fully independent
- Cross-thread communication uses `msg_ring` — no mutexes needed

### 4.2 Ring Setup Flags

```zig
const linux = std.os.linux;

const setup_flags =
    linux.IORING_SETUP_SQPOLL |          // zero-syscall submissions
    linux.IORING_SETUP_SINGLE_ISSUER |   // lock-free SQ internals
    linux.IORING_SETUP_COOP_TASKRUN |    // no signal-based wakeups
    linux.IORING_SETUP_DEFER_TASKRUN |   // no IPIs, batched completions
    linux.IORING_SETUP_SUBMIT_ALL |      // don't stop batch on individual errors
    linux.IORING_SETUP_CQSIZE;           // allow oversized CQ ring

var params = std.mem.zeroes(linux.io_uring_params);
params.flags = setup_flags;
params.cq_entries = 4096;  // oversized CQ to prevent overflow

var ring = try IoUring.init_params(256, &params);

// Register the ring fd itself (avoids fget/fput on every io_uring_enter)
// Note: may need manual syscall if high-level API doesn't expose this
try ring.register_ring_fd();

// Pre-register a sparse file table for direct descriptors
try ring.register_files_sparse(max_connections);

// Set the direct descriptor allocation range
try ring.register_file_alloc_range(0, max_connections);
```

### 4.3 The Ideal Receive Path

```zig
// === SETUP: Ring-Provided Buffer Ring ===
var recv_bufs = try IoUring.BufferGroup.init(
    &ring,
    allocator,
    0,          // group_id
    4096,       // buffer_size (tune to your message size / MTU)
    256,        // number of buffers (power of 2)
);
defer recv_bufs.deinit(allocator);

// === ARM: Single multishot recv (ONE SQE for the lifetime of the connection) ===
var sqe = try recv_bufs.recv_multishot(user_data, fd, 0);

// For EVEN MORE throughput, add bundle flag (kernel 6.10+):
sqe.ioprio |= linux.IORING_RECVSEND_BUNDLE;

// For typically-idle sockets, skip initial recv attempt:
sqe.ioprio |= linux.IORING_RECVSEND_POLL_FIRST;

// === COMPLETION: Process received data ===
const cqe = ...; // from copy_cqes
if (cqe.res > 0) {
    const data = recv_bufs.get(cqe);  // extracts buffer by ID from CQE flags
    process_message(data);
    recv_bufs.put(cqe);               // returns buffer to the ring (zero-syscall)
}

// Check if multishot is still armed
if (cqe.flags & linux.IORING_CQE_F_MORE == 0) {
    // Multishot terminated (error/close) — rearm if needed
    _ = try recv_bufs.recv_multishot(user_data, fd, 0);
}
```

### 4.4 The Ideal Send Path

```zig
// === OPTION A: Zero-Copy Send with Fixed Buffers ===
// Best for large messages (> ~1KB)

// Pre-register send buffers with the kernel (pages are pinned once)
const send_buffers = try allocator.alloc([4096]u8, 64);
var iovecs: [64]std.posix.iovec = undefined;
for (send_buffers, 0..) |*buf, i| {
    iovecs[i] = .{ .base = buf, .len = buf.len };
}
try ring.register_buffers(&iovecs);

// Send zero-copy using a fixed (pre-registered) buffer:
var sqe = try ring.get_sqe();
ring.send_zc_fixed(sqe, fd, data_ptr, data_len, 0, 0, buf_index);
sqe.flags |= linux.IOSQE_FIXED_FILE;  // fd is a direct descriptor

// Handle the TWO CQEs from zero-copy send:
// CQE 1: send result (check cqe.flags & IORING_CQE_F_MORE)
// CQE 2: notification (IORING_CQE_F_NOTIF) — NOW safe to reuse buffer


// === OPTION B: Send Bundle with Provided Buffer Ring ===
// Best for scatter-gather / multi-buffer sends (kernel 6.10+)

// Set up a send buffer ring (separate from recv ring!)
var send_ring = try IoUring.BufferGroup.init(
    &ring, allocator, 1, 4096, 128,  // group_id=1
);

// Fill the send ring with outgoing data chunks
IoUring.buf_ring_add(send_br, chunk1_ptr, chunk1_len, bid0, mask, 0);
IoUring.buf_ring_add(send_br, chunk2_ptr, chunk2_len, bid1, mask, 1);
IoUring.buf_ring_advance(send_br, 2);

// Single bundle send drains all buffers from the ring
var sqe = try ring.get_sqe();
// Manually prepare since there's no high-level bundle send wrapper:
sqe.prep_send(fd, null, 0, 0);
sqe.flags |= linux.IOSQE_BUFFER_SELECT | linux.IOSQE_FIXED_FILE;
sqe.buf_group = 1;
sqe.ioprio |= linux.IORING_RECVSEND_BUNDLE;
```

### 4.5 The Ideal Accept Path

```zig
// Single multishot accept with direct descriptors
// One SQE handles ALL incoming connections for the lifetime of the listener
var sqe = try ring.get_sqe();
ring.accept_multishot_direct(sqe, listen_fd, null, null, 0);
sqe.user_data = ACCEPT_USER_DATA;

// On completion:
const cqe = ...;
if (cqe.res >= 0) {
    const new_fd: u32 = @intCast(cqe.res);  // direct descriptor index

    // Pass fd to a worker ring via msg_ring (if using thread-per-ring)
    // OR arm multishot recv directly on this ring
    var recv_sqe = try recv_bufs.recv_multishot(
        make_user_data(.recv, new_fd),
        new_fd,
        0,
    );
    recv_sqe.flags |= linux.IOSQE_FIXED_FILE;
}
```

### 4.6 Event Loop

```zig
const batch_size = 256;
var cqes: [batch_size]linux.io_uring_cqe = undefined;

while (running) {
    // === SUBMIT + WAIT in a single operation ===
    // Submits all queued SQEs and waits for at least 1 CQE
    _ = try ring.submit_and_wait(1);

    // === BATCH-PROCESS all available completions ===
    const count = ring.copy_cqes(&cqes, 0);  // 0 = don't wait, just drain

    for (cqes[0..count]) |cqe| {
        const ud = decode_user_data(cqe.user_data);

        switch (ud.op) {
            .accept => {
                if (cqe.res >= 0) {
                    const fd: u32 = @intCast(cqe.res);
                    arm_multishot_recv(fd);
                }
                // Check if multishot accept is still armed
                if (cqe.flags & linux.IORING_CQE_F_MORE == 0) {
                    rearm_accept();
                }
            },
            .recv => {
                if (cqe.res > 0) {
                    const data = recv_bufs.get(cqe);
                    handle_message(ud.fd, data);
                    recv_bufs.put(cqe);  // zero-cost buffer return
                } else if (cqe.res == 0) {
                    // Connection closed
                    close_connection(ud.fd);
                }
                // Rearm if multishot terminated
                if (cqe.flags & linux.IORING_CQE_F_MORE == 0) {
                    if (cqe.res != 0) rearm_recv(ud.fd);
                }
            },
            .send => {
                if (cqe.flags & linux.IORING_CQE_F_MORE != 0) {
                    // Zero-copy: notification CQE coming, don't reuse buffer yet
                } else {
                    // Buffer safe to reuse
                    recycle_send_buffer(ud.buf_id);
                }
            },
            .send_notif => {
                // Zero-copy notification — buffer is NOW safe to reuse
                recycle_send_buffer(ud.buf_id);
            },
        }
    }
}
```

---

## 5. Buffer Management Strategy

```
RECEIVE SIDE: Ring-Provided Buffer Ring
═══════════════════════════════════════
  ┌─────────────┐     ┌──────────────────────┐
  │  App writes  │────▶│  Shared Buffer Ring   │
  │  buf_ring_add│     │  (mmap'd, lock-free)  │
  └─────────────┘     │                        │
                      │  tail ──▶ [buf0][buf1] │
                      │          [buf2][buf3].. │
                      │  ◀── head (kernel)      │
                      └──────────────────────────┘
                               │
                      Kernel picks buffers at
                      completion time, returns
                      buffer_id in CQE flags

  Lifecycle:
  1. init: populate all slots → buf_ring_advance
  2. recv CQE: get(cqe) → process → put(cqe) → auto buf_ring_add
  3. ENOBUFS: all buffers consumed → kick sends to drain → rearm recv

SEND SIDE: Two Options
═══════════════════════
  Option A — Fixed Registered Buffers + ZC Send:
    • Best for: request-response where you control the buffer
    • Pre-register N buffers → send_zc_fixed(buf_index)
    • Wait for NOTIF CQE before reusing
    • Double-buffer pattern: write to buf[i], send buf[i], write to buf[i+1]

  Option B — Send Buffer Ring + Bundles:
    • Best for: scatter-gather, proxying, forwarding received data
    • Received buffers go directly into send ring (zero copy between rx→tx!)
    • Single bundle SQE drains all queued buffers
    • Maintains ordering automatically

  PROXY PATTERN (minimum copies):
    recv_ring → [data arrives] → move buffer to send_ring → bundle send
    (The SAME memory is used for recv and send — true zero-copy proxy!)
```

---

## 6. Zig 0.16 `std.os.linux.IoUring` Compatibility

### Fully Supported (Use Directly)

| Feature | Zig API |
|---------|---------|
| Multishot accept (direct) | `ring.accept_multishot_direct(sqe, ...)` |
| Multishot recv | `BufferGroup.recv_multishot(user_data, fd, flags)` |
| Zero-copy send | `ring.send_zc(sqe, ...)` / `ring.send_zc_fixed(sqe, ...)` / `ring.sendmsg_zc(sqe, ...)` |
| Provided buffer rings | `BufferGroup.init(...)` / `.get(cqe)` / `.put(cqe)` |
| Fixed files | `ring.register_files_sparse(n)` / `ring.register_file_alloc_range(...)` |
| Direct descriptors | `accept_direct`, `socket_direct`, `close_direct` |
| SQPOLL | Pass `IORING_SETUP_SQPOLL` in setup flags |
| SINGLE_ISSUER | Pass `IORING_SETUP_SINGLE_ISSUER` in setup flags |
| COOP_TASKRUN | Pass `IORING_SETUP_COOP_TASKRUN` in setup flags |
| Socket creation | `ring.socket_direct_alloc(sqe, ...)` |
| Cancellation | `ring.cancel(sqe, ...)` |
| NAPI busy poll | `ring.register_napi(...)` / `ring.unregister_napi(...)` |
| Probing | `ring.get_probe()` — detect supported operations at runtime |

### Constants Exist — Manual SQE Manipulation Needed

These features have kernel constants defined in `std.os.linux` but no high-level wrapper method. Set SQE fields manually:

| Feature | How to Use |
|---------|-----------|
| **Send/Recv Bundles** | `sqe.ioprio \|= linux.IORING_RECVSEND_BUNDLE` |
| **`POLL_FIRST`** | `sqe.ioprio \|= linux.IORING_RECVSEND_POLL_FIRST` |
| **`msg_ring`** | `IORING_OP.MSG_RING` exists; prep SQE manually (set opcode, fd=target ring, off=user_data, len=data) |
| **`CQE_SKIP_SUCCESS`** | `sqe.flags \|= linux.IOSQE_CQE_SKIP_SUCCESS` |
| **`SEND_ZC_REPORT_USAGE`** | Available as a constant for ioprio field |

### Potential Issues & Workarounds

| Issue | Details | Workaround |
|-------|---------|-----------|
| **`DEFER_TASKRUN`** | The high-level `enter()` method may not pass `IORING_ENTER_EXT_ARG`. The constant IS defined. | Use `COOP_TASKRUN` instead (simpler, no special enter handling needed), OR call `io_uring_enter` syscall directly with `IORING_ENTER_EXT_ARG` + `io_uring_getevents_arg`. |
| **`FEAT_SEND_BUF_SELECT`** | Feature flag constant is **missing** from Zig's `linux.zig`. | Define it yourself: `const IORING_FEAT_SEND_BUF_SELECT = 1 << 14;` and check `params.features & IORING_FEAT_SEND_BUF_SELECT != 0`. |

### CQE Flags Reference

All of these are defined in Zig's `std.os.linux`:

| Flag | Meaning |
|------|---------|
| `IORING_CQE_F_MORE` | Multishot still armed; more CQEs will follow |
| `IORING_CQE_F_BUFFER` | CQE contains a buffer ID (from provided buffer ring) |
| `IORING_CQE_F_BUF_MORE` | More completions coming for the same buffer (incremental) |
| `IORING_CQE_F_SOCK_NONEMPTY` | Socket still has data after this recv |
| `IORING_CQE_F_NOTIF` | Zero-copy notification — safe to reuse buffer |

---

## 7. Recommended Development Plan

### Phase 1: Core Event Loop (Start Here)

1. `IoUring.init_params()` with `SQPOLL` + `SINGLE_ISSUER` + `COOP_TASKRUN`
2. `register_files_sparse()` for direct descriptors
3. Multishot `accept_multishot_direct` on listen socket
4. `BufferGroup` for recv buffers
5. `BufferGroup.recv_multishot()` per accepted connection
6. Regular `send()` for responses (non-ZC, simplest starting path)
7. Event loop with `submit_and_wait` + `copy_cqes` batch processing

### Phase 2: Zero-Copy Send

1. `register_buffers()` for send buffer pool
2. Switch to `send_zc_fixed()` for responses
3. Track NOTIF CQEs for buffer lifecycle
4. Double-buffer scheme for continuous sending without stalls

### Phase 3: Bundles + Advanced Features

1. Add `IORING_RECVSEND_BUNDLE` to recv for bulk data
2. Set up send buffer ring for bundle sends
3. Add `IORING_RECVSEND_POLL_FIRST` for idle connections
4. Add `IOSQE_CQE_SKIP_SUCCESS` for linked operations
5. Try `DEFER_TASKRUN` (may need `enter()` patching)
6. NAPI busy polling if NIC supports it

---

## 8. Performance Tuning Knobs

| Parameter | Recommended | Why |
|-----------|-------------|-----|
| SQ ring size | 256–512 | Only needs to fit one batch of submissions |
| CQ ring size | 4096+ | **Critical** — must never overflow; 4× SQ size minimum |
| Buffer ring entries | 256–1024 (power of 2) | More = less ENOBUFS; fewer = less memory |
| Buffer size | MTU-aligned (1500 or 4096) | Match your message size; 4096 for general purpose |
| `sq_thread_idle` | 1000ms (SQPOLL) | How long kernel thread spins before sleeping |
| Wait batch | 8–32 | Higher = better throughput, worse tail latency |
| CPU pinning | SQPOLL thread + app thread on same NUMA node | Avoids cross-NUMA cache misses |
| Huge pages | 2MB for buffer pools | Reduces TLB misses on large buffer pools |

---

## 9. Critical Gotchas

1. **Always check `IORING_CQE_F_MORE`** on every multishot CQE. If absent, the multishot has terminated and must be resubmitted.

2. **Zero-copy send produces TWO CQEs** — the send result and a notification. Don't reuse the buffer until you see `IORING_CQE_F_NOTIF`.

3. **Never overflow the CQ ring** — if it overflows, completions are dropped and you lose track of operations. Oversize it generously (`params.cq_entries = 4096`).

4. **Buffer ring entries must be power-of-2**, page-aligned, max 32768.

5. **`ENOBUFS` is your backpressure signal** — when the buffer ring is exhausted, recv returns `-ENOBUFS`. Handle it by draining sends first, then rearming recv.

6. **Bundle recv CQE `res` = total bytes across ALL buffers consumed**. The buffer ID in the CQE flags is the *first* buffer; walk contiguously from there.

7. **For small messages (< ~1KB), regular `send()` may beat `send_zc()`** — zero-copy has overhead from page pinning and DMA setup that only pays off for larger payloads.

8. **Direct descriptors don't support `SOCK_CLOEXEC`** — returns `-EINVAL`.

9. **Shutdown sequence should use linked SQEs:** `shutdown(SHUT_RDWR)` → `close_direct`, with `IOSQE_CQE_SKIP_SUCCESS` on the shutdown to reduce CQ traffic.

10. **Batch CQ processing:** Use `ring.copy_cqes(&buf, 0)` to drain all available completions at once. The CQ head is advanced atomically in a single batch — much faster than per-CQE `cqe_seen()`.

---

## 10. Steady-State Performance Profile

In steady state with all optimizations enabled:

| Aspect | Status |
|--------|--------|
| **Syscalls for submission** | 0 (SQPOLL) |
| **Syscalls for completion** | 0 (SQPOLL + COOP_TASKRUN) or 1 batched (DEFER_TASKRUN) |
| **Syscalls for buffer management** | 0 (ring-provided buffers) |
| **SQEs per recv** | 0 (multishot — submitted once, produces N CQEs) |
| **SQEs per accept** | 0 (multishot — submitted once) |
| **Memory copies on send** | 0 (zero-copy send with fixed buffers) |
| **fd refcount atomics** | 0 (direct descriptors) |
| **Page pinning per I/O** | 0 (registered/fixed buffers) |

The only remaining overhead is the kernel's TCP/IP stack processing and your application's message handling logic.

---

## References

- [io_uring design document (Jens Axboe)](https://kernel.dk/io_uring.pdf)
- [liburing proxy.c reference implementation](https://github.com/axboe/liburing/blob/master/examples/proxy.c)
- [io_uring man pages (man7.org)](https://man7.org/linux/man-pages/man7/io_uring.7.html)
- [Zig std.os.linux.IoUring source](https://github.com/ziglang/zig/blob/master/lib/std/os/linux/IoUring.zig)
- [Zig std.os.linux io_uring constants](https://github.com/ziglang/zig/blob/master/lib/std/os/linux.zig)
