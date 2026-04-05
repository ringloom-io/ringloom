# Aeron Architecture Reference

## For Reimplementation in Other Languages

This document provides a complete architectural reference of the Aeron media driver and client, derived from the C source code. It contains sufficient detail to reimplement the entire system in another language (e.g., Zig, Rust, Go).

---

## Table of Contents

1. [Overview](#1-overview)
2. [Shared Memory IPC — The CnC File](#2-shared-memory-ipc--the-cnc-file)
3. [Concurrent Data Structures](#3-concurrent-data-structures)
4. [Command/Control Protocol](#4-commandcontrol-protocol)
5. [UDP Wire Protocol](#5-udp-wire-protocol)
6. [Log Buffer Architecture](#6-log-buffer-architecture)
7. [Client Architecture](#7-client-architecture)
8. [Media Driver Architecture](#8-media-driver-architecture)
9. [Network Layer](#9-network-layer)
10. [Flow Control](#10-flow-control)
11. [Congestion Control](#11-congestion-control)
12. [Loss Detection and Retransmission](#12-loss-detection-and-retransmission)
13. [URI Format and Parsing](#13-uri-format-and-parsing)
14. [Counters and Monitoring](#14-counters-and-monitoring)
15. [Utility Data Structures](#15-utility-data-structures)
16. [Constants Reference](#16-constants-reference)

---

## 1. Overview

Aeron is a high-performance, low-latency messaging system composed of two processes:

- **Media Driver** — a standalone process (or embedded library) that manages all I/O, flow control, retransmission, and shared memory resources.
- **Client** — a library linked into user applications that communicates with the media driver exclusively through shared memory.

### Key Design Principles

1. **Zero-copy shared memory**: All data transfer between client and driver uses memory-mapped files. No system calls are needed for the data path.
2. **Lock-free algorithms**: All shared data structures use CAS (compare-and-swap) or atomic fetch-and-add operations. No mutexes are used on the critical path.
3. **Single-writer principle**: Each data structure has exactly one writer. Readers never block writers.
4. **Position-based flow control**: Progress is tracked via monotonically increasing 64-bit positions rather than sequence numbers.
5. **Duty-cycle agents**: All processing is done in tight loops that return work counts, driving idle strategies.

### Component Relationships

```
┌──────────────────────────────────────────────────────────────┐
│                      User Application                        │
│                                                              │
│  ┌─────────────────────────────────────────────────────────┐ │
│  │                    Aeron Client Library                  │ │
│  │                                                         │ │
│  │  ┌──────────────┐  ┌──────────────┐  ┌──────────────┐  │ │
│  │  │ Publication   │  │ Subscription │  │  Counter      │  │ │
│  │  │ (write to     │  │ (read from   │  │  (read/write  │  │ │
│  │  │  log buffer)  │  │  log buffer) │  │   counters)   │  │ │
│  │  └──────┬───────┘  └──────┬───────┘  └──────┬───────┘  │ │
│  │         │                 │                  │          │ │
│  │  ┌──────┴─────────────────┴──────────────────┴───────┐  │ │
│  │  │              Client Conductor                      │  │ │
│  │  │  (processes commands, receives responses,          │  │ │
│  │  │   manages resources, heartbeats)                   │  │ │
│  │  └──────────────────────┬────────────────────────────┘  │ │
│  └─────────────────────────┼───────────────────────────────┘ │
└────────────────────────────┼─────────────────────────────────┘
                             │
              ┌──────────────┴──────────────┐
              │    CnC File (cnc.dat)        │
              │    Memory-Mapped Shared      │
              │    ┌─────────────────────┐   │
              │    │ To-Driver Ring Buf   │   │  ← client writes commands
              │    │ To-Clients Broadcast │   │  ← driver writes responses
              │    │ Counters (meta+val)  │   │  ← shared position tracking
              │    │ Error Log            │   │  ← driver writes errors
              │    └─────────────────────┘   │
              └──────────────┬──────────────┘
                             │
┌────────────────────────────┼─────────────────────────────────┐
│                            │         Media Driver            │
│  ┌─────────────────────────┴────────────────────────────┐    │
│  │                   Conductor Agent                     │    │
│  │  (reads client commands, manages publications/        │    │
│  │   subscriptions, updates counters, heartbeats)        │    │
│  └──────┬──────────────────────────────────┬────────┘    │
│         │ proxy (ring buffer)              │ proxy        │
│  ┌──────┴───────┐                  ┌───────┴──────┐      │
│  │ Sender Agent  │                  │Receiver Agent│      │
│  │ (sends data   │                  │(receives UDP │      │
│  │  via UDP,     │                  │ data, writes │      │
│  │  retransmits) │                  │ to log bufs) │      │
│  └──────┬───────┘                  └───────┬──────┘      │
│         │                                  │             │
│         └────────────────┬─────────────────┘             │
│                          │                               │
│                     UDP Sockets                          │
└──────────────────────────┼───────────────────────────────┘
                           │
                      ┌────┴────┐
                      │ Network │
                      └─────────┘
```

In addition to the CnC file, **log buffer files** are created per publication (in `<aeron_dir>/publications/` and `<aeron_dir>/images/`) and memory-mapped by both the client and the driver. These contain the actual message data.

---

## 2. Shared Memory IPC — The CnC File

The CnC (Command and Control) file `cnc.dat` is the central shared memory region between the media driver and all clients. It is located at `<aeron_dir>/cnc.dat`.

### 2.1 CnC File Layout

```
Offset 0:                    CnC Metadata (128 bytes, 2 cache lines)
Offset 128:                  To-Driver Buffer (MPSC ring buffer)
Offset 128+A:                To-Clients Buffer (broadcast buffer)
Offset 128+A+B:              Counter Metadata Buffer
Offset 128+A+B+C:            Counter Values Buffer
Offset 128+A+B+C+D:          Error Log Buffer

Where:
  A = to_driver_buffer_length
  B = to_clients_buffer_length
  C = counter_metadata_buffer_length
  D = counter_values_buffer_length

Total file size = ALIGN(128 + A + B + C + D + error_log_length, page_size)
```

### 2.2 CnC Metadata Structure

The metadata occupies the first 128 bytes (2 cache lines). Fields are packed at 4-byte alignment:

```
Offset  Size  Type          Field
─────────────────────────────────────────────────
 0      4     volatile i32  cnc_version              (written LAST with release semantics)
 4      4     i32           to_driver_buffer_length
 8      4     i32           to_clients_buffer_length
12      4     i32           counter_metadata_buffer_length
16      4     i32           counter_values_buffer_length
20      4     i32           error_log_buffer_length
24      8     i64           client_liveness_timeout   (nanoseconds)
32      8     i64           start_timestamp           (epoch milliseconds)
40      8     i64           pid                       (driver process ID)
48      4     i32           file_page_size
52     76     padding       (to 128 bytes)
```

### 2.3 Version Encoding

Versions are encoded as: `(major << 16) | (minor << 8) | patch`

- **CnC version**: `0.2.0` = `0x00000200` = `512`
- **Control protocol version**: `1.0.0` = `0x00010000` = `65536`

Version compatibility check: `(client_version >> 16) == (driver_version >> 16)` (major must match).

### 2.4 Connection Handshake

When a client connects:

1. Resolve `<aeron_dir>/cnc.dat` path
2. Memory-map the file (retry loop with `driver_timeout_ms` deadline)
3. Read `cnc_version` with acquire semantics — wait until non-zero and version-compatible
4. Validate buffer lengths
5. Initialize the to-driver MPSC ring buffer from the mapped region
6. Read `consumer_heartbeat` from the ring buffer trailer — wait until non-zero
7. Verify heartbeat is fresh: `now_ms - heartbeat_ms < driver_timeout_ms`
8. Obtain `client_id` by atomically incrementing the ring buffer's `correlation_counter`

The driver signals CnC readiness by writing `cnc_version` **last** with a release store and `msync`.

---

## 3. Concurrent Data Structures

### 3.1 Foundational Primitives

#### Cache Line and Alignment

```
CACHE_LINE_LENGTH = 64 bytes
```

All shared memory structures pad between concurrently-accessed fields to **2 × cache lines = 128 bytes** to prevent false sharing.

```
ALIGN(value, alignment) = (value + (alignment - 1)) & ~(alignment - 1)
```

Capacities must always be a **power of two**, validated by:
```
IS_POWER_OF_TWO(value) = (value > 0) && ((value & (~value + 1)) == value)
```

#### Atomic Operations

| Operation | Semantics | x86-64 | ARM |
|-----------|-----------|--------|-----|
| `GET_ACQUIRE(dst, src)` | Load + acquire fence | Plain load + compiler fence | `ldapr` / C11 `memory_order_acquire` |
| `SET_RELEASE(dst, src)` | Release fence + store | Compiler fence + plain store | `stlr` / C11 `memory_order_release` |
| `GET_AND_ADD_INT64` | Atomic fetch-and-add | `lock; xaddq` | C11 `fetch_add` |
| `GET_AND_ADD_INT32` | Atomic fetch-and-add | `lock; xaddl` | C11 `fetch_add` |
| `CAS_INT64(ptr, expect, update)` | Compare-and-swap | `lock; cmpxchgq` | C11 `compare_exchange_weak` |
| `CAS_INT32(ptr, expect, update)` | Compare-and-swap | `lock; cmpxchgl` | C11 `compare_exchange_weak` |

On x86-64, loads are inherently acquire and stores are inherently release, so only compiler fences are needed (not hardware fences). On ARM, full acquire/release instructions are required.

---

### 3.2 MPSC Ring Buffer

The Multi-Producer, Single-Consumer ring buffer is used for client→driver commands and inter-agent communication within the driver.

#### Memory Layout

```
┌─────────────────────────────┐  ← offset 0
│                             │
│    Data Buffer              │  capacity bytes (power of 2)
│    (messages go here)       │
│                             │
├─────────────────────────────┤  ← offset = capacity
│    Trailer (768 bytes)      │
│                             │
│  +0:   begin_pad[128]       │  128 bytes padding
│  +128: tail_position (i64)  │  ← producers write here (CAS)
│        tail_pad[120]        │
│  +256: head_cache (i64)     │  ← producer-cached head (reduces contention)
│        head_cache_pad[120]  │
│  +384: head_position (i64)  │  ← consumer writes here
│        head_pad[120]        │
│  +512: correlation_ctr (i64)│  ← atomic counter for correlation IDs
│        corr_pad[120]        │
│  +640: consumer_hb (i64)    │  ← consumer heartbeat timestamp
│        hb_pad[120]          │
└─────────────────────────────┘
```

**Total trailer size**: 768 bytes = 6 × 128 bytes.

**Total buffer allocation**: `capacity + 768` bytes.

#### Record Header

Every message is preceded by an 8-byte record header:

```
Offset  Size  Type         Field
────────────────────────────────────
0       4     volatile i32 length      (negative = uncommitted, positive = committed)
4       4     i32          msg_type_id (≥1 for valid messages, -1 = padding)
8       ...   bytes        payload
```

Records are aligned to **8 bytes** (`AERON_RB_ALIGNMENT = 2 * sizeof(int32_t)`).

**Max message length**: `capacity / 8`.

#### Write Algorithm (Multi-Producer)

```
function mpsc_rb_write(ring_buffer, msg_type_id, buffer, length):
    assert msg_type_id >= 1
    assert length <= max_message_length

    record_length = length + RECORD_HEADER_LENGTH  // +8
    aligned_record_length = ALIGN(record_length, 8)
    required = aligned_record_length

    // === Claim space via CAS on tail ===
    loop:
        head = GET_ACQUIRE(head_cache_position)      // try cached head first
        tail = GET_ACQUIRE(tail_position)
        available = capacity - (tail - head)

        // Wrap-around check
        tail_index = tail & (capacity - 1)
        to_buffer_end = capacity - tail_index
        if required > to_buffer_end:
            // Need to wrap: must pad to end, start at index 0
            if head <= tail:
                head = GET_ACQUIRE(head_position)    // re-read actual head
                SET_RELEASE(head_cache_position, head)
            if required > (capacity - (tail - head)):
                return FULL                          // not enough space
            padding = to_buffer_end
            required = aligned_record_length + padding
        else:
            padding = 0

        if available < required:
            head = GET_ACQUIRE(head_position)        // re-read actual head
            SET_RELEASE(head_cache_position, head)
            if capacity - (tail - head) < required:
                return FULL
        
        if CAS_INT64(&tail_position, tail, tail + required):
            break  // claimed!

    // === Write padding record if wrapping ===
    if padding > 0:
        pad_index = tail & (capacity - 1)
        pad_header = &buffer[pad_index]
        pad_header.msg_type_id = PADDING_MSG_TYPE_ID  // -1
        SET_RELEASE(pad_header.length, padding)       // positive = committed padding

    // === Write message record ===
    record_index = (tail + padding) & (capacity - 1)
    header = &buffer[record_index]

    SET_RELEASE(header.length, -record_length)        // NEGATIVE = uncommitted sentinel
    memcpy(&buffer[record_index + 8], message_data, length)
    header.msg_type_id = msg_type_id
    SET_RELEASE(header.length, record_length)          // POSITIVE = committed (publication fence)
```

#### Read Algorithm (Single Consumer)

```
function mpsc_rb_read(ring_buffer, handler, limit):
    head = head_position                               // plain read (single consumer)
    head_index = head & (capacity - 1)
    messages_read = 0
    bytes_consumed = 0

    while bytes_consumed < capacity and messages_read < limit:
        record_index = head_index + bytes_consumed
        record = &buffer[record_index]

        length = GET_ACQUIRE(record.length)
        if length <= 0:
            break                                      // uncommitted or empty

        aligned_length = ALIGN(length, 8)
        bytes_consumed += aligned_length

        if record.msg_type_id != PADDING_MSG_TYPE_ID:  // skip padding
            handler(record.msg_type_id, &buffer[record_index + 8], length - 8)
            messages_read += 1

    // Zero consumed region and advance head
    if bytes_consumed > 0:
        memset(&buffer[head_index], 0, bytes_consumed)
        SET_RELEASE(head_position, head + bytes_consumed)

    return messages_read
```

#### Controlled Read

Like read, but the handler returns an action:
- **ABORT**: Rewind to before this message, stop. Message is re-delivered on next read.
- **BREAK**: Include this message in consumed count, then stop.
- **COMMIT**: Advance head immediately (mid-batch), continue reading.
- **CONTINUE**: Keep reading.

#### Unblock (Dead Producer Recovery)

If a producer dies mid-write, the consumer may find:
- **Negative length**: Convert to padding (set `msg_type_id = -1`, flip length positive).
- **Zero length at head but non-zero further ahead**: Insert padding spanning the gap.

---

### 3.3 SPSC Ring Buffer

The Single-Producer, Single-Consumer ring buffer is similar to MPSC but optimized:

- **No CAS on tail**: Producer uses a plain release-store to advance `tail_position`.
- **Pre-zeros next header**: After claiming space, writes `{length=0, msg_type_id=0}` at the next slot, enabling the consumer to detect end-of-data without zeroing consumed records.
- **Consumer does NOT zero consumed region** (unlike MPSC).
- **Claim needs extra space**: `aligned_record_length + RECORD_HEADER_LENGTH` (for pre-zeroing).
- **Supports scatter/gather writes** (`writev` with `struct iovec`).

---

### 3.4 Broadcast Buffer (One-to-Many)

The broadcast buffer is used for driver→clients responses. Unlike ring buffers, it **overwrites old data** — slow receivers are "lapped" and must catch up.

#### Trailer (128 bytes)

```
Offset  Size  Type         Field
────────────────────────────────────
0       8     volatile i64 tail_intent_counter   (set BEFORE writing — invalidation signal)
8       8     volatile i64 tail_counter          (set AFTER writing — data ready)
16      8     volatile i64 latest_counter        (position of most recent complete record)
24      104   bytes        padding to 128 bytes
```

**Record header**: Same as ring buffer (8 bytes: `length` + `msg_type_id`).

**Record alignment**: 8 bytes. **Max message length**: `capacity / 8`.

#### Transmitter (Single Writer — Driver Conductor)

```
function broadcast_transmit(transmitter, msg_type_id, buffer, length):
    tail = tail_counter                                // plain read (single writer)
    record_offset = tail & (capacity - 1)
    aligned_record_length = ALIGN(length + 8, 8)

    // Handle wrap-around
    to_end = capacity - record_offset
    if to_end < aligned_record_length:
        SET_RELEASE(tail_intent_counter, tail + aligned_record_length + to_end)
        // Write padding at current position
        write_padding(record_offset, to_end)
        tail += to_end
        record_offset = 0
    else:
        SET_RELEASE(tail_intent_counter, tail + aligned_record_length)

    // Write record
    write_header_and_payload(record_offset, msg_type_id, buffer, length)
    SET_RELEASE(latest_counter, tail)
    SET_RELEASE(tail_counter, tail + aligned_record_length)
```

#### Receiver Protocol

Each receiver keeps local state:
- `cursor`: current read position
- `next_record`: position of next record
- `lapped_count`: how many times this receiver was lapped
- `scratch_buffer`: copy buffer (default 4096 bytes) for safe reads

```
function broadcast_receive(receiver, handler):
    1. Read tail_counter (acquire)
    2. If tail > cursor (new data):
       a. Compute record_offset = cursor & (capacity - 1)
       b. Validate: cursor + capacity > tail_intent_counter (acquire)
          If NOT valid → receiver was lapped:
            - Increment lapped_count
            - Jump cursor to latest_counter (acquire)
       c. Read record header
       d. If padding: skip to start of buffer, re-read
       e. memcpy payload into scratch_buffer
       f. Validate again (check data wasn't overwritten during copy)
       g. Call handler with scratch_buffer
       h. Advance cursor to next_record
```

The three counters serve different roles:
- `tail_intent_counter` — set BEFORE write, tells receivers "I'm about to overwrite here"
- `tail_counter` — set AFTER write, tells receivers "new data is ready"
- `latest_counter` — points to most recent complete record (jump-to point for lapped receivers)

---

### 3.5 Counters Manager

The counters system provides shared atomic int64 values accessible by both client and driver. It uses two separate buffers within the CnC file.

#### Counter Value Descriptor (128 bytes each)

```
Offset  Size  Type         Field
────────────────────────────────────
0       8     volatile i64 counter_value
8       8     volatile i64 registration_id
16      8     i64          owner_id
24      8     i64          reference_id
32      96    bytes        padding (to 128 bytes = 2 cache lines)
```

#### Counter Metadata Descriptor (512 bytes each)

```
Offset  Size  Type         Field
────────────────────────────────────
0       4     volatile i32 state            (0=UNUSED, 1=ALLOCATED, -1=RECLAIMED)
4       4     i32          type_id
8       8     volatile i64 free_for_reuse_deadline_ms
16      112   bytes        key              (max 112 bytes of type-specific key data)
128     4     volatile i32 label_length
132     380   bytes        label            (human-readable label, max 380 bytes)
```

**Size relationship**: metadata buffer must be ≥ `values_buffer_length × 4` (since 512/128 = 4:1 ratio).

**Max counter ID**: `(values_buffer_length / 128) - 1`.

#### Counter Access

```
value_address  = values_buffer  + counter_id × 128
meta_address   = metadata_buffer + counter_id × 512
```

#### Allocation Algorithm

1. Check free list — iterate freed counter IDs, check if `now_ms ≥ free_for_reuse_deadline_ms` (acquire read). If eligible, reuse.
2. If no free IDs: increment `id_high_water_mark` (if not exceeding max).
3. Write metadata: `type_id`, `key`, `label`, `free_for_reuse_deadline_ms = INT64_MAX`.
4. Write `state = ALLOCATED` with **release store** (last write — makes all metadata visible to readers).

#### Deallocation Algorithm

1. Set `state = RECLAIMED` (release).
2. Clear key bytes.
3. Set `free_for_reuse_deadline_ms = now + free_to_reuse_timeout_ms`.
4. Add counter ID to free list.

#### Stream Position Counter Key Layout

For counters tracking publication/subscription positions:

```
Offset  Size  Type    Field
────────────────────────────
0       8     i64     registration_id
8       4     i32     session_id
12      4     i32     stream_id
16      4     i32     channel_length
20      var   char[]  channel (remaining key bytes)
```

---

### 3.6 Distinct Error Log

A flat buffer for recording unique error observations.

#### Error Log Entry

```
Offset  Size  Type         Field
────────────────────────────────────
0       4     volatile i32 length                    (0 = empty, >0 = entry length including header)
4       4     volatile i32 observation_count         (how many times this error occurred)
8       8     volatile i64 last_observation_timestamp (epoch ms)
16      8     i64          first_observation_timestamp (epoch ms)
24      var   bytes        description               (error string, NOT null-terminated)
```

**Header size**: 24 bytes. **Entry alignment**: 8 bytes.

**Recording**: New errors append at `next_offset`. Repeated errors atomically increment `observation_count` and update `last_observation_timestamp`. The `length` field is written **last** with release store.

**Reading**: Walk from offset 0, read `length` with acquire. If 0, stop. Advance by `ALIGN(length, 8)`.

---

### 3.7 Concurrent Array Queues

Lock-free pointer queues used for client→conductor command submission.

#### MPSC Concurrent Array Queue

```
Structure:
    padding[64]
    producer.tail         (volatile u64)     — CAS by multiple producers
    producer.head_cache   (u64)              — producer-local cache
    producer.shared_head_cache (volatile u64) — shared among producers
    padding[64]
    consumer.head         (volatile u64)     — single consumer
    padding[64]
    capacity              (power of 2)
    mask                  (capacity - 1)
    buffer                (void* volatile [capacity])
```

**Offer**: CAS loop on `producer.tail`. Uses `shared_head_cache` to reduce contention on consumer's head.

**Drain**: Sequential read, set each slot to NULL, advance `consumer.head`.

#### SPSC Concurrent Array Queue

Same structure but no CAS — single producer uses release-store on tail.

---

## 4. Command/Control Protocol

All commands and responses are transmitted through the CnC file's ring buffer (client→driver) and broadcast buffer (driver→clients).

### 4.1 Client → Driver Commands

Every command begins with a correlated header:

```
Offset  Size  Type  Field
──────────────────────────
0       8     i64   client_id
8       8     i64   correlation_id
```

The `client_id` is obtained during connection. The `correlation_id` is generated by atomically incrementing the ring buffer's `correlation_counter`.

#### Command Type IDs

| ID | Name | Payload After Correlated Header |
|----|------|---------------------------------|
| `0x01` | ADD_PUBLICATION | `i32 stream_id`, `i32 channel_length`, `char[] channel` |
| `0x02` | REMOVE_PUBLICATION | `i64 registration_id`, `u64 flags` |
| `0x03` | ADD_EXCLUSIVE_PUBLICATION | Same as ADD_PUBLICATION |
| `0x04` | ADD_SUBSCRIPTION | `i64 registration_correlation_id`, `i32 stream_id`, `i32 channel_length`, `char[] channel` |
| `0x05` | REMOVE_SUBSCRIPTION | `i64 registration_id` |
| `0x06` | CLIENT_KEEPALIVE | (no extra fields — only `client_id` matters) |
| `0x07` | ADD_DESTINATION | `i64 registration_id`, `i32 channel_length`, `char[] channel` |
| `0x08` | REMOVE_DESTINATION | Same as ADD_DESTINATION |
| `0x09` | ADD_COUNTER | `i32 type_id`, then key_length + key + label_length + label |
| `0x0A` | REMOVE_COUNTER | `i64 registration_id` |
| `0x0B` | CLIENT_CLOSE | (no extra fields) |
| `0x0C` | ADD_RCV_DESTINATION | Same as ADD_DESTINATION |
| `0x0D` | REMOVE_RCV_DESTINATION | Same as ADD_DESTINATION |
| `0x0E` | TERMINATE_DRIVER | `i32 token_length`, `bytes[] token` |
| `0x0F` | ADD_STATIC_COUNTER | `i64 registration_id`, `i32 type_id` |
| `0x10` | REJECT_IMAGE | `i64 image_correlation_id`, `i64 position`, `i32 reason_length`, `bytes[] reason` |
| `0x11` | REMOVE_DESTINATION_BY_ID | `i64 resource_registration_id`, `i64 destination_registration_id` |
| `0x12` | GET_NEXT_AVAILABLE_SESSION_ID | `i32 stream_id` |

**REMOVE_PUBLICATION flags**: `REVOKE = 0x1`.

### 4.2 Driver → Client Responses

Response type IDs are in the `0x0Fxx` range:

| ID | Name | Layout |
|----|------|--------|
| `0x0F01` | ON_ERROR | `i64 offending_correlation_id`, `i32 error_code`, `i32 error_msg_length`, `char[] msg` |
| `0x0F02` | ON_AVAILABLE_IMAGE | `i64 correlation_id`, `i32 session_id`, `i32 stream_id`, `i64 subscriber_registration_id`, `i32 subscriber_position_id`, then `i32 log_file_length`, `char[] log_file` (aligned to 4), `i32 source_identity_length`, `char[] source_identity` |
| `0x0F03` | ON_PUBLICATION_READY | `i64 correlation_id`, `i64 registration_id`, `i32 session_id`, `i32 stream_id`, `i32 position_limit_counter_id`, `i32 channel_status_indicator_id`, `i32 log_file_length`, `char[] log_file` |
| `0x0F04` | ON_OPERATION_SUCCESS | `i64 correlation_id` |
| `0x0F05` | ON_UNAVAILABLE_IMAGE | `i64 correlation_id`, `i64 subscription_registration_id`, `i32 stream_id`, `i32 channel_length`, `char[] channel` |
| `0x0F06` | ON_EXCLUSIVE_PUBLICATION_READY | Same as ON_PUBLICATION_READY |
| `0x0F07` | ON_SUBSCRIPTION_READY | `i64 correlation_id`, `i32 channel_status_indicator_id` |
| `0x0F08` | ON_COUNTER_READY | `i64 correlation_id`, `i32 counter_id` |
| `0x0F09` | ON_UNAVAILABLE_COUNTER | `i64 correlation_id`, `i32 counter_id` |
| `0x0F0A` | ON_CLIENT_TIMEOUT | `i64 client_id` |
| `0x0F0B` | ON_STATIC_COUNTER | `i64 correlation_id`, `i32 counter_id` |
| `0x0F0C` | ON_PUBLICATION_ERROR | `i64 registration_id`, `i64 dest_registration_id`, `i32 session_id`, `i32 stream_id`, `i64 receiver_id`, `i64 group_tag`, `i16 address_type`, `u16 source_port`, `u8[16] source_address`, `i32 error_code`, `i32 error_msg_length`, `char[] error_msg` |
| `0x0F0D` | ON_NEXT_AVAILABLE_SESSION_ID | `i64 correlation_id`, `i32 next_session_id` |

### 4.3 Error Codes

| Code | Name |
|------|------|
| 0 | UNUSED |
| 1 | INVALID_CHANNEL |
| 2 | UNKNOWN_SUBSCRIPTION |
| 3 | UNKNOWN_PUBLICATION |
| 4 | CHANNEL_ENDPOINT_ERROR |
| 5 | UNKNOWN_COUNTER |
| 6 | UNKNOWN_COMMAND_TYPE_ID |
| 7 | MALFORMED_COMMAND |
| 8 | NOT_SUPPORTED |
| 9 | UNKNOWN_HOST |
| 10 | RESOURCE_TEMPORARILY_UNAVAILABLE |
| 11 | GENERIC_ERROR |
| 12 | STORAGE_SPACE |
| 13 | IMAGE_REJECTED |
| 14 | PUBLICATION_REVOKED |

### 4.4 Typical Command Flows

**Add Publication:**
```
Client:  ADD_PUBLICATION {client_id, correlation_id, stream_id, channel}
         ──────────────────── to-driver ring buffer ────────────────────►
Driver:  ON_PUBLICATION_READY {correlation_id, registration_id, session_id,
         stream_id, position_limit_counter_id, channel_status_id, log_file}
         ◄──────────────── to-clients broadcast buffer ─────────────────
Client:  memory-maps log_file, creates Publication object
```

**Add Subscription:**
```
Client:  ADD_SUBSCRIPTION {client_id, correlation_id, stream_id, channel}
         ────────────────────────────────────────────────────────────────►
Driver:  ON_SUBSCRIPTION_READY {correlation_id, channel_status_id}
         ◄───────────────────────────────────────────────────────────────

  ... later, when a matching publisher connects ...

Driver:  ON_AVAILABLE_IMAGE {correlation_id, session_id, stream_id,
         subscriber_registration_id, subscriber_position_id, log_file, source_identity}
         ◄───────────────────────────────────────────────────────────────
Client:  memory-maps log_file, creates Image object for polling
```

**Client Heartbeat (periodic, every 500ms default):**
```
Client:  CLIENT_KEEPALIVE {client_id, 0}
```

---

## 5. UDP Wire Protocol

All on-wire frames use little-endian byte order and 4-byte packed alignment unless noted.

### 5.1 Frame Header (8 bytes, common prefix)

```
Offset  Size  Type         Field
────────────────────────────────────
0       4     volatile i32 frame_length
4       1     i8           version        (always 0)
5       1     u8           flags
6       2     i16          type
```

### 5.2 Frame Types

| Value | Name | Description |
|-------|------|-------------|
| `0x00` | PAD | Padding frame |
| `0x01` | DATA | Data frame |
| `0x02` | NAK | Negative acknowledgement |
| `0x03` | SM | Status message |
| `0x04` | ERR | Error frame |
| `0x05` | SETUP | Setup frame |
| `0x06` | RTTM | Round-trip time measurement |
| `0x07` | RES | Resolution frame |
| `0x08` | ATS_DATA | ATS data frame |
| `0x09` | ATS_SETUP | ATS setup frame |
| `0x0A` | ATS_SM | ATS status message |
| `0x0B` | RSP_SETUP | Response setup |

### 5.3 Data Header (32 bytes)

Used for both DATA and PAD frame types:

```
Offset  Size  Type         Field
────────────────────────────────────
0       4     volatile i32 frame_length
4       1     i8           version (0)
5       1     u8           flags
6       2     i16          type (DATA=0x01 or PAD=0x00)
8       4     i32          term_offset
12      4     i32          session_id
16      4     i32          stream_id
20      4     i32          term_id
24      8     i64          reserved_value
```

**Data header flags:**

| Flag | Bit | Meaning |
|------|-----|---------|
| BEGIN | `0x80` | First fragment of message |
| END | `0x40` | Last fragment of message |
| EOS | `0x20` | End of stream |
| REVOKED | `0x10` | Publication revoked |

**UNFRAGMENTED** = `BEGIN | END` = `0xC0`

### 5.4 Setup Header (40 bytes)

Sent by a publisher to establish a stream:

```
Offset  Size  Type  Field
────────────────────────────
0       8     ...   frame_header
8       4     i32   term_offset
12      4     i32   session_id
16      4     i32   stream_id
20      4     i32   initial_term_id
24      4     i32   active_term_id
28      4     i32   term_length
32      4     i32   mtu
36      4     i32   ttl
```

**Setup flags**: `SEND_RESPONSE_FLAG = 0x80`, `GROUP_FLAG = 0x40`.

### 5.5 Status Message Header (36 bytes + optional 8)

Sent by receivers back to publishers for flow control:

```
Offset  Size  Type  Field
────────────────────────────
0       8     ...   frame_header
8       4     i32   session_id
12      4     i32   stream_id
16      4     i32   consumption_term_id
20      4     i32   consumption_term_offset
24      4     i32   receiver_window
28      8     i64   receiver_id

// Optional extension (frame_length == 44):
36      8     i64   group_tag
```

**SM flags**: `SEND_SETUP_FLAG = 0x80`, `EOS_FLAG = 0x40`.

### 5.6 NAK Header (28 bytes)

Sent by receivers requesting retransmission:

```
Offset  Size  Type  Field
────────────────────────────
0       8     ...   frame_header
8       4     i32   session_id
12      4     i32   stream_id
16      4     i32   term_id
20      4     i32   term_offset
24      4     i32   length
```

### 5.7 RTT Measurement Header (40 bytes)

```
Offset  Size  Type  Field
────────────────────────────
0       8     ...   frame_header
8       4     i32   session_id
12      4     i32   stream_id
16      8     i64   echo_timestamp
24      8     i64   reception_delta
32      8     i64   receiver_id
```

**RTTM flag**: `REPLY_FLAG = 0x80`.

### 5.8 Error Frame (40 bytes + message)

```
Offset  Size  Type  Field
────────────────────────────
0       8     ...   frame_header
8       4     i32   session_id
12      4     i32   stream_id
16      8     i64   receiver_id
24      8     i64   group_tag
32      4     i32   error_code
36      4     i32   error_length
40      var   char  error_text (max 1023 bytes)
```

### 5.9 Resolution Header (for name resolver)

Uses `#pragma pack(1)`:

```
Base (8 bytes):
  0: i8   res_type       (0x01=NAME_TO_IP4, 0x02=NAME_TO_IP6)
  1: u8   res_flags      (SELF_FLAG=0x80)
  2: u16  udp_port
  4: i32  age_in_ms

IPv4 (14 bytes): base(8) + u8[4] addr + i16 name_length, then name bytes
IPv6 (26 bytes): base(8) + u8[16] addr + i16 name_length, then name bytes
```

Entries are aligned to 8 bytes.

### 5.10 Heartbeats

When no data is available, the sender sends **zero-length DATA frames** every 100ms to maintain the connection. These carry the current `term_offset` so the receiver knows the sender's position. If end-of-stream, the EOS flag is set on heartbeats.

---

## 6. Log Buffer Architecture

Log buffers are the shared memory regions used for actual message data transfer. Each publication and each image has its own log buffer file.

### 6.1 Overall Layout

```
┌──────────────────────────────┐  ← offset 0
│  Term 0                      │  term_length bytes
├──────────────────────────────┤
│  Term 1                      │  term_length bytes
├──────────────────────────────┤
│  Term 2                      │  term_length bytes
├──────────────────────────────┤
│  Log Metadata                │  4096 bytes (AERON_LOGBUFFER_META_DATA_LENGTH)
└──────────────────────────────┘

Total size = ALIGN(term_length × 3 + 4096, page_size)
```

**Three partitions** are used in a round-robin fashion. Only one is active at a time for writing. The index is `term_count % 3`.

**Constraints:**
- `term_length` must be a power of 2
- Minimum: 64 KB
- Maximum: 1 GB
- Frame alignment within terms: 32 bytes

### 6.2 Log Metadata Structure

Located immediately after the three term buffers (4096 bytes):

```
Offset  Size  Type          Field
─────────────────────────────────────────────────────────────
 0      24    volatile i64[3]  term_tail_counters          (packed term_id + offset)
24       4    volatile i32     active_term_count
28     100    padding
128      8    volatile i64     end_of_stream_position
136      4    volatile i32     is_connected                (0 or 1)
140      4    volatile i32     active_transport_count
144    112    padding
256      8    i64              correlation_id
264      4    i32              initial_term_id
268      4    i32              default_frame_header_length
272      4    i32              mtu_length
276      4    i32              term_length
280      4    i32              page_size
284      4    i32              publication_window_length
288      4    i32              receiver_window_length
292      4    i32              socket_sndbuf_length
296      4    i32              os_default_socket_sndbuf_length
300      4    i32              os_max_socket_sndbuf_length
304      4    i32              socket_rcvbuf_length
308      4    i32              os_default_socket_rcvbuf_length
312      4    i32              os_max_socket_rcvbuf_length
316      4    i32              max_resend
320    128    u8[]             default_header              (template data header)
448      8    i64              entity_tag
456      8    i64              response_correlation_id
464      8    i64              linger_timeout_ns
472      8    i64              untethered_window_limit_timeout_ns
480      8    i64              untethered_resting_timeout_ns
488      1    u8               group
489      1    u8               is_response
490      1    u8               rejoin
491      1    u8               reliable
492      1    u8               sparse
493      1    u8               signal_eos
494      1    u8               spies_simulate_connection
495      1    u8               tether
496      1    volatile u8      is_publication_revoked
497      1    u8               type                        (0=concurrent, 1=exclusive, 2=image)
498      2    padding
500      8    i64              untethered_linger_timeout_ns
```

### 6.3 Raw Tail Counter Encoding

Each `term_tail_counters[i]` is a packed 64-bit value:

```
Bits 63..32:  term_id     (int32)
Bits 31..0:   term_offset (int32 — byte offset within the term)
```

```
raw_tail_to_term_id(raw_tail)     = (int32)(raw_tail >> 32)
raw_tail_to_term_offset(raw_tail, term_length) = min(raw_tail & 0xFFFFFFFF, term_length)
pack_raw_tail(term_id, offset)    = ((int64)term_id << 32) | offset
```

### 6.4 Position Arithmetic

Position is a monotonically increasing 64-bit value spanning all terms:

```
position_bits_to_shift = number_of_trailing_zeroes(term_length)
    // e.g., for 64KB terms: shift = 16

term_count = sub_wrap_i32(active_term_id, initial_term_id)

partition_index = term_count % 3

position = ((int64)term_count << position_bits_to_shift) + term_offset

// Max possible position:
max_possible_position = term_length * (int64)(INT32_MAX + 1)
```

### 6.5 Data Frame Layout (within a term)

Messages are stored as data frames aligned to 32 bytes:

```
┌──────────────────────────────────┐
│ frame_length (volatile i32)      │  ← negative while writing, positive when committed
│ version (i8) | flags (u8)        │
│ type (i16)                       │
│ term_offset (i32)                │
│ session_id (i32)                 │
│ stream_id (i32)                  │
│ term_id (i32)                    │
│ reserved_value (i64)             │  ← 32 bytes of data header
├──────────────────────────────────┤
│ message payload                  │
│ ...                              │
│ padding to 32-byte alignment     │
└──────────────────────────────────┘
```

### 6.6 Term Appender — Concurrent Publication (Multi-Writer CAS-Free)

The concurrent publication uses **atomic fetch-and-add** (not CAS) on the raw tail counter to claim space. Multiple threads can offer concurrently.

```
function concurrent_offer(publication, buffer, length):
    if is_closed: return PUBLICATION_CLOSED
    
    limit = counter_get_acquire(position_limit)  // flow control limit from driver
    
    term_count = GET_ACQUIRE(log_metadata.active_term_count)
    index = term_count % 3
    raw_tail = GET_ACQUIRE(term_tail_counters[index])
    term_id = raw_tail >> 32
    term_offset = min(raw_tail & 0xFFFFFFFF, term_length)
    
    // Stale term check
    if term_count != (term_id - initial_term_id):
        return ADMIN_ACTION  // concurrent rotation happened, retry
    
    position = ((int64)term_count << position_bits_to_shift) + term_offset
    
    // Back-pressure check
    if position >= limit:
        if position + aligned_length >= max_possible_position:
            return MAX_POSITION_EXCEEDED
        else if is_connected:
            return BACK_PRESSURED
        else:
            return NOT_CONNECTED
    
    frame_length = length + DATA_HEADER_LENGTH  // +32
    aligned_frame_length = ALIGN(frame_length, 32)
    
    // === CLAIM SPACE (atomic, lock-free) ===
    raw_tail = GET_AND_ADD_INT64(&term_tail_counters[index], aligned_frame_length)
    term_offset = min(raw_tail & 0xFFFFFFFF, term_length)
    resulting_offset = term_offset + aligned_frame_length
    
    // End-of-term check
    if resulting_offset > term_length:
        if term_offset < term_length:
            // This thread writes the PAD frame at end of term
            write_pad_frame(term_buffer, term_offset, term_length - term_offset)
        if position >= max_possible_position:
            return MAX_POSITION_EXCEEDED
        rotate_log(log_metadata, term_count, term_id)  // CAS-based rotation
        return ADMIN_ACTION  // caller should retry
    
    // === WRITE FRAME ===
    header = &term_buffer[term_offset]
    SET_RELEASE(header.frame_length, -(int32)frame_length)    // NEGATIVE = uncommitted
    header.version = 0
    header.flags = BEGIN_FLAG | END_FLAG  // 0xC0 for unfragmented
    header.type = DATA
    header.term_offset = term_offset
    header.session_id = session_id
    header.stream_id = stream_id
    header.term_id = term_id
    memcpy(&term_buffer[term_offset + 32], buffer, length)
    if reserved_value_supplier:
        header.reserved_value = reserved_value_supplier(buffer, frame_length)
    SET_RELEASE(header.frame_length, (int32)frame_length)     // POSITIVE = committed
    
    return new_position
```

**Key insight**: The `GET_AND_ADD_INT64` on the raw tail gives each concurrent writer a unique, non-overlapping region. Writers never conflict because they each own their claimed range. The term_id in the upper 32 bits stays constant (adding to lower 32 bits only changes the offset). When the offset exceeds `term_length`, threads detect the overflow and cooperate on term rotation.

### 6.7 Term Appender — Exclusive Publication (Single Writer)

No CAS or atomic fetch-and-add needed. The exclusive publication caches all state locally:

```
Local state (cache-line padded):
    term_begin_position    (i64)
    term_offset            (i32)
    term_id                (i32)
    active_partition_index (size_t)

function exclusive_offer(publication, buffer, length):
    if is_closed: return PUBLICATION_CLOSED
    
    limit = counter_get_acquire(position_limit)
    position = term_begin_position + term_offset  // from local state, no shared reads
    
    if position >= limit:
        return back_pressure_status(...)
    
    frame_length = length + 32
    aligned_frame_length = ALIGN(frame_length, 32)
    resulting_offset = term_offset + aligned_frame_length
    
    // Update raw tail with RELEASE STORE (no CAS)
    SET_RELEASE(term_tail_counters[active_partition_index],
                pack_raw_tail(term_id, resulting_offset))
    
    if resulting_offset > term_length:
        handle_end_of_log_condition(...)
        return ADMIN_ACTION
    
    // Write frame (same as concurrent)
    header = &term_buffer[term_offset]
    SET_RELEASE(header.frame_length, -frame_length)
    // ... write header fields and payload ...
    SET_RELEASE(header.frame_length, frame_length)
    
    term_offset = resulting_offset  // update local state
    return term_begin_position + resulting_offset

function exclusive_rotate_term(publication):
    next_term_id = term_id + 1
    next_index = index_by_term(initial_term_id, next_term_id)
    
    // Update local state
    active_partition_index = next_index
    term_offset = 0
    term_id = next_term_id
    term_begin_position += term_length
    
    // Update shared state
    term_tail_counters[next_index] = pack_raw_tail(next_term_id, 0)
    SET_RELEASE(active_term_count, next_term_count)
```

### 6.8 Term Rotation (CAS-based, for concurrent publications)

```
function rotate_log(log_metadata, current_term_count, current_term_id):
    next_term_id = current_term_id + 1
    next_term_count = current_term_count + 1
    next_index = next_term_count % 3
    expected_term_id = next_term_id - 3  // what's in the slot from 3 terms ago

    // CAS the next partition's raw_tail
    loop:
        raw_tail = GET_ACQUIRE(term_tail_counters[next_index])
        if raw_tail_to_term_id(raw_tail) != expected_term_id:
            break  // already rotated by another thread
        if CAS_INT64(&term_tail_counters[next_index], raw_tail, pack_raw_tail(next_term_id, 0)):
            break  // we rotated it

    // CAS the active_term_count
    CAS_INT32(&active_term_count, current_term_count, next_term_count)
```

### 6.9 Message Fragmentation

Messages larger than `max_payload_length` (= `mtu_length - DATA_HEADER_LENGTH`) are split into multiple frames:

```
max_payload_length = mtu_length - 32

function fragmented_offer(publication, buffer, length):
    num_full_frames = length / max_payload_length
    remaining = length % max_payload_length
    
    total_framed_length = num_full_frames * (max_payload_length + 32) +
                          (remaining > 0 ? ALIGN(remaining + 32, 32) : 0)
    
    // Claim entire space atomically
    raw_tail = GET_AND_ADD_INT64(&term_tail_counters[index], total_framed_length)
    
    // Write fragments
    for each fragment:
        flags = 0
        if first fragment: flags |= BEGIN_FLAG  // 0x80
        if last fragment:  flags |= END_FLAG    // 0x40
        
        // Write header with negative length, copy payload, commit with positive length
        write_frame(term_buffer, offset, fragment_data, fragment_length, flags)
        offset += aligned_fragment_length
```

### 6.10 Image Poll Algorithm

```
function image_poll(image, handler, fragment_limit):
    if is_closed: return 0
    
    initial_position = counter_get(subscriber_position)
    index = (initial_position >> position_bits_to_shift) % 3
    term_buffer = log_buffers[index]
    initial_offset = initial_position & term_length_mask
    
    offset = initial_offset
    fragments_read = 0
    
    while fragments_read < fragment_limit and offset < term_length:
        frame_length = GET_ACQUIRE(term_buffer[offset].frame_length)
        
        if frame_length <= 0:
            break  // uncommitted or empty
        
        aligned_length = ALIGN(frame_length, 32)
        
        if term_buffer[offset].type != PAD:
            // Construct header info for callback
            header = { frame_header, initial_position + (offset - initial_offset) }
            handler(clientd, &term_buffer[offset + 32], frame_length - 32, header)
            fragments_read += 1
        
        offset += aligned_length
    
    // Update subscriber position
    new_position = initial_position + (offset - initial_offset)
    if new_position > initial_position:
        counter_set_release(subscriber_position, new_position)
    
    return fragments_read
```

### 6.11 Controlled Poll

Like regular poll, but the handler returns an action:

- **ABORT**: Rewind offset, decrement fragment count, stop. Frame is re-delivered next time.
- **BREAK**: Stop after this frame. Position is advanced to include it.
- **COMMIT**: Flush subscriber position immediately, continue reading.
- **CONTINUE**: Keep reading.

### 6.12 Buffer Claim (Zero-Copy Write)

`try_claim` claims space but does NOT copy data. Returns a `buffer_claim_t` pointing into the term buffer:

```
function try_claim(publication, length):
    // Same space-claiming logic as offer
    // Write header with NEGATIVE frame_length
    // Return pointer to payload area

function buffer_claim_commit(claim):
    SET_RELEASE(claim.header.frame_length, claim.frame_length)  // positive = committed

function buffer_claim_abort(claim):
    claim.header.type = PAD
    SET_RELEASE(claim.header.frame_length, claim.frame_length)  // committed as padding (skipped by readers)
```

### 6.13 Term Rebuilder (Receiver Side)

The receiver inserts received UDP data into the image's log buffer:

```
function term_rebuilder_insert(dest, src, length):
    if dest.frame_length == 0:  // only write if slot is empty
        // Copy payload first
        memcpy(dest + 32, src + 32, length - 32)
        // Copy header fields backwards (3, 2, 1)
        dest.hdr[3] = src.hdr[3]  // reserved_value
        dest.hdr[2] = src.hdr[2]  // stream_id + term_id
        dest.hdr[1] = src.hdr[1]  // term_offset + session_id
        SET_RELEASE(dest.hdr[0], src.hdr[0])  // frame_length LAST (publication fence)
```

**Critical ordering**: `frame_length` is written LAST with release semantics. Readers use acquire semantics when reading `frame_length`, ensuring all other fields are visible.

---

## 7. Client Architecture

### 7.1 Client Context

The context holds all configuration:

```
struct aeron_context_t:
    aeron_dir[4096]                    // path to aeron directory
    client_name[100]                   // human-readable client name
    
    // Callbacks (each with void* clientd companion)
    error_handler                      // default: print to stderr and exit(!)
    on_new_publication
    on_new_exclusive_publication
    on_new_subscription
    on_available_image
    on_unavailable_image
    on_available_counter
    on_unavailable_counter
    error_frame_handler
    agent_on_start_func
    on_close_client
    
    // Idle strategy
    idle_strategy_func                 // default: sleep-ns
    idle_strategy_state
    idle_strategy_name
    
    // Timing
    driver_timeout_ms = 10000          // 10 seconds
    keepalive_interval_ns = 500000000  // 500 ms
    resource_linger_duration_ns = 3000000000  // 3 seconds
    idle_sleep_duration_ns = 16000000  // 16 ms
    
    // Options
    use_conductor_agent_invoker = false
    pre_touch_mapped_memory = false
    
    // Runtime state
    cnc_map                            // memory-mapped CnC file
    command_queue                      // MPSC array queue (capacity 256)
```

Environment variable overrides: `AERON_DIR`, `AERON_CLIENT_NAME`, `AERON_DRIVER_TIMEOUT`, `AERON_CLIENT_RESOURCE_LINGER_DURATION`, `AERON_CLIENT_IDLE_SLEEP_DURATION`, `AERON_CLIENT_PRE_TOUCH_MAPPED_MEMORY`.

### 7.2 Client Struct

```
struct aeron_t:
    conductor       // aeron_client_conductor_t (inline, not a pointer)
    runner          // aeron_agent_runner_t
    context         // pointer to aeron_context_t
```

### 7.3 Client Conductor

The conductor is the heart of the client. It runs as a duty-cycle agent.

```
struct aeron_client_conductor_t:
    // Communication channels
    to_client_buffer                   // broadcast receiver (from driver)
    to_driver_buffer                   // MPSC ring buffer (to driver)
    counters_reader                    // shared counters reader
    
    // Resource tracking
    log_buffer_by_id_map               // int64 → log_buffer_t*
    resource_by_id_map                 // int64 → resource*
    image_by_key_map                   // byte_array → image*
    registering_resources[]            // pending registrations
    lingering_resources[]              // resources awaiting cleanup
    
    // Handler arrays (dynamically growable)
    available_counter_handlers[]
    unavailable_counter_handlers[]
    close_handlers[]
    
    // Heartbeat
    heartbeat_timestamp.addr           // pointer into counter values buffer
    heartbeat_timestamp.counter_id
    
    // Timing
    client_id
    time_of_last_service_ns
    time_of_last_keepalive_ns
    inter_service_timeout_ns
    
    // State
    is_closed                          // volatile, acquire/release
    is_terminating
```

### 7.4 Conductor Duty Cycle

```
function client_conductor_do_work(conductor):
    if is_terminating: return 0
    work_count = 0
    
    // 1. Process at most 1 user command from the command queue
    work_count += command_queue.drain(on_command, limit=1)
    
    // 2. Receive driver responses from broadcast buffer
    work_count += broadcast_receiver.receive(on_driver_response)
    
    // 3. Check timeouts (rate-limited by idle_sleep_duration)
    if now_ns > time_of_last_service_ns + idle_sleep_duration_ns:
        
        // Inter-service timeout (conductor stalled too long)
        if now_ns > time_of_last_service_ns + inter_service_timeout_ns:
            TERMINATE! force-close everything
        
        time_of_last_service_ns = now_ns
        
        // Liveness check
        if now_ns > time_of_last_keepalive_ns + keepalive_interval_ns:
            check_driver_heartbeat()  // read consumer_heartbeat from ring buffer
            send_keepalive()          // write CLIENT_KEEPALIVE to ring buffer
            update_client_heartbeat() // write epoch_ms to heartbeat counter
            time_of_last_keepalive_ns = now_ns
        
        // Clean up lingering resources
        for each lingering_resource (backwards):
            if resource is image:
                if image not in use by subscription: prune, decrement refcnt
                if refcnt <= 0: release log buffer, delete image
        
        // Check pending registrations for timeout
        for each registering_resource (backwards):
            if now_ns > registration_deadline_ns:
                mark as TIMED_OUT
    
    return work_count
```

### 7.5 Registration State Machine

Every async operation (add publication, add subscription, etc.) goes through:

```
AWAITING → REGISTERED    (success — driver responded)
AWAITING → ERRORED       (driver returned error)
AWAITING → TIMED_OUT     (no response within deadline)
```

### 7.6 Async Resource Pattern

All resource creation uses a two-phase async pattern:

```
// Phase 1: Initiate
async = aeron_async_add_publication(client, uri, stream_id)
    // Allocates registering_resource, enqueues command to conductor

// Phase 2: Poll (call repeatedly)
result = aeron_async_add_publication_poll(&publication, async)
    // Returns: 0 = still waiting, 1 = success, -1 = error
```

### 7.7 Publication Return Values

| Value | Constant | Meaning |
|-------|----------|---------|
| ≥ 0 | (position) | Success — new stream position after write |
| -1 | NOT_CONNECTED | No subscriber connected |
| -2 | BACK_PRESSURED | Subscriber too slow, position limit reached |
| -3 | ADMIN_ACTION | Term rotation in progress, retry |
| -4 | CLOSED | Publication was closed |
| -5 | MAX_POSITION_EXCEEDED | Log buffer exhausted (2^31 terms reached) |
| -6 | ERROR | Invalid parameters |

### 7.8 Fragment Assembler

Reassembles fragmented messages for the user. Four variants:

1. **Image Fragment Assembler** — single session, simple handler
2. **Image Controlled Fragment Assembler** — single session, controlled handler
3. **Fragment Assembler** — multi-session (maps session_id → buffer_builder)
4. **Controlled Fragment Assembler** — multi-session, controlled handler

**Buffer Builder** state:
```
struct buffer_builder:
    buffer[]                 // dynamically-growing (1.5× growth, max INT32_MAX - 8)
    limit                    // current data size
    next_term_offset         // expected offset of next fragment (-1 = not assembling)
    first_frame_length       // from BEGIN fragment
    header                   // captured from BEGIN fragment
```

**Algorithm:**
1. **Unfragmented** (BEGIN | END): pass directly to user handler
2. **BEGIN**: reset builder, capture header, append data, record `next_term_offset`
3. **Middle/END** (offset matches expected): append data
   - If END: deliver complete message, reset
   - Else: update `next_term_offset`
4. **Out of order**: reset builder (discard partial assembly)

### 7.9 Client Lifecycle Summary

```
1. aeron_context_init(&ctx)        — allocate, apply defaults + env vars
2. aeron_context_set_*(ctx, ...)   — configure
3. aeron_init(&client, ctx)        — connect to driver CnC, init conductor
4. aeron_start(client)             — spawn conductor thread (or prepare invoker)
5. aeron_async_add_publication(...)
   aeron_async_add_publication_poll(...)  — create resources
6. aeron_publication_offer(...)    — write data
   aeron_subscription_poll(...)    — read data
7. aeron_close(client)             — stop conductor, cleanup
8. aeron_context_close(ctx)        — unmap CnC, free context
```

---

## 8. Media Driver Architecture

### 8.1 Driver Structure

```
struct aeron_driver_t:
    context     // aeron_driver_context_t*
    conductor   // aeron_driver_conductor_t (inline)
    sender      // aeron_driver_sender_t (inline)
    receiver    // aeron_driver_receiver_t (inline)
    runners[3]  // aeron_agent_runner_t array
```

### 8.2 Threading Modes

| Mode | Threads | Agent Mapping |
|------|---------|---------------|
| **DEDICATED** (default) | 3 | Runner[0]=Conductor, Runner[1]=Sender, Runner[2]=Receiver |
| **SHARED_NETWORK** | 2 | Runner[0]=Conductor, Runner[1]=Sender+Receiver |
| **SHARED** | 1 | Runner[0]=Conductor+Sender+Receiver |
| **INVOKER** | 0 | Same as SHARED but caller drives the loop manually |

In `aeronmd.c` with `manual_main_loop=true`:
- DEDICATED: main thread = conductor, 2 spawned threads for sender + receiver
- SHARED_NETWORK: main thread = conductor, 1 spawned thread for sender+receiver
- SHARED/INVOKER: main thread runs everything

### 8.3 Inter-Agent Communication via Proxies

Agents **never share mutable state directly**. They communicate through MPSC ring buffers using **self-dispatching commands**:

```
struct command_base:
    func    // function pointer: void (*)(void *clientd, void *command)
    item    // optional pointer to associated resource
```

The consumer generically dispatches: `cmd->func(agent_state, cmd)`. No switch statement needed.

**Communication queues:**

| From → To | Queue | When Used |
|-----------|-------|-----------|
| Conductor → Sender | `sender_command_queue` | Add/remove endpoints, publications |
| Conductor → Receiver | `receiver_command_queue` | Add/remove endpoints, subscriptions, images |
| Sender → Conductor | `conductor_command_queue` | Re-resolve, release resources, errors |
| Receiver → Conductor | `conductor_command_queue` | Create image, re-resolve, release |

**Optimization**: When `SHARED` or `INVOKER` mode (all agents on same thread), proxies **directly call** the handler function instead of writing to the queue.

### 8.4 Agent Runners

Each agent implements a duty-cycle interface:

```
struct agent_runner:
    role_name           // e.g., "conductor", "sender", "receiver"
    do_work()           // returns int (work count)
    on_close()          // cleanup
    idle_strategy()     // called with work_count after each cycle
    thread              // platform thread handle
    running             // volatile bool (acquire/release)
    state               // UNUSED, INITED, STARTED, MANUAL, STOPPING, STOPPED
```

Thread main loop:
```
function agent_main(runner):
    set_thread_name(runner.role_name)
    if runner.on_start: runner.on_start(role_name)
    
    while GET_ACQUIRE(runner.running):
        work_count = runner.do_work(runner.agent_state)
        runner.idle_strategy(runner.idle_strategy_state, work_count)
```

### 8.5 Idle Strategies

| Name | Behavior |
|------|----------|
| **sleeping** / **sleep-ns** | If `work_count > 0`: return. Else: `nano_sleep(duration)`. Default: 1ns. |
| **yielding** | If `work_count > 0`: return. Else: `sched_yield()`. |
| **busy-spinning** | If `work_count > 0`: return. Else: CPU pause instruction. |
| **noop** | Does nothing. |
| **backoff** | Four phases: NOT_IDLE → SPINNING (max_spins × CPU pause) → YIELDING (max_yields × sched_yield) → PARKING (nano_sleep doubling from min_park to max_park). Any positive work_count resets to NOT_IDLE. Defaults: spins=10, yields=20, min_park=1µs, max_park=1ms. |

### 8.6 Driver Conductor

The conductor is the control plane — it processes client commands, manages all resources, and updates counters.

#### Conductor `do_work` Cycle

```
function conductor_do_work(conductor):
    work_count = 0
    now_ns = nano_clock()
    
    // Periodically update epoch clock (~every 1ms)
    if now_ns > clock_update_deadline_ns:
        update_cached_epoch_clock()
    
    // 1. Timeout checks (every timer_interval_ns, default 1s)
    if now_ns > timeout_check_deadline_ns:
        update_consumer_heartbeat()
        
        // Check all managed resources for end-of-life
        for each collection in [clients, ipc_publications, network_publications,
                                send_channel_endpoints, receive_channel_endpoints,
                                publication_images, lingering_resources]:
            for each resource:
                resource.on_time_event(now_ns)
                if resource.has_reached_end_of_life():
                    delete(resource)
        
        check_for_blocked_driver_commands()
        timeout_check_deadline_ns = now_ns + timer_interval_ns
    
    // 2. Process client commands (1 per cycle from CnC to-driver ring buffer)
    work_count += mpsc_rb_controlled_read(to_driver_commands, on_command, limit=1)
    
    // 3. Process inter-agent commands (1 per cycle from conductor_command_queue)
    work_count += mpsc_rb_read(conductor_command_queue, dispatch_by_func_ptr, limit=1)
    
    // 4. Track image rebuild for all publication images
    for each publication_image:
        aeron_publication_image_track_rebuild(image, now_ns)
    
    // 5. Update publication position limits
    for each network_publication:
        update_pub_pos_and_lmt(publication)
    for each ipc_publication:
        update_pub_pos_and_lmt(publication)
    
    // 6. Name resolver work
    name_resolver.do_work()
    
    // 7. Free end-of-life resources
    drain_end_of_life_queue()
    
    // 8. Process async executor completions
    executor.process_completions()
    
    return work_count
```

**Key**: Only **1 command per cycle** from each queue (`AERON_COMMAND_DRAIN_LIMIT = 1`). This keeps latency bounded.

### 8.7 Sender Agent

The sender transmits data frames over UDP and processes control messages (status messages, NAKs).

```
struct aeron_driver_sender_t:
    sender_proxy                    // embedded proxy for other agents
    poller                          // UDP transport poller
    network_publications[]          // publications this sender manages
    round_robin_index               // fair send distribution
    duty_cycle_counter              // for send-to-SM-poll ratio
    status_message_read_timeout_ns  // half of status_message_timeout_ns
```

#### Sender `do_work` Cycle

```
function sender_do_work(sender):
    now_ns = nano_clock()
    work_count = 0
    
    // 1. Process commands from conductor (1 per cycle)
    work_count += mpsc_rb_read(sender_command_queue, dispatch_by_func_ptr, limit=1)
    
    // 2. Send data (round-robin across publications)
    bytes_sent = 0
    for each publication starting at round_robin_index:
        bytes_sent += publication.send(now_ns)
    round_robin_index = (round_robin_index + 1) % publication_count
    
    // 3. Poll for control messages (SMs, NAKs) — duty-cycle gated
    //    Only poll if: no bytes sent, OR duty_cycle hit ratio, OR timeout expired
    if should_poll_for_control():
        bytes_received = poller.poll(recv_buffers, limit=1)
        // Dispatch: SM → flow control update, NAK → retransmit handler
    
    // 4. Check re-resolution deadlines
    check_re_resolution()
    
    return work_count + bytes_sent + bytes_received
```

**Send-to-SM-poll ratio**: Default **6** — the sender sends 6 cycles of data for every 1 cycle of control message polling. This prioritizes throughput.

### 8.8 Receiver Agent

The receiver reads UDP data and inserts it into log buffers. It also sends status messages and NAKs.

```
struct aeron_driver_receiver_t:
    receiver_proxy                  // embedded proxy
    poller                          // UDP transport poller
    images[]                        // publication images managed by this receiver
    pending_setups[]                // waiting for SETUP frames from publishers
```

#### Receiver `do_work` Cycle

```
function receiver_do_work(receiver):
    now_ns = nano_clock()
    work_count = 0
    
    // 1. Process commands from conductor (1 per cycle)
    work_count += mpsc_rb_read(receiver_command_queue, dispatch_by_func_ptr, limit=1)
    
    // 2. Poll for incoming data (batch of up to receiver_io_vector_capacity=4)
    bytes_received = poller.poll(recv_buffers, limit=4)
    // Dispatch: DATA → publication_image.insert_packet
    //           SETUP → create new image via conductor proxy
    //           RTTM → RTT measurement
    
    // 3. For each tracked image:
    for each image:
        image.send_pending_status_message()    // send SMs
        image.send_pending_loss()              // send NAKs
        image.initiate_rttm()                  // start RTT measurement
    
    // 4. Process pending setups
    for each pending_setup (backwards):
        if timed_out:
            if periodic: send SM with SEND_SETUP flag, reset timer
            else: remove
    
    // 5. Check re-resolution
    check_re_resolution()
    
    return work_count + bytes_received
```

### 8.9 Driver Configuration Defaults

| Parameter | Default | Description |
|-----------|---------|-------------|
| `threading_mode` | DEDICATED | 3 threads |
| `to_driver_buffer_length` | 1 MB + trailer | Client → conductor ring buffer |
| `to_clients_buffer_length` | 1 MB + trailer | Conductor → clients broadcast |
| `counters_values_buffer_length` | 8 MB | Counter values |
| `error_buffer_length` | 4 MB | Error log |
| `term_buffer_length` | 16 MB | Network publication term |
| `ipc_term_buffer_length` | 64 MB | IPC publication term |
| `mtu_length` | 1408 bytes | Network MTU |
| `ipc_mtu_length` | 1408 bytes | IPC MTU |
| `file_page_size` | 4 KB | Page alignment |
| `client_liveness_timeout_ns` | 10 s | Client heartbeat timeout |
| `timer_interval_ns` | 1 s | Conductor timeout check interval |
| `image_liveness_timeout_ns` | 10 s | Image liveness timeout |
| `publication_linger_timeout_ns` | 5 s | Pub lingers after close |
| `publication_unblock_timeout_ns` | 15 s | Unblock stuck publishers |
| `publication_connection_timeout_ns` | 5 s | Connection establishment |
| `status_message_timeout_ns` | 200 ms | SM timeout |
| `counter_free_to_reuse_ns` | 1 s | Counter slot reuse delay |
| `initial_window_length` | 128 KB | Receiver initial window |
| `socket_rcvbuf` | 128 KB | SO_RCVBUF |
| `socket_sndbuf` | 0 (OS default) | SO_SNDBUF |
| `send_to_sm_poll_ratio` | 6 | Send cycles per SM poll |
| `loss_report_length` | 1 MB | Loss report buffer |
| `nak_unicast_delay_ns` | 1 µs | NAK delay for unicast |
| `nak_multicast_max_backoff_ns` | 10 µs | Max multicast NAK backoff |
| `retransmit_unicast_delay_ns` | 0 | Immediate unicast retransmit |
| `retransmit_unicast_linger_ns` | 10 µs | Retransmit linger |
| `re_resolution_check_interval_ns` | 1 s | DNS re-resolution interval |
| `receiver_io_vector_capacity` | 4 | recvmmsg batch size |
| `sender_io_vector_capacity` | 4 | Control message batch size |
| `network_publication_max_messages_per_send` | 4 | sendmmsg batch |
| `resource_free_limit` | 10 | End-of-life free batch |
| `conductor_cycle_threshold_ns` | 100 µs | Stall detection |
| `COMMAND_DRAIN_LIMIT` | 1 | Commands per do_work cycle |

### 8.10 Driver Lifecycle

```
1. aeron_driver_context_init(&ctx)         — defaults + env vars
2. aeron_driver_init(&driver, ctx)         — creates CnC file, inits all agents
   a. Validate parameters
   b. Recreate aeron directory (check for existing active driver)
   c. Create & memory-map cnc.dat, fill metadata
   d. Create loss report file
   e. Init conductor (ring buffers, broadcast, counters, error log, name resolver)
   f. Init sender (poller, recv buffers, data paths)
   g. Init receiver (same)
   h. Init system counters (aeron version, protocol version, etc.)
   i. Write cnc_version with RELEASE semantics + msync (signals readiness)
   j. Init agent runners based on threading mode
3. aeron_driver_start(driver)              — start runner threads
4. Main loop: do_work → idle_strategy
5. aeron_driver_close(driver)              — stop all runners, join threads
6. aeron_driver_context_close(ctx)         — free context
```

---

## 9. Network Layer

### 9.1 Send Channel Endpoint

A send channel endpoint encapsulates one UDP socket for sending data and receiving control messages (SMs, NAKs):

```
struct send_channel_endpoint:
    // Conductor fields (padded)
    managed_resource
    refcnt
    udp_channel
    status
    
    // Transport
    transport                      // aeron_udp_channel_transport_t
    channel_status                 // counter
    
    // Multi-destination tracking (for MDC)
    destination_tracker            // NULL for simple unicast/multicast
    
    // Dispatch
    publication_dispatch_map       // (stream_id, session_id) → network_publication
    
    // Addresses
    current_data_addr              // resolved destination
    bind_addr                      // local bind address
```

**Socket creation** (`aeron_udp_channel_transport_init`):
1. `socket(family, SOCK_DGRAM, 0)` — UDP
2. **Unicast**: `bind(fd, bind_addr)`
3. **Multicast**: Create second socket for recv, set `SO_REUSEADDR` + `SO_REUSEPORT`, bind to `0.0.0.0:port`, `IP_ADD_MEMBERSHIP`, set `IP_MULTICAST_IF` and `IP_MULTICAST_TTL` on send fd
4. If `connect_addr` specified: `connect(fd, connect_addr)` — enables `send()` instead of `sendto()`
5. Set `SO_RCVBUF`, `SO_SNDBUF`
6. Set non-blocking

**Sending**: Uses scatter-gather I/O (`struct iovec`) with `sendmmsg` where available, falling back to `sendmsg` loops.

**Receiving control**: Dispatches by frame type:
- `NAK` → find publication by `(stream_id, session_id)` → `publication.on_nak()`
- `SM` → update destination tracker, trigger setup or flow control
- `ERR`, `RTTM`, `RSP_SETUP` → respective handlers

### 9.2 Receive Channel Endpoint

Encapsulates one or more UDP sockets for receiving data:

```
struct receive_channel_endpoint:
    // Conductor fields
    managed_resource
    udp_channel
    image_ref_count
    
    // Multiple destinations (for MDS)
    destinations[]                 // each with its own transport
    
    // Packet routing
    dispatcher                     // aeron_data_packet_dispatcher_t
    
    // Stream tracking
    stream_id_to_refcnt_map
    stream_and_session_id_to_refcnt_map
```

### 9.3 Data Packet Dispatcher

Routes incoming UDP packets to the correct `publication_image`:

```
struct data_packet_dispatcher:
    session_by_stream_id_map       // stream_id → stream_interest
    ignored_sessions_map

struct stream_interest:
    is_all_sessions
    image_by_session_id_map        // session_id → publication_image
    state_by_session_id_map        // session_id → state
```

**Per-session states:**

| State | Value | Meaning |
|-------|-------|---------|
| IMAGE_ACTIVE | 1 | Image exists, fast-path dispatch |
| IMAGE_PENDING_SETUP_FRAME | 2 | Awaiting SETUP from publisher |
| IMAGE_INIT_IN_PROGRESS | 3 | Create-image sent to conductor |
| IMAGE_COOL_DOWN | 4 | Recently removed, reject briefly |
| IMAGE_NO_INTEREST | 5 | No subscription for this session |

**Data dispatch algorithm:**
```
function on_data(endpoint, buffer, length, addr):
    stream_interest = lookup(session_by_stream_id_map, header.stream_id)
    if stream_interest:
        image = lookup(image_by_session_id_map, session_id)
        if image:
            image.insert_packet(buffer, length)  // fast path
        else if not EOS and has_interest:
            elicit_setup(session_id)  // send SM with SEND_SETUP flag
```

**Setup-to-image lifecycle:**
1. Data arrives for unknown session → send SM with `SEND_SETUP` flag (state = `PENDING_SETUP_FRAME`)
2. SETUP frame arrives → send create-image to conductor (state = `INIT_IN_PROGRESS`)
3. Conductor creates `publication_image`, adds to dispatcher
4. Subsequent data → fast-path `insert_packet`

### 9.4 Multicast Addressing

For multicast, Aeron uses a data/control address pair:

- **Data address**: must have an **odd** last byte in the address
- **Control address**: data address + 1 in the last byte

For IPv4: `data = 224.0.1.1:40123`, `control = 224.0.1.2:40123`
For IPv6: similar with last byte of the address.

The control address is where NAKs and status messages are sent. The data address is where data frames are sent.

### 9.5 Transport Poller

For ≤5 transports: simple linear iteration calling `recvmmsg` on each.
For >5 transports: `epoll_wait` (Linux) or `poll()` to find ready sockets first.

### 9.6 Destination Tracker (for MDC)

Maintains a list of destinations for multi-destination cast:

- **Dynamic mode**: destinations auto-added on receipt of SMs, auto-removed after timeout (5s). Matched by `(receiver_id, port)`.
- **Manual mode**: destinations added/removed explicitly. No timeout.

Sends to ALL active destinations using round-robin starting index for fairness.

---

## 10. Flow Control

Flow control determines how far ahead the sender is allowed to transmit. The sender position limit (`snd_lmt`) is updated based on status messages from receivers.

### 10.1 Strategy Interface

```
struct flow_control_strategy:
    on_status_message(sm, snd_lmt, initial_term_id, position_bits_to_shift, now_ns) → new_snd_lmt
    on_idle(now_ns, snd_lmt, ...) → new_snd_lmt
    has_required_receivers() → bool
    max_retransmission_length(resend_length, term_buffer_length, ...) → max_length
    on_setup(...)
    on_error(...)
    on_trigger_send_setup(...) → bool
    fini()
```

### 10.2 Max Flow Control (Unicast Default)

Tracks the **maximum** receiver window edge:

```
function on_status_message(sm):
    position = compute_position(sm.consumption_term_id, sm.consumption_term_offset)
    window_edge = position + sm.receiver_window
    return max(snd_lmt, window_edge)
```

On idle: returns existing `snd_lmt` unchanged.

### 10.3 Min/Tagged Flow Control (Multicast)

Tracks **all receivers** and uses the **minimum** window edge:

```
struct min_flow_control_state:
    receivers[]  // {last_position, last_position_plus_window, time_of_last_sm, receiver_id, eos_flagged}
    group_min_size
    receiver_timeout_ns
    group_tag      // for tagged variant: only track receivers with matching tag

function on_status_message(sm):
    for each receiver:
        if receiver.id == sm.receiver_id:
            update receiver position
    if new receiver and not too far behind:
        add to tracked list
    return min(all receiver.last_position_plus_window)
           // or snd_lmt if receiver_count < group_min_size

function on_idle(now_ns):
    for each receiver:
        if now_ns - last_sm_time > receiver_timeout:
            remove receiver
    return min(all remaining receiver.position_plus_window)
```

`has_required_receivers()` returns `false` until `group_min_size` receivers connect.

### 10.4 Publication Position Limit (Conductor Side)

The conductor computes `pub_lmt` (how far the client can write):

```
function update_pub_pos_and_lmt(publication):
    if has_subscribers:
        min_consumer_pos = min(all subscriber positions, snd_pos)
        new_limit = min_consumer_pos + term_window_length
        
        // Dirty buffer guard
        if term_gap(active_term, dirty_term) < 2:
            pub_lmt = new_limit
    else:
        pub_lmt = snd_pos  // back-pressure: no subscribers
```

### 10.5 Retransmission Length Limiting

Flow control also limits retransmission size:

```
retransmit_length = min(
    length_to_end_of_term,
    resend_length,
    receiver_window * retransmit_receiver_window_multiple
)

// Unicast: multiple = 16 (aggressive)
// Multicast: multiple = 4 (conservative, avoid flooding)
```

---

## 11. Congestion Control

Congestion control operates on the **receiver side** and determines the receiver window size reported in status messages.

### 11.1 Strategy Interface

```
struct congestion_control_strategy:
    on_track_rebuild(is_new_entry, now_ns, min_sub_pos, ending_term_id,
                     ending_term_offset, loss_found) → (window_length, should_force_sm)
    should_measure_rtt() → bool
    on_rttm_sent(now_ns)
    on_rttm(now_ns, rtt_ns)
    initial_window_length() → i32
    max_window_length() → i32
    fini()
```

### 11.2 Static Window (Default)

Fixed window size, never changes:

```
function on_track_rebuild(...):
    should_force_sm = false
    return window_length  // = min(initial_window_length, term_length / 2)
```

Does not measure RTT.

### 11.3 Cubic Congestion Control

TCP-CUBIC-inspired adaptive congestion control:

```
State:
    cwnd              // congestion window (in MTU units)
    w_max             // window before last loss
    k                 // time to reach w_max: cbrt(w_max * B / C)
    rtt_ns            // measured round-trip time
    last_loss_timestamp_ns
    tcp_mode          // enable TCP-friendly region

Constants: C = 0.4, B = 0.2

function on_track_rebuild(..., loss_found):
    if loss_found:
        should_force_sm = true
        w_max = cwnd
        k = cbrt(w_max * B / C)
        cwnd = max(cwnd * (1 - B), 1)     // reduce by 20%
        last_loss_timestamp = now
    
    else if cwnd < max_cwnd and window_update_timeout_expired:
        T = (now - last_loss_timestamp) / 1e9  // seconds since loss
        
        // Cubic growth
        cwnd = w_max + C * (T - k)^3
        
        // TCP-friendly region (optional)
        if tcp_mode and cwnd < w_max:
            W_tcp = w_max * (1-B) + 3*B/(2-B) * T/RTT
            cwnd = max(cwnd, W_tcp)
    
    return cwnd * mtu  // window in bytes
```

---

## 12. Loss Detection and Retransmission

### 12.1 Network Publication State Machine

```
ACTIVE → DRAINING → LINGER → DONE

ACTIVE:    Normal operation. Sender sends data, processes SMs/NAKs.
DRAINING:  All client publishers closed. EOS set. Waiting for sender to catch up.
LINGER:    All data sent. Waiting for linger_timeout (or unicast EOS SM).
DONE:      Ready for cleanup.
```

Special transition: ACTIVE → LINGER when publication is **revoked** (force-closed).

### 12.2 Network Publication Send

```
function network_publication_send(publication, now_ns):
    snd_pos = counter_get(snd_pos_position)
    
    // Send SETUP frame if no initial connection
    if !has_initial_connection or is_setup_elicited:
        send_setup_frame(...)
    
    // Send data frames (vectored I/O, up to max_messages_per_send)
    available_window = snd_lmt - snd_pos  // flow control window
    
    for i in 0..max_messages_per_send:
        if available_window <= 0:
            increment snd_bpe (back-pressure event counter)
            break
        
        scan_limit = min(available_window, mtu_length)
        available = term_scanner_scan(term_buffer, offset, scan_limit)
        
        if available > 0:
            iov[i] = { ptr, available }
            available_window -= (available + padding)
    
    if iov_count > 0:
        send_channel_send(endpoint, iov, iov_count, &bytes_sent)
        counter_set_release(snd_pos, highest_position)
    
    // If no data sent: send heartbeat, run flow control on_idle
    if bytes_sent == 0:
        heartbeat_check(now_ns)  // zero-length DATA frame every 100ms
        flow_control.on_idle(now_ns)
    
    // Always process retransmit timeouts
    retransmit_handler.process_timeouts(now_ns)
```

### 12.3 Publication Image (Receiver Side)

The image's state machine mirrors the publication's:

```
ACTIVE → DRAINING → LINGER → DONE

ACTIVE:    Receiving data, tracking gaps, sending SMs.
DRAINING:  No subscribers, or EOS received, or liveness timeout.
LINGER:    Subscribers caught up, EOS SMs sent.
DONE:      Cleanup.
```

#### Packet Insertion

```
function insert_packet(image, buffer, length, addr):
    proposed_position = compute_position(term_id, term_offset + length)
    
    // Over-run check: proposed > last_sm_position + term_length/2
    if flow_control_over_run(proposed_position):
        increment flow_control_over_run_counter
        return
    
    if is_heartbeat(length == DATA_HEADER_LENGTH):
        track_connection(addr)
        check_eos_flags()
        propose_max(rcv_hwm, proposed_position)
        return
    
    // Under-run check: packet_position < last_sm_position
    if flow_control_under_run(packet_position):
        increment flow_control_under_run_counter
        return
    
    // Insert into term buffer
    term_rebuilder_insert(term_buffer[term_offset], buffer, length)
    propose_max(rcv_hwm, proposed_position)
```

#### Track Rebuild (Conductor Thread)

Called every conductor cycle to advance receiver state:

```
function track_rebuild(image, now_ns):
    // Find min/max subscriber positions
    min_sub_pos = min(all subscriber positions)
    max_sub_pos = max(all subscriber positions)
    
    rebuild_position = max(rcv_pos, max_sub_pos)
    
    // Scan for gaps (loss detection)
    rebuild_offset = loss_detector.scan(
        term_buffer, rebuild_position, hwm_position, now_ns)
    
    new_rebuild_position = compute_position(term_id, rebuild_offset)
    propose_max(rcv_pos, new_rebuild_position)
    
    // Ask congestion control for window size
    (window_length, should_force_sm) = congestion_control.on_track_rebuild(
        min_sub_pos, hwm_position, rebuild_position, loss_found)
    
    // Schedule SM if consumer advanced enough
    if should_force_sm or min_sub_pos > next_sm_position + window/4:
        clean_buffer_to(min_sub_pos - term_length)
        schedule_status_message(min_sub_pos, window_length)
```

### 12.4 Loss Detector

Scans term buffers for gaps (frames with `frame_length == 0` between non-zero frames):

```
State:
    scanned_gap      // gap found in current scan
    active_gap       // gap being timed for NAK
    expiry_ns        // when to fire NAK

function scan(term_buffer, rebuild_position, hwm_position, now_ns):
    if rebuild_position < hwm_position:
        // Walk frames from rebuild_offset
        offset = term_gap_scanner(buffer, rebuild_offset, limit_offset)
        
        if gap found:
            if gap != active_gap:
                // New gap — activate with initial delay
                active_gap = scanned_gap
                expiry_ns = now_ns + delay_generator(initial=true)
            
            if now_ns >= expiry_ns:
                // Timer fired — notify (triggers NAK)
                on_gap_detected(active_gap.term_id, active_gap.offset, active_gap.length)
                expiry_ns = now_ns + delay_generator(initial=false)  // retry delay
```

#### Delay Generators

**Unicast**: Fixed 60ms delay, same for retries.

**Multicast**: Optimal feedback suppression:
```
// Randomized delay based on group size
// lambda = ln(group_size) + 1
// x = random(0, 1) * rand_max + base_x
// delay = constant_t * ln(x * factor_t)
```
Larger groups → wider delay distribution → prevents NAK implosion.

### 12.5 Retransmit Handler

Manages retransmission state on the sender side:

```
States per retransmit action:
    INACTIVE  → available for new NAK
    DELAYED   → waiting for delay timeout before retransmitting
    LINGERING → retransmit sent, suppressing duplicate NAKs

Pool size:
    Unicast: 1 (single outstanding retransmit; new NAKs replace old)
    Multicast: configurable up to 256 (max_retransmits)
```

**NAK processing:**
```
function on_nak(term_id, term_offset, length, now_ns):
    retransmit_length = flow_control.max_retransmission_length(length)
    
    // Check overlap with existing retransmits
    for each active action:
        if overlaps(action, term_id, term_offset):
            return  // suppress duplicate
    
    action = find_available_slot()
    if action == NULL:
        increment retransmit_overflow_counter
        return
    
    if delay_timeout == 0:
        resend(term_id, term_offset, length)  // immediate
        action.state = LINGERING
        action.expiry = now + linger_timeout
    else:
        action.state = DELAYED
        action.expiry = now + delay_timeout
```

**Timeout processing (every send cycle):**
```
for each action:
    if DELAYED and now > expiry:
        resend(action.term_id, action.term_offset, action.length)
        action.state = LINGERING
        action.expiry = now + linger_timeout
    
    if LINGERING and now > expiry:
        action.state = INACTIVE
```

**Resend logic:**
```
function resend(publication, term_id, term_offset, length):
    // Validate within resend window:
    //   bottom = sender_position - term_length/2 - max_message_length
    //   must be in [bottom, sender_position)
    
    // Fragment into MTU-sized chunks
    while remaining > 0:
        available = term_scanner_scan(buffer, offset, mtu_length)
        send_channel_send(endpoint, &iov, 1, &bytes_sent)
```

### 12.6 Unreliable Subscriptions

For unreliable subscriptions (`reliable=false`), gaps are **filled with padding frames** instead of requesting retransmission:

```
if is_reliable:
    send_nak(term_id, term_offset, length)
else:
    term_gap_filler_try_fill_gap(term_buffer, term_id, term_offset, length)
```

---

## 13. URI Format and Parsing

### 13.1 URI Grammar

```
aeron:<transport>?<key>=<value>|<key>=<value>|...

Transports: "udp", "ipc"
Separator: | (pipe, not &)
Spy prefix: aeron-spy:aeron:udp?...
```

Maximum URI length: 4096 bytes.

### 13.2 Parsing Algorithm

1. Copy URI into a mutable buffer
2. Verify `aeron:` prefix
3. Match `udp` or `ipc` transport
4. State machine splits on `=` (key/value separator) and `|` (pair separator)
5. NUL-terminate keys and values in place (zero-copy string slicing)
6. Dispatch each key/value pair to a param handler function

### 13.3 UDP Parameters

**First-class parameters** (stored in dedicated struct fields):

| Key | Description |
|-----|-------------|
| `endpoint` | Remote address `host:port` |
| `interface` | Local interface `ip/prefix` |
| `control` | Control address for MDC |
| `control-mode` | `manual`, `dynamic`, `response` |
| `ttl` | Multicast TTL |
| `tags` | Format: `channel_tag,entity_tag` |

**Additional parameters** (stored in a generic array):

| Key | Type | Description |
|-----|------|-------------|
| `reliable` | bool | Reliable delivery (default true) |
| `session-id` | i32 or `tag:N` | Session ID |
| `term-length` | size | Term buffer length |
| `mtu` | size | MTU length |
| `init-term-id` | i32 | Initial term ID |
| `term-id` | i32 | Current term ID |
| `term-offset` | u64 | Current term offset |
| `linger` | duration | Publication linger timeout |
| `sparse` | bool | Sparse term files |
| `eos` | bool | Signal end-of-stream |
| `tether` | bool | Tether subscriptions |
| `rejoin` | bool | Rejoin stream |
| `fc` | string | Flow control strategy (e.g., `min`, `max`, `tagged`) |
| `cc` | string | Congestion control (e.g., `static`, `cubic`) |
| `gtag` | i64 | Group tag |
| `group` | string | Group consideration |
| `ssc` | bool | Spies simulate connection |
| `so-sndbuf` | size | Socket SO_SNDBUF |
| `so-rcvbuf` | size | Socket SO_RCVBUF |
| `rcv-wnd` | size | Receiver window |
| `pub-wnd` | size | Publication window |
| `nak-delay` | duration | NAK delay |
| `max-resend` | u32 | Max retransmissions |
| `alias` | string | Channel alias |

### 13.4 Canonical Form

Used to deduplicate channel endpoints:

```
Format: UDP-<local_addr>-<remote_addr>[#tag_id | -counter]

Examples:
  UDP-0.0.0.0:0-192.168.1.1:40123
  UDP-192.168.1.100:0-224.0.1.1:40123
  UDP-0.0.0.0:12345-192.168.1.1:40123#5   (tagged)
```

### 13.5 IPC URIs

```
aeron:ipc                          // minimal
aeron:ipc?session-id=42            // with session
aeron:ipc?term-length=1048576      // with term length
```

---

## 14. Counters and Monitoring

### 14.1 System Counters (Driver)

Counter IDs 0-44 are reserved for system metrics:

| ID | Name |
|----|------|
| 0 | Bytes sent |
| 1 | Bytes received |
| 2 | Failed offers to receiver proxy |
| 3 | Failed offers to sender proxy |
| 4 | Failed offers to driver conductor proxy |
| 5 | NAKs sent |
| 6 | NAKs received |
| 7 | Status messages sent |
| 8 | Status messages received |
| 9 | Heartbeats sent |
| 10 | Heartbeats received |
| 11 | Retransmits sent |
| 12 | Flow control under runs |
| 13 | Flow control over runs |
| 14 | Invalid packets |
| 15 | Errors |
| 16 | Short sends |
| 17 | Free fails |
| 18 | Sender flow control limits |
| 19 | Unblocked publications |
| 20 | Unblocked control commands |
| 21 | Possible TTL asymmetry |
| 22 | Conductor max cycle time |
| 23 | Sender max cycle time |
| 24 | Receiver max cycle time |
| 25-44 | Various additional metrics |

### 14.2 Counter Type IDs

| Type ID | Name | Key Layout |
|---------|------|------------|
| 1 | PUBLISHER_LIMIT | stream position key |
| 2 | SENDER_POSITION | stream position key |
| 3 | RECEIVER_HWM | stream position key |
| 4 | SUBSCRIPTION_POSITION | stream position key |
| 5 | RECEIVER_POSITION | stream position key |
| 6 | SEND_CHANNEL_STATUS | channel endpoint key |
| 7 | RECEIVE_CHANNEL_STATUS | channel endpoint key |
| 9 | SENDER_LIMIT | stream position key |
| 10 | PER_IMAGE | custom |
| 11 | CLIENT_HEARTBEAT_TIMESTAMP | custom |
| 12 | PUBLISHER_POSITION | stream position key |
| 13 | SENDER_BPE | stream position key |
| 14 | LOCAL_SOCKADDR | custom |

### 14.3 Channel Endpoint Status Values

| Value | Meaning |
|-------|---------|
| 0 | INITIALIZING |
| 1 | ACTIVE |
| 2 | CLOSING |
| 3 | ERROR |
| 4 | ERRORED |

### 14.4 Loss Reporter

A flat buffer for recording packet loss observations:

```
struct loss_report_entry:
    volatile i64  observation_count
    volatile i64  total_bytes_lost
    i64           first_observation_timestamp
    volatile i64  last_observation_timestamp
    i32           session_id
    i32           stream_id
    // variable: i32 channel_length, char[] channel, i32 source_length, char[] source
```

Entries are aligned to 64 bytes (cache line).

---

## 15. Utility Data Structures

### 15.1 Hash Maps

All hash maps use **open addressing with linear probing** and Robin Hood compaction on deletion. Capacity is always power-of-two (masking instead of modulo).

#### Int64-to-Ptr Hash Map

Maps `int64_t` keys to `void*` values. Two parallel arrays: `keys[]` and `values[]`. Occupancy: `values[i] != NULL`. **Cannot store NULL values.**

**Hash function**: Stafford variant 13 of MurmurHash3's 64-bit finalizer:
```
x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9
x = (x ^ (x >> 27)) * 0x94d049bb133111eb
x = x ^ (x >> 31)
```

Load factor: 0.65 (default). Rehash: double capacity.

**Remove**: Robin Hood compaction — after deleting, walk forward moving entries backward if they belong before the gap.

#### Int64-to-Tagged-Ptr Hash Map

Like above but each entry has a `uint32_t tag` alongside the pointer. Uses an `internal_flags` field for occupancy (not NULL-checking), so **NULL values CAN be stored**.

#### Int64 Counter Map

Maps `int64_t` → `int64_t`. Interleaved flat array: `[key0, val0, key1, val1, ...]`. Uses **even hash** (indices always even). Probes by +2. Supports atomic-style `get_and_add`, `inc_and_get`, etc.

#### Array-to-Ptr and String-to-Ptr Hash Maps

Map byte arrays or strings to `void*`. Use **FNV-1a 64-bit** hash:
```
hash = 0xcbf29ce484222325  // FNV offset basis
for each byte:
    hash ^= byte
    hash *= 0x100000001b3   // FNV prime
```

**Key data is NOT copied** — maps store pointers into caller-owned data.

### 15.2 Bit Set

Compact bit vector backed by `uint64_t[]`:

```
get(index): entry = index / 64, offset = index % 64, return (bits[entry] >> offset) & 1
set(index, value): mask = 1 << offset, bits[entry] = (bits[entry] & ~mask) | (value << offset)
find_first(value): scan entries, use trailing_zeroes for exact position
```

### 15.3 Deque

Circular buffer-based double-ended queue:
- `add_last`: append at tail, grow 2× if full
- `remove_first`: pop from head

### 15.4 Linked Queue

Singly-linked FIFO queue with heap-allocated nodes. Used for blocking operations.

### 15.5 Dynamic Array Utilities

```
ENSURE_CAPACITY(array, type):  // grow by 50% when length >= capacity
fast_unordered_remove(array, index, last_index):  // O(1), copies last element over removed
```

---

## 16. Constants Reference

### 16.1 Buffer Sizes

| Structure | Size | Notes |
|-----------|------|-------|
| Ring Buffer Trailer | 768 bytes | 6 × 128-byte cache-line-padded slots |
| Ring Buffer Record Header | 8 bytes | `int32 length` + `int32 msg_type_id` |
| Ring Buffer Alignment | 8 bytes | |
| Broadcast Trailer | 128 bytes | 3 × int64 + padding |
| Broadcast Record Header | 8 bytes | |
| Log Metadata | 4096 bytes (fixed) | = `PAGE_MIN_SIZE` |
| Log Frame Alignment | 32 bytes | Frames in term aligned to 32 |
| Data Header | 32 bytes | |
| Counter Value | 128 bytes | 2 cache lines |
| Counter Metadata | 512 bytes | 4:1 ratio with value |
| Counter Max Key Length | 112 bytes | |
| Counter Max Label Length | 380 bytes | |
| Error Log Header | 24 bytes | |
| Error Log Alignment | 8 bytes | |
| CnC Metadata | 128 bytes | 2 cache lines |

### 16.2 Protocol Constants

| Constant | Value |
|----------|-------|
| `FRAME_HEADER_VERSION` | 0 |
| `AERON_DATA_HEADER_LENGTH` | 32 |
| `AERON_LOGBUFFER_PARTITION_COUNT` | 3 |
| `AERON_LOGBUFFER_TERM_MIN_LENGTH` | 64 KB |
| `AERON_LOGBUFFER_TERM_MAX_LENGTH` | 1 GB |
| `AERON_LOGBUFFER_META_DATA_LENGTH` | 4096 |
| `AERON_LOGBUFFER_FRAME_ALIGNMENT` | 32 |
| `AERON_PAGE_MIN_SIZE` | 4 KB |
| `AERON_PAGE_MAX_SIZE` | 1 GB |
| `AERON_RB_PADDING_MSG_TYPE_ID` | -1 |
| `AERON_BROADCAST_PADDING_MSG_TYPE_ID` | -1 |
| `AERON_FRAME_MAX_MESSAGE_LENGTH` | 16 MB |
| `AERON_MAX_PATH` | 4096 |
| `AERON_URI_MAX_LENGTH` | 4096 |
| `AERON_COUNTER_MAX_CLIENT_NAME_LENGTH` | 100 |
| `AERON_NETWORK_PUBLICATION_HEARTBEAT_TIMEOUT_NS` | 100 ms |
| `AERON_DRIVER_RECEIVER_PENDING_SETUP_TIMEOUT_NS` | 1 s |

### 16.3 Memory Ordering Summary

| Operation | Ordering | Rationale |
|-----------|----------|-----------|
| Write record length (negative) | Release | Makes sentinel visible before data |
| Write record length (positive) | Release | Commits record — all data visible |
| Read record length | Acquire | Ensures reading committed data |
| Write head_position | Release | Frees space for producers |
| Read head_position | Acquire | Producers see consumer progress |
| Write tail_position (MPSC) | CAS (full barrier) | Multi-producer coordination |
| Write tail_position (SPSC) | Release | Single producer visibility |
| Write counter state | Release | Metadata visible before ALLOCATED |
| Write frame_length in term | Release (last) | Entire frame visible atomically |
| Read frame_length in term | Acquire | Subsequent fields valid |
| Broadcast tail_intent_counter | Release + store fence | Invalidation before overwrite |
| CnC version | Release + msync | Signals file readiness |

### 16.4 Clock Functions

Two clock sources needed:

- **Monotonic nanosecond clock**: `CLOCK_MONOTONIC` (Linux), `CLOCK_MONOTONIC_RAW` (macOS), `QueryPerformanceCounter` (Windows).
- **Wall-clock milliseconds**: `CLOCK_REALTIME_COARSE` (Linux), `CLOCK_REALTIME` (others), `GetSystemTimeAsFileTime` (Windows).

The driver and client both use a **cached clock** to avoid syscalls on every cycle:
```
struct clock_cache:
    pre_pad[56]              // cache line alignment
    volatile i64 cached_epoch_time
    volatile i64 cached_nano_time
    post_pad[56]
```

Updated with release semantics, read with acquire.

### 16.5 Wrapping Arithmetic

Three functions for safe 32-bit wrapping (term IDs wrap):
```
add_wrap_i32(a, b) = (int32)((int64)a + (int64)b)
sub_wrap_i32(a, b) = (int32)((int64)a - (int64)b)
mul_wrap_i32(a, b) = (int32)((int64)a * (int64)b)
```

### 16.6 Random Number Generation

Used for session ID generation and multicast NAK backoff:
- Linux: `arc4random()` if available, else `/dev/urandom`
- Windows: `rand_s()`
- Fallback: `rand()`

---

## Appendix A: IPC Publication

IPC publications are significantly simpler than network publications because data never leaves shared memory:

| Aspect | Network | IPC |
|--------|---------|-----|
| Transport | UDP | Shared memory only |
| Sender position | `snd_pos` counter, advanced by sender thread | Not applicable |
| Flow control | SM-driven (`flow_control_strategy`) | Conductor manages `pub_lmt` from subscriber positions |
| Heartbeats | Zero-length DATA frames over UDP | Not needed |
| Retransmission | NAK → retransmit handler | Not needed (data stays in buffer) |
| Loss detection | Gap scanning | Not applicable |
| Setup handshake | SETUP/SM exchange | Not needed |

IPC `pub_lmt` uses a **trip limit** hysteresis to avoid updating on every cycle:
```
if new_limit > trip_limit:
    clean_buffer(min_sub_pos)
    pub_lmt = new_limit
    trip_limit = new_limit + trip_gain
```

---

## Appendix B: Port Management

For wildcard ports (port=0 in URI), the driver can use:
- **OS wildcard**: Let the OS assign an ephemeral port (default)
- **Port range**: Scan from `low_port` to `high_port` for an unused port

Configured via `AERON_SENDER_WILDCARD_PORT_RANGE` and `AERON_RECEIVER_WILDCARD_PORT_RANGE`.

---

## Appendix C: Name Resolution

The driver supports pluggable name resolution:

```
struct name_resolver:
    resolve_func(name, uri_param_name, is_re_resolution) → sockaddr_storage
    lookup_func(name) → resolved_name_string
    do_work_func() → work_count
    close_func()
```

Built-in resolvers:
- **default**: Standard DNS (`getaddrinfo`)
- **csv_table**: Static table from CSV configuration
- **driver**: Internal resolver for cluster use

Re-resolution occurs periodically (default: every 1 second) for long-lived endpoints when no activity is detected for 5 seconds.

---

## Appendix D: Error Handling

Thread-local error state using platform TLS:

```
struct per_thread_error:
    errcode      (int)
    offset       (size_t — current write position)
    errmsg[8192] (char — error message buffer)
```

- `SET_ERR(errcode, fmt, ...)`: Clear and set new error with `(errno) strerror` header
- `APPEND_ERR(fmt, ...)`: Append stack frame to existing error (error chaining)
- `aeron_errcode()`: Get current thread's error code
- `aeron_errmsg()`: Get current thread's error message

All allocation goes through `aeron_alloc(void **ptr, size_t size)` which always **zero-initializes** via `malloc + memset(0)`.

---

## Appendix E: Platform Abstraction Summary

For reimplementation, these are the platform-specific abstractions needed:

1. **Memory-mapped files**: mmap/munmap/msync (POSIX) or CreateFileMapping (Windows)
2. **Atomic operations**: CAS, fetch-and-add, acquire/release fences
3. **Threads**: pthread_create/join (POSIX) or CreateThread (Windows)
4. **Thread-local storage**: pthread_key_t (POSIX) or TlsAlloc (Windows)
5. **UDP sockets**: socket, bind, connect, sendmsg/sendmmsg, recvmsg/recvmmsg
6. **Multicast**: IP_ADD_MEMBERSHIP, IP_MULTICAST_IF, IP_MULTICAST_TTL
7. **Non-blocking I/O**: fcntl O_NONBLOCK (POSIX) or ioctlsocket FIONBIO (Windows)
8. **I/O polling**: epoll (Linux), poll (POSIX), WSAPoll (Windows)
9. **Network interfaces**: getifaddrs (POSIX) or GetAdaptersAddresses (Windows)
10. **Monotonic clock**: clock_gettime CLOCK_MONOTONIC or QueryPerformanceCounter
11. **Wall clock**: clock_gettime CLOCK_REALTIME or GetSystemTimeAsFileTime
12. **File system**: mkdir, stat, unlink, directory creation/deletion
13. **Dynamic loading**: dlopen/dlsym (POSIX) or LoadLibrary/GetProcAddress (Windows)
14. **Thread naming**: pthread_setname_np or SetThreadDescription
15. **Random numbers**: arc4random, /dev/urandom, or rand_s
16. **CPU pause**: `_mm_pause()` (x86) or `yield` (ARM)