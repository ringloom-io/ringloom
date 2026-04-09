//! Platform constants for the BRZ broker.
//!
//! Every numeric constant referenced by the ring buffer, metadata file,
//! wire protocol, and timing subsystems. Centralizing them here means the
//! rest of the codebase refers to symbolic names.

const std = @import("std");

// ── Buffer Size Constants ─────────────────────────────────────────────

/// Hardware cache line size on all target architectures (x86-64, ARM64).
pub const cache_line_length: usize = 64;

/// Padding to prevent false sharing between adjacent atomic fields.
/// Two cache lines — accounts for adjacent-line prefetch on Intel.
pub const cache_line_pad: usize = 128;

/// OS memory page size. Used for mmap alignment.
pub const page_size: usize = 4096;

/// Metadata header at the start of every broker/service .dat file.
/// Contains buffer lengths, service ID, node ID, PID, timestamps, etc.
pub const metadata_header_length: usize = 512;

/// Heartbeat timestamp starts at offset 256 within the header (cache-line aligned).
pub const heartbeat_offset_within_header: usize = 256;

/// The nextServiceId counter starts at offset 288 within the header (broker only).
pub const next_service_id_offset_within_header: usize = 288;

/// Flow control buffer length field offset within the header (broker only).
/// When 0, flow control region is absent (disabled).
pub const fc_buffer_length_offset: usize = 292;

/// Per-peer send counters length field offset within the header (broker only).
/// When 0, per-peer send counters region is absent (disabled).
pub const peer_send_counters_length_offset: usize = 296;

/// When blocking mode is enabled, three 128-byte cache-line-padded slots are
/// inserted between the header and the ring buffers.
pub const blocking_trailer_length: usize = 3 * cache_line_pad; // 384

/// MPSC ring buffer trailer: 6 × 128-byte cache-line-padded slots.
///   [0]   begin_pad          (128 bytes)
///   [128] tail_position      (i64 + 120 pad)
///   [256] head_cache         (i64 + 120 pad)
///   [384] head_position      (i64 + 120 pad)
///   [512] correlation_counter(i64 + 120 pad)
///   [640] consumer_heartbeat (i64 + 120 pad)
pub const ring_buffer_trailer_length: usize = 768;

/// Each record in the ring buffer has an 8-byte header:
///   i32 length      (negative = uncommitted, positive = committed)
///   i32 msg_type_id (≥1 valid, -1 = padding)
pub const ring_buffer_record_header_length: usize = 8;

/// Records in the ring buffer are aligned to 8 bytes.
pub const ring_buffer_alignment: usize = 8;

// ── Protocol Constants ────────────────────────────────────────────────

pub const frame_header_version: u8 = 0;

/// Sentinel msg_type_id written into padding records.
pub const padding_msg_type_id: i32 = -1;

/// Message type ID for control messages (registration, heartbeat, etc.).
pub const control_msg_type_id: i32 = 1;

/// Message type ID for application messages.
pub const application_msg_type_id: i32 = 2;

pub const flag_admin: u8 = 0x20;

// IPC message fragmentation flags (used by ring buffer message layer).
pub const flag_begin: u8 = 0x80;
pub const flag_end: u8 = 0x40;
pub const flag_unfragmented: u8 = 0xC0; // begin | end

pub const broker_service_id: i32 = 0;
pub const broker_service_name: []const u8 = "broker";

// ── TCP Protocol Constants ────────────────────────────────────────────

pub const protocol_version: u8 = 1;
pub const handshake_magic: u32 = 0x42525A00;
pub const heartbeat_template_id: u16 = 0xFFFF;
pub const direction_sender: u8 = 0x01;
pub const direction_receiver: u8 = 0x02;
pub const tcp_header_length: u32 = 24;
pub const tcp_handshake_length: u32 = 24;
pub const tcp_max_frame_length: u32 = 1_048_576; // 1 MB
pub const tcp_min_frame_length: u32 = 24; // header-only = heartbeat

// ── Timing Constants ──────────────────────────────────────────────────

/// Default TCP heartbeat interval.
pub const default_heartbeat_interval_ms: u64 = 500;

/// Default TCP heartbeat timeout.
pub const default_heartbeat_timeout_ms_tcp: u64 = 2000;

/// Default initial reconnect delay.
pub const default_reconnect_initial_delay_ms: u64 = 100;

/// Default maximum reconnect delay.
pub const default_reconnect_max_delay_ms: u64 = 1000;

/// Services write heartbeat timestamps at this interval.
pub const service_heartbeat_write_interval_ms: i64 = 1000;

/// Broker checks service heartbeats at this interval.
pub const service_heartbeat_check_interval_ms: i64 = 3000;

/// Service is considered dead after this timeout without a heartbeat.
pub const service_heartbeat_timeout_ms: i64 = 10000;

/// Default heartbeat timeout (ms) — if a service hasn't written a heartbeat
/// within this window, it is considered dead.
pub const default_svc_heartbeat_timeout_ms: i64 = 10_000;

/// How often the control loop checks for timed-out services.
pub const control_loop_timeout_check_interval_ns: i64 = 1 * std.time.ns_per_s;

/// Max commands drained from inter-thread command queues per duty cycle.
pub const command_drain_limit: u32 = 1;

/// Max control messages read per duty cycle.
pub const control_read_limit: u32 = 10;

/// Max messages read from the send ring buffer per duty cycle.
pub const send_batch_limit: u32 = 64;

/// Heartbeat interval in nanoseconds (derived from ms).
pub const default_heartbeat_interval_ns: i64 = @as(i64, @intCast(default_heartbeat_interval_ms)) * std.time.ns_per_ms;

/// Per-peer TCP write budget per duty cycle (round-robin fairness).
pub const write_budget_per_peer: u32 = 16;

/// Per-peer TCP read budget per duty cycle (round-robin fairness).
pub const read_budget_per_peer: u32 = 16;

// ── Default Configuration Values ──────────────────────────────────────

pub const default_control_buffer_length: usize = 64 * 1024; // 64 KB
pub const default_send_buffer_length: usize = 1024 * 1024; // 1 MB
pub const default_messages_buffer_length: usize = 1024 * 1024; // 1 MB
pub const default_tcp_send_buffer_size: u32 = 262_144; // 256 KB
pub const default_tcp_recv_buffer_size: u32 = 262_144; // 256 KB
pub const default_max_frame_length: u32 = 65_536; // 64 KB
pub const default_peer_write_queue_capacity: u32 = 4_096;
pub const default_counter_values_buffer_length: usize = 64 * 1024; // 64 KB
pub const default_error_log_buffer_length: usize = 256 * 1024; // 256 KB
pub const default_max_services: u32 = 256;
pub const default_max_peers: u32 = 16;

/// Default storage path for metadata files.
/// On Linux this is tmpfs (RAM-backed), giving shared-memory performance.
pub const default_storage_path: []const u8 = "/dev/shm";

/// Subdirectory under the group directory where service .dat files live.
pub const services_directory: []const u8 = "services";

// ── Helper Functions ──────────────────────────────────────────────────

/// Align `value` up to the nearest multiple of `alignment`.
/// `alignment` must be a power of two.
pub fn alignUp(value: usize, alignment: usize) usize {
    return (value + (alignment - 1)) & ~(alignment - 1);
}

/// Returns true if `value` is a positive power of two.
pub fn isPowerOfTwo(value: usize) bool {
    return value > 0 and (value & (value - 1)) == 0;
}

/// Returns true if `value` is properly aligned to `alignment`.
pub fn isAligned(value: usize, alignment: usize) bool {
    return (value & (alignment - 1)) == 0;
}

// ── Tests ─────────────────────────────────────────────────────────────

test "alignUp basics" {
    const expect = std.testing.expect;
    try expect(alignUp(0, 8) == 0);
    try expect(alignUp(1, 8) == 8);
    try expect(alignUp(7, 8) == 8);
    try expect(alignUp(8, 8) == 8);
    try expect(alignUp(9, 8) == 16);
    try expect(alignUp(4095, 4096) == 4096);
    try expect(alignUp(4096, 4096) == 4096);
}

test "isPowerOfTwo" {
    const expect = std.testing.expect;
    try expect(!isPowerOfTwo(0));
    try expect(isPowerOfTwo(1));
    try expect(isPowerOfTwo(2));
    try expect(!isPowerOfTwo(3));
    try expect(isPowerOfTwo(4));
    try expect(isPowerOfTwo(1024));
    try expect(!isPowerOfTwo(1023));
    try expect(isPowerOfTwo(1 << 20));
}

test "isAligned" {
    const expect = std.testing.expect;
    try expect(isAligned(0, 8));
    try expect(isAligned(8, 8));
    try expect(!isAligned(7, 8));
    try expect(isAligned(4096, 4096));
    try expect(!isAligned(4095, 4096));
}
