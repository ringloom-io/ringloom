//! Memory layout constants for the BRZ broker.
//!
//! Defines byte-level layout constants for metadata files, ring buffers,
//! and receive log buffers. These are used by the memory subsystem to
//! compute offsets, validate sizes, and overlay packed structs.

const std = @import("std");

// ── Import shared platform constants ──────────────────────────────────
const platform_constants = @import("../platform/constants.zig");

/// Hardware cache line size.
pub const cache_line_length: usize = platform_constants.cache_line_length;

/// Two cache lines — minimum padding between independently-written atomics.
pub const cache_line_pad: usize = platform_constants.cache_line_pad;

/// Metadata header occupies the first 512 bytes of every metadata file.
pub const metadata_header_length: usize = platform_constants.metadata_header_length;

/// Heartbeat timestamp starts at offset 256 within the header (cache-line aligned).
pub const heartbeat_offset_within_header: usize = platform_constants.heartbeat_offset_within_header;

/// The nextServiceId counter starts at offset 288 within the header (broker only).
pub const next_service_id_offset_within_header: usize = platform_constants.next_service_id_offset_within_header;

/// When blocking mode is enabled, three 128-byte cache-line-padded slots are
/// inserted between the header and the ring buffers.
pub const blocking_trailer_length: usize = platform_constants.blocking_trailer_length;

/// Receive log buffer metadata (tail_position + rebuild_position + padding).
pub const recv_log_metadata_length: usize = platform_constants.recv_log_metadata_length;

/// Ring buffer trailer length.
pub const ring_buffer_trailer_length: usize = platform_constants.ring_buffer_trailer_length;

/// Record alignment within the ring buffer.
pub const ring_buffer_alignment: usize = platform_constants.ring_buffer_alignment;

/// Record header: i32 length + i32 msg_type_id.
pub const ring_buffer_record_header_length: usize = platform_constants.ring_buffer_record_header_length;

/// Default buffer sizes.
pub const default_control_buffer_length: usize = platform_constants.default_control_buffer_length;
pub const default_send_buffer_length: usize = platform_constants.default_send_buffer_length;
pub const default_messages_buffer_length: usize = platform_constants.default_messages_buffer_length;
pub const default_recv_log_buffer_length: usize = platform_constants.default_recv_log_buffer_length;

/// Broker is always service ID 0.
pub const broker_service_id: i32 = platform_constants.broker_service_id;

/// Broker service name.
pub const broker_service_name: []const u8 = platform_constants.broker_service_name;

/// Memory page size for file alignment.
pub const page_size: usize = platform_constants.page_size;

/// Default heartbeat timeout (ms).
pub const default_heartbeat_timeout_ms: i64 = platform_constants.default_heartbeat_timeout_ms;

/// Default storage path.
pub const default_storage_path: []const u8 = platform_constants.default_storage_path;

/// Services subdirectory.
pub const services_directory: []const u8 = platform_constants.services_directory;

// ── Helper Functions (re-exported) ────────────────────────────────────

pub const isPowerOfTwo = platform_constants.isPowerOfTwo;
pub const alignUp = platform_constants.alignUp;
pub const isAligned = platform_constants.isAligned;

// ── Tests ─────────────────────────────────────────────────────────────

test "memory constants are consistent with platform constants" {
    try std.testing.expectEqual(platform_constants.cache_line_pad, cache_line_pad);
    try std.testing.expectEqual(platform_constants.metadata_header_length, metadata_header_length);
    try std.testing.expectEqual(@as(usize, 384), blocking_trailer_length);
    try std.testing.expectEqual(@as(usize, 256), recv_log_metadata_length);
}
