//! Memory layout constants for the RingLoom broker.
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

/// Current metadata layout version.
pub const metadata_version: i32 = platform_constants.metadata_version;

/// Heartbeat timestamp starts at offset 256 within the header (cache-line aligned).
pub const heartbeat_offset_within_header: usize = platform_constants.heartbeat_offset_within_header;

/// The nextServiceId counter starts at offset 288 within the header (broker only).
pub const next_service_id_offset_within_header: usize = platform_constants.next_service_id_offset_within_header;

/// Flow control buffer length field offset within the header (broker only).
pub const fc_buffer_length_offset: usize = platform_constants.fc_buffer_length_offset;

/// Reserved broker metadata header slot retained for v2 layout stability.
pub const broker_reserved_length_offset: usize = platform_constants.broker_reserved_length_offset;

/// Generic monitoring tail fields shared by broker and service metadata files.
pub const metadata_monitoring_version: i32 = platform_constants.metadata_monitoring_version;
pub const counter_values_length_offset: usize = platform_constants.counter_values_length_offset;
pub const counter_metadata_length_offset: usize = platform_constants.counter_metadata_length_offset;
pub const error_log_length_offset: usize = platform_constants.error_log_length_offset;
pub const monitoring_tail_offset_offset: usize = platform_constants.monitoring_tail_offset_offset;
pub const monitoring_tail_length_offset: usize = platform_constants.monitoring_tail_length_offset;
pub const metadata_monitoring_version_offset: usize = platform_constants.metadata_monitoring_version_offset;
pub const aeron_discovery_region_offset_offset: usize = platform_constants.aeron_discovery_region_offset_offset;
pub const aeron_discovery_region_length_offset: usize = platform_constants.aeron_discovery_region_length_offset;

/// When blocking mode is enabled, three 128-byte cache-line-padded slots are
/// inserted between the header and the ring buffers.
pub const blocking_trailer_length: usize = platform_constants.blocking_trailer_length;

/// Ring buffer trailer length.
pub const ring_buffer_trailer_length: usize = platform_constants.ring_buffer_trailer_length;

/// Record alignment within the ring buffer.
pub const ring_buffer_alignment: usize = platform_constants.ring_buffer_alignment;

/// Record header: i32 length + i32 msg_type_id.
pub const ring_buffer_record_header_length: usize = platform_constants.ring_buffer_record_header_length;

/// Default buffer sizes.
pub const default_control_buffer_length: usize = platform_constants.default_control_buffer_length;
pub const default_messages_buffer_length: usize = platform_constants.default_messages_buffer_length;
pub const default_counter_values_buffer_length: usize = platform_constants.default_counter_values_buffer_length;
pub const default_error_log_buffer_length: usize = platform_constants.default_error_log_buffer_length;
pub const max_aeron_directory_length: usize = platform_constants.max_aeron_directory_length;
pub const max_aeron_channel_length: usize = platform_constants.max_aeron_channel_length;
pub const default_max_peers: usize = platform_constants.default_max_peers;

/// Broker is always service ID 0.
pub const broker_service_id: i32 = platform_constants.broker_service_id;

/// Broker service name.
pub const broker_service_name: []const u8 = platform_constants.broker_service_name;

/// Memory page size for file alignment.
pub const page_size: usize = platform_constants.page_size;

/// Default heartbeat timeout (ms).
pub const default_svc_heartbeat_timeout_ms: i64 = platform_constants.default_svc_heartbeat_timeout_ms;

/// Default storage path.
pub const default_storage_path: []const u8 = platform_constants.default_storage_path;

/// Message type ID for control messages.
pub const control_msg_type_id: i32 = platform_constants.control_msg_type_id;

/// Message type ID for application messages.
pub const application_msg_type_id: i32 = platform_constants.application_msg_type_id;

/// Internal ring-buffer message type for MessageHeader-wrapped application records.
pub const message_envelope_msg_type_id: i32 = platform_constants.message_envelope_msg_type_id;

/// Services subdirectory.
pub const services_directory: []const u8 = platform_constants.services_directory;

// ── Protocol Flags ────────────────────────────────────────────────────

/// Admin message flag.
pub const flag_admin: u8 = platform_constants.flag_admin;

/// Topic publish frame flag (payload begins with a TopicPublishHeader).
pub const flag_topic: u8 = platform_constants.flag_topic;

/// IPC message fragmentation flags.
pub const flag_begin: u8 = platform_constants.flag_begin;
pub const flag_end: u8 = platform_constants.flag_end;
pub const flag_unfragmented: u8 = platform_constants.flag_unfragmented;

// ── Timing & Limits ──────────────────────────────────────────────────

/// Services write heartbeat timestamps at this interval.
pub const service_heartbeat_write_interval_ms: i64 = platform_constants.service_heartbeat_write_interval_ms;

/// Max control messages read per duty cycle.
pub const control_read_limit: u32 = platform_constants.control_read_limit;

// ── Helper Functions (re-exported) ────────────────────────────────────

pub const isPowerOfTwo = platform_constants.isPowerOfTwo;
pub const alignUp = platform_constants.alignUp;
pub const isAligned = platform_constants.isAligned;

// ── Tests ─────────────────────────────────────────────────────────────

test "memory constants are consistent with platform constants" {
    try std.testing.expectEqual(platform_constants.cache_line_pad, cache_line_pad);
    try std.testing.expectEqual(platform_constants.metadata_header_length, metadata_header_length);
    try std.testing.expectEqual(@as(usize, 384), blocking_trailer_length);
}
