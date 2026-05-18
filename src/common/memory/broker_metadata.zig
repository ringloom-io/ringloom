//! Broker metadata file layout for the RingLoom broker.
//!
//! The broker's metadata file is memory-mapped on /dev/shm and shared with
//! all local services. V2 metadata contains the broker control ring buffer
//! (services → broker), Aeron discovery information, and monitoring regions.

const std = @import("std");
const constants = @import("constants.zig");
const platform = @import("../platform.zig");
const FlowControlRegion = @import("flow_control.zig").FlowControlRegion;
const counters = @import("../concurrent/counters.zig");

/// Overlay for the immutable broker metadata header prefix.
/// Read/written once at creation; immutable after that (except volatile fields).
pub const BrokerMetadataHeader = extern struct {
    metadata_version: i32,
    control_buffer_length: i32,
    messages_buffer_length: i32,
    service_id: i32,
    node_id: i16,
    _padding0: i16 = 0,
    pid: i64,
    start_timestamp_ms: i64,

    comptime {
        // The fixed fields must stay within the immutable header area.
        std.debug.assert(@sizeOf(BrokerMetadataHeader) == 40);
        // Ensure the struct doesn't grow past the volatile field offsets.
        std.debug.assert(@sizeOf(BrokerMetadataHeader) <= platform.constants.heartbeat_offset_within_header);
        std.debug.assert(@sizeOf(BrokerMetadataHeader) <= platform.constants.next_service_id_offset_within_header);
        std.debug.assert(@sizeOf(BrokerMetadataHeader) <= platform.constants.fc_buffer_length_offset);
        std.debug.assert(@sizeOf(BrokerMetadataHeader) <= platform.constants.broker_reserved_length_offset);
        std.debug.assert(platform.constants.aeron_discovery_region_offset_offset >= @sizeOf(BrokerMetadataHeader));
        std.debug.assert(platform.constants.aeron_discovery_region_length_offset >= @sizeOf(BrokerMetadataHeader));
    }
};

pub const BrokerAeronPeerEndpoint = extern struct {
    node_id: u8 = 0,
    _reserved0: [3]u8 = [_]u8{0} ** 3,
    data_stream_id: i32 = 0,
    data_channel_length: u16 = 0,
    _reserved1: [6]u8 = [_]u8{0} ** 6,
    data_channel: [constants.max_aeron_channel_length]u8 = [_]u8{0} ** constants.max_aeron_channel_length,

    pub fn dataChannel(self: *const BrokerAeronPeerEndpoint) []const u8 {
        const length = @min(@as(usize, @intCast(self.data_channel_length)), self.data_channel.len);
        return self.data_channel[0..length];
    }
};

pub const BrokerAeronPeerConfig = struct {
    node_id: u8,
    data_stream_id: i32,
    data_channel: []const u8,
};

pub const BrokerAeronDiscovery = extern struct {
    broker_ingress_stream_id: i32 = 0,
    admin_stream_base: i32 = 0,
    data_stream_base: i32 = 0,
    peer_count: u8 = 0,
    _reserved0: [3]u8 = [_]u8{0} ** 3,
    aeron_directory_length: u16 = 0,
    local_data_channel_length: u16 = 0,
    local_admin_channel_length: u16 = 0,
    _reserved1: u16 = 0,
    aeron_directory: [constants.max_aeron_directory_length]u8 = [_]u8{0} ** constants.max_aeron_directory_length,
    local_data_channel: [constants.max_aeron_channel_length]u8 = [_]u8{0} ** constants.max_aeron_channel_length,
    local_admin_channel: [constants.max_aeron_channel_length]u8 = [_]u8{0} ** constants.max_aeron_channel_length,
    peer_data_channels: [constants.default_max_peers]BrokerAeronPeerEndpoint = [_]BrokerAeronPeerEndpoint{.{}} ** constants.default_max_peers,

    pub fn directory(self: *const BrokerAeronDiscovery) []const u8 {
        return self.aeron_directory[0..@as(usize, @intCast(self.aeron_directory_length))];
    }

    pub fn dataChannel(self: *const BrokerAeronDiscovery) []const u8 {
        return self.local_data_channel[0..@as(usize, @intCast(self.local_data_channel_length))];
    }

    pub fn adminChannel(self: *const BrokerAeronDiscovery) []const u8 {
        return self.local_admin_channel[0..@as(usize, @intCast(self.local_admin_channel_length))];
    }

    pub fn peers(self: *const BrokerAeronDiscovery) []const BrokerAeronPeerEndpoint {
        const count = @min(@as(usize, @intCast(self.peer_count)), self.peer_data_channels.len);
        return self.peer_data_channels[0..count];
    }

    pub fn peerByNodeId(self: *const BrokerAeronDiscovery, node_id: u8) ?*const BrokerAeronPeerEndpoint {
        for (self.peers()) |*peer| {
            if (peer.node_id == node_id) return peer;
        }
        return null;
    }
};

pub const BrokerMetadataFile = struct {
    /// The full mmap'd region.
    mapped_bytes: []align(constants.page_size) u8,

    /// Pointer to the header overlay (first 32 bytes).
    header: *BrokerMetadataHeader,

    /// Byte slice covering the control ring buffer region.
    control_buffer: []u8,

    /// V2 Aeron discovery region.
    aeron_discovery: *BrokerAeronDiscovery,

    /// Flow control counters region (null if disabled).
    fc_region: ?FlowControlRegion = null,

    /// Generic counter values region, stored in the metadata monitoring tail.
    counter_values_buffer: []align(constants.cache_line_pad) u8,

    /// Generic counter metadata region, stored in the metadata monitoring tail.
    counter_metadata_buffer: []u8,

    /// Deduplicating error-log region, stored in the metadata monitoring tail.
    error_log_buffer: []u8,

    /// File descriptor (kept open for the lifetime of the mapping).
    fd: std.posix.fd_t,

    const Self = @This();

    // ── Construction ──────────────────────────────────────────────────

    /// Options for flow control region allocation (optional).
    pub const FlowControlOptions = struct {
        fc_max_entries: u32 = 0,
        counter_values_buffer_length: usize = constants.default_counter_values_buffer_length,
        counter_metadata_buffer_length: usize = 0,
        error_log_buffer_length: usize = constants.default_error_log_buffer_length,
        aeron_directory: []const u8 = "",
        broker_ingress_stream_id: i32 = 0,
        admin_stream_base: i32 = 0,
        data_stream_base: i32 = 0,
        local_data_channel: []const u8 = "",
        local_admin_channel: []const u8 = "",
        peer_data_channels: []const BrokerAeronPeerConfig = &.{},
    };

    /// Create and initialize a new broker metadata file.
    ///
    /// - `storage_path`: e.g. "/dev/shm"
    /// - `group`: e.g. "default"
    /// - `node_id`: this broker's node identifier
    /// - `control_buffer_length`: must be a power of two
    /// - `messages_buffer_length`: ignored by v2 metadata; retained for callers
    ///   that still size the transitional process-local sender ring.
    /// - `fc_options`: optional flow control region sizes (0 = disabled)
    pub fn create(
        storage_path: []const u8,
        group: []const u8,
        node_id: i16,
        control_buffer_length: usize,
        messages_buffer_length: usize,
    ) !BrokerMetadataFile {
        return createWithFlowControl(
            storage_path,
            group,
            node_id,
            control_buffer_length,
            messages_buffer_length,
            .{},
        );
    }

    /// Create and initialize a new broker metadata file with flow control.
    pub fn createWithFlowControl(
        storage_path: []const u8,
        group: []const u8,
        node_id: i16,
        control_buffer_length: usize,
        messages_buffer_length: usize,
        fc_options: FlowControlOptions,
    ) !BrokerMetadataFile {
        if (!constants.isPowerOfTwo(control_buffer_length))
            return error.ControlBufferNotPowerOfTwo;
        _ = messages_buffer_length;
        try validateAeronOptions(fc_options);

        // The ring buffer needs data_capacity + trailer. The caller specifies the
        // data capacity (must be power-of-two); we add the trailer here.
        const trailer = constants.ring_buffer_trailer_length;
        const ctrl_region = control_buffer_length + trailer;
        const aeron_region_len = constants.alignUp(@sizeOf(BrokerAeronDiscovery), constants.cache_line_pad);

        // Compute flow control region sizes.
        const fc_buf_len: usize = if (fc_options.fc_max_entries > 0)
            FlowControlRegion.regionSize(fc_options.fc_max_entries)
        else
            0;
        const counter_values_len = alignedCounterValuesLength(fc_options.counter_values_buffer_length);
        const counter_metadata_len = counterMetadataLength(
            fc_options.counter_values_buffer_length,
            fc_options.counter_metadata_buffer_length,
        );
        const error_log_len = constants.alignUp(fc_options.error_log_buffer_length, @sizeOf(i64));

        const aeron_region_offset = constants.metadata_header_length;
        const control_region_offset = aeron_region_offset + aeron_region_len;
        const base_size = control_region_offset + ctrl_region + fc_buf_len;
        const monitoring_tail_offset = constants.alignUp(base_size, constants.cache_line_pad);
        const monitoring_tail_len = counter_values_len + counter_metadata_len + error_log_len;

        const total_size = constants.alignUp(
            monitoring_tail_offset + monitoring_tail_len,
            constants.page_size,
        );

        // Build path: <storage_path>/<group>/services/broker_<node_id>.dat
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const path = try buildBrokerPath(&path_buf, storage_path, group, node_id);

        // Ensure parent directory exists.
        try ensureDirectoryExists(path);

        // Create file, truncate to total_size, mmap.
        const fd = try platform.createFile(path);
        errdefer platform.closeFd(fd);
        try platform.ftruncate(fd, total_size);

        const mapped = try platform.mmap(fd, total_size);
        errdefer platform.munmap(mapped);

        // Zero-fill.
        @memset(mapped, 0);

        const fc_region_offset = control_region_offset + ctrl_region;
        const counter_values_offset = monitoring_tail_offset;
        const counter_metadata_offset = counter_values_offset + counter_values_len;
        const error_log_offset = counter_metadata_offset + counter_metadata_len;

        // Initialize FC regions if requested.
        var fc_region: ?FlowControlRegion = null;
        if (fc_buf_len > 0) {
            const fc_slice = mapped[fc_region_offset..][0..fc_buf_len];
            fc_region = FlowControlRegion.initNew(fc_slice, fc_options.fc_max_entries) catch
                return error.FlowControlInitFailed;
        }

        var self = BrokerMetadataFile{
            .mapped_bytes = mapped,
            .header = @ptrCast(@alignCast(mapped.ptr)),
            .control_buffer = mapped[control_region_offset..][0..ctrl_region],
            .aeron_discovery = @ptrCast(@alignCast(mapped.ptr + aeron_region_offset)),
            .fc_region = fc_region,
            .counter_values_buffer = @alignCast(mapped[counter_values_offset..][0..counter_values_len]),
            .counter_metadata_buffer = mapped[counter_metadata_offset..][0..counter_metadata_len],
            .error_log_buffer = mapped[error_log_offset..][0..error_log_len],
            .fd = fd,
        };

        // Write the immutable header fields — store data capacity (without trailer).
        self.header.metadata_version = constants.metadata_version;
        self.header.control_buffer_length = @intCast(control_buffer_length);
        self.header.messages_buffer_length = 0;
        self.header.service_id = constants.broker_service_id;
        self.header.node_id = node_id;
        self.header.pid = platform.getPid();
        self.header.start_timestamp_ms = platform.Clock.epochMillis();
        self.aeron_discovery.* = .{
            .broker_ingress_stream_id = fc_options.broker_ingress_stream_id,
            .admin_stream_base = fc_options.admin_stream_base,
            .data_stream_base = fc_options.data_stream_base,
        };
        copyAeronDirectory(
            &self.aeron_discovery.aeron_directory,
            &self.aeron_discovery.aeron_directory_length,
            fc_options.aeron_directory,
        );
        copyAeronChannel(
            &self.aeron_discovery.local_data_channel,
            &self.aeron_discovery.local_data_channel_length,
            fc_options.local_data_channel,
        );
        copyAeronChannel(
            &self.aeron_discovery.local_admin_channel,
            &self.aeron_discovery.local_admin_channel_length,
            fc_options.local_admin_channel,
        );
        copyAeronPeerChannels(self.aeron_discovery, fc_options.peer_data_channels);

        // Store FC region sizes in the header (at fixed offsets beyond 32-byte struct).
        self.storeFcBufferLength(@intCast(fc_buf_len));
        self.storeBrokerReservedLength(0);
        self.storeMetadataMonitoringVersion(constants.metadata_monitoring_version);
        self.storeCounterValuesBufferLength(@intCast(counter_values_len));
        self.storeCounterMetadataBufferLength(@intCast(counter_metadata_len));
        self.storeErrorLogBufferLength(@intCast(error_log_len));
        self.storeMonitoringTailOffset(@intCast(monitoring_tail_offset));
        self.storeMonitoringTailLength(@intCast(monitoring_tail_len));
        self.storeAeronDiscoveryRegionOffset(@intCast(aeron_region_offset));
        self.storeAeronDiscoveryRegionLength(@intCast(aeron_region_len));

        // Initialize next_service_id to 1 (0 is reserved for broker).
        self.storeNextServiceId(1);

        // Write initial heartbeat.
        self.storeHeartbeat(platform.Clock.epochMillis());

        return self;
    }

    /// Open an existing broker metadata file (read-write).
    /// Validates that the header fields are internally consistent.
    /// Detects flow control regions if present (backward compatible with older files).
    pub fn open(
        storage_path: []const u8,
        group: []const u8,
        node_id: i16,
    ) !BrokerMetadataFile {
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const path = try buildBrokerPath(&path_buf, storage_path, group, node_id);

        const fd = try platform.openFile(path);
        errdefer platform.closeFd(fd);

        const file_size = try platform.fileSize(fd);
        const mapped = try platform.mmap(fd, file_size);
        errdefer platform.munmap(mapped);

        if (file_size < constants.metadata_header_length)
            return error.FileTooSmall;

        const header: *BrokerMetadataHeader = @ptrCast(@alignCast(mapped.ptr));
        if (header.metadata_version != constants.metadata_version)
            return error.UnsupportedMetadataVersion;

        const ctrl_len: usize = @intCast(header.control_buffer_length);
        const msgs_len: usize = @intCast(header.messages_buffer_length);

        if (!constants.isPowerOfTwo(ctrl_len))
            return error.ControlBufferNotPowerOfTwo;
        if (msgs_len != 0)
            return error.UnsupportedMetadataVersion;

        const trailer = constants.ring_buffer_trailer_length;
        const ctrl_region = ctrl_len + trailer;

        // Read FC/monitoring region lengths from header.
        var self = BrokerMetadataFile{
            .mapped_bytes = mapped,
            .header = header,
            .control_buffer = undefined,
            .aeron_discovery = undefined,
            .counter_values_buffer = undefined,
            .counter_metadata_buffer = undefined,
            .error_log_buffer = undefined,
            .fd = fd,
        };

        const aeron_region_offset: usize = @intCast(self.loadAeronDiscoveryRegionOffset());
        const aeron_region_len: usize = @intCast(self.loadAeronDiscoveryRegionLength());
        if (aeron_region_offset < constants.metadata_header_length)
            return error.FileSizeMismatch;
        if (aeron_region_len < @sizeOf(BrokerAeronDiscovery))
            return error.FileSizeMismatch;

        const control_region_offset = aeron_region_offset + aeron_region_len;
        const base_size = control_region_offset + ctrl_region;
        const fc_buf_len: usize = @intCast(self.loadFcBufferLength());
        const counter_values_len: usize = @intCast(self.loadCounterValuesBufferLength());
        const counter_metadata_len: usize = @intCast(self.loadCounterMetadataBufferLength());
        const error_log_len: usize = @intCast(self.loadErrorLogBufferLength());
        const monitoring_tail_offset: usize = @intCast(self.loadMonitoringTailOffset());
        const monitoring_tail_len: usize = @intCast(self.loadMonitoringTailLength());

        const fc_region_offset = base_size;
        const counter_values_offset = monitoring_tail_offset;
        const counter_metadata_offset = counter_values_offset + counter_values_len;
        const error_log_offset = counter_metadata_offset + counter_metadata_len;

        if (monitoring_tail_len != counter_values_len + counter_metadata_len + error_log_len)
            return error.FileSizeMismatch;
        if (monitoring_tail_offset < fc_region_offset + fc_buf_len)
            return error.FileSizeMismatch;
        if (counter_values_len % counters.counter_value_length != 0)
            return error.FileSizeMismatch;
        if (counter_metadata_len % counters.counter_metadata_length != 0)
            return error.FileSizeMismatch;

        // Validate that the file is large enough for all regions.
        const total_expected = constants.alignUp(
            monitoring_tail_offset + monitoring_tail_len,
            constants.page_size,
        );
        if (file_size < total_expected)
            return error.FileSizeMismatch;

        // Initialize FC region overlays.
        if (fc_buf_len > 0) {
            const fc_slice = mapped[fc_region_offset..][0..fc_buf_len];
            self.fc_region = FlowControlRegion.initExisting(fc_slice) catch
                return error.FlowControlInitFailed;
        }

        self.control_buffer = mapped[control_region_offset..][0..ctrl_region];
        self.aeron_discovery = @ptrCast(@alignCast(mapped.ptr + aeron_region_offset));
        self.counter_values_buffer = @alignCast(mapped[counter_values_offset..][0..counter_values_len]);
        self.counter_metadata_buffer = mapped[counter_metadata_offset..][0..counter_metadata_len];
        self.error_log_buffer = mapped[error_log_offset..][0..error_log_len];

        return self;
    }

    // ── Atomic Accessors ──────────────────────────────────────────────

    /// Atomically load the broker's heartbeat timestamp.
    pub fn loadHeartbeat(self: *const BrokerMetadataFile) i64 {
        const ptr = self.heartbeatPtr();
        return @atomicLoad(i64, ptr, .acquire);
    }

    /// Atomically store the broker's heartbeat timestamp.
    pub fn storeHeartbeat(self: *BrokerMetadataFile, time_ms: i64) void {
        const ptr = self.heartbeatPtr();
        @atomicStore(i64, ptr, time_ms, .release);
    }

    /// Atomically load the current next_service_id value.
    pub fn loadNextServiceId(self: *const BrokerMetadataFile) i32 {
        const ptr = self.nextServiceIdPtr();
        return @atomicLoad(i32, ptr, .acquire);
    }

    /// Atomically store the next_service_id value (used during initialization
    /// or after scanning existing services).
    pub fn storeNextServiceId(self: *BrokerMetadataFile, value: i32) void {
        const ptr = self.nextServiceIdPtr();
        @atomicStore(i32, ptr, value, .release);
    }

    /// Atomically increment next_service_id and return the new value.
    /// This is the primary method used when assigning IDs to new services.
    pub fn incrementAndGetNextServiceId(self: *BrokerMetadataFile) i32 {
        const ptr = self.nextServiceIdPtr();
        const prev = @atomicRmw(i32, ptr, .Add, @as(i32, 1), .acq_rel);
        return prev + 1;
    }

    /// Load the flow control buffer region length from the header (0 = absent).
    pub fn loadFcBufferLength(self: *const BrokerMetadataFile) i32 {
        const ptr = self.fcBufferLengthPtr();
        return @atomicLoad(i32, ptr, .acquire);
    }

    /// Store the flow control buffer region length in the header.
    pub fn storeFcBufferLength(self: *BrokerMetadataFile, value: i32) void {
        const ptr = self.fcBufferLengthPtr();
        @atomicStore(i32, ptr, value, .release);
    }

    /// Load the reserved v2 broker metadata length slot.
    pub fn loadBrokerReservedLength(self: *const BrokerMetadataFile) i32 {
        const ptr = self.brokerReservedLengthPtr();
        return @atomicLoad(i32, ptr, .acquire);
    }

    /// Store the reserved v2 broker metadata length slot.
    pub fn storeBrokerReservedLength(self: *BrokerMetadataFile, value: i32) void {
        const ptr = self.brokerReservedLengthPtr();
        @atomicStore(i32, ptr, value, .release);
    }

    pub fn loadMetadataMonitoringVersion(self: *const BrokerMetadataFile) i32 {
        return @atomicLoad(i32, self.metadataMonitoringVersionPtr(), .acquire);
    }

    pub fn storeMetadataMonitoringVersion(self: *BrokerMetadataFile, value: i32) void {
        @atomicStore(i32, self.metadataMonitoringVersionPtr(), value, .release);
    }

    pub fn loadCounterValuesBufferLength(self: *const BrokerMetadataFile) i32 {
        return @atomicLoad(i32, self.counterValuesBufferLengthPtr(), .acquire);
    }

    pub fn storeCounterValuesBufferLength(self: *BrokerMetadataFile, value: i32) void {
        @atomicStore(i32, self.counterValuesBufferLengthPtr(), value, .release);
    }

    pub fn loadCounterMetadataBufferLength(self: *const BrokerMetadataFile) i32 {
        return @atomicLoad(i32, self.counterMetadataBufferLengthPtr(), .acquire);
    }

    pub fn storeCounterMetadataBufferLength(self: *BrokerMetadataFile, value: i32) void {
        @atomicStore(i32, self.counterMetadataBufferLengthPtr(), value, .release);
    }

    pub fn loadErrorLogBufferLength(self: *const BrokerMetadataFile) i32 {
        return @atomicLoad(i32, self.errorLogBufferLengthPtr(), .acquire);
    }

    pub fn storeErrorLogBufferLength(self: *BrokerMetadataFile, value: i32) void {
        @atomicStore(i32, self.errorLogBufferLengthPtr(), value, .release);
    }

    pub fn loadMonitoringTailOffset(self: *const BrokerMetadataFile) i64 {
        return @atomicLoad(i64, self.monitoringTailOffsetPtr(), .acquire);
    }

    pub fn storeMonitoringTailOffset(self: *BrokerMetadataFile, value: i64) void {
        @atomicStore(i64, self.monitoringTailOffsetPtr(), value, .release);
    }

    pub fn loadMonitoringTailLength(self: *const BrokerMetadataFile) i64 {
        return @atomicLoad(i64, self.monitoringTailLengthPtr(), .acquire);
    }

    pub fn storeMonitoringTailLength(self: *BrokerMetadataFile, value: i64) void {
        @atomicStore(i64, self.monitoringTailLengthPtr(), value, .release);
    }

    pub fn loadAeronDiscoveryRegionOffset(self: *const BrokerMetadataFile) i64 {
        return @atomicLoad(i64, self.aeronDiscoveryRegionOffsetPtr(), .acquire);
    }

    pub fn storeAeronDiscoveryRegionOffset(self: *BrokerMetadataFile, value: i64) void {
        @atomicStore(i64, self.aeronDiscoveryRegionOffsetPtr(), value, .release);
    }

    pub fn loadAeronDiscoveryRegionLength(self: *const BrokerMetadataFile) i64 {
        return @atomicLoad(i64, self.aeronDiscoveryRegionLengthPtr(), .acquire);
    }

    pub fn storeAeronDiscoveryRegionLength(self: *BrokerMetadataFile, value: i64) void {
        @atomicStore(i64, self.aeronDiscoveryRegionLengthPtr(), value, .release);
    }

    // ── Flow Control Accessors ───────────────────────────────────────

    /// Returns the flow control region, or null if not present.
    pub fn getFlowControlRegion(self: *const BrokerMetadataFile) ?FlowControlRegion {
        return self.fc_region;
    }

    // ── Buffer Accessors ──────────────────────────────────────────────

    /// Returns the byte slice backing the control ring buffer.
    pub fn getControlBuffer(self: *const BrokerMetadataFile) []u8 {
        return self.control_buffer;
    }

    pub fn getAeronDiscovery(self: *const BrokerMetadataFile) *const BrokerAeronDiscovery {
        return self.aeron_discovery;
    }

    pub fn getMutableAeronDiscovery(self: *BrokerMetadataFile) *BrokerAeronDiscovery {
        return self.aeron_discovery;
    }

    pub fn getCounterValuesBuffer(self: *const BrokerMetadataFile) []align(constants.cache_line_pad) u8 {
        return self.counter_values_buffer;
    }

    pub fn getCounterMetadataBuffer(self: *const BrokerMetadataFile) []u8 {
        return self.counter_metadata_buffer;
    }

    pub fn getErrorLogBuffer(self: *const BrokerMetadataFile) []u8 {
        return self.error_log_buffer;
    }

    // ── Cleanup ───────────────────────────────────────────────────────

    /// Unmap the file and close the file descriptor.
    pub fn close(self: *BrokerMetadataFile) void {
        platform.munmap(self.mapped_bytes);
        platform.closeFd(self.fd);
        self.* = undefined;
    }

    // ── Internal ──────────────────────────────────────────────────────

    fn heartbeatPtr(self: *const BrokerMetadataFile) *volatile i64 {
        const base: [*]u8 = self.mapped_bytes.ptr;
        const offset = constants.heartbeat_offset_within_header;
        return @ptrCast(@alignCast(base + offset));
    }

    fn nextServiceIdPtr(self: *const BrokerMetadataFile) *volatile i32 {
        const base: [*]u8 = self.mapped_bytes.ptr;
        const offset = constants.next_service_id_offset_within_header;
        return @ptrCast(@alignCast(base + offset));
    }

    fn fcBufferLengthPtr(self: *const BrokerMetadataFile) *volatile i32 {
        const base: [*]u8 = self.mapped_bytes.ptr;
        const offset = constants.fc_buffer_length_offset;
        return @ptrCast(@alignCast(base + offset));
    }

    fn brokerReservedLengthPtr(self: *const BrokerMetadataFile) *volatile i32 {
        const base: [*]u8 = self.mapped_bytes.ptr;
        const offset = constants.broker_reserved_length_offset;
        return @ptrCast(@alignCast(base + offset));
    }

    fn metadataMonitoringVersionPtr(self: *const BrokerMetadataFile) *volatile i32 {
        const base: [*]u8 = self.mapped_bytes.ptr;
        return @ptrCast(@alignCast(base + constants.metadata_monitoring_version_offset));
    }

    fn counterValuesBufferLengthPtr(self: *const BrokerMetadataFile) *volatile i32 {
        const base: [*]u8 = self.mapped_bytes.ptr;
        return @ptrCast(@alignCast(base + constants.counter_values_length_offset));
    }

    fn counterMetadataBufferLengthPtr(self: *const BrokerMetadataFile) *volatile i32 {
        const base: [*]u8 = self.mapped_bytes.ptr;
        return @ptrCast(@alignCast(base + constants.counter_metadata_length_offset));
    }

    fn errorLogBufferLengthPtr(self: *const BrokerMetadataFile) *volatile i32 {
        const base: [*]u8 = self.mapped_bytes.ptr;
        return @ptrCast(@alignCast(base + constants.error_log_length_offset));
    }

    fn monitoringTailOffsetPtr(self: *const BrokerMetadataFile) *volatile i64 {
        const base: [*]u8 = self.mapped_bytes.ptr;
        return @ptrCast(@alignCast(base + constants.monitoring_tail_offset_offset));
    }

    fn monitoringTailLengthPtr(self: *const BrokerMetadataFile) *volatile i64 {
        const base: [*]u8 = self.mapped_bytes.ptr;
        return @ptrCast(@alignCast(base + constants.monitoring_tail_length_offset));
    }

    fn aeronDiscoveryRegionOffsetPtr(self: *const BrokerMetadataFile) *volatile i64 {
        const base: [*]u8 = self.mapped_bytes.ptr;
        return @ptrCast(@alignCast(base + constants.aeron_discovery_region_offset_offset));
    }

    fn aeronDiscoveryRegionLengthPtr(self: *const BrokerMetadataFile) *volatile i64 {
        const base: [*]u8 = self.mapped_bytes.ptr;
        return @ptrCast(@alignCast(base + constants.aeron_discovery_region_length_offset));
    }

    fn alignedCounterValuesLength(counter_values_buffer_length: usize) usize {
        return constants.alignUp(counter_values_buffer_length, constants.cache_line_pad);
    }

    fn counterMetadataLength(counter_values_buffer_length: usize, explicit_length: usize) usize {
        if (explicit_length > 0) return constants.alignUp(explicit_length, counters.counter_metadata_length);
        const slots = @max(@as(usize, 1), alignedCounterValuesLength(counter_values_buffer_length) / counters.counter_value_length);
        return slots * counters.counter_metadata_length;
    }

    fn validateAeronOptions(opts: FlowControlOptions) !void {
        if (opts.aeron_directory.len > constants.max_aeron_directory_length)
            return error.AeronDirectoryTooLong;
        if (opts.local_data_channel.len > constants.max_aeron_channel_length)
            return error.AeronChannelTooLong;
        if (opts.local_admin_channel.len > constants.max_aeron_channel_length)
            return error.AeronChannelTooLong;
        if (opts.peer_data_channels.len > constants.default_max_peers)
            return error.TooManyAeronPeers;
        for (opts.peer_data_channels) |peer| {
            if (peer.data_channel.len > constants.max_aeron_channel_length)
                return error.AeronChannelTooLong;
        }
    }

    fn copyAeronDirectory(
        dest: *[constants.max_aeron_directory_length]u8,
        length: *u16,
        value: []const u8,
    ) void {
        @memset(dest, 0);
        @memcpy(dest[0..value.len], value);
        length.* = @intCast(value.len);
    }

    fn copyAeronChannel(
        dest: *[constants.max_aeron_channel_length]u8,
        length: *u16,
        value: []const u8,
    ) void {
        @memset(dest, 0);
        @memcpy(dest[0..value.len], value);
        length.* = @intCast(value.len);
    }

    fn copyAeronPeerChannels(
        discovery: *BrokerAeronDiscovery,
        peers: []const BrokerAeronPeerConfig,
    ) void {
        discovery.peer_count = @intCast(peers.len);
        for (&discovery.peer_data_channels) |*slot| {
            slot.* = .{};
        }
        for (peers, 0..) |peer, i| {
            discovery.peer_data_channels[i] = .{
                .node_id = peer.node_id,
                .data_stream_id = peer.data_stream_id,
                .data_channel_length = @intCast(peer.data_channel.len),
            };
            @memcpy(discovery.peer_data_channels[i].data_channel[0..peer.data_channel.len], peer.data_channel);
        }
    }

    fn buildBrokerPath(
        buf: *[std.fs.max_path_bytes]u8,
        storage_path: []const u8,
        group: []const u8,
        node_id: i16,
    ) ![]const u8 {
        return std.fmt.bufPrint(buf, "{s}/{s}/services/broker_{d}.dat", .{
            storage_path,
            group,
            node_id,
        }) catch return error.PathTooLong;
    }

    /// Returns true if a filename matches the broker metadata naming convention
    /// (e.g. "broker_0.dat", "broker_1.dat", "broker_42.dat").
    pub fn isBrokerMetadataFile(name: []const u8) bool {
        if (!std.mem.startsWith(u8, name, "broker_")) return false;
        if (!std.mem.endsWith(u8, name, ".dat")) return false;
        // Check that the middle part is a valid integer.
        const middle = name["broker_".len .. name.len - ".dat".len];
        if (middle.len == 0) return false;
        _ = std.fmt.parseInt(i16, middle, 10) catch return false;
        return true;
    }

    fn ensureDirectoryExists(file_path: []const u8) !void {
        const dir_path = std.fs.path.dirname(file_path) orelse return error.InvalidPath;
        const io = platform.defaultIo();
        var root_dir = std.Io.Dir.openDirAbsolute(io, "/", .{}) catch return error.FileNotFound;
        defer root_dir.close(io);
        const relative = if (dir_path.len > 0 and dir_path[0] == '/') dir_path[1..] else dir_path;
        root_dir.createDirPath(io, relative) catch return error.FileNotFound;
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "BrokerMetadataHeader has correct size" {
    try testing.expectEqual(@as(usize, 40), @sizeOf(BrokerMetadataHeader));
}

test "create broker metadata v2 file and verify layout" {
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(storage_path);

    try tmp_dir.dir.createDirPath(testing.io, "test-group/services");

    var file = try BrokerMetadataFile.create(
        storage_path,
        "test-group",
        42, // node_id
        64 * 1024, // 64 KB control buffer
        1024 * 1024, // transitional process-local sender capacity, not mapped
    );
    defer file.close();

    // Verify header fields.
    try testing.expectEqual(constants.metadata_version, file.header.metadata_version);
    try testing.expectEqual(@as(i32, 64 * 1024), file.header.control_buffer_length);
    try testing.expectEqual(@as(i32, 0), file.header.messages_buffer_length);
    try testing.expectEqual(@as(i32, 0), file.header.service_id);
    try testing.expectEqual(@as(i16, 42), file.header.node_id);
    try testing.expect(file.header.pid > 0);
    try testing.expect(file.header.start_timestamp_ms > 0);

    // Verify buffer slice sizes (include ring buffer trailer).
    const trailer = constants.ring_buffer_trailer_length;
    try testing.expectEqual(@as(usize, 64 * 1024 + trailer), file.control_buffer.len);

    // Verify control buffer starts after the fixed Aeron discovery region.
    const aeron_region_len = constants.alignUp(@sizeOf(BrokerAeronDiscovery), constants.cache_line_pad);
    const control_offset = @intFromPtr(file.control_buffer.ptr) - @intFromPtr(file.mapped_bytes.ptr);
    try testing.expectEqual(@as(usize, 512 + aeron_region_len), control_offset);

    try testing.expectEqual(@as(i64, 512), file.loadAeronDiscoveryRegionOffset());
    try testing.expectEqual(@as(i64, @intCast(aeron_region_len)), file.loadAeronDiscoveryRegionLength());

    // Verify nextServiceId initialized to 1.
    try testing.expectEqual(@as(i32, 1), file.loadNextServiceId());

    // Verify heartbeat was written.
    try testing.expect(file.loadHeartbeat() > 0);
}

test "broker metadata v2 Aeron discovery round trip and file size exclude send ring" {
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(storage_path);
    try tmp_dir.dir.createDirPath(testing.io, "test-group/services");

    var file = try BrokerMetadataFile.createWithFlowControl(
        storage_path,
        "test-group",
        1,
        4096,
        1024 * 1024,
        .{
            .aeron_directory = "/tmp/ringloom-aeron-test",
            .broker_ingress_stream_id = 10_001,
            .admin_stream_base = 20_000,
            .data_stream_base = 30_000,
            .local_data_channel = "aeron:udp?endpoint=127.0.0.1:40123",
            .local_admin_channel = "aeron:udp?endpoint=127.0.0.1:40124",
            .peer_data_channels = &.{
                .{
                    .node_id = 2,
                    .data_stream_id = 30_002,
                    .data_channel = "aeron:udp?endpoint=127.0.0.2:40123|term-length=16777216",
                },
            },
        },
    );
    const mapped_len = file.mapped_bytes.len;
    file.close();

    var opened = try BrokerMetadataFile.open(storage_path, "test-group", 1);
    defer opened.close();

    const discovery = opened.getAeronDiscovery();
    try testing.expectEqual(@as(i32, 10_001), discovery.broker_ingress_stream_id);
    try testing.expectEqual(@as(i32, 20_000), discovery.admin_stream_base);
    try testing.expectEqual(@as(i32, 30_000), discovery.data_stream_base);
    try testing.expectEqualStrings("/tmp/ringloom-aeron-test", discovery.directory());
    try testing.expectEqualStrings("aeron:udp?endpoint=127.0.0.1:40123", discovery.dataChannel());
    try testing.expectEqualStrings("aeron:udp?endpoint=127.0.0.1:40124", discovery.adminChannel());
    try testing.expectEqual(@as(u8, 1), discovery.peer_count);
    const peer = discovery.peerByNodeId(2).?;
    try testing.expectEqual(@as(i32, 30_002), peer.data_stream_id);
    try testing.expectEqualStrings(
        "aeron:udp?endpoint=127.0.0.2:40123|term-length=16777216",
        peer.dataChannel(),
    );
    try testing.expect(discovery.peerByNodeId(3) == null);

    const trailer = constants.ring_buffer_trailer_length;
    const aeron_region_len = constants.alignUp(@sizeOf(BrokerAeronDiscovery), constants.cache_line_pad);
    const base_without_send = constants.metadata_header_length + aeron_region_len + 4096 + trailer;
    const minimum_with_send = constants.alignUp(base_without_send + 1024 * 1024 + trailer, constants.page_size);
    try testing.expect(mapped_len < minimum_with_send);
}

test "incrementAndGetNextServiceId is atomic" {
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(storage_path);
    try tmp_dir.dir.createDirPath(testing.io, "test-group/services");

    var file = try BrokerMetadataFile.create(storage_path, "test-group", 1, 64 * 1024, 64 * 1024);
    defer file.close();

    // Initial value is 1.
    try testing.expectEqual(@as(i32, 1), file.loadNextServiceId());

    // Increment returns the new value.
    try testing.expectEqual(@as(i32, 2), file.incrementAndGetNextServiceId());
    try testing.expectEqual(@as(i32, 3), file.incrementAndGetNextServiceId());
    try testing.expectEqual(@as(i32, 4), file.incrementAndGetNextServiceId());

    // Verify the stored value.
    try testing.expectEqual(@as(i32, 4), file.loadNextServiceId());
}

test "reject non-power-of-two buffer sizes" {
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(storage_path);
    try tmp_dir.dir.createDirPath(testing.io, "test-group/services");

    // 1000 is not a power of two.
    const result = BrokerMetadataFile.create(storage_path, "test-group", 1, 1000, 64 * 1024);
    try testing.expectError(error.ControlBufferNotPowerOfTwo, result);
}

test "two mappings share a broker metadata v2 file" {
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(storage_path);
    try tmp_dir.dir.createDirPath(testing.io, "test-group/services");

    // Thread 1 (broker): create the file.
    var broker_file = try BrokerMetadataFile.create(
        storage_path,
        "test-group",
        1,
        4096,
        4096,
    );
    defer broker_file.close();

    // Write a known pattern into the control buffer region.
    const pattern: u64 = 0xDEADBEEFCAFEBABE;
    @memcpy(broker_file.control_buffer[0..8], std.mem.asBytes(&pattern));

    // Thread 2 (service): open the same file.
    var service_view = try BrokerMetadataFile.open(storage_path, "test-group", 1);
    defer service_view.close();

    // Verify the service sees the same data.
    const read_pattern = std.mem.bytesToValue(u64, service_view.control_buffer[0..8]);
    try testing.expectEqual(pattern, read_pattern);

    // Aeron discovery metadata is mapped into both views.
    broker_file.getMutableAeronDiscovery().broker_ingress_stream_id = 10_001;
    try testing.expectEqual(
        @as(i32, 10_001),
        service_view.getAeronDiscovery().broker_ingress_stream_id,
    );
}

test "atomic nextServiceId visible across mappings" {
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(storage_path);
    try tmp_dir.dir.createDirPath(testing.io, "test-group/services");

    var file1 = try BrokerMetadataFile.create(storage_path, "test-group", 1, 4096, 4096);
    defer file1.close();

    var file2 = try BrokerMetadataFile.open(storage_path, "test-group", 1);
    defer file2.close();

    // file1 sets nextServiceId.
    file1.storeNextServiceId(10);

    // file2 reads it (both map the same physical page).
    try testing.expectEqual(@as(i32, 10), file2.loadNextServiceId());

    // file2 increments.
    const new_id = file2.incrementAndGetNextServiceId();
    try testing.expectEqual(@as(i32, 11), new_id);

    // file1 sees the increment.
    try testing.expectEqual(@as(i32, 11), file1.loadNextServiceId());
}

test "concurrent incrementAndGetNextServiceId from multiple threads" {
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(storage_path);
    try tmp_dir.dir.createDirPath(testing.io, "test-group/services");

    var file = try BrokerMetadataFile.create(storage_path, "test-group", 1, 4096, 4096);
    defer file.close();

    const num_threads = 8;
    const increments_per_thread = 1000;

    var threads: [num_threads]std.Thread = undefined;
    for (&threads) |*t| {
        t.* = try std.Thread.spawn(.{}, struct {
            fn run(f: *BrokerMetadataFile) void {
                var i: usize = 0;
                while (i < increments_per_thread) : (i += 1) {
                    _ = f.incrementAndGetNextServiceId();
                }
            }
        }.run, .{&file});
    }

    for (&threads) |*t| {
        t.join();
    }

    // Initial value was 1, plus 8 * 1000 increments.
    const expected: i32 = 1 + num_threads * increments_per_thread;
    try testing.expectEqual(expected, file.loadNextServiceId());
}

test "create with flow control regions" {
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(storage_path);
    try tmp_dir.dir.createDirPath(testing.io, "test-group/services");

    var file = try BrokerMetadataFile.createWithFlowControl(
        storage_path,
        "test-group",
        1,
        4096,
        4096,
        .{ .fc_max_entries = 8 },
    );
    defer file.close();

    // Verify FC region is present.
    try testing.expect(file.fc_region != null);

    // Verify header stores the region lengths.
    const expected_fc_len: i32 = @intCast(FlowControlRegion.regionSize(8));
    try testing.expectEqual(expected_fc_len, file.loadFcBufferLength());
    try testing.expectEqual(@as(i32, 0), file.loadBrokerReservedLength());

    // Use the FC region: allocate a slot.
    var fc = file.fc_region.?;
    const slot = fc.allocateSlot(42, 1, 100_000);
    try testing.expect(slot != null);
    const entry = fc.getEntry(slot.?).?;
    try testing.expectEqual(@as(i32, 42), entry.service_id);
}

test "open reads back flow control regions" {
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(storage_path);
    try tmp_dir.dir.createDirPath(testing.io, "test-group/services");

    // Create with FC.
    var file1 = try BrokerMetadataFile.createWithFlowControl(
        storage_path,
        "test-group",
        1,
        4096,
        4096,
        .{ .fc_max_entries = 4 },
    );

    // Write data into FC region.
    var fc1 = file1.fc_region.?;
    const slot_idx = fc1.allocateSlot(99, 1, 500_000).?;
    const entry1 = fc1.getEntry(slot_idx).?;
    entry1.storeRemainingBytes(123_456);

    file1.close();

    // Re-open and verify FC region is detected.
    var file2 = try BrokerMetadataFile.open(storage_path, "test-group", 1);
    defer file2.close();
    try testing.expect(file2.fc_region != null);
    try testing.expect(file2.fc_region != null);

    // Verify data survived.
    const fc2 = file2.fc_region.?;
    const entry2 = fc2.getEntry(slot_idx).?;
    try testing.expectEqual(@as(u32, 123_456), entry2.loadRemainingBytes());
}

test "open backward compat with no FC regions" {
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(storage_path);
    try tmp_dir.dir.createDirPath(testing.io, "test-group/services");

    // Create without FC (old-style).
    var file1 = try BrokerMetadataFile.create(storage_path, "test-group", 1, 4096, 4096);
    file1.close();

    // Open should succeed, FC regions should be null.
    var file2 = try BrokerMetadataFile.open(storage_path, "test-group", 1);
    defer file2.close();
    try testing.expect(file2.fc_region == null);
    try testing.expect(file2.fc_region == null);
    try testing.expectEqual(@as(i32, 0), file2.loadFcBufferLength());
    try testing.expectEqual(@as(i32, 0), file2.loadBrokerReservedLength());
}

test "different node_ids create independent files" {
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(storage_path);
    try tmp_dir.dir.createDirPath(testing.io, "test-group/services");

    // Create broker metadata for node 1 and node 2 under the same group.
    var broker1 = try BrokerMetadataFile.create(storage_path, "test-group", 1, 4096, 4096);
    defer broker1.close();

    var broker2 = try BrokerMetadataFile.create(storage_path, "test-group", 2, 4096, 4096);
    defer broker2.close();

    // Verify they have different node_ids.
    try testing.expectEqual(@as(i16, 1), broker1.header.node_id);
    try testing.expectEqual(@as(i16, 2), broker2.header.node_id);

    // Verify independent mappings: writing to one doesn't affect the other.
    broker1.storeNextServiceId(100);
    broker2.storeNextServiceId(200);
    try testing.expectEqual(@as(i32, 100), broker1.loadNextServiceId());
    try testing.expectEqual(@as(i32, 200), broker2.loadNextServiceId());

    // Open each by node_id and verify isolation.
    var opened1 = try BrokerMetadataFile.open(storage_path, "test-group", 1);
    defer opened1.close();
    var opened2 = try BrokerMetadataFile.open(storage_path, "test-group", 2);
    defer opened2.close();

    try testing.expectEqual(@as(i16, 1), opened1.header.node_id);
    try testing.expectEqual(@as(i16, 2), opened2.header.node_id);
    try testing.expectEqual(@as(i32, 100), opened1.loadNextServiceId());
    try testing.expectEqual(@as(i32, 200), opened2.loadNextServiceId());
}

test "isBrokerMetadataFile identifies broker files" {
    try testing.expect(BrokerMetadataFile.isBrokerMetadataFile("broker_0.dat"));
    try testing.expect(BrokerMetadataFile.isBrokerMetadataFile("broker_1.dat"));
    try testing.expect(BrokerMetadataFile.isBrokerMetadataFile("broker_42.dat"));
    try testing.expect(!BrokerMetadataFile.isBrokerMetadataFile("echo_1.dat"));
    try testing.expect(!BrokerMetadataFile.isBrokerMetadataFile("broker_.dat"));
    try testing.expect(!BrokerMetadataFile.isBrokerMetadataFile("broker_abc.dat"));
}
