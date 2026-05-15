//! Broker metadata file layout for the RingLoom broker.
//!
//! The broker's metadata file is memory-mapped on /dev/shm and shared with
//! all local services. It contains the broker's control ring buffer
//! (services → broker) and send ring buffer (services → broker for cross-host).

const std = @import("std");
const constants = @import("constants.zig");
const platform = @import("../platform.zig");
const FlowControlRegion = @import("flow_control.zig").FlowControlRegion;
const PeerSendCountersRegion = @import("peer_send_counters.zig").PeerSendCountersRegion;
const send_buffers = @import("send_buffer_directory.zig");
const SendBufferDirectory = send_buffers.SendBufferDirectory;
const SendBufferHandle = send_buffers.SendBufferHandle;
const counters = @import("../concurrent/counters.zig");

/// Overlay for the first 32 bytes of the broker metadata header.
/// Read/written once at creation; immutable after that (except volatile fields).
pub const BrokerMetadataHeader = extern struct {
    control_buffer_length: i32,
    messages_buffer_length: i32,
    service_id: i32,
    node_id: i16,
    _padding0: i16 = 0,
    pid: i64,
    start_timestamp_ms: i64,
    metadata_version: u32 = constants.metadata_version_v2,
    send_buffer_entry_count: u32 = constants.default_send_buffer_entry_count,
    send_directory_offset: u64 = 0,
    send_directory_length: u64 = 0,
    send_region_offset: u64 = 0,
    send_region_length: u64 = 0,

    comptime {
        // Ensure the struct doesn't grow past the volatile field offsets.
        std.debug.assert(@sizeOf(BrokerMetadataHeader) <= platform.constants.heartbeat_offset_within_header);
        std.debug.assert(@sizeOf(BrokerMetadataHeader) <= platform.constants.next_service_id_offset_within_header);
        std.debug.assert(@sizeOf(BrokerMetadataHeader) <= platform.constants.fc_buffer_length_offset);
        std.debug.assert(@sizeOf(BrokerMetadataHeader) <= platform.constants.peer_send_counters_length_offset);
    }
};

pub const BrokerMetadataFile = struct {
    /// The full mmap'd region.
    mapped_bytes: []align(constants.page_size) u8,

    /// Pointer to the header overlay (first 32 bytes).
    header: *BrokerMetadataHeader,

    /// Byte slice covering the control ring buffer region.
    control_buffer: []u8,

    /// Send-buffer directory keyed by remote destination service.
    send_buffer_directory: SendBufferDirectory,

    /// Byte slice covering all fixed per-destination send ring slots.
    send_region: []u8,

    /// Flow control counters region (null if disabled).
    fc_region: ?FlowControlRegion = null,

    /// Per-peer send counters region (null if disabled).
    peer_send_counters: ?PeerSendCountersRegion = null,

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
        peer_send_max_peers: u32 = 0,
        counter_values_buffer_length: usize = constants.default_counter_values_buffer_length,
        counter_metadata_buffer_length: usize = 0,
        error_log_buffer_length: usize = constants.default_error_log_buffer_length,
    };

    /// Create and initialize a new broker metadata file.
    ///
    /// - `storage_path`: e.g. "/dev/shm"
    /// - `group`: e.g. "default"
    /// - `node_id`: this broker's node identifier
    /// - `control_buffer_length`: must be a power of two
    /// - `messages_buffer_length`: must be a power of two
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
        if (!constants.isPowerOfTwo(messages_buffer_length))
            return error.MessagesBufferNotPowerOfTwo;

        // The ring buffer needs data_capacity + trailer. The caller specifies the
        // data capacity (must be power-of-two); we add the trailer here.
        const trailer = constants.ring_buffer_trailer_length;
        const ctrl_region = control_buffer_length + trailer;
        const send_entry_count = constants.default_send_buffer_entry_count;
        const send_directory_len = SendBufferDirectory.regionSize(send_entry_count);
        const send_slot_len = SendBufferDirectory.slotLength(messages_buffer_length);
        const send_region_len = send_slot_len * @as(usize, send_entry_count);

        // Compute flow control region sizes.
        const fc_buf_len: usize = if (fc_options.fc_max_entries > 0)
            FlowControlRegion.regionSize(fc_options.fc_max_entries)
        else
            0;
        const peer_send_len: usize = if (fc_options.peer_send_max_peers > 0)
            PeerSendCountersRegion.regionSize(fc_options.peer_send_max_peers)
        else
            0;

        const counter_values_len = alignedCounterValuesLength(fc_options.counter_values_buffer_length);
        const counter_metadata_len = counterMetadataLength(
            fc_options.counter_values_buffer_length,
            fc_options.counter_metadata_buffer_length,
        );
        const error_log_len = constants.alignUp(fc_options.error_log_buffer_length, @sizeOf(i64));

        const send_directory_offset = constants.metadata_header_length + ctrl_region;
        const send_region_offset = send_directory_offset + send_directory_len;
        const fc_region_offset = send_region_offset + send_region_len;
        const peer_send_region_offset = fc_region_offset + fc_buf_len;
        const base_size = peer_send_region_offset + peer_send_len;
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

        const counter_values_offset = monitoring_tail_offset;
        const counter_metadata_offset = counter_values_offset + counter_values_len;
        const error_log_offset = counter_metadata_offset + counter_metadata_len;

        const directory_slice = mapped[send_directory_offset..][0..send_directory_len];
        const send_directory = SendBufferDirectory.initNew(
            directory_slice,
            send_entry_count,
            send_region_offset,
            send_region_len,
            messages_buffer_length,
        ) catch return error.SendBufferDirectoryInitFailed;

        // Initialize FC regions if requested.
        var fc_region: ?FlowControlRegion = null;
        if (fc_buf_len > 0) {
            const fc_slice = mapped[fc_region_offset..][0..fc_buf_len];
            fc_region = FlowControlRegion.initNew(fc_slice, fc_options.fc_max_entries) catch
                return error.FlowControlInitFailed;
        }

        var peer_send_counters: ?PeerSendCountersRegion = null;
        if (peer_send_len > 0) {
            const peer_slice = mapped[peer_send_region_offset..][0..peer_send_len];
            peer_send_counters = PeerSendCountersRegion.initNew(peer_slice, fc_options.peer_send_max_peers) catch
                return error.PeerSendCountersInitFailed;
        }

        var self = BrokerMetadataFile{
            .mapped_bytes = mapped,
            .header = @ptrCast(@alignCast(mapped.ptr)),
            .control_buffer = mapped[constants.metadata_header_length..][0..ctrl_region],
            .send_buffer_directory = send_directory,
            .send_region = mapped[send_region_offset..][0..send_region_len],
            .fc_region = fc_region,
            .peer_send_counters = peer_send_counters,
            .counter_values_buffer = @alignCast(mapped[counter_values_offset..][0..counter_values_len]),
            .counter_metadata_buffer = mapped[counter_metadata_offset..][0..counter_metadata_len],
            .error_log_buffer = mapped[error_log_offset..][0..error_log_len],
            .fd = fd,
        };

        // Write the immutable header fields — store data capacity (without trailer).
        self.header.control_buffer_length = @intCast(control_buffer_length);
        self.header.messages_buffer_length = @intCast(messages_buffer_length);
        self.header.service_id = constants.broker_service_id;
        self.header.node_id = node_id;
        self.header.pid = platform.getPid();
        self.header.start_timestamp_ms = platform.Clock.epochMillis();
        self.header.metadata_version = constants.metadata_version_v2;
        self.header.send_buffer_entry_count = send_entry_count;
        self.header.send_directory_offset = @intCast(send_directory_offset);
        self.header.send_directory_length = @intCast(send_directory_len);
        self.header.send_region_offset = @intCast(send_region_offset);
        self.header.send_region_length = @intCast(send_region_len);

        // Store FC region sizes in the header (at fixed offsets beyond 32-byte struct).
        self.storeFcBufferLength(@intCast(fc_buf_len));
        self.storePeerSendCountersLength(@intCast(peer_send_len));
        self.storeMetadataMonitoringVersion(constants.metadata_monitoring_version);
        self.storeCounterValuesBufferLength(@intCast(counter_values_len));
        self.storeCounterMetadataBufferLength(@intCast(counter_metadata_len));
        self.storeErrorLogBufferLength(@intCast(error_log_len));
        self.storeMonitoringTailOffset(@intCast(monitoring_tail_offset));
        self.storeMonitoringTailLength(@intCast(monitoring_tail_len));

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
        if (header.metadata_version != constants.metadata_version_v2)
            return error.InvalidMetadataVersion;

        const ctrl_len: usize = @intCast(header.control_buffer_length);
        const msgs_len: usize = @intCast(header.messages_buffer_length);

        if (!constants.isPowerOfTwo(ctrl_len))
            return error.ControlBufferNotPowerOfTwo;
        if (!constants.isPowerOfTwo(msgs_len))
            return error.MessagesBufferNotPowerOfTwo;

        const trailer = constants.ring_buffer_trailer_length;
        const ctrl_region = ctrl_len + trailer;
        const send_directory_offset: usize = @intCast(header.send_directory_offset);
        const send_directory_len: usize = @intCast(header.send_directory_length);
        const send_region_offset: usize = @intCast(header.send_region_offset);
        const send_region_len: usize = @intCast(header.send_region_length);

        if (send_directory_offset < constants.metadata_header_length + ctrl_region)
            return error.FileSizeMismatch;
        if (send_region_offset < send_directory_offset + send_directory_len)
            return error.FileSizeMismatch;

        const directory_slice = mapped[send_directory_offset..][0..send_directory_len];
        const send_directory = SendBufferDirectory.initExisting(directory_slice) catch
            return error.SendBufferDirectoryInitFailed;

        const base_size = send_region_offset + send_region_len;

        // Read FC/monitoring region lengths from header.
        var self = BrokerMetadataFile{
            .mapped_bytes = mapped,
            .header = header,
            .control_buffer = mapped[constants.metadata_header_length..][0..ctrl_region],
            .send_buffer_directory = send_directory,
            .send_region = mapped[send_region_offset..][0..send_region_len],
            .counter_values_buffer = undefined,
            .counter_metadata_buffer = undefined,
            .error_log_buffer = undefined,
            .fd = fd,
        };

        const fc_buf_len: usize = @intCast(self.loadFcBufferLength());
        const peer_send_len: usize = @intCast(self.loadPeerSendCountersLength());
        const counter_values_len: usize = @intCast(self.loadCounterValuesBufferLength());
        const counter_metadata_len: usize = @intCast(self.loadCounterMetadataBufferLength());
        const error_log_len: usize = @intCast(self.loadErrorLogBufferLength());
        const monitoring_tail_offset: usize = @intCast(self.loadMonitoringTailOffset());
        const monitoring_tail_len: usize = @intCast(self.loadMonitoringTailLength());

        const fc_region_offset = base_size;
        const peer_send_region_offset = fc_region_offset + fc_buf_len;
        const counter_values_offset = monitoring_tail_offset;
        const counter_metadata_offset = counter_values_offset + counter_values_len;
        const error_log_offset = counter_metadata_offset + counter_metadata_len;

        if (monitoring_tail_len != counter_values_len + counter_metadata_len + error_log_len)
            return error.FileSizeMismatch;
        if (monitoring_tail_offset < peer_send_region_offset + peer_send_len)
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

        if (peer_send_len > 0) {
            const peer_slice = mapped[peer_send_region_offset..][0..peer_send_len];
            self.peer_send_counters = PeerSendCountersRegion.initExisting(peer_slice) catch
                return error.PeerSendCountersInitFailed;
        }

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

    /// Load the peer send counters region length from the header (0 = absent).
    pub fn loadPeerSendCountersLength(self: *const BrokerMetadataFile) i32 {
        const ptr = self.peerSendCountersLengthPtr();
        return @atomicLoad(i32, ptr, .acquire);
    }

    /// Store the peer send counters region length in the header.
    pub fn storePeerSendCountersLength(self: *BrokerMetadataFile, value: i32) void {
        const ptr = self.peerSendCountersLengthPtr();
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

    // ── Flow Control Accessors ───────────────────────────────────────

    /// Returns the flow control region, or null if not present.
    pub fn getFlowControlRegion(self: *const BrokerMetadataFile) ?FlowControlRegion {
        return self.fc_region;
    }

    /// Returns the peer send counters region, or null if not present.
    pub fn getPeerSendCountersRegion(self: *const BrokerMetadataFile) ?PeerSendCountersRegion {
        return self.peer_send_counters;
    }

    // ── Buffer Accessors ──────────────────────────────────────────────

    /// Returns the byte slice backing the control ring buffer.
    pub fn getControlBuffer(self: *const BrokerMetadataFile) []u8 {
        return self.control_buffer;
    }

    /// Returns the per-destination send-buffer directory.
    pub fn getSendBufferDirectory(self: *const BrokerMetadataFile) *const SendBufferDirectory {
        return &self.send_buffer_directory;
    }

    pub fn getMutableSendBufferDirectory(self: *BrokerMetadataFile) *SendBufferDirectory {
        return &self.send_buffer_directory;
    }

    /// Returns the fixed per-destination send region.
    pub fn getSendRegion(self: *const BrokerMetadataFile) []u8 {
        return self.send_region;
    }

    /// Compatibility accessor for tooling that still expects a single send
    /// buffer. Production send paths use `getSendBufferDirectory`.
    pub fn getSendBuffer(self: *const BrokerMetadataFile) []u8 {
        const len = SendBufferDirectory.slotLength(@intCast(self.header.messages_buffer_length));
        return self.send_region[0..@min(len, self.send_region.len)];
    }

    pub fn findOrAllocateSendBuffer(
        self: *BrokerMetadataFile,
        target_node_id: i16,
        target_service_id: i32,
    ) !SendBufferHandle {
        return self.send_buffer_directory.findOrAllocateDestination(
            self.mapped_bytes,
            target_node_id,
            target_service_id,
        );
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

    fn peerSendCountersLengthPtr(self: *const BrokerMetadataFile) *volatile i32 {
        const base: [*]u8 = self.mapped_bytes.ptr;
        const offset = constants.peer_send_counters_length_offset;
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

    fn alignedCounterValuesLength(counter_values_buffer_length: usize) usize {
        return constants.alignUp(counter_values_buffer_length, constants.cache_line_pad);
    }

    fn counterMetadataLength(counter_values_buffer_length: usize, explicit_length: usize) usize {
        if (explicit_length > 0) return constants.alignUp(explicit_length, counters.counter_metadata_length);
        const slots = @max(@as(usize, 1), alignedCounterValuesLength(counter_values_buffer_length) / counters.counter_value_length);
        return slots * counters.counter_metadata_length;
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
    try testing.expectEqual(@as(usize, 72), @sizeOf(BrokerMetadataHeader));
}

test "create broker metadata file and verify layout" {
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
        1024 * 1024, // 1 MB send buffer
    );
    defer file.close();

    // Verify header fields.
    try testing.expectEqual(@as(i32, 64 * 1024), file.header.control_buffer_length);
    try testing.expectEqual(@as(i32, 1024 * 1024), file.header.messages_buffer_length);
    try testing.expectEqual(@as(i32, 0), file.header.service_id);
    try testing.expectEqual(@as(i16, 42), file.header.node_id);
    try testing.expect(file.header.pid > 0);
    try testing.expect(file.header.start_timestamp_ms > 0);
    try testing.expectEqual(constants.metadata_version_v2, file.header.metadata_version);
    try testing.expectEqual(constants.default_send_buffer_entry_count, file.header.send_buffer_entry_count);

    // Verify buffer slice sizes (include ring buffer trailer).
    const trailer = constants.ring_buffer_trailer_length;
    try testing.expectEqual(@as(usize, 64 * 1024 + trailer), file.control_buffer.len);
    try testing.expectEqual(
        SendBufferDirectory.slotLength(1024 * 1024) * constants.default_send_buffer_entry_count,
        file.send_region.len,
    );

    // Verify control buffer starts at offset 512.
    const control_offset = @intFromPtr(file.control_buffer.ptr) - @intFromPtr(file.mapped_bytes.ptr);
    try testing.expectEqual(@as(usize, 512), control_offset);

    // Verify send directory starts right after control buffer and send region
    // follows the directory.
    try testing.expectEqual(
        @as(u64, 512 + 64 * 1024 + trailer),
        file.header.send_directory_offset,
    );
    try testing.expectEqual(
        file.header.send_directory_offset + file.header.send_directory_length,
        file.header.send_region_offset,
    );

    // Verify nextServiceId initialized to 1.
    try testing.expectEqual(@as(i32, 1), file.loadNextServiceId());

    // Verify heartbeat was written.
    try testing.expect(file.loadHeartbeat() > 0);
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

test "two threads share a broker metadata file" {
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

    // Service provisions and writes to a destination send buffer.
    const msg: u64 = 0x1234567890ABCDEF;
    const handle = try service_view.findOrAllocateSendBuffer(2, 7);
    const service_ring = try service_view.send_buffer_directory.ringSliceForHandle(
        service_view.mapped_bytes,
        handle,
    );
    @memcpy(service_ring[0..8], std.mem.asBytes(&msg));

    // Broker reads it back.
    const broker_ring = try broker_file.send_buffer_directory.ringSliceForHandle(
        broker_file.mapped_bytes,
        handle,
    );
    const read_msg = std.mem.bytesToValue(u64, broker_ring[0..8]);
    try testing.expectEqual(msg, read_msg);
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
        .{ .fc_max_entries = 8, .peer_send_max_peers = 4 },
    );
    defer file.close();

    // Verify FC region is present.
    try testing.expect(file.fc_region != null);
    try testing.expect(file.peer_send_counters != null);

    // Verify header stores the region lengths.
    const expected_fc_len: i32 = @intCast(FlowControlRegion.regionSize(8));
    const expected_peer_len: i32 = @intCast(PeerSendCountersRegion.regionSize(4));
    try testing.expectEqual(expected_fc_len, file.loadFcBufferLength());
    try testing.expectEqual(expected_peer_len, file.loadPeerSendCountersLength());

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
        .{ .fc_max_entries = 4, .peer_send_max_peers = 2 },
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
    try testing.expect(file2.peer_send_counters != null);

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
    try testing.expect(file2.peer_send_counters == null);
    try testing.expectEqual(@as(i32, 0), file2.loadFcBufferLength());
    try testing.expectEqual(@as(i32, 0), file2.loadPeerSendCountersLength());
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
