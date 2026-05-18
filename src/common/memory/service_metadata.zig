//! Service metadata file layout for the RingLoom broker.
//!
//! Each service on a host has a metadata file on /dev/shm containing:
//! - A header with service identity and configuration
//! - An optional blocking trailer for kernel-assisted producer parking
//! - A control ring buffer (broker → service)
//! - A messages ring buffer (producers → service)

const std = @import("std");
const constants = @import("constants.zig");
const platform = @import("../platform.zig");
const counters = @import("../concurrent/counters.zig");

/// Overlay for the fixed fields of the service metadata header.
pub const ServiceMetadataHeader = extern struct {
    metadata_version: i32,
    control_buffer_length: i32,
    messages_buffer_length: i32,
    service_id: i32,
    node_id: i16,
    blocking_mode: i16, // 0 = non-blocking, 1 = blocking
    pid: i64,
    start_timestamp_ms: i64,
    heartbeat_timeout_ms: i32,

    comptime {
        std.debug.assert(@sizeOf(ServiceMetadataHeader) == 48);
        std.debug.assert(@sizeOf(ServiceMetadataHeader) <= @import("../platform.zig").constants.heartbeat_offset_within_header);
    }
};

pub const ServiceAeronDiscovery = extern struct {
    broker_ingress_stream_id: i32 = 0,
    _reserved0: i32 = 0,
    broker_start_timestamp_ms: i64 = 0,
    aeron_directory_length: u16 = 0,
    _reserved1: [6]u8 = [_]u8{0} ** 6,
    aeron_directory: [constants.max_aeron_directory_length]u8 = [_]u8{0} ** constants.max_aeron_directory_length,

    pub fn directory(self: *const ServiceAeronDiscovery) []const u8 {
        return self.aeron_directory[0..@as(usize, @intCast(self.aeron_directory_length))];
    }
};

/// Overlay for one 128-byte blocking trailer slot.
pub const BlockingTrailerSlot = extern struct {
    value: i32 = 0,
    _pad: [124]u8 = [_]u8{0} ** 124,

    comptime {
        std.debug.assert(@sizeOf(BlockingTrailerSlot) == 128);
    }
};

/// Overlay for the timeout slot (i64 value + padding = 128 bytes).
pub const BlockingTrailerTimeoutSlot = extern struct {
    value: i64 = 0,
    _pad: [120]u8 = [_]u8{0} ** 120,

    comptime {
        std.debug.assert(@sizeOf(BlockingTrailerTimeoutSlot) == 128);
    }
};

/// Overlay for the full blocking trailer (384 bytes = 3 × 128).
pub const BlockingTrailer = extern struct {
    writer_wait_state: BlockingTrailerSlot,
    reader_wait_state: BlockingTrailerSlot,
    wait_timeout: BlockingTrailerTimeoutSlot,

    comptime {
        std.debug.assert(@sizeOf(BlockingTrailer) == 384);
    }
};

pub const ServiceMetadataFile = struct {
    mapped_bytes: []align(constants.page_size) u8,
    header: *ServiceMetadataHeader,

    /// Non-null only when blocking_mode == 1.
    blocking_trailer: ?*BlockingTrailer,

    aeron_discovery: *ServiceAeronDiscovery,
    control_buffer: []u8,
    messages_buffer: []u8,
    counter_values_buffer: []align(constants.cache_line_pad) u8,
    counter_metadata_buffer: []u8,
    error_log_buffer: []u8,
    fd: std.posix.fd_t,

    const Self = @This();

    // ── Construction ──────────────────────────────────────────────────

    pub const CreateOptions = struct {
        storage_path: []const u8,
        group: []const u8,
        service_name: []const u8,
        service_id: i32,
        node_id: i16,
        blocking_mode: bool = false,
        heartbeat_timeout_ms: i32 = @intCast(constants.default_svc_heartbeat_timeout_ms),
        control_buffer_length: usize = constants.default_control_buffer_length,
        messages_buffer_length: usize = constants.default_messages_buffer_length,
        counter_values_buffer_length: usize = constants.default_counter_values_buffer_length,
        counter_metadata_buffer_length: usize = 0,
        error_log_buffer_length: usize = constants.default_error_log_buffer_length,
        aeron_directory: []const u8 = "",
        broker_ingress_stream_id: i32 = 0,
        broker_start_timestamp_ms: i64 = 0,
    };

    /// Create and initialize a new service metadata file.
    pub fn create(opts: CreateOptions) !ServiceMetadataFile {
        if (!constants.isPowerOfTwo(opts.control_buffer_length))
            return error.ControlBufferNotPowerOfTwo;
        if (!constants.isPowerOfTwo(opts.messages_buffer_length))
            return error.MessagesBufferNotPowerOfTwo;
        if (opts.aeron_directory.len > constants.max_aeron_directory_length)
            return error.AeronDirectoryTooLong;

        const blocking_extra: usize = if (opts.blocking_mode)
            constants.blocking_trailer_length
        else
            0;
        const aeron_region_len = constants.alignUp(@sizeOf(ServiceAeronDiscovery), constants.cache_line_pad);

        // Ring buffers need data_capacity + trailer. The caller specifies data
        // capacity (power-of-two); we add the trailer here.
        const rb_trailer = constants.ring_buffer_trailer_length;
        const ctrl_region = opts.control_buffer_length + rb_trailer;
        const msgs_region = opts.messages_buffer_length + rb_trailer;
        const counter_values_len = alignedCounterValuesLength(opts.counter_values_buffer_length);
        const counter_metadata_len = counterMetadataLength(
            opts.counter_values_buffer_length,
            opts.counter_metadata_buffer_length,
        );
        const error_log_len = constants.alignUp(opts.error_log_buffer_length, @sizeOf(i64));
        const base_size = constants.metadata_header_length +
            aeron_region_len +
            blocking_extra +
            ctrl_region +
            msgs_region;
        const monitoring_tail_offset = constants.alignUp(base_size, constants.cache_line_pad);
        const monitoring_tail_len = counter_values_len + counter_metadata_len + error_log_len;

        const total_size = constants.alignUp(
            monitoring_tail_offset + monitoring_tail_len,
            constants.page_size,
        );

        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const path = try buildServicePath(
            &path_buf,
            opts.storage_path,
            opts.group,
            opts.service_name,
            opts.service_id,
            opts.node_id,
        );

        try ensureDirectoryExists(path);

        const fd = try platform.createFile(path);
        errdefer platform.closeFd(fd);
        try platform.ftruncate(fd, total_size);

        const mapped = try platform.mmap(fd, total_size);
        errdefer platform.munmap(mapped);

        @memset(mapped, 0);

        const aeron_region_offset = constants.metadata_header_length;
        const blocking_offset = aeron_region_offset + aeron_region_len;
        const buffers_offset = blocking_offset + blocking_extra;
        const counter_values_offset = monitoring_tail_offset;
        const counter_metadata_offset = counter_values_offset + counter_values_len;
        const error_log_offset = counter_metadata_offset + counter_metadata_len;

        var self = ServiceMetadataFile{
            .mapped_bytes = mapped,
            .header = @ptrCast(@alignCast(mapped.ptr)),
            .blocking_trailer = if (opts.blocking_mode)
                @ptrCast(@alignCast(mapped.ptr + blocking_offset))
            else
                null,
            .aeron_discovery = @ptrCast(@alignCast(mapped.ptr + aeron_region_offset)),
            .control_buffer = mapped[buffers_offset..][0..ctrl_region],
            .messages_buffer = mapped[buffers_offset + ctrl_region ..][0..msgs_region],
            .counter_values_buffer = @alignCast(mapped[counter_values_offset..][0..counter_values_len]),
            .counter_metadata_buffer = mapped[counter_metadata_offset..][0..counter_metadata_len],
            .error_log_buffer = mapped[error_log_offset..][0..error_log_len],
            .fd = fd,
        };

        // Write immutable header fields — store data capacity (without trailer).
        self.header.metadata_version = constants.metadata_version;
        self.header.control_buffer_length = @intCast(opts.control_buffer_length);
        self.header.messages_buffer_length = @intCast(opts.messages_buffer_length);
        self.header.service_id = opts.service_id;
        self.header.node_id = opts.node_id;
        self.header.blocking_mode = if (opts.blocking_mode) 1 else 0;
        self.header.pid = platform.getPid();
        self.header.start_timestamp_ms = platform.Clock.epochMillis();
        self.header.heartbeat_timeout_ms = opts.heartbeat_timeout_ms;
        self.aeron_discovery.* = .{
            .broker_ingress_stream_id = opts.broker_ingress_stream_id,
            .broker_start_timestamp_ms = opts.broker_start_timestamp_ms,
        };
        copyAeronDirectory(
            &self.aeron_discovery.aeron_directory,
            &self.aeron_discovery.aeron_directory_length,
            opts.aeron_directory,
        );
        self.storeMetadataMonitoringVersion(constants.metadata_monitoring_version);
        self.storeCounterValuesBufferLength(@intCast(counter_values_len));
        self.storeCounterMetadataBufferLength(@intCast(counter_metadata_len));
        self.storeErrorLogBufferLength(@intCast(error_log_len));
        self.storeMonitoringTailOffset(@intCast(monitoring_tail_offset));
        self.storeMonitoringTailLength(@intCast(monitoring_tail_len));
        self.storeAeronDiscoveryRegionOffset(@intCast(aeron_region_offset));
        self.storeAeronDiscoveryRegionLength(@intCast(aeron_region_len));

        // Write initial heartbeat.
        self.storeHeartbeat(platform.Clock.epochMillis());

        // If blocking, initialize the wait timeout.
        if (self.blocking_trailer) |trailer| {
            // Default: 1 second timeout for blocking waits.
            trailer.wait_timeout.value = 1_000_000_000; // 1s in nanoseconds
        }

        return self;
    }

    /// Open an existing service metadata file.
    pub fn open(
        storage_path: []const u8,
        group: []const u8,
        service_name: []const u8,
        service_id: i32,
        node_id: i16,
    ) !ServiceMetadataFile {
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const path = try buildServicePath(
            &path_buf,
            storage_path,
            group,
            service_name,
            service_id,
            node_id,
        );

        return openPath(path) catch |err| switch (err) {
            error.FileNotFound => {
                var legacy_path_buf: [std.fs.max_path_bytes]u8 = undefined;
                const legacy_path = try buildLegacyServicePath(
                    &legacy_path_buf,
                    storage_path,
                    group,
                    service_name,
                    service_id,
                );
                return openPath(legacy_path);
            },
            else => return err,
        };
    }

    fn openPath(path: []const u8) !ServiceMetadataFile {
        const fd = try platform.openFile(path);
        errdefer platform.closeFd(fd);

        const file_size = try platform.fileSize(fd);
        const mapped = try platform.mmap(fd, file_size);
        errdefer platform.munmap(mapped);

        if (file_size < constants.metadata_header_length)
            return error.FileTooSmall;

        const header: *ServiceMetadataHeader = @ptrCast(@alignCast(mapped.ptr));
        if (header.metadata_version != constants.metadata_version)
            return error.UnsupportedMetadataVersion;

        const ctrl_len: usize = @intCast(header.control_buffer_length);
        const msgs_len: usize = @intCast(header.messages_buffer_length);
        const is_blocking = header.blocking_mode == 1;

        if (!constants.isPowerOfTwo(ctrl_len))
            return error.ControlBufferNotPowerOfTwo;
        if (!constants.isPowerOfTwo(msgs_len))
            return error.MessagesBufferNotPowerOfTwo;

        const blocking_extra: usize = if (is_blocking) constants.blocking_trailer_length else 0;
        const aeron_region_offset: usize = @intCast(loadAt(i64, mapped, constants.aeron_discovery_region_offset_offset));
        const aeron_region_len: usize = @intCast(loadAt(i64, mapped, constants.aeron_discovery_region_length_offset));
        if (aeron_region_offset < constants.metadata_header_length)
            return error.FileSizeMismatch;
        if (aeron_region_len < @sizeOf(ServiceAeronDiscovery))
            return error.FileSizeMismatch;
        const blocking_offset = aeron_region_offset + aeron_region_len;
        const buffers_offset = blocking_offset + blocking_extra;

        const trailer = constants.ring_buffer_trailer_length;
        const ctrl_region = ctrl_len + trailer;
        const msgs_region = msgs_len + trailer;

        const base_size = buffers_offset + ctrl_region + msgs_region;
        const counter_values_len: usize = @intCast(loadAt(i32, mapped, constants.counter_values_length_offset));
        const counter_metadata_len: usize = @intCast(loadAt(i32, mapped, constants.counter_metadata_length_offset));
        const error_log_len: usize = @intCast(loadAt(i32, mapped, constants.error_log_length_offset));
        const monitoring_tail_offset: usize = @intCast(loadAt(i64, mapped, constants.monitoring_tail_offset_offset));
        const monitoring_tail_len: usize = @intCast(loadAt(i64, mapped, constants.monitoring_tail_length_offset));
        if (monitoring_tail_len != counter_values_len + counter_metadata_len + error_log_len)
            return error.FileSizeMismatch;
        if (monitoring_tail_offset < base_size)
            return error.FileSizeMismatch;
        if (counter_values_len % counters.counter_value_length != 0)
            return error.FileSizeMismatch;
        if (counter_metadata_len % counters.counter_metadata_length != 0)
            return error.FileSizeMismatch;

        const expected = constants.alignUp(
            monitoring_tail_offset + monitoring_tail_len,
            constants.page_size,
        );
        if (file_size < expected)
            return error.FileSizeMismatch;

        const counter_values_offset = monitoring_tail_offset;
        const counter_metadata_offset = counter_values_offset + counter_values_len;
        const error_log_offset = counter_metadata_offset + counter_metadata_len;

        return ServiceMetadataFile{
            .mapped_bytes = mapped,
            .header = header,
            .blocking_trailer = if (is_blocking)
                @ptrCast(@alignCast(mapped.ptr + blocking_offset))
            else
                null,
            .aeron_discovery = @ptrCast(@alignCast(mapped.ptr + aeron_region_offset)),
            .control_buffer = mapped[buffers_offset..][0..ctrl_region],
            .messages_buffer = mapped[buffers_offset + ctrl_region ..][0..msgs_region],
            .counter_values_buffer = @alignCast(mapped[counter_values_offset..][0..counter_values_len]),
            .counter_metadata_buffer = mapped[counter_metadata_offset..][0..counter_metadata_len],
            .error_log_buffer = mapped[error_log_offset..][0..error_log_len],
            .fd = fd,
        };
    }

    // ── Atomic Accessors ──────────────────────────────────────────────

    pub fn loadHeartbeat(self: *const ServiceMetadataFile) i64 {
        const ptr = self.heartbeatPtr();
        return @atomicLoad(i64, ptr, .acquire);
    }

    pub fn storeHeartbeat(self: *ServiceMetadataFile, time_ms: i64) void {
        const ptr = self.heartbeatPtr();
        @atomicStore(i64, ptr, time_ms, .release);
    }

    pub fn loadMetadataMonitoringVersion(self: *const ServiceMetadataFile) i32 {
        return @atomicLoad(i32, self.metadataMonitoringVersionPtr(), .acquire);
    }

    pub fn storeMetadataMonitoringVersion(self: *ServiceMetadataFile, value: i32) void {
        @atomicStore(i32, self.metadataMonitoringVersionPtr(), value, .release);
    }

    pub fn loadCounterValuesBufferLength(self: *const ServiceMetadataFile) i32 {
        return @atomicLoad(i32, self.counterValuesBufferLengthPtr(), .acquire);
    }

    pub fn storeCounterValuesBufferLength(self: *ServiceMetadataFile, value: i32) void {
        @atomicStore(i32, self.counterValuesBufferLengthPtr(), value, .release);
    }

    pub fn loadCounterMetadataBufferLength(self: *const ServiceMetadataFile) i32 {
        return @atomicLoad(i32, self.counterMetadataBufferLengthPtr(), .acquire);
    }

    pub fn storeCounterMetadataBufferLength(self: *ServiceMetadataFile, value: i32) void {
        @atomicStore(i32, self.counterMetadataBufferLengthPtr(), value, .release);
    }

    pub fn loadErrorLogBufferLength(self: *const ServiceMetadataFile) i32 {
        return @atomicLoad(i32, self.errorLogBufferLengthPtr(), .acquire);
    }

    pub fn storeErrorLogBufferLength(self: *ServiceMetadataFile, value: i32) void {
        @atomicStore(i32, self.errorLogBufferLengthPtr(), value, .release);
    }

    pub fn loadMonitoringTailOffset(self: *const ServiceMetadataFile) i64 {
        return @atomicLoad(i64, self.monitoringTailOffsetPtr(), .acquire);
    }

    pub fn storeMonitoringTailOffset(self: *ServiceMetadataFile, value: i64) void {
        @atomicStore(i64, self.monitoringTailOffsetPtr(), value, .release);
    }

    pub fn loadMonitoringTailLength(self: *const ServiceMetadataFile) i64 {
        return @atomicLoad(i64, self.monitoringTailLengthPtr(), .acquire);
    }

    pub fn storeMonitoringTailLength(self: *ServiceMetadataFile, value: i64) void {
        @atomicStore(i64, self.monitoringTailLengthPtr(), value, .release);
    }

    pub fn loadAeronDiscoveryRegionOffset(self: *const ServiceMetadataFile) i64 {
        return @atomicLoad(i64, self.aeronDiscoveryRegionOffsetPtr(), .acquire);
    }

    pub fn storeAeronDiscoveryRegionOffset(self: *ServiceMetadataFile, value: i64) void {
        @atomicStore(i64, self.aeronDiscoveryRegionOffsetPtr(), value, .release);
    }

    pub fn loadAeronDiscoveryRegionLength(self: *const ServiceMetadataFile) i64 {
        return @atomicLoad(i64, self.aeronDiscoveryRegionLengthPtr(), .acquire);
    }

    pub fn storeAeronDiscoveryRegionLength(self: *ServiceMetadataFile, value: i64) void {
        @atomicStore(i64, self.aeronDiscoveryRegionLengthPtr(), value, .release);
    }

    /// Returns true if this service's process is still alive.
    pub fn isProcessAlive(self: *const ServiceMetadataFile) bool {
        return platform.isProcessAlive(@intCast(self.header.pid));
    }

    /// Returns true if blocking mode is enabled for this service.
    pub fn isBlocking(self: *const ServiceMetadataFile) bool {
        return self.header.blocking_mode == 1;
    }

    // ── Buffer Accessors ──────────────────────────────────────────────

    pub fn getControlBuffer(self: *const ServiceMetadataFile) []u8 {
        return self.control_buffer;
    }

    pub fn getMessagesBuffer(self: *const ServiceMetadataFile) []u8 {
        return self.messages_buffer;
    }

    pub fn getAeronDiscovery(self: *const ServiceMetadataFile) *const ServiceAeronDiscovery {
        return self.aeron_discovery;
    }

    pub fn getCounterValuesBuffer(self: *const ServiceMetadataFile) []align(constants.cache_line_pad) u8 {
        return self.counter_values_buffer;
    }

    pub fn getCounterMetadataBuffer(self: *const ServiceMetadataFile) []u8 {
        return self.counter_metadata_buffer;
    }

    pub fn getErrorLogBuffer(self: *const ServiceMetadataFile) []u8 {
        return self.error_log_buffer;
    }

    pub fn getBlockingTrailer(self: *const ServiceMetadataFile) ?*BlockingTrailer {
        return self.blocking_trailer;
    }

    // ── Cleanup ───────────────────────────────────────────────────────

    pub fn close(self: *ServiceMetadataFile) void {
        platform.munmap(self.mapped_bytes);
        platform.closeFd(self.fd);
        self.* = undefined;
    }

    // ── Internal ──────────────────────────────────────────────────────

    fn heartbeatPtr(self: *const ServiceMetadataFile) *volatile i64 {
        const base: [*]u8 = self.mapped_bytes.ptr;
        return @ptrCast(@alignCast(base + constants.heartbeat_offset_within_header));
    }

    fn metadataMonitoringVersionPtr(self: *const ServiceMetadataFile) *volatile i32 {
        const base: [*]u8 = self.mapped_bytes.ptr;
        return @ptrCast(@alignCast(base + constants.metadata_monitoring_version_offset));
    }

    fn counterValuesBufferLengthPtr(self: *const ServiceMetadataFile) *volatile i32 {
        const base: [*]u8 = self.mapped_bytes.ptr;
        return @ptrCast(@alignCast(base + constants.counter_values_length_offset));
    }

    fn counterMetadataBufferLengthPtr(self: *const ServiceMetadataFile) *volatile i32 {
        const base: [*]u8 = self.mapped_bytes.ptr;
        return @ptrCast(@alignCast(base + constants.counter_metadata_length_offset));
    }

    fn errorLogBufferLengthPtr(self: *const ServiceMetadataFile) *volatile i32 {
        const base: [*]u8 = self.mapped_bytes.ptr;
        return @ptrCast(@alignCast(base + constants.error_log_length_offset));
    }

    fn monitoringTailOffsetPtr(self: *const ServiceMetadataFile) *volatile i64 {
        const base: [*]u8 = self.mapped_bytes.ptr;
        return @ptrCast(@alignCast(base + constants.monitoring_tail_offset_offset));
    }

    fn monitoringTailLengthPtr(self: *const ServiceMetadataFile) *volatile i64 {
        const base: [*]u8 = self.mapped_bytes.ptr;
        return @ptrCast(@alignCast(base + constants.monitoring_tail_length_offset));
    }

    fn aeronDiscoveryRegionOffsetPtr(self: *const ServiceMetadataFile) *volatile i64 {
        const base: [*]u8 = self.mapped_bytes.ptr;
        return @ptrCast(@alignCast(base + constants.aeron_discovery_region_offset_offset));
    }

    fn aeronDiscoveryRegionLengthPtr(self: *const ServiceMetadataFile) *volatile i64 {
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

    fn copyAeronDirectory(
        dest: *[constants.max_aeron_directory_length]u8,
        length: *u16,
        value: []const u8,
    ) void {
        @memset(dest, 0);
        @memcpy(dest[0..value.len], value);
        length.* = @intCast(value.len);
    }

    fn loadAt(comptime T: type, mapped: []align(constants.page_size) u8, offset: usize) T {
        const ptr: *const volatile T = @ptrCast(@alignCast(mapped.ptr + offset));
        return @atomicLoad(T, ptr, .acquire);
    }

    fn buildServicePath(
        buf: *[std.fs.max_path_bytes]u8,
        storage_path: []const u8,
        group: []const u8,
        service_name: []const u8,
        service_id: i32,
        node_id: i16,
    ) ![]const u8 {
        return std.fmt.bufPrint(buf, "{s}/{s}/services/{s}_node{d}_{d}.dat", .{
            storage_path,
            group,
            service_name,
            node_id,
            service_id,
        }) catch return error.PathTooLong;
    }

    fn buildLegacyServicePath(
        buf: *[std.fs.max_path_bytes]u8,
        storage_path: []const u8,
        group: []const u8,
        service_name: []const u8,
        service_id: i32,
    ) ![]const u8 {
        return std.fmt.bufPrint(buf, "{s}/{s}/services/{s}_{d}.dat", .{
            storage_path,
            group,
            service_name,
            service_id,
        }) catch return error.PathTooLong;
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

test "ServiceMetadataHeader has correct size" {
    try testing.expectEqual(@as(usize, 48), @sizeOf(ServiceMetadataHeader));
}

test "BlockingTrailer has correct size" {
    try testing.expectEqual(@as(usize, 384), @sizeOf(BlockingTrailer));
}

test "create service metadata file without blocking — verify offsets" {
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(storage_path);
    try tmp_dir.dir.createDirPath(testing.io, "test-group/services");

    var file = try ServiceMetadataFile.create(.{
        .storage_path = storage_path,
        .group = "test-group",
        .service_name = "pricing",
        .service_id = 5,
        .node_id = 1,
        .blocking_mode = false,
        .control_buffer_length = 64 * 1024,
        .messages_buffer_length = 256 * 1024,
    });
    defer file.close();

    // Header fields.
    try testing.expectEqual(constants.metadata_version, file.header.metadata_version);
    try testing.expectEqual(@as(i32, 5), file.header.service_id);
    try testing.expectEqual(@as(i16, 1), file.header.node_id);
    try testing.expectEqual(@as(i16, 0), file.header.blocking_mode);
    try testing.expect(!file.isBlocking());
    try testing.expect(file.blocking_trailer == null);

    // Non-blocking: control buffer starts after the fixed Aeron discovery region.
    const aeron_region_len = constants.alignUp(@sizeOf(ServiceAeronDiscovery), constants.cache_line_pad);
    const ctrl_offset = @intFromPtr(file.control_buffer.ptr) - @intFromPtr(file.mapped_bytes.ptr);
    try testing.expectEqual(@as(usize, 512 + aeron_region_len), ctrl_offset);

    // Messages buffer starts right after control buffer (including ring buffer trailer).
    const rb_trailer = constants.ring_buffer_trailer_length;
    const msgs_offset = @intFromPtr(file.messages_buffer.ptr) - @intFromPtr(file.mapped_bytes.ptr);
    try testing.expectEqual(@as(usize, 512 + aeron_region_len + 64 * 1024 + rb_trailer), msgs_offset);
}

test "create service metadata file with blocking — verify offsets include trailer" {
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(storage_path);
    try tmp_dir.dir.createDirPath(testing.io, "test-group/services");

    var file = try ServiceMetadataFile.create(.{
        .storage_path = storage_path,
        .group = "test-group",
        .service_name = "orders",
        .service_id = 7,
        .node_id = 2,
        .blocking_mode = true,
        .control_buffer_length = 64 * 1024,
        .messages_buffer_length = 128 * 1024,
    });
    defer file.close();

    try testing.expectEqual(@as(i16, 1), file.header.blocking_mode);
    try testing.expect(file.isBlocking());
    try testing.expect(file.blocking_trailer != null);

    // Blocking: control buffer starts after header + Aeron discovery + trailer.
    const aeron_region_len = constants.alignUp(@sizeOf(ServiceAeronDiscovery), constants.cache_line_pad);
    const ctrl_offset = @intFromPtr(file.control_buffer.ptr) - @intFromPtr(file.mapped_bytes.ptr);
    try testing.expectEqual(@as(usize, 512 + aeron_region_len + 384), ctrl_offset);

    // Messages buffer starts at 896 + 64 KB + ring buffer trailer.
    const rb_trailer = constants.ring_buffer_trailer_length;
    const msgs_offset = @intFromPtr(file.messages_buffer.ptr) - @intFromPtr(file.mapped_bytes.ptr);
    try testing.expectEqual(@as(usize, 512 + aeron_region_len + 384 + 64 * 1024 + rb_trailer), msgs_offset);

    // Blocking trailer should have the default timeout.
    const trailer = file.blocking_trailer.?;
    try testing.expectEqual(@as(i64, 1_000_000_000), trailer.wait_timeout.value);
}

test "service metadata v2 Aeron discovery round trip keeps messages buffer" {
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(storage_path);
    try tmp_dir.dir.createDirPath(testing.io, "test-group/services");

    var file = try ServiceMetadataFile.create(.{
        .storage_path = storage_path,
        .group = "test-group",
        .service_name = "orders",
        .service_id = 7,
        .node_id = 2,
        .messages_buffer_length = 128 * 1024,
        .aeron_directory = "/tmp/ringloom-aeron-service",
        .broker_ingress_stream_id = 10_007,
        .broker_start_timestamp_ms = 1234,
    });
    file.close();

    var opened = try ServiceMetadataFile.open(storage_path, "test-group", "orders", 7, 2);
    defer opened.close();

    const discovery = opened.getAeronDiscovery();
    try testing.expectEqual(@as(i32, 10_007), discovery.broker_ingress_stream_id);
    try testing.expectEqual(@as(i64, 1234), discovery.broker_start_timestamp_ms);
    try testing.expectEqualStrings("/tmp/ringloom-aeron-service", discovery.directory());
    try testing.expectEqual(
        @as(usize, 128 * 1024 + constants.ring_buffer_trailer_length),
        opened.getMessagesBuffer().len,
    );
}

test "heartbeat read and write are consistent" {
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(storage_path);
    try tmp_dir.dir.createDirPath(testing.io, "test-group/services");

    var file = try ServiceMetadataFile.create(.{
        .storage_path = storage_path,
        .group = "test-group",
        .service_name = "test-svc",
        .service_id = 1,
        .node_id = 0,
    });
    defer file.close();

    file.storeHeartbeat(123456789);
    try testing.expectEqual(@as(i64, 123456789), file.loadHeartbeat());

    file.storeHeartbeat(987654321);
    try testing.expectEqual(@as(i64, 987654321), file.loadHeartbeat());
}

test "service metadata path includes node id" {
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realPathFileAlloc(testing.io, ".", testing.allocator);
    defer testing.allocator.free(storage_path);
    try tmp_dir.dir.createDirPath(testing.io, "test-group/services");

    var node1 = try ServiceMetadataFile.create(.{
        .storage_path = storage_path,
        .group = "test-group",
        .service_name = "shared",
        .service_id = 1,
        .node_id = 1,
    });
    defer node1.close();

    var node2 = try ServiceMetadataFile.create(.{
        .storage_path = storage_path,
        .group = "test-group",
        .service_name = "shared",
        .service_id = 1,
        .node_id = 2,
    });
    defer node2.close();

    try testing.expectEqual(@as(i16, 1), node1.header.node_id);
    try testing.expectEqual(@as(i16, 2), node2.header.node_id);
}
