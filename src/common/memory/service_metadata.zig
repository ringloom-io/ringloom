//! Service metadata file layout for the BRZ broker.
//!
//! Each service on a host has a metadata file on /dev/shm containing:
//! - A header with service identity and configuration
//! - An optional blocking trailer for kernel-assisted producer parking
//! - A control ring buffer (broker → service)
//! - A messages ring buffer (producers → service)

const std = @import("std");
const constants = @import("constants.zig");
const platform = @import("../platform.zig");

/// Overlay for the fixed fields of the service metadata header.
pub const ServiceMetadataHeader = extern struct {
    control_buffer_length: i32,
    messages_buffer_length: i32,
    service_id: i32,
    node_id: i16,
    blocking_mode: i16, // 0 = non-blocking, 1 = blocking
    pid: i64,
    start_timestamp_ms: i64,
    heartbeat_timeout_ms: i32,

    comptime {
        std.debug.assert(@sizeOf(ServiceMetadataHeader) == 40);
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

    control_buffer: []u8,
    messages_buffer: []u8,
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
    };

    /// Create and initialize a new service metadata file.
    pub fn create(opts: CreateOptions) !ServiceMetadataFile {
        if (!constants.isPowerOfTwo(opts.control_buffer_length))
            return error.ControlBufferNotPowerOfTwo;
        if (!constants.isPowerOfTwo(opts.messages_buffer_length))
            return error.MessagesBufferNotPowerOfTwo;

        const blocking_extra: usize = if (opts.blocking_mode)
            constants.blocking_trailer_length
        else
            0;

        // Ring buffers need data_capacity + trailer. The caller specifies data
        // capacity (power-of-two); we add the trailer here.
        const rb_trailer = constants.ring_buffer_trailer_length;
        const ctrl_region = opts.control_buffer_length + rb_trailer;
        const msgs_region = opts.messages_buffer_length + rb_trailer;

        const total_size = constants.alignUp(
            constants.metadata_header_length +
                blocking_extra +
                ctrl_region +
                msgs_region,
            constants.page_size,
        );

        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const path = try buildServicePath(
            &path_buf,
            opts.storage_path,
            opts.group,
            opts.service_name,
            opts.service_id,
        );

        try ensureDirectoryExists(path);

        const fd = try platform.createFile(path);
        errdefer platform.closeFd(fd);
        try platform.ftruncate(fd, total_size);

        const mapped = try platform.mmap(fd, total_size);
        errdefer platform.munmap(mapped);

        @memset(mapped, 0);

        const buffers_offset = constants.metadata_header_length + blocking_extra;

        var self = ServiceMetadataFile{
            .mapped_bytes = mapped,
            .header = @ptrCast(@alignCast(mapped.ptr)),
            .blocking_trailer = if (opts.blocking_mode)
                @ptrCast(@alignCast(mapped.ptr + constants.metadata_header_length))
            else
                null,
            .control_buffer = mapped[buffers_offset..][0..ctrl_region],
            .messages_buffer = mapped[buffers_offset + ctrl_region ..][0..msgs_region],
            .fd = fd,
        };

        // Write immutable header fields — store data capacity (without trailer).
        self.header.control_buffer_length = @intCast(opts.control_buffer_length);
        self.header.messages_buffer_length = @intCast(opts.messages_buffer_length);
        self.header.service_id = opts.service_id;
        self.header.node_id = opts.node_id;
        self.header.blocking_mode = if (opts.blocking_mode) 1 else 0;
        self.header.pid = platform.getPid();
        self.header.start_timestamp_ms = platform.epochMillis();
        self.header.heartbeat_timeout_ms = opts.heartbeat_timeout_ms;

        // Write initial heartbeat.
        self.storeHeartbeat(platform.epochMillis());

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
    ) !ServiceMetadataFile {
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const path = try buildServicePath(
            &path_buf,
            storage_path,
            group,
            service_name,
            service_id,
        );

        const fd = try platform.openFile(path);
        errdefer platform.closeFd(fd);

        const file_size = try platform.fileSize(fd);
        const mapped = try platform.mmap(fd, file_size);
        errdefer platform.munmap(mapped);

        if (file_size < constants.metadata_header_length)
            return error.FileTooSmall;

        const header: *ServiceMetadataHeader = @ptrCast(@alignCast(mapped.ptr));

        const ctrl_len: usize = @intCast(header.control_buffer_length);
        const msgs_len: usize = @intCast(header.messages_buffer_length);
        const is_blocking = header.blocking_mode == 1;

        if (!constants.isPowerOfTwo(ctrl_len))
            return error.ControlBufferNotPowerOfTwo;
        if (!constants.isPowerOfTwo(msgs_len))
            return error.MessagesBufferNotPowerOfTwo;

        const blocking_extra: usize = if (is_blocking) constants.blocking_trailer_length else 0;
        const buffers_offset = constants.metadata_header_length + blocking_extra;

        const trailer = constants.ring_buffer_trailer_length;
        const ctrl_region = ctrl_len + trailer;
        const msgs_region = msgs_len + trailer;

        const expected = constants.alignUp(
            buffers_offset + ctrl_region + msgs_region,
            constants.page_size,
        );
        if (file_size < expected)
            return error.FileSizeMismatch;

        return ServiceMetadataFile{
            .mapped_bytes = mapped,
            .header = header,
            .blocking_trailer = if (is_blocking)
                @ptrCast(@alignCast(mapped.ptr + constants.metadata_header_length))
            else
                null,
            .control_buffer = mapped[buffers_offset..][0..ctrl_region],
            .messages_buffer = mapped[buffers_offset + ctrl_region ..][0..msgs_region],
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

    fn buildServicePath(
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
        const io = std.Io.Threaded.global_single_threaded.io();
        var root_dir = std.Io.Dir.openDirAbsolute(io, "/", .{}) catch return error.FileNotFound;
        defer root_dir.close(io);
        const relative = if (dir_path.len > 0 and dir_path[0] == '/') dir_path[1..] else dir_path;
        root_dir.createDirPath(io, relative) catch return error.FileNotFound;
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "ServiceMetadataHeader has correct size" {
    try testing.expectEqual(@as(usize, 40), @sizeOf(ServiceMetadataHeader));
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
    try testing.expectEqual(@as(i32, 5), file.header.service_id);
    try testing.expectEqual(@as(i16, 1), file.header.node_id);
    try testing.expectEqual(@as(i16, 0), file.header.blocking_mode);
    try testing.expect(!file.isBlocking());
    try testing.expect(file.blocking_trailer == null);

    // Non-blocking: control buffer starts at offset 512 (no blocking trailer).
    const ctrl_offset = @intFromPtr(file.control_buffer.ptr) - @intFromPtr(file.mapped_bytes.ptr);
    try testing.expectEqual(@as(usize, 512), ctrl_offset);

    // Messages buffer starts right after control buffer (including ring buffer trailer).
    const rb_trailer = constants.ring_buffer_trailer_length;
    const msgs_offset = @intFromPtr(file.messages_buffer.ptr) - @intFromPtr(file.mapped_bytes.ptr);
    try testing.expectEqual(@as(usize, 512 + 64 * 1024 + rb_trailer), msgs_offset);
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

    // Blocking: control buffer starts at 512 + 384 = 896.
    const ctrl_offset = @intFromPtr(file.control_buffer.ptr) - @intFromPtr(file.mapped_bytes.ptr);
    try testing.expectEqual(@as(usize, 512 + 384), ctrl_offset);

    // Messages buffer starts at 896 + 64 KB + ring buffer trailer.
    const rb_trailer = constants.ring_buffer_trailer_length;
    const msgs_offset = @intFromPtr(file.messages_buffer.ptr) - @intFromPtr(file.mapped_bytes.ptr);
    try testing.expectEqual(@as(usize, 512 + 384 + 64 * 1024 + rb_trailer), msgs_offset);

    // Blocking trailer should have the default timeout.
    const trailer = file.blocking_trailer.?;
    try testing.expectEqual(@as(i64, 1_000_000_000), trailer.wait_timeout.value);
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
