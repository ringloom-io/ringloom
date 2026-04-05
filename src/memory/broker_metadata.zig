//! Broker metadata file layout for the BRZ broker.
//!
//! The broker's metadata file is memory-mapped on /dev/shm and shared with
//! all local services. It contains the broker's control ring buffer
//! (services → broker) and send ring buffer (services → broker for cross-host).

const std = @import("std");
const constants = @import("constants.zig");
const platform = @import("../platform.zig");

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

    comptime {
        // The fixed fields must pack to exactly 32 bytes.
        std.debug.assert(@sizeOf(BrokerMetadataHeader) == 32);
    }
};

pub const BrokerMetadataFile = struct {
    /// The full mmap'd region.
    mapped_bytes: []align(constants.page_size) u8,

    /// Pointer to the header overlay (first 32 bytes).
    header: *BrokerMetadataHeader,

    /// Byte slice covering the control ring buffer region.
    control_buffer: []u8,

    /// Byte slice covering the send ring buffer region.
    send_buffer: []u8,

    /// File descriptor (kept open for the lifetime of the mapping).
    fd: std.posix.fd_t,

    const Self = @This();

    // ── Construction ──────────────────────────────────────────────────

    /// Create and initialize a new broker metadata file.
    ///
    /// - `storage_path`: e.g. "/dev/shm"
    /// - `group`: e.g. "default"
    /// - `node_id`: this broker's node identifier
    /// - `control_buffer_length`: must be a power of two
    /// - `messages_buffer_length`: must be a power of two
    pub fn create(
        storage_path: []const u8,
        group: []const u8,
        node_id: i16,
        control_buffer_length: usize,
        messages_buffer_length: usize,
    ) !BrokerMetadataFile {
        if (!constants.isPowerOfTwo(control_buffer_length))
            return error.ControlBufferNotPowerOfTwo;
        if (!constants.isPowerOfTwo(messages_buffer_length))
            return error.MessagesBufferNotPowerOfTwo;

        const total_size = constants.alignUp(
            constants.metadata_header_length + control_buffer_length + messages_buffer_length,
            constants.page_size,
        );

        // Build path: <storage_path>/<group>/services/broker_0.dat
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const path = try buildBrokerPath(&path_buf, storage_path, group);

        // Ensure parent directory exists.
        try ensureDirectoryExists(path);

        // Create file, truncate to total_size, mmap.
        const fd = try platform.createFile(path);
        errdefer std.posix.close(fd);
        try platform.ftruncate(fd, total_size);

        const mapped = try platform.mmap(fd, total_size);
        errdefer platform.munmap(mapped);

        // Zero-fill.
        @memset(mapped, 0);

        var self = BrokerMetadataFile{
            .mapped_bytes = mapped,
            .header = @ptrCast(@alignCast(mapped.ptr)),
            .control_buffer = mapped[constants.metadata_header_length..][0..control_buffer_length],
            .send_buffer = mapped[constants.metadata_header_length + control_buffer_length ..][0..messages_buffer_length],
            .fd = fd,
        };

        // Write the immutable header fields.
        self.header.control_buffer_length = @intCast(control_buffer_length);
        self.header.messages_buffer_length = @intCast(messages_buffer_length);
        self.header.service_id = constants.broker_service_id;
        self.header.node_id = node_id;
        self.header.pid = platform.getPid();
        self.header.start_timestamp_ms = platform.epochMillis();

        // Initialize next_service_id to 1 (0 is reserved for broker).
        self.storeNextServiceId(1);

        // Write initial heartbeat.
        self.storeHeartbeat(platform.epochMillis());

        return self;
    }

    /// Open an existing broker metadata file (read-write).
    /// Validates that the header fields are internally consistent.
    pub fn open(
        storage_path: []const u8,
        group: []const u8,
    ) !BrokerMetadataFile {
        var path_buf: [std.fs.max_path_bytes]u8 = undefined;
        const path = try buildBrokerPath(&path_buf, storage_path, group);

        const fd = try platform.openFile(path);
        errdefer std.posix.close(fd);

        const file_size = try platform.fileSize(fd);
        const mapped = try platform.mmap(fd, file_size);
        errdefer platform.munmap(mapped);

        if (file_size < constants.metadata_header_length)
            return error.FileTooSmall;

        const header: *BrokerMetadataHeader = @ptrCast(@alignCast(mapped.ptr));

        const ctrl_len: usize = @intCast(header.control_buffer_length);
        const msgs_len: usize = @intCast(header.messages_buffer_length);

        if (!constants.isPowerOfTwo(ctrl_len))
            return error.ControlBufferNotPowerOfTwo;
        if (!constants.isPowerOfTwo(msgs_len))
            return error.MessagesBufferNotPowerOfTwo;

        const expected = constants.alignUp(
            constants.metadata_header_length + ctrl_len + msgs_len,
            constants.page_size,
        );
        if (file_size < expected)
            return error.FileSizeMismatch;

        return BrokerMetadataFile{
            .mapped_bytes = mapped,
            .header = header,
            .control_buffer = mapped[constants.metadata_header_length..][0..ctrl_len],
            .send_buffer = mapped[constants.metadata_header_length + ctrl_len ..][0..msgs_len],
            .fd = fd,
        };
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

    // ── Buffer Accessors ──────────────────────────────────────────────

    /// Returns the byte slice backing the control ring buffer.
    pub fn getControlBuffer(self: *const BrokerMetadataFile) []u8 {
        return self.control_buffer;
    }

    /// Returns the byte slice backing the send ring buffer.
    pub fn getSendBuffer(self: *const BrokerMetadataFile) []u8 {
        return self.send_buffer;
    }

    // ── Cleanup ───────────────────────────────────────────────────────

    /// Unmap the file and close the file descriptor.
    pub fn close(self: *BrokerMetadataFile) void {
        platform.munmap(self.mapped_bytes);
        std.posix.close(self.fd);
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

    fn buildBrokerPath(
        buf: *[std.fs.max_path_bytes]u8,
        storage_path: []const u8,
        group: []const u8,
    ) ![]const u8 {
        return std.fmt.bufPrint(buf, "{s}/{s}/services/broker_0.dat", .{
            storage_path,
            group,
        }) catch return error.PathTooLong;
    }

    fn ensureDirectoryExists(file_path: []const u8) !void {
        const dir = std.fs.path.dirname(file_path) orelse return error.InvalidPath;
        std.fs.makeDirAbsolute(dir) catch |err| switch (err) {
            error.PathAlreadyExists => {},
            else => return err,
        };
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "BrokerMetadataHeader has correct size" {
    try testing.expectEqual(@as(usize, 32), @sizeOf(BrokerMetadataHeader));
}

test "create broker metadata file and verify layout" {
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realpathAlloc(testing.allocator, ".");
    defer testing.allocator.free(storage_path);

    try tmp_dir.dir.makePath("test-group/services");

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

    // Verify buffer slice sizes.
    try testing.expectEqual(@as(usize, 64 * 1024), file.control_buffer.len);
    try testing.expectEqual(@as(usize, 1024 * 1024), file.send_buffer.len);

    // Verify control buffer starts at offset 512.
    const control_offset = @intFromPtr(file.control_buffer.ptr) - @intFromPtr(file.mapped_bytes.ptr);
    try testing.expectEqual(@as(usize, 512), control_offset);

    // Verify send buffer starts right after control buffer.
    const send_offset = @intFromPtr(file.send_buffer.ptr) - @intFromPtr(file.mapped_bytes.ptr);
    try testing.expectEqual(@as(usize, 512 + 64 * 1024), send_offset);

    // Verify nextServiceId initialized to 1.
    try testing.expectEqual(@as(i32, 1), file.loadNextServiceId());

    // Verify heartbeat was written.
    try testing.expect(file.loadHeartbeat() > 0);
}

test "incrementAndGetNextServiceId is atomic" {
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realpathAlloc(testing.allocator, ".");
    defer testing.allocator.free(storage_path);
    try tmp_dir.dir.makePath("test-group/services");

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
    const storage_path = try tmp_dir.dir.realpathAlloc(testing.allocator, ".");
    defer testing.allocator.free(storage_path);
    try tmp_dir.dir.makePath("test-group/services");

    // 1000 is not a power of two.
    const result = BrokerMetadataFile.create(storage_path, "test-group", 1, 1000, 64 * 1024);
    try testing.expectError(error.ControlBufferNotPowerOfTwo, result);
}

test "two threads share a broker metadata file" {
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realpathAlloc(testing.allocator, ".");
    defer testing.allocator.free(storage_path);
    try tmp_dir.dir.makePath("test-group/services");

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
    var service_view = try BrokerMetadataFile.open(storage_path, "test-group");
    defer service_view.close();

    // Verify the service sees the same data.
    const read_pattern = std.mem.bytesToValue(u64, service_view.control_buffer[0..8]);
    try testing.expectEqual(pattern, read_pattern);

    // Service writes to the send buffer.
    const msg: u64 = 0x1234567890ABCDEF;
    @memcpy(service_view.send_buffer[0..8], std.mem.asBytes(&msg));

    // Broker reads it back.
    const read_msg = std.mem.bytesToValue(u64, broker_file.send_buffer[0..8]);
    try testing.expectEqual(msg, read_msg);
}

test "atomic nextServiceId visible across mappings" {
    var tmp_dir = testing.tmpDir(.{});
    defer tmp_dir.cleanup();
    const storage_path = try tmp_dir.dir.realpathAlloc(testing.allocator, ".");
    defer testing.allocator.free(storage_path);
    try tmp_dir.dir.makePath("test-group/services");

    var file1 = try BrokerMetadataFile.create(storage_path, "test-group", 1, 4096, 4096);
    defer file1.close();

    var file2 = try BrokerMetadataFile.open(storage_path, "test-group");
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
    const storage_path = try tmp_dir.dir.realpathAlloc(testing.allocator, ".");
    defer testing.allocator.free(storage_path);
    try tmp_dir.dir.makePath("test-group/services");

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
