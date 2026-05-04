//! io_uring wrapper for the RingLoom broker (Linux only).
//!
//! Wraps the Zig standard library's `std.os.linux.IoUring` and adds:
//! - Ergonomic methods for UDP send/recv with `msghdr` setup
//! - Registered buffer integration
//! - Pending-submission tracking for batched submit
//! - Completion polling with a callback interface
//! - Multishot receive support
//! - SQPOLL mode configuration

const std = @import("std");
const linux = std.os.linux;
const posix = std.posix;
const constants = @import("ringloom_common").platform.constants;

/// Configuration for io_uring initialization.
pub const IoUringConfig = struct {
    /// Number of SQE slots (must be power of two, max 32768).
    queue_depth: u16 = 256,

    /// Number of CQE slots. Defaults to 4x SQ depth when zero.
    cq_depth: u32 = 0,

    /// Enable SQPOLL mode. Requires CAP_SYS_NICE or appropriate
    /// rlimit_memlock settings.
    sqpoll: bool = false,

    /// Enable SINGLE_ISSUER when available. Each broker event-loop owns its
    /// ring from one thread, so this is the preferred steady-state mode.
    single_issuer: bool = true,

    /// Enable COOP_TASKRUN when available to reduce forced task-work wakeups.
    coop_taskrun: bool = true,

    /// Ask the kernel to continue submitting a batch even if an SQE fails.
    submit_all: bool = true,

    /// SQPOLL kernel thread idle timeout in milliseconds. If no SQEs
    /// are submitted for this duration, the kernel thread goes to sleep.
    sqpoll_idle_ms: u32 = 1000,

    /// CPU to pin the SQPOLL kernel thread to (optional).
    sqpoll_cpu: ?u32 = null,
};

pub const IoUringCapabilities = struct {
    requested_flags: u32 = 0,
    active_flags: u32 = 0,
    features: u32 = 0,
    sqpoll_active: bool = false,
    single_issuer_active: bool = false,
    coop_taskrun_active: bool = false,
    submit_all_active: bool = false,
    accept_supported: bool = false,
    recv_supported: bool = false,
    writev_supported: bool = false,
    provided_buffers_supported: bool = false,
};

/// Wrapper around the Zig standard library's IoUring.
///
/// Provides ergonomic methods for UDP send/recv, batched submission,
/// completion polling, and registered buffer management.
pub const IoUring = struct {
    ring: linux.IoUring,
    capabilities: IoUringCapabilities,
    pending_submissions: u32 = 0,
    registered_buffers: bool = false,

    /// Internal CQE buffer for pollCompletions. Sized to handle a reasonable
    /// batch without dynamic allocation.
    cqe_buf: [64]linux.io_uring_cqe = undefined,

    const Self = @This();

    // ── Lifecycle ──────────────────────────────────────────────

    /// Initialize the io_uring instance with basic flags.
    ///
    /// `queue_depth` — number of SQE slots (must be power of two).
    /// `flags`       — io_uring setup flags (e.g. IORING_SETUP_SQPOLL).
    pub fn init(queue_depth: u16, flags: u32) !IoUring {
        const combined_flags = flags | linux.IORING_SETUP_CQSIZE;
        var params = std.mem.zeroInit(linux.io_uring_params, .{
            .flags = combined_flags,
            .cq_entries = @as(u32, queue_depth) * 2,
        });

        const ring = try linux.IoUring.init_params(queue_depth, &params);
        var capabilities = detectCapabilities(&ring, combined_flags);
        capabilities.requested_flags = combined_flags;
        capabilities.active_flags = params.flags;
        capabilities.features = ring.features;
        return .{
            .ring = ring,
            .capabilities = capabilities,
        };
    }

    /// Initialize with a configuration struct, including SQPOLL support.
    pub fn initWithConfig(config: IoUringConfig) !IoUring {
        var requested_flags: u32 = linux.IORING_SETUP_CQSIZE;

        if (config.sqpoll) {
            requested_flags |= linux.IORING_SETUP_SQPOLL;
        }
        if (config.single_issuer) {
            requested_flags |= linux.IORING_SETUP_SINGLE_ISSUER;
        }
        if (config.coop_taskrun) {
            requested_flags |= linux.IORING_SETUP_COOP_TASKRUN;
        }
        if (config.submit_all) {
            requested_flags |= linux.IORING_SETUP_SUBMIT_ALL;
        }

        const cq_depth = if (config.cq_depth == 0)
            @as(u32, config.queue_depth) * 4
        else
            config.cq_depth;

        return initWithFallback(config, requested_flags, cq_depth);
    }

    fn initWithFallback(config: IoUringConfig, requested_flags: u32, cq_depth: u32) !IoUring {
        var attempts = [_]u32{
            requested_flags,
            requested_flags & ~@as(u32, linux.IORING_SETUP_SQPOLL) & ~@as(u32, linux.IORING_SETUP_SQ_AFF),
            requested_flags & ~@as(u32, linux.IORING_SETUP_SQPOLL) & ~@as(u32, linux.IORING_SETUP_SQ_AFF) &
                ~@as(u32, linux.IORING_SETUP_COOP_TASKRUN),
            linux.IORING_SETUP_CQSIZE,
        };

        var last_err: anyerror = error.ArgumentsInvalid;
        for (&attempts) |*flags| {
            var params = std.mem.zeroInit(linux.io_uring_params, .{
                .flags = flags.*,
                .cq_entries = cq_depth,
                .sq_thread_idle = config.sqpoll_idle_ms,
            });

            if ((flags.* & linux.IORING_SETUP_SQPOLL) != 0) {
                if (config.sqpoll_cpu) |cpu| {
                    params.flags |= linux.IORING_SETUP_SQ_AFF;
                    params.sq_thread_cpu = cpu;
                }
            }

            const ring = linux.IoUring.init_params(config.queue_depth, &params) catch |err| {
                last_err = err;
                continue;
            };

            var capabilities = detectCapabilities(&ring, requested_flags);
            capabilities.requested_flags = requested_flags;
            capabilities.active_flags = params.flags;
            capabilities.features = ring.features;
            return .{
                .ring = ring,
                .capabilities = capabilities,
            };
        }

        return last_err;
    }

    fn detectCapabilities(ring: *const linux.IoUring, requested_flags: u32) IoUringCapabilities {
        var capabilities = IoUringCapabilities{
            .requested_flags = requested_flags,
            .active_flags = ring.flags,
            .features = ring.features,
            .sqpoll_active = (ring.flags & linux.IORING_SETUP_SQPOLL) != 0,
            .single_issuer_active = (ring.flags & linux.IORING_SETUP_SINGLE_ISSUER) != 0,
            .coop_taskrun_active = (ring.flags & linux.IORING_SETUP_COOP_TASKRUN) != 0,
            .submit_all_active = (ring.flags & linux.IORING_SETUP_SUBMIT_ALL) != 0,
        };

        var mutable_ring = ring.*;
        const probe = mutable_ring.get_probe() catch return capabilities;
        capabilities.accept_supported = probe.is_supported(.ACCEPT);
        capabilities.recv_supported = probe.is_supported(.RECV);
        capabilities.writev_supported = probe.is_supported(.WRITEV);
        capabilities.provided_buffers_supported = probe.is_supported(.PROVIDE_BUFFERS);
        return capabilities;
    }

    /// Clean up all io_uring resources.
    pub fn deinit(self: *Self) void {
        self.ring.deinit();
    }

    // ── Submission: UDP Send ───────────────────────────────────

    /// Queue a UDP sendmsg operation. Does NOT submit to kernel yet —
    /// call `submit()` or `submitAndWait()` after batching all SQEs.
    ///
    /// The caller must keep `msghdr_buf` and `iov` alive until the
    /// corresponding CQE is consumed.
    ///
    /// `user_data` is an opaque tag returned in the CQE for correlation.
    pub fn prepareSend(
        self: *Self,
        socket_fd: posix.fd_t,
        buf: []const u8,
        dest_addr: *const posix.sockaddr,
        dest_addr_len: posix.socklen_t,
        msghdr_buf: *posix.msghdr_const,
        iov: *posix.iovec_const,
        user_data: u64,
    ) !void {
        // Set up iovec
        iov.* = .{
            .base = buf.ptr,
            .len = buf.len,
        };

        // Set up msghdr_const
        msghdr_buf.* = .{
            .name = dest_addr,
            .namelen = dest_addr_len,
            .iov = @ptrCast(iov),
            .iovlen = 1,
            .control = null,
            .controllen = 0,
            .flags = 0,
        };

        _ = try self.ring.sendmsg(user_data, socket_fd, msghdr_buf, 0);
        self.pending_submissions += 1;
    }

    // ── Submission: UDP Receive ────────────────────────────────

    /// Queue a UDP recvmsg operation.
    ///
    /// The caller must keep `msghdr_buf` and `iov` alive until the
    /// corresponding CQE is consumed.
    pub fn prepareRecv(
        self: *Self,
        socket_fd: posix.fd_t,
        buf: []u8,
        src_addr: *posix.sockaddr,
        src_addr_len: *posix.socklen_t,
        msghdr_buf: *posix.msghdr,
        iov: *posix.iovec,
        user_data: u64,
    ) !void {
        iov.* = .{
            .base = buf.ptr,
            .len = buf.len,
        };

        msghdr_buf.* = .{
            .name = src_addr,
            .namelen = src_addr_len.*,
            .iov = @ptrCast(iov),
            .iovlen = 1,
            .control = null,
            .controllen = 0,
            .flags = 0,
        };

        _ = try self.ring.recvmsg(user_data, socket_fd, msghdr_buf, 0);
        self.pending_submissions += 1;
    }

    // ── Submission: Timeout ────────────────────────────────────

    /// Queue a timeout. The CQE fires after `ns` nanoseconds.
    pub fn prepareTimeout(self: *Self, ns: u64, user_data: u64) !void {
        const ts = linux.kernel_timespec{
            .sec = @intCast(ns / std.time.ns_per_s),
            .nsec = @intCast(ns % std.time.ns_per_s),
        };

        _ = try self.ring.timeout(user_data, &ts, 0, 0);
        self.pending_submissions += 1;
    }

    // ── Submission: NOP ────────────────────────────────────────

    /// Queue a no-op. Useful for waking up a blocked ring or
    /// flushing SQPOLL.
    pub fn prepareNop(self: *Self, user_data: u64) !void {
        _ = try self.ring.nop(user_data);
        self.pending_submissions += 1;
    }

    /// Queue a regular multishot accept operation. Direct descriptors are a
    /// later optimization; this keeps fallback to existing fd handling simple.
    pub fn prepareAcceptMultishot(
        self: *Self,
        listen_fd: posix.fd_t,
        user_data: u64,
        flags: u32,
    ) !void {
        _ = try self.ring.accept_multishot(user_data, listen_fd, null, null, flags);
        self.pending_submissions += 1;
    }

    /// Queue a multishot recv using a ring-provided buffer group.
    pub fn prepareRecvMultishot(
        self: *Self,
        group: *linux.IoUring.BufferGroup,
        socket_fd: posix.fd_t,
        user_data: u64,
        flags: u32,
    ) !void {
        _ = try group.recv_multishot(user_data, socket_fd, flags);
        self.pending_submissions += 1;
    }

    // ── Submit ─────────────────────────────────────────────────

    /// Submit all pending SQEs to the kernel. Returns the number submitted.
    /// This is a single `io_uring_enter()` call regardless of how many SQEs
    /// were queued.
    pub fn submit(self: *Self) !u32 {
        if (self.pending_submissions == 0) return 0;

        const submitted = try self.ring.submit();
        self.pending_submissions = 0;
        return submitted;
    }

    /// Submit all pending SQEs and wait for at least `min_complete`
    /// completions. Combines submit + wait into a single syscall.
    pub fn submitAndWait(self: *Self, min_complete: u32) !u32 {
        const submitted = try self.ring.submit_and_wait(min_complete);
        self.pending_submissions = 0;
        return submitted;
    }

    // ── Completion Polling ─────────────────────────────────────

    /// The result of a single completed I/O operation.
    pub const Completion = struct {
        user_data: u64,
        /// >=0 on success (bytes transferred), <0 on error (-errno)
        result: i32,
        flags: u32,
    };

    /// Poll for completed I/O operations. Calls `handler` for each CQE,
    /// up to `limit` completions. Returns number of completions processed.
    ///
    /// This does NOT block. If no completions are ready, returns 0.
    pub fn pollCompletions(
        self: *Self,
        comptime handler: fn (completion: Completion) void,
        limit: u32,
    ) u32 {
        const max = @min(limit, self.cqe_buf.len);
        const count = self.ring.copy_cqes(self.cqe_buf[0..max], 0) catch return 0;

        for (self.cqe_buf[0..count]) |cqe| {
            handler(.{
                .user_data = cqe.user_data,
                .result = cqe.res,
                .flags = cqe.flags,
            });
        }

        return count;
    }

    /// Poll completions using a runtime function pointer (for cases where
    /// comptime dispatch is not possible).
    pub fn pollCompletionsDynamic(
        self: *Self,
        context: *anyopaque,
        handler: *const fn (context: *anyopaque, completion: Completion) void,
        limit: u32,
    ) u32 {
        const max = @min(limit, self.cqe_buf.len);
        const count = self.ring.copy_cqes(self.cqe_buf[0..max], 0) catch return 0;

        for (self.cqe_buf[0..count]) |cqe| {
            handler(context, .{
                .user_data = cqe.user_data,
                .result = cqe.res,
                .flags = cqe.flags,
            });
        }

        return count;
    }

    // ── Registered Buffers ─────────────────────────────────────

    /// Register a set of fixed buffers with the kernel. Must be called once
    /// at init time. After registration, SQEs can reference buffers by index
    /// instead of pointer, avoiding per-I/O page table walks.
    pub fn registerBuffers(self: *Self, iovecs: []const posix.iovec) !void {
        try self.ring.register_buffers(iovecs);
        self.registered_buffers = true;
    }

    /// Unregister previously registered buffers.
    pub fn unregisterBuffers(self: *Self) !void {
        try self.ring.unregister_buffers();
        self.registered_buffers = false;
    }

    // ── Multishot Helpers ──────────────────────────────────────

    /// Extract the provided buffer index from a multishot CQE.
    pub fn extractBufferIndex(cqe_flags: u32) u16 {
        return @intCast(cqe_flags >> linux.IORING_CQE_BUFFER_SHIFT);
    }

    /// Check if a multishot CQE indicates more data is coming.
    pub fn isMultishotMore(cqe_flags: u32) bool {
        return (cqe_flags & linux.IORING_CQE_F_MORE) != 0;
    }
};

// ── Tests ─────────────────────────────────────────────────────────────

test "IoUring init and deinit" {
    if (comptime @import("builtin").os.tag != .linux) return error.SkipZigTest;

    var ring = try IoUring.init(32, 0);
    defer ring.deinit();

    try std.testing.expectEqual(@as(u32, 0), ring.pending_submissions);
    try std.testing.expect(!ring.registered_buffers);
}

test "IoUring NOP submit and complete" {
    if (comptime @import("builtin").os.tag != .linux) return error.SkipZigTest;

    // Given
    var ring = try IoUring.init(32, 0);
    defer ring.deinit();

    // When: submit a NOP
    try ring.prepareNop(0xDEADBEEF);
    try std.testing.expectEqual(@as(u32, 1), ring.pending_submissions);
    const submitted = try ring.submit();

    // Then: one SQE submitted
    try std.testing.expectEqual(@as(u32, 1), submitted);
    try std.testing.expectEqual(@as(u32, 0), ring.pending_submissions);

    // Wait for kernel to process, then poll
    _ = try ring.ring.submit_and_wait(1);

    // When: poll for completion
    const completed = ring.pollCompletions(struct {
        fn handler(_: IoUring.Completion) void {
            // Comptime fn can't capture, but we verify via count
        }
    }.handler, 10);

    // Then: got at least one CQE
    try std.testing.expect(completed >= 1);
}

test "IoUring multiple NOPs batched submit" {
    if (comptime @import("builtin").os.tag != .linux) return error.SkipZigTest;

    // Given
    var ring = try IoUring.init(32, 0);
    defer ring.deinit();

    // When: queue multiple NOPs
    try ring.prepareNop(1);
    try ring.prepareNop(2);
    try ring.prepareNop(3);
    try std.testing.expectEqual(@as(u32, 3), ring.pending_submissions);

    // Submit all at once
    const submitted = try ring.submit();

    // Then
    try std.testing.expectEqual(@as(u32, 3), submitted);
    try std.testing.expectEqual(@as(u32, 0), ring.pending_submissions);
}

test "IoUring submit with no pending returns zero" {
    if (comptime @import("builtin").os.tag != .linux) return error.SkipZigTest;

    var ring = try IoUring.init(32, 0);
    defer ring.deinit();

    const submitted = try ring.submit();
    try std.testing.expectEqual(@as(u32, 0), submitted);
}

test "IoUringConfig defaults" {
    const config = IoUringConfig{};
    try std.testing.expectEqual(@as(u16, 256), config.queue_depth);
    try std.testing.expect(!config.sqpoll);
    try std.testing.expectEqual(@as(u32, 1000), config.sqpoll_idle_ms);
    try std.testing.expect(config.sqpoll_cpu == null);
}

test "IoUring initWithConfig default" {
    if (comptime @import("builtin").os.tag != .linux) return error.SkipZigTest;

    var ring = try IoUring.initWithConfig(.{
        .queue_depth = 64,
    });
    defer ring.deinit();

    try std.testing.expectEqual(@as(u32, 0), ring.pending_submissions);
}

test "Completion struct layout" {
    const c = IoUring.Completion{
        .user_data = 42,
        .result = -11,
        .flags = 0,
    };
    try std.testing.expectEqual(@as(u64, 42), c.user_data);
    try std.testing.expectEqual(@as(i32, -11), c.result);
}

test "extractBufferIndex" {
    // Buffer index in upper 16 bits of flags
    const flags: u32 = 42 << linux.IORING_CQE_BUFFER_SHIFT;
    try std.testing.expectEqual(@as(u16, 42), IoUring.extractBufferIndex(flags));
}

test "isMultishotMore" {
    try std.testing.expect(IoUring.isMultishotMore(linux.IORING_CQE_F_MORE));
    try std.testing.expect(!IoUring.isMultishotMore(0));
}
