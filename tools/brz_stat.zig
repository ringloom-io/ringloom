//! brz-stat — External monitoring tool for the BRZ broker.
//!
//! Reads the broker's metadata file via mmap (read-only) and prints
//! counters and error log entries. Zero overhead on the broker process.
//!
//! Usage:
//!   brz-stat [path-to-broker.dat]
//!
//! If no path is given, defaults to /dev/shm/brz/services/broker_0.dat.

const std = @import("std");

const metadata_header_length: usize = 512;
const ring_buffer_trailer_length: usize = 768;
const counter_value_length: usize = 128;
const counter_metadata_length: usize = 256;
const entry_header_length: usize = 24;
const entry_alignment: usize = 8;

const BrokerMetadataHeader = extern struct {
    control_buffer_length: i32,
    messages_buffer_length: i32,
    service_id: i32,
    node_id: i16,
    _padding0: i16,
    pid: i64,
    start_timestamp_ms: i64,
    _reserved: [224]u8,
    heartbeat_time_ms: i64, // offset 256
    _padding1: [24]u8,
    next_service_id: i32, // offset 288
    _padding2: [212]u8,
    // Extended fields:
    counter_values_buffer_size: i32,
    error_log_buffer_size: i32,
};

const FdWriter = std.io.GenericWriter(std.posix.fd_t, std.posix.WriteError, std.posix.write);

fn fdWriter(fd: std.posix.fd_t) FdWriter {
    return .{ .context = fd };
}

pub fn main() !void {
    const allocator = std.heap.page_allocator;
    const args = try std.process.argsAlloc(allocator);
    defer std.process.argsFree(allocator, args);

    const path = if (args.len > 1) args[1] else "/dev/shm/brz/services/broker_0.dat";

    const stdout = fdWriter(std.posix.STDOUT_FILENO);
    const stderr = fdWriter(std.posix.STDERR_FILENO);

    // Open and mmap the file read-only.
    const file = std.fs.cwd().openFile(path, .{ .mode = .read_only }) catch |err| {
        try stderr.print("error: cannot open '{s}': {s}\n", .{ path, @errorName(err) });
        std.process.exit(1);
    };
    defer file.close();

    const stat = try file.stat();
    if (stat.size < metadata_header_length) {
        try stderr.print("error: file too small ({d} bytes) — not a valid broker metadata file\n", .{stat.size});
        std.process.exit(1);
    }

    const mapped = try std.posix.mmap(
        null,
        stat.size,
        std.posix.PROT.READ,
        .{ .TYPE = .SHARED },
        file.handle,
        0,
    );
    defer std.posix.munmap(mapped);

    // Read header to find buffer offsets.
    const header: *const BrokerMetadataHeader = @ptrCast(@alignCast(mapped.ptr));
    const control_buf_size: usize = @intCast(header.control_buffer_length);
    const send_buf_size: usize = @intCast(header.messages_buffer_length);

    // Calculate offsets (must match broker's layout computation).
    const control_end = metadata_header_length + control_buf_size + ring_buffer_trailer_length;
    const send_end = control_end + send_buf_size + ring_buffer_trailer_length;

    // Counter values buffer starts at send_end.
    const counter_values_offset = send_end;
    const counter_values_size: usize = @intCast(header.counter_values_buffer_size);
    const max_counter_id = counter_values_size / counter_value_length;
    const counter_metadata_offset = counter_values_offset + counter_values_size;
    const counter_metadata_size = max_counter_id * counter_metadata_length;
    const error_log_offset = counter_metadata_offset + counter_metadata_size;
    const error_log_size: usize = @intCast(header.error_log_buffer_size);

    // Validate offsets fit within mapped region.
    const required_size = error_log_offset + error_log_size;
    if (required_size > stat.size) {
        try stderr.print("error: file size ({d}) too small for declared buffers (need {d})\n", .{ stat.size, required_size });
        std.process.exit(1);
    }

    // Print header info.
    try stdout.print("=== BRZ Broker Status ({s}) ===\n", .{path});
    try stdout.print("Node ID:           {d}\n", .{header.node_id});
    try stdout.print("Service ID:        {d}\n", .{header.service_id});
    try stdout.print("PID:               {d}\n", .{header.pid});
    try stdout.print("Start Timestamp:   {d} ms\n", .{header.start_timestamp_ms});
    try stdout.print("Heartbeat:         {d} ms\n", .{header.heartbeat_time_ms});
    try stdout.print("Next Service ID:   {d}\n", .{header.next_service_id});
    try stdout.print("Control Buffer:    {d} bytes\n", .{control_buf_size});
    try stdout.print("Send Buffer:       {d} bytes\n", .{send_buf_size});
    try stdout.print("Counter Values:    {d} bytes ({d} slots)\n", .{ counter_values_size, max_counter_id });
    try stdout.print("Error Log:         {d} bytes\n", .{error_log_size});
    try stdout.print("\n", .{});

    // Print counters.
    try stdout.print("--- Counters ---\n", .{});
    const values_buf = mapped[counter_values_offset..][0..counter_values_size];
    const meta_buf = mapped[counter_metadata_offset..][0..counter_metadata_size];

    var counter_count: usize = 0;
    var id: usize = 0;
    while (id < max_counter_id) : (id += 1) {
        const meta_base = id * counter_metadata_length;
        const state_ptr: *const i32 = @ptrCast(@alignCast(meta_buf.ptr + meta_base));
        const state = @atomicLoad(i32, state_ptr, .acquire);

        if (state == 1) { // allocated
            const type_id_ptr: *const i32 = @ptrCast(@alignCast(meta_buf.ptr + meta_base + 4));
            const label_len_ptr: *const i32 = @ptrCast(@alignCast(meta_buf.ptr + meta_base + 8));
            const label_len: usize = @intCast(label_len_ptr.*);
            const label_text = meta_buf[meta_base + 12 ..][0..label_len];

            const value_offset = id * counter_value_length;
            const value_ptr: *const i64 = @ptrCast(@alignCast(values_buf.ptr + value_offset));
            const value = @atomicLoad(i64, value_ptr, .acquire);

            try stdout.print("  [{d:>3}] type={d:<3} {s:<40} {d}\n", .{ id, type_id_ptr.*, label_text, value });
            counter_count += 1;
        }
    }

    if (counter_count == 0) {
        try stdout.print("  (no counters allocated)\n", .{});
    }

    // Print error log.
    try stdout.print("\n--- Error Log ---\n", .{});
    const error_buf = mapped[error_log_offset..][0..error_log_size];

    var error_count: usize = 0;
    var offset: usize = 0;
    while (offset < error_log_size) {
        const len_ptr: *const i32 = @ptrCast(@alignCast(error_buf.ptr + offset));
        const entry_len = @atomicLoad(i32, len_ptr, .acquire);
        if (entry_len <= 0) break;

        const base = offset;
        const obs_ptr: *const i32 = @ptrCast(@alignCast(error_buf.ptr + base + 4));
        const last_ts_ptr: *const i64 = @ptrCast(@alignCast(error_buf.ptr + base + 8));
        const first_ts_ptr: *const i64 = @ptrCast(@alignCast(error_buf.ptr + base + 16));

        const obs_count = @atomicLoad(i32, obs_ptr, .acquire);
        const last_ts = @atomicLoad(i64, last_ts_ptr, .acquire);
        const first_ts = first_ts_ptr.*;

        const desc_len: usize = @intCast(entry_len - @as(i32, @intCast(entry_header_length)));
        const description = error_buf[base + entry_header_length ..][0..desc_len];

        try stdout.print("  [{d}x] {s}\n", .{ obs_count, description });
        try stdout.print("        first={d}  last={d}\n", .{ first_ts, last_ts });

        error_count += 1;
        offset += std.mem.alignForward(usize, @intCast(entry_len), entry_alignment);
    }

    if (error_count == 0) {
        try stdout.print("  (no errors recorded)\n", .{});
    }

    try stdout.print("\n--- Done ({d} counters, {d} errors) ---\n", .{ counter_count, error_count });
}
