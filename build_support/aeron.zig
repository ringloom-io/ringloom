// SPDX-License-Identifier: Apache-2.0
const std = @import("std");

// Aeron 1.51.0 C source lists are derived from Aeron's CMake files and the
// reference Zig build at /home/dragan/code/brz/aeron-zig.
const client_root = "aeron-client/src/main/c";
const driver_root = "aeron-driver/src/main/c";

pub const BuildResult = struct {
    module: *std.Build.Module,
    driver_library: *std.Build.Step.Compile,
};

pub const Options = struct {
    module_name: ?[]const u8 = "ringloom_aeron",
    library_name: []const u8 = "aeron_driver",
    use_llvm: ?bool = null,
};

pub fn buildRingLoomAeron(
    b: *std.Build,
    target: std.Build.ResolvedTarget,
    optimize: std.builtin.OptimizeMode,
    options: Options,
) BuildResult {
    const upstream = b.dependency("aeron", .{});

    const aeron_driver = b.addLibrary(.{
        .name = options.library_name,
        .linkage = .static,
        .root_module = b.createModule(.{
            .target = target,
            .optimize = optimize,
            .link_libc = true,
            .pic = true,
        }),
        .use_llvm = options.use_llvm,
    });
    aeron_driver.root_module.addIncludePath(upstream.path(client_root));
    aeron_driver.root_module.addIncludePath(upstream.path(driver_root));
    aeron_driver.root_module.addIncludePath(b.path("src/aeron"));
    aeron_driver.root_module.addCSourceFiles(.{
        .root = upstream.path(client_root),
        .files = &client_sources,
        .flags = &driver_cflags,
    });
    aeron_driver.root_module.addCSourceFiles(.{
        .root = upstream.path(driver_root),
        .files = &driver_only_sources,
        .flags = &driver_cflags,
    });
    aeron_driver.root_module.addCSourceFile(.{
        .file = b.path("src/aeron/aeron_shim.c"),
        .flags = &driver_cflags,
    });

    const module = if (options.module_name) |name| b.addModule(name, .{
        .root_source_file = b.path("src/aeron/root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .pic = true,
    }) else b.createModule(.{
        .root_source_file = b.path("src/aeron/root.zig"),
        .target = target,
        .optimize = optimize,
        .link_libc = true,
        .pic = true,
    });
    module.addIncludePath(upstream.path(client_root));
    module.addIncludePath(upstream.path(driver_root));
    module.addIncludePath(b.path("src/aeron"));
    module.linkLibrary(aeron_driver);

    return .{
        .module = module,
        .driver_library = aeron_driver,
    };
}

const version_flags = [_][]const u8{
    "-DAERON_VERSION_TXT=\"1.51.0\"",
    "-DAERON_VERSION_MAJOR=1",
    "-DAERON_VERSION_MINOR=51",
    "-DAERON_VERSION_PATCH=0",
    "-DAERON_VERSION_GITSHA=\"ringloom-zig-build\"",
};

const common_cflags = [_][]const u8{
    "-D_DEFAULT_SOURCE",
    "-DHAVE_FALLOCATE",
    "-DHAVE_POSIX_FALLOCATE",
    "-DDISABLE_BOUNDS_CHECKS",
    "-std=c11",
    "-fvisibility=hidden",
} ++ version_flags;

const driver_cflags = common_cflags ++ [_][]const u8{
    "-DAERON_DRIVER",
    "-DHAVE_EPOLL",
    "-DHAVE_POLL",
    "-DHAVE_STRUCT_MMSGHDR",
    "-DHAVE_RECVMMSG",
    "-DHAVE_SENDMMSG",
};

const client_sources = [_][]const u8{
    "collections/aeron_array_to_ptr_hash_map.c",
    "collections/aeron_bit_set.c",
    "collections/aeron_hashing.c",
    "collections/aeron_int64_counter_map.c",
    "collections/aeron_int64_to_ptr_hash_map.c",
    "collections/aeron_int64_to_tagged_ptr_hash_map.c",
    "collections/aeron_linked_queue.c",
    "collections/aeron_map.c",
    "collections/aeron_str_to_ptr_hash_map.c",
    "concurrent/aeron_atomic.c",
    "concurrent/aeron_blocking_linked_queue.c",
    "concurrent/aeron_broadcast_receiver.c",
    "concurrent/aeron_broadcast_transmitter.c",
    "concurrent/aeron_counters_manager.c",
    "concurrent/aeron_distinct_error_log.c",
    "concurrent/aeron_logbuffer_descriptor.c",
    "concurrent/aeron_mpsc_concurrent_array_queue.c",
    "concurrent/aeron_mpsc_rb.c",
    "concurrent/aeron_spsc_concurrent_array_queue.c",
    "concurrent/aeron_spsc_rb.c",
    "concurrent/aeron_term_gap_filler.c",
    "concurrent/aeron_term_gap_scanner.c",
    "concurrent/aeron_term_rebuilder.c",
    "concurrent/aeron_term_scanner.c",
    "concurrent/aeron_term_unblocker.c",
    "concurrent/aeron_thread.c",
    "protocol/aeron_udp_protocol.c",
    "reports/aeron_loss_reporter.c",
    "status/aeron_local_sockaddr.c",
    "util/aeron_arrayutil.c",
    "util/aeron_bitutil.c",
    "util/aeron_clock.c",
    "util/aeron_deque.c",
    "util/aeron_dlopen.c",
    "util/aeron_env.c",
    "util/aeron_error.c",
    "util/aeron_fileutil.c",
    "util/aeron_http_util.c",
    "util/aeron_math.c",
    "util/aeron_netutil.c",
    "util/aeron_parse_util.c",
    "util/aeron_properties_util.c",
    "util/aeron_strutil.c",
    "util/aeron_symbol_table.c",
    "uri/aeron_uri.c",
    "uri/aeron_uri_string_builder.c",
    "aeron_agent.c",
    "aeron_alloc.c",
    "aeron_client.c",
    "aeron_client_conductor.c",
    "aeron_cnc.c",
    "aeron_cnc_file_descriptor.c",
    "aeron_context.c",
    "aeron_counter.c",
    "aeron_exclusive_publication.c",
    "aeron_fragment_assembler.c",
    "aeron_image.c",
    "aeron_log_buffer.c",
    "aeron_publication.c",
    "aeron_socket.c",
    "aeron_subscription.c",
    "aeron_windows.c",
    "aeronc.c",
    "aeron_version.c",
};

const driver_only_sources = [_][]const u8{
    "agent/aeron_driver_agent.c",
    "aeron_async_executor.c",
    "concurrent/aeron_logbuffer_unblocker.c",
    "media/aeron_debug_channel_endpoint_configuration.c",
    "media/aeron_fixed_loss_generator.c",
    "media/aeron_multi_gap_loss_generator.c",
    "media/aeron_random_loss_generator.c",
    "media/aeron_receive_channel_endpoint.c",
    "media/aeron_receive_destination.c",
    "media/aeron_send_channel_endpoint.c",
    "media/aeron_timestamps.c",
    "media/aeron_udp_channel.c",
    "media/aeron_udp_channel_transport.c",
    "media/aeron_udp_channel_transport_bindings.c",
    "media/aeron_udp_channel_transport_fixed_loss.c",
    "media/aeron_udp_channel_transport_multi_gap_loss.c",
    "media/aeron_udp_channel_transport_loss.c",
    "media/aeron_udp_destination_tracker.c",
    "media/aeron_udp_transport_poller.c",
    "uri/aeron_driver_uri.c",
    "aeron_congestion_control.c",
    "aeron_csv_table_name_resolver.c",
    "aeron_data_packet_dispatcher.c",
    "aeron_driver_version.c",
    "aeron_driver.c",
    "aeron_driver_conductor.c",
    "aeron_driver_conductor_proxy.c",
    "aeron_driver_context.c",
    "aeron_driver_name_resolver.c",
    "aeron_driver_receiver.c",
    "aeron_driver_receiver_proxy.c",
    "aeron_driver_sender.c",
    "aeron_driver_sender_proxy.c",
    "aeron_flow_control.c",
    "aeron_ipc_publication.c",
    "aeron_loss_detector.c",
    "aeron_min_flow_control.c",
    "aeron_name_resolver.c",
    "aeron_name_resolver_cache.c",
    "aeron_network_publication.c",
    "aeron_port_manager.c",
    "aeron_position.c",
    "aeron_publication_image.c",
    "aeron_retransmit_handler.c",
    "aeron_system_counters.c",
    "aeron_termination_validator.c",
};
