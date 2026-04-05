//! BRZ Broker — high-performance IPC framework using shared-memory ring buffers.
//!
//! This is the root source file for the brz_broker library module.
//! It re-exports all public APIs and ensures all tests are discovered.

pub const platform = @import("platform.zig");
pub const memory = @import("memory.zig");
pub const concurrent = @import("concurrent.zig");
pub const transport = @import("transport.zig");
pub const protocol = @import("protocol.zig");
pub const sender = @import("sender.zig");
pub const receiver = @import("receiver.zig");
pub const flow_control = @import("flow_control.zig");
pub const ipc = @import("ipc.zig");
pub const message = @import("message.zig");
pub const service = @import("service.zig");
pub const control = @import("control.zig");
pub const cluster = @import("cluster.zig");
pub const threading = @import("threading.zig");

// Re-export commonly used platform types at the top level for convenience.
pub const AtomicI32 = platform.AtomicI32;
pub const AtomicI64 = platform.AtomicI64;
pub const AtomicBool = platform.AtomicBool;
pub const CacheLinePaddedAtomicI64 = platform.CacheLinePaddedAtomicI64;
pub const CacheLinePaddedAtomicI32 = platform.CacheLinePaddedAtomicI32;
pub const Clock = platform.Clock;
pub const MappedFile = platform.MappedFile;
pub const ThreadRunner = platform.ThreadRunner;
pub const EventLoop = platform.EventLoop;
pub const IdleStrategy = platform.IdleStrategy;
pub const ProcessSynchronizer = platform.ProcessSynchronizer;
pub const WaitResult = platform.WaitResult;

// Ensure all tests in all submodules are discovered by `zig build test`.
comptime {
    // Platform layer (task 01)
    _ = @import("platform/constants.zig");
    _ = @import("platform/atomic.zig");
    _ = @import("platform/mapped_file.zig");
    _ = @import("platform/clock.zig");
    _ = @import("platform/thread.zig");
    _ = @import("platform/process_sync.zig");

    // Memory layout layer (task 02)
    _ = @import("memory/constants.zig");
    _ = @import("memory/broker_metadata.zig");
    _ = @import("memory/service_metadata.zig");
    _ = @import("memory/receive_log.zig");
    _ = @import("memory/service_scanner.zig");
    _ = @import("memory/metadata_descriptor_provider.zig");
    _ = @import("memory/buffers_provider.zig");

    // Concurrent utilities (task 03)
    _ = @import("concurrent/error_log.zig");
    _ = @import("concurrent/error_state.zig");
    _ = @import("concurrent/counters.zig");
    _ = @import("concurrent/ring_buffer.zig");

    // Protocol layer (task 04)
    _ = @import("protocol/frames.zig");
    _ = @import("protocol/frame_parser.zig");

    // Transport layer (task 04)
    _ = @import("transport/buffer_pool.zig");
    _ = @import("transport/udp_socket.zig");
    _ = @import("transport/network_io.zig");
    if (@import("builtin").os.tag == .linux) {
        _ = @import("transport/io_uring.zig");
    }
    if (@import("builtin").os.tag == .macos) {
        _ = @import("transport/kqueue.zig");
    }

    // Receiver layer (task 06)
    _ = @import("receiver/loss_detector.zig");
    _ = @import("receiver/fragment_assembler.zig");
    _ = @import("receiver/peer_receiver.zig");
    _ = @import("receiver/receive_log_buffer.zig");
    _ = @import("receiver/message_router.zig");
    _ = @import("receiver/receiver_event_loop.zig");

    // Flow control layer (task 07)
    _ = @import("flow_control/sender_flow_control.zig");
    _ = @import("flow_control/receiver_flow_control.zig");
    _ = @import("flow_control/status_message.zig");
    _ = @import("flow_control/zero_window_probe.zig");
    _ = @import("flow_control/back_pressure.zig");
    _ = @import("flow_control/counters.zig");
    _ = @import("flow_control/test_flow_control.zig");

    // IPC layer (task 08)
    _ = @import("ipc/ipc_producer.zig");
    _ = @import("ipc/ipc_consumer.zig");
    _ = @import("ipc/ipc_test.zig");

    // Message layer (task 08)
    _ = @import("message/message_header.zig");
    _ = @import("message/control_encoding.zig");
    _ = @import("message/message_fragmenting_producer.zig");
    _ = @import("message/message_assembler.zig");

    // Service layer (task 08)
    _ = @import("service/brz_engine.zig");
    _ = @import("service/message_consumer.zig");
    _ = @import("service/control_agent.zig");
    _ = @import("service/service_client.zig");
    _ = @import("service/service_client_registry.zig");
    _ = @import("service/service_instance.zig");
    _ = @import("service/load_balancer.zig");
    _ = @import("service/service_client_test.zig");

    // Control plane (task 09)
    _ = @import("control/control_messages.zig");
    _ = @import("control/service_registry.zig");
    _ = @import("control/service_heartbeat_checker.zig");
    _ = @import("control/service_leader_election.zig");
    _ = @import("control/control_loop.zig");

    // Cluster management (task 11 stub)
    _ = @import("cluster/cluster_manager.zig");

    // Command queue (task 09)
    _ = @import("concurrent/command_queue.zig");

    // Threading model (task 10)
    _ = @import("threading.zig");

    // Sender layer (task 05)
    _ = @import("sender/sender_event_loop.zig");
    _ = @import("sender/peer_sender.zig");
    _ = @import("sender/retransmit_buffer.zig");
    _ = @import("sender/retransmit_handler.zig");
    _ = @import("sender/message_fragmenter.zig");
    _ = @import("sender/send_buffer_pool.zig");
    _ = @import("sender/sender_command.zig");
}

test "root module compiles" {
    // Smoke test: verify the module graph is valid.
    _ = platform;
    _ = memory;
    _ = concurrent;
    _ = transport;
    _ = protocol;
    _ = sender;
    _ = receiver;
    _ = flow_control;
    _ = ipc;
    _ = message;
    _ = service;
    _ = control;
    _ = cluster;
}
