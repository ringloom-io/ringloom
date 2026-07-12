//! RingLoom Common — shared substrate used by both broker and service runtimes.
//!
//! This is the root source file for the `ringloom_common` library module.
//! It re-exports all stable public APIs: platform abstractions, concurrent
//! utilities, memory layout, message codecs, monitoring primitives, and shared
//! configuration helpers.

pub const platform = @import("platform.zig");
pub const memory = @import("memory.zig");
pub const concurrent = @import("concurrent.zig");
pub const message = @import("message.zig");
pub const monitoring = @import("monitoring.zig");

// Shared topic wire types and identity — used by broker, service, and bindings.
pub const topics = @import("topics.zig");

// Configuration — shared config loading helpers.
pub const config = struct {
    pub const broker_config = @import("config/broker_config.zig");
    pub const BrokerConfig = broker_config.BrokerConfig;
    pub const ThreadingMode = broker_config.ThreadingMode;
    pub const IdleStrategyName = broker_config.IdleStrategyName;
    pub const PeerEndpoint = broker_config.PeerEndpoint;
    pub const TopicsConfig = broker_config.TopicsConfig;

    pub const config_loader = @import("config/config_loader.zig");
    pub const ConfigLoader = config_loader.ConfigLoader;
    pub const ConfigError = config_loader.ConfigError;
};

// ── Convenience re-exports: commonly used platform types ─────────────

pub const AtomicI32 = platform.AtomicI32;
pub const AtomicI64 = platform.AtomicI64;
pub const AtomicBool = platform.AtomicBool;
pub const CacheLinePaddedAtomicI64 = platform.CacheLinePaddedAtomicI64;
pub const CacheLinePaddedAtomicI32 = platform.CacheLinePaddedAtomicI32;
pub const Clock = platform.Clock;
pub const MappedFile = platform.MappedFile;
pub const ThreadRunner = platform.ThreadRunner;
pub const EventLoop = platform.EventLoop;
pub const CompositeEventLoop = platform.CompositeEventLoop;
pub const IdleStrategy = platform.IdleStrategy;
pub const ProcessSynchronizer = platform.ProcessSynchronizer;
pub const WaitResult = platform.WaitResult;

// ── Convenience re-exports: commonly used concurrent types ───────────

pub const RingBuffer = concurrent.RingBuffer;
pub const CountersManager = concurrent.CountersManager;
pub const CommandQueue = concurrent.CommandQueue;
pub const Command = concurrent.Command;

// ── Convenience re-exports: commonly used memory types ───────────────

pub const BrokerMetadataFile = memory.BrokerMetadataFile;
pub const ServiceMetadataFile = memory.ServiceMetadataFile;
pub const BuffersProvider = memory.BuffersProvider;

// ── Test discovery ───────────────────────────────────────────────────
// Ensure all tests in common submodules are discovered by `zig build test`.

comptime {
    // Platform layer
    _ = @import("platform/constants.zig");
    _ = @import("platform/atomic.zig");
    _ = @import("platform/mapped_file.zig");
    _ = @import("platform/clock.zig");
    _ = @import("platform/thread.zig");
    _ = @import("platform/composite_event_loop.zig");
    _ = @import("platform/process_sync.zig");
    _ = @import("platform/socket.zig");

    // Memory layout layer
    _ = @import("memory/constants.zig");
    _ = @import("memory/broker_metadata.zig");
    _ = @import("memory/service_metadata.zig");
    _ = @import("memory/flow_control.zig");
    _ = @import("memory/service_scanner.zig");
    _ = @import("memory/metadata_descriptor_provider.zig");
    _ = @import("memory/buffers_provider.zig");

    // Concurrent utilities
    _ = @import("concurrent/error_log.zig");
    _ = @import("concurrent/error_state.zig");
    _ = @import("concurrent/counters.zig");
    _ = @import("concurrent/ring_buffer.zig");
    _ = @import("concurrent/command_queue.zig");

    // Message layer
    _ = @import("message/message_header.zig");
    _ = @import("message/data_header.zig");
    _ = @import("message/control_encoding.zig");
    _ = @import("message/control_messages.zig");
    _ = @import("message/topic_control_messages.zig");
    _ = @import("message/message_fragmenting_producer.zig");
    _ = @import("message/message_assembler.zig");
    _ = @import("message/flow_control_messages.zig");
    _ = @import("message/latency_trace.zig");

    // Topics (shared wire types)
    _ = @import("topics/topic_id.zig");
    _ = @import("topics/topic_config.zig");

    // Monitoring
    _ = @import("monitoring/system_counter.zig");
    _ = @import("monitoring/system_counters.zig");
    _ = @import("monitoring/service_counters.zig");
    _ = @import("monitoring/cycle_time.zig");
    _ = @import("monitoring/counter_snapshot.zig");
    _ = @import("monitoring/monitoring.zig");
    _ = @import("monitoring/periodic_dump.zig");
    _ = @import("monitoring/metadata_reader.zig");
    _ = @import("monitoring/prometheus.zig");

    // Configuration
    _ = @import("config/broker_config.zig");
    _ = @import("config/config_loader.zig");
}

test "common module compiles" {
    _ = platform;
    _ = memory;
    _ = concurrent;
    _ = message;
    _ = topics;
    _ = config;
    _ = monitoring;
}
