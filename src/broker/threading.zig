//! Threading model for the RingLoom broker.
//!
//! This is the single import point for all threading-related functionality.
//! It re-exports the shared composite event loop, threading mode, and broker
//! threads modules.

const ringloom_common = @import("ringloom_common");

pub const composite_event_loop = ringloom_common.platform.composite_event_loop;
pub const CompositeEventLoop = composite_event_loop.CompositeEventLoop;

pub const threading_mode = @import("threading/threading_mode.zig");
pub const ThreadingMode = threading_mode.ThreadingMode;

pub const broker_threads = @import("threading/broker_threads.zig");
pub const BrokerThreads = broker_threads.BrokerThreads;

// Ensure all threading module tests are discovered by `zig build test`.
comptime {
    _ = @import("threading/threading_mode.zig");
    _ = @import("threading/broker_threads.zig");
}
