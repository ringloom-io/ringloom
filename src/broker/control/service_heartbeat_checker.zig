//! Heartbeat Checker — detects dead services by reading heartbeat timestamps
//! from their metadata files.
//!
//! The checker is stateless — it reads the registry and heartbeat timestamps,
//! and returns a list of service IDs to remove. The two-phase approach
//! (collect then remove) avoids mutating the registry while iterating.

const std = @import("std");
const brz_common = @import("brz_common");
const platform = brz_common.platform;
const constants = platform.constants;
const ServiceRegistry = @import("service_registry.zig").ServiceRegistry;
const BuffersProvider = brz_common.memory.buffers_provider.BuffersProvider;
const log = std.log.scoped(.heartbeat_checker);

pub const ServiceHeartbeatChecker = struct {
    /// Pre-allocated buffer for service IDs to remove.
    /// Avoids allocation during the check.
    to_remove: [constants.default_max_services]i32 = undefined,

    const Self = @This();

    pub fn init() Self {
        return .{};
    }

    /// Check all local services for heartbeat timeout.
    /// Returns a slice of service IDs that should be removed.
    pub fn check(
        self: *Self,
        registry: *ServiceRegistry,
        _: i64,
    ) []const i32 {
        // Services store heartbeat timestamps as epoch millis, so we must
        // compare against epoch millis — not monotonic nanos converted to ms.
        const now_ms = platform.Clock.epochMillis();
        var remove_count: u32 = 0;

        var buffers_iter = registry.local_buffers.iterator();
        while (buffers_iter.next()) |entry| {
            const service_id = entry.key_ptr.*;
            const buffers: *BuffersProvider = entry.value_ptr.*;

            const last_heartbeat = buffers.readHeartbeat();
            const elapsed = now_ms - last_heartbeat;

            if (elapsed > constants.service_heartbeat_timeout_ms) {
                log.info("heartbeat timeout: service {} (elapsed={}ms, timeout={}ms)", .{
                    service_id,
                    elapsed,
                    constants.service_heartbeat_timeout_ms,
                });

                if (remove_count < self.to_remove.len) {
                    self.to_remove[remove_count] = service_id;
                    remove_count += 1;
                }
            }
        }

        return self.to_remove[0..remove_count];
    }
};
