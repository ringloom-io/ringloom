//! Service Leader Election — determines which instance of a service is the
//! leader across the entire cluster.
//!
//! Election rule: lowest serviceId wins (effectively first-registered wins,
//! since service IDs are monotonically incremented by the broker).
//!
//! A service opts in by setting `leaderElectionEnabled = true` in its
//! registration. If not opted in, no leader is tracked for that service.
//!
//! This module is single-threaded (broker-agent thread only).

const std = @import("std");

pub const ServiceLeaderElectionManager = struct {
    /// Per-service leader state, keyed by service name (null-padded [32]u8).
    leader_states: std.AutoHashMap([32]u8, LeaderState),
    /// This broker's nodeId — used to check if we are the cluster leader.
    local_node_id: u8,

    /// Mutable state for one service name.
    pub const LeaderState = struct {
        /// Set of registered instances, encoded as (serviceId << 16 | nodeId).
        registered_instances: std.AutoHashMap(u32, void),
        /// Current leader serviceId, or null if no leader.
        leader_service_id: ?u16 = null,
        /// Current leader nodeId.
        leader_node_id: ?u8 = null,

        pub fn encodeKey(service_id: u16, node_id: u8) u32 {
            return (@as(u32, service_id) << 16) | @as(u32, node_id);
        }

        pub fn decodeServiceId(key: u32) u16 {
            return @intCast(key >> 16);
        }

        pub fn decodeNodeId(key: u32) u8 {
            return @intCast(key & 0xFF);
        }
    };

    pub const ElectionResult = struct {
        service_id: u16,
        node_id: u8,
        changed: bool,
    };

    pub const ServiceLeaderChange = struct {
        service_name: [32]u8,
        result: ElectionResult,
    };

    pub fn init(allocator: std.mem.Allocator, local_node_id: u8) ServiceLeaderElectionManager {
        return .{
            .leader_states = std.AutoHashMap([32]u8, LeaderState).init(allocator),
            .local_node_id = local_node_id,
        };
    }

    pub fn deinit(self: *ServiceLeaderElectionManager) void {
        var iter = self.leader_states.valueIterator();
        while (iter.next()) |state| {
            state.registered_instances.deinit();
        }
        self.leader_states.deinit();
    }

    /// Register a service instance for leader election tracking.
    pub fn registerInstance(
        self: *ServiceLeaderElectionManager,
        service_name: [32]u8,
        service_id: u16,
        node_id: u8,
    ) void {
        const gop = self.leader_states.getOrPut(service_name) catch return;
        if (!gop.found_existing) {
            gop.value_ptr.* = .{
                .registered_instances = std.AutoHashMap(u32, void).init(
                    self.leader_states.allocator,
                ),
            };
        }
        gop.value_ptr.registered_instances.put(
            LeaderState.encodeKey(service_id, node_id),
            {},
        ) catch {};
    }

    /// Unregister a service instance. If it was the leader, clear the leader.
    pub fn unregisterInstance(
        self: *ServiceLeaderElectionManager,
        service_name: [32]u8,
        service_id: u16,
    ) void {
        const state = self.leader_states.getPtr(service_name) orelse return;

        // Remove all entries with this serviceId (across any nodeId)
        var iter = state.registered_instances.keyIterator();
        var to_remove: [64]u32 = undefined;
        var remove_count: usize = 0;
        while (iter.next()) |key_ptr| {
            if (LeaderState.decodeServiceId(key_ptr.*) == service_id) {
                if (remove_count < to_remove.len) {
                    to_remove[remove_count] = key_ptr.*;
                    remove_count += 1;
                }
            }
        }
        for (to_remove[0..remove_count]) |key| {
            _ = state.registered_instances.remove(key);
        }

        // Clear leader if it was the removed instance
        if (state.leader_service_id) |leader_id| {
            if (leader_id == service_id) {
                state.leader_service_id = null;
                state.leader_node_id = null;
            }
        }

        // Clean up empty entries
        if (state.registered_instances.count() == 0) {
            state.registered_instances.deinit();
            _ = self.leader_states.remove(service_name);
        }
    }

    /// Remove all instances registered for a given nodeId across all services.
    /// Returns the number of services affected.
    pub fn removeByNodeId(self: *ServiceLeaderElectionManager, node_id: u8) usize {
        var affected: usize = 0;
        var services_to_remove: [256][32]u8 = undefined;
        var services_remove_count: usize = 0;

        var iter = self.leader_states.iterator();
        while (iter.next()) |entry| {
            const state = entry.value_ptr;

            // Find keys matching this nodeId
            var key_iter = state.registered_instances.keyIterator();
            var to_remove: [64]u32 = undefined;
            var remove_count: usize = 0;
            while (key_iter.next()) |key_ptr| {
                if (LeaderState.decodeNodeId(key_ptr.*) == node_id) {
                    if (remove_count < to_remove.len) {
                        to_remove[remove_count] = key_ptr.*;
                        remove_count += 1;
                    }
                }
            }

            if (remove_count > 0) {
                for (to_remove[0..remove_count]) |key| {
                    _ = state.registered_instances.remove(key);
                }
                affected += 1;

                // Clear leader if it was on the removed node
                if (state.leader_node_id) |leader_nid| {
                    if (leader_nid == node_id) {
                        state.leader_service_id = null;
                        state.leader_node_id = null;
                    }
                }

                // Mark empty services for removal
                if (state.registered_instances.count() == 0) {
                    if (services_remove_count < services_to_remove.len) {
                        services_to_remove[services_remove_count] = entry.key_ptr.*;
                        services_remove_count += 1;
                    }
                }
            }
        }

        // Remove empty service entries
        for (services_to_remove[0..services_remove_count]) |name| {
            if (self.leader_states.getPtr(name)) |state| {
                state.registered_instances.deinit();
            }
            _ = self.leader_states.remove(name);
        }

        return affected;
    }

    /// Elect a leader for the given service. If a leader already exists,
    /// returns it. Otherwise picks the lowest serviceId.
    pub fn electLeader(
        self: *ServiceLeaderElectionManager,
        service_name: [32]u8,
    ) ?ElectionResult {
        const state = self.leader_states.getPtr(service_name) orelse return null;
        if (state.registered_instances.count() == 0) return null;

        // If a leader is already set, return it (no change)
        if (state.leader_service_id) |leader_sid| {
            return .{
                .service_id = leader_sid,
                .node_id = state.leader_node_id.?,
                .changed = false,
            };
        }

        return self.pickLowestServiceId(state);
    }

    /// Force re-election: clear current leader and pick the lowest serviceId.
    pub fn reElectLeader(
        self: *ServiceLeaderElectionManager,
        service_name: [32]u8,
    ) ?ElectionResult {
        const state = self.leader_states.getPtr(service_name) orelse return null;
        if (state.registered_instances.count() == 0) return null;

        const previous = state.leader_service_id;
        state.leader_service_id = null;
        state.leader_node_id = null;

        const result = self.pickLowestServiceId(state) orelse return null;

        // If the same leader was re-elected, mark as not changed
        if (previous != null and previous.? == result.service_id) {
            return .{
                .service_id = result.service_id,
                .node_id = result.node_id,
                .changed = false,
            };
        }
        return result;
    }

    /// Re-evaluate all tracked services. Called after a broker leadership
    /// change (split-brain recovery). Returns the count of services where
    /// leadership changed, populating `changes_out`.
    pub fn reEvaluateAllLeaders(
        self: *ServiceLeaderElectionManager,
        changes_out: []ServiceLeaderChange,
    ) usize {
        var count: usize = 0;
        var iter = self.leader_states.iterator();
        while (iter.next()) |entry| {
            const state = entry.value_ptr;
            const previous = state.leader_service_id;
            state.leader_service_id = null;
            state.leader_node_id = null;

            if (self.pickLowestServiceId(state)) |result| {
                if (previous == null or previous.? != result.service_id) {
                    if (count < changes_out.len) {
                        changes_out[count] = .{
                            .service_name = entry.key_ptr.*,
                            .result = result,
                        };
                        count += 1;
                    }
                }
            }
        }
        return count;
    }

    /// Set the leader from a remote broker's designation (this broker is
    /// not the cluster leader).
    pub fn setLeaderFromRemote(
        self: *ServiceLeaderElectionManager,
        service_name: [32]u8,
        service_id: u16,
        node_id: u8,
    ) void {
        const gop = self.leader_states.getOrPut(service_name) catch return;
        if (!gop.found_existing) {
            gop.value_ptr.* = .{
                .registered_instances = std.AutoHashMap(u32, void).init(
                    self.leader_states.allocator,
                ),
            };
        }
        gop.value_ptr.leader_service_id = service_id;
        gop.value_ptr.leader_node_id = node_id;
    }

    /// Check if leader election is enabled for a service.
    pub fn isLeaderElectionEnabled(
        self: *const ServiceLeaderElectionManager,
        service_name: [32]u8,
    ) bool {
        return self.leader_states.contains(service_name);
    }

    /// Get the current leader for a service name, if any.
    pub fn getLeader(self: *const ServiceLeaderElectionManager, service_name: [32]u8) ?ElectionResult {
        const state = self.leader_states.get(service_name) orelse return null;
        const sid = state.leader_service_id orelse return null;
        const nid = state.leader_node_id orelse return null;
        return .{
            .service_id = sid,
            .node_id = nid,
            .changed = false,
        };
    }

    // ── Internal ─────────────────────────────────────────────────────

    fn pickLowestServiceId(_: *ServiceLeaderElectionManager, state: *LeaderState) ?ElectionResult {
        var lowest_sid: u16 = std.math.maxInt(u16);
        var lowest_nid: u8 = 0;

        var iter = state.registered_instances.keyIterator();
        while (iter.next()) |key_ptr| {
            const sid = LeaderState.decodeServiceId(key_ptr.*);
            if (sid < lowest_sid) {
                lowest_sid = sid;
                lowest_nid = LeaderState.decodeNodeId(key_ptr.*);
            }
        }

        if (lowest_sid == std.math.maxInt(u16)) return null;

        state.leader_service_id = lowest_sid;
        state.leader_node_id = lowest_nid;

        return .{
            .service_id = lowest_sid,
            .node_id = lowest_nid,
            .changed = true,
        };
    }
};

// ── Helper ────────────────────────────────────────────────────────────

pub fn padServiceName(name: []const u8) [32]u8 {
    var result: [32]u8 = [_]u8{0} ** 32;
    const len = @min(name.len, 32);
    @memcpy(result[0..len], name[0..len]);
    return result;
}

// ── Tests ─────────────────────────────────────────────────────────────

const testing = std.testing;

test "lowest serviceId wins service leader election" {
    // Given: three instances of "pricing" across two nodes
    var mgr = ServiceLeaderElectionManager.init(testing.allocator, 1);
    defer mgr.deinit();

    const name = padServiceName("pricing");
    mgr.registerInstance(name, 5, 1); // serviceId=5 on node 1
    mgr.registerInstance(name, 3, 2); // serviceId=3 on node 2
    mgr.registerInstance(name, 7, 1); // serviceId=7 on node 1

    // When: elect leader
    const result = mgr.electLeader(name);

    // Then: serviceId 3 wins (lowest)
    try testing.expect(result != null);
    try testing.expectEqual(@as(u16, 3), result.?.service_id);
    try testing.expectEqual(@as(u8, 2), result.?.node_id);
    try testing.expect(result.?.changed);
}

test "electLeader returns existing leader without change" {
    // Given: a leader already elected
    var mgr = ServiceLeaderElectionManager.init(testing.allocator, 1);
    defer mgr.deinit();

    const name = padServiceName("pricing");
    mgr.registerInstance(name, 3, 2);
    mgr.registerInstance(name, 5, 1);
    _ = mgr.electLeader(name); // 3 wins

    // When: elect again
    const result = mgr.electLeader(name);

    // Then: same leader, not changed
    try testing.expect(result != null);
    try testing.expectEqual(@as(u16, 3), result.?.service_id);
    try testing.expect(!result.?.changed);
}

test "re-evaluate after leader removed" {
    // Given: serviceId 3 was the leader
    var mgr = ServiceLeaderElectionManager.init(testing.allocator, 1);
    defer mgr.deinit();

    const name = padServiceName("pricing");
    mgr.registerInstance(name, 3, 2);
    mgr.registerInstance(name, 5, 1);
    _ = mgr.electLeader(name); // 3 wins

    // When: serviceId 3 is removed
    mgr.unregisterInstance(name, 3);
    const result = mgr.reElectLeader(name);

    // Then: serviceId 5 is the new leader
    try testing.expect(result != null);
    try testing.expectEqual(@as(u16, 5), result.?.service_id);
    try testing.expect(result.?.changed);
}

test "reElectLeader with same winner returns not changed" {
    // Given
    var mgr = ServiceLeaderElectionManager.init(testing.allocator, 1);
    defer mgr.deinit();

    const name = padServiceName("pricing");
    mgr.registerInstance(name, 3, 2);
    mgr.registerInstance(name, 5, 1);
    _ = mgr.electLeader(name); // 3 wins

    // When: re-elect without removing anything
    const result = mgr.reElectLeader(name);

    // Then: same leader, not changed
    try testing.expect(result != null);
    try testing.expectEqual(@as(u16, 3), result.?.service_id);
    try testing.expect(!result.?.changed);
}

test "reEvaluateAllLeaders after broker leadership change" {
    // Given: two services with leaders
    var mgr = ServiceLeaderElectionManager.init(testing.allocator, 1);
    defer mgr.deinit();

    const pricing = padServiceName("pricing");
    const orders = padServiceName("orders");
    mgr.registerInstance(pricing, 3, 2);
    mgr.registerInstance(pricing, 5, 1);
    mgr.registerInstance(orders, 1, 1);
    mgr.registerInstance(orders, 2, 2);
    _ = mgr.electLeader(pricing);
    _ = mgr.electLeader(orders);

    // When: re-evaluate all (simulating new broker leader)
    var changes: [16]ServiceLeaderElectionManager.ServiceLeaderChange = undefined;
    const count = mgr.reEvaluateAllLeaders(&changes);

    // Then: leaders are unchanged (same lowest IDs), so no changes reported
    try testing.expectEqual(@as(usize, 0), count);
}

test "setLeaderFromRemote updates leader" {
    // Given
    var mgr = ServiceLeaderElectionManager.init(testing.allocator, 1);
    defer mgr.deinit();

    const name = padServiceName("pricing");

    // When
    mgr.setLeaderFromRemote(name, 5, 2);

    // Then
    const leader = mgr.getLeader(name);
    try testing.expect(leader != null);
    try testing.expectEqual(@as(u16, 5), leader.?.service_id);
    try testing.expectEqual(@as(u8, 2), leader.?.node_id);
}

test "isLeaderElectionEnabled" {
    // Given
    var mgr = ServiceLeaderElectionManager.init(testing.allocator, 1);
    defer mgr.deinit();

    const name = padServiceName("pricing");

    // Initially not enabled
    try testing.expect(!mgr.isLeaderElectionEnabled(name));

    // After registering an instance
    mgr.registerInstance(name, 1, 1);
    try testing.expect(mgr.isLeaderElectionEnabled(name));

    // After removing all instances
    mgr.unregisterInstance(name, 1);
    try testing.expect(!mgr.isLeaderElectionEnabled(name));
}

test "removeByNodeId removes all instances for a node" {
    // Given
    var mgr = ServiceLeaderElectionManager.init(testing.allocator, 1);
    defer mgr.deinit();

    const pricing = padServiceName("pricing");
    const orders = padServiceName("orders");
    mgr.registerInstance(pricing, 3, 2);
    mgr.registerInstance(pricing, 5, 1);
    mgr.registerInstance(orders, 1, 2);

    // When: remove all from node 2
    const affected = mgr.removeByNodeId(2);

    // Then
    try testing.expectEqual(@as(usize, 2), affected);
    // pricing should still have serviceId=5 on node 1
    try testing.expect(mgr.isLeaderElectionEnabled(pricing));
    // orders should be gone entirely
    try testing.expect(!mgr.isLeaderElectionEnabled(orders));
}

test "electLeader returns null for unknown service" {
    // Given
    var mgr = ServiceLeaderElectionManager.init(testing.allocator, 1);
    defer mgr.deinit();

    // When / Then
    try testing.expect(mgr.electLeader(padServiceName("nonexistent")) == null);
}

test "unregisterInstance clears leader when leader is removed" {
    // Given
    var mgr = ServiceLeaderElectionManager.init(testing.allocator, 1);
    defer mgr.deinit();

    const name = padServiceName("pricing");
    mgr.registerInstance(name, 3, 2);
    mgr.registerInstance(name, 5, 1);
    _ = mgr.electLeader(name); // 3 wins

    // When: remove the leader (serviceId=3)
    mgr.unregisterInstance(name, 3);

    // Then: leader is cleared
    const leader = mgr.getLeader(name);
    try testing.expect(leader == null);
}
