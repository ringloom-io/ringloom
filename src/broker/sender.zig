// SPDX-License-Identifier: Apache-2.0
//! Broker sender event loop for v2 Aeron transport.
//!
//! Services publish remote data directly to peer broker UDP streams. The broker
//! sender loop remains only to drive Aeron's sender/network agent in embedded
//! driver modes where that duty cycle is assigned to RingLoom.

const std = @import("std");

const broker_aeron = @import("aeron.zig");

const log = std.log.scoped(.sender);

pub const SenderEventLoop = struct {
    aeron_agent: ?broker_aeron.AgentInvoker,

    const Self = @This();

    pub fn init() Self {
        return .{ .aeron_agent = null };
    }

    pub fn setAeronAgent(self: *Self, agent: ?broker_aeron.AgentInvoker) void {
        self.aeron_agent = agent;
    }

    pub fn deinit(_: *Self) void {}

    pub fn doWork(self: *Self) u32 {
        return self.invokeAeronAgent();
    }

    fn invokeAeronAgent(self: *Self) u32 {
        if (self.aeron_agent) |*agent| {
            return agent.invoke() catch |err| {
                log.err("aeron sender invocation failed: {}", .{err});
                return 0;
            };
        }
        return 0;
    }

    pub fn doWorkFn(ctx: *anyopaque) u32 {
        const self: *Self = @ptrCast(@alignCast(ctx));
        return self.doWork();
    }

    pub fn onCloseFn(_: *anyopaque) void {}
};

test "sender loop invokes assigned Aeron agent" {
    const Counter = struct {
        value: u32 = 0,

        fn invoke(context: *anyopaque) anyerror!u32 {
            const self: *@This() = @ptrCast(@alignCast(context));
            self.value += 1;
            return 3;
        }
    };

    var counter = Counter{};
    var sender_loop = SenderEventLoop.init();
    sender_loop.setAeronAgent(.{
        .context = &counter,
        .invokeFn = Counter.invoke,
    });

    try std.testing.expectEqual(@as(u32, 3), sender_loop.doWork());
    try std.testing.expectEqual(@as(u32, 1), counter.value);
}
