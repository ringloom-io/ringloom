// SPDX-License-Identifier: Apache-2.0
//! Minimal spinlock guarding the topic store's queue map (see topic_store.zig).
//! Held only briefly; contention is rare.

const std = @import("std");

pub const SpinLock = struct {
    flag: std.atomic.Value(bool) = std.atomic.Value(bool).init(false),

    pub fn lock(self: *SpinLock) void {
        while (self.flag.swap(true, .acquire)) {
            std.atomic.spinLoopHint();
        }
    }

    pub fn unlock(self: *SpinLock) void {
        self.flag.store(false, .release);
    }
};
