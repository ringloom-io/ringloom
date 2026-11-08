// SPDX-License-Identifier: Apache-2.0
//! Replicated KV stores — broker side (docs/kv-store-spec.md).
//!
//! K06 (writer lease): `kv_grant_tracker`, `kv_writer_lease`, `kv_messages`,
//! `kv_local_stores`. The applier/subsystem (K05) adds its modules here too.

pub const kv_grant_tracker = @import("kv/kv_grant_tracker.zig");
pub const kv_writer_lease = @import("kv/kv_writer_lease.zig");
pub const kv_messages = @import("kv/kv_messages.zig");
pub const kv_local_stores = @import("kv/kv_local_stores.zig");

pub const GrantTracker = kv_grant_tracker.GrantTracker;
pub const KvLocalStores = kv_local_stores.KvLocalStores;

// Ensure all kv submodule tests are discovered by `zig build test`.
comptime {
    _ = kv_grant_tracker;
    _ = kv_writer_lease;
    _ = kv_messages;
    _ = kv_local_stores;
}

pub const kv_applier = @import("kv/kv_applier.zig");
pub const kv_store_slot = @import("kv/kv_store_slot.zig");
pub const kv_subsystem = @import("kv/kv_subsystem.zig");
pub const KvSubsystem = kv_subsystem.KvSubsystem;
test {
    _ = kv_applier;
    _ = kv_store_slot;
    _ = kv_subsystem;
}
