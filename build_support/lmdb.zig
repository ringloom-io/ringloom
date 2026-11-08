// SPDX-License-Identifier: Apache-2.0
//! Builds vendored LMDB (OpenLDAP Public License) into a Zig module, together
//! with the counting allocation shim (docs/kv-store-spec.md §7.2, §14.1).
const std = @import("std");

/// Engine selected with `-Dkv-engine=`. Values are `KvControlHeader.engine_kind`.
pub const Engine = enum(u32) { lmdb = 1 };

const lmdb_sources = [_][]const u8{ "mdb.c", "midl.c" };

// malloc/calloc/realloc/free are redirected to the counting shim. Key size is
// derived from the page size (MDB_MAXKEYSIZE=0) so that the 8-byte expiry
// prefix still fits keys of the configured maximum length (511).
const lmdb_cflags = [_][]const u8{
    "-std=gnu11",
    "-O2",
    "-fno-sanitize=undefined",
    "-w",
    "-fvisibility=hidden",
    "-DMDB_MAXKEYSIZE=0",
    "-Dmalloc=ringloom_kv_malloc",
    "-Dcalloc=ringloom_kv_calloc",
    "-Drealloc=ringloom_kv_realloc",
    "-Dfree=ringloom_kv_free",
};

const shim_cflags = [_][]const u8{ "-std=gnu11", "-O2", "-fno-sanitize=undefined", "-fvisibility=hidden" };

/// Adds LMDB + the allocation shim to `module`. Any artifact that depends on
/// `module` links them automatically.
pub fn addLmdb(b: *std.Build, module: *std.Build.Module) void {
    const upstream = b.dependency("lmdb", .{});
    module.link_libc = true;
    module.addCSourceFiles(.{
        .root = upstream.path("libraries/liblmdb"),
        .files = &lmdb_sources,
        .flags = &lmdb_cflags,
    });
    module.addCSourceFile(.{ .file = b.path("src/common/kv/alloc_shim.c"), .flags = &shim_cflags });
}
