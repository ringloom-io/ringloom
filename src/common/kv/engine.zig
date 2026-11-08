// SPDX-License-Identifier: Apache-2.0
//! KV storage engine selection (docs/kv-store-spec.md §7.1).
//!
//! The engine is chosen at build time with `-Dkv-engine=lmdb` (the default and
//! currently the only value). This file re-exports the selected implementation
//! so broker and service code never name a concrete engine. Every engine
//! provides the same surface, exercised by `engine_conformance_test.zig`.
//!
//! **Value layout.** `put` takes a `KvValueHeader` plus the user value and the
//! engine stores `header(16 B, little-endian) ‖ value`. `ReadSession.get` and
//! `Entry.value` return that stored form: the first 16 bytes are the header
//! (decode with `KvValueHeader.read`, unaligned), the rest is the user value.
const build_options = @import("kv_build_options");

/// `KvControlHeader.engine_kind` of the engine this binary was built with
/// (1 = LMDB).
pub const engine_kind: u32 = build_options.engine_kind;

const impl = switch (engine_kind) {
    1 => @import("lmdb_engine.zig"),
    else => @compileError("unsupported -Dkv-engine"),
};

pub const Error = impl.Error;
pub const Entry = impl.Entry;
pub const WriteEnv = impl.WriteEnv;
pub const WriteTxn = impl.WriteTxn;
pub const ReadEnv = impl.ReadEnv;
pub const ReadSession = impl.ReadSession;
pub const EnvStats = impl.EnvStats;
pub const DataStats = impl.DataStats;
/// Process-wide counters of heap operations performed inside the engine (§14.1).
pub const alloc_stats = impl.alloc_stats;
/// Size in bytes of the `KvValueHeader` prefix on every stored value.
pub const value_header_len = impl.value_header_len;
