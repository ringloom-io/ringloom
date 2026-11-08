// SPDX-License-Identifier: Apache-2.0
//! Replicated KV store shared types (docs/kv-store-spec.md): wire/storage
//! formats and the shared-memory control header. Used by broker and service.

pub const format = @import("kv/kv_format.zig");
pub const control = @import("kv/kv_control.zig");
/// Selected storage engine (§7); see kv/engine.zig.
pub const engine = @import("kv/engine.zig");

pub const KvStoreConfig = format.KvStoreConfig;
pub const KvFrameHeader = format.KvFrameHeader;
pub const KvFrameKind = format.KvFrameKind;
pub const KvRecordHeader = format.KvRecordHeader;
pub const KvOp = format.KvOp;
pub const KvWriterGrant = format.KvWriterGrant;
pub const KvWriterRelease = format.KvWriterRelease;
pub const KvValueHeader = format.KvValueHeader;
pub const KvEngineMeta = format.KvEngineMeta;
pub const DataFrameBuilder = format.DataFrameBuilder;
pub const FenceState = format.FenceState;
pub const FrameView = format.FrameView;
pub const decodeFrame = format.decodeFrame;
pub const peekHeader = format.peekHeader;

pub const KvControlHeader = control.KvControlHeader;
pub const KvStoreState = control.KvStoreState;
pub const ControlFile = control.ControlFile;

test {
    _ = format;
    _ = control;
    _ = engine;
    _ = @import("kv/engine_conformance_test.zig");
}
