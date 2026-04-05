//! UDP wire protocol — frame definitions and parser.
//!
//! This is the single import point for all protocol-related functionality.
//! The rest of the codebase imports this module instead of individual files.

pub const frames = @import("protocol/frames.zig");
pub const frame_parser = @import("protocol/frame_parser.zig");

// Re-export commonly used types at the top level for convenience.
pub const FrameHeader = frames.FrameHeader;
pub const DataFrameHeader = frames.DataFrameHeader;
pub const SetupFrame = frames.SetupFrame;
pub const StatusMessage = frames.StatusMessage;
pub const NakFrame = frames.NakFrame;
pub const FrameType = frames.FrameType;
pub const makeHeartbeat = frames.makeHeartbeat;

pub const ParsedFrame = frame_parser.ParsedFrame;
pub const parseFrame = frame_parser.parseFrame;
pub const readDataFrame = frame_parser.readDataFrame;
pub const encodeDataFrame = frame_parser.encodeDataFrame;

// Ensure all protocol module tests are discovered by `zig build test`.
comptime {
    _ = @import("protocol/frames.zig");
    _ = @import("protocol/frame_parser.zig");
}
