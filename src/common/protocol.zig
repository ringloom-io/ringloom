//! TCP wire protocol — frame header and parser utilities.
//!
//! This is the single import point for all protocol-related functionality.
//! TCP framing types are defined in the ringloom_tcp module; this module
//! provides broker-level parsing and encoding helpers.

pub const frame_parser = @import("protocol/frame_parser.zig");

// Re-export commonly used types.
pub const ParsedFrame = frame_parser.ParsedFrame;
pub const parseFrame = frame_parser.parseFrame;
pub const encodeDataFrame = frame_parser.encodeDataFrame;

// Ensure all protocol module tests are discovered by `zig build test`.
comptime {
    _ = @import("protocol/frame_parser.zig");
}
