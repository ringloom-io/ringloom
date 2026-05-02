//! Message handling module for the BRZ broker.
//!
//! This is the single import point for message-related functionality.
//! It re-exports the message header, control encoding, fragmentation,
//! and reassembly types.

pub const message_header = @import("message/message_header.zig");
pub const MessageHeader = message_header.MessageHeader;

pub const control_encoding = @import("message/control_encoding.zig");

pub const message_fragmenting_producer = @import("message/message_fragmenting_producer.zig");
pub const MessageFragmentingProducer = message_fragmenting_producer.MessageFragmentingProducer;

pub const message_assembler = @import("message/message_assembler.zig");
pub const MessageAssembler = message_assembler.MessageAssembler;

pub const flow_control_messages = @import("message/flow_control_messages.zig");
pub const latency_trace = @import("message/latency_trace.zig");

// Ensure all message module tests are discovered by `zig build test`.
comptime {
    _ = @import("message/message_header.zig");
    _ = @import("message/control_encoding.zig");
    _ = @import("message/message_fragmenting_producer.zig");
    _ = @import("message/message_assembler.zig");
    _ = @import("message/flow_control_messages.zig");
    _ = @import("message/latency_trace.zig");
}
