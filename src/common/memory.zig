//! Memory layout and shared memory subsystem for the BRZ broker.
//!
//! This is the single import point for all memory-related functionality.
//! It re-exports the metadata file types, service scanner,
//! and singleton providers.

pub const constants = @import("memory/constants.zig");

pub const broker_metadata = @import("memory/broker_metadata.zig");
pub const BrokerMetadataFile = broker_metadata.BrokerMetadataFile;
pub const BrokerMetadataHeader = broker_metadata.BrokerMetadataHeader;

pub const service_metadata = @import("memory/service_metadata.zig");
pub const ServiceMetadataFile = service_metadata.ServiceMetadataFile;
pub const ServiceMetadataHeader = service_metadata.ServiceMetadataHeader;
pub const BlockingTrailer = service_metadata.BlockingTrailer;
pub const BlockingTrailerSlot = service_metadata.BlockingTrailerSlot;

pub const flow_control = @import("memory/flow_control.zig");
pub const FlowControlRegion = flow_control.FlowControlRegion;
pub const FlowControlEntry = flow_control.FlowControlEntry;
pub const FlowControlHeader = flow_control.FlowControlHeader;
pub const SlotState = flow_control.SlotState;
pub const PressureState = flow_control.PressureState;

pub const peer_send_counters = @import("memory/peer_send_counters.zig");
pub const PeerSendCountersRegion = peer_send_counters.PeerSendCountersRegion;
pub const PeerEntry = peer_send_counters.PeerEntry;
pub const PeerSendCountersHeader = peer_send_counters.PeerSendCountersHeader;

pub const service_scanner = @import("memory/service_scanner.zig");
pub const ServiceScanner = service_scanner.ServiceScanner;
pub const ServiceInstance = service_scanner.ServiceInstance;
pub const ScanResult = service_scanner.ScanResult;

pub const metadata_descriptor_provider = @import("memory/metadata_descriptor_provider.zig");
pub const MetadataDescriptorProvider = metadata_descriptor_provider.MetadataDescriptorProvider;

pub const buffers_provider = @import("memory/buffers_provider.zig");
pub const BuffersProvider = buffers_provider.BuffersProvider;

// Ensure all memory module tests are discovered by `zig build test`.
comptime {
    _ = @import("memory/constants.zig");
    _ = @import("memory/broker_metadata.zig");
    _ = @import("memory/service_metadata.zig");
    _ = @import("memory/flow_control.zig");
    _ = @import("memory/peer_send_counters.zig");
    _ = @import("memory/service_scanner.zig");
    _ = @import("memory/metadata_descriptor_provider.zig");
    _ = @import("memory/buffers_provider.zig");
}
