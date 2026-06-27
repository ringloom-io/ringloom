// SPDX-License-Identifier: Apache-2.0

//! Persistent topics subsystem for the RingLoom broker.
//!
//! MPMC, persistent, broadcast publish/subscribe topics layered on the broker's
//! embedded Aeron transport and on ringloom-queue for durable storage and
//! replication. See `docs/topics-architecture.md`.

pub const topic_id = @import("topics/topic_id.zig");
pub const TopicId = topic_id.TopicId;
pub const topicIdOf = topic_id.topicIdOf;

pub const topic_config = @import("topics/topic_config.zig");
pub const TopicConfig = topic_config.TopicConfig;
pub const AckMode = topic_config.AckMode;

pub const topic_commands = @import("topics/topic_commands.zig");
pub const topic_store = @import("topics/topic_store.zig");
pub const repl_channel = @import("topics/repl_channel.zig");
pub const repl_session = @import("topics/repl_session.zig");
pub const topic_engine = @import("topics/topic_engine.zig");
pub const topic_prefetcher = @import("topics/topic_prefetcher.zig");
pub const topic_registry = @import("topics/topic_registry.zig");
pub const topic_leader_election = @import("topics/topic_leader_election.zig");
pub const topic_admin = @import("topics/topic_admin.zig");
pub const topic_messages = @import("topics/topic_messages.zig");
pub const subsystem = @import("topics/subsystem.zig");

pub const TopicStore = topic_store.TopicStore;
pub const ReplHub = repl_session.ReplHub;
pub const Publisher = repl_channel.Publisher;
pub const TopicEngine = topic_engine.TopicEngine;
pub const PublishView = topic_engine.PublishView;
pub const TopicPrefetcher = topic_prefetcher.TopicPrefetcher;
pub const TopicRegistry = topic_registry.TopicRegistry;
pub const TopicLeaderElection = topic_leader_election.TopicLeaderElection;
pub const TopicSubsystem = subsystem.TopicSubsystem;

// Ensure all topics submodule tests are discovered by `zig build test`.
comptime {
    _ = @import("topics/topic_id.zig");
    _ = @import("topics/topic_config.zig");
    _ = @import("topics/topic_commands.zig");
    _ = @import("topics/topic_store.zig");
    _ = @import("topics/repl_channel.zig");
    _ = @import("topics/repl_session.zig");
    _ = @import("topics/topic_engine.zig");
    _ = @import("topics/topic_prefetcher.zig");
    _ = @import("topics/topic_registry.zig");
    _ = @import("topics/topic_leader_election.zig");
    _ = @import("topics/topic_admin.zig");
    _ = @import("topics/topic_messages.zig");
    _ = @import("topics/subsystem.zig");
}
