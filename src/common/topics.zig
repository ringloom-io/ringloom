// SPDX-License-Identifier: Apache-2.0
//! Shared topic wire types and identity, used by both the broker and the
//! service runtime (and transitively by every language binding).
//!
//! This is the single source of truth for the on-the-wire topic layout: the
//! deterministic `topic_id`, the immutable `TopicConfig`, and the per-publish
//! acknowledgement mode. Broker-side topic *machinery* (registry, engine,
//! replication) remains in the broker module; only the wire-shared types live
//! here so the service can encode/decode the same structs without duplicating
//! them.

pub const topic_id = @import("topics/topic_id.zig");
pub const TopicId = topic_id.TopicId;
pub const topicIdOf = topic_id.topicIdOf;
pub const isValidTopicId = topic_id.isValid;
pub const invalid_topic_id = topic_id.invalid_topic_id;

pub const topic_config = @import("topics/topic_config.zig");
pub const TopicConfig = topic_config.TopicConfig;
pub const AckMode = topic_config.AckMode;

// Ensure all topic submodule tests are discovered by `zig build test`.
comptime {
    _ = @import("topics/topic_id.zig");
    _ = @import("topics/topic_config.zig");
}
