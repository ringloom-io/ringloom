// SPDX-License-Identifier: Apache-2.0
//! Deterministic topic identity.
//!
//! Moved to `ringloom_common` (`src/common/topics/topic_id.zig`) so the service
//! runtime and bindings share one source of truth. This file is a thin re-export
//! shim kept for existing broker-side imports.

const common = @import("ringloom_common");

pub const TopicId = common.topics.topic_id.TopicId;
pub const invalid_topic_id = common.topics.topic_id.invalid_topic_id;
pub const topicIdOf = common.topics.topic_id.topicIdOf;
pub const isValid = common.topics.topic_id.isValid;

// Re-run the shared tests through this module's test namespace.
test {
    _ = common.topics.topic_id;
}
