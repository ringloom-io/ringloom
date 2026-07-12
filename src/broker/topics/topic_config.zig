// SPDX-License-Identifier: Apache-2.0
//! Immutable per-topic configuration and per-publish acknowledgement mode.
//!
//! Moved to `ringloom_common` (`src/common/topics/topic_config.zig`) so the
//! service runtime and bindings share one source of truth. This file is a thin
//! re-export shim kept for existing broker-side imports.

const common = @import("ringloom_common");

pub const AckMode = common.topics.topic_config.AckMode;
pub const flag_use_huge_pages = common.topics.topic_config.flag_use_huge_pages;
pub const TopicConfig = common.topics.topic_config.TopicConfig;
pub const ValidateError = common.topics.topic_config.ValidateError;
pub const validateAgainstExisting = common.topics.topic_config.validateAgainstExisting;

// Re-run the shared tests through this module's test namespace.
test {
    _ = common.topics.topic_config;
}
