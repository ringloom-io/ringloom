// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Immutable configuration for a persistent topic publication or subscription.
 *
 * <p>Mirrors the native {@code ringloom_topic_config_t} struct. The {@code rollScheme}
 * maps to a ringloom-queue {@code RollScheme} name (e.g. {@code "FAST_DAILY"}).</p>
 */
public record TopicConfig(String rollScheme, int retentionCycles, int flags) {
    /** Default topic config: {@code FAST_DAILY} roll scheme, keep all cycles. */
    public static final TopicConfig DEFAULT = new TopicConfig("FAST_DAILY", 0, 0);

    /** Creates a topic config with validated fields. */
    public TopicConfig {
        Objects.requireNonNull(rollScheme, "rollScheme");
        byte[] bytes = rollScheme.getBytes(StandardCharsets.UTF_8);
        if (bytes.length == 0 || bytes.length > 16) {
            throw new IllegalArgumentException("rollScheme must be 1..16 UTF-8 bytes");
        }
        if (retentionCycles < 0) {
            throw new IllegalArgumentException("retentionCycles must be non-negative");
        }
    }
}
