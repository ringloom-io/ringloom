// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

public record RingStats(
    long capacityBytes,
    long usedBytes,
    long freeBytes,
    long producerPosition,
    long consumerPosition
) {
}
