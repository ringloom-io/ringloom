// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

public record MetricSample(String name, MetricKind kind, long value) {
}
