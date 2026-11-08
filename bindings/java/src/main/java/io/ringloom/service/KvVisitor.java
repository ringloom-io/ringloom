// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

import java.lang.foreign.MemorySegment;
/** Borrowed read-only key/value segments expire when this callback returns. Return false to stop.
 * Iteration order is engine-defined. Must not re-enter any KV API. */
@FunctionalInterface
public interface KvVisitor { boolean visit(MemorySegment key, MemorySegment value); }
