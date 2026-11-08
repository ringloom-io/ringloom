// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

/** Allocation-free iteration callback. Addresses point at read-only native memory that is valid only
 * during the callback. Must not re-enter any KV API. */
@FunctionalInterface
public interface KvRawVisitor {
    /** Addresses are valid only during the callback. Return false to stop. */
    boolean visit(long keyAddress, long keyLength, long valueAddress, long valueLength);
}
