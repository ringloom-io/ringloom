// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

/** How a {@link KvSession} may be shared between threads. */
public enum KvSessionMode {
    /**
     * Default. Any thread may use the session, but only one at a time: a call that overlaps another
     * call throws {@link IllegalStateException}. While a {@link KvSession#view} snapshot is held the
     * session is pinned to the viewing thread until {@link KvSession#release}. {@link KvSession#close}
     * may run on any thread while the session is idle and not pinned by another thread.
     */
    SERIAL,
    /** Pre-ABI-7 behaviour: every call, including close, must come from the creating thread
     * ({@link WrongThreadException} otherwise). */
    CONFINED
}
