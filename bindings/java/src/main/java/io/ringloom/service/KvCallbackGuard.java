// SPDX-License-Identifier: Apache-2.0
package io.ringloom.service;

/** Callback context shared with parent APIs without initializing native KV bindings. */
final class KvCallbackGuard {
    private static final ThreadLocal<Boolean> CALLBACK = ThreadLocal.withInitial(() -> false);

    private KvCallbackGuard() {}

    /** Marks the current thread as running a KV callback; returns the previous state for {@link #exit}. */
    static boolean enter() {
        boolean previous = CALLBACK.get();
        CALLBACK.set(true);
        return previous;
    }

    /** Restores the callback state captured by {@link #enter}. */
    static void exit(boolean previous) {
        CALLBACK.set(previous);
    }

    static void check() {
        if (CALLBACK.get()) {
            throw new IllegalStateException("KV callback must not re-enter KV, service, or client APIs");
        }
    }
}
