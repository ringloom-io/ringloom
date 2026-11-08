// SPDX-License-Identifier: Apache-2.0
//
// Counting allocator shim for the vendored LMDB sources (docs/kv-store-spec.md
// §14.1). mdb.c and midl.c are compiled with
//   -Dmalloc=ringloom_kv_malloc -Dcalloc=ringloom_kv_calloc
//   -Drealloc=ringloom_kv_realloc -Dfree=ringloom_kv_free
// so every heap operation inside the engine is counted. This file is compiled
// WITHOUT those defines and forwards to libc.
#include <stdatomic.h>
#include <stdint.h>
#include <stdlib.h>

// A scope attributes allocation attempts only on the calling thread. Nesting
// restores the previous owner; unrelated LMDB environments cannot contaminate it.
static _Thread_local uint64_t *g_owner;
uint64_t *ringloom_kv_alloc_scope(uint64_t *owner) {
    uint64_t *previous = g_owner;
    g_owner = owner;
    return previous;
}
static void count_owned(void) { if (g_owner) ++*g_owner; }

static _Atomic uint64_t g_mallocs;
static _Atomic uint64_t g_callocs;
static _Atomic uint64_t g_reallocs;
static _Atomic uint64_t g_frees;
static _Atomic uint64_t g_bytes;

void *ringloom_kv_malloc(size_t n) {
    count_owned();
    atomic_fetch_add_explicit(&g_mallocs, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&g_bytes, n, memory_order_relaxed);
    return malloc(n);
}

void *ringloom_kv_calloc(size_t count, size_t size) {
    count_owned();
    atomic_fetch_add_explicit(&g_callocs, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&g_bytes, count * size, memory_order_relaxed);
    return calloc(count, size);
}

void *ringloom_kv_realloc(void *p, size_t n) {
    count_owned();
    atomic_fetch_add_explicit(&g_reallocs, 1, memory_order_relaxed);
    atomic_fetch_add_explicit(&g_bytes, n, memory_order_relaxed);
    return realloc(p, n);
}

void ringloom_kv_free(void *p) {
    if (p != NULL) atomic_fetch_add_explicit(&g_frees, 1, memory_order_relaxed);
    free(p);
}

/// malloc + calloc + realloc calls since process start (or last reset).
uint64_t ringloom_kv_engine_allocs(void) {
    return atomic_load_explicit(&g_mallocs, memory_order_relaxed) +
           atomic_load_explicit(&g_callocs, memory_order_relaxed) +
           atomic_load_explicit(&g_reallocs, memory_order_relaxed);
}
uint64_t ringloom_kv_engine_frees(void) { return atomic_load_explicit(&g_frees, memory_order_relaxed); }
uint64_t ringloom_kv_engine_alloc_bytes(void) { return atomic_load_explicit(&g_bytes, memory_order_relaxed); }
void ringloom_kv_engine_alloc_reset(void) {
    atomic_store_explicit(&g_mallocs, 0, memory_order_relaxed);
    atomic_store_explicit(&g_callocs, 0, memory_order_relaxed);
    atomic_store_explicit(&g_reallocs, 0, memory_order_relaxed);
    atomic_store_explicit(&g_frees, 0, memory_order_relaxed);
    atomic_store_explicit(&g_bytes, 0, memory_order_relaxed);
}
