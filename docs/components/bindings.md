# C ABI and Language Bindings

RingLoom exposes the service runtime to non-Zig applications through a C ABI. The
C++, Java, and Node.js bindings layer on that ABI and should not require application
authors to manage Aeron publications directly.

## Binding stack

```text
Application code
  C++ / Java / Node.js binding
  C ABI: src/service/c_abi.zig + include/ringloom_service.h
  ringloom_service runtime
  local ring buffers + service Aeron runtime
```

## C ABI responsibilities

The C ABI owns:

1. Engine creation/start/stop/destruction.
2. Service registration and configuration.
3. Handler registration.
4. Send/request APIs.
5. Zero-copy claim/commit/abort APIs where exposed.
6. Polling APIs for externally driven consumers.
7. Metrics and diagnostic accessors.
8. Error-code translation into stable C-facing enums.

The ABI hides local-vs-remote route selection. A C caller sends through the same
function whether the selected target is local ring-buffer IPC or remote Aeron UDP.

## Aeron runtime packaging

Bindings must package or locate the native RingLoom service library and Aeron runtime
dependencies. Missing native libraries should fail with actionable diagnostics.

Binding users should not include Aeron headers or construct channels/streams. Those
details are discovered from broker metadata and handled by `ringloom_service`.

## Per-language bindings

| Binding | Source | Build/test command | Notes |
|---|---|---|---|
| C ABI | `src/service/c_abi.zig`, `include/ringloom_service.h` | `zig build service-c` | Stable native contract. |
| C++ | `bindings/cpp/` | `zig build test-cpp` | Header-oriented wrapper over C ABI. |
| Java | `bindings/java/` | `zig build test-java`, `zig build test-java-framework` | JNI/native library integration. |
| Node.js | `bindings/node/` | `zig build test-node` | Native addon/package integration. |

The per-language README files remain the install/build authority for language-specific
tooling. This document defines the shared architecture expectations.

## Compatibility expectations

1. Existing public send, request, handler, lifecycle, and metrics APIs should remain
   stable unless a versioned ABI change is explicit.
2. Remote-route errors should be visible to callers; they must not be collapsed into
   generic success/failure without detail.
3. Local `tryClaim` claims ring-buffer payload bytes; remote `tryClaim` claims Aeron
   bytes after the RingLoom data header.
4. Binding tests should cover local sends, remote sends, request/response,
   registration, metrics, and missing-native-library diagnostics.
