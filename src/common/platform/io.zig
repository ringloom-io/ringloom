const std = @import("std");

/// Process-default I/O used by legacy platform helpers that do not yet accept
/// an explicit `std.Io` parameter.
pub fn default() std.Io {
    return std.Io.Threaded.global_single_threaded.io();
}
