//! Prometheus text-format helpers.

const std = @import("std");

pub fn writeLabelValue(writer: *std.Io.Writer, value: []const u8) !void {
    for (value) |c| {
        switch (c) {
            '\\' => try writer.writeAll("\\\\"),
            '"' => try writer.writeAll("\\\""),
            '\n' => try writer.writeAll("\\n"),
            else => try writer.writeByte(c),
        }
    }
}

pub fn writeSanitizedMetricName(writer: *std.Io.Writer, name: []const u8) !void {
    if (name.len == 0) {
        try writer.writeByte('_');
        return;
    }
    if (std.ascii.isDigit(name[0])) try writer.writeByte('_');
    for (name) |raw| {
        const c = std.ascii.toLower(raw);
        if (std.ascii.isAlphanumeric(c) or c == '_') {
            try writer.writeByte(c);
        } else if (c == '-' or c == '.' or c == ' ') {
            try writer.writeByte('_');
        } else {
            try writer.writeByte('_');
        }
    }
}

pub fn writeHelpType(
    writer: *std.Io.Writer,
    metric: []const u8,
    help: []const u8,
    metric_type: []const u8,
) !void {
    try writer.print("# HELP {s} {s}\n", .{ metric, help });
    try writer.print("# TYPE {s} {s}\n", .{ metric, metric_type });
}

test "label escaping" {
    var buf: [64]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&buf);
    try writeLabelValue(&writer, "a\\b\"c\n");
    try std.testing.expectEqualStrings("a\\\\b\\\"c\\n", writer.buffered());
}

test "metric-name sanitization" {
    var buf: [64]u8 = undefined;
    var writer: std.Io.Writer = .fixed(&buf);
    try writeSanitizedMetricName(&writer, "9Bytes-Sent.Total");
    try std.testing.expectEqualStrings("_9bytes_sent_total", writer.buffered());
}
