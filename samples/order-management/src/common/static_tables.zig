// SPDX-License-Identifier: Apache-2.0

const std = @import("std");
const protocol = @import("protocol.zig");

pub const account_count = 4;
pub const symbol_count = 4;
pub const min_price_nanos: i64 = 1_000_000_000;
pub const max_price_nanos: i64 = 1_000_000_000_000;

pub const Account = struct {
    id: u32,
    credit_nanos: i64,
};

pub const accounts = [_]Account{
    .{ .id = 1001, .credit_nanos = 4_000_000_000_000_000_000 },
    .{ .id = 1002, .credit_nanos = 4_000_000_000_000_000_000 },
    .{ .id = 1003, .credit_nanos = 4_000_000_000_000_000_000 },
    .{ .id = 1004, .credit_nanos = 4_000_000_000_000_000_000 },
};

pub const symbols = [_]protocol.Symbol{ .aapl, .msft, .nvda, .zig };
pub const symbol_notional_limits = [_]i64{
    2_000_000_000_000_000_000,
    3_000_000_000_000_000_000,
    5_000_000_000_000_000_000,
    1_000_000_000_000_000_000,
};

pub fn accountIndex(account_id: u32) ?usize {
    for (accounts, 0..) |account, i| {
        if (account.id == account_id) return i;
    }
    return null;
}

pub fn symbolIndex(symbol: protocol.Symbol) ?usize {
    for (symbols, 0..) |known, i| {
        if (known == symbol) return i;
    }
    return null;
}

pub fn isKnownSymbol(symbol: protocol.Symbol) bool {
    return symbolIndex(symbol) != null;
}

pub fn startingCredit(account_id: u32) i64 {
    const idx = accountIndex(account_id) orelse return 0;
    return accounts[idx].credit_nanos;
}

pub fn validPrice(price_nanos: i64) bool {
    return price_nanos >= min_price_nanos and price_nanos <= max_price_nanos;
}

pub fn deterministicPrice(sequence: u64, symbol: protocol.Symbol) i64 {
    const base: i64 = switch (symbol) {
        .aapl => 185_000_000_000,
        .msft => 410_000_000_000,
        .nvda => 920_000_000_000,
        .zig => 42_000_000_000,
    };
    return base + @as(i64, @intCast(sequence % 500)) * 1_000_000;
}

test "static tables map known accounts and symbols densely" {
    try std.testing.expectEqual(@as(?usize, 0), accountIndex(1001));
    try std.testing.expectEqual(@as(?usize, 3), symbolIndex(.zig));
    try std.testing.expect(isKnownSymbol(.aapl));
}
