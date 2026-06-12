//! XET Protocol Compression: None, LZ4, ByteGrouping4LZ4

const std = @import("std");
const constants = @import("constants.zig");
const lz4 = @import("lz4");

pub const CompressionError = error{
    CompressionFailed,
    DecompressionFailed,
    InvalidCompressionType,
    BufferTooSmall,
    InvalidUncompressedSize,
};

pub const CompressionResult = struct {
    data: []u8,
    type: constants.CompressionType,
};

fn lz4FrameCompress(allocator: std.mem.Allocator, data: []const u8) ![]u8 {
    const max_size = lz4.lz4f.compressFrameBound(data.len, null);
    const compressed_buffer = try allocator.alloc(u8, max_size);
    errdefer allocator.free(compressed_buffer);

    const compressed_size = lz4.lz4f.compressFrame(
        allocator,
        data,
        compressed_buffer,
        null,
    ) catch return error.CompressionFailed;

    return allocator.realloc(compressed_buffer, compressed_size);
}

/// Compress `payload` with LZ4. If that does not shrink below `original.len`,
/// fall back to a copy of `original` (the untransformed bytes) tagged as `.None`.
fn compressLZ4(
    allocator: std.mem.Allocator,
    payload: []const u8,
    original: []const u8,
    compression_type: constants.CompressionType,
) !CompressionResult {
    const compressed_data = try lz4FrameCompress(allocator, payload);

    if (compressed_data.len >= original.len) {
        allocator.free(compressed_data);
        return .{ .data = try allocator.dupe(u8, original), .type = .None };
    }

    return .{ .data = compressed_data, .type = compression_type };
}

pub fn compress(
    allocator: std.mem.Allocator,
    data: []const u8,
    compression_type: constants.CompressionType,
) !CompressionResult {
    switch (compression_type) {
        .None => {
            return .{ .data = try allocator.dupe(u8, data), .type = .None };
        },
        .LZ4 => {
            return compressLZ4(allocator, data, data, .LZ4);
        },
        .ByteGrouping4LZ4 => {
            const grouped = try applyByteGrouping(allocator, data);
            defer allocator.free(grouped);
            return compressLZ4(allocator, grouped, data, .ByteGrouping4LZ4);
        },
        .FullBitsliceLZ4 => {
            const bitsliced = try applyFullBitslice(allocator, data);
            defer allocator.free(bitsliced);
            return compressLZ4(allocator, bitsliced, data, .FullBitsliceLZ4);
        },
    }
}

fn lz4FrameDecompress(allocator: std.mem.Allocator, data: []const u8, expected_size: usize) ![]u8 {
    const decompressed_buffer = try allocator.alloc(u8, expected_size);
    errdefer allocator.free(decompressed_buffer);

    const decompressed_size = lz4.lz4f.decompressFrame(
        allocator,
        data,
        decompressed_buffer,
    ) catch return error.DecompressionFailed;

    if (decompressed_size != expected_size) return error.InvalidUncompressedSize;

    return decompressed_buffer;
}

pub fn decompress(
    allocator: std.mem.Allocator,
    data: []const u8,
    compression_type: constants.CompressionType,
    uncompressed_size: usize,
) ![]u8 {
    switch (compression_type) {
        .None => {
            if (data.len != uncompressed_size) {
                return error.InvalidUncompressedSize;
            }
            return allocator.dupe(u8, data);
        },
        .LZ4 => {
            return lz4FrameDecompress(allocator, data, uncompressed_size);
        },
        .ByteGrouping4LZ4 => {
            const lz4_decompressed = try lz4FrameDecompress(allocator, data, uncompressed_size);
            defer allocator.free(lz4_decompressed);
            return reverseByteGrouping(allocator, lz4_decompressed);
        },
        .FullBitsliceLZ4 => {
            const lz4_decompressed = try lz4FrameDecompress(allocator, data, uncompressed_size);
            defer allocator.free(lz4_decompressed);
            return reverseFullBitslice(allocator, lz4_decompressed);
        },
    }
}

const ByteGroupOffsets = struct {
    split: usize,
    rem: usize,
    g1: usize,
    g2: usize,
    g3: usize,

    fn init(len: usize) ByteGroupOffsets {
        const split = len / 4;
        const rem = len % 4;
        const g0_size = split + @min(1, rem);
        const g1_size = split + @min(1, if (rem >= 1) rem - 1 else 0);
        const g2_size = split + @min(1, if (rem >= 2) rem - 2 else 0);
        return .{
            .split = split,
            .rem = rem,
            .g1 = g0_size,
            .g2 = g0_size + g1_size,
            .g3 = g0_size + g1_size + g2_size,
        };
    }
};

pub fn applyByteGrouping(allocator: std.mem.Allocator, data: []const u8) ![]u8 {
    const result = try allocator.alloc(u8, data.len);
    errdefer allocator.free(result);

    const g = ByteGroupOffsets.init(data.len);

    for (0..g.split) |i| {
        result[i] = data[4 * i];
        result[g.g1 + i] = data[4 * i + 1];
        result[g.g2 + i] = data[4 * i + 2];
        result[g.g3 + i] = data[4 * i + 3];
    }

    if (g.rem >= 1) result[g.split] = data[4 * g.split];
    if (g.rem >= 2) result[g.g1 + g.split] = data[4 * g.split + 1];
    if (g.rem >= 3) result[g.g2 + g.split] = data[4 * g.split + 2];

    return result;
}

pub fn reverseByteGrouping(allocator: std.mem.Allocator, data: []const u8) ![]u8 {
    const result = try allocator.alloc(u8, data.len);
    errdefer allocator.free(result);

    const g = ByteGroupOffsets.init(data.len);

    for (0..g.split) |i| {
        result[4 * i] = data[i];
        result[4 * i + 1] = data[g.g1 + i];
        result[4 * i + 2] = data[g.g2 + i];
        result[4 * i + 3] = data[g.g3 + i];
    }

    if (g.rem >= 1) result[4 * g.split] = data[g.split];
    if (g.rem >= 2) result[4 * g.split + 1] = data[g.g1 + g.split];
    if (g.rem >= 3) result[4 * g.split + 2] = data[g.g2 + g.split];

    return result;
}

pub fn applyFullBitslice(allocator: std.mem.Allocator, data: []const u8) ![]u8 {
    const result = try allocator.alloc(u8, data.len);
    errdefer allocator.free(result);

    const n = data.len;
    for (0..n) |out_byte_idx| {
        var out_byte: u8 = 0;
        for (0..8) |out_bit_idx| {
            const k = out_byte_idx * 8 + out_bit_idx;
            const in_byte_idx = k % n;
            const in_bit_idx = k / n;

            const bit: u8 = (data[in_byte_idx] >> @intCast(in_bit_idx)) & 1;
            out_byte |= bit << @intCast(out_bit_idx);
        }
        result[out_byte_idx] = out_byte;
    }

    return result;
}

pub fn reverseFullBitslice(allocator: std.mem.Allocator, data: []const u8) ![]u8 {
    const result = try allocator.alloc(u8, data.len);
    errdefer allocator.free(result);
    @memset(result, 0);

    const n = data.len;
    if (n == 0) return result;

    for (0..n) |in_byte_idx| {
        for (0..8) |in_bit_idx| {
            const k = in_byte_idx * 8 + in_bit_idx;
            const orig_byte_idx = k % n;
            const orig_bit_idx = k / n;

            const bit: u8 = (data[in_byte_idx] >> @intCast(in_bit_idx)) & 1;
            result[orig_byte_idx] |= bit << @intCast(orig_bit_idx);
        }
    }

    return result;
}

test "compress with no compression" {
    const allocator = std.testing.allocator;
    const data = "Hello, World!";

    const result = try compress(allocator, data, .None);
    defer allocator.free(result.data);

    try std.testing.expectEqualSlices(u8, data, result.data);
    try std.testing.expectEqual(constants.CompressionType.None, result.type);
}

test "decompress with no compression" {
    const allocator = std.testing.allocator;
    const data = "Hello, World!";

    const result = try decompress(allocator, data, .None, data.len);
    defer allocator.free(result);

    try std.testing.expectEqualSlices(u8, data, result);
}

test "byte grouping with 4-byte aligned data" {
    const allocator = std.testing.allocator;
    // Input: A1 A2 A3 A4 B1 B2 B3 B4
    const data = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8 };

    const grouped = try applyByteGrouping(allocator, &data);
    defer allocator.free(grouped);

    // Expected: A1 B1 A2 B2 A3 B3 A4 B4
    const expected = [_]u8{ 1, 5, 2, 6, 3, 7, 4, 8 };
    try std.testing.expectEqualSlices(u8, &expected, grouped);
}

test "byte grouping with non-aligned data" {
    const allocator = std.testing.allocator;
    // 10 bytes = 2 full groups (8 bytes) + 2 remaining
    // Input: [1,2,3,4,5,6,7,8,9,10]
    const data = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10 };

    const grouped = try applyByteGrouping(allocator, &data);
    defer allocator.free(grouped);

    // split=2, rem=2
    // g0: [1,5,9] (size=3)
    // g1: [2,6,10] (size=3)
    // g2: [3,7] (size=2)
    // g3: [4,8] (size=2)
    // Concatenated: [1,5,9,2,6,10,3,7,4,8]
    const expected = [_]u8{ 1, 5, 9, 2, 6, 10, 3, 7, 4, 8 };
    try std.testing.expectEqualSlices(u8, &expected, grouped);
}

test "byte grouping round trip" {
    const allocator = std.testing.allocator;
    const original = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };

    const grouped = try applyByteGrouping(allocator, &original);
    defer allocator.free(grouped);

    const ungrouped = try reverseByteGrouping(allocator, grouped);
    defer allocator.free(ungrouped);

    try std.testing.expectEqualSlices(u8, &original, ungrouped);
}

test "byte grouping with single byte" {
    const allocator = std.testing.allocator;
    const data = [_]u8{42};

    const grouped = try applyByteGrouping(allocator, &data);
    defer allocator.free(grouped);

    try std.testing.expectEqualSlices(u8, &data, grouped);
}

test "LZ4 compression and decompression" {
    const allocator = std.testing.allocator;
    const base_str = "Hello, World! This is a test of LZ4 compression. ";
    const original = base_str ++ base_str ++ base_str ++ base_str ++ base_str ++
        base_str ++ base_str ++ base_str ++ base_str ++ base_str;

    const compressed_result = try compress(allocator, original, .LZ4);
    defer allocator.free(compressed_result.data);

    try std.testing.expect(compressed_result.data.len < original.len);
    try std.testing.expectEqual(constants.CompressionType.LZ4, compressed_result.type);

    const decompressed = try decompress(
        allocator,
        compressed_result.data,
        compressed_result.type,
        original.len,
    );
    defer allocator.free(decompressed);

    try std.testing.expectEqualSlices(u8, original, decompressed);
}

test "LZ4 with incompressible data" {
    const allocator = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(12345);
    const random = prng.random();

    var data: [100]u8 = undefined;
    random.bytes(&data);

    const result = try compress(allocator, &data, .LZ4);
    defer allocator.free(result.data);

    // Should fall back to no compression if it doesn't reduce size
    // (may or may not compress, depends on random data)
}

test "ByteGrouping4LZ4 compression and decompression" {
    const allocator = std.testing.allocator;
    // Create data with patterns that benefit from byte grouping
    const base_pattern = [_]u8{ 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0 };
    const pattern = base_pattern ++ base_pattern ++ base_pattern ++ base_pattern ++ base_pattern ++
        base_pattern ++ base_pattern ++ base_pattern ++ base_pattern ++ base_pattern ++
        base_pattern ++ base_pattern ++ base_pattern ++ base_pattern ++ base_pattern ++
        base_pattern ++ base_pattern ++ base_pattern ++ base_pattern ++ base_pattern;

    const compressed_result = try compress(allocator, &pattern, .ByteGrouping4LZ4);
    defer allocator.free(compressed_result.data);

    // ByteGrouping should improve compression for this pattern
    try std.testing.expect(compressed_result.data.len < pattern.len);
    try std.testing.expectEqual(constants.CompressionType.ByteGrouping4LZ4, compressed_result.type);

    const decompressed = try decompress(
        allocator,
        compressed_result.data,
        compressed_result.type,
        pattern.len,
    );
    defer allocator.free(decompressed);

    try std.testing.expectEqualSlices(u8, &pattern, decompressed);
}

test "ByteGrouping4LZ4 falls back to None when not smaller" {
    const allocator = std.testing.allocator;
    // Small, incompressible data; grouped form would not shrink.
    const data = "abcd";

    const compressed_result = try compress(allocator, data, .ByteGrouping4LZ4);
    defer allocator.free(compressed_result.data);

    try std.testing.expectEqual(constants.CompressionType.None, compressed_result.type);
    try std.testing.expectEqualSlices(u8, data, compressed_result.data);
}

test "LZ4 compression with small data" {
    const allocator = std.testing.allocator;
    const original = "Hi";

    const compressed_result = try compress(allocator, original, .LZ4);
    defer allocator.free(compressed_result.data);

    // Small data might not compress well, but should still work
    const decompressed = try decompress(
        allocator,
        compressed_result.data,
        compressed_result.type,
        original.len,
    );
    defer allocator.free(decompressed);

    try std.testing.expectEqualSlices(u8, original, decompressed);
}

test "byte grouping with remainder 1" {
    const allocator = std.testing.allocator;
    // 13 bytes = 3 full groups (12 bytes) + 1 remaining
    const data = [_]u8{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };

    const grouped = try applyByteGrouping(allocator, &data);
    defer allocator.free(grouped);

    // split=3, rem=1
    // g0: [0,4,8,12] (size=4)
    // g1: [1,5,9] (size=3)
    // g2: [2,6,10] (size=3)
    // g3: [3,7,11] (size=3)
    const expected = [_]u8{ 0, 4, 8, 12, 1, 5, 9, 2, 6, 10, 3, 7, 11 };
    try std.testing.expectEqualSlices(u8, &expected, grouped);

    // Verify round trip
    const ungrouped = try reverseByteGrouping(allocator, grouped);
    defer allocator.free(ungrouped);
    try std.testing.expectEqualSlices(u8, &data, ungrouped);
}

test "byte grouping with remainder 3" {
    const allocator = std.testing.allocator;
    // 15 bytes = 3 full groups (12 bytes) + 3 remaining
    const data = [_]u8{ 0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14 };

    const grouped = try applyByteGrouping(allocator, &data);
    defer allocator.free(grouped);

    // split=3, rem=3
    // g0: [0,4,8,12] (size=4)
    // g1: [1,5,9,13] (size=4)
    // g2: [2,6,10,14] (size=4)
    // g3: [3,7,11] (size=3)
    const expected = [_]u8{ 0, 4, 8, 12, 1, 5, 9, 13, 2, 6, 10, 14, 3, 7, 11 };
    try std.testing.expectEqualSlices(u8, &expected, grouped);

    // Verify round trip
    const ungrouped = try reverseByteGrouping(allocator, grouped);
    defer allocator.free(ungrouped);
    try std.testing.expectEqualSlices(u8, &data, ungrouped);
}

test "byte grouping large data round trip" {
    const allocator = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(42);
    const random = prng.random();

    const sizes = [_]usize{
        64 * 1024,
        64 * 1024 - 53,
        64 * 1024 + 135,
        1000,
        1,
        7,
    };

    for (sizes) |size| {
        const data = try allocator.alloc(u8, size);
        defer allocator.free(data);
        random.bytes(data);

        const grouped = try applyByteGrouping(allocator, data);
        defer allocator.free(grouped);

        const ungrouped = try reverseByteGrouping(allocator, grouped);
        defer allocator.free(ungrouped);

        try std.testing.expectEqualSlices(u8, data, ungrouped);
    }
}

test "ByteGrouping4LZ4 with model-like data" {
    const allocator = std.testing.allocator;
    // Simulate float32 data (4 bytes per float) with patterns
    // This is similar to what model weights look like
    var data = try allocator.alloc(u8, 1024);
    defer allocator.free(data);

    // Fill with pattern: little-endian float-like values
    for (0..256) |i| {
        const idx = i * 4;
        data[idx] = @truncate(i);
        data[idx + 1] = 0;
        data[idx + 2] = 0;
        data[idx + 3] = 0x3F; // Sign/exponent bits
    }

    // Compress with regular LZ4
    const lz4_result = try compress(allocator, data, .LZ4);
    defer allocator.free(lz4_result.data);

    // Compress with ByteGrouping4LZ4
    const bg4_result = try compress(allocator, data, .ByteGrouping4LZ4);
    defer allocator.free(bg4_result.data);

    // ByteGrouping4LZ4 should compress better for this pattern
    try std.testing.expect(bg4_result.data.len < lz4_result.data.len);

    const decompressed = try decompress(
        allocator,
        bg4_result.data,
        bg4_result.type,
        data.len,
    );
    defer allocator.free(decompressed);

    try std.testing.expectEqualSlices(u8, data, decompressed);
}

test "full bitslice with 8 bytes" {
    const allocator = std.testing.allocator;
    const data = [_]u8{ 0b11111111, 0b00000000, 0b10101010, 0b01010101, 0b11110000, 0b00001111, 0b11001100, 0b00110011 };

    const sliced = try applyFullBitslice(allocator, &data);
    defer allocator.free(sliced);

    const unsliced = try reverseFullBitslice(allocator, sliced);
    defer allocator.free(unsliced);

    try std.testing.expectEqualSlices(u8, &data, unsliced);
}

test "full bitslice round trip" {
    const allocator = std.testing.allocator;
    const original = [_]u8{ 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12 };

    const sliced = try applyFullBitslice(allocator, &original);
    defer allocator.free(sliced);

    const unsliced = try reverseFullBitslice(allocator, sliced);
    defer allocator.free(unsliced);

    try std.testing.expectEqualSlices(u8, &original, unsliced);
}

test "full bitslice with single byte" {
    const allocator = std.testing.allocator;
    const data = [_]u8{0b10110100};

    const sliced = try applyFullBitslice(allocator, &data);
    defer allocator.free(sliced);

    try std.testing.expectEqualSlices(u8, &data, sliced);
}

test "full bitslice large data round trip" {
    const allocator = std.testing.allocator;
    var prng = std.Random.DefaultPrng.init(42);
    const random = prng.random();

    const sizes = [_]usize{
        64 * 1024,
        64 * 1024 - 53,
        64 * 1024 + 135,
        1000,
        1,
        7,
        8,
        9,
        15,
        16,
        17,
    };

    for (sizes) |size| {
        const data = try allocator.alloc(u8, size);
        defer allocator.free(data);
        random.bytes(data);

        const sliced = try applyFullBitslice(allocator, data);
        defer allocator.free(sliced);

        const unsliced = try reverseFullBitslice(allocator, sliced);
        defer allocator.free(unsliced);

        try std.testing.expectEqualSlices(u8, data, unsliced);
    }
}

test "FullBitsliceLZ4 falls back to None when not smaller" {
    const allocator = std.testing.allocator;
    // Very small, hard-to-compress data should not stay bitsliced when falling back.
    const data = "abc";

    const compressed_result = try compress(allocator, data, .FullBitsliceLZ4);
    defer allocator.free(compressed_result.data);

    try std.testing.expectEqual(constants.CompressionType.None, compressed_result.type);
    try std.testing.expectEqualSlices(u8, data, compressed_result.data);
}

test "FullBitsliceLZ4 compression and decompression" {
    const allocator = std.testing.allocator;
    const base_pattern = [_]u8{ 1, 0, 0, 0, 2, 0, 0, 0, 3, 0, 0, 0 };
    const pattern = base_pattern ++ base_pattern ++ base_pattern ++ base_pattern ++ base_pattern ++
        base_pattern ++ base_pattern ++ base_pattern ++ base_pattern ++ base_pattern ++
        base_pattern ++ base_pattern ++ base_pattern ++ base_pattern ++ base_pattern ++
        base_pattern ++ base_pattern ++ base_pattern ++ base_pattern ++ base_pattern;

    const compressed_result = try compress(allocator, &pattern, .FullBitsliceLZ4);
    defer allocator.free(compressed_result.data);

    try std.testing.expect(compressed_result.data.len < pattern.len);
    try std.testing.expectEqual(constants.CompressionType.FullBitsliceLZ4, compressed_result.type);

    const decompressed = try decompress(
        allocator,
        compressed_result.data,
        compressed_result.type,
        pattern.len,
    );
    defer allocator.free(decompressed);

    try std.testing.expectEqualSlices(u8, &pattern, decompressed);
}

test "FullBitsliceLZ4 with model-like data" {
    const allocator = std.testing.allocator;
    var data = try allocator.alloc(u8, 1024);
    defer allocator.free(data);

    for (0..256) |i| {
        const idx = i * 4;
        data[idx] = @truncate(i);
        data[idx + 1] = 0;
        data[idx + 2] = 0;
        data[idx + 3] = 0x3F;
    }

    const lz4_result = try compress(allocator, data, .LZ4);
    defer allocator.free(lz4_result.data);

    const fbs_result = try compress(allocator, data, .FullBitsliceLZ4);
    defer allocator.free(fbs_result.data);

    const decompressed = try decompress(
        allocator,
        fbs_result.data,
        fbs_result.type,
        data.len,
    );
    defer allocator.free(decompressed);

    try std.testing.expectEqualSlices(u8, data, decompressed);
}
