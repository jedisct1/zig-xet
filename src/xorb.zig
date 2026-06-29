//! Xorb Format: 8-byte header + compressed data
//! Header: version(1) + compressed_size(3) + compression_type(1) + uncompressed_size(3)

const std = @import("std");
const constants = @import("constants.zig");
const hashing = @import("hashing.zig");
const compression = @import("compression.zig");

pub const ChunkHeader = extern struct {
    version: u8,
    compressed_size_low: u8,
    compressed_size_mid: u8,
    compressed_size_high: u8,
    compression_type: u8,
    uncompressed_size_low: u8,
    uncompressed_size_mid: u8,
    uncompressed_size_high: u8,

    pub fn init(
        version: u8,
        compression_type: u8,
        compressed_size: usize,
        uncompressed_size: usize,
    ) ChunkHeader {
        return .{
            .version = version,
            .compressed_size_low = @truncate(compressed_size),
            .compressed_size_mid = @truncate(compressed_size >> 8),
            .compressed_size_high = @truncate(compressed_size >> 16),
            .compression_type = compression_type,
            .uncompressed_size_low = @truncate(uncompressed_size),
            .uncompressed_size_mid = @truncate(uncompressed_size >> 8),
            .uncompressed_size_high = @truncate(uncompressed_size >> 16),
        };
    }

    pub fn getCompressedSize(self: ChunkHeader) usize {
        return @as(usize, self.compressed_size_low) |
            (@as(usize, self.compressed_size_mid) << 8) |
            (@as(usize, self.compressed_size_high) << 16);
    }

    pub fn getUncompressedSize(self: ChunkHeader) usize {
        return @as(usize, self.uncompressed_size_low) |
            (@as(usize, self.uncompressed_size_mid) << 8) |
            (@as(usize, self.uncompressed_size_high) << 16);
    }

    pub fn getCompressionType(self: ChunkHeader) !constants.CompressionType {
        return std.enums.fromInt(constants.CompressionType, self.compression_type) orelse
            error.InvalidCompressionType;
    }
};

pub const Chunk = struct {
    hash: hashing.Hash,
    data: []const u8,
    index: u32,
};

pub const XorbBuilder = struct {
    allocator: std.mem.Allocator,
    chunks: std.ArrayList(Chunk),
    total_size: usize,

    pub fn init(allocator: std.mem.Allocator) XorbBuilder {
        return .{
            .allocator = allocator,
            .chunks = .empty,
            .total_size = 0,
        };
    }

    pub fn deinit(self: *XorbBuilder) void {
        self.chunks.deinit(self.allocator);
    }

    pub fn addChunk(self: *XorbBuilder, data: []const u8) !bool {
        if (self.total_size + data.len > constants.MaxXorbSize) {
            return false;
        }

        try self.chunks.append(self.allocator, .{
            .hash = hashing.computeDataHash(data),
            .data = data,
            .index = @intCast(self.chunks.items.len),
        });
        self.total_size += data.len;
        return true;
    }

    pub fn serialize(self: *XorbBuilder, compression_type: constants.CompressionType) ![]u8 {
        var buffer: std.ArrayList(u8) = .empty;
        errdefer buffer.deinit(self.allocator);

        for (self.chunks.items) |chunk| {
            const compressed = try compression.compress(self.allocator, chunk.data, compression_type);
            defer self.allocator.free(compressed.data);

            const header = ChunkHeader.init(
                constants.XorbVersion,
                @intFromEnum(compressed.type),
                compressed.data.len,
                chunk.data.len,
            );

            try buffer.appendSlice(self.allocator, std.mem.asBytes(&header));
            try buffer.appendSlice(self.allocator, compressed.data);
        }

        return buffer.toOwnedSlice(self.allocator);
    }

    pub fn computeHash(self: *XorbBuilder) !hashing.Hash {
        if (self.chunks.items.len == 0) return error.EmptyXorb;

        const nodes = try self.allocator.alloc(hashing.MerkleNode, self.chunks.items.len);
        defer self.allocator.free(nodes);

        for (self.chunks.items, nodes) |chunk, *node| {
            node.* = .{
                .hash = chunk.hash,
                .size = chunk.data.len,
            };
        }

        return hashing.buildMerkleTree(self.allocator, nodes);
    }
};

pub const XorbReader = struct {
    allocator: std.mem.Allocator,
    data: []const u8,
    position: usize,

    pub fn init(allocator: std.mem.Allocator, data: []const u8) XorbReader {
        return .{
            .allocator = allocator,
            .data = data,
            .position = 0,
        };
    }

    pub fn nextChunk(self: *XorbReader) !?[]u8 {
        if (self.position >= self.data.len) return null;

        if (self.position + constants.XorbChunkHeaderSize > self.data.len) return error.TruncatedXorb;

        const header_bytes = self.data[self.position .. self.position + constants.XorbChunkHeaderSize];
        const header = std.mem.bytesToValue(ChunkHeader, header_bytes[0..@sizeOf(ChunkHeader)]);

        if (header.version != constants.XorbVersion) return error.UnsupportedVersion;

        self.position += constants.XorbChunkHeaderSize;

        const compressed_size = header.getCompressedSize();
        const uncompressed_size = header.getUncompressedSize();
        const compression_type = try header.getCompressionType();

        if (uncompressed_size == 0 and compressed_size > 0) return error.InvalidChunkSize;

        if (self.position + compressed_size > self.data.len) return error.TruncatedXorb;

        const compressed_data = self.data[self.position .. self.position + compressed_size];
        self.position += compressed_size;

        return try compression.decompress(
            self.allocator,
            compressed_data,
            compression_type,
            uncompressed_size,
        );
    }

    pub fn getChunk(self: *XorbReader, index: u32) ![]u8 {
        var reader = XorbReader.init(self.allocator, self.data);
        var current_index: u32 = 0;

        while (try reader.nextChunk()) |chunk| {
            if (current_index == index) {
                return chunk;
            }
            self.allocator.free(chunk);
            current_index += 1;
        }

        return error.ChunkNotFound;
    }

    pub fn extractChunkRange(self: *XorbReader, start: u32, end: u32) ![]u8 {
        if (start >= end) return error.InvalidRange;

        var reader = XorbReader.init(self.allocator, self.data);
        var current_index: u32 = 0;
        var result: std.ArrayList(u8) = .empty;
        errdefer result.deinit(self.allocator);

        while (try reader.nextChunk()) |chunk| {
            defer self.allocator.free(chunk);
            if (current_index >= start and current_index < end) {
                try result.appendSlice(self.allocator, chunk);
            }
            current_index += 1;
            if (current_index >= end) break;
        }

        if (current_index < end) return error.RangeOutOfBounds;

        return result.toOwnedSlice(self.allocator);
    }
};

test "chunk header size encoding/decoding" {
    const test_sizes = [_]usize{ 0, 1, 255, 256, 65535, 65536, 0xFFFFFF };
    for (test_sizes) |size| {
        const header = ChunkHeader.init(0, 0, size, size);
        try std.testing.expectEqual(size, header.getCompressedSize());
        try std.testing.expectEqual(size, header.getUncompressedSize());
    }
}

test "chunk header is 8 bytes" {
    try std.testing.expectEqual(8, @sizeOf(ChunkHeader));
}

test "xorb builder initialization" {
    const allocator = std.testing.allocator;
    var builder = XorbBuilder.init(allocator);
    defer builder.deinit();

    try std.testing.expectEqual(@as(usize, 0), builder.chunks.items.len);
    try std.testing.expectEqual(@as(usize, 0), builder.total_size);
}

test "xorb builder add chunk" {
    const allocator = std.testing.allocator;
    var builder = XorbBuilder.init(allocator);
    defer builder.deinit();

    const data = "Hello, World!";
    const added = try builder.addChunk(data);

    try std.testing.expect(added);
    try std.testing.expectEqual(@as(usize, 1), builder.chunks.items.len);
    try std.testing.expectEqual(data, builder.chunks.items[0].data);
}

test "xorb serialization and deserialization" {
    const allocator = std.testing.allocator;
    var builder = XorbBuilder.init(allocator);
    defer builder.deinit();

    _ = try builder.addChunk("First chunk");
    _ = try builder.addChunk("Second chunk");
    _ = try builder.addChunk("Third chunk");

    const serialized = try builder.serialize(.None);
    defer allocator.free(serialized);

    var reader = XorbReader.init(allocator, serialized);

    const chunk1 = (try reader.nextChunk()).?;
    defer allocator.free(chunk1);
    try std.testing.expectEqualSlices(u8, "First chunk", chunk1);

    const chunk2 = (try reader.nextChunk()).?;
    defer allocator.free(chunk2);
    try std.testing.expectEqualSlices(u8, "Second chunk", chunk2);

    const chunk3 = (try reader.nextChunk()).?;
    defer allocator.free(chunk3);
    try std.testing.expectEqualSlices(u8, "Third chunk", chunk3);

    const no_more = try reader.nextChunk();
    try std.testing.expect(no_more == null);
}

test "xorb hash computation" {
    const allocator = std.testing.allocator;
    var builder = XorbBuilder.init(allocator);
    defer builder.deinit();

    _ = try builder.addChunk("test data");

    const hash1 = try builder.computeHash();
    const hash2 = try builder.computeHash();

    try std.testing.expectEqualSlices(u8, &hash1, &hash2);
}

test "xorb get chunk by index" {
    const allocator = std.testing.allocator;
    var builder = XorbBuilder.init(allocator);
    defer builder.deinit();

    _ = try builder.addChunk("Chunk 0");
    _ = try builder.addChunk("Chunk 1");
    _ = try builder.addChunk("Chunk 2");

    const serialized = try builder.serialize(.None);
    defer allocator.free(serialized);

    var reader = XorbReader.init(allocator, serialized);

    const chunk1 = try reader.getChunk(1);
    defer allocator.free(chunk1);
    try std.testing.expectEqualSlices(u8, "Chunk 1", chunk1);
}
