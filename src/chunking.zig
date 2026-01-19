//! Content-Defined Chunking (CDC) with multiple algorithm support
//!
//! Gearhash (default for XET protocol):
//!   Rolling hash: h = (h << 1) + TABLE[byte] with wrapping arithmetic
//!   Boundary: (h & 0xFFFF000000000000) == 0
//!   Cut-point skipping: skip first (MIN - 65) bytes
//!
//! UltraCDC (alternative):
//!   Fast CDC using hamming distance-based boundary detection
//!   Adaptive masking with low-entropy detection

const std = @import("std");
const constants = @import("constants.zig");
const ultracdc = @import("ultracdc");

pub const ChunkingAlgorithm = enum {
    gearhash,
    ultracdc,
};

pub const ChunkBoundary = struct {
    start: usize,
    end: usize,

    pub fn size(self: ChunkBoundary) usize {
        return self.end - self.start;
    }
};

pub const Chunker = struct {
    hash: u64,
    position: usize,
    chunk_start: usize,
    first_chunk: bool,
    algorithm: ChunkingAlgorithm,

    pub fn init() Chunker {
        return initWithAlgorithm(.gearhash);
    }

    pub fn initWithAlgorithm(algorithm: ChunkingAlgorithm) Chunker {
        return .{
            .hash = 0,
            .position = 0,
            .chunk_start = 0,
            .first_chunk = true,
            .algorithm = algorithm,
        };
    }

    pub fn reset(self: *Chunker) void {
        self.hash = 0;
        self.position = 0;
        self.chunk_start = 0;
        self.first_chunk = true;
    }

    inline fn updateHash(self: *Chunker, byte: u8) void {
        self.hash = self.hash +% (self.hash +% constants.GearHashTable[byte]);
    }

    inline fn isBoundary(self: *Chunker) bool {
        const chunk_size = self.position - self.chunk_start;
        if (chunk_size >= constants.MaxChunkSize) return true;
        if (chunk_size < constants.MinChunkSize) return false;
        return (self.hash & constants.GearHashMask) == 0;
    }

    pub fn findNextChunk(self: *Chunker, data: []const u8) ?ChunkBoundary {
        return switch (self.algorithm) {
            .gearhash => self.findNextChunkGearhash(data),
            .ultracdc => self.findNextChunkUltracdc(data),
        };
    }

    fn findNextChunkGearhash(self: *Chunker, data: []const u8) ?ChunkBoundary {
        var offset: usize = 0;

        while (offset < data.len) : (offset += 1) {
            const byte = data[offset];
            self.position += 1;

            const chunk_size = self.position - self.chunk_start;
            if (self.first_chunk and chunk_size < constants.MinChunkSize - 64 - 1) continue;

            self.updateHash(byte);

            if (self.isBoundary()) {
                const boundary = ChunkBoundary{
                    .start = self.chunk_start,
                    .end = self.position,
                };
                self.chunk_start = self.position;
                self.hash = 0;
                self.first_chunk = false;
                return boundary;
            }
        }
        return null;
    }

    fn findNextChunkUltracdc(self: *Chunker, data: []const u8) ?ChunkBoundary {
        const remaining = data.len;
        if (remaining == 0) return null;

        const options = ultracdc.ChunkerOptions{
            .min_size = constants.MinChunkSize,
            .normal_size = constants.TargetChunkSize,
            .max_size = constants.MaxChunkSize,
        };

        const cutpoint = ultracdc.UltraCDC.find(options, data, remaining);
        if (cutpoint >= remaining) return null;

        const boundary = ChunkBoundary{
            .start = self.chunk_start,
            .end = self.chunk_start + cutpoint,
        };
        self.position = self.chunk_start + cutpoint;
        self.chunk_start = self.position;
        self.first_chunk = false;
        return boundary;
    }

    pub fn finalize(self: *Chunker) ?ChunkBoundary {
        if (self.position > self.chunk_start) {
            const boundary = ChunkBoundary{
                .start = self.chunk_start,
                .end = self.position,
            };
            self.chunk_start = self.position;
            return boundary;
        }
        return null;
    }
};

pub fn chunkBuffer(allocator: std.mem.Allocator, data: []const u8) !std.ArrayList(ChunkBoundary) {
    return chunkBufferWithAlgorithm(allocator, data, .gearhash);
}

pub fn chunkBufferWithAlgorithm(allocator: std.mem.Allocator, data: []const u8, algorithm: ChunkingAlgorithm) !std.ArrayList(ChunkBoundary) {
    var chunks: std.ArrayList(ChunkBoundary) = .empty;
    errdefer chunks.deinit(allocator);

    var chunker = Chunker.initWithAlgorithm(algorithm);
    var offset: usize = 0;

    while (offset < data.len) {
        if (chunker.findNextChunk(data[offset..])) |boundary| {
            try chunks.append(allocator, boundary);
            offset = boundary.end;
        } else break;
    }

    if (chunker.finalize()) |boundary| {
        try chunks.append(allocator, boundary);
    }

    return chunks;
}

test "chunker initialization" {
    const chunker = Chunker.init();
    try std.testing.expectEqual(@as(u64, 0), chunker.hash);
    try std.testing.expectEqual(@as(usize, 0), chunker.position);
    try std.testing.expectEqual(@as(usize, 0), chunker.chunk_start);
    try std.testing.expect(chunker.first_chunk);
}

test "chunker reset" {
    var chunker = Chunker.init();
    chunker.hash = 12345;
    chunker.position = 100;
    chunker.chunk_start = 50;
    chunker.first_chunk = false;

    chunker.reset();

    try std.testing.expectEqual(@as(u64, 0), chunker.hash);
    try std.testing.expectEqual(@as(usize, 0), chunker.position);
    try std.testing.expectEqual(@as(usize, 0), chunker.chunk_start);
    try std.testing.expect(chunker.first_chunk);
}

test "chunk boundary size calculation" {
    const boundary = ChunkBoundary{ .start = 100, .end = 200 };
    try std.testing.expectEqual(@as(usize, 100), boundary.size());
}

test "small buffer chunking" {
    const allocator = std.testing.allocator;

    // Create a small buffer (smaller than min chunk size)
    const data = "Hello, World!";
    var chunks = try chunkBuffer(allocator, data);
    defer chunks.deinit(allocator);

    // Should produce exactly one chunk
    try std.testing.expectEqual(@as(usize, 1), chunks.items.len);
    try std.testing.expectEqual(@as(usize, 0), chunks.items[0].start);
    try std.testing.expectEqual(@as(usize, data.len), chunks.items[0].end);
}

test "deterministic chunking" {
    const allocator = std.testing.allocator;

    // Create a larger test buffer
    var data: [100000]u8 = undefined;
    for (&data, 0..) |*byte, i| {
        byte.* = @intCast(i % 256);
    }

    // Chunk the same data twice
    var chunks1 = try chunkBuffer(allocator, &data);
    defer chunks1.deinit(allocator);

    var chunks2 = try chunkBuffer(allocator, &data);
    defer chunks2.deinit(allocator);

    // Should produce identical results
    try std.testing.expectEqual(chunks1.items.len, chunks2.items.len);
    for (chunks1.items, chunks2.items) |c1, c2| {
        try std.testing.expectEqual(c1.start, c2.start);
        try std.testing.expectEqual(c1.end, c2.end);
    }
}

test "chunk sizes are within bounds" {
    const allocator = std.testing.allocator;

    // Create a large random-like buffer
    var data: [1024 * 1024]u8 = undefined;
    var prng = std.Random.DefaultPrng.init(12345);
    const random = prng.random();
    random.bytes(&data);

    var chunks = try chunkBuffer(allocator, &data);
    defer chunks.deinit(allocator);

    // Check that all chunks (except possibly the last) are within bounds
    for (chunks.items, 0..) |chunk, i| {
        const size = chunk.size();
        if (i < chunks.items.len - 1) {
            // All non-final chunks must be <= MaxChunkSize
            try std.testing.expect(size <= constants.MaxChunkSize);
        }
        // All chunks should be > 0
        try std.testing.expect(size > 0);
    }
}

test "max chunk size enforcement" {
    const allocator = std.testing.allocator;

    // Create a buffer with all zeros (unlikely to hit content-defined boundary)
    const data: [constants.MaxChunkSize * 2]u8 = @splat(0);
    var chunks = try chunkBuffer(allocator, &data);
    defer chunks.deinit(allocator);

    // Should be forced to split at MaxChunkSize
    try std.testing.expect(chunks.items.len >= 2);
    for (chunks.items[0 .. chunks.items.len - 1]) |chunk| {
        try std.testing.expectEqual(constants.MaxChunkSize, chunk.size());
    }
}

test "ultracdc chunker initialization" {
    const chunker = Chunker.initWithAlgorithm(.ultracdc);
    try std.testing.expectEqual(ChunkingAlgorithm.ultracdc, chunker.algorithm);
    try std.testing.expectEqual(@as(usize, 0), chunker.position);
    try std.testing.expectEqual(@as(usize, 0), chunker.chunk_start);
}

test "ultracdc deterministic chunking" {
    const allocator = std.testing.allocator;

    var data: [100000]u8 = undefined;
    for (&data, 0..) |*byte, i| {
        byte.* = @intCast(i % 256);
    }

    var chunks1 = try chunkBufferWithAlgorithm(allocator, &data, .ultracdc);
    defer chunks1.deinit(allocator);

    var chunks2 = try chunkBufferWithAlgorithm(allocator, &data, .ultracdc);
    defer chunks2.deinit(allocator);

    try std.testing.expectEqual(chunks1.items.len, chunks2.items.len);
    for (chunks1.items, chunks2.items) |c1, c2| {
        try std.testing.expectEqual(c1.start, c2.start);
        try std.testing.expectEqual(c1.end, c2.end);
    }
}

test "ultracdc chunk sizes are within bounds" {
    const allocator = std.testing.allocator;

    var data: [1024 * 1024]u8 = undefined;
    var prng = std.Random.DefaultPrng.init(12345);
    const random = prng.random();
    random.bytes(&data);

    var chunks = try chunkBufferWithAlgorithm(allocator, &data, .ultracdc);
    defer chunks.deinit(allocator);

    for (chunks.items, 0..) |chunk, i| {
        const size = chunk.size();
        if (i < chunks.items.len - 1) {
            try std.testing.expect(size <= constants.MaxChunkSize);
        }
        try std.testing.expect(size > 0);
    }
}

test "gearhash and ultracdc produce different boundaries" {
    const allocator = std.testing.allocator;

    var data: [500000]u8 = undefined;
    var prng = std.Random.DefaultPrng.init(42);
    const random = prng.random();
    random.bytes(&data);

    var gearhash_chunks = try chunkBufferWithAlgorithm(allocator, &data, .gearhash);
    defer gearhash_chunks.deinit(allocator);

    var ultracdc_chunks = try chunkBufferWithAlgorithm(allocator, &data, .ultracdc);
    defer ultracdc_chunks.deinit(allocator);

    try std.testing.expect(gearhash_chunks.items.len > 1);
    try std.testing.expect(ultracdc_chunks.items.len > 1);

    const counts_differ = gearhash_chunks.items.len != ultracdc_chunks.items.len;
    var boundaries_differ = false;
    if (!counts_differ and gearhash_chunks.items.len > 0) {
        for (gearhash_chunks.items, ultracdc_chunks.items) |g, u| {
            if (g.end != u.end) {
                boundaries_differ = true;
                break;
            }
        }
    }
    try std.testing.expect(counts_differ or boundaries_differ);
}
