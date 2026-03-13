const std = @import("std");
const xet = @import("xet");

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer _ = gpa.deinit();
    const allocator = gpa.allocator();

    std.debug.print("XET Protocol Implementation in Zig\n", .{});
    std.debug.print("==================================\n\n", .{});

    // Demonstrate chunking
    std.debug.print("1. Content-Defined Chunking Demo:\n", .{});
    const base_str = "Hello, World! This is a test of the XET protocol chunking system. ";
    var sample_data_list = std.ArrayList(u8).empty;
    defer sample_data_list.deinit(allocator);
    var n: usize = 0;
    while (n < 1000) : (n += 1) {
        try sample_data_list.appendSlice(allocator, base_str);
    }
    const sample_data = sample_data_list.items;
    var chunks = try xet.chunking.chunkBuffer(allocator, sample_data);
    defer chunks.deinit(allocator);

    std.debug.print("   - Input size: {} bytes\n", .{sample_data.len});
    std.debug.print("   - Number of chunks: {}\n", .{chunks.items.len});

    for (chunks.items, 0..) |chunk, i| {
        std.debug.print("   - Chunk {}: {} bytes (offset {}-{})\n", .{
            i,
            chunk.size(),
            chunk.start,
            chunk.end,
        });
        if (i >= 2) {
            std.debug.print("   ...\n", .{});
            break;
        }
    }

    // Demonstrate hashing
    std.debug.print("\n2. BLAKE3 Hashing Demo:\n", .{});
    const test_data = "test data";
    const data_hash = xet.hashing.computeDataHash(test_data);
    const hash_hex = xet.hashing.hashToHex(data_hash);
    std.debug.print("   - Data: \"{s}\"\n", .{test_data});
    std.debug.print("   - Hash: {s}\n", .{hash_hex});

    // Demonstrate Merkle tree
    std.debug.print("\n3. Merkle Tree Demo:\n", .{});
    const merkle_chunks = [_]xet.hashing.MerkleNode{
        .{ .hash = xet.hashing.computeDataHash("chunk1"), .size = 6 },
        .{ .hash = xet.hashing.computeDataHash("chunk2"), .size = 6 },
        .{ .hash = xet.hashing.computeDataHash("chunk3"), .size = 6 },
    };
    const merkle_root = try xet.hashing.buildMerkleTree(allocator, &merkle_chunks);
    const merkle_hex = xet.hashing.hashToHex(merkle_root);
    std.debug.print("   - Chunks: {}\n", .{merkle_chunks.len});
    std.debug.print("   - Merkle root: {s}\n", .{merkle_hex});

    // Demonstrate Xorb serialization
    std.debug.print("\n4. Xorb Serialization Demo:\n", .{});
    var builder = xet.xorb.XorbBuilder.init(allocator);
    defer builder.deinit();

    _ = try builder.addChunk("First chunk in xorb");
    _ = try builder.addChunk("Second chunk in xorb");
    _ = try builder.addChunk("Third chunk in xorb");

    const xorb_data = try builder.serialize(.None);
    defer allocator.free(xorb_data);

    const xorb_hash = try builder.computeHash();
    const xorb_hex = xet.hashing.hashToHex(xorb_hash);

    std.debug.print("   - Chunks in xorb: {}\n", .{builder.chunks.items.len});
    std.debug.print("   - Serialized size: {} bytes\n", .{xorb_data.len});
    std.debug.print("   - Xorb hash: {s}\n", .{xorb_hex});

    // Demonstrate Shard format
    std.debug.print("\n5. Shard Format Demo:\n", .{});
    var shard_builder = xet.shard.ShardBuilder.init(allocator);
    defer shard_builder.deinit();

    const file_hash = xet.hashing.computeDataHash("example file");
    const entries = [_]xet.shard.FileDataSequenceEntry{
        .{
            .xorb_hash = xorb_hash,
            .cas_flags = 0,
            .unpacked_segment_size = 1000,
            .chunk_index_start = 0,
            .chunk_index_end = 3,
        },
    };

    try shard_builder.addFileInfo(file_hash, &entries);

    const shard_data = try shard_builder.serialize();
    defer allocator.free(shard_data);

    std.debug.print("   - Shard size: {} bytes\n", .{shard_data.len});
    std.debug.print("   - Header: {} bytes\n", .{xet.constants.MdbHeaderSize});
    std.debug.print("   - Footer: {} bytes\n", .{xet.constants.MdbFooterSize});

    std.debug.print("\nAll XET protocol components working!\n", .{});
    std.debug.print("\nRun 'zig build test' to run the comprehensive test suite.\n", .{});
}
