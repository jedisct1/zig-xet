const std = @import("std");
const Allocator = std.mem.Allocator;
const chunking = @import("chunking.zig");
const hashing = @import("hashing.zig");
const xorb = @import("xorb.zig");
const shard = @import("shard.zig");
const cas_client = @import("cas_client.zig");
const constants = @import("constants.zig");

pub const UploadResult = struct {
    file_hash: hashing.Hash,
    file_hash_hex: [64]u8,
    total_size: usize,
};

pub fn uploadData(
    allocator: Allocator,
    cas: *cas_client.CasClient,
    data: []const u8,
) !UploadResult {
    return uploadDataWithOptions(allocator, cas, data, .LZ4, .gearhash);
}

pub fn uploadDataWithCompression(
    allocator: Allocator,
    cas: *cas_client.CasClient,
    data: []const u8,
    compression_type: constants.CompressionType,
) !UploadResult {
    return uploadDataWithOptions(allocator, cas, data, compression_type, .gearhash);
}

pub fn uploadDataWithOptions(
    allocator: Allocator,
    cas: *cas_client.CasClient,
    data: []const u8,
    compression_type: constants.CompressionType,
    chunking_algorithm: chunking.ChunkingAlgorithm,
) !UploadResult {
    if (data.len == 0) return error.EmptyData;

    var chunks = try chunking.chunkBufferWithAlgorithm(allocator, data, chunking_algorithm);
    defer chunks.deinit(allocator);

    var xorb_builder = xorb.XorbBuilder.init(allocator);
    defer xorb_builder.deinit();

    for (chunks.items) |chunk_range| {
        _ = try xorb_builder.addChunk(data[chunk_range.start..chunk_range.end]);
    }

    const xorb_data = try xorb_builder.serialize(compression_type);
    defer allocator.free(xorb_data);
    const xorb_hash = try xorb_builder.computeHash();

    _ = try cas.uploadXorb(xorb_hash, xorb_data);

    const file_hash = hashing.computeFileHash(xorb_hash);

    const built_chunks = xorb_builder.chunks.items;
    const chunk_hashes = try allocator.alloc(hashing.Hash, built_chunks.len);
    defer allocator.free(chunk_hashes);
    const chunk_sizes = try allocator.alloc(u32, built_chunks.len);
    defer allocator.free(chunk_sizes);

    for (built_chunks, chunk_hashes, chunk_sizes) |chunk, *hash, *size| {
        hash.* = chunk.hash;
        size.* = @intCast(chunk.data.len);
    }

    const shard_data = try buildUploadShard(
        allocator,
        file_hash,
        xorb_hash,
        data.len,
        chunk_hashes,
        chunk_sizes,
        xorb_data,
    );
    defer allocator.free(shard_data);

    _ = try cas.uploadShard(shard_data);

    return .{
        .file_hash = file_hash,
        .file_hash_hex = hashing.hashToHex(file_hash),
        .total_size = data.len,
    };
}

fn buildUploadShard(
    allocator: Allocator,
    file_hash: hashing.Hash,
    xorb_hash: hashing.Hash,
    total_size: usize,
    chunk_hashes: []const hashing.Hash,
    chunk_sizes: []const u32,
    xorb_data: []const u8,
) ![]u8 {
    var builder = shard.ShardBuilder.init(allocator);
    defer builder.deinit();

    try builder.addFileInfoWithVerification(
        file_hash,
        &.{.{
            .xorb_hash = xorb_hash,
            .cas_flags = 0,
            .unpacked_segment_size = @intCast(total_size),
            .chunk_index_start = 0,
            .chunk_index_end = @intCast(chunk_hashes.len),
        }},
        chunk_hashes,
    );

    const cas_entries = try allocator.alloc(shard.CASChunkSequenceEntry, chunk_hashes.len);
    defer allocator.free(cas_entries);

    var raw_offset: u32 = 0;
    for (chunk_hashes, chunk_sizes, cas_entries) |chunk_hash, chunk_size, *entry| {
        entry.* = .{
            .chunk_hash = chunk_hash,
            .byte_range_start = raw_offset,
            .unpacked_segment_size = chunk_size,
            .reserved = @splat(0),
        };
        raw_offset += chunk_size;
    }

    try builder.addCASInfo(
        xorb_hash,
        cas_entries,
        @intCast(total_size),
        @intCast(xorb_data.len),
    );

    return builder.serialize();
}
