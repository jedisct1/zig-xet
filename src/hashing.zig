//! XET Protocol Hashing - BLAKE3 with domain separation keys

const std = @import("std");
const constants = @import("constants.zig");

pub const Hash = [constants.HashSize]u8;

pub fn computeDataHash(data: []const u8) Hash {
    return keyedHash(constants.DataKey, data);
}

pub fn computeInternalNodeHash(data: []const u8) Hash {
    return keyedHash(constants.InternalNodeKey, data);
}

pub fn computeFileHash(merkle_root: Hash) Hash {
    return computeFileHashWithSalt(merkle_root, constants.FileHashKey);
}

/// Compute file hash with a custom salt/key.
/// This is used when shards have keyed hash protection enabled.
/// When salt is all zeros, this is equivalent to computeFileHash().
pub fn computeFileHashWithSalt(merkle_root: Hash, salt: [32]u8) Hash {
    return keyedHash(salt, &merkle_root);
}

pub fn computeVerificationHash(data: []const u8) Hash {
    return keyedHash(constants.VerificationKey, data);
}

/// Compute a BLAKE3 keyed hash.
pub fn keyedHash(key: [32]u8, message: []const u8) Hash {
    var hash: Hash = undefined;
    std.crypto.hash.Blake3.hash(message, &hash, .{ .key = key });
    return hash;
}

/// Compute a keyed hash of a hash value.
/// This is used for chunk hash protection in keyed shards.
pub fn keyedHashOfHash(key: [32]u8, hash_input: Hash) Hash {
    return keyedHash(key, &hash_input);
}

/// Transform a chunk hash using a key.
/// If the key is all zeros, returns the original hash unchanged.
pub fn keyedChunkHash(chunk_hash: Hash, key: [32]u8) Hash {
    if (isZeroKey(key)) {
        return chunk_hash;
    }
    return keyedHashOfHash(key, chunk_hash);
}

/// Check if a key is all zeros (no keyed hash protection).
pub fn isZeroKey(key: [32]u8) bool {
    return std.mem.allEqual(u8, &key, 0);
}

pub fn hashToHex(hash: Hash) [64]u8 {
    var result: [64]u8 = undefined;
    for (0..4) |i| {
        const val = std.mem.readInt(u64, hash[i * 8 ..][0..8], .little);
        _ = std.fmt.bufPrint(result[i * 16 ..][0..16], "{x:0>16}", .{val}) catch unreachable;
    }
    return result;
}

/// Hashes are formatted as [u64; 4] with each u64 in little-endian format.
pub fn hexToHash(hex: []const u8) !Hash {
    if (hex.len != 64) return error.InvalidHexLength;
    var hash: Hash = undefined;
    for (0..4) |i| {
        const val = try std.fmt.parseInt(u64, hex[i * 16 ..][0..16], 16);
        std.mem.writeInt(u64, hash[i * 8 ..][0..8], val, .little);
    }
    return hash;
}

pub const MerkleNode = struct {
    hash: Hash,
    size: u64,
};

const MeanTreeBranchingFactor: u64 = 4;

fn nextMergeCut(nodes: []const MerkleNode) usize {
    if (nodes.len <= 2) {
        return nodes.len;
    }

    const end = @min(2 * MeanTreeBranchingFactor + 1, nodes.len);

    for (2..end) |i| {
        const hash_as_u64 = std.mem.readInt(u64, nodes[i].hash[24..32], .little);
        if (hash_as_u64 % MeanTreeBranchingFactor == 0) {
            return i + 1;
        }
    }

    return end;
}

fn mergedHashOfSequence(allocator: std.mem.Allocator, nodes: []const MerkleNode) !MerkleNode {
    var buffer: std.Io.Writer.Allocating = .init(allocator);
    defer buffer.deinit();

    var total_size: u64 = 0;
    for (nodes) |node| {
        buffer.writer.print("{s} : {d}\n", .{ hashToHex(node.hash), node.size }) catch return error.OutOfMemory;
        total_size += node.size;
    }

    return .{ .hash = computeInternalNodeHash(buffer.written()), .size = total_size };
}

pub fn buildMerkleTree(allocator: std.mem.Allocator, chunks: []const MerkleNode) !Hash {
    if (chunks.len == 0) {
        return @splat(0);
    }

    if (chunks.len == 1) {
        return chunks[0].hash;
    }

    var hv = try std.ArrayList(MerkleNode).initCapacity(allocator, chunks.len);
    defer hv.deinit(allocator);
    hv.appendSliceAssumeCapacity(chunks);

    while (hv.items.len > 1) {
        var write_idx: usize = 0;
        var read_idx: usize = 0;

        while (read_idx < hv.items.len) {
            const next_cut = read_idx + nextMergeCut(hv.items[read_idx..]);
            hv.items[write_idx] = try mergedHashOfSequence(allocator, hv.items[read_idx..next_cut]);
            write_idx += 1;
            read_idx = next_cut;
        }

        hv.shrinkRetainingCapacity(write_idx);
    }

    return hv.items[0].hash;
}

test "data hash is deterministic" {
    const data = "Hello, World!";
    const hash1 = computeDataHash(data);
    const hash2 = computeDataHash(data);
    try std.testing.expectEqualSlices(u8, &hash1, &hash2);
}

test "different data produces different hashes" {
    const data1 = "Hello, World!";
    const data2 = "Hello, Zig!";
    const hash1 = computeDataHash(data1);
    const hash2 = computeDataHash(data2);
    try std.testing.expect(!std.mem.eql(u8, &hash1, &hash2));
}

test "data hash and internal node hash are different for same input" {
    const data = "Hello, World!";
    const data_hash = computeDataHash(data);
    const internal_hash = computeInternalNodeHash(data);
    try std.testing.expect(!std.mem.eql(u8, &data_hash, &internal_hash));
}

test "hash to hex conversion" {
    const data = "test";
    const hash = computeDataHash(data);
    const hex = hashToHex(hash);

    try std.testing.expectEqual(@as(usize, 64), hex.len);
    for (hex) |c| {
        try std.testing.expect((c >= '0' and c <= '9') or (c >= 'a' and c <= 'f'));
    }
}

test "hex to hash round trip" {
    const data = "test";
    const original = computeDataHash(data);
    const hex = hashToHex(original);
    const parsed = try hexToHash(&hex);

    try std.testing.expectEqualSlices(u8, &original, &parsed);
}

test "invalid hex length" {
    const invalid_hex = "123abc"; // Too short
    try std.testing.expectError(error.InvalidHexLength, hexToHash(invalid_hex));
}

test "merkle tree with single chunk" {
    const allocator = std.testing.allocator;
    const chunks = [_]MerkleNode{
        .{ .hash = computeDataHash("test"), .size = 4 },
    };

    const root = try buildMerkleTree(allocator, &chunks);
    try std.testing.expectEqualSlices(u8, &chunks[0].hash, &root);
}

test "merkle tree with multiple chunks" {
    const allocator = std.testing.allocator;
    const chunks = [_]MerkleNode{
        .{ .hash = computeDataHash("chunk1"), .size = 6 },
        .{ .hash = computeDataHash("chunk2"), .size = 6 },
        .{ .hash = computeDataHash("chunk3"), .size = 6 },
    };

    const root1 = try buildMerkleTree(allocator, &chunks);
    const root2 = try buildMerkleTree(allocator, &chunks);

    try std.testing.expectEqualSlices(u8, &root1, &root2);
    try std.testing.expect(!std.mem.eql(u8, &root1, &chunks[0].hash));
    try std.testing.expect(!std.mem.eql(u8, &root1, &chunks[1].hash));
    try std.testing.expect(!std.mem.eql(u8, &root1, &chunks[2].hash));
}

test "file hash is different from merkle root" {
    const merkle_root = computeDataHash("test");
    const file_hash = computeFileHash(merkle_root);
    try std.testing.expect(!std.mem.eql(u8, &merkle_root, &file_hash));
}

test "keyedHash produces different output for different keys" {
    const message = "test message";
    const key1: [32]u8 = @splat(0);
    const key2: [32]u8 = @splat(1);

    const hash1 = keyedHash(key1, message);
    const hash2 = keyedHash(key2, message);
    try std.testing.expect(!std.mem.eql(u8, &hash1, &hash2));
}

test "merkle tree - empty input" {
    const allocator = std.testing.allocator;
    const chunks: []const MerkleNode = &.{};

    const root = try buildMerkleTree(allocator, chunks);
    const expected = "0000000000000000000000000000000000000000000000000000000000000000";

    try std.testing.expectEqualStrings(expected, &hashToHex(root));
}

test "merkle tree - single chunk (all zeros)" {
    const allocator = std.testing.allocator;
    const hash = try hexToHash("0000000000000000000000000000000000000000000000000000000000000000");
    const chunks = [_]MerkleNode{
        .{ .hash = hash, .size = 0 },
    };

    const root = try buildMerkleTree(allocator, &chunks);
    try std.testing.expectEqualSlices(u8, &hash, &root);
}

test "merkle tree - single chunk (non-zero)" {
    const allocator = std.testing.allocator;
    const hash = try hexToHash("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6");
    const chunks = [_]MerkleNode{
        .{ .hash = hash, .size = 100 },
    };

    const root = try buildMerkleTree(allocator, &chunks);
    const expected = "cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6";

    try std.testing.expectEqualStrings(expected, &hashToHex(root));
}

test "merkle tree - three chunks" {
    const allocator = std.testing.allocator;
    const chunks = [_]MerkleNode{
        .{ .hash = try hexToHash("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6"), .size = 100 },
        .{ .hash = try hexToHash("c3e67584b5c4fc2a89837ec39e40f2c8a6bb0b2987ac94cd4b31e5fbdd210a72"), .size = 200 },
        .{ .hash = try hexToHash("0d2beb91b9196929a5ddec9f6e306924ddf4a24268e3e59fd8464738d525af37"), .size = 300 },
    };

    const root = try buildMerkleTree(allocator, &chunks);
    const expected = "71ec1275fca074724e2dd666921b3277c7cee603e4d025bcab2d4050015be2bc";

    try std.testing.expectEqualStrings(expected, &hashToHex(root));
}

test "merkle tree - four identical chunks" {
    const allocator = std.testing.allocator;
    const hash = try hexToHash("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6");
    const chunks = [_]MerkleNode{
        .{ .hash = hash, .size = 100 },
        .{ .hash = hash, .size = 100 },
        .{ .hash = hash, .size = 100 },
        .{ .hash = hash, .size = 100 },
    };

    const root = try buildMerkleTree(allocator, &chunks);
    const expected = "89f2ada89ff8c96763c6b25010e6dd76a4c05b1466207633ea559acf2093211b";

    try std.testing.expectEqualStrings(expected, &hashToHex(root));
}

test "merkle tree - six chunks" {
    const allocator = std.testing.allocator;
    const chunks = [_]MerkleNode{
        .{ .hash = try hexToHash("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6"), .size = 100 },
        .{ .hash = try hexToHash("c3e67584b5c4fc2a89837ec39e40f2c8a6bb0b2987ac94cd4b31e5fbdd210a72"), .size = 200 },
        .{ .hash = try hexToHash("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6"), .size = 100 },
        .{ .hash = try hexToHash("c3e67584b5c4fc2a89837ec39e40f2c8a6bb0b2987ac94cd4b31e5fbdd210a72"), .size = 200 },
        .{ .hash = try hexToHash("0d2beb91b9196929a5ddec9f6e306924ddf4a24268e3e59fd8464738d525af37"), .size = 300 },
        .{ .hash = try hexToHash("adf8773496a9b7319b2e50dc98093f344053b17d8ad37100b9c07d9805988784"), .size = 400 },
    };

    const root = try buildMerkleTree(allocator, &chunks);
    const expected = "52c826f99507aa05d0b45e9837fa1709e0485425cfbcb1e80db3905cf98b3ee9";

    try std.testing.expectEqualStrings(expected, &hashToHex(root));
}

test "merkle tree - eight chunks (powers of 2)" {
    const allocator = std.testing.allocator;
    const chunks = [_]MerkleNode{
        .{ .hash = try hexToHash("0000000000000000000000000000000000000000000000000000000000000000"), .size = 0 },
        .{ .hash = try hexToHash("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6"), .size = 100 },
        .{ .hash = try hexToHash("c3e67584b5c4fc2a89837ec39e40f2c8a6bb0b2987ac94cd4b31e5fbdd210a72"), .size = 200 },
        .{ .hash = try hexToHash("0d2beb91b9196929a5ddec9f6e306924ddf4a24268e3e59fd8464738d525af37"), .size = 300 },
        .{ .hash = try hexToHash("adf8773496a9b7319b2e50dc98093f344053b17d8ad37100b9c07d9805988784"), .size = 400 },
        .{ .hash = try hexToHash("4ac202caf347fc1e9c874b1ef6a1c5e619141eb775a6f43f0f0124ccd0060d9e"), .size = 500 },
        .{ .hash = try hexToHash("b3b28636f65c149ea52eb1f94669466f70f033b54cea792824c696ba6ef3c389"), .size = 600 },
        .{ .hash = try hexToHash("0e2c1a002aae913d2c0fc8ddfa4e9e14b7b311b3b0d458726d5d9f6a6318013c"), .size = 700 },
    };

    const root = try buildMerkleTree(allocator, &chunks);
    const expected = "f62abe77e3fb9c954fe52b0028027ddc90c064c45951a4fd2211d87e5c0011db";

    try std.testing.expectEqualStrings(expected, &hashToHex(root));
}

test "merkle tree - 32 identical chunks" {
    const allocator = std.testing.allocator;
    const hash = try hexToHash("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6");

    var chunks: [32]MerkleNode = undefined;
    for (&chunks) |*chunk| {
        chunk.* = .{ .hash = hash, .size = 100 };
    }

    const root = try buildMerkleTree(allocator, &chunks);
    const expected = "0a0123c1617921883b7e13902095fcb86676e77c49120c33b233003b0af0e0a6";

    try std.testing.expectEqualStrings(expected, &hashToHex(root));
}

test "isZeroKey returns true for all-zero key" {
    const zero_key: [32]u8 = @splat(0);
    try std.testing.expect(isZeroKey(zero_key));
}

test "isZeroKey returns false for non-zero key" {
    var non_zero_key: [32]u8 = @splat(0);
    non_zero_key[0] = 1;
    try std.testing.expect(!isZeroKey(non_zero_key));
}

test "keyedChunkHash returns original hash for zero key" {
    const chunk_hash = computeDataHash("test chunk");
    const zero_key: [32]u8 = @splat(0);

    const result = keyedChunkHash(chunk_hash, zero_key);
    try std.testing.expectEqualSlices(u8, &chunk_hash, &result);
}

test "keyedChunkHash transforms hash for non-zero key" {
    const chunk_hash = computeDataHash("test chunk");
    var key: [32]u8 = @splat(0);
    key[0] = 42;

    const result = keyedChunkHash(chunk_hash, key);
    try std.testing.expect(!std.mem.eql(u8, &chunk_hash, &result));

    const result2 = keyedChunkHash(chunk_hash, key);
    try std.testing.expectEqualSlices(u8, &result, &result2);
}

test "computeFileHashWithSalt with zero salt equals computeFileHash" {
    const merkle_root = computeDataHash("test root");
    const zero_salt: [32]u8 = @splat(0);

    const file_hash = computeFileHash(merkle_root);
    const file_hash_with_salt = computeFileHashWithSalt(merkle_root, zero_salt);

    try std.testing.expectEqualSlices(u8, &file_hash, &file_hash_with_salt);
}

test "computeFileHashWithSalt produces different hash for non-zero salt" {
    const merkle_root = computeDataHash("test root");
    const zero_salt: [32]u8 = @splat(0);
    var non_zero_salt: [32]u8 = @splat(0);
    non_zero_salt[0] = 123;

    const file_hash_zero = computeFileHashWithSalt(merkle_root, zero_salt);
    const file_hash_nonzero = computeFileHashWithSalt(merkle_root, non_zero_salt);

    try std.testing.expect(!std.mem.eql(u8, &file_hash_zero, &file_hash_nonzero));
}

test "keyedHashOfHash is consistent with keyedHash on hash bytes" {
    var key: [32]u8 = undefined;
    for (&key, 0..) |*b, i| {
        b.* = @truncate(i);
    }

    const hash_input = computeDataHash("test");
    const result1 = keyedHashOfHash(key, hash_input);
    const result2 = keyedHash(key, &hash_input);

    try std.testing.expectEqualSlices(u8, &result1, &result2);
}
