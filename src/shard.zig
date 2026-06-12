//! MDB Shard Format: All entries 48 bytes, little-endian

const std = @import("std");
const constants = @import("constants.zig");
const hashing = @import("hashing.zig");

pub const ShardHeader = extern struct {
    magic_tag: [32]u8,
    version: u64,
    footer_size: u64,

    comptime {
        if (@sizeOf(ShardHeader) != constants.MdbHeaderSize) {
            @compileError("ShardHeader size mismatch");
        }
    }

    pub fn init() ShardHeader {
        return .{
            .magic_tag = constants.MdbShardHeaderTag,
            .version = constants.MdbHeaderVersion,
            .footer_size = constants.MdbFooterSize,
        };
    }
};

pub const ShardFooter = extern struct {
    version: u64,
    file_info_offset: u64,
    cas_info_offset: u64,
    reserved1: [48]u8,
    chunk_hash_key: [32]u8,
    creation_timestamp: u64,
    key_expiry: u64,
    reserved2: [72]u8,
    footer_offset: u64,

    comptime {
        if (@sizeOf(ShardFooter) != constants.MdbFooterSize) {
            @compileError("ShardFooter size mismatch");
        }
    }

    pub fn init() ShardFooter {
        return .{
            .version = constants.MdbFooterVersion,
            .file_info_offset = 0,
            .cas_info_offset = 0,
            .reserved1 = @splat(0),
            .chunk_hash_key = @splat(0),
            .creation_timestamp = 0,
            .key_expiry = 0,
            .reserved2 = @splat(0),
            .footer_offset = 0,
        };
    }

    /// Check if this shard has keyed hash protected chunk hashes.
    pub fn isKeyed(self: ShardFooter) bool {
        return !hashing.isZeroKey(self.chunk_hash_key);
    }

    /// Get the chunk hash key if the shard is protected, null otherwise.
    pub fn getChunkHashKey(self: ShardFooter) ?[32]u8 {
        if (self.isKeyed()) {
            return self.chunk_hash_key;
        }
        return null;
    }
};

pub const FileDataSequenceHeader = extern struct {
    /// File hash (32 bytes)
    file_hash: [32]u8,
    /// File flags (4 bytes)
    file_flags: u32,
    /// Number of entries
    entry_count: u32,
    /// Reserved (8 bytes)
    reserved: [8]u8,

    comptime {
        if (@sizeOf(FileDataSequenceHeader) != constants.MdbEntrySize) {
            @compileError("FileDataSequenceHeader size mismatch");
        }
    }
};

pub const FileDataSequenceEntry = extern struct {
    /// Xorb hash (32 bytes)
    xorb_hash: [32]u8,
    /// CAS flags (4 bytes)
    cas_flags: u32,
    /// Unpacked segment size (4 bytes)
    unpacked_segment_size: u32,
    /// Chunk index start (4 bytes)
    chunk_index_start: u32,
    /// Chunk index end (4 bytes, exclusive)
    chunk_index_end: u32,

    comptime {
        if (@sizeOf(FileDataSequenceEntry) != constants.MdbEntrySize) {
            @compileError("FileDataSequenceEntry size mismatch");
        }
    }
};

pub const FileVerificationEntry = extern struct {
    /// Range hash for verification (32 bytes)
    range_hash: [32]u8,
    /// Reserved (16 bytes)
    reserved: [16]u8,

    comptime {
        if (@sizeOf(FileVerificationEntry) != constants.MdbEntrySize) {
            @compileError("FileVerificationEntry size mismatch");
        }
    }
};

pub const FileMetadataExt = extern struct {
    /// SHA256 hash (32 bytes)
    sha256_hash: [32]u8,
    /// Reserved (16 bytes)
    reserved: [16]u8,

    comptime {
        if (@sizeOf(FileMetadataExt) != constants.MdbEntrySize) {
            @compileError("FileMetadataExt size mismatch");
        }
    }
};

pub const CASChunkSequenceHeader = extern struct {
    /// Xorb hash (32 bytes)
    xorb_hash: [32]u8,
    /// CAS flags (4 bytes)
    cas_flags: u32,
    /// Entry count (4 bytes)
    entry_count: u32,
    /// Total raw bytes in xorb (4 bytes)
    total_raw_bytes: u32,
    /// Serialized xorb size (4 bytes)
    serialized_xorb_size: u32,

    comptime {
        if (@sizeOf(CASChunkSequenceHeader) != constants.MdbEntrySize) {
            @compileError("CASChunkSequenceHeader size mismatch");
        }
    }
};

pub const CASChunkSequenceEntry = extern struct {
    /// Chunk hash (32 bytes)
    chunk_hash: [32]u8,
    /// Byte range start position (4 bytes)
    byte_range_start: u32,
    /// Unpacked segment size (4 bytes)
    unpacked_segment_size: u32,
    /// Reserved (8 bytes)
    reserved: [8]u8,

    comptime {
        if (@sizeOf(CASChunkSequenceEntry) != constants.MdbEntrySize) {
            @compileError("CASChunkSequenceEntry size mismatch");
        }
    }
};

pub const ChunkLocation = struct {
    hash: [32]u8,
    xorb_hash: [32]u8,
    byte_offset: u32,
    size: u32,
};

pub const ShardBuilder = struct {
    allocator: std.mem.Allocator,
    header: ShardHeader,
    footer: ShardFooter,
    file_info: std.ArrayList(u8),
    cas_info: std.ArrayList(u8),

    pub fn init(allocator: std.mem.Allocator) ShardBuilder {
        return .{
            .allocator = allocator,
            .header = ShardHeader.init(),
            .footer = ShardFooter.init(),
            .file_info = .empty,
            .cas_info = .empty,
        };
    }

    pub fn deinit(self: *ShardBuilder) void {
        self.file_info.deinit(self.allocator);
        self.cas_info.deinit(self.allocator);
    }

    pub fn addFileInfo(
        self: *ShardBuilder,
        file_hash: hashing.Hash,
        entries: []const FileDataSequenceEntry,
    ) !void {
        try self.addFileInfoEx(file_hash, entries, 0, null);
    }

    pub fn addFileInfoWithVerification(
        self: *ShardBuilder,
        file_hash: hashing.Hash,
        entries: []const FileDataSequenceEntry,
        chunk_hashes: []const hashing.Hash,
    ) !void {
        const range_hash = computeVerificationHash(chunk_hashes);
        try self.addFileInfoEx(file_hash, entries, 0x80000000, &range_hash);
    }

    fn addFileInfoEx(
        self: *ShardBuilder,
        file_hash: hashing.Hash,
        entries: []const FileDataSequenceEntry,
        file_flags: u32,
        verification_hash: ?*const hashing.Hash,
    ) !void {
        const header = FileDataSequenceHeader{
            .file_hash = file_hash,
            .file_flags = file_flags,
            .entry_count = @intCast(entries.len),
            .reserved = @splat(0),
        };
        try self.file_info.appendSlice(self.allocator, std.mem.asBytes(&header));

        for (entries) |entry| {
            try self.file_info.appendSlice(self.allocator, std.mem.asBytes(&entry));
        }

        if (verification_hash) |vh| {
            const verification = FileVerificationEntry{
                .range_hash = vh.*,
                .reserved = @splat(0),
            };
            try self.file_info.appendSlice(self.allocator, std.mem.asBytes(&verification));
        }
    }

    pub fn addCASInfo(
        self: *ShardBuilder,
        xorb_hash: hashing.Hash,
        entries: []const CASChunkSequenceEntry,
        total_raw_bytes: u32,
        serialized_size: u32,
    ) !void {
        const header = CASChunkSequenceHeader{
            .xorb_hash = xorb_hash,
            .cas_flags = 0,
            .entry_count = @intCast(entries.len),
            .total_raw_bytes = total_raw_bytes,
            .serialized_xorb_size = serialized_size,
        };
        try self.cas_info.appendSlice(self.allocator, std.mem.asBytes(&header));

        for (entries) |entry| {
            try self.cas_info.appendSlice(self.allocator, std.mem.asBytes(&entry));
        }
    }

    /// Set the key for chunk hash protection.
    /// When set, chunk hashes in this shard are transformed using a keyed hash.
    pub fn setChunkHashKey(self: *ShardBuilder, key: [32]u8) void {
        self.footer.chunk_hash_key = key;
    }

    /// Set the creation timestamp (seconds since epoch).
    pub fn setCreationTimestamp(self: *ShardBuilder, timestamp: u64) void {
        self.footer.creation_timestamp = timestamp;
    }

    /// Set the key expiry timestamp (seconds since epoch).
    pub fn setKeyExpiry(self: *ShardBuilder, expiry: u64) void {
        self.footer.key_expiry = expiry;
    }

    pub fn serialize(self: *ShardBuilder) ![]u8 {
        var buffer: std.ArrayList(u8) = .empty;
        errdefer buffer.deinit(self.allocator);

        try buffer.appendSlice(self.allocator, std.mem.asBytes(&self.header));

        self.footer.file_info_offset = buffer.items.len;
        try buffer.appendSlice(self.allocator, self.file_info.items);
        try buffer.appendSlice(self.allocator, &constants.MdbBookendMarker);

        self.footer.cas_info_offset = buffer.items.len;
        try buffer.appendSlice(self.allocator, self.cas_info.items);
        try buffer.appendSlice(self.allocator, &constants.MdbBookendMarker);

        self.footer.footer_offset = buffer.items.len;
        try buffer.appendSlice(self.allocator, std.mem.asBytes(&self.footer));

        return buffer.toOwnedSlice(self.allocator);
    }
};

pub const ShardReader = struct {
    allocator: std.mem.Allocator,
    data: []const u8,
    header: ShardHeader,
    footer: ShardFooter,

    pub fn init(allocator: std.mem.Allocator, data: []const u8) !ShardReader {
        if (data.len < constants.MdbHeaderSize) return error.TruncatedShard;

        const header = std.mem.bytesToValue(ShardHeader, data[0..constants.MdbHeaderSize]);
        if (header.version != constants.MdbHeaderVersion) return error.UnsupportedVersion;

        if (data.len < constants.MdbFooterSize) return error.TruncatedShard;

        const footer_offset = data.len - constants.MdbFooterSize;
        const footer = std.mem.bytesToValue(ShardFooter, data[footer_offset..][0..constants.MdbFooterSize]);
        if (footer.version != constants.MdbFooterVersion) return error.UnsupportedFooterVersion;

        return .{
            .allocator = allocator,
            .data = data,
            .header = header,
            .footer = footer,
        };
    }

    pub fn getFileInfoSection(self: *ShardReader) []const u8 {
        const start = self.footer.file_info_offset;
        const end = self.footer.cas_info_offset;
        return self.data[start..end];
    }

    pub fn getCASInfoSection(self: *ShardReader) []const u8 {
        const start = self.footer.cas_info_offset;
        const end = self.footer.footer_offset;
        return self.data[start..end];
    }

    pub fn parseCASInfo(self: *ShardReader) !std.ArrayList(ChunkLocation) {
        var locations: std.ArrayList(ChunkLocation) = .empty;
        errdefer locations.deinit(self.allocator);

        const cas_section = self.getCASInfoSection();
        if (cas_section.len == 0) return locations;

        var offset: usize = 0;

        while (offset + constants.MdbEntrySize <= cas_section.len) {
            if (offset + constants.MdbBookendMarker.len <= cas_section.len and
                std.mem.eql(u8, cas_section[offset..][0..constants.MdbBookendMarker.len], &constants.MdbBookendMarker))
            {
                break;
            }

            const header = std.mem.bytesToValue(CASChunkSequenceHeader, cas_section[offset..][0..constants.MdbEntrySize]);
            offset += constants.MdbEntrySize;

            var i: u32 = 0;
            while (i < header.entry_count and offset + constants.MdbEntrySize <= cas_section.len) : (i += 1) {
                const entry = std.mem.bytesToValue(CASChunkSequenceEntry, cas_section[offset..][0..constants.MdbEntrySize]);
                offset += constants.MdbEntrySize;

                try locations.append(self.allocator, .{
                    .hash = entry.chunk_hash,
                    .xorb_hash = header.xorb_hash,
                    .byte_offset = entry.byte_range_start,
                    .size = entry.unpacked_segment_size,
                });
            }
        }

        return locations;
    }

    /// Check if this shard has keyed hash protected chunk hashes.
    pub fn isKeyed(self: *const ShardReader) bool {
        return self.footer.isKeyed();
    }

    /// Get the chunk hash key if the shard is protected, null otherwise.
    pub fn getChunkHashKey(self: *const ShardReader) ?[32]u8 {
        return self.footer.getChunkHashKey();
    }

    /// Transform a chunk hash for lookup in this shard.
    /// If the shard is keyed, applies keyed hash transformation.
    /// If not keyed, returns the original hash.
    pub fn keyedChunkHash(self: *const ShardReader, chunk_hash: hashing.Hash) hashing.Hash {
        return hashing.keyedChunkHash(chunk_hash, self.footer.chunk_hash_key);
    }

    /// Find a chunk location by its hash (handles keyed shards automatically).
    /// For keyed shards, the input hash should be the original (unkeyed) hash.
    pub fn findChunkLocation(self: *ShardReader, chunk_hash: hashing.Hash) !?ChunkLocation {
        const keyed_hash = self.keyedChunkHash(chunk_hash);
        var locations = try self.parseCASInfo();
        defer locations.deinit(self.allocator);

        for (locations.items) |loc| {
            if (std.mem.eql(u8, &loc.hash, &keyed_hash)) {
                return loc;
            }
        }
        return null;
    }

    /// Get the key expiry timestamp (seconds since epoch).
    /// Returns null if no expiry is set (value is 0).
    pub fn getKeyExpiry(self: *const ShardReader) ?u64 {
        if (self.footer.key_expiry == 0) {
            return null;
        }
        return self.footer.key_expiry;
    }

    /// Get the creation timestamp (seconds since epoch).
    /// Returns null if no timestamp is set (value is 0).
    pub fn getCreationTimestamp(self: *const ShardReader) ?u64 {
        if (self.footer.creation_timestamp == 0) {
            return null;
        }
        return self.footer.creation_timestamp;
    }
};

pub fn computeVerificationHash(chunk_hashes: []const hashing.Hash) hashing.Hash {
    return hashing.computeVerificationHash(std.mem.sliceAsBytes(chunk_hashes));
}

test "shard header size" {
    try std.testing.expectEqual(48, @sizeOf(ShardHeader));
}

test "shard footer size" {
    try std.testing.expectEqual(200, @sizeOf(ShardFooter));
}

test "all entry structures are 48 bytes" {
    try std.testing.expectEqual(48, @sizeOf(FileDataSequenceHeader));
    try std.testing.expectEqual(48, @sizeOf(FileDataSequenceEntry));
    try std.testing.expectEqual(48, @sizeOf(FileVerificationEntry));
    try std.testing.expectEqual(48, @sizeOf(FileMetadataExt));
    try std.testing.expectEqual(48, @sizeOf(CASChunkSequenceHeader));
    try std.testing.expectEqual(48, @sizeOf(CASChunkSequenceEntry));
}

test "shard builder initialization" {
    const allocator = std.testing.allocator;
    var builder = ShardBuilder.init(allocator);
    defer builder.deinit();

    try std.testing.expectEqual(constants.MdbHeaderVersion, builder.header.version);
    try std.testing.expectEqual(constants.MdbFooterVersion, builder.footer.version);
}

test "shard serialization and deserialization" {
    const allocator = std.testing.allocator;
    var builder = ShardBuilder.init(allocator);
    defer builder.deinit();

    const file_hash = hashing.computeDataHash("test file");
    const entries = [_]FileDataSequenceEntry{
        .{
            .xorb_hash = hashing.computeDataHash("xorb1"),
            .cas_flags = 0,
            .unpacked_segment_size = 1000,
            .chunk_index_start = 0,
            .chunk_index_end = 5,
        },
    };

    try builder.addFileInfo(file_hash, &entries);

    const cas_entries = [_]CASChunkSequenceEntry{
        .{
            .chunk_hash = hashing.computeDataHash("chunk1"),
            .byte_range_start = 0,
            .unpacked_segment_size = 200,
            .reserved = @splat(0),
        },
    };

    try builder.addCASInfo(hashing.computeDataHash("xorb1"), &cas_entries, 1000, 500);

    const serialized = try builder.serialize();
    defer allocator.free(serialized);

    var reader = try ShardReader.init(allocator, serialized);

    try std.testing.expectEqual(constants.MdbHeaderVersion, reader.header.version);
    try std.testing.expectEqual(constants.MdbFooterVersion, reader.footer.version);

    const file_info_section = reader.getFileInfoSection();
    try std.testing.expect(file_info_section.len > 0);

    const cas_info_section = reader.getCASInfoSection();
    try std.testing.expect(cas_info_section.len > 0);
}

test "shard parseCASInfo extracts chunk locations" {
    const allocator = std.testing.allocator;
    var builder = ShardBuilder.init(allocator);
    defer builder.deinit();

    const xorb_hash = hashing.computeDataHash("test_xorb");
    const chunk1_hash = hashing.computeDataHash("chunk1");
    const chunk2_hash = hashing.computeDataHash("chunk2");
    const chunk3_hash = hashing.computeDataHash("chunk3");

    const cas_entries = [_]CASChunkSequenceEntry{
        .{
            .chunk_hash = chunk1_hash,
            .byte_range_start = 0,
            .unpacked_segment_size = 8192,
            .reserved = @splat(0),
        },
        .{
            .chunk_hash = chunk2_hash,
            .byte_range_start = 8192,
            .unpacked_segment_size = 16384,
            .reserved = @splat(0),
        },
        .{
            .chunk_hash = chunk3_hash,
            .byte_range_start = 24576,
            .unpacked_segment_size = 4096,
            .reserved = @splat(0),
        },
    };

    try builder.addCASInfo(xorb_hash, &cas_entries, 28672, 15000);

    const serialized = try builder.serialize();
    defer allocator.free(serialized);

    var reader = try ShardReader.init(allocator, serialized);
    var locations = try reader.parseCASInfo();
    defer locations.deinit(allocator);

    try std.testing.expectEqual(@as(usize, 3), locations.items.len);
    try std.testing.expectEqualSlices(u8, &chunk1_hash, &locations.items[0].hash);
    try std.testing.expectEqualSlices(u8, &xorb_hash, &locations.items[0].xorb_hash);
    try std.testing.expectEqual(@as(u32, 0), locations.items[0].byte_offset);
    try std.testing.expectEqual(@as(u32, 8192), locations.items[0].size);
    try std.testing.expectEqualSlices(u8, &chunk2_hash, &locations.items[1].hash);
    try std.testing.expectEqualSlices(u8, &xorb_hash, &locations.items[1].xorb_hash);
    try std.testing.expectEqual(@as(u32, 8192), locations.items[1].byte_offset);
    try std.testing.expectEqual(@as(u32, 16384), locations.items[1].size);
    try std.testing.expectEqualSlices(u8, &chunk3_hash, &locations.items[2].hash);
    try std.testing.expectEqualSlices(u8, &xorb_hash, &locations.items[2].xorb_hash);
    try std.testing.expectEqual(@as(u32, 24576), locations.items[2].byte_offset);
    try std.testing.expectEqual(@as(u32, 4096), locations.items[2].size);
}

test "shard footer isKeyed returns false for zero key" {
    var footer = ShardFooter.init();
    try std.testing.expect(!footer.isKeyed());
    try std.testing.expectEqual(@as(?[32]u8, null), footer.getChunkHashKey());
}

test "shard footer isKeyed returns true for non-zero key" {
    var footer = ShardFooter.init();
    footer.chunk_hash_key[0] = 42;

    try std.testing.expect(footer.isKeyed());

    const key = footer.getChunkHashKey();
    try std.testing.expect(key != null);
    try std.testing.expectEqual(@as(u8, 42), key.?[0]);
}

test "shard builder setChunkHashKey sets the key" {
    const allocator = std.testing.allocator;
    var builder = ShardBuilder.init(allocator);
    defer builder.deinit();

    var chunk_hash_key: [32]u8 = @splat(0);
    chunk_hash_key[0] = 123;
    chunk_hash_key[31] = 45;

    builder.setChunkHashKey(chunk_hash_key);

    try std.testing.expectEqual(@as(u8, 123), builder.footer.chunk_hash_key[0]);
    try std.testing.expectEqual(@as(u8, 45), builder.footer.chunk_hash_key[31]);
}

test "keyed shard stores and retrieves chunk hash key" {
    const allocator = std.testing.allocator;
    var builder = ShardBuilder.init(allocator);
    defer builder.deinit();

    var chunk_hash_key: [32]u8 = undefined;
    for (&chunk_hash_key, 0..) |*b, i| {
        b.* = @truncate(i * 7);
    }
    builder.setChunkHashKey(chunk_hash_key);
    builder.setCreationTimestamp(1700000000);
    builder.setKeyExpiry(1700086400);

    const serialized = try builder.serialize();
    defer allocator.free(serialized);

    var reader = try ShardReader.init(allocator, serialized);

    try std.testing.expect(reader.isKeyed());
    const retrieved_key = reader.getChunkHashKey();
    try std.testing.expect(retrieved_key != null);
    try std.testing.expectEqualSlices(u8, &chunk_hash_key, &retrieved_key.?);

    try std.testing.expectEqual(@as(?u64, 1700000000), reader.getCreationTimestamp());
    try std.testing.expectEqual(@as(?u64, 1700086400), reader.getKeyExpiry());
}

test "keyed shard keyedChunkHash transforms hash" {
    const allocator = std.testing.allocator;
    var builder = ShardBuilder.init(allocator);
    defer builder.deinit();

    var chunk_hash_key: [32]u8 = @splat(0);
    chunk_hash_key[0] = 99;
    builder.setChunkHashKey(chunk_hash_key);

    const serialized = try builder.serialize();
    defer allocator.free(serialized);

    var reader = try ShardReader.init(allocator, serialized);

    const original_hash = hashing.computeDataHash("test chunk");
    const keyed_hash = reader.keyedChunkHash(original_hash);

    try std.testing.expect(!std.mem.eql(u8, &original_hash, &keyed_hash));
    const expected_keyed = hashing.keyedChunkHash(original_hash, chunk_hash_key);
    try std.testing.expectEqualSlices(u8, &expected_keyed, &keyed_hash);
}

test "unkeyed shard keyedChunkHash returns original" {
    const allocator = std.testing.allocator;
    var builder = ShardBuilder.init(allocator);
    defer builder.deinit();

    const serialized = try builder.serialize();
    defer allocator.free(serialized);

    var reader = try ShardReader.init(allocator, serialized);

    const original_hash = hashing.computeDataHash("test chunk");
    const keyed_hash = reader.keyedChunkHash(original_hash);
    try std.testing.expectEqualSlices(u8, &original_hash, &keyed_hash);
}

test "keyed shard findChunkLocation with keyed hash transformation" {
    const allocator = std.testing.allocator;
    var builder = ShardBuilder.init(allocator);
    defer builder.deinit();

    var chunk_hash_key: [32]u8 = @splat(0);
    chunk_hash_key[0] = 77;
    builder.setChunkHashKey(chunk_hash_key);

    const original_chunk_hash = hashing.computeDataHash("chunk data");
    const keyed_chunk_hash = hashing.keyedChunkHash(original_chunk_hash, chunk_hash_key);
    const xorb_hash = hashing.computeDataHash("xorb");

    const cas_entries = [_]CASChunkSequenceEntry{
        .{
            .chunk_hash = keyed_chunk_hash,
            .byte_range_start = 100,
            .unpacked_segment_size = 5000,
            .reserved = @splat(0),
        },
    };

    try builder.addCASInfo(xorb_hash, &cas_entries, 5000, 3000);

    const serialized = try builder.serialize();
    defer allocator.free(serialized);

    var reader = try ShardReader.init(allocator, serialized);

    // Look up using original (unkeyed) hash
    const location = try reader.findChunkLocation(original_chunk_hash);
    try std.testing.expect(location != null);
    try std.testing.expectEqual(@as(u32, 100), location.?.byte_offset);
    try std.testing.expectEqual(@as(u32, 5000), location.?.size);
    try std.testing.expectEqualSlices(u8, &xorb_hash, &location.?.xorb_hash);
}
