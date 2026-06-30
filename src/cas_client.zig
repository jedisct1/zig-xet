//! HTTP client for the XET Content-Addressable Storage API:
//! authentication with Hugging Face Hub tokens, file reconstruction queries,
//! chunk deduplication queries, xorb and shard uploads, and classification
//! of errors as retryable or non-retryable.

const std = @import("std");
const Allocator = std.mem.Allocator;
const hashing = @import("hashing.zig");

/// Read a full HTTP response body, transparently decompressing gzip/deflate.
///
/// Zig's HTTP client advertises `accept-encoding: gzip` by default, so the Hub
/// may return a compressed body that `response.reader()` would hand back as-is;
/// reading text or JSON has to go through this path. `max_bytes` caps the result.
pub fn readBodyDecompressing(
    response: *std.http.Client.Response,
    allocator: Allocator,
    max_bytes: usize,
) ![]u8 {
    var transfer_buffer: [16 * 1024]u8 = undefined;
    var decompress_buffer: [std.compress.flate.max_window_len]u8 = undefined;
    var decompress: std.http.Decompress = undefined;
    var reader = response.readerDecompressing(&transfer_buffer, &decompress, &decompress_buffer);
    return reader.allocRemaining(allocator, std.Io.Limit.limited(max_bytes));
}

/// Authentication token structure
pub const XetToken = struct {
    access_token: []const u8,
    exp: i64,
    cas_url: []const u8,
    allocator: Allocator,

    pub fn deinit(self: *XetToken) void {
        self.allocator.free(self.access_token);
        self.allocator.free(self.cas_url);
    }
};

/// HTTP error classification
pub const ErrorClass = enum {
    retryable,
    non_retryable,
};

/// CAS API error types
pub const CasError = error{
    BadRequest, // 400
    Unauthorized, // 401
    Forbidden, // 403
    NotFound, // 404
    RangeNotSatisfiable, // 416
    TooManyRequests, // 429
    InternalServerError, // 500
    ServiceUnavailable, // 503
    GatewayTimeout, // 504
    NetworkError,
    InvalidResponse,
    OutOfMemory,
};

/// Classification of HTTP errors
pub fn classifyError(err: CasError) ErrorClass {
    return switch (err) {
        error.TooManyRequests,
        error.InternalServerError,
        error.ServiceUnavailable,
        error.GatewayTimeout,
        error.NetworkError,
        => .retryable,

        else => .non_retryable,
    };
}

/// Convert HTTP status code to CasError
fn statusToError(status: std.http.Status) CasError {
    return switch (status) {
        .bad_request => error.BadRequest,
        .unauthorized => error.Unauthorized,
        .forbidden => error.Forbidden,
        .not_found => error.NotFound,
        .range_not_satisfiable => error.RangeNotSatisfiable,
        .too_many_requests => error.TooManyRequests,
        .internal_server_error => error.InternalServerError,
        .service_unavailable => error.ServiceUnavailable,
        .gateway_timeout => error.GatewayTimeout,
        else => error.InvalidResponse,
    };
}

/// Hash conversion: Convert 32-byte hash to 64-char hex string using little-endian 8-byte segments
/// This is a critical requirement from the XET protocol specification
pub fn hashToApiHex(hash: [32]u8, allocator: Allocator) ![]u8 {
    const hex = hashing.hashToHex(hash);
    return allocator.dupe(u8, &hex);
}

/// Convert 64-char API hex string back to 32-byte hash
pub fn apiHexToHash(hex: []const u8) ![32]u8 {
    return hashing.hexToHash(hex) catch error.InvalidResponse;
}

/// Chunk range within a xorb (start and end indices)
pub const ChunkRange = struct {
    start: u32,
    end: u32,
};

/// Reconstruction term - describes a xorb and chunk range needed to reconstruct part of a file
pub const ReconstructionTerm = struct {
    /// Xorb hash (32-byte BLAKE3 hash)
    hash: [32]u8,
    /// Total unpacked length of data from this term (in bytes)
    unpacked_length: u32,
    /// Chunk range within the xorb (start and end chunk indices)
    range: ChunkRange,
};

/// Fetch information for downloading xorb ranges
pub const FetchInfo = struct {
    /// Chunk range within the xorb
    range: ChunkRange,
    /// URL to fetch the xorb range from
    url: []const u8,
    /// HTTP byte range (inclusive end)
    url_range: struct { start: u64, end: u64 },

    allocator: Allocator,

    pub fn deinit(self: *FetchInfo) void {
        self.allocator.free(self.url);
    }
};

/// Reconstruction response structure (matches CAS API format)
pub const ReconstructionResponse = struct {
    /// Offset into the first range (for range queries)
    offset_into_first_range: u64,
    /// List of reconstruction terms
    terms: []ReconstructionTerm,
    /// Fetch information for downloading xorbs (map from xorb hash to fetch info)
    fetch_info: std.StringHashMap([]FetchInfo),

    allocator: Allocator,

    pub fn deinit(self: *ReconstructionResponse) void {
        self.allocator.free(self.terms);
        freeFetchInfoMap(self.allocator, &self.fetch_info);
    }
};

fn freeFetchInfoMap(allocator: Allocator, map: *std.StringHashMap([]FetchInfo)) void {
    var iter = map.iterator();
    while (iter.next()) |entry| {
        allocator.free(entry.key_ptr.*);
        for (entry.value_ptr.*) |*info| info.deinit();
        allocator.free(entry.value_ptr.*);
    }
    map.deinit();
}

fn parseReconstruction(allocator: Allocator, body: []const u8) !ReconstructionResponse {
    const parsed = try std.json.parseFromSlice(std.json.Value, allocator, body, .{});
    defer parsed.deinit();

    const root = parsed.value.object;

    const offset_into_first_range: u64 = if (root.get("offset_into_first_range")) |offset_val|
        @intCast(offset_val.integer)
    else
        0;

    const terms_array = root.get("terms").?.array;
    const terms = try allocator.alloc(ReconstructionTerm, terms_array.items.len);
    errdefer allocator.free(terms);

    for (terms_array.items, terms) |term_val, *term| {
        const term_obj = term_val.object;
        const range_obj = term_obj.get("range").?.object;
        term.* = .{
            .hash = try apiHexToHash(term_obj.get("hash").?.string),
            .unpacked_length = @intCast(term_obj.get("unpacked_length").?.integer),
            .range = .{
                .start = @intCast(range_obj.get("start").?.integer),
                .end = @intCast(range_obj.get("end").?.integer),
            },
        };
    }

    var fetch_info_map = std.StringHashMap([]FetchInfo).init(allocator);
    errdefer freeFetchInfoMap(allocator, &fetch_info_map);

    if (root.get("fetch_info")) |fetch_info_val| {
        var fetch_iter = fetch_info_val.object.iterator();

        while (fetch_iter.next()) |entry| {
            const xorb_hash_key = try allocator.dupe(u8, entry.key_ptr.*);
            errdefer allocator.free(xorb_hash_key);

            const fetch_array = entry.value_ptr.*.array;
            const fetch_infos = try allocator.alloc(FetchInfo, fetch_array.items.len);
            errdefer allocator.free(fetch_infos);

            for (fetch_array.items, fetch_infos) |fetch_val, *info| {
                const fetch_obj = fetch_val.object;
                const fetch_range_obj = fetch_obj.get("range").?.object;
                const url_range_obj = fetch_obj.get("url_range").?.object;

                info.* = .{
                    .range = .{
                        .start = @intCast(fetch_range_obj.get("start").?.integer),
                        .end = @intCast(fetch_range_obj.get("end").?.integer),
                    },
                    .url = try allocator.dupe(u8, fetch_obj.get("url").?.string),
                    .url_range = .{
                        .start = @intCast(url_range_obj.get("start").?.integer),
                        .end = @intCast(url_range_obj.get("end").?.integer),
                    },
                    .allocator = allocator,
                };
            }

            try fetch_info_map.put(xorb_hash_key, fetch_infos);
        }
    }

    return .{
        .offset_into_first_range = offset_into_first_range,
        .terms = terms,
        .fetch_info = fetch_info_map,
        .allocator = allocator,
    };
}

/// Xorb upload response
pub const XorbUploadResponse = struct {
    was_inserted: bool,
};

/// Shard upload response
pub const ShardUploadResponse = struct {
    result: u8, // 0 = exists, 1 = registered
};

/// CAS Client
pub const CasClient = struct {
    allocator: Allocator,
    cas_url: []const u8,
    access_token: []const u8,
    http_client: std.http.Client,

    pub fn init(allocator: Allocator, io: std.Io, cas_url: []const u8, access_token: []const u8) !CasClient {
        return CasClient{
            .allocator = allocator,
            .cas_url = try allocator.dupe(u8, cas_url),
            .access_token = try allocator.dupe(u8, access_token),
            .http_client = std.http.Client{ .allocator = allocator, .io = io },
        };
    }

    pub fn deinit(self: *CasClient) void {
        self.allocator.free(self.cas_url);
        self.allocator.free(self.access_token);
        self.http_client.deinit();
    }

    fn makeAuthHeader(self: *CasClient) ![]u8 {
        return try std.fmt.allocPrint(self.allocator, "Bearer {s}", .{self.access_token});
    }

    /// Get file reconstruction information
    /// file_hash: 32-byte hash of the file
    /// range: Optional byte range (start, end) - both inclusive
    pub fn getReconstruction(
        self: *CasClient,
        file_hash: [32]u8,
        range: ?struct { start: u64, end: u64 },
    ) !ReconstructionResponse {
        const hash_hex = try hashToApiHex(file_hash, self.allocator);
        defer self.allocator.free(hash_hex);

        const url = try std.fmt.allocPrint(
            self.allocator,
            "{s}/reconstructions/{s}",
            .{ self.cas_url, hash_hex },
        );
        defer self.allocator.free(url);

        const auth_header = try self.makeAuthHeader();
        defer self.allocator.free(auth_header);

        var range_header_buf: [64]u8 = undefined;
        var extra_headers_storage: [2]std.http.Header = undefined;
        var extra_headers_count: usize = 1;
        extra_headers_storage[0] = .{ .name = "Authorization", .value = auth_header };

        if (range) |r| {
            const range_header = try std.fmt.bufPrint(
                &range_header_buf,
                "bytes={d}-{d}",
                .{ r.start, r.end },
            );
            extra_headers_storage[1] = .{ .name = "Range", .value = range_header };
            extra_headers_count = 2;
        }

        const uri = try std.Uri.parse(url);
        var req = try self.http_client.request(.GET, uri, .{
            .extra_headers = extra_headers_storage[0..extra_headers_count],
        });
        defer req.deinit();

        try req.sendBodiless();
        var response = try req.receiveHead(&.{});

        if (response.head.status != .ok) {
            return statusToError(response.head.status);
        }

        const body = try readBodyDecompressing(&response, self.allocator, 10 * 1024 * 1024);
        defer self.allocator.free(body);

        return parseReconstruction(self.allocator, body);
    }

    /// Query chunk deduplication information
    /// Returns shard data in binary format
    pub fn queryChunkDedupe(
        self: *CasClient,
        chunk_hash: [32]u8,
    ) ![]u8 {
        const hash_hex = try hashToApiHex(chunk_hash, self.allocator);
        defer self.allocator.free(hash_hex);

        const url = try std.fmt.allocPrint(
            self.allocator,
            "{s}/chunks/default-merkledb/{s}",
            .{ self.cas_url, hash_hex },
        );
        defer self.allocator.free(url);

        const auth_header = try self.makeAuthHeader();
        defer self.allocator.free(auth_header);

        const extra_headers = [_]std.http.Header{
            .{ .name = "Authorization", .value = auth_header },
        };

        const uri = try std.Uri.parse(url);
        var req = try self.http_client.request(.GET, uri, .{
            .extra_headers = &extra_headers,
        });
        defer req.deinit();

        try req.sendBodiless();
        var response = try req.receiveHead(&.{});

        if (response.head.status != .ok) {
            return statusToError(response.head.status);
        }

        var reader = response.reader(&.{});
        // Use 80 MB limit to allow for protocol overhead while still protecting against excessive memory usage
        // The protocol specifies 64 MiB max for content, but we need headroom for HTTP headers/overhead
        const shard_data = try reader.allocRemaining(self.allocator, @enumFromInt(80 * 1024 * 1024));
        return shard_data;
    }

    /// Upload Xorb to CAS
    pub fn uploadXorb(
        self: *CasClient,
        xorb_hash: [32]u8,
        xorb_data: []const u8,
    ) !XorbUploadResponse {
        const hash_hex = try hashToApiHex(xorb_hash, self.allocator);
        defer self.allocator.free(hash_hex);

        const url = try std.fmt.allocPrint(
            self.allocator,
            "{s}/xorbs/default/{s}",
            .{ self.cas_url, hash_hex },
        );
        defer self.allocator.free(url);

        const auth_header = try self.makeAuthHeader();
        defer self.allocator.free(auth_header);

        const extra_headers = [_]std.http.Header{
            .{ .name = "Authorization", .value = auth_header },
        };

        const uri = try std.Uri.parse(url);
        var req = try self.http_client.request(.POST, uri, .{
            .extra_headers = &extra_headers,
            .headers = .{
                .content_type = .{ .override = "application/octet-stream" },
            },
        });
        defer req.deinit();

        req.transfer_encoding = .{ .content_length = xorb_data.len };
        var req_body = try req.sendBodyUnflushed(&.{});
        try req_body.writer.writeAll(xorb_data);
        try req_body.end();
        try req.connection.?.flush();
        var response = try req.receiveHead(&.{});

        if (response.head.status != .ok) {
            return statusToError(response.head.status);
        }

        var reader = response.reader(&.{});
        const body = try reader.allocRemaining(self.allocator, @enumFromInt(1024));
        defer self.allocator.free(body);

        const parsed = try std.json.parseFromSlice(
            std.json.Value,
            self.allocator,
            body,
            .{},
        );
        defer parsed.deinit();

        const was_inserted = parsed.value.object.get("was_inserted").?.bool;

        return XorbUploadResponse{ .was_inserted = was_inserted };
    }

    /// Fetch Xorb data from CAS
    /// Returns the xorb binary data
    pub fn fetchXorb(
        self: *CasClient,
        xorb_hash: [32]u8,
    ) ![]u8 {
        const hash_hex = try hashToApiHex(xorb_hash, self.allocator);
        defer self.allocator.free(hash_hex);

        const url = try std.fmt.allocPrint(
            self.allocator,
            "{s}/xorbs/default/{s}",
            .{ self.cas_url, hash_hex },
        );
        defer self.allocator.free(url);

        const auth_header = try self.makeAuthHeader();
        defer self.allocator.free(auth_header);

        const extra_headers = [_]std.http.Header{
            .{ .name = "Authorization", .value = auth_header },
        };

        const uri = try std.Uri.parse(url);
        var req = try self.http_client.request(.GET, uri, .{
            .extra_headers = &extra_headers,
        });
        defer req.deinit();

        try req.sendBodiless();
        var response = try req.receiveHead(&.{});

        if (response.head.status != .ok) {
            return statusToError(response.head.status);
        }

        var reader = response.reader(&.{});
        // Use 80 MB limit to allow for protocol overhead while still protecting against excessive memory usage
        // The protocol specifies 64 MiB max for Xorb content, but we need headroom for HTTP headers/overhead
        const xorb_data = try reader.allocRemaining(self.allocator, @enumFromInt(80 * 1024 * 1024));
        return xorb_data;
    }

    /// Fetch Xorb data from a URL (with optional byte range)
    /// Returns the xorb binary data
    pub fn fetchXorbFromUrl(
        self: *CasClient,
        url: []const u8,
        byte_range: ?struct { start: u64, end: u64 },
    ) ![]u8 {
        var range_header_buf: [64]u8 = undefined;
        var extra_headers_storage: [1]std.http.Header = undefined;
        var extra_headers_count: usize = 0;

        if (byte_range) |r| {
            const range_header = try std.fmt.bufPrint(
                &range_header_buf,
                "bytes={d}-{d}",
                .{ r.start, r.end },
            );
            extra_headers_storage[0] = .{ .name = "Range", .value = range_header };
            extra_headers_count = 1;
        }

        // Make HTTP request with Range header if provided
        const uri = try std.Uri.parse(url);
        var req = try self.http_client.request(.GET, uri, .{
            .extra_headers = extra_headers_storage[0..extra_headers_count],
        });
        defer req.deinit();

        try req.sendBodiless();
        var response = try req.receiveHead(&.{});

        // Check status code (206 for partial content, 200 for full)
        // Note: 403 Forbidden can occur when signed URLs expire - this should trigger
        // a fetch info refresh in the caller
        if (response.head.status != .ok and response.head.status != .partial_content) {
            return statusToError(response.head.status);
        }

        var reader = response.reader(&.{});
        // Use 80 MB limit to allow for protocol overhead while still protecting against excessive memory usage
        // The protocol specifies 64 MiB max for Xorb content, but we need headroom for HTTP headers/overhead
        const xorb_data = try reader.allocRemaining(self.allocator, @enumFromInt(80 * 1024 * 1024));
        return xorb_data;
    }

    /// Upload Shard to CAS
    pub fn uploadShard(
        self: *CasClient,
        shard_data: []const u8,
    ) !ShardUploadResponse {
        const url = try std.fmt.allocPrint(
            self.allocator,
            "{s}/shards",
            .{self.cas_url},
        );
        defer self.allocator.free(url);

        const auth_header = try self.makeAuthHeader();
        defer self.allocator.free(auth_header);

        const extra_headers = [_]std.http.Header{
            .{ .name = "Authorization", .value = auth_header },
        };

        const uri = try std.Uri.parse(url);
        var req = try self.http_client.request(.POST, uri, .{
            .extra_headers = &extra_headers,
            .headers = .{
                .content_type = .{ .override = "application/octet-stream" },
            },
        });
        defer req.deinit();

        req.transfer_encoding = .{ .content_length = shard_data.len };
        var req_body = try req.sendBodyUnflushed(&.{});
        try req_body.writer.writeAll(shard_data);
        try req_body.end();
        try req.connection.?.flush();
        var response = try req.receiveHead(&.{});

        if (response.head.status != .ok) {
            return statusToError(response.head.status);
        }

        var reader = response.reader(&.{});
        const body = try reader.allocRemaining(self.allocator, @enumFromInt(1024));
        defer self.allocator.free(body);

        const parsed = try std.json.parseFromSlice(
            std.json.Value,
            self.allocator,
            body,
            .{},
        );
        defer parsed.deinit();

        const result = @as(u8, @intCast(parsed.value.object.get("result").?.integer));

        return ShardUploadResponse{ .result = result };
    }
};

test "parseReconstruction extracts terms and fetch info" {
    const allocator = std.testing.allocator;

    const body =
        \\{
        \\  "offset_into_first_range": 42,
        \\  "terms": [
        \\    {
        \\      "hash": "cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6",
        \\      "unpacked_length": 1000,
        \\      "range": { "start": 0, "end": 5 }
        \\    }
        \\  ],
        \\  "fetch_info": {
        \\    "cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6": [
        \\      {
        \\        "range": { "start": 0, "end": 5 },
        \\        "url": "https://example.com/xorb",
        \\        "url_range": { "start": 0, "end": 999 }
        \\      }
        \\    ]
        \\  }
        \\}
    ;

    var recon = try parseReconstruction(allocator, body);
    defer recon.deinit();

    try std.testing.expectEqual(@as(u64, 42), recon.offset_into_first_range);
    try std.testing.expectEqual(@as(usize, 1), recon.terms.len);
    try std.testing.expectEqual(@as(u32, 1000), recon.terms[0].unpacked_length);
    try std.testing.expectEqual(@as(u32, 0), recon.terms[0].range.start);
    try std.testing.expectEqual(@as(u32, 5), recon.terms[0].range.end);

    const expected_hash = try hashing.hexToHash("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6");
    try std.testing.expectEqualSlices(u8, &expected_hash, &recon.terms[0].hash);

    const infos = recon.fetch_info.get("cfc5d07f6f03c29bbf424132963fe08d19a37d5757aaf520bf08119f05cd56d6").?;
    try std.testing.expectEqual(@as(usize, 1), infos.len);
    try std.testing.expectEqualStrings("https://example.com/xorb", infos[0].url);
    try std.testing.expectEqual(@as(u64, 999), infos[0].url_range.end);
}

test "hash conversion - API hex format" {
    const testing = std.testing;
    const allocator = testing.allocator;

    // Test hash conversion to API hex format
    var hash: [32]u8 = undefined;
    for (0..32) |i| {
        hash[i] = @intCast(i);
    }

    const hex = try hashToApiHex(hash, allocator);
    defer allocator.free(hex);

    // Verify length
    try testing.expectEqual(@as(usize, 64), hex.len);

    // Convert back and verify
    const hash_back = try apiHexToHash(hex);
    try testing.expectEqualSlices(u8, &hash, &hash_back);
}

test "hash conversion - roundtrip" {
    const testing = std.testing;
    const allocator = testing.allocator;

    // Test with known hash
    const hash: [32]u8 = .{
        0x01, 0x23, 0x45, 0x67, 0x89, 0xab, 0xcd, 0xef,
        0xfe, 0xdc, 0xba, 0x98, 0x76, 0x54, 0x32, 0x10,
        0x11, 0x22, 0x33, 0x44, 0x55, 0x66, 0x77, 0x88,
        0x99, 0xaa, 0xbb, 0xcc, 0xdd, 0xee, 0xff, 0x00,
    };

    const hex = try hashToApiHex(hash, allocator);
    defer allocator.free(hex);

    const hash_back = try apiHexToHash(hex);
    try testing.expectEqualSlices(u8, &hash, &hash_back);
}

test "error classification" {
    const testing = std.testing;

    // Non-retryable errors
    try testing.expectEqual(ErrorClass.non_retryable, classifyError(error.BadRequest));
    try testing.expectEqual(ErrorClass.non_retryable, classifyError(error.Unauthorized));
    try testing.expectEqual(ErrorClass.non_retryable, classifyError(error.Forbidden));
    try testing.expectEqual(ErrorClass.non_retryable, classifyError(error.NotFound));
    try testing.expectEqual(ErrorClass.non_retryable, classifyError(error.RangeNotSatisfiable));

    // Retryable errors
    try testing.expectEqual(ErrorClass.retryable, classifyError(error.TooManyRequests));
    try testing.expectEqual(ErrorClass.retryable, classifyError(error.InternalServerError));
    try testing.expectEqual(ErrorClass.retryable, classifyError(error.ServiceUnavailable));
    try testing.expectEqual(ErrorClass.retryable, classifyError(error.GatewayTimeout));
    try testing.expectEqual(ErrorClass.retryable, classifyError(error.NetworkError));
}
