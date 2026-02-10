//! Parallel chunk fetcher using Io.Group
//!
//! This module provides parallel downloading, decompression, and hashing of chunks
//! using std.Io.Group.concurrent for true concurrent I/O operations.

const std = @import("std");
const Allocator = std.mem.Allocator;
const cas_client = @import("cas_client.zig");
const xorb = @import("xorb.zig");
const hashing = @import("hashing.zig");

/// Result of chunk processing
pub const ChunkResult = struct {
    data: []u8,
    hash: ?hashing.Hash,
    index: usize,
    allocator: Allocator,

    pub fn deinit(self: *ChunkResult) void {
        self.allocator.free(self.data);
    }
};

/// Context for a single chunk fetch operation
const ChunkFetchContext = struct {
    allocator: Allocator,
    io: std.Io,
    http_client: *std.http.Client,
    term: cas_client.ReconstructionTerm,
    fetch_info: []cas_client.FetchInfo,
    index: usize,
    compute_hashes: bool,
    results: []?ChunkResult,
    mutex: *std.Io.Mutex,
    error_occurred: *std.atomic.Value(bool),
    first_error: *?anyerror,
    error_mutex: *std.Io.Mutex,
};

/// Fetch data from a presigned URL
fn fetchFromUrl(
    allocator: Allocator,
    http_client: *std.http.Client,
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

    const uri = try std.Uri.parse(url);
    var req = try http_client.request(.GET, uri, .{
        .extra_headers = extra_headers_storage[0..extra_headers_count],
    });
    defer req.deinit();

    try req.sendBodiless();
    var response = try req.receiveHead(&.{});

    if (response.head.status != .ok and response.head.status != .partial_content) {
        return error.HttpError;
    }

    var reader = response.reader(&.{});
    return try reader.allocRemaining(allocator, @enumFromInt(128 * 1024 * 1024));
}

/// Process a single chunk - called concurrently via Io.Group
fn processChunk(ctx: *ChunkFetchContext) void {
    if (ctx.error_occurred.load(.acquire)) return;

    const result = processChunkInner(ctx) catch |err| {
        ctx.error_mutex.lockUncancelable(ctx.io);
        defer ctx.error_mutex.unlock(ctx.io);

        if (ctx.first_error.* == null) {
            ctx.first_error.* = err;
        }
        ctx.error_occurred.store(true, .release);
        return;
    };

    ctx.mutex.lockUncancelable(ctx.io);
    defer ctx.mutex.unlock(ctx.io);
    ctx.results[ctx.index] = result;
}

fn processChunkInner(ctx: *ChunkFetchContext) !ChunkResult {
    const matching_fetch_info = blk: {
        for (ctx.fetch_info) |fetch_info| {
            if (fetch_info.range.start <= ctx.term.range.start and
                fetch_info.range.end >= ctx.term.range.end)
            {
                break :blk fetch_info;
            }
        }
        return error.NoMatchingFetchInfo;
    };

    const xorb_data = try fetchFromUrl(
        ctx.allocator,
        ctx.http_client,
        matching_fetch_info.url,
        .{ .start = matching_fetch_info.url_range.start, .end = matching_fetch_info.url_range.end },
    );
    defer ctx.allocator.free(xorb_data);

    const local_start = ctx.term.range.start - matching_fetch_info.range.start;
    const local_end = ctx.term.range.end - matching_fetch_info.range.start;

    var xorb_reader = xorb.XorbReader.init(ctx.allocator, xorb_data);
    const chunk_data = try xorb_reader.extractChunkRange(local_start, local_end);
    errdefer ctx.allocator.free(chunk_data);

    const chunk_hash = if (ctx.compute_hashes)
        hashing.computeDataHash(chunk_data)
    else
        null;

    return ChunkResult{
        .data = chunk_data,
        .hash = chunk_hash,
        .index = ctx.index,
        .allocator = ctx.allocator,
    };
}

/// Parallel chunk fetcher using Io.Group
pub const ParallelFetcher = struct {
    allocator: Allocator,
    io: std.Io,
    http_client: *std.http.Client,
    compute_hashes: bool,

    pub fn init(
        allocator: Allocator,
        io: std.Io,
        http_client: *std.http.Client,
        compute_hashes: bool,
    ) ParallelFetcher {
        return ParallelFetcher{
            .allocator = allocator,
            .io = io,
            .http_client = http_client,
            .compute_hashes = compute_hashes,
        };
    }

    /// Fetch all chunks in parallel and return results in order
    pub fn fetchAll(
        self: *ParallelFetcher,
        terms: []cas_client.ReconstructionTerm,
        fetch_info_map: std.StringHashMap([]cas_client.FetchInfo),
    ) ![]ChunkResult {
        if (terms.len == 0) {
            return &[_]ChunkResult{};
        }

        const results = try self.allocator.alloc(?ChunkResult, terms.len);
        defer self.allocator.free(results);
        @memset(results, null);

        var mutex: std.Io.Mutex = .init;
        var error_occurred = std.atomic.Value(bool).init(false);
        var first_error: ?anyerror = null;
        var error_mutex: std.Io.Mutex = .init;

        const contexts = try self.allocator.alloc(ChunkFetchContext, terms.len);
        defer self.allocator.free(contexts);

        for (terms, 0..) |term, i| {
            const hash_hex = try cas_client.hashToApiHex(term.hash, self.allocator);
            defer self.allocator.free(hash_hex);

            const fetch_infos = fetch_info_map.get(hash_hex) orelse return error.MissingFetchInfo;

            contexts[i] = .{
                .allocator = self.allocator,
                .io = self.io,
                .http_client = self.http_client,
                .term = term,
                .fetch_info = fetch_infos,
                .index = i,
                .compute_hashes = self.compute_hashes,
                .results = results,
                .mutex = &mutex,
                .error_occurred = &error_occurred,
                .first_error = &first_error,
                .error_mutex = &error_mutex,
            };
        }

        var group: std.Io.Group = .init;

        for (contexts) |*ctx| {
            group.concurrent(self.io, processChunk, .{ctx}) catch |err| {
                switch (err) {
                    error.ConcurrencyUnavailable => {
                        processChunk(ctx);
                    },
                }
            };
        }

        group.await(self.io) catch unreachable;

        if (error_occurred.load(.acquire)) {
            for (results) |*opt_result| {
                if (opt_result.*) |*result| {
                    result.deinit();
                }
            }
            return first_error orelse error.UnknownError;
        }

        var ordered_results = try self.allocator.alloc(ChunkResult, terms.len);
        errdefer {
            for (ordered_results) |*result| {
                result.deinit();
            }
            self.allocator.free(ordered_results);
        }

        for (results, 0..) |opt_result, i| {
            if (opt_result) |result| {
                ordered_results[i] = result;
            } else {
                return error.MissingResult;
            }
        }

        return ordered_results;
    }

    /// Fetch all chunks and write directly to writer
    pub fn fetchAndWrite(
        self: *ParallelFetcher,
        terms: []cas_client.ReconstructionTerm,
        fetch_info_map: std.StringHashMap([]cas_client.FetchInfo),
        writer: *std.Io.Writer,
    ) !void {
        const results = try self.fetchAll(terms, fetch_info_map);
        defer {
            for (results) |*result| {
                var mut_result = result.*;
                mut_result.deinit();
            }
            self.allocator.free(results);
        }

        for (results) |result| {
            try writer.writeAll(result.data);
        }
    }
};
