const std = @import("std");
const Allocator = std.mem.Allocator;

pub const HF_ENDPOINT = "https://huggingface.co";

pub const BucketError = error{
    NotFound,
    Unauthorized,
    Forbidden,
    InvalidResponse,
    OutOfMemory,
    ApiError,
    EmptyData,
};

pub const XetConnectionInfo = struct {
    access_token: []const u8,
    cas_url: []const u8,
    allocator: Allocator,

    pub fn deinit(self: *XetConnectionInfo) void {
        self.allocator.free(self.access_token);
        self.allocator.free(self.cas_url);
    }
};

pub const BucketFileMetadata = struct {
    size: u64,
    xet_hash: []const u8,
    allocator: Allocator,

    pub fn deinit(self: *BucketFileMetadata) void {
        self.allocator.free(self.xet_hash);
    }
};

pub fn ensureBucket(
    allocator: Allocator,
    io: std.Io,
    bucket_id: []const u8,
    hf_token: []const u8,
) !void {
    const slash_idx = std.mem.indexOfScalar(u8, bucket_id, '/') orelse
        return BucketError.InvalidResponse;

    const url = try std.fmt.allocPrint(
        allocator,
        "{s}/api/buckets/{s}/{s}",
        .{ HF_ENDPOINT, bucket_id[0..slash_idx], bucket_id[slash_idx + 1 ..] },
    );
    defer allocator.free(url);

    const auth_header = try std.fmt.allocPrint(allocator, "Bearer {s}", .{hf_token});
    defer allocator.free(auth_header);

    var http_client = std.http.Client{ .allocator = allocator, .io = io };
    defer http_client.deinit();

    const uri = try std.Uri.parse(url);
    var req = try http_client.request(.POST, uri, .{
        .extra_headers = &.{
            .{ .name = "Authorization", .value = auth_header },
            .{ .name = "Content-Type", .value = "application/json" },
        },
    });
    defer req.deinit();

    req.transfer_encoding = .chunked;
    var req_body = try req.sendBodyUnflushed(&.{});
    try req_body.writer.writeAll("{}\n");
    try req_body.end();
    try req.connection.?.flush();

    const response = try req.receiveHead(&.{});

    if (response.head.status != .ok and
        response.head.status != .created and
        response.head.status != .conflict)
    {
        return mapStatusError(response.head.status);
    }
}

pub fn getXetToken(
    allocator: Allocator,
    io: std.Io,
    bucket_id: []const u8,
    hf_token: []const u8,
    token_type: []const u8,
) !XetConnectionInfo {
    const url = try std.fmt.allocPrint(
        allocator,
        "{s}/api/buckets/{s}/xet-{s}-token",
        .{ HF_ENDPOINT, bucket_id, token_type },
    );
    defer allocator.free(url);

    const auth_header = try std.fmt.allocPrint(allocator, "Bearer {s}", .{hf_token});
    defer allocator.free(auth_header);

    var http_client = std.http.Client{ .allocator = allocator, .io = io };
    defer http_client.deinit();

    const uri = try std.Uri.parse(url);
    var req = try http_client.request(.GET, uri, .{
        .extra_headers = &.{
            .{ .name = "Authorization", .value = auth_header },
        },
    });
    defer req.deinit();

    try req.sendBodiless();
    const response = try req.receiveHead(&.{});

    if (response.head.status != .ok) {
        return mapStatusError(response.head.status);
    }

    var xet_cas_url: ?[]const u8 = null;
    var xet_access_token: ?[]const u8 = null;
    var header_iter = response.head.iterateHeaders();

    while (header_iter.next()) |header| {
        if (std.ascii.eqlIgnoreCase(header.name, "X-Xet-Cas-Url")) {
            xet_cas_url = header.value;
        } else if (std.ascii.eqlIgnoreCase(header.name, "X-Xet-Access-Token")) {
            xet_access_token = header.value;
        }
    }

    return .{
        .access_token = try allocator.dupe(u8, xet_access_token orelse return BucketError.InvalidResponse),
        .cas_url = try allocator.dupe(u8, xet_cas_url orelse return BucketError.InvalidResponse),
        .allocator = allocator,
    };
}

pub fn getFileMetadata(
    allocator: Allocator,
    io: std.Io,
    bucket_id: []const u8,
    hf_token: []const u8,
    remote_path: []const u8,
) !BucketFileMetadata {
    const url = try std.fmt.allocPrint(
        allocator,
        "{s}/buckets/{s}/resolve/{s}",
        .{ HF_ENDPOINT, bucket_id, remote_path },
    );
    defer allocator.free(url);

    const auth_header = try std.fmt.allocPrint(allocator, "Bearer {s}", .{hf_token});
    defer allocator.free(auth_header);

    var http_client = std.http.Client{ .allocator = allocator, .io = io };
    defer http_client.deinit();

    const uri = try std.Uri.parse(url);
    var req = try http_client.request(.HEAD, uri, .{
        .extra_headers = &.{
            .{ .name = "Authorization", .value = auth_header },
        },
    });
    defer req.deinit();

    try req.sendBodiless();
    const response = try req.receiveHead(&.{});

    var xet_hash: ?[]const u8 = null;
    var content_length: ?[]const u8 = null;
    var header_iter = response.head.iterateHeaders();

    while (header_iter.next()) |header| {
        if (std.ascii.eqlIgnoreCase(header.name, "x-xet-hash")) {
            xet_hash = header.value;
        } else if (std.ascii.eqlIgnoreCase(header.name, "content-length")) {
            content_length = header.value;
        }
    }

    const size: u64 = blk: {
        const cl = content_length orelse return BucketError.InvalidResponse;
        break :blk try std.fmt.parseInt(u64, cl, 10);
    };

    return .{
        .size = size,
        .xet_hash = try allocator.dupe(u8, xet_hash orelse return BucketError.NotFound),
        .allocator = allocator,
    };
}

pub fn registerFile(
    allocator: Allocator,
    io: std.Io,
    bucket_id: []const u8,
    hf_token: []const u8,
    remote_path: []const u8,
    xet_hash_hex: []const u8,
) !void {
    const url = try std.fmt.allocPrint(
        allocator,
        "{s}/api/buckets/{s}/batch",
        .{ HF_ENDPOINT, bucket_id },
    );
    defer allocator.free(url);

    const auth_header = try std.fmt.allocPrint(allocator, "Bearer {s}", .{hf_token});
    defer allocator.free(auth_header);

    var http_client = std.http.Client{ .allocator = allocator, .io = io };
    defer http_client.deinit();

    const uri = try std.Uri.parse(url);
    var req = try http_client.request(.POST, uri, .{
        .extra_headers = &.{
            .{ .name = "Authorization", .value = auth_header },
            .{ .name = "Content-Type", .value = "application/x-ndjson" },
        },
    });
    defer req.deinit();

    req.transfer_encoding = .chunked;
    var req_body = try req.sendBodyUnflushed(&.{});
    try std.json.Stringify.value(.{
        .type = "addFile",
        .path = remote_path,
        .xetHash = xet_hash_hex,
        .mtime = 0,
    }, .{}, &req_body.writer);
    try req_body.writer.writeAll("\n");
    try req_body.end();
    try req.connection.?.flush();

    const response = try req.receiveHead(&.{});

    if (response.head.status != .ok and
        response.head.status != .created)
    {
        return mapStatusError(response.head.status);
    }
}

fn mapStatusError(status: std.http.Status) BucketError {
    return switch (status) {
        .not_found => BucketError.NotFound,
        .unauthorized => BucketError.Unauthorized,
        .forbidden => BucketError.Forbidden,
        else => BucketError.ApiError,
    };
}
