const std = @import("std");
const xet = @import("xet");

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;
    const io = init.io;

    var args_iter = std.process.Args.Iterator.init(init.minimal.args);
    var args: std.ArrayList([]const u8) = .empty;
    defer args.deinit(allocator);
    while (args_iter.next()) |arg| {
        try args.append(allocator, arg);
    }

    var stdout_buffer: [4096]u8 = undefined;
    var stdout_writer = std.Io.File.stdout().writer(io, &stdout_buffer);
    defer stdout_writer.interface.flush() catch {};
    const stdout = &stdout_writer.interface;

    var stderr_buffer: [4096]u8 = undefined;
    var stderr_writer = std.Io.File.stderr().writer(io, &stderr_buffer);
    defer stderr_writer.interface.flush() catch {};
    const stderr = &stderr_writer.interface;

    var file_path: ?[]const u8 = null;
    var bucket_id: ?[]const u8 = null;
    var remote_path: ?[]const u8 = null;
    var compression_type: xet.constants.CompressionType = .LZ4;
    var chunking_algorithm: xet.chunking.ChunkingAlgorithm = .gearhash;

    var i: usize = 1;
    while (i < args.items.len) : (i += 1) {
        const arg = args.items[i];

        if (std.mem.eql(u8, arg, "-h") or std.mem.eql(u8, arg, "--help")) {
            try printUsage(args.items[0], stdout);
            return;
        } else if (std.mem.eql(u8, arg, "-r") or std.mem.eql(u8, arg, "--remote-path")) {
            i += 1;
            if (i >= args.items.len) {
                try stderr.print("Error: --remote-path requires an argument\n", .{});
                return error.InvalidArgs;
            }
            remote_path = args.items[i];
        } else if (std.mem.eql(u8, arg, "-c") or std.mem.eql(u8, arg, "--compression")) {
            i += 1;
            if (i >= args.items.len) {
                try stderr.print("Error: --compression requires an argument\n", .{});
                return error.InvalidArgs;
            }
            const comp_str = args.items[i];
            compression_type = xet.constants.CompressionType.fromString(comp_str) orelse {
                try stderr.print("Error: Unknown compression type: {s}\n", .{comp_str});
                try stderr.print("Valid types: none, lz4, bg4, fbs\n", .{});
                return error.InvalidArgs;
            };
        } else if (std.mem.eql(u8, arg, "-k") or std.mem.eql(u8, arg, "--chunking")) {
            i += 1;
            if (i >= args.items.len) {
                try stderr.print("Error: --chunking requires an argument\n", .{});
                return error.InvalidArgs;
            }
            const chunk_str = args.items[i];
            chunking_algorithm = std.meta.stringToEnum(xet.chunking.ChunkingAlgorithm, chunk_str) orelse {
                try stderr.print("Error: Unknown chunking algorithm: {s}\n", .{chunk_str});
                try stderr.print("Valid algorithms: gearhash, ultracdc\n", .{});
                return error.InvalidArgs;
            };
        } else if (arg.len > 0 and arg[0] == '-') {
            try stderr.print("Error: Unknown option: {s}\n", .{arg});
            try printUsage(args.items[0], stderr);
            return error.InvalidArgs;
        } else {
            if (bucket_id == null) {
                bucket_id = arg;
            } else if (file_path == null) {
                file_path = arg;
            } else {
                try stderr.print("Error: Unexpected argument: {s}\n", .{arg});
                return error.InvalidArgs;
            }
        }
    }

    if (bucket_id == null or file_path == null) {
        try stderr.print("Error: Missing required arguments\n\n", .{});
        try printUsage(args.items[0], stderr);
        return error.InvalidArgs;
    }

    const path = file_path.?;
    const bucket = bucket_id.?;
    const remote = remote_path orelse std.Io.Dir.path.basename(path);

    const hf_token = std.process.Environ.getAlloc(init.minimal.environ, allocator, "HF_TOKEN") catch {
        try stderr.print("Error: HF_TOKEN environment variable not set.\n", .{});
        try stderr.print("Get a token at: https://huggingface.co/settings/tokens\n", .{});
        return error.EnvironmentVariableNotFound;
    };
    defer allocator.free(hf_token);

    try stdout.print("XET Protocol File Upload\n", .{});
    try stdout.print("========================\n\n", .{});
    try stdout.print("Bucket:      {s}\n", .{bucket});
    try stdout.print("Local file:  {s}\n", .{path});
    try stdout.print("Remote path: {s}\n", .{remote});
    try stdout.print("Compression: {s}\n", .{@tagName(compression_type)});
    try stdout.print("Chunking:    {s}\n\n", .{@tagName(chunking_algorithm)});
    try stdout.flush();

    const start_time = std.Io.Clock.Timestamp.now(io, .boot);

    try stdout.print("Reading file...\n", .{});
    try stdout.flush();

    const file = std.Io.Dir.openFile(.cwd(), io, path, .{}) catch |err| {
        try stderr.print("Error: Cannot open file '{s}': {}\n", .{ path, err });
        return err;
    };
    defer file.close(io);
    const stat = try file.stat(io);

    const data = try allocator.alloc(u8, stat.size);
    defer allocator.free(data);

    var reader_buffer: [64 * 1024]u8 = undefined;
    var file_reader = std.Io.File.Reader.init(file, io, &reader_buffer);
    const total_read = try file_reader.interface.readSliceShort(data);

    const file_data = data[0..total_read];
    const size_mb = @as(f64, @floatFromInt(total_read)) / (1024.0 * 1024.0);
    try stdout.print("File size: {d} bytes ({d:.2} MB)\n\n", .{ total_read, size_mb });
    try stdout.flush();

    try stdout.print("Ensuring bucket exists...\n", .{});
    try stdout.flush();
    xet.bucket_api.ensureBucket(allocator, io, bucket, hf_token) catch |err| {
        try stderr.print("Error: Failed to ensure bucket: {}\n", .{err});
        return err;
    };

    try stdout.print("Requesting write token...\n", .{});
    try stdout.flush();
    var connection = xet.bucket_api.getXetToken(allocator, io, bucket, hf_token, "write") catch |err| {
        try stderr.print("Error: Failed to get write token: {}\n", .{err});
        return err;
    };
    defer connection.deinit();

    try stdout.print("Uploading data (chunking, compressing, uploading)...\n", .{});
    try stdout.flush();

    var cas = try xet.cas_client.CasClient.init(allocator, io, connection.cas_url, connection.access_token);
    defer cas.deinit();

    const result = xet.upload.uploadDataWithOptions(allocator, &cas, file_data, compression_type, chunking_algorithm) catch |err| {
        try stderr.print("Error: Upload failed: {}\n", .{err});
        return err;
    };

    try stdout.print("Registering file in bucket...\n", .{});
    try stdout.flush();
    xet.bucket_api.registerFile(allocator, io, bucket, hf_token, remote, &result.file_hash_hex) catch |err| {
        try stderr.print("Error: Failed to register file: {}\n", .{err});
        return err;
    };

    const elapsed = start_time.untilNow(io);
    const elapsed_ns: i96 = elapsed.raw.nanoseconds;
    const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / @as(f64, std.time.ns_per_ms);
    const elapsed_s = elapsed_ms / 1000.0;

    try stdout.print("\nUpload complete!\n", .{});
    try stdout.print("  File hash: {s}\n", .{result.file_hash_hex});
    try stdout.print("  Size:      {d} bytes ({d:.2} MB)\n", .{ result.total_size, size_mb });
    try stdout.print("  Time:      {d:.2}s\n", .{elapsed_s});
    if (elapsed_s > 0) {
        try stdout.print("  Speed:     {d:.2} MB/s\n", .{size_mb / elapsed_s});
    }
    try stdout.print("  Bucket:    {s}\n", .{bucket});
    try stdout.print("  Path:      {s}\n", .{remote});
    try stdout.flush();
}

fn printUsage(prog_name: []const u8, writer: *std.Io.Writer) !void {
    try writer.print(
        \\Usage: {s} <bucket_id> <file> [options]
        \\
        \\Upload a file to HuggingFace using the XET protocol.
        \\
        \\Arguments:
        \\  bucket_id             HuggingFace bucket (e.g., user/repo-name)
        \\  file                  Local file path to upload
        \\
        \\Options:
        \\  -r, --remote-path <path>
        \\                        Remote file path in bucket (default: filename)
        \\  -c, --compression <type>
        \\                        Compression type: none, lz4, bg4, fbs (default: lz4)
        \\  -k, --chunking <alg>  Chunking algorithm: gearhash, ultracdc (default: gearhash)
        \\  -h, --help            Show this help message
        \\
        \\Environment:
        \\  HF_TOKEN              HuggingFace API token (required)
        \\
        \\Examples:
        \\  HF_TOKEN=hf_xxx {s} user/my-repo model.safetensors
        \\  HF_TOKEN=hf_xxx {s} user/my-repo data.bin -r models/data.bin
        \\  HF_TOKEN=hf_xxx {s} user/my-repo data.bin -c bg4
        \\  HF_TOKEN=hf_xxx {s} user/my-repo data.bin -k ultracdc
        \\
    , .{ prog_name, prog_name, prog_name, prog_name, prog_name });
}
