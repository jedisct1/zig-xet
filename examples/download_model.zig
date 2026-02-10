const std = @import("std");
const xet = @import("xet");

/// Download a model from Hugging Face using the XET protocol
///
/// This example demonstrates how to use the high-level model download API.
/// The library handles all the complexity:
/// - Authentication with Hugging Face Hub
/// - Requesting XET tokens
/// - Querying CAS for file reconstruction info
/// - Fetching chunks from xorbs (with full deduplication support)
/// - Reconstructing and saving the file
///
/// Usage:
///   HF_TOKEN=your_token zig build run-example-download -- <repo_id> [filename]
///
/// Examples:
///   # List files in a repository
///   HF_TOKEN=hf_xxx zig build run-example-download -- jedisct1/MiMo-7B-RL-GGUF
///
///   # Download a specific file
///   HF_TOKEN=hf_xxx zig build run-example-download -- jedisct1/MiMo-7B-RL-GGUF MiMo-7B-RL-Q8_0.gguf
///
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

    if (args.items.len < 2) {
        try stderr.print("Usage: {s} <repo_id> [filename]\n", .{args.items[0]});
        try stderr.print("\nExamples:\n", .{});
        try stderr.print("  # List files in a repository\n", .{});
        try stderr.print("  HF_TOKEN=hf_xxx {s} jedisct1/MiMo-7B-RL-GGUF\n\n", .{args.items[0]});
        try stderr.print("  # Download a specific file\n", .{});
        try stderr.print("  HF_TOKEN=hf_xxx {s} jedisct1/MiMo-7B-RL-GGUF MiMo-7B-RL-Q8_0.gguf\n", .{args.items[0]});
        try stderr.flush();
        return error.InvalidArgs;
    }

    const repo_id = args.items[1];
    const filename: ?[]const u8 = if (args.items.len > 2) args.items[2] else null;

    try stdout.print("Fetching file list for {s}...\n", .{repo_id});
    try stdout.flush();

    var file_list = try xet.model_download.listFiles(
        allocator,
        io,
        init.minimal.environ,
        repo_id,
        "model",
        "main",
        null,
    );
    defer file_list.deinit();

    var selected_file: ?xet.model_download.FileInfo = null;

    if (filename) |name| {
        for (file_list.files) |*file| {
            if (file.xet_hash != null and
                (std.mem.eql(u8, file.path, name) or
                    std.mem.endsWith(u8, file.path, name)))
            {
                selected_file = file.*;
                break;
            }
        }
        if (selected_file == null) {
            try stderr.print("File '{s}' not found or not stored with XET.\n", .{name});
            try stderr.print("\nAvailable XET files:\n", .{});
            for (file_list.files) |file| {
                if (file.xet_hash != null) {
                    const size_mb = @as(f64, @floatFromInt(file.size)) / (1024.0 * 1024.0);
                    try stderr.print("  {s} ({d:.2} MB)\n", .{ file.path, size_mb });
                }
            }
            try stderr.flush();
            return error.FileNotFound;
        }
    } else {
        try stdout.print("\nAvailable XET files in {s}:\n", .{repo_id});
        for (file_list.files) |file| {
            if (file.xet_hash != null) {
                const size_mb = @as(f64, @floatFromInt(file.size)) / (1024.0 * 1024.0);
                const size_gb = size_mb / 1024.0;
                if (size_gb >= 1.0) {
                    try stdout.print("  {s} ({d:.2} GB)\n", .{ file.path, size_gb });
                } else {
                    try stdout.print("  {s} ({d:.2} MB)\n", .{ file.path, size_mb });
                }
            }
        }
        try stdout.print("\nTo download a file, run:\n", .{});
        try stdout.print("  {s} {s} <filename>\n", .{ args.items[0], repo_id });
        try stdout.flush();
        return;
    }

    const file = selected_file.?;
    const file_hash_hex = file.xet_hash.?;
    const output_path = std.Io.Dir.path.basename(file.path);

    const config = xet.model_download.DownloadConfig{
        .repo_id = repo_id,
        .repo_type = "model",
        .revision = "main",
        .file_hash_hex = file_hash_hex,
    };

    try stdout.print("XET Protocol Model Download\n", .{});
    try stdout.print("===========================\n\n", .{});
    try stdout.print("Repository: {s}\n", .{config.repo_id});
    try stdout.print("File: {s}\n", .{file.path});
    try stdout.print("Output: {s}\n\n", .{output_path});

    try stdout.print("Downloading model using XET protocol...\n", .{});
    try stdout.print("This may take a while for large models.\n\n", .{});

    try stdout.flush();

    const start_time = std.Io.Clock.Timestamp.now(io, .boot);

    xet.model_download.downloadModelToFile(allocator, io, init.minimal.environ, config, output_path) catch |err| {
        try stderr.print("\nError: Download failed: {}\n", .{err});
        if (err == error.EnvironmentVariableNotFound) {
            try stderr.print("Make sure HF_TOKEN environment variable is set.\n", .{});
            try stderr.print("Get a token at: https://huggingface.co/settings/tokens\n", .{});
        } else if (err == error.AuthenticationFailed) {
            try stderr.print("Authentication failed. Check that:\n", .{});
            try stderr.print("  - HF_TOKEN is valid and not expired\n", .{});
            try stderr.print("  - You have access to the repository\n", .{});
        }
        try stderr.flush();
        return err;
    };

    const elapsed = start_time.untilNow(io);
    const elapsed_ns: i96 = elapsed.raw.nanoseconds;
    const elapsed_ms = @as(f64, @floatFromInt(elapsed_ns)) / @as(f64, std.time.ns_per_ms);
    const elapsed_s = elapsed_ms / 1000.0;

    const output_file = try std.Io.Dir.openFile(.cwd(), io, output_path, .{});
    defer output_file.close(io);
    const file_stat = try output_file.stat(io);
    const file_size = file_stat.size;

    try stdout.print("Download complete!\n", .{});
    try stdout.print("  Time: {d:.2}s\n", .{elapsed_s});
    try stdout.print("  Size: {d} bytes ({d:.2} GB)\n", .{
        file_size,
        @as(f64, @floatFromInt(file_size)) / (1024.0 * 1024.0 * 1024.0),
    });
    try stdout.print("  Speed: {d:.2} MB/s\n", .{
        @as(f64, @floatFromInt(file_size)) / (1024.0 * 1024.0) / elapsed_s,
    });
    try stdout.print("  Output: {s}\n", .{output_path});
    try stdout.flush();
}
