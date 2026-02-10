const std = @import("std");
const xet = @import("xet");

/// Download a model from Hugging Face using parallel chunk fetching
///
/// This example demonstrates how to use the parallel fetching API to download
/// models faster by fetching, decompressing, and processing chunks concurrently
/// using Io.Group.concurrent for concurrent I/O operations.
///
/// Usage:
///   HF_TOKEN=your_token zig build run-example-parallel -- <repo_id> [filename]
///
/// Examples:
///   # Download a specific file
///   HF_TOKEN=hf_xxx zig build run-example-parallel -- jedisct1/MiMo-7B-RL-GGUF MiMo-7B-RL-Q8_0.gguf
///
///   # List available files in a repository
///   HF_TOKEN=hf_xxx zig build run-example-parallel -- apple/DiffuCoder-7B-Instruct
pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    var args_iter = std.process.Args.Iterator.init(init.minimal.args);
    var args: std.ArrayList([]const u8) = .empty;
    defer args.deinit(allocator);
    while (args_iter.next()) |arg| {
        try args.append(allocator, arg);
    }

    if (args.items.len < 2) {
        std.debug.print("Usage: {s} <repo_id> [filename]\n", .{args.items[0]});
        std.debug.print("\nExamples:\n", .{});
        std.debug.print("  # List files in a repository\n", .{});
        std.debug.print("  HF_TOKEN=hf_xxx {s} jedisct1/MiMo-7B-RL-GGUF\n\n", .{args.items[0]});
        std.debug.print("  # Download a specific file\n", .{});
        std.debug.print("  HF_TOKEN=hf_xxx {s} jedisct1/MiMo-7B-RL-GGUF MiMo-7B-RL-Q8_0.gguf\n", .{args.items[0]});
        return error.InvalidArgs;
    }

    const repo_id = args.items[1];
    const filename: ?[]const u8 = if (args.items.len > 2) args.items[2] else null;

    var xet_files: std.ArrayList(xet.model_download.FileInfo) = .empty;
    defer {
        for (xet_files.items) |*f| f.deinit();
        xet_files.deinit(allocator);
    }

    {
        const io = init.io;

        std.debug.print("Fetching file list for {s}...\n", .{repo_id});

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

        for (file_list.files) |file| {
            if (file.xet_hash != null) {
                const path_copy = try allocator.dupe(u8, file.path);
                errdefer allocator.free(path_copy);
                const hash_copy = if (file.xet_hash) |h| try allocator.dupe(u8, h) else null;
                try xet_files.append(allocator, .{
                    .path = path_copy,
                    .size = file.size,
                    .xet_hash = hash_copy,
                    .allocator = allocator,
                });
            }
        }
    }

    if (xet_files.items.len == 0) {
        std.debug.print("No XET files found in repository.\n", .{});
        return error.NoXetFiles;
    }

    var selected_file: ?xet.model_download.FileInfo = null;

    if (filename) |name| {
        for (xet_files.items) |file| {
            if (std.mem.eql(u8, file.path, name) or
                std.mem.endsWith(u8, file.path, name))
            {
                selected_file = file;
                break;
            }
        }
        if (selected_file == null) {
            std.debug.print("File '{s}' not found in repository.\n", .{name});
            std.debug.print("\nAvailable XET files:\n", .{});
            for (xet_files.items) |file| {
                const size_mb = @as(f64, @floatFromInt(file.size)) / (1024.0 * 1024.0);
                std.debug.print("  {s} ({d:.2} MB)\n", .{ file.path, size_mb });
            }
            return error.FileNotFound;
        }
    } else {
        std.debug.print("\nAvailable XET files in {s}:\n", .{repo_id});
        std.debug.print("─────────────────────────────────────────────────────\n", .{});
        for (xet_files.items) |file| {
            const size_mb = @as(f64, @floatFromInt(file.size)) / (1024.0 * 1024.0);
            const size_gb = size_mb / 1024.0;
            if (size_gb >= 1.0) {
                std.debug.print("  {s} ({d:.2} GB)\n", .{ file.path, size_gb });
            } else {
                std.debug.print("  {s} ({d:.2} MB)\n", .{ file.path, size_mb });
            }
        }
        std.debug.print("\nTo download a file, run:\n", .{});
        std.debug.print("  {s} {s} <filename>\n", .{ args.items[0], repo_id });
        return;
    }

    const file = selected_file.?;
    const file_hash_hex = file.xet_hash.?;

    const output_path = std.Io.Dir.path.basename(file.path);

    std.debug.print("\nDownloading with parallel fetching...\n", .{});
    std.debug.print("  Repository: {s}\n", .{repo_id});
    std.debug.print("  File:       {s}\n", .{file.path});
    std.debug.print("  Output:     {s}\n", .{output_path});
    std.debug.print("\n", .{});

    const config = xet.model_download.DownloadConfig{
        .repo_id = repo_id,
        .repo_type = "model",
        .revision = "main",
        .file_hash_hex = file_hash_hex,
    };

    const start_time = std.Io.Clock.Timestamp.now(init.io, .boot);

    const download_io = init.io;

    try xet.model_download.downloadModelToFileParallel(
        allocator,
        download_io,
        init.minimal.environ,
        config,
        output_path,
        false,
    );

    const elapsed = start_time.untilNow(init.io);
    const duration_ms: i64 = elapsed.raw.toMilliseconds();

    std.debug.print("\nDownload complete!\n", .{});
    std.debug.print("  Time: {d}ms\n", .{duration_ms});

    const out_file = try std.Io.Dir.openFile(.cwd(), download_io, output_path, .{});
    defer out_file.close(download_io);
    const file_stat = try out_file.stat(download_io);
    const file_size = file_stat.size;
    const size_mb = @as(f64, @floatFromInt(file_size)) / (1024.0 * 1024.0);

    std.debug.print("  Size: {d:.2} MB\n", .{size_mb});

    const speed_mbps = size_mb / (@as(f64, @floatFromInt(duration_ms)) / 1000.0);
    std.debug.print("  Speed: {d:.2} MB/s\n", .{speed_mbps});
}
