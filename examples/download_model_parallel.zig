//! Download a model from Hugging Face using parallel chunk fetching.
//!
//! This example demonstrates how to use the parallel fetching API to download
//! models faster by fetching, decompressing, and processing chunks concurrently
//! using Io.Group.concurrent for concurrent I/O operations.
//!
//! Usage:
//!   HF_TOKEN=your_token zig build run-example-parallel -- <repo_id> [filename]
//!   zig build run-example-parallel -- --help
//!
//! Examples:
//!   # Download a specific file
//!   HF_TOKEN=hf_xxx zig build run-example-parallel -- jedisct1/MiMo-7B-RL-GGUF MiMo-7B-RL-Q8_0.gguf
//!
//!   # Download every XET file in the repository
//!   HF_TOKEN=hf_xxx zig build run-example-parallel -- jedisct1/MiMo-7B-RL-GGUF all
//!
//!   # List available files in a repository
//!   HF_TOKEN=hf_xxx zig build run-example-parallel -- apple/DiffuCoder-7B-Instruct

const std = @import("std");
const xet = @import("xet");

fn printUsage(prog: []const u8) void {
    std.debug.print("Usage: {s} <repo_id> [filename]\n\n", .{prog});
    std.debug.print("Download a model file from Hugging Face using parallel chunk fetching.\n", .{});
    std.debug.print("Requires the HF_TOKEN environment variable to be set.\n", .{});
    std.debug.print("\nArguments:\n", .{});
    std.debug.print("  <repo_id>   Hugging Face repository, e.g. owner/model\n", .{});
    std.debug.print("  [filename]  File to download; if omitted, lists available XET files.\n", .{});
    std.debug.print("              Use \"all\" to download every XET file in the repository.\n", .{});
    std.debug.print("\nOptions:\n", .{});
    std.debug.print("  -h, --help  Show this help message and exit\n", .{});
    std.debug.print("\nExamples:\n", .{});
    std.debug.print("  # List files in a repository\n", .{});
    std.debug.print("  HF_TOKEN=hf_xxx {s} jedisct1/MiMo-7B-RL-GGUF\n\n", .{prog});
    std.debug.print("  # Download a specific file\n", .{});
    std.debug.print("  HF_TOKEN=hf_xxx {s} jedisct1/MiMo-7B-RL-GGUF MiMo-7B-RL-Q8_0.gguf\n\n", .{prog});
    std.debug.print("  # Download every XET file in the repository\n", .{});
    std.debug.print("  HF_TOKEN=hf_xxx {s} jedisct1/MiMo-7B-RL-GGUF all\n", .{prog});
}

/// Download a single XET file with parallel chunk fetching, creating parent
/// directories as needed, and report timing and throughput.
fn downloadFile(
    allocator: std.mem.Allocator,
    io: std.Io,
    environ: std.process.Environ,
    repo_id: []const u8,
    file: xet.model_download.FileInfo,
    output_path: []const u8,
) !void {
    if (std.Io.Dir.path.dirname(output_path)) |parent| {
        if (parent.len > 0) {
            try std.Io.Dir.cwd().createDirPath(io, parent);
        }
    }

    const config = xet.model_download.DownloadConfig{
        .repo_id = repo_id,
        .repo_type = "model",
        .revision = "main",
        .file_hash_hex = file.xet_hash.?,
    };

    std.debug.print("Downloading {s} -> {s}\n", .{ file.path, output_path });

    const start_time = std.Io.Clock.Timestamp.now(io, .boot);

    try xet.model_download.downloadModelToFileParallel(
        allocator,
        io,
        environ,
        config,
        output_path,
        false,
    );

    const elapsed = start_time.untilNow(io);
    const duration_ms: i64 = elapsed.raw.toMilliseconds();

    const file_size = file.size;
    const size_mb = @as(f64, @floatFromInt(file_size)) / (1024.0 * 1024.0);
    const speed_mbps = if (duration_ms > 0)
        size_mb / (@as(f64, @floatFromInt(duration_ms)) / 1000.0)
    else
        0;

    std.debug.print("  {d} bytes in {d}ms ({d:.2} MB/s)\n", .{ file_size, duration_ms, speed_mbps });
}

pub fn main(init: std.process.Init) !void {
    const allocator = init.gpa;

    var args_iter = std.process.Args.Iterator.init(init.minimal.args);
    var args: std.ArrayList([]const u8) = .empty;
    defer args.deinit(allocator);
    while (args_iter.next()) |arg| {
        try args.append(allocator, arg);
    }

    if (args.items.len < 2) {
        printUsage(args.items[0]);
        return error.InvalidArgs;
    }

    if (std.mem.eql(u8, args.items[1], "--help") or std.mem.eql(u8, args.items[1], "-h")) {
        printUsage(args.items[0]);
        return;
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

    const io = init.io;
    const environ = init.minimal.environ;

    if (filename == null) {
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
        std.debug.print("To download every XET file, run:\n", .{});
        std.debug.print("  {s} {s} all\n", .{ args.items[0], repo_id });
        return;
    }

    const name = filename.?;

    if (std.mem.eql(u8, name, "all")) {
        const model_dir = std.Io.Dir.path.basename(repo_id);
        std.debug.print("Downloading {d} XET file(s) from {s} into {s}/, preserving directory layout.\n\n", .{ xet_files.items.len, repo_id, model_dir });

        const start_time = std.Io.Clock.Timestamp.now(io, .boot);

        var downloaded: usize = 0;
        var failed: usize = 0;
        var total_bytes: u64 = 0;
        for (xet_files.items) |file| {
            const output_path = try std.Io.Dir.path.join(allocator, &.{ model_dir, file.path });
            defer allocator.free(output_path);
            downloadFile(allocator, io, environ, repo_id, file, output_path) catch |err| {
                std.debug.print("  Error: download of {s} failed: {}\n", .{ file.path, err });
                failed += 1;
                continue;
            };
            downloaded += 1;
            total_bytes += file.size;
        }

        const elapsed = start_time.untilNow(io);
        const duration_ms: i64 = elapsed.raw.toMilliseconds();

        std.debug.print("\nAll downloads complete!\n", .{});
        std.debug.print("  Files: {d}/{d}\n", .{ downloaded, xet_files.items.len });
        std.debug.print("  Size: {d:.2} MB\n", .{@as(f64, @floatFromInt(total_bytes)) / (1024.0 * 1024.0)});
        std.debug.print("  Time: {d}ms\n", .{duration_ms});

        if (failed > 0) {
            std.debug.print("{d} file(s) failed to download.\n", .{failed});
            return error.SomeDownloadsFailed;
        }
        return;
    }

    var selected_file: ?xet.model_download.FileInfo = null;
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

    const file = selected_file.?;
    const output_path = std.Io.Dir.path.basename(file.path);

    std.debug.print("\nDownloading with parallel fetching...\n", .{});
    std.debug.print("  Repository: {s}\n\n", .{repo_id});

    try downloadFile(allocator, io, environ, repo_id, file, output_path);

    std.debug.print("Output: {s}\n", .{output_path});
}
