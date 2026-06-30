//! Download a model from Hugging Face using the XET protocol.
//!
//! This example demonstrates how to use the high-level model download API.
//! The library handles all the complexity:
//! - Authentication with Hugging Face Hub
//! - Requesting XET tokens
//! - Querying CAS for file reconstruction info
//! - Fetching chunks from xorbs (with full deduplication support)
//! - Reconstructing and saving the file
//!
//! Usage:
//!   HF_TOKEN=your_token zig build run-example-download -- <repo_id> [filename]
//!   zig build run-example-download -- --help
//!
//! Examples:
//!   # List files in a repository
//!   HF_TOKEN=hf_xxx zig build run-example-download -- jedisct1/MiMo-7B-RL-GGUF
//!
//!   # Download a specific file
//!   HF_TOKEN=hf_xxx zig build run-example-download -- jedisct1/MiMo-7B-RL-GGUF MiMo-7B-RL-Q8_0.gguf
//!
//!   # Download every XET file in the repository
//!   HF_TOKEN=hf_xxx zig build run-example-download -- jedisct1/MiMo-7B-RL-GGUF all

const std = @import("std");
const xet = @import("xet");
const xprogress = @import("progress.zig");

fn printUsage(w: *std.Io.Writer, prog: []const u8) !void {
    try w.print("Usage: {s} <repo_id> [filename]\n\n", .{prog});
    try w.print("Download a model file from Hugging Face using the XET protocol.\n", .{});
    try w.print("Requires the HF_TOKEN environment variable to be set.\n", .{});
    try w.print("\nArguments:\n", .{});
    try w.print("  <repo_id>   Hugging Face repository, e.g. owner/model\n", .{});
    try w.print("  [filename]  File to download; if omitted, lists available XET files.\n", .{});
    try w.print("              Use \"all\" to download every XET file in the repository.\n", .{});
    try w.print("\nOptions:\n", .{});
    try w.print("  -h, --help  Show this help message and exit\n", .{});
    try w.print("\nExamples:\n", .{});
    try w.print("  # List files in a repository\n", .{});
    try w.print("  HF_TOKEN=hf_xxx {s} jedisct1/MiMo-7B-RL-GGUF\n\n", .{prog});
    try w.print("  # Download a specific file\n", .{});
    try w.print("  HF_TOKEN=hf_xxx {s} jedisct1/MiMo-7B-RL-GGUF MiMo-7B-RL-Q8_0.gguf\n\n", .{prog});
    try w.print("  # Download every XET file in the repository\n", .{});
    try w.print("  HF_TOKEN=hf_xxx {s} jedisct1/MiMo-7B-RL-GGUF all\n", .{prog});
}

/// Download a single XET file to `output_path`, creating parent directories as
/// needed, and report timing and throughput.
fn downloadFile(
    allocator: std.mem.Allocator,
    io: std.Io,
    environ: std.process.Environ,
    repo_id: []const u8,
    file: xet.model_download.FileInfo,
    output_path: []const u8,
    index: usize,
    count: usize,
    stdout: *std.Io.Writer,
    stderr: *std.Io.Writer,
) !void {
    if (std.Io.Dir.path.dirname(output_path)) |parent| {
        if (parent.len > 0) {
            try std.Io.Dir.cwd().createDirPath(io, parent);
        }
    }

    try stdout.print("[{d}/{d}] Downloading {s} -> {s}\n", .{ index, count, file.path, output_path });
    try stdout.flush();

    var bar = xprogress.Bar.init(io);

    const config = xet.model_download.DownloadConfig{
        .repo_id = repo_id,
        .repo_type = "model",
        .revision = "main",
        .file_hash_hex = file.xet_hash.?,
        .progress = bar.callback(),
    };

    const start_time = std.Io.Clock.Timestamp.now(io, .boot);

    xet.model_download.downloadModelToFile(allocator, io, environ, config, output_path) catch |err| {
        bar.finish();
        try stderr.print("Error: download of {s} failed: {}\n", .{ file.path, err });
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
    bar.finish();

    const elapsed = start_time.untilNow(io);
    const elapsed_ns: i96 = elapsed.raw.nanoseconds;
    const elapsed_s = @as(f64, @floatFromInt(elapsed_ns)) / @as(f64, std.time.ns_per_s);

    const file_size = file.size;
    const size_mb = @as(f64, @floatFromInt(file_size)) / (1024.0 * 1024.0);

    try stdout.print("  {d} bytes in {d:.2}s ({d:.2} MB/s)\n", .{
        file_size,
        elapsed_s,
        if (elapsed_s > 0) size_mb / elapsed_s else 0,
    });
    try stdout.flush();
}

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
        try printUsage(stderr, args.items[0]);
        try stderr.flush();
        return error.InvalidArgs;
    }

    if (std.mem.eql(u8, args.items[1], "--help") or std.mem.eql(u8, args.items[1], "-h")) {
        try printUsage(stdout, args.items[0]);
        try stdout.flush();
        return;
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

    if (filename == null) {
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
        try stdout.print("To download every XET file, run:\n", .{});
        try stdout.print("  {s} {s} all\n", .{ args.items[0], repo_id });
        try stdout.flush();
        return;
    }

    const name = filename.?;

    if (std.mem.eql(u8, name, "all")) {
        var total_files: usize = 0;
        for (file_list.files) |file| {
            if (file.xet_hash != null) total_files += 1;
        }
        if (total_files == 0) {
            try stderr.print("No XET files found in repository.\n", .{});
            try stderr.flush();
            return error.NoXetFiles;
        }

        const model_dir = std.Io.Dir.path.basename(repo_id);
        try stdout.print("Downloading {d} XET file(s) from {s} into {s}/, preserving directory layout.\n\n", .{ total_files, repo_id, model_dir });
        try stdout.flush();

        const start_time = std.Io.Clock.Timestamp.now(io, .boot);

        var downloaded: usize = 0;
        var failed: usize = 0;
        var total_bytes: u64 = 0;
        var index: usize = 0;
        for (file_list.files) |*file| {
            if (file.xet_hash == null) continue;
            index += 1;
            const output_path = try std.Io.Dir.path.join(allocator, &.{ model_dir, file.path });
            defer allocator.free(output_path);
            downloadFile(allocator, io, init.minimal.environ, repo_id, file.*, output_path, index, total_files, stdout, stderr) catch {
                failed += 1;
                continue;
            };
            downloaded += 1;
            total_bytes += file.size;
        }

        const elapsed = start_time.untilNow(io);
        const elapsed_ns: i96 = elapsed.raw.nanoseconds;
        const elapsed_s = @as(f64, @floatFromInt(elapsed_ns)) / @as(f64, std.time.ns_per_s);

        try stdout.print("\nAll downloads complete!\n", .{});
        try stdout.print("  Files: {d}/{d}\n", .{ downloaded, total_files });
        try stdout.print("  Size: {d} bytes ({d:.2} GB)\n", .{
            total_bytes,
            @as(f64, @floatFromInt(total_bytes)) / (1024.0 * 1024.0 * 1024.0),
        });
        try stdout.print("  Time: {d:.2}s\n", .{elapsed_s});
        try stdout.flush();

        if (failed > 0) {
            try stderr.print("{d} file(s) failed to download.\n", .{failed});
            try stderr.flush();
            return error.SomeDownloadsFailed;
        }
        return;
    }

    var selected_file: ?xet.model_download.FileInfo = null;
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

    const file = selected_file.?;
    const output_path = std.Io.Dir.path.basename(file.path);

    try stdout.print("XET Protocol Model Download\n", .{});
    try stdout.print("===========================\n\n", .{});
    try stdout.print("Repository: {s}\n\n", .{repo_id});

    try downloadFile(allocator, io, init.minimal.environ, repo_id, file, output_path, 1, 1, stdout, stderr);

    try stdout.print("Output: {s}\n", .{output_path});
    try stdout.flush();
}
