const std = @import("std");
const xet = @import("xet");

const ChunkLocation = struct {
    xorb_index: u32,
    chunk_index: u32,
};

const XorbInfo = struct {
    hash: xet.hashing.Hash,
    serialized: []u8,
    chunk_count: u32,

    fn deinit(self: *XorbInfo, allocator: std.mem.Allocator) void {
        allocator.free(self.serialized);
    }
};

const FileProcessingResult = struct {
    file_hash: xet.hashing.Hash,
    file_size: u64,
    chunk_count: u32,
    unique_chunk_count: u32,
    xorbs: std.ArrayList(XorbInfo),
    dedup_savings: u64,

    fn deinit(self: *FileProcessingResult, allocator: std.mem.Allocator) void {
        for (self.xorbs.items) |*xorb_info| {
            xorb_info.deinit(allocator);
        }
        self.xorbs.deinit(allocator);
    }

    fn totalXorbSize(self: *const FileProcessingResult) u64 {
        var total: u64 = 0;
        for (self.xorbs.items) |xorb_info| {
            total += xorb_info.serialized.len;
        }
        return total;
    }
};

const XorbCreator = struct {
    allocator: std.mem.Allocator,
    io: std.Io,
    compression_type: xet.constants.CompressionType,
    chunking_algorithm: xet.chunking.ChunkingAlgorithm,
    max_xorb_size: usize,
    verbose: bool,

    chunk_dedup: std.AutoHashMap(xet.hashing.Hash, ChunkLocation),
    xorbs: std.ArrayList(XorbInfo),
    current_builder: ?*xet.xorb.XorbBuilder,
    current_chunk_count: u32,
    current_estimated_size: usize,
    all_merkle_nodes: std.ArrayList(xet.hashing.MerkleNode),
    chunk_data_copies: std.ArrayList([]u8),

    fn init(
        allocator: std.mem.Allocator,
        io: std.Io,
        compression_type: xet.constants.CompressionType,
        chunking_algorithm: xet.chunking.ChunkingAlgorithm,
        verbose: bool,
    ) XorbCreator {
        return .{
            .allocator = allocator,
            .io = io,
            .compression_type = compression_type,
            .chunking_algorithm = chunking_algorithm,
            .max_xorb_size = xet.constants.MaxXorbSize,
            .verbose = verbose,
            .chunk_dedup = std.AutoHashMap(xet.hashing.Hash, ChunkLocation).init(allocator),
            .xorbs = .empty,
            .current_builder = null,
            .current_chunk_count = 0,
            .current_estimated_size = 0,
            .all_merkle_nodes = .empty,
            .chunk_data_copies = .empty,
        };
    }

    fn deinit(self: *XorbCreator) void {
        self.chunk_dedup.deinit();
        if (self.current_builder) |builder| {
            builder.deinit();
            self.allocator.destroy(builder);
        }
        for (self.xorbs.items) |*xorb_info| {
            xorb_info.deinit(self.allocator);
        }
        self.xorbs.deinit(self.allocator);
        self.all_merkle_nodes.deinit(self.allocator);
        for (self.chunk_data_copies.items) |chunk_copy| {
            self.allocator.free(chunk_copy);
        }
        self.chunk_data_copies.deinit(self.allocator);
    }

    fn newBuilder(self: *XorbCreator) !void {
        const builder = try self.allocator.create(xet.xorb.XorbBuilder);
        builder.* = xet.xorb.XorbBuilder.init(self.allocator);
        self.current_builder = builder;
        self.current_chunk_count = 0;
        self.current_estimated_size = 0;
    }

    fn finalizeCurrentXorb(self: *XorbCreator) !void {
        if (self.current_builder) |builder| {
            if (builder.chunks.items.len > 0) {
                const serialized = try builder.serialize(self.compression_type);
                const xorb_hash = try builder.computeHash();

                const xorb_info = XorbInfo{
                    .hash = xorb_hash,
                    .serialized = serialized,
                    .chunk_count = self.current_chunk_count,
                };

                try self.xorbs.append(self.allocator, xorb_info);
            }

            builder.deinit();
            self.allocator.destroy(builder);
            self.current_builder = null;
        }
    }

    fn addChunk(self: *XorbCreator, chunk_data: []const u8) !struct { location: ChunkLocation, is_duplicate: bool } {
        const chunk_hash = xet.hashing.computeDataHash(chunk_data);

        try self.all_merkle_nodes.append(self.allocator, .{
            .hash = chunk_hash,
            .size = @intCast(chunk_data.len),
        });

        if (self.chunk_dedup.get(chunk_hash)) |location| {
            return .{ .location = location, .is_duplicate = true };
        }

        if (self.current_builder == null) {
            try self.newBuilder();
        }

        const estimated_chunk_size = xet.constants.XorbChunkHeaderSize + chunk_data.len;
        if (self.current_estimated_size + estimated_chunk_size > self.max_xorb_size) {
            try self.finalizeCurrentXorb();
            try self.newBuilder();
        }

        const location = ChunkLocation{
            .xorb_index = @intCast(self.xorbs.items.len),
            .chunk_index = self.current_chunk_count,
        };

        const chunk_copy = try self.allocator.dupe(u8, chunk_data);
        errdefer self.allocator.free(chunk_copy);

        try self.chunk_data_copies.append(self.allocator, chunk_copy);
        _ = try self.current_builder.?.addChunk(chunk_copy);
        self.current_chunk_count += 1;
        self.current_estimated_size += estimated_chunk_size;

        try self.chunk_dedup.put(chunk_hash, location);

        return .{ .location = location, .is_duplicate = false };
    }

    fn processFile(self: *XorbCreator, file_path: []const u8) !FileProcessingResult {
        const file = try std.Io.Dir.openFile(.cwd(), self.io, file_path, .{});
        defer file.close(self.io);

        return try self.processFileHandle(file);
    }

    fn processFileHandle(self: *XorbCreator, file: std.Io.File) !FileProcessingResult {
        var chunker = xet.chunking.Chunker.initWithAlgorithm(self.chunking_algorithm);
        var total_chunks: u32 = 0;
        var dedup_savings: u64 = 0;
        var bytes_read_total: u64 = 0;

        const buffer_size = 1024 * 1024;
        const read_buffer = try self.allocator.alloc(u8, buffer_size);
        defer self.allocator.free(read_buffer);

        var reader_buffer: [64 * 1024]u8 = undefined;
        var file_reader = std.Io.File.Reader.init(file, self.io, &reader_buffer);
        const file_size = file_reader.getSize() catch 0;

        var accumulated = std.ArrayList(u8).empty;
        defer accumulated.deinit(self.allocator);

        var local_offset: usize = 0;

        while (true) {
            const bytes_read = file_reader.interface.readSliceShort(read_buffer) catch |err| {
                if (err == error.EndOfStream) break;
                return err;
            };
            if (bytes_read == 0) break;

            bytes_read_total += bytes_read;

            try accumulated.appendSlice(self.allocator, read_buffer[0..bytes_read]);

            while (chunker.findNextChunk(accumulated.items[local_offset..])) |boundary| {
                const chunk_data = accumulated.items[boundary.start..boundary.end];

                const result = try self.addChunk(chunk_data);
                if (result.is_duplicate) {
                    dedup_savings += chunk_data.len;
                }
                total_chunks += 1;
                local_offset = boundary.end;

                if (self.verbose and total_chunks % 1000 == 0) {
                    const progress: f64 =
                        @as(f64, @floatFromInt(bytes_read_total)) / @as(f64, @floatFromInt(file_size)) * 100.0;
                    std.debug.print("  Processed {d} chunks ({d:.1}%)...\n", .{ total_chunks, progress });
                }
            }

            if (local_offset > 0 and accumulated.items.len > 2 * xet.constants.MaxChunkSize) {
                const remaining = accumulated.items.len - local_offset;
                if (remaining > 0) {
                    std.mem.copyForwards(u8, accumulated.items[0..remaining], accumulated.items[local_offset..]);
                }
                accumulated.shrinkRetainingCapacity(remaining);

                chunker.position -= local_offset;
                chunker.chunk_start -= local_offset;
                local_offset = 0;
            }
        }

        if (accumulated.items.len > local_offset) {
            const remaining_data = accumulated.items[local_offset..];
            if (remaining_data.len > 0) {
                const result = try self.addChunk(remaining_data);
                if (result.is_duplicate) {
                    dedup_savings += remaining_data.len;
                }
                total_chunks += 1;
            }
        }

        try self.finalizeCurrentXorb();

        const merkle_root = try xet.hashing.buildMerkleTree(
            self.allocator,
            self.all_merkle_nodes.items,
        );
        const file_hash = xet.hashing.computeFileHash(merkle_root);

        var result_xorbs: std.ArrayList(XorbInfo) = .empty;
        try result_xorbs.appendSlice(self.allocator, self.xorbs.items);
        self.xorbs.clearRetainingCapacity();

        return FileProcessingResult{
            .file_hash = file_hash,
            .file_size = bytes_read_total,
            .chunk_count = total_chunks,
            .unique_chunk_count = @intCast(self.chunk_dedup.count()),
            .xorbs = result_xorbs,
            .dedup_savings = dedup_savings,
        };
    }
};

fn writeXorbs(
    allocator: std.mem.Allocator,
    io: std.Io,
    result: *const FileProcessingResult,
    output_dir: []const u8,
    verbose: bool,
) !void {
    var dir: ?std.Io.Dir = null;

    if (!std.mem.eql(u8, output_dir, ".")) {
        std.Io.Dir.createDirPath(.cwd(), io, output_dir) catch |err| {
            if (err != error.PathAlreadyExists) return err;
        };
        dir = try std.Io.Dir.openDir(.cwd(), io, output_dir, .{});
    }
    defer if (dir) |d| d.close(io);

    for (result.xorbs.items) |xorb_info| {
        const hash_hex = xet.hashing.hashToHex(xorb_info.hash);
        const filename = try std.fmt.allocPrint(allocator, "{s}.xorb", .{hash_hex});
        defer allocator.free(filename);

        const target_dir = dir orelse std.Io.Dir.cwd();
        const file = try std.Io.Dir.createFile(target_dir, io, filename, .{});
        defer file.close(io);

        try file.writeStreamingAll(io, xorb_info.serialized);

        if (verbose) {
            std.debug.print("  Written: {s} ({d} bytes)\n", .{ filename, xorb_info.serialized.len });
        }
    }
}

fn printUsage(prog_name: []const u8, stderr: anytype) !void {
    try stderr.print(
        \\Usage: {s} <file> [options]
        \\
        \\Convert a file into XET xorb format with chunking, compression, and deduplication.
        \\
        \\Options:
        \\  -o, --output <dir>    Output directory for xorb files (default: current directory)
        \\  -c, --compression <type>
        \\                        Compression type: none, lz4, bg4, fbs (default: lz4)
        \\  -k, --chunking <alg>  Chunking algorithm: gearhash, ultracdc (default: gearhash)
        \\  -v, --verbose         Verbose output
        \\  -h, --help            Show this help message
        \\
        \\Examples:
        \\  {s} model.safetensors -o ./xorbs
        \\  {s} data.bin -c bg4 -v -o ./output
        \\  {s} data.bin -k ultracdc -o ./output
        \\
    , .{ prog_name, prog_name, prog_name, prog_name });
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

    var input_file: ?[]const u8 = null;
    var output_dir: []const u8 = ".";
    var compression_type: xet.constants.CompressionType = .LZ4;
    var chunking_algorithm: xet.chunking.ChunkingAlgorithm = .gearhash;
    var verbose = false;

    var i: usize = 1;
    while (i < args.items.len) : (i += 1) {
        const arg = args.items[i];

        if (std.mem.eql(u8, arg, "-h") or std.mem.eql(u8, arg, "--help")) {
            try printUsage(args.items[0], stdout);
            return;
        } else if (std.mem.eql(u8, arg, "-o") or std.mem.eql(u8, arg, "--output")) {
            i += 1;
            if (i >= args.items.len) {
                try stderr.print("Error: --output requires an argument\n", .{});
                return error.InvalidArgs;
            }
            output_dir = args.items[i];
        } else if (std.mem.eql(u8, arg, "-c") or std.mem.eql(u8, arg, "--compression")) {
            i += 1;
            if (i >= args.items.len) {
                try stderr.print("Error: --compression requires an argument\n", .{});
                return error.InvalidArgs;
            }
            const comp_str = args.items[i];
            if (std.mem.eql(u8, comp_str, "none")) {
                compression_type = .None;
            } else if (std.mem.eql(u8, comp_str, "lz4")) {
                compression_type = .LZ4;
            } else if (std.mem.eql(u8, comp_str, "bg4")) {
                compression_type = .ByteGrouping4LZ4;
            } else if (std.mem.eql(u8, comp_str, "fbs")) {
                compression_type = .FullBitsliceLZ4;
            } else {
                try stderr.print("Error: Unknown compression type: {s}\n", .{comp_str});
                try stderr.print("Valid types: none, lz4, bg4, fbs\n", .{});
                return error.InvalidArgs;
            }
        } else if (std.mem.eql(u8, arg, "-k") or std.mem.eql(u8, arg, "--chunking")) {
            i += 1;
            if (i >= args.items.len) {
                try stderr.print("Error: --chunking requires an argument\n", .{});
                return error.InvalidArgs;
            }
            const chunk_str = args.items[i];
            if (std.mem.eql(u8, chunk_str, "gearhash")) {
                chunking_algorithm = .gearhash;
            } else if (std.mem.eql(u8, chunk_str, "ultracdc")) {
                chunking_algorithm = .ultracdc;
            } else {
                try stderr.print("Error: Unknown chunking algorithm: {s}\n", .{chunk_str});
                try stderr.print("Valid algorithms: gearhash, ultracdc\n", .{});
                return error.InvalidArgs;
            }
        } else if (std.mem.eql(u8, arg, "-v") or std.mem.eql(u8, arg, "--verbose")) {
            verbose = true;
        } else if (arg[0] == '-') {
            try stderr.print("Error: Unknown option: {s}\n", .{arg});
            try printUsage(args.items[0], stderr);
            return error.InvalidArgs;
        } else {
            if (input_file != null) {
                try stderr.print("Error: Multiple input files specified\n", .{});
                return error.InvalidArgs;
            }
            input_file = arg;
        }
    }

    if (input_file == null) {
        try stderr.print("Error: No input file specified\n\n", .{});
        try printUsage(args.items[0], stderr);
        try stderr.flush();
        return error.InvalidArgs;
    }

    const file_path = input_file.?;

    std.Io.Dir.access(.cwd(), io, file_path, .{}) catch {
        try stderr.print("Error: File not found: {s}\n", .{file_path});
        try stderr.flush();
        return error.FileNotFound;
    };

    if (verbose) {
        try stdout.print("Processing: {s}\n", .{file_path});
        try stdout.print("Compression: {s}\n", .{@tagName(compression_type)});
        try stdout.print("Chunking: {s}\n", .{@tagName(chunking_algorithm)});
        try stdout.print("Output directory: {s}\n\n", .{output_dir});
        try stdout.flush();
    }

    var creator = XorbCreator.init(allocator, io, compression_type, chunking_algorithm, verbose);
    defer creator.deinit();

    var result = try creator.processFile(file_path);
    defer result.deinit(allocator);

    const file_hash_hex = xet.hashing.hashToHex(result.file_hash);
    const total_xorb_size = result.totalXorbSize();
    const compression_ratio: f64 = if (result.file_size > 0)
        (1.0 - @as(f64, @floatFromInt(total_xorb_size)) / @as(f64, @floatFromInt(result.file_size))) * 100.0
    else
        0;

    try stdout.print("\nFile hash:     {s}\n", .{file_hash_hex});
    try stdout.print("File size:     {d} bytes", .{result.file_size});
    if (result.file_size >= 1024 * 1024 * 1024) {
        try stdout.print(" ({d:.2} GB)\n", .{@as(f64, @floatFromInt(result.file_size)) / (1024.0 * 1024.0 * 1024.0)});
    } else if (result.file_size >= 1024 * 1024) {
        try stdout.print(" ({d:.2} MB)\n", .{@as(f64, @floatFromInt(result.file_size)) / (1024.0 * 1024.0)});
    } else {
        try stdout.print("\n", .{});
    }
    try stdout.print("Chunks:        {d}\n", .{result.chunk_count});
    try stdout.print("Unique chunks: {d}\n", .{result.unique_chunk_count});
    try stdout.print("Xorbs:         {d}\n", .{result.xorbs.items.len});

    if (result.dedup_savings > 0) {
        const dedup_ratio = @as(f64, @floatFromInt(result.dedup_savings)) /
            @as(f64, @floatFromInt(result.file_size)) * 100.0;
        try stdout.print("Dedup savings: {d} bytes ({d:.1}%)\n", .{ result.dedup_savings, dedup_ratio });
    }

    try stdout.print("Xorb size:     {d} bytes ({d:.1}% compression)\n", .{ total_xorb_size, compression_ratio });
    try stdout.flush();

    if (verbose) {
        try stdout.print("\nWriting xorbs...\n", .{});
        try stdout.flush();
    }

    try writeXorbs(allocator, io, &result, output_dir, verbose);

    try stdout.print("\nOutput directory: {s}\n", .{output_dir});

    for (result.xorbs.items) |xorb_info| {
        const hash_hex = xet.hashing.hashToHex(xorb_info.hash);
        try stdout.print("  {s}.xorb ({d} bytes, {d} chunks)\n", .{
            hash_hex,
            xorb_info.serialized.len,
            xorb_info.chunk_count,
        });
    }

    try stdout.flush();
}
