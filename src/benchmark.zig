const std = @import("std");
const chunking = @import("chunking.zig");
const hashing = @import("hashing.zig");
const compression = @import("compression.zig");
const xorb = @import("xorb.zig");

const Io = std.Io;

pub const BenchmarkResult = struct {
    name: []const u8,
    duration_ns: u64,
    throughput_mbs: f64,
    iterations: usize,
};

pub fn formatDuration(ns: u64) void {
    if (ns < 1000) {
        std.debug.print("{d} ns", .{ns});
    } else if (ns < 1_000_000) {
        std.debug.print("{d:.2} µs", .{@as(f64, @floatFromInt(ns)) / 1000.0});
    } else if (ns < 1_000_000_000) {
        std.debug.print("{d:.2} ms", .{@as(f64, @floatFromInt(ns)) / 1_000_000.0});
    } else {
        std.debug.print("{d:.3} s", .{@as(f64, @floatFromInt(ns)) / 1_000_000_000.0});
    }
}

pub fn printResult(result: BenchmarkResult) void {
    std.debug.print("{s}: ", .{result.name});
    formatDuration(result.duration_ns);
    if (result.throughput_mbs > 0) {
        std.debug.print(" ({d:.2} MB/s)", .{result.throughput_mbs});
    }
    if (result.iterations > 1) {
        std.debug.print(" [{d} iterations]", .{result.iterations});
    }
    std.debug.print("\n", .{});
}

fn generateRandomData(allocator: std.mem.Allocator, size: usize, seed: u64) ![]u8 {
    const data = try allocator.alloc(u8, size);
    var rng = std.Random.DefaultPrng.init(seed);
    const random = rng.random();
    random.bytes(data);
    return data;
}

fn elapsedNs(start: Io.Clock.Timestamp, io: Io) u64 {
    return @intCast(start.untilNow(io).raw.nanoseconds);
}

fn throughputMbs(data_size: usize, duration_ns: u64) f64 {
    return (@as(f64, @floatFromInt(data_size)) / @as(f64, @floatFromInt(duration_ns))) * 1_000_000_000.0 / (1024.0 * 1024.0);
}

pub fn benchmarkChunking(allocator: std.mem.Allocator, io: Io) !BenchmarkResult {
    const data_size = 100 * 1024 * 1024;
    const data = try generateRandomData(allocator, data_size, 12345);
    defer allocator.free(data);

    const start = Io.Clock.Timestamp.now(io, .boot);

    var boundaries = try chunking.chunkBuffer(allocator, data);
    defer boundaries.deinit(allocator);

    const duration = elapsedNs(start, io);

    std.mem.doNotOptimizeAway(&boundaries);

    return .{
        .name = "Chunking (100 MB)",
        .duration_ns = duration,
        .throughput_mbs = throughputMbs(data_size, duration),
        .iterations = 1,
    };
}

pub fn benchmarkHashing(allocator: std.mem.Allocator, io: Io) !BenchmarkResult {
    const data_size = 100 * 1024 * 1024;
    const data = try generateRandomData(allocator, data_size, 54321);
    defer allocator.free(data);

    const start = Io.Clock.Timestamp.now(io, .boot);

    const hash = hashing.computeDataHash(data);

    const duration = elapsedNs(start, io);

    std.mem.doNotOptimizeAway(&hash);

    return .{
        .name = "BLAKE3 Hashing (100 MB)",
        .duration_ns = duration,
        .throughput_mbs = throughputMbs(data_size, duration),
        .iterations = 1,
    };
}

pub fn benchmarkMerkleTree(allocator: std.mem.Allocator, io: Io) !BenchmarkResult {
    var chunk_infos: std.ArrayList(hashing.MerkleNode) = .empty;
    defer chunk_infos.deinit(allocator);

    for (0..100) |i| {
        try chunk_infos.append(allocator, .{
            .hash = @splat(@intCast(i)),
            .size = 65536,
        });
    }

    const start = Io.Clock.Timestamp.now(io, .boot);

    const merkle_root = try hashing.buildMerkleTree(allocator, chunk_infos.items);
    const file_hash = hashing.computeFileHash(merkle_root);

    const duration = elapsedNs(start, io);

    std.mem.doNotOptimizeAway(&file_hash);

    return .{
        .name = "Merkle Tree (100 chunks)",
        .duration_ns = duration,
        .throughput_mbs = 0,
        .iterations = 1,
    };
}

pub fn benchmarkLZ4Compression(allocator: std.mem.Allocator, io: Io) !BenchmarkResult {
    const data_size = 50 * 1024 * 1024;
    const data = try generateRandomData(allocator, data_size, 98765);
    defer allocator.free(data);

    const start = Io.Clock.Timestamp.now(io, .boot);

    const result = try compression.compress(allocator, data, .LZ4);
    defer allocator.free(result.data);

    const duration = elapsedNs(start, io);

    std.mem.doNotOptimizeAway(&result);

    return .{
        .name = "LZ4 Compression (50 MB)",
        .duration_ns = duration,
        .throughput_mbs = throughputMbs(data_size, duration),
        .iterations = 1,
    };
}

pub fn benchmarkLZ4Decompression(allocator: std.mem.Allocator, io: Io) !BenchmarkResult {
    const data_size = 50 * 1024 * 1024;
    const data = try generateRandomData(allocator, data_size, 98765);
    defer allocator.free(data);

    const compressed = try compression.compress(allocator, data, .LZ4);
    defer allocator.free(compressed.data);

    const start = Io.Clock.Timestamp.now(io, .boot);

    const decompressed = try compression.decompress(allocator, compressed.data, compressed.type, data_size);
    defer allocator.free(decompressed);

    const duration = elapsedNs(start, io);

    std.mem.doNotOptimizeAway(&decompressed);

    return .{
        .name = "LZ4 Decompression (50 MB)",
        .duration_ns = duration,
        .throughput_mbs = throughputMbs(data_size, duration),
        .iterations = 1,
    };
}

pub fn benchmarkByteGrouping4LZ4(allocator: std.mem.Allocator, io: Io) !BenchmarkResult {
    const data_size = 50 * 1024 * 1024;
    const data = try allocator.alloc(u8, data_size);
    defer allocator.free(data);

    for (0..data_size / 4) |i| {
        std.mem.writeInt(u32, data[i * 4 ..][0..4], @intCast(i), .little);
    }

    const start = Io.Clock.Timestamp.now(io, .boot);

    const result = try compression.compress(allocator, data, .ByteGrouping4LZ4);
    defer allocator.free(result.data);

    const duration = elapsedNs(start, io);

    std.mem.doNotOptimizeAway(&result);

    return .{
        .name = "ByteGrouping4LZ4 (50 MB)",
        .duration_ns = duration,
        .throughput_mbs = throughputMbs(data_size, duration),
        .iterations = 1,
    };
}

pub fn benchmarkXorbSerialization(allocator: std.mem.Allocator, io: Io) !BenchmarkResult {
    const data_size = 50 * 1024 * 1024;
    const data = try generateRandomData(allocator, data_size, 11111);
    defer allocator.free(data);

    var boundaries = try chunking.chunkBuffer(allocator, data);
    defer boundaries.deinit(allocator);

    var builder = xorb.XorbBuilder.init(allocator);
    defer builder.deinit();

    for (boundaries.items) |boundary| {
        const chunk = data[boundary.start..boundary.end];
        _ = try builder.addChunk(chunk);
    }

    const start = Io.Clock.Timestamp.now(io, .boot);

    const serialized = try builder.serialize(.None);
    defer allocator.free(serialized);

    const duration = elapsedNs(start, io);

    std.mem.doNotOptimizeAway(&serialized);

    return .{
        .name = "Xorb Serialization (50 MB)",
        .duration_ns = duration,
        .throughput_mbs = throughputMbs(data_size, duration),
        .iterations = 1,
    };
}

pub fn benchmarkEndToEnd(allocator: std.mem.Allocator, io: Io) !BenchmarkResult {
    const data_size = 50 * 1024 * 1024;
    const data = try generateRandomData(allocator, data_size, 99999);
    defer allocator.free(data);

    const start = Io.Clock.Timestamp.now(io, .boot);

    var boundaries = try chunking.chunkBuffer(allocator, data);
    defer boundaries.deinit(allocator);

    var chunk_infos: std.ArrayList(hashing.MerkleNode) = .empty;
    defer chunk_infos.deinit(allocator);

    for (boundaries.items) |boundary| {
        const chunk = data[boundary.start..boundary.end];
        const chunk_hash = hashing.computeDataHash(chunk);
        try chunk_infos.append(allocator, .{
            .hash = chunk_hash,
            .size = chunk.len,
        });
    }

    const merkle_root = try hashing.buildMerkleTree(allocator, chunk_infos.items);
    const file_hash = hashing.computeFileHash(merkle_root);

    const duration = elapsedNs(start, io);

    std.mem.doNotOptimizeAway(&file_hash);

    return .{
        .name = "End-to-End (50 MB)",
        .duration_ns = duration,
        .throughput_mbs = throughputMbs(data_size, duration),
        .iterations = 1,
    };
}

pub fn benchmarkEndToEndLarge(allocator: std.mem.Allocator, io: Io) !BenchmarkResult {
    const data_size = 1024 * 1024 * 1024;
    std.debug.print("Allocating 1 GB for benchmark...\n", .{});
    const data = try generateRandomData(allocator, data_size, 99999);
    defer allocator.free(data);

    std.debug.print("Starting end-to-end benchmark...\n", .{});
    const start = Io.Clock.Timestamp.now(io, .boot);

    var boundaries = try chunking.chunkBuffer(allocator, data);
    defer boundaries.deinit(allocator);

    var chunk_infos: std.ArrayList(hashing.MerkleNode) = .empty;
    defer chunk_infos.deinit(allocator);

    for (boundaries.items) |boundary| {
        const chunk = data[boundary.start..boundary.end];
        const chunk_hash = hashing.computeDataHash(chunk);
        try chunk_infos.append(allocator, .{
            .hash = chunk_hash,
            .size = chunk.len,
        });
    }

    const merkle_root = try hashing.buildMerkleTree(allocator, chunk_infos.items);
    const file_hash = hashing.computeFileHash(merkle_root);

    const duration = elapsedNs(start, io);

    std.mem.doNotOptimizeAway(&file_hash);

    return .{
        .name = "End-to-End (1 GB)",
        .duration_ns = duration,
        .throughput_mbs = throughputMbs(data_size, duration),
        .iterations = 1,
    };
}

pub fn runAllBenchmarks(allocator: std.mem.Allocator, io: Io) !void {
    std.debug.print("\n=== XET Protocol Performance Benchmarks ===\n\n", .{});

    const BenchFn = *const fn (std.mem.Allocator, Io) anyerror!BenchmarkResult;
    const benchmarks = [_]struct {
        name: []const u8,
        func: BenchFn,
    }{
        .{ .name = "Chunking", .func = benchmarkChunking },
        .{ .name = "Hashing", .func = benchmarkHashing },
        .{ .name = "Merkle Tree", .func = benchmarkMerkleTree },
        .{ .name = "LZ4 Compression", .func = benchmarkLZ4Compression },
        .{ .name = "LZ4 Decompression", .func = benchmarkLZ4Decompression },
        .{ .name = "ByteGrouping4LZ4", .func = benchmarkByteGrouping4LZ4 },
        .{ .name = "Xorb Serialization", .func = benchmarkXorbSerialization },
        .{ .name = "End-to-End", .func = benchmarkEndToEnd },
        .{ .name = "End-to-End Large", .func = benchmarkEndToEndLarge },
    };

    for (benchmarks) |bench| {
        const result = try bench.func(allocator, io);
        printResult(result);
    }

    std.debug.print("\n=== Benchmarks Complete ===\n", .{});
}

test "benchmark smoke test" {
    const allocator = std.testing.allocator;
    const io = std.Io.Threaded.global_single_threaded.io();
    _ = try benchmarkChunking(allocator, io);
}
