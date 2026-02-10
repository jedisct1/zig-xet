const std = @import("std");
const zig_xet = @import("xet");

pub fn main(init: std.process.Init) !void {
    try zig_xet.benchmark.runAllBenchmarks(init.gpa, init.io);
}
