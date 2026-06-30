//! Terminal progress bar shared by the download examples.
//!
//! Renders a single line to stderr that updates in place as a download
//! advances, plugging into the library through `xet.progress.ProgressCallback`.
//! The callback only carries byte counts, so all of the rendering lives here in
//! the example rather than in the library.

const std = @import("std");
const xet = @import("xet");

pub const Bar = struct {
    io: std.Io,
    start: std.Io.Clock.Timestamp,
    last_permille: i64 = -1,

    pub fn init(io: std.Io) Bar {
        return .{
            .io = io,
            .start = std.Io.Clock.Timestamp.now(io, .boot),
        };
    }

    /// A progress callback bound to this bar, suitable for `DownloadConfig.progress`.
    pub fn callback(self: *Bar) xet.progress.ProgressCallback {
        return .{ .context = self, .func = onProgress };
    }

    /// Move to a fresh line once the download is done, but only if a bar was
    /// actually drawn (empty files never report any progress).
    pub fn finish(self: *Bar) void {
        if (self.last_permille >= 0) {
            self.last_permille = -1;
            std.debug.print("\n", .{});
        }
    }

    fn onProgress(context: *anyopaque, completed: u64, total: u64) void {
        const self: *Bar = @ptrCast(@alignCast(context));
        if (total == 0) return;

        const permille: i64 = @intCast(completed * 1000 / total);
        if (permille == self.last_permille) return;
        self.last_permille = permille;

        const width = 30;
        const filled: usize = @intCast(completed * width / total);
        var bar: [width]u8 = undefined;
        @memset(bar[0..filled], '#');
        @memset(bar[filled..], '-');

        const elapsed = self.start.untilNow(self.io);
        const elapsed_ns: i96 = elapsed.raw.nanoseconds;
        const elapsed_s = @as(f64, @floatFromInt(elapsed_ns)) / @as(f64, std.time.ns_per_s);
        const done_mb = @as(f64, @floatFromInt(completed)) / (1024.0 * 1024.0);
        const total_mb = @as(f64, @floatFromInt(total)) / (1024.0 * 1024.0);
        const speed = if (elapsed_s > 0) done_mb / elapsed_s else 0;

        std.debug.print("\r  [{s}] {d:>5.1}%  {d:.1}/{d:.1} MB  {d:.1} MB/s   ", .{
            bar[0..],
            @as(f64, @floatFromInt(permille)) / 10.0,
            done_mb,
            total_mb,
            speed,
        });
    }
};
