const std = @import("std");

/// Optional callback invoked during a download to report incremental progress.
///
/// `completed` and `total` are byte counts, where `total` is the full size of
/// the file being reconstructed (the sum of all reconstruction term lengths).
/// On the parallel download path the callback may be invoked from worker
/// threads, but the fetcher serializes those calls so it never runs
/// concurrently and `completed` never decreases between calls.
pub const ProgressCallback = struct {
    context: *anyopaque,
    func: *const fn (context: *anyopaque, completed: u64, total: u64) void,

    pub fn report(self: ProgressCallback, completed: u64, total: u64) void {
        self.func(self.context, completed, total);
    }
};
