//! XET Protocol Implementation in Zig
//!
//! This library implements the XET protocol for content-addressed data handling
//! through chunking, hashing, and deduplication.
//!
//! Main modules:
//! - constants: Protocol constants (Gearhash TABLE, BLAKE3 keys, etc.)
//! - chunking: Gearhash content-defined chunking (CDC)
//! - hashing: BLAKE3 hashing with 4 types + Merkle trees
//! - compression: LZ4 and ByteGrouping4LZ4 compression
//! - xorb: Xorb format serialization/deserialization
//! - shard: MDB shard format I/O
//! - cas_client: HTTP CAS API client (not available on WASM)
//! - reconstruction: File reconstruction from terms (limited on WASM - no parallel operations)
//! - model_download: High-level API for downloading models from Hugging Face (not available on WASM)

const std = @import("std");
const builtin = @import("builtin");

// Export core modules (always available)
pub const constants = @import("constants.zig");
pub const chunking = @import("chunking.zig");
pub const hashing = @import("hashing.zig");
pub const compression = @import("compression.zig");
pub const xorb = @import("xorb.zig");
pub const shard = @import("shard.zig");
pub const benchmark = @import("benchmark.zig");

// Export reconstruction (always available, but with limited functionality on WASM)
pub const reconstruction = @import("reconstruction.zig");

// Export network/threading modules only on non-WASM targets
pub const has_network_support = builtin.target.os.tag != .wasi;

pub const cas_client = if (has_network_support) @import("cas_client.zig") else struct {};
pub const model_download = if (has_network_support) @import("model_download.zig") else struct {};
pub const parallel_fetcher = if (has_network_support) @import("parallel_fetcher.zig") else struct {};
pub const upload = if (has_network_support) @import("upload.zig") else struct {};
pub const bucket_api = if (has_network_support) @import("bucket_api.zig") else struct {};

test {
    std.testing.refAllDecls(@This());
    _ = @import("verification_test.zig");
    _ = @import("bg4_verification_test.zig");
}
