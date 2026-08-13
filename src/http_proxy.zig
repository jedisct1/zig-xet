//! HTTP client with proxy support.
//!
//! The standard environment variables select the proxy: `https_proxy`,
//! `http_proxy` and `all_proxy`, in lower or upper case. Credentials in
//! the proxy URL are sent as Basic proxy authorization.
//!
//! HTTPS requests go through a CONNECT tunnel, with TLS running end to
//! end so the proxy never sees plaintext. `std.http.Client` cannot do
//! this by itself, so each tunnel is relayed through a loopback listener
//! where the client performs its normal TLS handshake, validated against
//! the target host.
//!
//! Known limitations:
//!
//! - Requests that follow an HTTP redirect bypass the proxy.
//! - Proxied HTTPS needs a concurrent `std.Io` implementation and fails
//!   with `error.ConcurrencyUnavailable` otherwise.
//! - A local process could race the relay for its single connection and
//!   break a download. TLS stays end to end, so it learns nothing.

const std = @import("std");
const Allocator = std.mem.Allocator;
const HostName = std.Io.net.HostName;

const relay_ip = "127.0.0.1";
const max_connect_response_len = 8 * 1024;

/// An HTTP client that honors the proxy environment variables.
///
/// Create requests with `request`; everything else behaves like
/// `std.http.Client`, including the rule that the address must stay
/// stable once requests are made.
pub const Client = struct {
    io: std.Io,
    http_client: std.http.Client,
    proxy_arena: std.heap.ArenaAllocator,
    https_proxy: ?*std.http.Client.Proxy,
    tunnels: std.Io.Group,

    pub fn init(allocator: Allocator, io: std.Io, environ: std.process.Environ) !Client {
        var self = Client{
            .io = io,
            .http_client = .{ .allocator = allocator, .io = io },
            .proxy_arena = std.heap.ArenaAllocator.init(allocator),
            .https_proxy = null,
            .tunnels = .init,
        };
        errdefer self.proxy_arena.deinit();

        var env_map = try environ.createMap(allocator);
        defer env_map.deinit();
        try self.http_client.initDefaultProxies(self.proxy_arena.allocator(), &env_map);

        // std.http.Client's own HTTPS tunneling would send requests in
        // plaintext, so the wrapper takes it over. Plain HTTP proxying
        // works upstream and stays on the client.
        self.https_proxy = self.http_client.https_proxy;
        self.http_client.https_proxy = null;
        return self;
    }

    pub fn deinit(self: *Client) void {
        self.http_client.deinit();
        self.tunnels.cancel(self.io);
        self.proxy_arena.deinit();
    }

    /// Create a request, routed through the HTTPS proxy when one is set.
    /// Same contract as `std.http.Client.request`.
    pub fn request(
        self: *Client,
        method: std.http.Method,
        uri: std.Uri,
        options: std.http.Client.RequestOptions,
    ) !std.http.Client.Request {
        tunnel: {
            const proxy = self.https_proxy orelse break :tunnel;
            if (options.connection != null) break :tunnel;
            const protocol = std.http.Client.Protocol.fromUri(uri) orelse break :tunnel;
            if (protocol != .tls) break :tunnel;
            if (proxy.protocol != .plain) return error.TlsProxyUnsupported;

            var host_buffer: [HostName.max_len]u8 = undefined;
            const host = HostName.fromUri(uri, &host_buffer) catch break :tunnel;
            const port = uri.port orelse 443;

            const connection = try self.connectThroughProxy(proxy, host, port);
            var tunneled_options = options;
            tunneled_options.connection = connection;
            return self.http_client.request(method, uri, tunneled_options) catch |err| {
                connection.closing = true;
                self.http_client.connection_pool.release(connection, self.io);
                return err;
            };
        }
        return self.http_client.request(method, uri, options);
    }

    /// Get a TLS connection to `host:port` through the proxy, pooled or
    /// freshly tunneled.
    fn connectThroughProxy(
        self: *Client,
        proxy: *std.http.Client.Proxy,
        host: HostName,
        port: u16,
    ) !*std.http.Client.Connection {
        const client = &self.http_client;
        const io = self.io;

        if (try client.connection_pool.findConnection(io, .{
            .host = host,
            .port = port,
            .protocol = .tls,
        })) |connection| return connection;

        try self.loadTlsPrerequisites();

        const relay_port = try self.startTunnel(proxy, host, port);

        // Ensures the single-use relay never stays blocked in accept,
        // whichever way the connection attempt goes.
        defer pokeRelay(io, relay_port);

        return client.connectTcpOptions(.{
            .host = HostName.init(relay_ip) catch unreachable,
            .port = relay_port,
            .protocol = .tls,
            .proxied_host = host,
            .proxied_port = port,
        });
    }

    /// Open a CONNECT tunnel through the proxy and hand it to a relay
    /// task. Returns the loopback port the relay listens on.
    fn startTunnel(
        self: *Client,
        proxy: *std.http.Client.Proxy,
        host: HostName,
        port: u16,
    ) !u16 {
        const gpa = self.http_client.allocator;
        const io = self.io;

        const proxy_stream = try proxy.host.connect(io, proxy.port, .{ .mode = .stream });
        errdefer proxy_stream.close(io);

        try connectHandshake(io, proxy_stream, host, port, proxy.authorization);

        const loopback = std.Io.net.IpAddress.parse(relay_ip, 0) catch unreachable;
        var server = try loopback.listen(io, .{});
        errdefer server.deinit(io);
        const relay_port = server.socket.address.getPort();

        const tunnel = try gpa.create(Tunnel);
        errdefer gpa.destroy(tunnel);
        tunnel.* = .{
            .gpa = gpa,
            .io = io,
            .server = server,
            .proxy_stream = proxy_stream,
        };
        try self.tunnels.concurrent(io, Tunnel.run, .{tunnel});
        return relay_port;
    }

    /// Prepare the certificate bundle and clock that `std.http.Client`
    /// normally sets up lazily; the tunnel needs them earlier.
    fn loadTlsPrerequisites(self: *Client) !void {
        const client = &self.http_client;
        const io = self.io;
        {
            try client.ca_bundle_lock.lockShared(io);
            defer client.ca_bundle_lock.unlockShared(io);
            if (client.now != null) return;
        }
        var bundle: std.crypto.Certificate.Bundle = .empty;
        defer bundle.deinit(client.allocator);
        const now = std.Io.Clock.real.now(io);
        bundle.rescan(client.allocator, io, now) catch |err| switch (err) {
            error.Canceled => |e| return e,
            else => return error.CertificateBundleLoadFailure,
        };
        try client.ca_bundle_lock.lock(io);
        defer client.ca_bundle_lock.unlock(io);
        client.now = now;
        std.mem.swap(std.crypto.Certificate.Bundle, &client.ca_bundle, &bundle);
    }
};

/// A single-use CONNECT tunnel relaying bytes between one loopback
/// connection and the proxy. Frees itself when done.
const Tunnel = struct {
    gpa: Allocator,
    io: std.Io,
    server: std.Io.net.Server,
    proxy_stream: std.Io.net.Stream,
    to_proxy_buffer: [buffer_len]u8 = undefined,
    to_client_buffer: [buffer_len]u8 = undefined,

    const buffer_len = 16 * 1024;

    fn run(tunnel: *Tunnel) void {
        const io = tunnel.io;
        defer tunnel.gpa.destroy(tunnel);

        const client_stream = tunnel.server.accept(io) catch {
            tunnel.server.deinit(io);
            tunnel.proxy_stream.close(io);
            return;
        };

        tunnel.server.deinit(io);
        defer client_stream.close(io);
        defer tunnel.proxy_stream.close(io);

        var directions: std.Io.Group = .init;
        directions.concurrent(io, pump, .{
            io,
            client_stream,
            tunnel.proxy_stream,
            @as([]u8, &tunnel.to_proxy_buffer),
        }) catch return;
        pump(io, tunnel.proxy_stream, client_stream, &tunnel.to_client_buffer);
        directions.await(io) catch unreachable;
    }
};

/// Relay one direction of a tunnel, half-closing `dst` at the end of
/// stream. Nothing is buffered: held-back bytes would stall the TLS
/// handshake.
fn pump(io: std.Io, src: std.Io.net.Stream, dst: std.Io.net.Stream, buffer: []u8) void {
    var writer = dst.writer(io, &.{});
    while (true) {
        var slices: [1][]u8 = .{buffer};
        const len = src.read(io, &slices) catch break;
        if (len == 0) break;
        writer.interface.writeAll(buffer[0..len]) catch break;
    }
    dst.shutdown(io, .send) catch {};
}

/// Connect to the relay and hang up, just to unblock its accept call.
fn pokeRelay(io: std.Io, port: u16) void {
    const address = std.Io.net.IpAddress.parse(relay_ip, port) catch unreachable;
    const stream = address.connect(io, .{ .mode = .stream }) catch return;
    stream.close(io);
}

/// Ask the proxy for a tunnel to `host:port` and wait for its approval.
fn connectHandshake(
    io: std.Io,
    stream: std.Io.net.Stream,
    host: HostName,
    port: u16,
    authorization: ?[]const u8,
) !void {
    var write_buffer: [1024]u8 = undefined;
    var stream_writer = stream.writer(io, &write_buffer);
    const w = &stream_writer.interface;
    w.print("CONNECT {s}:{d} HTTP/1.1\r\nhost: {s}:{d}\r\n", .{ host.bytes, port, host.bytes, port }) catch
        return error.ProxyConnectFailed;
    if (authorization) |auth| {
        w.print("proxy-authorization: {s}\r\n", .{auth}) catch return error.ProxyConnectFailed;
    }
    w.writeAll("\r\n") catch return error.ProxyConnectFailed;
    w.flush() catch return error.ProxyConnectFailed;

    // One byte at a time, so no bytes of the tunneled TLS session get
    // consumed by accident.
    var read_buffer: [1]u8 = undefined;
    var stream_reader = stream.reader(io, &read_buffer);
    const r = &stream_reader.interface;

    var status_buffer: [128]u8 = undefined;
    var status_len: usize = 0;
    while (true) {
        const byte = r.takeByte() catch return error.ProxyConnectFailed;
        if (byte == '\n') break;
        if (status_len == status_buffer.len) return error.ProxyConnectFailed;
        status_buffer[status_len] = byte;
        status_len += 1;
    }
    const status_line = std.mem.trimEnd(u8, status_buffer[0..status_len], "\r");
    var words = std.mem.splitScalar(u8, status_line, ' ');
    _ = words.next();
    const code = words.next() orelse return error.ProxyConnectFailed;
    if (!std.mem.eql(u8, code, "200")) return error.ProxyConnectRefused;

    // Skip the rest of the response head.
    var line_len: usize = 0;
    var total_len: usize = status_len;
    while (true) {
        const byte = r.takeByte() catch return error.ProxyConnectFailed;
        total_len += 1;
        if (total_len > max_connect_response_len) return error.ProxyConnectFailed;
        switch (byte) {
            '\n' => {
                if (line_len == 0) break;
                line_len = 0;
            },
            '\r' => {},
            else => line_len += 1,
        }
    }
}

test "empty environment leaves the client without proxies" {
    const io = std.Io.Threaded.global_single_threaded.io();

    var client = try Client.init(std.testing.allocator, io, .empty);
    defer client.deinit();

    try std.testing.expect(client.http_client.http_proxy == null);
    try std.testing.expect(client.https_proxy == null);
}

test "https proxy settings are read from the environment" {
    if (std.process.Environ.Block != std.process.Environ.PosixBlock) return error.SkipZigTest;

    const io = std.Io.Threaded.global_single_threaded.io();

    const entries = [_:null]?[*:0]const u8{"https_proxy=http://user:secret@proxy.example.com:3128"};
    const environ: std.process.Environ = .{ .block = .{ .slice = &entries } };

    var client = try Client.init(std.testing.allocator, io, environ);
    defer client.deinit();

    try std.testing.expect(client.http_client.http_proxy == null);

    // The setting must move off the inner client, which would misuse it.
    try std.testing.expect(client.http_client.https_proxy == null);
    const proxy = client.https_proxy orelse return error.TestExpectedProxy;
    try std.testing.expectEqualStrings("proxy.example.com", proxy.host.bytes);
    try std.testing.expectEqual(3128, proxy.port);
    try std.testing.expect(proxy.authorization != null);
}

test "plain http proxy settings stay on the inner client" {
    if (std.process.Environ.Block != std.process.Environ.PosixBlock) return error.SkipZigTest;

    const io = std.Io.Threaded.global_single_threaded.io();

    const entries = [_:null]?[*:0]const u8{"http_proxy=http://proxy.example.com:8080"};
    const environ: std.process.Environ = .{ .block = .{ .slice = &entries } };

    var client = try Client.init(std.testing.allocator, io, environ);
    defer client.deinit();

    try std.testing.expect(client.https_proxy == null);
    const proxy = client.http_client.http_proxy orelse return error.TestExpectedProxy;
    try std.testing.expectEqualStrings("proxy.example.com", proxy.host.bytes);
    try std.testing.expectEqual(8080, proxy.port);
}
