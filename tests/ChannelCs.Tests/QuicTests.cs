using System.Diagnostics;
using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Text;
using System.Text.Json;
using ChannelCs.Client;
using ChannelCs.Server;

namespace ChannelCs.Tests;

using ServerState = Dictionary<string, string?>;
using ClientState = Dictionary<string, object?>;

/// <summary>The TypeScript reference's QUIC test certificate (localhost, 127.0.0.1 and ::1) and its fingerprint.</summary>
internal static class QuicTestFiles
{
    public const string Fingerprint =
        "DD:D2:37:00:BD:8A:BD:29:14:15:D8:65:84:BE:C2:80:10:FF:21:DA:A1:C5:9D:B3:4A:7F:36:63:EF:4B:55:9B";

    /// <summary>The channel-cs checkout.</summary>
    public static string Root
    {
        get
        {
            var directory = new DirectoryInfo(AppContext.BaseDirectory);
            while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "ChannelCs.slnx")))
            {
                directory = directory.Parent;
            }

            return directory?.FullName ?? throw new InvalidOperationException("channel-cs checkout not found");
        }
    }

    public static string Channel => Path.GetFullPath(Path.Combine(Root, "..", "channel"));

    public static string CertPath => Path.Combine(Channel, "tests", "quic", "cert.pem");

    public static string Cert => File.ReadAllText(CertPath);

    public static string Key => File.ReadAllText(Path.Combine(Channel, "tests", "quic", "key.pem"));

    public static void RequireQuic()
    {
        Assert.True(QuicListener.IsSupported && QuicConnection.IsSupported,
            "System.Net.Quic isn't supported here: install libmsquic (HubTools' container has it)");
    }
}

public sealed class QuicFramingTests
{
    [Fact]
    public async Task Frames_Use_Varint_Lengths_And_Skip_Keepalives()
    {
        foreach (var size in new[] { 0, 1, 63, 64, 16383, 16384, 70000 })
        {
            var payload = Encoding.ASCII.GetBytes(new string('x', size));
            var frame = QuicFraming.Encode(payload);
            var expected = size < 64 ? 1 : size < 16384 ? 2 : 4;
            Assert.Equal(expected, frame.Length - size);

            // A keepalive in front is skipped
            var bytes = new byte[] { 0 }.Concat(frame).Concat(QuicFraming.Encode("next"u8)).ToArray();
            var reader = new QuicFrameReader(new MemoryStream(bytes));
            if (size > 0)
            {
                Assert.Equal(payload, await reader.ReadAsync());
            }

            Assert.Equal("next", Encoding.ASCII.GetString((await reader.ReadAsync())!));
            Assert.Null(await reader.ReadAsync());
        }
    }

    [Fact]
    public async Task Frames_Decode_8_Byte_Lengths_And_Refuse_Oversize()
    {
        var big = new byte[] { 0xc0, 0, 0, 0, 0, 0, 0, 5, (byte)'h', (byte)'e', (byte)'l', (byte)'l', (byte)'o' };
        Assert.Equal("hello", Encoding.ASCII.GetString((await new QuicFrameReader(new MemoryStream(big)).ReadAsync())!));

        var header = new byte[8];
        Assert.Equal(8, QuicFraming.WriteHeader(header, 1L << 30));
        Assert.Equal(0xc0, header[0]);

        var over = new QuicFrameReader(new MemoryStream(QuicFraming.Encode(new byte[11])), 10);
        await Assert.ThrowsAsync<QuicFrameTooLargeException>(async () => await over.ReadAsync());
        // 16 MiB + 1 is above the default limit
        var limit = new QuicFrameReader(new MemoryStream([0x81, 0, 0, 1]));
        await Assert.ThrowsAsync<QuicFrameTooLargeException>(async () => await limit.ReadAsync());
    }

    [Fact]
    public async Task Frames_Cut_Short_Throw()
    {
        var frame = QuicFraming.Encode("hello"u8);
        await Assert.ThrowsAsync<EndOfStreamException>(async () =>
            await new QuicFrameReader(new MemoryStream(frame[..3])).ReadAsync());
        await Assert.ThrowsAsync<EndOfStreamException>(async () =>
            await new QuicFrameReader(new MemoryStream([0x40])).ReadAsync());
    }

    [Fact]
    public async Task Writer_Writes_Frames_In_Order_And_Stops_When_Closed()
    {
        var stream = new MemoryStream();
        var writer = new QuicFrameWriter(stream);
        for (var i = 0; i < 100; i += 1)
        {
            Assert.True(writer.Write(Encoding.ASCII.GetBytes(i.ToString())));
        }

        Assert.True(writer.Write([]));
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (stream.Length < 291 && DateTime.UtcNow < deadline)
        {
            await Task.Delay(5);
        }

        var reader = new QuicFrameReader(new MemoryStream(stream.ToArray()));
        for (var i = 0; i < 100; i += 1)
        {
            Assert.Equal(i.ToString(), Encoding.ASCII.GetString((await reader.ReadAsync())!));
        }

        Assert.Null(await reader.ReadAsync());
        writer.Close();
        Assert.False(writer.Write("late"u8));
    }

    [Fact]
    public void Hello_Holds_Headers_And_Server_Lowercases_Them()
    {
        var hello = QuicClientTransport.Hello(new Dictionary<string, string> { ["X-Name"] = "cs", ["Empty"] = "" });
        Assert.Equal("""{"hello":{"X-Name":"cs"}}""", Encoding.UTF8.GetString(hello));

        var headers = ParseHello("""{"hello":{"X-Name":"cs","ignored":1}}""");
        Assert.Equal("cs", headers!["x-name"]);
        Assert.Single(headers);
        Assert.Null(ParseHello("""{"id":1,"path":"hello"}"""));
        Assert.Null(ParseHello("""{"hello":"x"}"""));
        Assert.Null(ParseHello("[]"));
        Assert.ThrowsAny<JsonException>(() => ParseHello("{nope"));
    }

    [Fact]
    public void Listen_Address_Forms()
    {
        Assert.Equal(new IPEndPoint(IPAddress.Loopback, 5), ChannelQuicServerExtensions.ParseEndPoint("5"));
        Assert.Equal(new IPEndPoint(IPAddress.Loopback, 5), ChannelQuicServerExtensions.ParseEndPoint("localhost:5"));
        Assert.Equal(new IPEndPoint(IPAddress.Any, 5), ChannelQuicServerExtensions.ParseEndPoint("0.0.0.0:5"));
        Assert.Equal(new IPEndPoint(IPAddress.IPv6Loopback, 5), ChannelQuicServerExtensions.ParseEndPoint("[::1]:5"));
        Assert.Throws<ArgumentException>(() => ChannelQuicServerExtensions.ParseEndPoint("example.com:5"));
        Assert.Throws<ArgumentException>(() => ChannelQuicServerExtensions.ParseEndPoint("127.0.0.1:x"));
    }

    private static Dictionary<string, string?>? ParseHello(string json) =>
        QuicChannelServer<ServerState>.ParseHello(Encoding.UTF8.GetBytes(json));
}

/// <summary>QUIC client and server of this port against each other and the TypeScript reference's client.</summary>
public sealed class QuicTransportTests
{
    private sealed class Fixture : IAsyncDisposable
    {
        public readonly Subscription Ticks = new();
        public int Connected;
        public int Disconnected;
        public int Dropped;

        public QuicChannelServer<ServerState> Server { get; private set; } = null!;

        public static Fixture Listen(string address)
        {
            QuicTestFiles.RequireQuic();
            var fixture = new Fixture();
            fixture.Start(address);
            return fixture;
        }

        public void Start(string address)
        {
            Server = ServerChannel().ListenQuic(address, new QuicListenOptions<ServerState>
            {
                Cert = QuicTestFiles.Cert,
                Key = QuicTestFiles.Key,
                StateFactory = headers => ValueTask.FromResult(new ServerState(headers)),
                OnConnect = _ => Interlocked.Increment(ref Connected),
                OnDisconnect = _ => Interlocked.Increment(ref Disconnected),
            });
        }

        public string Address => $"quic://127.0.0.1:{Server.Port}";

        private Channel<ServerState> ServerChannel()
        {
            ChannelPostHandler<ServerState> mirror = async request => await request.Sender.SendAsync<string>("hello");
            ChannelPostHandler<ServerState> slow = async _ =>
            {
                await Task.Delay(300);
                return "slow";
            };
            return new Channel<ServerState>()
                .Post("hello", _ => "world")
                .Post("echo", request => request.BodyRaw)
                .Post("name", request => request.State.GetValueOrDefault("x-name"))
                .Post("mirror", mirror)
                .Post("slow", slow)
                // Once: the client sends unanswered requests again after reconnecting
                .Post("disconnect", request =>
                {
                    if (Interlocked.Exchange(ref Dropped, 1) == 0)
                    {
                        request.Sender.Stop();
                    }

                    return "dropped before";
                })
                .Stream("count", _ => Count(3))
                .Post("tick", _ =>
                {
                    Ticks.Send("a", "tick");
                    return (object?)null;
                })
                .Events(new Dictionary<string, Subscription> { ["ticks"] = Ticks });
        }

        public async ValueTask DisposeAsync()
        {
            await Server.StopAsync();
        }
    }

    private static async IAsyncEnumerable<object?> Count(int count)
    {
        for (var i = 0; i < count; i += 1)
        {
            yield return i;
            await Task.Yield();
        }
    }

    private static ClientSender<ClientState> Connect(string address, QuicClientOptions quic, IReadOnlyDictionary<string, string>? headers = null)
    {
        return new Channel<ClientState>()
            .Post("hello", _ => "client world")
            .Connect(address, new ClientOptions<ClientState>
            {
                Quic = quic,
                Headers = headers is null ? null : () => ValueTask.FromResult(headers),
            });
    }

    private static async Task WaitFor(Func<bool> ready, int seconds = 5)
    {
        var deadline = DateTime.UtcNow.AddSeconds(seconds);
        while (!ready())
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException("timed out");
            }

            await Task.Delay(5);
        }
    }

    [Fact(Timeout = 30000)]
    public async Task Client_And_Server()
    {
        await using var fixture = Fixture.Listen("127.0.0.1:0");
        using var client = Connect(fixture.Address, new QuicClientOptions { Ca = QuicTestFiles.Cert },
            new Dictionary<string, string> { ["X-Name"] = "cs" });

        // post
        Assert.Equal("world", await client.SendAsync<string>("hello"));
        var text = new string('é', 20000);
        var echoed = await client.SendAsync<JsonElement>("echo", new { list = new object?[] { 1, "two", null }, text });
        Assert.Equal(text, echoed.GetProperty("text").GetString());
        Assert.Equal("""[1,"two",null]""", echoed.GetProperty("list").GetRawText());

        // hello headers become the state, names lowercased
        Assert.Equal("cs", await client.SendAsync<string>("name"));

        // the server calls the client back
        Assert.Equal("client world", await client.SendAsync<string>("mirror"));

        // stream
        var values = new List<int>();
        await foreach (var value in client.Values<int>("count"))
        {
            values.Add(value);
        }

        Assert.Equal([0, 1, 2], values);

        // events
        var count = 0;
        var subscription = await client.SubscribeAsync("ticks", "a", _ => Interlocked.Increment(ref count));
        fixture.Ticks.Send("a", "tick");
        fixture.Ticks.Send("b", "not for us");
        fixture.Ticks.Send("a", "tick");
        await WaitFor(() => Volatile.Read(ref count) == 2);
        subscription.Cancel();
        await Task.Delay(20);
        fixture.Ticks.Send("a", "after cancel");
        await Task.Delay(50);
        Assert.Equal(2, Volatile.Read(ref count));

        // many in flight
        var requests = Enumerable.Range(0, 2000).Select(i => client.Request<int>("echo", i)).ToArray();
        for (var i = 0; i < requests.Length; i += 1)
        {
            Assert.Equal(i, await requests[i].Response);
        }
    }

    [Fact(Timeout = 30000)]
    public async Task Certificate_Trust()
    {
        await using var fixture = Fixture.Listen("127.0.0.1:0");

        using var pinned = Connect(fixture.Address, new QuicClientOptions
        {
            Fingerprint = QuicTestFiles.Fingerprint.Replace(":", "").ToLowerInvariant(),
        });
        Assert.Equal("world", await pinned.SendAsync<string>("hello"));
        using var byName = Connect(fixture.Address, new QuicClientOptions { Ca = QuicTestFiles.Cert, ServerName = "localhost" });
        Assert.Equal("world", await byName.SendAsync<string>("hello"));

        var refused = new Dictionary<string, QuicClientOptions>
        {
            ["system roots"] = new(),
            ["wrong pin"] = new() { Fingerprint = string.Concat(Enumerable.Repeat("00", 32)) },
            ["name not in cert"] = new() { Ca = QuicTestFiles.Cert, ServerName = "example.com" },
        };
        foreach (var (name, options) in refused)
        {
            using var client = Connect(fixture.Address, options);
            await Task.Delay(500);
            Assert.False(client.WebSocket.IsConnected, name);
        }

        Assert.Throws<ArgumentException>(() => Connect(fixture.Address, new QuicClientOptions { Fingerprint = "nope" }));
        Assert.Throws<ArgumentException>(() => Connect(fixture.Address, new QuicClientOptions { Ca = "not a certificate" }));
    }

    /// <summary>Dials the server without the Channel client, to check the wire rules.</summary>
    private static async Task<(QuicConnection Connection, QuicStream Stream)> RawSession(Fixture fixture)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var connection = await QuicConnection.ConnectAsync(new QuicClientConnectionOptions
        {
            RemoteEndPoint = new IPEndPoint(IPAddress.Loopback, fixture.Server.Port),
            DefaultStreamErrorCode = 0,
            DefaultCloseErrorCode = 0,
            ClientAuthenticationOptions = new SslClientAuthenticationOptions
            {
                ApplicationProtocols = [new SslApplicationProtocol(QuicFraming.Alpn)],
                TargetHost = "localhost",
                RemoteCertificateValidationCallback = (_, _, _, _) => true,
            },
        }, timeout.Token);
        var stream = await connection.OpenOutboundStreamAsync(QuicStreamType.Bidirectional, timeout.Token);
        return (connection, stream);
    }

    private static byte[] Hello(object headers) =>
        QuicFraming.Encode(JsonSerializer.SerializeToUtf8Bytes(new { hello = headers }));

    /// <summary>True when the server closes the connection within 2 s.</summary>
    private static async Task<bool> Closed(QuicStream stream)
    {
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(2));
        var buffer = new byte[1024];
        try
        {
            while (await stream.ReadAsync(buffer, timeout.Token) > 0)
            {
            }

            return true;
        }
        catch (QuicException)
        {
            return true;
        }
        catch (OperationCanceledException)
        {
            return false;
        }
    }

    [Fact(Timeout = 30000)]
    public async Task Wire_Raw_Frames()
    {
        await using var fixture = Fixture.Listen("127.0.0.1:0");
        var (connection, stream) = await RawSession(fixture);
        await using var _ = connection;
        var frames = Hello(new Dictionary<string, object> { ["X-Name"] = "raw", ["ignored"] = 1 })
            .Append((byte)0) // keepalive
            .Concat(QuicFraming.Encode("""[{"id":1,"path":"name"},{"id":2,"path":"hello"}]"""u8))
            .ToArray();
        await stream.WriteAsync(frames);

        var reader = new QuicFrameReader(stream);
        var got = new Dictionary<int, string?>();
        while (got.Count < 2)
        {
            using var message = JsonDocument.Parse((await reader.ReadAsync())!);
            got[message.RootElement.GetProperty("id").GetInt32()] = message.RootElement.GetProperty("body").GetString();
        }

        Assert.Equal("raw", got[1]);
        Assert.Equal("world", got[2]);
    }

    [Fact(Timeout = 30000)]
    public async Task Wire_One_Stream_Per_Connection()
    {
        await using var fixture = Fixture.Listen("127.0.0.1:0");
        var (connection, stream) = await RawSession(fixture);
        await using var _ = connection;
        await stream.WriteAsync(Hello(new { }));

        foreach (var type in new[] { QuicStreamType.Bidirectional, QuicStreamType.Unidirectional })
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromMilliseconds(500));
            // The server grants no more streams: opening one waits for credit that never comes
            await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
                await connection.OpenOutboundStreamAsync(type, timeout.Token));
        }
    }

    [Theory(Timeout = 30000)]
    [InlineData("not a hello")]
    [InlineData("invalid json")]
    [InlineData("over the limit")]
    public async Task Wire_Bad_Frames_Close(string name)
    {
        await using var fixture = Fixture.Listen("127.0.0.1:0");
        var (connection, stream) = await RawSession(fixture);
        await using var _ = connection;
        var first = name switch
        {
            "not a hello" => QuicFraming.Encode("""{"id":1,"path":"hello"}"""u8),
            "invalid json" => Hello(new { }).Concat(QuicFraming.Encode("{nope"u8)).ToArray(),
            _ => Hello(new { }).Concat(new byte[] { 0x81, 0, 0, 1 }).ToArray(),
        };
        await stream.WriteAsync(first);
        Assert.True(await Closed(stream), name);
    }

    [Fact(Timeout = 30000)]
    public async Task Reconnect_Replays_Requests()
    {
        var fixture = Fixture.Listen("127.0.0.1:0");
        var port = fixture.Server.Port;
        var client = Connect($"quic://127.0.0.1:{port}", new QuicClientOptions { Ca = QuicTestFiles.Cert });
        try
        {
            Assert.Equal("world", await client.SendAsync<string>("hello"));

            // The server drops the connection on the first try; the client reconnects and sends it again
            Assert.Equal("dropped before", await client.SendAsync<string>("disconnect"));
            Assert.Equal(2, Volatile.Read(ref fixture.Connected));
            Assert.Equal("world", await client.SendAsync<string>("hello"));

            // A request in flight when the server goes away is sent again to the next one on the same port
            var request = client.Request<string>("slow");
            await Task.Delay(50);
            await fixture.Server.StopAsync();
            await WaitFor(() => !client.WebSocket.IsConnected);
            fixture.Start($"127.0.0.1:{port}");
            Assert.Equal("slow", await request.Response.WaitAsync(TimeSpan.FromSeconds(10)));
        }
        finally
        {
            client.Dispose();
            await fixture.DisposeAsync();
        }
    }

    [Fact(Timeout = 30000)]
    public async Task Stop_While_Disconnected_Fails_Requests()
    {
        QuicTestFiles.RequireQuic();
        // Nothing listens there
        using var client = Connect("quic://127.0.0.1:1", new QuicClientOptions { Ca = QuicTestFiles.Cert });
        var request = client.Request<string>("hello");
        await Task.Delay(50);
        client.Stop();
        await Assert.ThrowsAsync<ChannelException>(() => request.Response.WaitAsync(TimeSpan.FromSeconds(2)));
    }

    [Fact(Timeout = 30000)]
    public async Task Disconnects_Are_Reported()
    {
        await using var fixture = Fixture.Listen("127.0.0.1:0");
        var client = Connect(fixture.Address, new QuicClientOptions { Ca = QuicTestFiles.Cert });
        Assert.Equal("world", await client.SendAsync<string>("hello"));
        client.Dispose();
        await WaitFor(() => Volatile.Read(ref fixture.Disconnected) == 1);
        Assert.Equal(1, Volatile.Read(ref fixture.Connected));
    }

    /// <summary>Serves QUIC from C# and calls it with the TypeScript reference's client (Quic/client.ts).</summary>
    [Fact(Timeout = 60000)]
    public async Task Interop_TypeScript_Client()
    {
        await using var fixture = Fixture.Listen("127.0.0.1:0");
        var script = Path.Combine(QuicTestFiles.Root, "tests", "ChannelCs.Tests", "Quic", "client.ts");
        var start = new ProcessStartInfo("bun", [script, fixture.Server.Port.ToString(), QuicTestFiles.CertPath])
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };
        using var bun = System.Diagnostics.Process.Start(start)!;
        using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(30));
        var output = bun.StandardOutput.ReadToEndAsync(timeout.Token);
        var errors = bun.StandardError.ReadToEndAsync(timeout.Token);
        try
        {
            await bun.WaitForExitAsync(timeout.Token);
        }
        finally
        {
            if (!bun.HasExited)
            {
                bun.Kill(true);
            }
        }

        Assert.True(bun.ExitCode == 0, $"bun exited with {bun.ExitCode}: {await errors}");
        using var result = JsonDocument.Parse(await output);
        Assert.Equal(
            """{"hello":"world","echo":{"a":[1,"b"]},"name":"ts","mirror":"ts world","values":[0,1,2],"event":"tick"}""",
            JsonSerializer.Serialize(result.RootElement));
        await WaitFor(() => Volatile.Read(ref fixture.Disconnected) == Volatile.Read(ref fixture.Connected));
    }
}
