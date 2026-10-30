using System.Net.Quic;
using System.Text.Json;
using ChannelCs.Client;

namespace ChannelCs.IntegrationTests;

using ClientState = Dictionary<string, object?>;

/// <summary>
/// The QUIC client against the TypeScript reference's fixture (channel/tests/server.ts), which serves QUIC on UDP
/// next to its WebSocket on TCP: 127.0.0.1:2049, or <c>CHANNEL_TEST_PORT</c>.
/// </summary>
public sealed class QuicInteropTests
{
    private static int Port => int.TryParse(Environment.GetEnvironmentVariable("CHANNEL_TEST_PORT"), out var port) ? port : 2049;

    private static string CertPath
    {
        get
        {
            var directory = new DirectoryInfo(AppContext.BaseDirectory);
            while (directory is not null && !File.Exists(Path.Combine(directory.FullName, "ChannelCs.slnx")))
            {
                directory = directory.Parent;
            }

            return Path.GetFullPath(Path.Combine(directory!.FullName, "..", "channel", "tests", "quic", "cert.pem"));
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

    private static async Task WaitFor(Func<bool> ready, string what)
    {
        var deadline = DateTime.UtcNow.AddSeconds(5);
        while (!ready())
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException(what);
            }

            await Task.Delay(5);
        }
    }

    [Fact(Timeout = 30000)]
    public async Task Client_Against_TypeScript_Server()
    {
        Assert.True(QuicConnection.IsSupported, "System.Net.Quic isn't supported here: install libmsquic");
        var names = new Subscription();
        using var client = new Channel<ClientState>()
            .Post("hello", _ => "client world")
            .Stream("stream/values", _ => Count(3))
            .Events(new Dictionary<string, Subscription> { ["names"] = names })
            .Connect($"quic://127.0.0.1:{Port}", new ClientOptions<ClientState>
            {
                Quic = new QuicClientOptions { Ca = File.ReadAllText(CertPath) },
                Headers = () => ValueTask.FromResult<IReadOnlyDictionary<string, string>>(
                    new Dictionary<string, string> { ["X-Port"] = "cs" }),
            });
        await WaitFor(() => client.WebSocket.IsConnected, $"the fixture doesn't serve QUIC on 127.0.0.1:{Port}");

        Assert.Equal("world", await client.SendAsync<string>("hello"));
        var echo = await client.SendAsync<JsonElement>("echo", new { a = new object[] { 1, "b" } });
        Assert.Equal("""{"a":[1,"b"]}""", echo.GetRawText());
        Assert.Equal("client world", await client.SendAsync<string>("mirror"));
        await client.SendAsync<object?>("auth", new Dictionary<string, string> { ["name"] = "quic" });
        Assert.Equal("quic", await client.SendAsync<string>("auth/name"));
        var headers = await client.SendAsync<Dictionary<string, string>>("v2/headers");
        Assert.Equal("cs", headers!["x-port"]);

        var values = new List<int>();
        await foreach (var value in client.Values<int>("mirror/stream"))
        {
            values.Add(value);
        }

        Assert.Equal([0, 1, 2], values);

        // The fixture subscribes to this client's names/1 and cancels after the first event
        await client.SendAsync<object?>("mirror/events");
        names.Send("1", "h");

        var error = await Assert.ThrowsAsync<ChannelException>(() => client.SendAsync<object?>("v2/error/code"));
        Assert.Equal("slow down", error.Message);

        // The fixture drops the connection without answering; cancelled, the request isn't sent again after the
        // client reconnects
        await client.SendAsync<object?>("v2/drops/reset");
        var drop = client.Request<object?>("v2/drop");
        await WaitFor(() => !client.WebSocket.IsConnected, "v2/drop didn't close the connection");
        drop.Cancel();
        Assert.Equal("world", await client.SendAsync<string>("hello"));
        await Task.Delay(100);
        Assert.Equal(1, await client.SendAsync<int>("v2/drops"));
    }

    [Fact(Timeout = 30000)]
    public async Task Pinned_Fingerprint_Against_TypeScript_Server()
    {
        Assert.True(QuicConnection.IsSupported, "System.Net.Quic isn't supported here: install libmsquic");
        using var client = new Channel<ClientState>().Connect($"quic://localhost:{Port}", new ClientOptions<ClientState>
        {
            Quic = new QuicClientOptions
            {
                Fingerprint = "DD:D2:37:00:BD:8A:BD:29:14:15:D8:65:84:BE:C2:80:10:FF:21:DA:A1:C5:9D:B3:4A:7F:36:63:EF:4B:55:9B",
            },
        });
        Assert.Equal("world", await client.SendAsync<string>("hello").WaitAsync(TimeSpan.FromSeconds(10)));
    }
}
