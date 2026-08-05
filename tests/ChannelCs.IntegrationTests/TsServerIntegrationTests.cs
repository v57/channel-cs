using System.Net.WebSockets;
using ChannelCs.Client;

namespace ChannelCs.IntegrationTests;

[CollectionDefinition("ts-server", DisableParallelization = true)]
public sealed class TsServerCollection : ICollectionFixture<TsServerFixture>
{
}

public sealed class TsServerFixture : IAsyncLifetime
{
    public async Task InitializeAsync()
    {
        using var socket = new ClientWebSocket();
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));

        try
        {
            await socket.ConnectAsync(new Uri("ws://127.0.0.1:2049"), cts.Token);
            await socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "ok", CancellationToken.None);
        }
        catch (Exception exception)
        {
            throw new InvalidOperationException(
                "TypeScript fixture server is not reachable at ws://127.0.0.1:2049. Start /Users/dimas/github/hub/channel/tests/server.ts before running integration tests.",
                exception);
        }
    }

    public Task DisposeAsync()
    {
        return Task.CompletedTask;
    }
}

[Collection("ts-server")]
public sealed class TsServerIntegrationTests
{
    [Fact(Timeout = 20000)]
    public async Task Post_Hello_Returns_World()
    {
        using var client = CreateSimpleClient();
        var response = await client.SendAsync<string>("hello");
        Assert.Equal("world", response);
    }

    [Fact(Timeout = 20000)]
    public async Task Post_Echo_RoundTrip()
    {
        using var client = CreateSimpleClient();
        var value = Random.Shared.NextDouble();
        var response = await client.SendAsync<double>("echo", value);
        Assert.Equal(value, response);
    }

    [Fact(Timeout = 20000)]
    public async Task Post_Empty_Returns_Null()
    {
        using var client = CreateSimpleClient();
        var response = await client.SendAsync<string>("empty");
        Assert.Null(response);
    }

    [Fact(Timeout = 20000)]
    public async Task Post_NotFound_Returns_Error()
    {
        using var client = CreateSimpleClient();
        var exception = await Assert.ThrowsAsync<ChannelException>(() => client.SendAsync<string>("notfound"));
        Assert.Equal("api not found", exception.Message);
    }

    [Fact(Timeout = 20000)]
    public async Task Stream_Values_Returns_Sequence()
    {
        using var client = CreateSimpleClient();
        var values = new List<int>();

        await foreach (var value in client.Values<int>("stream/values"))
        {
            values.Add(value);
        }

        Assert.Equal([0, 1, 2], values);
    }

    [Fact(Timeout = 20000)]
    public async Task Stream_Cancel_Stops_After_Break()
    {
        using var client = CreateSimpleClient();
        var values = new List<int>();

        await foreach (var value in client.Values<int>("stream/cancel"))
        {
            values.Add(value);
            if (value == 2)
            {
                break;
            }
        }

        Assert.Equal([0, 1, 2], values);
    }

    [Fact(Timeout = 20000)]
    public async Task Server_Mirror_Calls_Client_Post_Handler()
    {
        using var client = CreateMirrorClient(out _, out _);
        var response = await client.SendAsync<string>("mirror");
        Assert.Equal("client world", response);
    }

    [Fact(Timeout = 20000)]
    public async Task Server_Mirror_Stream_Calls_Client_Stream_Handler()
    {
        using var client = CreateMirrorClient(out _, out _);
        var values = new List<int>();

        await foreach (var value in client.Values<int>("mirror/stream"))
        {
            values.Add(value);
        }

        Assert.Equal([0, 1, 2], values);
    }

    [Fact(Timeout = 20000)]
    public async Task Server_Mirror_Stream_Cancel_Stops_Client_Stream()
    {
        using var client = CreateMirrorClient(out _, out var valuesSent);
        var values = new List<int>();

        await foreach (var value in client.Values<int>("mirror/stream/cancel"))
        {
            values.Add(value);
            if (value == 2)
            {
                break;
            }
        }

        await Task.Delay(100);
        Assert.Equal([0, 1, 2], values);
        Assert.InRange(valuesSent(), 3, 9);
    }

    [Fact(Timeout = 20000)]
    public async Task Server_Mirror_Events_Completes()
    {
        using var client = CreateMirrorClient(out var names, out _);

        await client.SendAsync<object?>("mirror/events");
        await Task.Delay(50);

        names.Send("2", "po");
        names.Send("1", "h");
        await Task.Delay(50);
        names.Send("1", "we");
        await Task.Delay(50);
    }

    [Fact(Timeout = 20000)]
    public async Task State_Auth_And_Name_Work()
    {
        using var client = CreateSimpleClient();

        await client.SendAsync<string>("auth", new Dictionary<string, string> { ["name"] = "tester" });
        var name = await client.SendAsync<string>("auth/name");

        Assert.Equal("tester", name);
    }

    [Fact(Timeout = 20000)]
    public async Task State_Unauthorized_Returns_Error()
    {
        using var client = CreateSimpleClient();
        var exception = await Assert.ThrowsAsync<ChannelException>(() => client.SendAsync<string>("auth/name"));
        Assert.Equal("unauthorized", exception.Message);
    }

    [Fact(Timeout = 20000)]
    public async Task Subscribe_NotFound_Returns_Error()
    {
        using var client = CreateSimpleClient();

        var exception = await Assert.ThrowsAsync<ChannelException>(
            () => client.SubscribeAsync("notfound", "test", _ => { }));

        Assert.Equal("subscription not found", exception.Message);
    }

    [Fact(Timeout = 20000)]
    public async Task Disconnect_Endpoint_Allows_Reconnect()
    {
        using var client = CreateSimpleClient();

        var request = client.Request<object?>("disconnect");
        await Task.Delay(200);
        request.Cancel();

        var response = await client.SendAsync<string>("hello");
        Assert.Equal("world", response);
    }

    private static ClientSender<Dictionary<string, object?>> CreateSimpleClient()
    {
        var channel = new Channel<Dictionary<string, object?>>();
        var client = channel.Connect(2049);
        WaitForConnected(client);
        return client;
    }

    private static ClientSender<Dictionary<string, object?>> CreateMirrorClient(
        out Subscription names,
        out Func<int> valuesSent)
    {
        var sent = 0;
        names = new Subscription();

        var channel = new Channel<Dictionary<string, object?>>() 
            .Post("hello", _ => "client world")
            .Stream(
                "stream/values",
                _ => ValueTask.FromResult<IAsyncEnumerable<object?>>(YieldRange(3, 1, null)))
            .Stream(
                "stream/cancel",
                _ => ValueTask.FromResult<IAsyncEnumerable<object?>>(YieldRange(10, 30, () => Interlocked.Increment(ref sent))))
            .Events(new Dictionary<string, Subscription>
            {
                ["names"] = names,
            });

        valuesSent = () => sent;
        var client = channel.Connect(2049);
        WaitForConnected(client);
        return client;
    }

    private static async IAsyncEnumerable<object?> YieldRange(int count, int millisecondsDelay, Action? onYield)
    {
        for (var i = 0; i < count; i += 1)
        {
            yield return i;
            onYield?.Invoke();
            await Task.Delay(millisecondsDelay);
        }
    }

    private static void WaitForConnected(ClientSender<Dictionary<string, object?>> client)
    {
        var started = DateTime.UtcNow;
        while ((DateTime.UtcNow - started).TotalSeconds < 3)
        {
            if (client.WebSocket.IsConnected)
            {
                return;
            }

            Thread.Sleep(20);
        }

        throw new TimeoutException("WebSocket client did not connect to ws://127.0.0.1:2049 within 3 seconds.");
    }
}
