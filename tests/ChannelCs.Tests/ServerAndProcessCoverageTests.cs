using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Text;
using ChannelCs.Client;
using ChannelCs.Process;
using ChannelCs.Server;

namespace ChannelCs.Tests;

public sealed class ServerAndProcessCoverageTests
{
    [Fact]
    public async Task ListenOptions_StateFactory_OnConnect_OnDisconnect_Are_Used()
    {
        var port = GetFreePort();
        var onConnect = new TaskCompletionSource<BodyContext<Dictionary<string, string?>>>(TaskCreationOptions.RunContinuationsAsynchronously);
        var onDisconnect = new TaskCompletionSource<BodyContext<Dictionary<string, string?>>>(TaskCreationOptions.RunContinuationsAsynchronously);

        var channel = new Channel<Dictionary<string, string?>>()
            .Post("state/name", request => request.State.TryGetValue("x-name", out var value) ? value : null);

        using var server = channel.Listen(
            $"127.0.0.1:{port}",
            new ListenOptions<Dictionary<string, string?>>
            {
                StateFactory = headers =>
                {
                    headers.TryGetValue("x-name", out var name);
                    return ValueTask.FromResult(new Dictionary<string, string?>
                    {
                        ["x-name"] = name,
                    });
                },
                OnConnect = context => onConnect.TrySetResult(context),
                OnDisconnect = context => onDisconnect.TrySetResult(context),
            });

        using var client = new Channel<Dictionary<string, object?>>().Connect(
            port,
            new ClientOptions<Dictionary<string, object?>>
            {
                Headers = () => ValueTask.FromResult<IReadOnlyDictionary<string, string>>(new Dictionary<string, string>
                {
                    ["x-name"] = "alice",
                }),
            });

        WaitForConnected(client);
        var name = await client.SendAsync<string>("state/name");
        Assert.Equal("alice", name);

        var connected = await onConnect.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal("alice", connected.State["x-name"]);

        client.Stop();
        var disconnected = await onDisconnect.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.NotNull(disconnected.Sender);
    }

    [Fact]
    public async Task Listen_Handles_Non_WebSocket_Request_With_200()
    {
        var port = GetFreePort();
        using var server = new Channel<Dictionary<string, object?>>().Listen($"127.0.0.1:{port}");

        using var http = new HttpClient();
        var response = await http.GetAsync($"http://127.0.0.1:{port}/");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public async Task Listen_Int_Overload_Starts_Server()
    {
        var port = GetFreePort();
        using var server = new Channel<Dictionary<string, object?>>().Listen(port);

        using var http = new HttpClient();
        var response = await http.GetAsync($"http://127.0.0.1:{port}/");

        Assert.Equal(HttpStatusCode.OK, response.StatusCode);
    }

    [Fact]
    public void Listen_Invalid_Address_Throws()
    {
        var exception = Assert.Throws<ArgumentException>(
            () => new Channel<Dictionary<string, object?>>().Listen("127.0.0.1:not-a-port"));

        Assert.Equal("address", exception.ParamName);
    }

    [Fact]
    public async Task Listen_Closes_Invalid_Json_WebSocket_Payload()
    {
        var port = GetFreePort();
        using var server = new Channel<Dictionary<string, object?>>().Listen($"127.0.0.1:{port}");
        using var socket = new ClientWebSocket();

        await socket.ConnectAsync(new Uri($"ws://127.0.0.1:{port}/"), CancellationToken.None);
        await socket.SendAsync(Encoding.UTF8.GetBytes("{"), WebSocketMessageType.Text, true, CancellationToken.None);

        var buffer = new byte[256];
        var result = await socket.ReceiveAsync(buffer, CancellationToken.None);

        Assert.Equal(WebSocketMessageType.Close, result.MessageType);
    }

    [Fact]
    public async Task Server_Publishes_Subscription_Events_To_Subscribed_Client()
    {
        var port = GetFreePort();
        var updates = new Subscription();

        var channel = new Channel<Dictionary<string, object?>>()
            .Events(new Dictionary<string, Subscription>
            {
                ["updates"] = updates,
            })
            .Post("trigger", _ =>
            {
                updates.Send("room", "tick");
                return "ok";
            });

        using var server = channel.Listen($"127.0.0.1:{port}");
        using var client = new Channel<Dictionary<string, object?>>().Connect(port);
        WaitForConnected(client);

        var eventTcs = new TaskCompletionSource<string>(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancellable = await client.SubscribeAsync("updates", "room", body =>
        {
            eventTcs.TrySetResult(body.GetString() ?? string.Empty);
        });

        await client.SendAsync<object?>("trigger");
        var @event = await eventTcs.Task.WaitAsync(TimeSpan.FromSeconds(2));
        Assert.Equal("tick", @event);

        cancellable.Cancel();
    }

    [Fact]
    public void Server_Stop_Is_Idempotent()
    {
        var port = GetFreePort();
        var server = new Channel<Dictionary<string, object?>>().Listen($"127.0.0.1:{port}");

        server.Stop();
        server.Stop();

        server.Dispose();
    }

    [Fact]
    public void ServerSubscriptions_Add_Remove_And_Dispatch_Work()
    {
        var subscriptions = new ServerSubscriptions();
        var firstCount = 0;
        var secondCount = 0;

        var cancelFirst = subscriptions.AddTopic("topic", _ => firstCount += 1);
        var cancelSecond = subscriptions.AddTopic("topic", _ => secondCount += 1);

        subscriptions.ReceivedEvent("topic", new { value = 1 });
        Assert.Equal(1, firstCount);
        Assert.Equal(1, secondCount);

        Assert.False(cancelFirst());
        Assert.True(cancelSecond());
        Assert.False(cancelSecond());

        subscriptions.ReceivedEvent("topic", new { value = 2 });
        subscriptions.ReceivedEvent("unknown", new { value = 3 });
    }

    [Fact]
    public async Task Process_Default_Overload_Uses_Default_State()
    {
        var sender = new Channel<string?>()
            .Post("state", request => request.State)
            .Process();

        var state = await sender.SendAsync<string>("state");
        Assert.Null(state);

        sender.Stop();
    }

    [Fact]
    public async Task Process_Subscribe_Cancel_Uses_AddTopic_Fallback_Path()
    {
        var sender = new Channel<Dictionary<string, object?>>()
            .Events(new Dictionary<string, Subscription>
            {
                ["names"] = new Subscription(),
            })
            .Process(new Dictionary<string, object?>());

        var subscription = await sender.SubscribeAsync("names", "1", _ => { });
        subscription.Cancel();

        sender.Stop();
    }

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
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

        throw new TimeoutException("WebSocket client did not connect to local server within 3 seconds.");
    }
}
