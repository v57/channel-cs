using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using ChannelCs.Client;
using ChannelCs.Server;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;

namespace ChannelCs.Tests;

// Regressions for AUDIT.md §4 and AUDIT2.md §2/§4 (2026-10).
public sealed class AuditRegressionTests
{
    [Fact]
    public void Subscription_Body_Sets_Body_Factory_Not_Topic()
    {
        var subscription = new Subscription();
        var events = new List<SubscriptionEvent>();
        subscription.AddPublisher(new ListPublisher(events));

        subscription.Topic(request => $"topic-{request}");
        subscription.Body(request => $"body-{request}");
        subscription.Send("x");
        subscription.Send("y", "payload");

        Assert.Equal(
            [new SubscriptionEvent("topic-x", "body-x"), new SubscriptionEvent("topic-y", "payload")],
            events);
    }

    [Fact]
    public void Subscription_Publish_Drops_All_Falsy_Bodies_Like_TS()
    {
        var subscription = new Subscription();
        var events = new List<SubscriptionEvent>();
        subscription.AddPublisher(new ListPublisher(events));

        foreach (var body in new object?[]
                 {
                     null, false, 0, 0L, 0.0, double.NaN, "",
                     JsonDocument.Parse("0").RootElement, JsonDocument.Parse("\"\"").RootElement,
                     JsonDocument.Parse("false").RootElement, JsonDocument.Parse("null").RootElement,
                 })
        {
            subscription.Publish("t", body);
        }

        Assert.Empty(events);

        subscription.Publish("t", "0");
        subscription.Publish("t", true);
        subscription.Publish("t", 1);
        subscription.Publish("t", JsonDocument.Parse("{}").RootElement);
        Assert.Equal(4, events.Count);
    }

    [Fact]
    public void Codec_Is_Lenient_About_Field_Types()
    {
        var messages = JsonCodec.Deserialize(Encoding.UTF8.GetBytes(
            """[null,{"id":"x","path":{"a":1}},{"id":1,"error":{"code":500,"message":"nope"}},{"cancel":"abc"},{"unsub":123},{"id":2.5,"extra":[1,2]}]"""));

        Assert.Equal(5, messages.Count);
        Assert.Null(messages[0].Id);
        Assert.Equal("x", messages[0].RawId?.GetString());
        Assert.Equal("""{"a":1}""", messages[0].Path);
        Assert.Equal(1, messages[1].Id);
        Assert.Equal("""{"code":500,"message":"nope"}""", messages[1].Error);
        Assert.Null(messages[2].Cancel);
        Assert.Equal("123", messages[3].Unsub);
        Assert.Null(messages[4].Id);
        Assert.Equal(2.5, messages[4].RawId?.GetDouble());

        var encoded = Encoding.UTF8.GetString(JsonCodec.Serialize(new ChannelMessage { RawId = messages[0].RawId, Body = 3 }));
        Assert.Equal("""{"id":"x","body":3}""", encoded);
    }

    [Fact]
    public async Task Server_Echoes_Non_Numeric_Id_And_Hides_Serializer_Errors()
    {
        var port = GetFreePort();
        var channel = new Channel<Dictionary<string, object?>>()
            .Post("count", request => (object?)request.Body<Dictionary<string, string>>()!["text"].Length);
        using var server = channel.Listen(port);

        using var socket = new ClientWebSocket();
        await socket.ConnectAsync(new Uri($"ws://127.0.0.1:{port}"), CancellationToken.None);

        await SendTextAsync(socket, """{"id":"x","path":"count","body":{"text":"abc"}}""");
        Assert.Equal("""{"id":"x","body":3}""", await ReceiveTextAsync(socket));

        await SendTextAsync(socket, """{"id":7,"path":"count","body":42}""");
        Assert.Equal("""{"id":7,"error":"invalid body"}""", await ReceiveTextAsync(socket));

        await SendTextAsync(socket, """[null,{"id":8,"path":{"a":1}}]""");
        Assert.Equal("""{"id":8,"error":"api not found"}""", await ReceiveTextAsync(socket));
    }

    [Fact]
    public async Task Server_Closes_Oversized_Message_With_1009()
    {
        var port = GetFreePort();
        using var server = new Channel<Dictionary<string, object?>>()
            .Listen(port, new ListenOptions<Dictionary<string, object?>> { MaxMessageSize = 1024 });

        using var socket = new ClientWebSocket();
        await socket.ConnectAsync(new Uri($"ws://127.0.0.1:{port}"), CancellationToken.None);
        var chunk = new byte[512];
        for (var i = 0; i < 4; i += 1)
        {
            try
            {
                await socket.SendAsync(chunk, WebSocketMessageType.Text, false, CancellationToken.None);
            }
            catch (WebSocketException)
            {
                break;
            }
        }

        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var result = await socket.ReceiveAsync(new byte[64], cts.Token);
        Assert.Equal(WebSocketMessageType.Close, result.MessageType);
        Assert.Equal(WebSocketCloseStatus.MessageTooBig, socket.CloseStatus);
    }

    [Theory]
    [InlineData("badjson", "invalid message from server")]
    [InlineData("errorobj", """{"code":500,"message":"nope"}""")]
    [InlineData("deepjson", "invalid message from server")]
    [InlineData("toobig", "message too big")]
    public async Task Client_Settles_Pending_Call_On_Hostile_Reply(string scenario, string expectedError)
    {
        await using var hostile = await HostileServer.StartAsync(scenario);
        using var client = new Channel<Dictionary<string, object?>>()
            .Connect(hostile.Port, new ClientOptions<Dictionary<string, object?>> { MaxMessageSize = 64 * 1024 });

        var call = client.SendAsync<JsonElement>("text/count", new { text = "hello" });
        var winner = await Task.WhenAny(call, Task.Delay(5000));
        Assert.Same(call, winner);
        var error = await Assert.ThrowsAsync<ChannelException>(() => call);
        Assert.Equal(expectedError, error.Message);
    }

    [Fact]
    public async Task Client_Accepts_Binary_Reply()
    {
        await using var hostile = await HostileServer.StartAsync("binary");
        using var client = new Channel<Dictionary<string, object?>>().Connect(hostile.Port);

        var call = client.SendAsync<int>("text/count", new { text = "hello" });
        Assert.Same(call, await Task.WhenAny(call, Task.Delay(5000)));
        Assert.Equal(5, await call);
    }

    [Fact]
    public async Task Client_Stop_Settles_Pending_Calls()
    {
        var port = GetFreePort(); // nothing listens here: the call stays queued
        var client = new Channel<Dictionary<string, object?>>().Connect(port);
        var call = client.SendAsync<int>("never");
        await Task.Delay(50);
        client.Stop();

        Assert.Same(call, await Task.WhenAny(call, Task.Delay(5000)));
        var error = await Assert.ThrowsAsync<ChannelException>(() => call);
        Assert.Equal("disconnected", error.Message);
        client.Dispose();
    }

    private static async Task SendTextAsync(WebSocket socket, string text)
    {
        await socket.SendAsync(Encoding.UTF8.GetBytes(text), WebSocketMessageType.Text, true, CancellationToken.None);
    }

    private static async Task<string> ReceiveTextAsync(WebSocket socket)
    {
        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(5));
        var buffer = new byte[64 * 1024];
        using var stream = new MemoryStream();
        WebSocketReceiveResult result;
        do
        {
            result = await socket.ReceiveAsync(buffer, cts.Token);
            stream.Write(buffer, 0, result.Count);
        }
        while (!result.EndOfMessage);

        return Encoding.UTF8.GetString(stream.ToArray());
    }

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private sealed class ListPublisher(List<SubscriptionEvent> events) : ISubscriptionPublisher
    {
        public void Publish(SubscriptionEvent @event) => events.Add(@event);
    }

    /// <summary>Answers the first request of each connection with a hostile reply (mirrors audit2/hostile.ts).</summary>
    private sealed class HostileServer : IAsyncDisposable
    {
        private readonly WebApplication _app;

        private HostileServer(WebApplication app, int port)
        {
            _app = app;
            Port = port;
        }

        public int Port { get; }

        public static async Task<HostileServer> StartAsync(string scenario)
        {
            var port = GetFreePort();
            var builder = WebApplication.CreateSlimBuilder();
            builder.WebHost.UseUrls($"http://127.0.0.1:{port}");
            builder.Logging.ClearProviders();
            var app = builder.Build();
            app.UseWebSockets();
            app.Map("/", async (HttpContext context) =>
            {
                using var socket = await context.WebSockets.AcceptWebSocketAsync();
                var request = await ReceiveTextAsync(socket);
                using var document = JsonDocument.Parse(request);
                var root = document.RootElement;
                var first = root.ValueKind == JsonValueKind.Array ? root[0] : root;
                var id = first.GetProperty("id").GetInt32();

                switch (scenario)
                {
                    case "badjson":
                        await SendTextAsync(socket, "{oops");
                        break;
                    case "errorobj":
                        await SendTextAsync(socket, "{\"id\":" + id + ",\"error\":{\"code\":500,\"message\":\"nope\"}}");
                        break;
                    case "deepjson":
                        await SendTextAsync(socket, "{\"id\":" + id + ",\"body\":" + new string('[', 1000) + new string(']', 1000) + "}");
                        break;
                    case "toobig":
                        await SendTextAsync(socket, "{\"id\":" + id + ",\"body\":\"" + new string('a', 128 * 1024) + "\"}");
                        break;
                    case "binary":
                        await socket.SendAsync(
                            Encoding.UTF8.GetBytes($$"""{"id":{{id}},"body":5}"""),
                            WebSocketMessageType.Binary,
                            true,
                            CancellationToken.None);
                        break;
                }

                try
                {
                    using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
                    while (socket.State == WebSocketState.Open)
                    {
                        await socket.ReceiveAsync(new byte[1024], cts.Token);
                    }
                }
                catch
                {
                    // Client went away.
                }
            });
            await app.StartAsync();
            return new HostileServer(app, port);
        }

        public async ValueTask DisposeAsync()
        {
            await _app.StopAsync();
            await _app.DisposeAsync();
        }
    }
}
