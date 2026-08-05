using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Text.Json;
using ChannelCs.Client;
using ChannelCs.Process;
using ChannelCs.Server;

namespace ChannelCs.Tests;

public sealed class DeepCoverageTests
{
    [Fact]
    public async Task Channel_Api_Parse_And_Merge_Covers_All_Handler_Shapes()
    {
        var channel = new Channel<Dictionary<string, object?>>();

        ChannelPostHandler<Dictionary<string, object?>> postDelegate = _ => ValueTask.FromResult<object?>("postDelegate");
        ChannelStreamHandler<Dictionary<string, object?>> streamDelegate = _ =>
            ValueTask.FromResult<IAsyncEnumerable<object?>>(Yield("streamDelegate"));

        channel.Api(new Dictionary<string, object?>
        {
            ["postDelegate"] = postDelegate,
            ["streamDelegate"] = streamDelegate,
            ["streamAsync"] = (Func<ChannelRequestContext<Dictionary<string, object?>>, Task<IAsyncEnumerable<object?>>>)
                (_ => Task.FromResult<IAsyncEnumerable<object?>>(Yield("streamAsync"))),
            ["streamSync"] = (Func<ChannelRequestContext<Dictionary<string, object?>>, IAsyncEnumerable<object?>>)
                (_ => Yield("streamSync")),
            ["postAsync"] = (Func<ChannelRequestContext<Dictionary<string, object?>>, Task<object?>>)
                (_ => Task.FromResult<object?>("postAsync")),
            ["postSync"] = (Func<ChannelRequestContext<Dictionary<string, object?>>, object?>)
                (_ => "postSync"),
            ["withoutCtxAsync"] = (Func<Task<object?>>)(() => Task.FromResult<object?>("withoutCtxAsync")),
            ["withoutCtx"] = (Func<object?>)(() => "withoutCtx"),
            ["group"] = new Dictionary<string, object?>
            {
                ["_"] = (Func<object?>)(() => "groupRoot"),
                ["nested"] = (Func<object?>)(() => "nested"),
                ["ignoredNull"] = null,
            },
        });

        var mergedEvent = new Subscription();
        var merged = new Channel<Dictionary<string, object?>>()
            .Post("post", _ => "mergedPost")
            .Stream("stream", _ => Yield("mergedStream"))
            .Events(new Dictionary<string, Subscription>
            {
                ["evt"] = mergedEvent,
            });

        channel.Merge(merged, "m");

        var sender = channel.Process(new Dictionary<string, object?>());

        Assert.Equal("postDelegate", await sender.SendAsync<string>("postDelegate"));
        Assert.Equal("postAsync", await sender.SendAsync<string>("postAsync"));
        Assert.Equal("postSync", await sender.SendAsync<string>("postSync"));
        Assert.Equal("withoutCtxAsync", await sender.SendAsync<string>("withoutCtxAsync"));
        Assert.Equal("withoutCtx", await sender.SendAsync<string>("withoutCtx"));
        Assert.Equal("groupRoot", await sender.SendAsync<string>("group"));
        Assert.Equal("nested", await sender.SendAsync<string>("group/nested"));
        Assert.Equal("mergedPost", await sender.SendAsync<string>("m/post"));

        Assert.Equal("streamDelegate", await FirstAsync(sender.Values<string>("streamDelegate")));
        Assert.Equal("streamAsync", await FirstAsync(sender.Values<string>("streamAsync")));
        Assert.Equal("streamSync", await FirstAsync(sender.Values<string>("streamSync")));
        Assert.Equal("mergedStream", await FirstAsync(sender.Values<string>("m/stream")));

        Assert.NotNull(channel.EventsApi);
        Assert.True(channel.EventsApi!.ContainsKey("m/evt"));

        sender.Stop();
    }

    [Fact]
    public async Task Channel_Error_And_Edge_Branches_Are_Handled()
    {
        var badSubscription = new Subscription
        {
            BodyFactory = async _ =>
            {
                await Task.Yield();
                throw new InvalidOperationException("sub-async-fail");
            },
        };

        var channel = new Channel<Dictionary<string, object?>>()
            .Post(
                "throwSync",
                (Func<ChannelRequestContext<Dictionary<string, object?>>, object?>)
                (_ => throw new InvalidOperationException("sync-fail")))
            .Post("throwAsync", _ => ThrowPathAsync())
            .Stream(
                "throwInit",
                (Func<ChannelRequestContext<Dictionary<string, object?>>, IAsyncEnumerable<object?>>)
                (_ => throw new InvalidOperationException("stream-init-fail")))
            .Stream("throwEnum", _ => ThrowingStream())
            .Events(new Dictionary<string, Subscription>
            {
                ["bad"] = badSubscription,
            });

        var sender = channel.CreateSender(new TestConnection());
        var controller = new CapturingController(sender);

        channel.Receive(new ChannelMessage { Path = "missing" }, controller);
        channel.Receive(new ChannelMessage { Id = 1, Path = "throwSync" }, controller);
        channel.Receive(new ChannelMessage { Path = "throwAsync" }, controller);

        channel.Receive(new ChannelMessage { Stream = "throwEnum" }, controller);
        channel.Receive(new ChannelMessage { Id = 2, Stream = "missing" }, controller);
        channel.Receive(new ChannelMessage { Id = 3, Stream = "throwInit" }, controller);
        channel.Receive(new ChannelMessage { Id = 4, Stream = "throwEnum" }, controller);

        channel.Receive(new ChannelMessage { Sub = "missing" }, controller);
        channel.Receive(new ChannelMessage { Id = 5, Sub = "missing" }, controller);
        channel.Receive(new ChannelMessage { Id = 6, Sub = "bad", Body = "x" }, controller);

        var handleCancel = typeof(Channel<Dictionary<string, object?>>)
            .GetMethod("HandleCancel", System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic)!;
        handleCancel.Invoke(channel, [new ChannelMessage(), controller]);

        await Task.Delay(40);

        Assert.Contains(controller.Responses, response => response.Id == 1 && response.Error == "sync-fail");
        Assert.Contains(controller.Responses, response => response.Id == 2 && response.Error == "api not found");
        Assert.Contains(controller.Responses, response => response.Id == 3 && response.Error == "stream-init-fail");
        Assert.Contains(controller.Responses, response => response.Id == 4 && response.Error == "stream-enum-fail");
        Assert.Contains(controller.Responses, response => response.Id == 5 && response.Error == "subscription not found");
        Assert.Contains(controller.Responses, response => response.Id == 6 && response.Error == "sub-async-fail");
    }

    [Fact]
    public void Channel_Disconnect_Handles_Stream_Dispose_Exception()
    {
        var channel = new Channel<Dictionary<string, object?>>();
        var sender = channel.CreateSender(new TestConnection());

        var streams = GetNonPublicProperty<ConcurrentDictionary<int, IAsyncDisposable>>(sender, "Streams");
        streams[1] = new ThrowingAsyncDisposable();

        sender.Request<object?>("never");
        Assert.True(channel.PendingRequestsCount > 0);

        channel.Disconnect(new Dictionary<string, object?>(), sender);

        Assert.Equal(0, channel.PendingRequestsCount);
    }

    [Fact]
    public void Process_Internal_Connection_Branches_Covered()
    {
        var channel = new Channel<Dictionary<string, object?>>()
            .Post("hello", _ => "world");
        var sender = channel.Process(new Dictionary<string, object?>());

        var connection = GetNonPublicField<object>(sender, "_connection");

        Invoke(connection, "Send", new List<ChannelMessage>
        {
            new() { Path = "hello" },
        });
        Invoke(connection, "Notify", new ChannelMessage { Path = "hello" });
        Invoke(connection, "Sent", 42L);
        Assert.False((bool)Invoke(connection, "Cancel", 1L)!);

        var remove = (Func<bool>)Invoke(connection, "AddTopic", "topic", (Action<JsonElement>)(_ => { }))!;
        Assert.False(remove());

        var controller = GetNonPublicField<object?>(connection, "_controller");
        Assert.NotNull(controller);
        Invoke(controller!, "Subscribe", "topic");
        Invoke(controller!, "Unsubscribe", "topic");
        Invoke(controller!, "Event", "topic", new { value = 1 });

        Invoke(connection, "Stop");
        Invoke(connection, "Stop");

        Invoke(connection, "Send", new ChannelMessage { Path = "hello" });

        sender.Stop();
    }

    [Fact]
    public async Task WebSocketClient_Internal_State_Branches_Covered()
    {
        var port = GetFreePort();
        using var server = new Channel<Dictionary<string, object?>>().Listen($"127.0.0.1:{port}");
        using var client = new WebSocketClient(new Uri($"ws://127.0.0.1:{port}"));

        var idWhenDisconnected = client.Send(new { value = 1 });
        Assert.Equal(0, idWhenDisconnected);

        await WaitUntilAsync(() => client.IsConnected, TimeSpan.FromSeconds(3));

        SetNonPublicField(client, "_isWaiting", 1);
        SetNonPublicField(client, "_isWaitingLength", 4001);
        client.Send("w1");
        Assert.Equal(3, GetNonPublicField<int>(client, "_isWaiting"));

        SetNonPublicField(client, "_isWaiting", 2);
        client.Send("queued");
        Assert.True(GetNonPublicField<List<object>>(client, "_queue").Count > 0);

        SetNonPublicField(client, "_isWaiting", 3);
        client.Send("flush");

        client.Throttle();
        Assert.Equal(3, GetNonPublicField<int>(client, "_isWaiting"));

        Assert.False(client.Cancel(999));

        var calls = 0;
        var remove1 = client.AddTopic("t", _ => calls += 1);
        var remove2 = client.AddTopic("t", _ => calls += 1);
        client.ReceivedEvent("unknown", new { });
        client.ReceivedEvent("t", new { });
        Assert.Equal(2, calls);
        Assert.False(remove1());
        Assert.True(remove2());
        Assert.False(remove2());

        SetNonPublicField(client, "_isWaitingLength", 5000);
        await InvokeTask(client, "TransitionToBatchModeAsync");
        Assert.Equal(3, GetNonPublicField<int>(client, "_isWaiting"));

        SetNonPublicField(client, "_isWaiting", 2);
        GetNonPublicField<List<object>>(client, "_queue").Add("item");
        await InvokeTask(client, "FlushQueuedAsync");

        client.Stop();

        var pendingId = client.Send("pending");
        Assert.True(client.Cancel(pendingId));

        await InvokeTask(client, "TransitionToBatchModeAsync");
        await InvokeTask(client, "FlushQueuedAsync");
    }

    [Fact]
    public async Task Client_OnConnect_Callback_Is_Invoked()
    {
        var port = GetFreePort();
        using var server = new Channel<Dictionary<string, object?>>().Listen($"127.0.0.1:{port}");

        var connected = new TaskCompletionSource<bool>(TaskCreationOptions.RunContinuationsAsynchronously);
        using var client = new Channel<Dictionary<string, object?>>().Connect(
            port,
            new ClientOptions<Dictionary<string, object?>>
            {
                OnConnect = _ =>
                {
                    connected.TrySetResult(true);
                    return Task.CompletedTask;
                },
            });

        await connected.Task.WaitAsync(TimeSpan.FromSeconds(3));
    }

    [Fact]
    public async Task RequestContext_Exposes_ContextRaw_Sender_And_Context_Decoding()
    {
        var sender = new Channel<Dictionary<string, object?>>()
            .Post("ctx", request =>
            {
                var context = request.Context<Dictionary<string, string>>();
                return $"{request.ContextRaw is not null}:{request.Sender is not null}:{context?["name"]}";
            })
            .Process(new Dictionary<string, object?>());

        var value = await sender.SendAsync<string>(
            "ctx",
            null,
            new Dictionary<string, string> { ["name"] = "alice" });

        Assert.Equal("True:True:alice", value);
        sender.Stop();
    }

    private static async Task<object?> ThrowPathAsync()
    {
        await Task.Yield();
        throw new InvalidOperationException("async-fail");
    }

    private static async IAsyncEnumerable<object?> ThrowingStream()
    {
        yield return 1;
        await Task.Yield();
        throw new InvalidOperationException("stream-enum-fail");
    }

    private static async IAsyncEnumerable<object?> Yield(object value)
    {
        yield return value;
        await Task.Yield();
    }

    private static async Task<T?> FirstAsync<T>(IAsyncEnumerable<T> values)
    {
        var hasValue = false;
        T? first = default;

        await foreach (var value in values)
        {
            if (!hasValue)
            {
                hasValue = true;
                first = value;
            }
        }

        return first;
    }

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static async Task WaitUntilAsync(Func<bool> predicate, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!predicate())
        {
            if (DateTime.UtcNow >= deadline)
            {
                throw new TimeoutException("Timed out while waiting for condition.");
            }

            await Task.Delay(10);
        }
    }

    private static object? Invoke(object target, string method, params object?[] args)
    {
        var types = args.Select(arg => arg?.GetType() ?? typeof(object)).ToArray();
        var info = target.GetType().GetMethod(
            method,
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic,
            null,
            types,
            null);

        if (info is null)
        {
            throw new MissingMethodException(target.GetType().FullName, method);
        }

        return info.Invoke(target, args);
    }

    private static T GetNonPublicField<T>(object target, string fieldName)
    {
        var field = target.GetType().GetField(fieldName, System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        if (field is null)
        {
            throw new MissingFieldException(target.GetType().FullName, fieldName);
        }

        return (T)field.GetValue(target)!;
    }

    private static T GetNonPublicProperty<T>(object target, string propertyName)
    {
        var property = target.GetType().GetProperty(
            propertyName,
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        if (property is null)
        {
            throw new MissingMemberException(target.GetType().FullName, propertyName);
        }

        return (T)property.GetValue(target)!;
    }

    private static void SetNonPublicField(object target, string fieldName, object value)
    {
        var field = target.GetType().GetField(fieldName, System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        if (field is null)
        {
            throw new MissingFieldException(target.GetType().FullName, fieldName);
        }

        field.SetValue(target, value);
    }

    private static void SetNonPublicProperty(object target, string propertyName, object value)
    {
        var property = target.GetType().GetProperty(
            propertyName,
            System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public | System.Reflection.BindingFlags.NonPublic);
        if (property?.SetMethod is null)
        {
            throw new MissingMemberException(target.GetType().FullName, propertyName);
        }

        property.SetValue(target, value);
    }

    private static Task InvokeTask(object target, string method)
    {
        var info = target.GetType().GetMethod(method, System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.NonPublic);
        if (info is null)
        {
            throw new MissingMethodException(target.GetType().FullName, method);
        }

        return (Task)info.Invoke(target, null)!;
    }

    private sealed class ThrowingAsyncDisposable : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            throw new InvalidOperationException("dispose-fail");
        }
    }

    private sealed class TestConnection : IConnection
    {
        public long Send(object body)
        {
            return 0;
        }

        public void Sent(long id)
        {
        }

        public bool Cancel(long id)
        {
            return false;
        }

        public void Notify(object body)
        {
        }

        public Func<bool> AddTopic(string topic, Action<JsonElement> onEvent)
        {
            return static () => false;
        }

        public void Stop()
        {
        }
    }

    private sealed class CapturingController : IChannelController<Dictionary<string, object?>>
    {
        public CapturingController(ChannelSender<Dictionary<string, object?>> sender)
        {
            Sender = sender;
            State = [];
        }

        public List<ChannelMessage> Responses { get; } = [];

        public ChannelSender<Dictionary<string, object?>> Sender { get; }

        public Dictionary<string, object?> State { get; }

        public void Response(ChannelMessage response)
        {
            Responses.Add(response);
        }

        public void Subscribe(string topic)
        {
        }

        public void Unsubscribe(string topic)
        {
        }

        public void Event(string topic, object body)
        {
        }
    }
}
