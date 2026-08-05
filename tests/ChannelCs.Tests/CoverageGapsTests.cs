using System.Collections;
using System.Collections.Concurrent;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;
using ChannelCs.Client;
using ChannelCs.Server;

namespace ChannelCs.Tests;

public sealed class CoverageGapsTests
{
    [Fact]
    public void CancellableRequest_Branches_Are_Covered()
    {
        var cancelled = 0;
        var request = CreateCancellableRequest(() => cancelled += 1);
        request.OnCancel(() => throw new InvalidOperationException("ignored-cancel-action"));
        request.Cancel();

        var immediate = 0;
        request.OnCancel(() => immediate += 1);
        request.Cancel();
        InvokeNonPublicVoid(request, "CancelFromDisconnect");

        var withoutActions = CreateCancellableRequest(() => { });
        InvokeNonPublicVoid(withoutActions, "CancelFromDisconnect");

        var disconnectThrowingAction = CreateCancellableRequest(() => { });
        disconnectThrowingAction.OnCancel(() => throw new InvalidOperationException("ignored-disconnect-action"));
        InvokeNonPublicVoid(disconnectThrowingAction, "CancelFromDisconnect");

        Assert.Equal(1, cancelled);
        Assert.Equal(1, immediate);
    }

    [Fact]
    public async Task ChannelSender_Request_And_SendAsync_Cancellation_Branches_Are_Covered()
    {
        var channel = new Channel<Dictionary<string, object?>>();
        var connection = new RecordingConnection { CancelResult = false };
        var sender = channel.CreateSender(connection);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(20));
        var cancelled = await Assert.ThrowsAsync<ChannelException>(
            () => sender.SendAsync<object?>("never", cancellationToken: cts.Token));
        Assert.Equal("cancelled", cancelled.Message);

        var twice = sender.Request<object?>("twice");
        twice.Cancel();
        twice.Cancel();

        var hack = sender.Request<object?>("hack");
        SetRequestHandleState(hack, requestId: 777, transportId: null);
        hack.Cancel();

        Assert.Contains(connection.Notifications, message => message.Cancel == 777);
    }

    [Fact]
    public async Task ChannelSender_Subscribe_MissingTopic_And_Cancellation_Branches_Are_Covered()
    {
        var channel = new Channel<Dictionary<string, object?>>();
        var connection = new RecordingConnection { CancelResult = true };
        var sender = channel.CreateSender(connection);
        var controller = new RecordingController(sender);

        var missingTopic = sender.SubscribeAsync("sub", null, _ => { });
        var request = Assert.IsType<ChannelMessage>(connection.SentBodies.Last());
        channel.Receive(new ChannelMessage { Id = request.Id }, controller);

        var missingTopicException = await Assert.ThrowsAsync<ChannelException>(() => missingTopic);
        Assert.Equal("subscription topic missing", missingTopicException.Message);

        using var cts = new CancellationTokenSource();
        var cancelledTask = sender.SubscribeAsync("sub", null, _ => { }, cancellationToken: cts.Token);
        cts.Cancel();
        await Assert.ThrowsAsync<OperationCanceledException>(() => cancelledTask);

        Assert.NotEmpty(connection.CancelIds);
        Assert.NotEmpty(connection.SentIds);
    }

    [Fact]
    public async Task ValuesStream_Error_EnsureStarted_And_Return_Branches_Are_Covered()
    {
        var channel = new Channel<Dictionary<string, object?>>();
        var connection = new RecordingConnection { CancelResult = false };
        var sender = channel.CreateSender(connection);
        var controller = new RecordingController(sender);

        var errorValues = sender.Values<int>("errors");
        var errorTask = ConsumeStream(errorValues);
        var errorRequest = WaitForSentMessage(connection, message => !string.IsNullOrEmpty(message.Stream));
        channel.Receive(new ChannelMessage { Id = errorRequest.Id, Error = "stream-error" }, controller);
        var streamException = await Assert.ThrowsAsync<ChannelException>(() => errorTask);
        Assert.Equal("stream-error", streamException.Message);

        var manualValues = sender.Values<int>("manual");
        InvokeNonPublicVoid(manualValues, "EnsureStarted");
        InvokeNonPublicVoid(manualValues, "EnsureStarted");

        SetNonPublicField(manualValues, "_transportId", null);
        await InvokeNonPublicAwaitable(manualValues, "ReturnAsync");
        InvokeNonPublicVoid(manualValues, "MarkTransportSent");

        Assert.Contains(connection.SentBodies, body => body is ChannelMessage message && message.Cancel is not null);
    }

    [Fact]
    public async Task Channel_Async_Path_And_Disconnect_Success_Dispose_Branches_Are_Covered()
    {
        var channel = new Channel<Dictionary<string, object?>>()
            .Post(
                "later",
                (Func<ChannelRequestContext<Dictionary<string, object?>>, Task<object?>>)(async _ =>
                {
                    await Task.Yield();
                    return "ok";
                }));
        var sender = channel.CreateSender(new RecordingConnection());
        var controller = new RecordingController(sender);

        channel.Receive(new ChannelMessage { Id = 31, Path = "later" }, controller);
        await Task.Delay(30);
        Assert.Contains(controller.Responses, response => response.Id == 31 && (string?)response.Body == "ok");

        var requestHandle = sender.Request<object?>("pending");
        var pendingMap = GetNonPublicField<IDictionary>(channel, "_requests");
        var pending = pendingMap.Values.Cast<object>().First();
        var requestProperty = pending.GetType().GetProperty("Request", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)!;
        var pendingRequest = (ChannelMessage)requestProperty.GetValue(pending)!;
        Assert.Equal("pending", pendingRequest.Path);

        var streams = GetNonPublicProperty<ConcurrentDictionary<int, IAsyncDisposable>>(sender, "Streams");
        streams[99] = new NoopAsyncDisposable();
        channel.Disconnect(new Dictionary<string, object?>(), sender);

        requestHandle.Cancel();
    }

    [Fact]
    public async Task ChannelServer_ReceiveLoop_And_Internal_Nested_Branches_Are_Covered()
    {
        var channel = new Channel<Dictionary<string, object?>>();
        var sender = channel.CreateSender(new RecordingConnection());
        using var server = channel.Listen("127.0.0.1:0");
        var controller = CreateServerController(sender);

        var receiveObjectDisposed = new ScriptedWebSocket(_ => WebSocketState.Open);
        receiveObjectDisposed.EnqueueReceiveException(new ObjectDisposedException("disposed"));
        await InvokeServerReceiveLoop(server, receiveObjectDisposed, controller);

        var receiveClose = new ScriptedWebSocket(_ => WebSocketState.Open);
        receiveClose.EnqueueReceiveClose();
        await InvokeServerReceiveLoop(server, receiveClose, controller);

        var receiveInvalidJson = new ScriptedWebSocket(_ => WebSocketState.Open);
        receiveInvalidJson.EnqueueReceiveText("{");
        await InvokeServerReceiveLoop(server, receiveInvalidJson, controller);
        Assert.True(receiveInvalidJson.CloseAsyncCalls > 0);

        InvokeNonPublicVoid(server, "Publish", new SubscriptionEvent("topic", null));

        var subscriptions = new ServerSubscriptions();
        var body = CreateBodyContext(sender, subscriptions);
        Assert.NotNull(body.Subscriptions);

        var closedConnectionSocket = new ScriptedWebSocket(_ => WebSocketState.Closed);
        var closedConnection = CreateServerConnection(closedConnectionSocket, subscriptions);
        Assert.Equal(0L, (long)InvokeNonPublic(closedConnection, "Send", new ChannelMessage { Path = "x" })!);
        InvokeNonPublicVoid(closedConnection, "Sent", 1L);
        Assert.False((bool)InvokeNonPublic(closedConnection, "Cancel", 2L)!);
        var removeTopic = (Func<bool>)InvokeNonPublic(
            closedConnection,
            "AddTopic",
            "topic",
            (Action<JsonElement>)(_ => { }))!;
        Assert.True(removeTopic());

        var stateType = GetServerNestedType("ConnectionState");
        var state = Activator.CreateInstance(
            stateType,
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
            null,
            [closedConnection, new ConcurrentDictionary<string, byte>(), body],
            null)!;
        var bodyValue = stateType.GetProperty("Body", BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)!.GetValue(state);
        Assert.NotNull(bodyValue);

        var secondCheckClosedSocket = new ScriptedWebSocket(read => read == 1 ? WebSocketState.Open : WebSocketState.Closed);
        var secondCheckClosedConnection = CreateServerConnection(secondCheckClosedSocket, subscriptions);
        InvokeNonPublicVoid(secondCheckClosedConnection, "Notify", new ChannelMessage { Path = "x" });
        await Task.Delay(20);

        var sendThrowsSocket = new ScriptedWebSocket(_ => WebSocketState.Open) { SendException = new InvalidOperationException("send-fail") };
        var sendThrowsConnection = CreateServerConnection(sendThrowsSocket, subscriptions);
        InvokeNonPublicVoid(sendThrowsConnection, "Notify", new ChannelMessage { Path = "x" });
        await Task.Delay(20);

        var eventCalls = 0;
        var serverController = CreateServerController(
            sender,
            onEvent: (_, _) => eventCalls += 1);
        InvokeNonPublicVoid(serverController, "Event", "topic", new { value = 1 });
        Assert.Equal(1, eventCalls);
    }

    [Fact]
    public async Task Server_StateFactory_Receives_Query_Parameters_As_Headers()
    {
        var port = GetFreePort();
        IReadOnlyDictionary<string, string?>? seenHeaders = null;
        using var server = new Channel<Dictionary<string, object?>>().Listen(
            $"127.0.0.1:{port}",
            new ListenOptions<Dictionary<string, object?>>
            {
                StateFactory = headers =>
                {
                    seenHeaders = headers;
                    return ValueTask.FromResult(new Dictionary<string, object?>());
                },
            });

        using var socket = new ClientWebSocket();
        await socket.ConnectAsync(new Uri($"ws://127.0.0.1:{port}/?token=abc"), CancellationToken.None);
        await WaitUntilAsync(() => seenHeaders is not null, TimeSpan.FromSeconds(2));

        Assert.NotNull(seenHeaders);
        Assert.Equal("abc", seenHeaders!["token"]);
    }

    [Fact]
    public async Task Subscription_BodyFactory_Getter_And_JsonCodec_Whitespace_Branches_Are_Covered()
    {
        var subscription = new Subscription();
        var published = new List<SubscriptionEvent>();
        subscription.AddPublisher(new ListPublisher(published));

        var factory = subscription.BodyFactory;
        Assert.NotNull(factory);
        Assert.Null(await factory("x"));

        await InvokeNonPublicAwaitable(
            subscription,
            "PublishAsync",
            "topic",
            new ValueTask<object?>(Task.FromResult<object?>("body")));
        Assert.Contains(published, @event => @event.Topic == "topic" && (string?)@event.Body == "body");

        Assert.ThrowsAny<JsonException>(() => JsonCodec.Deserialize(Encoding.UTF8.GetBytes(" \t\r\n ")));
    }

    [Fact]
    public async Task WebSocketClient_Stop_And_SendRaw_Error_Branches_Are_Covered()
    {
        using var client = new WebSocketClient(new Uri("ws://127.0.0.1:1"));

        var sendThrows = CreateClientWebSocketShim(
            state: WebSocketState.Open,
            socket: new ThrowingInnerWebSocket(sendException: new InvalidOperationException("send-fail")));
        SetNonPublicField(client, "_socket", sendThrows);
        await InvokeNonPublicAwaitable(client, "SendRawAsync", "one");

        var sendLock = GetNonPublicField<SemaphoreSlim>(client, "_sendLock");
        await sendLock.WaitAsync();
        var openSocket = CreateClientWebSocketShim(
            state: WebSocketState.Open,
            socket: new ThrowingInnerWebSocket());
        SetNonPublicField(client, "_socket", openSocket);
        var blockedSend = InvokeNonPublicAwaitable(client, "SendRawAsync", "two");
        var closedSocket = CreateClientWebSocketShim(
            state: WebSocketState.Closed,
            socket: new ThrowingInnerWebSocket(state: WebSocketState.Closed));
        SetNonPublicField(client, "_socket", closedSocket);
        sendLock.Release();
        await blockedSend;

        var stopSocket = CreateClientWebSocketShim(
            state: WebSocketState.Open,
            socket: new ThrowingInnerWebSocket(
                abortException: new InvalidOperationException("abort-fail"),
                disposeException: new InvalidOperationException("dispose-fail")));
        SetNonPublicField(client, "_socket", stopSocket);
        client.Stop();
    }

    private static ChannelMessage WaitForSentMessage(RecordingConnection connection, Func<ChannelMessage, bool> predicate)
    {
        var started = DateTime.UtcNow;
        while ((DateTime.UtcNow - started) < TimeSpan.FromSeconds(2))
        {
            foreach (var body in connection.SentBodies)
            {
                if (body is ChannelMessage message && predicate(message))
                {
                    return message;
                }
            }

            Thread.Sleep(10);
        }

        throw new TimeoutException("Timed out waiting for sent channel message.");
    }

    private static async Task ConsumeStream<T>(IAsyncEnumerable<T> values)
    {
        await foreach (var _ in values)
        {
        }
    }

    private static CancellableRequest CreateCancellableRequest(Action onCancelled)
    {
        return (CancellableRequest)Activator.CreateInstance(
            typeof(CancellableRequest),
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
            null,
            [onCancelled],
            null)!;
    }

    private static void SetRequestHandleState<T>(IRequestHandle<T> handle, int? requestId, long? transportId)
    {
        var cancel = GetNonPublicField<Action>(handle!, "_cancel");
        var closure = cancel.Target!;
        var fields = closure.GetType().GetFields(BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public);

        var requestField = fields.First(field => field.Name.Contains("requestId", StringComparison.OrdinalIgnoreCase));
        requestField.SetValue(closure, requestId);

        var transportField = fields.First(field => field.Name.Contains("transportId", StringComparison.OrdinalIgnoreCase));
        transportField.SetValue(closure, transportId);
    }

    private static object CreateServerController(
        ChannelSender<Dictionary<string, object?>> sender,
        Action<string, object>? onEvent = null)
    {
        var controllerType = GetServerNestedType("Controller");
        var onEventCallback = onEvent ?? ((_, _) => { });
        return Activator.CreateInstance(
            controllerType,
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
            null,
            [
                (Action<ChannelMessage>)(_ => { }),
                (Action<string>)(_ => { }),
                (Action<string>)(_ => { }),
                (Action<string, object>)((topic, body) => onEventCallback(topic, body)),
                sender,
                new Dictionary<string, object?>()
            ],
            null)!;
    }

    private static BodyContext<Dictionary<string, object?>> CreateBodyContext(
        ChannelSender<Dictionary<string, object?>> sender,
        ServerSubscriptions subscriptions)
    {
        return (BodyContext<Dictionary<string, object?>>)Activator.CreateInstance(
            typeof(BodyContext<Dictionary<string, object?>>),
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
            null,
            [new Dictionary<string, object?>(), sender, subscriptions],
            null)!;
    }

    private static object CreateServerConnection(WebSocket socket, ServerSubscriptions subscriptions)
    {
        var serverConnectionType = GetServerNestedType("ServerConnection");
        return Activator.CreateInstance(
            serverConnectionType,
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic,
            null,
            [socket, subscriptions],
            null)!;
    }

    private static Type GetServerNestedType(string name)
    {
        var type = typeof(ChannelServer<Dictionary<string, object?>>).GetNestedType(name, BindingFlags.NonPublic)!;
        if (!type.ContainsGenericParameters)
        {
            return type;
        }

        return type.MakeGenericType(typeof(Dictionary<string, object?>));
    }

    private static async Task InvokeServerReceiveLoop(
        ChannelServer<Dictionary<string, object?>> server,
        WebSocket socket,
        object controller)
    {
        var task = (Task)InvokeNonPublic(server, "ReceiveLoopAsync", socket, controller)!;
        await task;
    }

    private static ClientWebSocket CreateClientWebSocketShim(WebSocketState state, WebSocket socket)
    {
        var clientSocket = (ClientWebSocket)RuntimeHelpers.GetUninitializedObject(typeof(ClientWebSocket));
        var internalStateType = typeof(ClientWebSocket).GetNestedType("InternalState", BindingFlags.NonPublic)!;

        SetFieldOnType(
            typeof(ClientWebSocket),
            clientSocket,
            "_state",
            Enum.Parse(internalStateType, "Connected"));

        var handleType = typeof(ClientWebSocket).Assembly.GetType("System.Net.WebSockets.WebSocketHandle")!;
        var handle = RuntimeHelpers.GetUninitializedObject(handleType);

        SetFieldOnType(handleType, handle, "_state", state);
        SetFieldOnType(handleType, handle, "<WebSocket>k__BackingField", socket);

        SetFieldOnType(typeof(ClientWebSocket), clientSocket, "_innerWebSocket", handle);
        return clientSocket;
    }

    private static object? InvokeNonPublic(object target, string methodName, params object?[] args)
    {
        var method = target.GetType().GetMethod(
            methodName,
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)!;
        return method.Invoke(target, args);
    }

    private static void InvokeNonPublicVoid(object target, string methodName, params object?[] args)
    {
        InvokeNonPublic(target, methodName, args);
    }

    private static async Task InvokeNonPublicAwaitable(object target, string methodName, params object?[] args)
    {
        var method = target.GetType().GetMethod(
            methodName,
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic)!;
        var result = method.Invoke(target, args);
        switch (result)
        {
            case Task task:
                await task;
                break;
            case ValueTask valueTask:
                await valueTask;
                break;
            default:
                throw new InvalidOperationException($"Method '{methodName}' did not return Task/ValueTask.");
        }
    }

    private static void SetFieldOnType(Type type, object target, string fieldName, object? value)
    {
        var field = type.GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)!;
        field.SetValue(target, value);
    }

    private static void SetNonPublicField(object target, string fieldName, object? value)
    {
        SetFieldOnType(target.GetType(), target, fieldName, value);
    }

    private static T GetNonPublicField<T>(object target, string fieldName)
    {
        var field = target.GetType().GetField(fieldName, BindingFlags.Instance | BindingFlags.NonPublic | BindingFlags.Public)!;
        return (T)field.GetValue(target)!;
    }

    private static T GetNonPublicProperty<T>(object target, string propertyName)
    {
        var property = target.GetType().GetProperty(propertyName, BindingFlags.Instance | BindingFlags.NonPublic)!;
        return (T)property.GetValue(target)!;
    }

    private static async Task WaitUntilAsync(Func<bool> predicate, TimeSpan timeout)
    {
        var deadline = DateTime.UtcNow + timeout;
        while (!predicate())
        {
            if (DateTime.UtcNow >= deadline)
            {
                throw new TimeoutException("Timed out waiting for condition.");
            }

            await Task.Delay(10);
        }
    }

    private static int GetFreePort()
    {
        var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private sealed class RecordingConnection : IConnection
    {
        public long NextId { get; set; } = 1000;

        public bool CancelResult { get; set; }

        public List<object> SentBodies { get; } = [];

        public List<long> SentIds { get; } = [];

        public List<long> CancelIds { get; } = [];

        public List<ChannelMessage> Notifications { get; } = [];

        public long Send(object body)
        {
            SentBodies.Add(body);
            return NextId++;
        }

        public void Sent(long id)
        {
            SentIds.Add(id);
        }

        public bool Cancel(long id)
        {
            CancelIds.Add(id);
            return CancelResult;
        }

        public void Notify(object body)
        {
            if (body is ChannelMessage message)
            {
                Notifications.Add(message);
            }
        }

        public Func<bool> AddTopic(string topic, Action<JsonElement> onEvent)
        {
            return static () => false;
        }

        public void Stop()
        {
        }
    }

    private sealed class RecordingController : IChannelController<Dictionary<string, object?>>
    {
        public RecordingController(ChannelSender<Dictionary<string, object?>> sender)
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

    private sealed class NoopAsyncDisposable : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            return ValueTask.CompletedTask;
        }
    }

    private sealed class ScriptedWebSocket : WebSocket
    {
        private readonly Func<int, WebSocketState> _stateByRead;
        private readonly Queue<ReceiveStep> _receives = [];
        private int _stateReads;

        public ScriptedWebSocket(Func<int, WebSocketState> stateByRead)
        {
            _stateByRead = stateByRead;
        }

        public int CloseAsyncCalls { get; private set; }

        public Exception? SendException { get; init; }

        public override WebSocketCloseStatus? CloseStatus => null;

        public override string? CloseStatusDescription => null;

        public override string? SubProtocol => null;

        public override WebSocketState State => _stateByRead(Interlocked.Increment(ref _stateReads));

        public void EnqueueReceiveException(Exception exception)
        {
            _receives.Enqueue(ReceiveStep.Exception(exception));
        }

        public void EnqueueReceiveClose()
        {
            _receives.Enqueue(ReceiveStep.Close());
        }

        public void EnqueueReceiveText(string value)
        {
            _receives.Enqueue(ReceiveStep.Text(value));
        }

        public override void Abort()
        {
        }

        public override Task CloseAsync(WebSocketCloseStatus closeStatus, string? statusDescription, CancellationToken cancellationToken)
        {
            CloseAsyncCalls += 1;
            return Task.CompletedTask;
        }

        public override Task CloseOutputAsync(WebSocketCloseStatus closeStatus, string? statusDescription, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public override void Dispose()
        {
        }

        public override Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken)
        {
            if (_receives.Count == 0)
            {
                return Task.FromResult(new WebSocketReceiveResult(0, WebSocketMessageType.Close, true));
            }

            var step = _receives.Dequeue();
            if (step.ExceptionValue is not null)
            {
                throw step.ExceptionValue;
            }

            if (step.MessageType == WebSocketMessageType.Text && step.Payload.Length > 0)
            {
                step.Payload.AsSpan().CopyTo(buffer.AsSpan());
            }

            return Task.FromResult(new WebSocketReceiveResult(
                step.Payload.Length,
                step.MessageType,
                step.EndOfMessage));
        }

        public override Task SendAsync(ArraySegment<byte> buffer, WebSocketMessageType messageType, bool endOfMessage, CancellationToken cancellationToken)
        {
            if (SendException is not null)
            {
                throw SendException;
            }

            return Task.CompletedTask;
        }

        private readonly record struct ReceiveStep(WebSocketMessageType MessageType, bool EndOfMessage, byte[] Payload, Exception? ExceptionValue)
        {
            public static ReceiveStep Exception(Exception exception)
            {
                return new ReceiveStep(WebSocketMessageType.Text, true, [], exception);
            }

            public static ReceiveStep Close()
            {
                return new ReceiveStep(WebSocketMessageType.Close, true, [], null);
            }

            public static ReceiveStep Text(string value)
            {
                return new ReceiveStep(WebSocketMessageType.Text, true, Encoding.UTF8.GetBytes(value), null);
            }
        }
    }

    private sealed class ThrowingInnerWebSocket : WebSocket
    {
        private readonly Exception? _abortException;
        private readonly Exception? _disposeException;
        private readonly Exception? _sendException;
        private readonly WebSocketState _state;

        public ThrowingInnerWebSocket(
            Exception? abortException = null,
            Exception? disposeException = null,
            Exception? sendException = null,
            WebSocketState state = WebSocketState.Open)
        {
            _abortException = abortException;
            _disposeException = disposeException;
            _sendException = sendException;
            _state = state;
        }

        public override WebSocketCloseStatus? CloseStatus => null;

        public override string? CloseStatusDescription => null;

        public override string? SubProtocol => null;

        public override WebSocketState State => _state;

        public override void Abort()
        {
            if (_abortException is not null)
            {
                throw _abortException;
            }
        }

        public override Task CloseAsync(WebSocketCloseStatus closeStatus, string? statusDescription, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public override Task CloseOutputAsync(WebSocketCloseStatus closeStatus, string? statusDescription, CancellationToken cancellationToken)
        {
            return Task.CompletedTask;
        }

        public override void Dispose()
        {
            if (_disposeException is not null)
            {
                throw _disposeException;
            }
        }

        public override Task<WebSocketReceiveResult> ReceiveAsync(ArraySegment<byte> buffer, CancellationToken cancellationToken)
        {
            throw new NotSupportedException();
        }

        public override Task SendAsync(ArraySegment<byte> buffer, WebSocketMessageType messageType, bool endOfMessage, CancellationToken cancellationToken)
        {
            if (_sendException is not null)
            {
                throw _sendException;
            }

            return Task.CompletedTask;
        }
    }

    private sealed class ListPublisher : ISubscriptionPublisher
    {
        private readonly List<SubscriptionEvent> _published;

        public ListPublisher(List<SubscriptionEvent> published)
        {
            _published = published;
        }

        public void Publish(SubscriptionEvent @event)
        {
            _published.Add(@event);
        }
    }
}
