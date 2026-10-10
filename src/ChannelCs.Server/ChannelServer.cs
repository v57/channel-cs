using System.Collections.Concurrent;
using System.Net.WebSockets;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Hosting;

namespace ChannelCs.Server;

public sealed class ChannelServer<TState> : IDisposable
{
    private readonly Channel<TState> _channel;
    private readonly WebApplication _app;
    private readonly ListenOptions<TState> _options;
    private readonly ConcurrentDictionary<Guid, ConnectionState> _connections = [];
    private readonly ISubscriptionPublisher _publisher;
    private int _isStopped;

    internal ChannelServer(Channel<TState> channel, WebApplication app, ListenOptions<TState> options)
    {
        _channel = channel;
        _app = app;
        _options = options;
        _publisher = new ServerPublisher(this);

        if (_channel.EventsApi is not null)
        {
            foreach (var subscription in _channel.EventsApi.Values)
            {
                subscription.AddPublisher(_publisher);
            }
        }
    }

    internal async Task HandleHttpAsync(HttpContext context)
    {
        if (!context.WebSockets.IsWebSocketRequest)
        {
            context.Response.StatusCode = StatusCodes.Status200OK;
            return;
        }

        var headers = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        foreach (var query in context.Request.Query)
        {
            headers[query.Key] = query.Value.ToString();
        }

        foreach (var header in context.Request.Headers)
        {
            headers[header.Key] = header.Value.ToString();
        }

        var state = _options.StateFactory is not null
            ? await _options.StateFactory(headers).ConfigureAwait(false)
            : ChannelStateFactory.CreateDefault<TState>();

        using var socket = await context.WebSockets.AcceptWebSocketAsync().ConfigureAwait(false);

        var subscriptions = new ServerSubscriptions();
        var connection = new ServerConnection(socket, subscriptions);
        var sender = _channel.CreateSender(connection);
        var topics = new ConcurrentDictionary<string, byte>();
        var connectionId = Guid.NewGuid();

        connection.SetStopAction(() =>
        {
            try
            {
                if (socket.State == WebSocketState.Open)
                {
                    socket.CloseAsync(WebSocketCloseStatus.NormalClosure, "stop", CancellationToken.None)
                        .GetAwaiter()
                        .GetResult();
                }
            }
            catch
            {
                // Ignore stop errors.
            }
        });

        var bodyContext = new BodyContext<TState>(state, sender, subscriptions);
        _connections[connectionId] = new ConnectionState(connection, topics, bodyContext);
        _options.OnConnect?.Invoke(bodyContext);

        var controller = new Controller(
            response: connection.Notify,
            subscribe: topic => topics[topic] = 0,
            unsubscribe: topic => topics.TryRemove(topic, out _),
            onEvent: subscriptions.ReceivedEvent,
            sender: sender,
            state: state);

        try
        {
            await ReceiveLoopAsync(socket, controller).ConfigureAwait(false);
        }
        finally
        {
            _connections.TryRemove(connectionId, out _);
            _channel.Disconnect(state, sender);
            _options.OnDisconnect?.Invoke(bodyContext);
        }
    }

    public void Stop()
    {
        if (Interlocked.Exchange(ref _isStopped, 1) != 0)
        {
            return;
        }

        if (_channel.EventsApi is not null)
        {
            foreach (var subscription in _channel.EventsApi.Values)
            {
                subscription.RemovePublisher(_publisher);
            }
        }

        foreach (var connection in _connections.Values)
        {
            connection.Connection.Stop();
        }

        _app.StopAsync().GetAwaiter().GetResult();
        _app.DisposeAsync().AsTask().GetAwaiter().GetResult();
    }

    public void Dispose()
    {
        Stop();
    }

    private async Task ReceiveLoopAsync(WebSocket socket, Controller controller)
    {
        var buffer = new byte[8192];
        using var stream = new MemoryStream();
        var maxMessageSize = _options.MaxMessageSize > 0 ? _options.MaxMessageSize : 16 * 1024 * 1024;

        while (socket.State == WebSocketState.Open)
        {
            stream.SetLength(0);

            WebSocketReceiveResult result;
            do
            {
                try
                {
                    result = await socket.ReceiveAsync(buffer, CancellationToken.None).ConfigureAwait(false);
                }
                catch (WebSocketException)
                {
                    return;
                }
                catch (ObjectDisposedException)
                {
                    return;
                }

                if (result.MessageType == WebSocketMessageType.Close)
                {
                    return;
                }

                if (result.MessageType != WebSocketMessageType.Text)
                {
                    continue;
                }

                if (stream.Length + result.Count > maxMessageSize)
                {
                    try
                    {
                        await socket.CloseAsync(WebSocketCloseStatus.MessageTooBig, "message too big", CancellationToken.None)
                            .ConfigureAwait(false);
                    }
                    catch
                    {
                        // Peer may already be gone.
                    }

                    return;
                }

                stream.Write(buffer.AsSpan(0, result.Count));
            }
            while (!result.EndOfMessage);

            if (stream.Length == 0)
            {
                continue;
            }

            IReadOnlyList<ChannelMessage> requests;
            try
            {
                requests = JsonCodec.Deserialize(stream.GetBuffer().AsMemory(0, (int)stream.Length));
            }
            catch
            {
                try
                {
                    await socket.CloseAsync(WebSocketCloseStatus.InvalidPayloadData, "invalid json", CancellationToken.None)
                        .ConfigureAwait(false);
                }
                catch
                {
                    // Peer may already be gone.
                }

                return;
            }

            try
            {
                _channel.Receive(requests, controller);
            }
            catch
            {
                // Keep one bad message (e.g. a throwing event handler) from killing this connection's loop.
            }
        }
    }

    private void Publish(SubscriptionEvent @event)
    {
        if (@event.Body is null)
        {
            return;
        }

        foreach (var connection in _connections.Values)
        {
            if (!connection.Topics.ContainsKey(@event.Topic))
            {
                continue;
            }

            connection.Connection.Notify(new ChannelMessage
            {
                Topic = @event.Topic,
                Body = @event.Body,
            });
        }
    }

    private sealed class ServerPublisher : ISubscriptionPublisher
    {
        private readonly ChannelServer<TState> _server;

        public ServerPublisher(ChannelServer<TState> server)
        {
            _server = server;
        }

        public void Publish(SubscriptionEvent @event)
        {
            _server.Publish(@event);
        }
    }

    private sealed class ConnectionState
    {
        public ConnectionState(ServerConnection connection, ConcurrentDictionary<string, byte> topics, BodyContext<TState> body)
        {
            Connection = connection;
            Topics = topics;
            Body = body;
        }

        public ServerConnection Connection { get; }

        public ConcurrentDictionary<string, byte> Topics { get; }

        public BodyContext<TState> Body { get; }
    }

    private sealed class ServerConnection : IConnection
    {
        private readonly WebSocket _socket;
        private readonly ServerSubscriptions _subscriptions;
        private readonly SemaphoreSlim _sendLock = new(1, 1);
        private Action? _stop;

        public ServerConnection(WebSocket socket, ServerSubscriptions subscriptions)
        {
            _socket = socket;
            _subscriptions = subscriptions;
        }

        public long Send(object body)
        {
            _ = SendRawAsync(body);
            return 0;
        }

        public void Sent(long id)
        {
            // No-op for server transport.
        }

        public bool Cancel(long id)
        {
            return false;
        }

        public void Notify(object body)
        {
            _ = SendRawAsync(body);
        }

        public Func<bool> AddTopic(string topic, Action<System.Text.Json.JsonElement> onEvent)
        {
            return _subscriptions.AddTopic(topic, onEvent);
        }

        public void Stop()
        {
            _stop?.Invoke();
        }

        public void SetStopAction(Action stop)
        {
            _stop = stop;
        }

        private async Task SendRawAsync(object body)
        {
            if (_socket.State != WebSocketState.Open)
            {
                return;
            }

            var bytes = JsonCodec.Serialize(body);
            await _sendLock.WaitAsync(CancellationToken.None).ConfigureAwait(false);
            try
            {
                if (_socket.State != WebSocketState.Open)
                {
                    return;
                }

                await _socket.SendAsync(bytes, WebSocketMessageType.Text, true, CancellationToken.None)
                    .ConfigureAwait(false);
            }
            catch
            {
                // Ignore send errors.
            }
            finally
            {
                _sendLock.Release();
            }
        }
    }

    private sealed class Controller : IChannelController<TState>
    {
        private readonly Action<ChannelMessage> _response;
        private readonly Action<string> _subscribe;
        private readonly Action<string> _unsubscribe;
        private readonly Action<string, object> _onEvent;

        public Controller(
            Action<ChannelMessage> response,
            Action<string> subscribe,
            Action<string> unsubscribe,
            Action<string, object> onEvent,
            ChannelSender<TState> sender,
            TState state)
        {
            _response = response;
            _subscribe = subscribe;
            _unsubscribe = unsubscribe;
            _onEvent = onEvent;
            Sender = sender;
            State = state;
        }

        public ChannelSender<TState> Sender { get; }

        public TState State { get; }

        public void Response(ChannelMessage response)
        {
            _response(response);
        }

        public void Subscribe(string topic)
        {
            _subscribe(topic);
        }

        public void Unsubscribe(string topic)
        {
            _unsubscribe(topic);
        }

        public void Event(string topic, object body)
        {
            _onEvent(topic, body);
        }
    }
}
