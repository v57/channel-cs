using System.Collections.Concurrent;

namespace ChannelCs;

public class Channel<TState>
{
    private readonly object _gate = new();
    private int _id;
    private readonly Dictionary<int, PendingRequest> _requests = [];
    private readonly Dictionary<string, ChannelPostHandler<TState>> _postApi = [];
    private readonly List<(Func<string, bool> Path, ChannelPostHandler<TState> Handler)> _otherPostApi = [];
    private readonly Dictionary<string, ChannelStreamHandler<TState>> _streamApi = [];
    private readonly List<(Func<string, bool> Path, ChannelStreamHandler<TState> Handler)> _otherStreamApi = [];

    private Action<TState, ChannelSender<TState>>? _onDisconnect;
    private Dictionary<string, Subscription>? _eventsApi;

    public IReadOnlyDictionary<string, Subscription>? EventsApi => _eventsApi;

    public int PendingRequestsCount
    {
        get
        {
            lock (_gate)
            {
                return _requests.Count;
            }
        }
    }

    public Channel<TState> Post(string path, Func<ChannelRequestContext<TState>, object?> handler)
    {
        _postApi[path] = request => ValueTask.FromResult(handler(request));
        return this;
    }

    public Channel<TState> Post(string path, Func<ChannelRequestContext<TState>, Task<object?>> handler)
    {
        _postApi[path] = request => new ValueTask<object?>(handler(request));
        return this;
    }

    public Channel<TState> Post(string path, ChannelPostHandler<TState> handler)
    {
        _postApi[path] = handler;
        return this;
    }

    public Channel<TState> PostOther(Func<string, bool> pathPredicate, ChannelPostHandler<TState> handler)
    {
        _otherPostApi.Add((pathPredicate, handler));
        return this;
    }

    public Channel<TState> Stream(string path, Func<ChannelRequestContext<TState>, IAsyncEnumerable<object?>> handler)
    {
        _streamApi[path] = request => ValueTask.FromResult(handler(request));
        return this;
    }

    public Channel<TState> Stream(string path, Func<ChannelRequestContext<TState>, Task<IAsyncEnumerable<object?>>> handler)
    {
        _streamApi[path] = request => new ValueTask<IAsyncEnumerable<object?>>(handler(request));
        return this;
    }

    public Channel<TState> Stream(string path, ChannelStreamHandler<TState> handler)
    {
        _streamApi[path] = handler;
        return this;
    }

    public Channel<TState> StreamOther(Func<string, bool> pathPredicate, ChannelStreamHandler<TState> handler)
    {
        _otherStreamApi.Add((pathPredicate, handler));
        return this;
    }

    public Channel<TState> OnDisconnect(Action<TState, ChannelSender<TState>> action)
    {
        _onDisconnect = action;
        return this;
    }

    public void Disconnect(TState state, ChannelSender<TState> sender)
    {
        try
        {
            _onDisconnect?.Invoke(state, sender);
        }
        catch
        {
            // Keep parity with TS: ignore disconnect callback errors.
        }

        foreach (var stream in sender.Streams.Values)
        {
            try
            {
                stream.DisposeAsync().AsTask().GetAwaiter().GetResult();
            }
            catch
            {
                // Keep parity with TS: ignore stream dispose errors.
            }
        }

        foreach (var request in sender.Requests.Values)
        {
            request.CancelFromDisconnect();
        }

        // Outgoing calls of this sender can never be answered now: settle them instead of leaving them hanging.
        FailPending(sender, "disconnected");

        sender.Streams.Clear();
        sender.Requests.Clear();
    }

    /// <summary>
    /// Settles every outgoing request, stream and subscription made through <paramref name="sender"/> with
    /// <paramref name="error"/>. Used when the transport closes for good or receives a reply it can't decode,
    /// so no caller waits forever.
    /// </summary>
    public void FailPending(ChannelSender<TState> sender, string error)
    {
        List<(int Id, PendingRequest Request)> failed = [];
        lock (_gate)
        {
            foreach (var pair in _requests)
            {
                if (ReferenceEquals(pair.Value.Sender, sender))
                {
                    failed.Add((pair.Key, pair.Value));
                }
            }

            foreach (var item in failed)
            {
                _requests.Remove(item.Id);
            }
        }

        foreach (var item in failed)
        {
            try
            {
                item.Request.Response(new ChannelMessage { Id = item.Id, Error = error });
            }
            catch
            {
                // A failing response callback must not stop the others from settling.
            }
        }
    }

    internal ChannelMessage MakeRequest(
        string path,
        object? body,
        object? context,
        Action<ChannelMessage> response,
        ChannelSender<TState>? sender = null)
    {
        var id = NextId();
        var request = new ChannelMessage
        {
            Id = id,
            Path = path,
            Body = body,
            Context = context,
        };

        lock (_gate)
        {
            _requests[id] = new PendingRequest(PendingRequestType.Path, request, response, sender);
        }

        return request;
    }

    internal ChannelMessage MakeStream(
        string stream,
        object? body,
        object? context,
        Action<ChannelMessage> response,
        ChannelSender<TState>? sender = null)
    {
        var id = NextId();
        var request = new ChannelMessage
        {
            Id = id,
            Stream = stream,
            Body = body,
            Context = context,
        };

        lock (_gate)
        {
            _requests[id] = new PendingRequest(PendingRequestType.Stream, request, response, sender);
        }

        return request;
    }

    internal ChannelMessage MakeSubscription(
        string sub,
        object? body,
        object? context,
        Action<ChannelMessage> response,
        ChannelSender<TState>? sender = null)
    {
        var id = NextId();
        var request = new ChannelMessage
        {
            Id = id,
            Sub = sub,
            Body = body,
            Context = context,
        };

        lock (_gate)
        {
            _requests[id] = new PendingRequest(PendingRequestType.Subscription, request, response, sender);
        }

        return request;
    }

    public void Cancel(int id)
    {
        lock (_gate)
        {
            _requests.Remove(id);
        }
    }

    public void Receive(IReadOnlyList<ChannelMessage> messages, IChannelController<TState> controller)
    {
        foreach (var message in messages)
        {
            ReceiveOne(message, controller);
        }
    }

    public void Receive(ChannelMessage message, IChannelController<TState> controller)
    {
        ReceiveOne(message, controller);
    }

    private void ReceiveOne(ChannelMessage message, IChannelController<TState> controller)
    {
        if (!string.IsNullOrEmpty(message.Path))
        {
            HandlePathRequest(message, controller);
            return;
        }

        if (!string.IsNullOrEmpty(message.Stream))
        {
            HandleStreamRequest(message, controller);
            return;
        }

        if (message.Cancel is not null)
        {
            HandleCancel(message, controller);
            return;
        }

        if (!string.IsNullOrEmpty(message.Sub))
        {
            HandleSubscriptionRequest(message, controller);
            return;
        }

        if (!string.IsNullOrEmpty(message.Unsub))
        {
            controller.Unsubscribe(message.Unsub);
            return;
        }

        if (!string.IsNullOrEmpty(message.Topic))
        {
            if (message.Id is int topicRequestId)
            {
                PendingRequest? request;
                lock (_gate)
                {
                    _requests.TryGetValue(topicRequestId, out request);
                    _requests.Remove(topicRequestId);
                }

                request?.Response(message);
            }

            // TS parity: `if (some.body)` - falsy event bodies are not delivered.
            if (!Subscription.IsFalsy(message.Body))
            {
                controller.Event(message.Topic, message.Body!);
            }

            return;
        }

        if (message.Id is not int responseId)
        {
            return;
        }

        PendingRequest? pending;
        lock (_gate)
        {
            _requests.TryGetValue(responseId, out pending);
            if (pending is null)
            {
                return;
            }

            if (pending.Type != PendingRequestType.Stream || message.Done == true)
            {
                _requests.Remove(responseId);
            }
        }

        pending.Response(message);
    }

    public Channel<TState> Events(IDictionary<string, Subscription> events, string prefix = "")
    {
        _eventsApi ??= [];
        var normalizedPrefix = prefix.Length > 0 ? $"{prefix}/" : string.Empty;
        foreach (var pair in events)
        {
            pair.Value.Prefix = normalizedPrefix + pair.Key;
            _eventsApi[normalizedPrefix + pair.Key] = pair.Value;
        }

        return this;
    }

    public Channel<TState> Events(object events, string prefix = "")
    {
        _eventsApi ??= [];
        Subscription.Parse(events, prefix, _eventsApi);
        return this;
    }

    public Channel<TState> Api(IDictionary<string, object?> api)
    {
        ParseApi(api, string.Empty);
        return this;
    }

    public Channel<TState> Merge(Channel<TState> channel, string prefix = "")
    {
        var normalizedPrefix = prefix.Length > 0 ? $"{prefix}/" : string.Empty;

        if (channel._eventsApi is not null)
        {
            Events(channel._eventsApi, prefix);
        }

        foreach (var pair in channel._postApi)
        {
            Post(normalizedPrefix + pair.Key, pair.Value);
        }

        foreach (var pair in channel._streamApi)
        {
            Stream(normalizedPrefix + pair.Key, pair.Value);
        }

        return this;
    }

    public ChannelSender<TState> CreateSender(IConnection connection)
    {
        return new ChannelSender<TState>(this, connection);
    }

    private int NextId()
    {
        return Interlocked.Increment(ref _id) - 1;
    }

    private static ChannelMessage Reply(ChannelMessage request)
    {
        return new ChannelMessage { Id = request.Id, RawId = request.Id is null ? request.RawId : null };
    }

    private static bool HasId(ChannelMessage message)
    {
        return message.Id is not null || message.RawId is not null;
    }

    private void HandlePathRequest(ChannelMessage message, IChannelController<TState> controller)
    {
        var id = message.Id;
        var path = message.Path!;
        var hasId = HasId(message);

        var api = _postApi.TryGetValue(path, out var handler)
            ? handler
            : _otherPostApi.FirstOrDefault(entry => entry.Path(path)).Handler;

        CancellableRequest? task = null;

        try
        {
            if (api is null)
            {
                throw new ChannelException("api not found");
            }

            if (id is int requestId)
            {
                task = new CancellableRequest(() =>
                    controller.Response(new ChannelMessage
                    {
                        Id = requestId,
                        Error = "cancelled",
                    }));
            }

            var body = api(new ChannelRequestContext<TState>(
                message.Body,
                message.Context,
                controller.Sender,
                controller.State,
                path,
                task));

            if (!hasId)
            {
                _ = IgnoreValueTask(body);
                return;
            }

            if (body.IsCompletedSuccessfully)
            {
                var reply = Reply(message);
                reply.Body = body.Result;
                controller.Response(reply);
                if (id is int responseId)
                {
                    controller.Sender.Requests.TryRemove(responseId, out _);
                }

                return;
            }

            if (task is not null && id is int taskId)
            {
                controller.Sender.Requests[taskId] = task;
            }

            _ = CompletePathRequestAsync(body, message, task, controller);
        }
        catch (Exception exception)
        {
            if (!hasId)
            {
                return;
            }

            if (task?.IsCancelled != true)
            {
                var reply = Reply(message);
                reply.Error = ToError(exception);
                controller.Response(reply);
            }

            if (task is not null && id is int responseId)
            {
                controller.Sender.Requests.TryRemove(responseId, out _);
            }
        }
    }

    private async Task CompletePathRequestAsync(
        ValueTask<object?> bodyTask,
        ChannelMessage request,
        CancellableRequest? task,
        IChannelController<TState> controller)
    {
        try
        {
            var body = await bodyTask.ConfigureAwait(false);
            if (task?.IsCancelled != true)
            {
                var reply = Reply(request);
                reply.Body = body;
                controller.Response(reply);
            }
        }
        catch (Exception exception)
        {
            if (task?.IsCancelled != true)
            {
                var reply = Reply(request);
                reply.Error = ToError(exception);
                controller.Response(reply);
            }
        }
        finally
        {
            if (task is not null && request.Id is int id)
            {
                controller.Sender.Requests.TryRemove(id, out _);
            }
        }
    }

    private void HandleStreamRequest(ChannelMessage message, IChannelController<TState> controller)
    {
        if (message.Id is not int id)
        {
            if (message.RawId is not null)
            {
                var reply = Reply(message);
                reply.Error = "stream requires a numeric id";
                controller.Response(reply);
            }

            return;
        }

        try
        {
            var streamName = message.Stream!;
            var stream = _streamApi.TryGetValue(streamName, out var handler)
                ? handler
                : _otherStreamApi.FirstOrDefault(entry => entry.Path(streamName)).Handler;

            if (stream is null)
            {
                throw new ChannelException("api not found");
            }

            _ = StreamRequestAsync(id, streamName, message, stream, controller);
        }
        catch (Exception exception)
        {
            controller.Response(new ChannelMessage { Id = id, Error = ToError(exception) });
        }
    }

    private async Task StreamRequestAsync(
        int id,
        string path,
        ChannelMessage message,
        ChannelStreamHandler<TState> stream,
        IChannelController<TState> controller)
    {
        IAsyncEnumerable<object?> values;
        try
        {
            values = await stream(new ChannelRequestContext<TState>(
                message.Body,
                message.Context,
                controller.Sender,
                controller.State,
                path,
                task: null)).ConfigureAwait(false);
        }
        catch (Exception exception)
        {
            controller.Response(new ChannelMessage { Id = id, Error = ToError(exception) });
            return;
        }

        await using var enumerator = values.GetAsyncEnumerator();
        controller.Sender.Streams[id] = enumerator;

        try
        {
            while (await enumerator.MoveNextAsync().ConfigureAwait(false))
            {
                controller.Response(new ChannelMessage { Id = id, Body = enumerator.Current });
            }

            controller.Response(new ChannelMessage { Id = id, Done = true });
        }
        catch (Exception exception)
        {
            controller.Response(new ChannelMessage { Id = id, Error = ToError(exception) });
        }
        finally
        {
            controller.Sender.Streams.TryRemove(id, out _);
        }
    }

    private void HandleCancel(ChannelMessage message, IChannelController<TState> controller)
    {
        if (message.Cancel is not int cancelId)
        {
            return;
        }

        if (controller.Sender.Streams.TryRemove(cancelId, out var stream))
        {
            _ = stream.DisposeAsync();
        }

        if (controller.Sender.Requests.TryRemove(cancelId, out var request))
        {
            request.Cancel();
        }
    }

    private void HandleSubscriptionRequest(ChannelMessage message, IChannelController<TState> controller)
    {
        try
        {
            if (_eventsApi is null || !_eventsApi.TryGetValue(message.Sub!, out var subscription))
            {
                throw new ChannelException("subscription not found");
            }

            var topic = subscription.TopicFor(message.Body);
            topic = $"{subscription.Prefix}/{topic}";

            var bodyTask = subscription.BodyForAsync(message.Body);
            if (bodyTask.IsCompletedSuccessfully)
            {
                controller.Subscribe(topic);
                var reply = Reply(message);
                reply.Topic = topic;
                reply.Body = bodyTask.Result;
                controller.Response(reply);
                return;
            }

            _ = CompleteSubscriptionAsync(bodyTask, message, topic, controller);
        }
        catch (Exception exception)
        {
            if (HasId(message))
            {
                var reply = Reply(message);
                reply.Error = ToError(exception);
                controller.Response(reply);
            }
        }
    }

    private async Task CompleteSubscriptionAsync(
        ValueTask<object?> bodyTask,
        ChannelMessage request,
        string topic,
        IChannelController<TState> controller)
    {
        try
        {
            var body = await bodyTask.ConfigureAwait(false);
            controller.Subscribe(topic);
            var reply = Reply(request);
            reply.Topic = topic;
            reply.Body = body;
            controller.Response(reply);
        }
        catch (Exception exception)
        {
            var reply = Reply(request);
            reply.Error = ToError(exception);
            controller.Response(reply);
        }
    }

    private void ParseApi(IDictionary<string, object?> source, string prefix)
    {
        foreach (var pair in source)
        {
            var key = pair.Key;
            var value = pair.Value;

            if (value is null)
            {
                continue;
            }

            if (value is IDictionary<string, object?> nested)
            {
                ParseApi(nested, prefix.Length == 0 ? key : $"{prefix}/{key}");
                continue;
            }

            var path = key == "_" ? prefix : (prefix.Length == 0 ? key : $"{prefix}/{key}");

            switch (value)
            {
                case ChannelStreamHandler<TState> stream:
                    Stream(path, stream);
                    break;
                case ChannelPostHandler<TState> post:
                    Post(path, post);
                    break;
                case Func<ChannelRequestContext<TState>, Task<IAsyncEnumerable<object?>>> streamAsync:
                    Stream(path, streamAsync);
                    break;
                case Func<ChannelRequestContext<TState>, IAsyncEnumerable<object?>> streamSync:
                    Stream(path, streamSync);
                    break;
                case Func<ChannelRequestContext<TState>, Task<object?>> postAsync:
                    Post(path, postAsync);
                    break;
                case Func<ChannelRequestContext<TState>, object?> postSync:
                    Post(path, postSync);
                    break;
                case Func<Task<object?>> withoutContextAsync:
                    Post(path, _ => withoutContextAsync());
                    break;
                case Func<object?> withoutContext:
                    Post(path, _ => withoutContext());
                    break;
            }
        }
    }

    private static async Task IgnoreValueTask(ValueTask<object?> task)
    {
        try
        {
            await task.ConfigureAwait(false);
        }
        catch
        {
            // Keep parity with TS: fire-and-forget requests without id swallow errors.
        }
    }

    private static string ToError(Exception exception)
    {
        if (exception is ChannelException channelException)
        {
            return channelException.Message;
        }

        // Don't leak System.Text.Json internals (CLR type names, byte offsets) to the peer.
        if (exception is System.Text.Json.JsonException)
        {
            return "invalid body";
        }

        return exception.Message;
    }

    private sealed record PendingRequest(
        PendingRequestType Type,
        ChannelMessage Request,
        Action<ChannelMessage> Response,
        ChannelSender<TState>? Sender);

    private enum PendingRequestType
    {
        Path,
        Stream,
        Subscription,
    }
}
