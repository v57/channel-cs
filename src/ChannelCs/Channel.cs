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

        lock (_gate)
        {
            var toDelete = _requests
                .Where(pair => ReferenceEquals(pair.Value.Sender, sender))
                .Select(pair => pair.Key)
                .ToArray();

            foreach (var key in toDelete)
            {
                _requests.Remove(key);
            }
        }

        sender.Streams.Clear();
        sender.Requests.Clear();
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

            if (message.Body is not null)
            {
                controller.Event(message.Topic, message.Body);
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

    private void HandlePathRequest(ChannelMessage message, IChannelController<TState> controller)
    {
        var id = message.Id;
        var path = message.Path!;

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

            if (id is not int responseId)
            {
                _ = IgnoreValueTask(body);
                return;
            }

            if (body.IsCompletedSuccessfully)
            {
                controller.Response(new ChannelMessage { Id = responseId, Body = body.Result });
                controller.Sender.Requests.TryRemove(responseId, out _);
                return;
            }

            if (task is not null)
            {
                controller.Sender.Requests[responseId] = task;
            }

            _ = CompletePathRequestAsync(body, responseId, task, controller);
        }
        catch (Exception exception)
        {
            if (id is not int responseId)
            {
                return;
            }

            if (task?.IsCancelled != true)
            {
                controller.Response(new ChannelMessage { Id = responseId, Error = ToError(exception) });
            }

            if (task is not null)
            {
                controller.Sender.Requests.TryRemove(responseId, out _);
            }
        }
    }

    private async Task CompletePathRequestAsync(
        ValueTask<object?> bodyTask,
        int id,
        CancellableRequest? task,
        IChannelController<TState> controller)
    {
        try
        {
            var body = await bodyTask.ConfigureAwait(false);
            if (task?.IsCancelled != true)
            {
                controller.Response(new ChannelMessage { Id = id, Body = body });
            }
        }
        catch (Exception exception)
        {
            if (task?.IsCancelled != true)
            {
                controller.Response(new ChannelMessage { Id = id, Error = ToError(exception) });
            }
        }
        finally
        {
            if (task is not null)
            {
                controller.Sender.Requests.TryRemove(id, out _);
            }
        }
    }

    private void HandleStreamRequest(ChannelMessage message, IChannelController<TState> controller)
    {
        if (message.Id is not int id)
        {
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
        var id = message.Id;
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
                controller.Response(new ChannelMessage { Id = id, Topic = topic, Body = bodyTask.Result });
                return;
            }

            _ = CompleteSubscriptionAsync(bodyTask, id, topic, controller);
        }
        catch (Exception exception)
        {
            if (id is not null)
            {
                controller.Response(new ChannelMessage { Id = id, Error = ToError(exception) });
            }
        }
    }

    private async Task CompleteSubscriptionAsync(
        ValueTask<object?> bodyTask,
        int? id,
        string topic,
        IChannelController<TState> controller)
    {
        try
        {
            var body = await bodyTask.ConfigureAwait(false);
            controller.Subscribe(topic);
            controller.Response(new ChannelMessage { Id = id, Topic = topic, Body = body });
        }
        catch (Exception exception)
        {
            controller.Response(new ChannelMessage { Id = id, Error = ToError(exception) });
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
