using System.Collections.Concurrent;
using System.Text.Json;
using System.Threading.Channels;

namespace ChannelCs;

public interface IConnection
{
    long Send(object body);

    void Sent(long id);

    bool Cancel(long id);

    void Notify(object body);

    Func<bool> AddTopic(string topic, Action<JsonElement> onEvent);

    void Stop();
}

public sealed class CancellableRequest : ICancellable
{
    private readonly Action _onCancelled;
    private readonly object _gate = new();
    private List<Action>? _cancelActions;

    internal CancellableRequest(Action onCancelled)
    {
        _onCancelled = onCancelled;
    }

    public bool IsCancelled { get; private set; }

    public void OnCancel(Action action)
    {
        lock (_gate)
        {
            if (IsCancelled)
            {
                action();
                return;
            }

            _cancelActions ??= [];
            _cancelActions.Add(action);
        }
    }

    public void Cancel()
    {
        List<Action>? actions;
        lock (_gate)
        {
            if (IsCancelled)
            {
                return;
            }

            IsCancelled = true;
            actions = _cancelActions;
        }

        if (actions is not null)
        {
            foreach (var action in actions)
            {
                try
                {
                    action();
                }
                catch
                {
                    // Keep parity with TS: ignore cancel callback errors.
                }
            }
        }

        _onCancelled();
    }

    internal void CancelFromDisconnect()
    {
        List<Action>? actions;
        lock (_gate)
        {
            if (IsCancelled)
            {
                return;
            }

            IsCancelled = true;
            actions = _cancelActions;
        }

        if (actions is null)
        {
            return;
        }

        foreach (var action in actions)
        {
            try
            {
                action();
            }
            catch
            {
                // Keep parity with TS: ignore cancel callback errors.
            }
        }
    }
}

public sealed class ChannelSender<TState> : IChannelSender
{
    private readonly Channel<TState> _channel;
    private readonly IConnection _connection;

    internal ChannelSender(Channel<TState> channel, IConnection connection)
    {
        _channel = channel;
        _connection = connection;
    }

    internal ConcurrentDictionary<int, IAsyncDisposable> Streams { get; } = new();

    internal ConcurrentDictionary<int, CancellableRequest> Requests { get; } = new();

    public int ActiveStreamCount => Streams.Count;

    public int ActiveRequestCount => Requests.Count;

    public async Task<T?> SendAsync<T>(
        string path,
        object? body = null,
        object? context = null,
        CancellationToken cancellationToken = default)
    {
        var request = Request<T>(path, body, context);
        using var registration = cancellationToken.CanBeCanceled
            ? cancellationToken.Register(static state => ((ICancellable)state!).Cancel(), request)
            : default;
        return await request.Response.ConfigureAwait(false);
    }

    public IRequestHandle<T> Request<T>(string path, object? body = null, object? context = null)
    {
        long? transportId = null;
        int? requestId = null;

        var tcs = new TaskCompletionSource<T?>(TaskCreationOptions.RunContinuationsAsynchronously);
        var request = _channel.MakeRequest(
            path,
            body,
            context,
            response =>
            {
                if (!string.IsNullOrEmpty(response.Error))
                {
                    tcs.TrySetException(new ChannelException(response.Error));
                }
                else
                {
                    tcs.TrySetResult(JsonCodec.DeserializeBody<T>(response.Body));
                }

                if (transportId is long sentId)
                {
                    _connection.Sent(sentId);
                    transportId = null;
                }

                requestId = null;
            },
            this);

        requestId = request.Id;
        transportId = _connection.Send(request);

        return new RequestHandle<T>(
            tcs.Task,
            () =>
            {
                if (requestId is not int rid)
                {
                    return;
                }

                if (transportId is long tid)
                {
                    var cancelled = _connection.Cancel(tid);
                    if (!cancelled)
                    {
                        _connection.Notify(new ChannelMessage { Cancel = rid });
                    }

                    _connection.Sent(tid);
                }
                else
                {
                    _connection.Notify(new ChannelMessage { Cancel = rid });
                }

                _channel.Cancel(rid);
                requestId = null;
                transportId = null;
                tcs.TrySetException(new ChannelException("cancelled"));
            });
    }

    public IAsyncEnumerable<T> ValuesAsync<T>(
        string path,
        object? body = null,
        object? context = null,
        CancellationToken cancellationToken = default)
    {
        return new ValuesStream<TState, T>(_channel, _connection, this, path, body, context, cancellationToken);
    }

    public IAsyncEnumerable<T> Values<T>(
        string path,
        object? body = null,
        object? context = null,
        CancellationToken cancellationToken = default)
    {
        return ValuesAsync<T>(path, body, context, cancellationToken);
    }

    public Task<ICancellable> SubscribeAsync(
        string path,
        object? body,
        Action<JsonElement> onEvent,
        object? context = null,
        CancellationToken cancellationToken = default)
    {
        long? transportId = null;
        var tcs = new TaskCompletionSource<ICancellable>(TaskCreationOptions.RunContinuationsAsynchronously);

        var request = _channel.MakeSubscription(
            path,
            body,
            context,
            response =>
            {
                if (!string.IsNullOrEmpty(response.Error))
                {
                    tcs.TrySetException(new ChannelException(response.Error));
                    return;
                }

                if (response.Topic is null)
                {
                    tcs.TrySetException(new ChannelException("subscription topic missing"));
                    return;
                }

                var cancelLocal = _connection.AddTopic(response.Topic, onEvent);
                tcs.TrySetResult(new ActionCancellable(() =>
                {
                    var unsub = cancelLocal();
                    if (unsub)
                    {
                        _connection.Notify(new ChannelMessage { Unsub = response.Topic });
                    }
                }));

                if (transportId is long sentId)
                {
                    _connection.Sent(sentId);
                    transportId = null;
                }
            },
            this);

        transportId = _connection.Send(request);

        if (cancellationToken.CanBeCanceled)
        {
            cancellationToken.Register(() =>
            {
                tcs.TrySetException(new OperationCanceledException(cancellationToken));
                if (transportId is long sentId)
                {
                    _connection.Cancel(sentId);
                    _connection.Sent(sentId);
                }
            });
        }

        return tcs.Task;
    }

    public void Stop()
    {
        _connection.Stop();
    }

    private sealed class RequestHandle<T> : IRequestHandle<T>
    {
        private readonly Action _cancel;

        public RequestHandle(Task<T?> response, Action cancel)
        {
            Response = response;
            _cancel = cancel;
        }

        public Task<T?> Response { get; }

        public void Cancel()
        {
            _cancel();
        }
    }

    private sealed class ValuesStream<TSenderState, TValue> : IAsyncEnumerable<TValue>
    {
        private readonly Channel<TSenderState> _channel;
        private readonly IConnection _connection;
        private readonly ChannelSender<TSenderState> _sender;
        private readonly string _path;
        private readonly object? _body;
        private readonly object? _context;
        private readonly System.Threading.Channels.Channel<ChannelMessage> _responses =
            System.Threading.Channels.Channel.CreateUnbounded<ChannelMessage>();
        private readonly CancellationToken _cancellationToken;

        private readonly object _gate = new();
        private bool _started;
        private bool _completed;
        private int? _requestId;
        private long? _transportId;

        public ValuesStream(
            Channel<TSenderState> channel,
            IConnection connection,
            ChannelSender<TSenderState> sender,
            string path,
            object? body,
            object? context,
            CancellationToken cancellationToken)
        {
            _channel = channel;
            _connection = connection;
            _sender = sender;
            _path = path;
            _body = body;
            _context = context;
            _cancellationToken = cancellationToken;
        }

        public async IAsyncEnumerator<TValue> GetAsyncEnumerator(CancellationToken cancellationToken = default)
        {
            EnsureStarted();
            using var linkedCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken, _cancellationToken);

            try
            {
                while (await _responses.Reader.WaitToReadAsync(linkedCts.Token).ConfigureAwait(false))
                {
                    while (_responses.Reader.TryRead(out var response))
                    {
                        if (!string.IsNullOrEmpty(response.Error))
                        {
                            throw new ChannelException(response.Error);
                        }

                        if (response.Done == true)
                        {
                            yield break;
                        }

                        yield return JsonCodec.DeserializeBody<TValue>(response.Body)!;
                    }
                }
            }
            finally
            {
                await ReturnAsync().ConfigureAwait(false);
            }
        }

        private void EnsureStarted()
        {
            lock (_gate)
            {
                if (_started)
                {
                    return;
                }

                _started = true;
                var request = _channel.MakeStream(
                    _path,
                    _body,
                    _context,
                    response =>
                    {
                        if (response.Done == true || !string.IsNullOrEmpty(response.Error))
                        {
                            lock (_gate)
                            {
                                _completed = true;
                                MarkTransportSent();
                            }

                            _responses.Writer.TryWrite(response);
                            _responses.Writer.TryComplete();
                            return;
                        }

                        _responses.Writer.TryWrite(response);
                    },
                    _sender);

                _requestId = request.Id;
                _transportId = _connection.Send(request);
            }
        }

        private async ValueTask ReturnAsync()
        {
            int? requestId;
            long? transportId;
            lock (_gate)
            {
                if (_completed || _requestId is null)
                {
                    return;
                }

                requestId = _requestId;
                transportId = _transportId;
                _requestId = null;
                _completed = true;
            }

            var rid = requestId.Value;

            if (transportId is long tid)
            {
                if (!_connection.Cancel(tid))
                {
                    _connection.Send(new ChannelMessage { Cancel = rid });
                }

                _connection.Sent(tid);
            }
            else
            {
                _connection.Send(new ChannelMessage { Cancel = rid });
            }

            _channel.Cancel(rid);
            _responses.Writer.TryComplete();
            await ValueTask.CompletedTask.ConfigureAwait(false);
        }

        private void MarkTransportSent()
        {
            if (_transportId is not long transportId)
            {
                return;
            }

            _connection.Sent(transportId);
            _transportId = null;
            _requestId = null;
        }
    }

    private sealed class ActionCancellable : ICancellable
    {
        private readonly Action _cancel;

        public ActionCancellable(Action cancel)
        {
            _cancel = cancel;
        }

        public void Cancel()
        {
            _cancel();
        }
    }
}
