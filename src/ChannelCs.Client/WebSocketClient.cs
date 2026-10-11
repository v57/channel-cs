using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;

namespace ChannelCs.Client;

public sealed class WebSocketClient : IConnection, IDisposable
{
    private readonly Uri _address;
    private readonly Func<ValueTask<IReadOnlyDictionary<string, string>>>? _headers;
    private readonly CancellationTokenSource _lifetime = new();
    private readonly SemaphoreSlim _sendLock = new(1, 1);
    private readonly ConcurrentDictionary<long, object> _pending = new();
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<long, Action<JsonElement>>> _subscribed = new();
    private readonly object _stateGate = new();
    private readonly List<object> _queue = [];

    private ClientWebSocket? _socket;
    private long _id;
    private long _nextEventId;
    private int _isWaiting;
    private int _isWaitingLength;
    private int _attempts;

    /// <summary>Default cap for one incoming message (TS/Bun's default max payload).</summary>
    public const int DefaultMaxMessageSize = 16 * 1024 * 1024;

    public WebSocketClient(
        Uri address,
        Func<ValueTask<IReadOnlyDictionary<string, string>>>? headers = null,
        int maxMessageSize = DefaultMaxMessageSize)
    {
        _address = address;
        _headers = headers;
        MaxMessageSize = maxMessageSize > 0 ? maxMessageSize : DefaultMaxMessageSize;
        _ = Task.Run(RunAsync);
    }

    public int MaxMessageSize { get; }

    /// <summary>
    /// Wait before reconnect attempt <paramref name="attempt"/> (0 = first retry after a close): 100 ms doubling up to
    /// 5 s, scaled to 50-100% by <paramref name="random"/> (0..1) so clients don't retry in lockstep. Same as TS.
    /// </summary>
    public static TimeSpan ReconnectDelay(int attempt, double random) =>
        TimeSpan.FromMilliseconds(Math.Min(5000, 100 * Math.Pow(2, Math.Min(attempt, 16))) * (0.5 + random / 2));

    /// <summary>
    /// Called when the peer sends something this client can't use (undecodable reply, oversized or invalid
    /// frame, rejected handshake). Pending calls can't be matched to a reply any more, so the owner should
    /// settle them with this error instead of letting them wait forever.
    /// </summary>
    public Action<string>? OnProtocolError { get; set; }

    public Func<Task>? OnOpenAsync { get; set; }

    public Action? OnClose { get; set; }

    public Action<IReadOnlyList<ChannelMessage>>? OnMessage { get; set; }

    public bool IsConnected { get; private set; }

    public bool IsRunning { get; private set; } = true;

    public long Send(object body)
    {
        var id = Interlocked.Increment(ref _id) - 1;
        _pending[id] = body;

        if (!IsConnected)
        {
            return id;
        }

        object? immediate = null;

        lock (_stateGate)
        {
            switch (_isWaiting)
            {
                case 0:
                    immediate = body;
                    _isWaiting = 1;
                    _isWaitingLength = 0;
                    _ = TransitionToBatchModeAsync();
                    break;
                case 1:
                    immediate = body;
                    _isWaitingLength += 1;
                    if (_isWaitingLength > 4000)
                    {
                        _isWaiting = 3;
                    }

                    break;
                case 2:
                    _queue.Add(body);
                    break;
                case 3:
                    immediate = body;
                    _isWaiting = 2;
                    _ = FlushQueuedAsync();
                    break;
            }
        }

        if (immediate is not null)
        {
            _ = SendRawAsync(immediate);
        }

        return id;
    }

    public void Sent(long id)
    {
        _pending.TryRemove(id, out _);
    }

    public bool Cancel(long id)
    {
        if (IsConnected)
        {
            return false;
        }

        return _pending.TryRemove(id, out _);
    }

    public void Notify(object body)
    {
        _ = SendRawAsync(body);
    }

    public Func<bool> AddTopic(string topic, Action<JsonElement> onEvent)
    {
        var handlers = _subscribed.GetOrAdd(topic, static _ => []);
        var id = Interlocked.Increment(ref _nextEventId);
        handlers[id] = onEvent;

        return () =>
        {
            if (!handlers.TryRemove(id, out _))
            {
                return false;
            }

            if (handlers.IsEmpty)
            {
                _subscribed.TryRemove(topic, out _);
                return true;
            }

            return false;
        };
    }

    public void Stop()
    {
        IsRunning = false;
        _pending.Clear();
        _lifetime.Cancel();

        try
        {
            _socket?.Abort();
        }
        catch
        {
            // Ignore transport teardown errors.
        }

        try
        {
            _socket?.Dispose();
        }
        catch
        {
            // Ignore transport teardown errors.
        }
    }

    public void Throttle()
    {
        lock (_stateGate)
        {
            _isWaiting = 3;
        }
    }

    public void ReceivedEvent(string topic, object body)
    {
        if (!_subscribed.TryGetValue(topic, out var handlers))
        {
            return;
        }

        var element = JsonCodec.ToJsonElement(body);
        foreach (var handler in handlers.Values)
        {
            try
            {
                handler(element);
            }
            catch
            {
                // A throwing event handler must not tear down the receive loop.
            }
        }
    }

    public void Dispose()
    {
        Stop();
        _sendLock.Dispose();
        _lifetime.Dispose();
    }

    private async Task RunAsync()
    {
        while (IsRunning && !_lifetime.IsCancellationRequested)
        {
            ClientWebSocket? socket = null;
            try
            {
                socket = new ClientWebSocket();
                if (_headers is not null)
                {
                    var headers = await _headers().ConfigureAwait(false);
                    foreach (var header in headers)
                    {
                        if (!string.IsNullOrEmpty(header.Value))
                        {
                            socket.Options.SetRequestHeader(header.Key, header.Value);
                        }
                    }
                }

                try
                {
                    await socket.ConnectAsync(_address, _lifetime.Token).ConfigureAwait(false);
                }
                catch (WebSocketException exception) when (exception.WebSocketErrorCode == WebSocketError.NotAWebSocket)
                {
                    // The server answered but refused the upgrade (e.g. HTTP 500). Queued calls would be resent
                    // into the same refusal forever: fail them, keep retrying for new ones.
                    ReportProtocolError("connection rejected by server");
                    throw;
                }

                var replay = _pending.Values.ToArray();
                _socket = socket;
                IsConnected = true;
                _attempts = 0;

                if (OnOpenAsync is not null)
                {
                    await OnOpenAsync().ConfigureAwait(false);
                }

                if (replay.Length > 0)
                {
                    await SendRawAsync(replay).ConfigureAwait(false);
                }

                await ReceiveLoopAsync(socket).ConfigureAwait(false);
            }
            catch (OperationCanceledException) when (_lifetime.IsCancellationRequested)
            {
                break;
            }
            catch (WebSocketException exception) when (
                ReferenceEquals(_socket, socket)
                && exception.WebSocketErrorCode is not (WebSocketError.ConnectionClosedPrematurely or WebSocketError.Success))
            {
                // Protocol violation from the peer (bad frame, invalid UTF-8, ...): no reply will be matched.
                ReportProtocolError("invalid message from server");
            }
            catch
            {
                // Transport loss: keep retrying while running; pending requests are replayed on reconnect (TS parity).
            }
            finally
            {
                if (ReferenceEquals(_socket, socket))
                {
                    _socket = null;
                }

                IsConnected = false;
                OnClose?.Invoke();

                if (socket is not null)
                {
                    DisposeSocket(socket);
                }
            }

            try
            {
                // Back off while the server stays down instead of retrying every 100 ms forever.
                await Task.Delay(ReconnectDelay(_attempts++, Random.Shared.NextDouble()), _lifetime.Token)
                    .ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                break;
            }
        }
    }

    private async Task ReceiveLoopAsync(ClientWebSocket socket)
    {
        var buffer = new byte[8192];
        using var stream = new MemoryStream();

        while (IsRunning && socket.State == WebSocketState.Open && !_lifetime.IsCancellationRequested)
        {
            stream.SetLength(0);

            WebSocketReceiveResult result;
            do
            {
                result = await socket.ReceiveAsync(buffer, _lifetime.Token).ConfigureAwait(false);
                if (result.MessageType == WebSocketMessageType.Close)
                {
                    return;
                }

                // Binary messages are decoded like text ones: dropping them stranded the pending call.
                if (stream.Length + result.Count > MaxMessageSize)
                {
                    ReportProtocolError("message too big");
                    await CloseQuietlyAsync(socket, WebSocketCloseStatus.MessageTooBig, "message too big")
                        .ConfigureAwait(false);
                    return;
                }

                stream.Write(buffer.AsSpan(0, result.Count));
            }
            while (!result.EndOfMessage);

            if (stream.Length == 0)
            {
                continue;
            }

            IReadOnlyList<ChannelMessage> messages;
            try
            {
                messages = JsonCodec.Deserialize(stream.GetBuffer().AsMemory(0, (int)stream.Length));
            }
            catch (Exception exception) when (exception is JsonException or InvalidOperationException
                or ArgumentException or DecoderFallbackException)
            {
                // The reply can't be matched to its request: settle the pending calls instead of hanging them.
                ReportProtocolError("invalid message from server");
                continue;
            }

            try
            {
                OnMessage?.Invoke(messages);
            }
            catch
            {
                // Errors in response/handler callbacks stay local to that message.
            }
        }
    }

    private void ReportProtocolError(string error)
    {
        try
        {
            OnProtocolError?.Invoke(error);
        }
        catch
        {
            // Never let the owner's callback break the receive loop.
        }
    }

    private static async Task CloseQuietlyAsync(ClientWebSocket socket, WebSocketCloseStatus status, string reason)
    {
        try
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(1));
            await socket.CloseOutputAsync(status, reason, timeout.Token).ConfigureAwait(false);
        }
        catch
        {
            // Best effort; the socket is disposed by the caller.
        }
    }

    private async Task SendRawAsync(object body)
    {
        var socket = _socket;
        if (socket is null || socket.State != WebSocketState.Open)
        {
            return;
        }

        var bytes = JsonCodec.Serialize(body);
        await _sendLock.WaitAsync(_lifetime.Token).ConfigureAwait(false);
        try
        {
            socket = _socket;
            if (socket is null || socket.State != WebSocketState.Open)
            {
                return;
            }

            await socket.SendAsync(bytes, WebSocketMessageType.Text, true, _lifetime.Token).ConfigureAwait(false);
        }
        catch
        {
            // Keep parity with TS behavior: send failures are ignored and reconnect loop handles recovery.
        }
        finally
        {
            _sendLock.Release();
        }
    }

    private async Task TransitionToBatchModeAsync()
    {
        try
        {
            await Task.Delay(10, _lifetime.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        lock (_stateGate)
        {
            if (_isWaitingLength > 4000)
            {
                _isWaiting = 3;
            }
        }
    }

    private async Task FlushQueuedAsync()
    {
        try
        {
            await Task.Delay(1, _lifetime.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        object[]? queued = null;
        lock (_stateGate)
        {
            _isWaiting = 3;
            if (_queue.Count > 0)
            {
                queued = [.. _queue];
                _queue.Clear();
            }
        }

        if (queued is not null)
        {
            await SendRawAsync(queued).ConfigureAwait(false);
        }
    }

    [System.Diagnostics.CodeAnalysis.ExcludeFromCodeCoverage]
    private static void DisposeSocket(ClientWebSocket socket)
    {
        try
        {
            socket.Dispose();
        }
        catch
        {
            // Ignore transport teardown errors.
        }
    }
}
