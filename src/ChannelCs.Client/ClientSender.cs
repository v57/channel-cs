using System.Text.Json;

namespace ChannelCs.Client;

public sealed class ClientSender<TState> : IChannelSender, IDisposable
{
    internal ClientSender(ChannelSender<TState> sender, WebSocketClient webSocket)
    {
        Sender = sender;
        WebSocket = webSocket;
    }

    public ChannelSender<TState> Sender { get; }

    public WebSocketClient WebSocket { get; }

    public Task<T?> SendAsync<T>(
        string path,
        object? body = null,
        object? context = null,
        CancellationToken cancellationToken = default)
    {
        return Sender.SendAsync<T>(path, body, context, cancellationToken);
    }

    public IRequestHandle<T> Request<T>(string path, object? body = null, object? context = null)
    {
        return Sender.Request<T>(path, body, context);
    }

    public IAsyncEnumerable<T> ValuesAsync<T>(
        string path,
        object? body = null,
        object? context = null,
        CancellationToken cancellationToken = default)
    {
        return Sender.ValuesAsync<T>(path, body, context, cancellationToken);
    }

    public IAsyncEnumerable<T> Values<T>(
        string path,
        object? body = null,
        object? context = null,
        CancellationToken cancellationToken = default)
    {
        return Sender.Values<T>(path, body, context, cancellationToken);
    }

    public Task<ICancellable> SubscribeAsync(
        string path,
        object? body,
        Action<JsonElement> onEvent,
        object? context = null,
        CancellationToken cancellationToken = default)
    {
        return Sender.SubscribeAsync(path, body, onEvent, context, cancellationToken);
    }

    public void Stop()
    {
        Sender.Stop();
    }

    public void Dispose()
    {
        Stop();
        WebSocket.Dispose();
    }
}
