using System.Text.Json;

namespace ChannelCs;

public delegate ValueTask<object?> ChannelPostHandler<TState>(ChannelRequestContext<TState> request);

public delegate ValueTask<IAsyncEnumerable<object?>> ChannelStreamHandler<TState>(ChannelRequestContext<TState> request);

public interface ICancellable
{
    void Cancel();
}

public interface IRequestHandle<T> : ICancellable
{
    Task<T?> Response { get; }
}

public interface IChannelSender
{
    Task<T?> SendAsync<T>(
        string path,
        object? body = null,
        object? context = null,
        CancellationToken cancellationToken = default);

    IRequestHandle<T> Request<T>(string path, object? body = null, object? context = null);

    IAsyncEnumerable<T> ValuesAsync<T>(
        string path,
        object? body = null,
        object? context = null,
        CancellationToken cancellationToken = default);

    IAsyncEnumerable<T> Values<T>(
        string path,
        object? body = null,
        object? context = null,
        CancellationToken cancellationToken = default);

    Task<ICancellable> SubscribeAsync(
        string path,
        object? body,
        Action<JsonElement> onEvent,
        object? context = null,
        CancellationToken cancellationToken = default);

    void Stop();
}

public interface IChannelController<TState>
{
    void Response(ChannelMessage response);

    void Subscribe(string topic);

    void Unsubscribe(string topic);

    void Event(string topic, object body);

    ChannelSender<TState> Sender { get; }

    TState State { get; }
}
