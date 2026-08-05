namespace ChannelCs.Client;

public sealed class ClientOptions<TState>
{
    public Func<ValueTask<IReadOnlyDictionary<string, string>>>? Headers { get; init; }

    public Func<ChannelSender<TState>, Task>? OnConnect { get; init; }

    public Func<TState>? StateFactory { get; init; }
}
