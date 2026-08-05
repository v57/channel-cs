namespace ChannelCs.Server;

public sealed class ListenOptions<TState>
{
    public Func<IReadOnlyDictionary<string, string?>, ValueTask<TState>>? StateFactory { get; init; }

    public Action<BodyContext<TState>>? OnConnect { get; init; }

    public Action<BodyContext<TState>>? OnDisconnect { get; init; }
}

public sealed class BodyContext<TState>
{
    internal BodyContext(TState state, ChannelSender<TState> sender, ServerSubscriptions subscriptions)
    {
        State = state;
        Sender = sender;
        Subscriptions = subscriptions;
    }

    public TState State { get; }

    public ChannelSender<TState> Sender { get; }

    public ServerSubscriptions Subscriptions { get; }
}

public sealed class ServerSubscriptions
{
    private long _id;
    private readonly Dictionary<string, Dictionary<long, Action<System.Text.Json.JsonElement>>> _subscribed = [];

    public Func<bool> AddTopic(string topic, Action<System.Text.Json.JsonElement> onEvent)
    {
        var id = Interlocked.Increment(ref _id);
        if (!_subscribed.TryGetValue(topic, out var handlers))
        {
            handlers = [];
            _subscribed[topic] = handlers;
        }

        handlers[id] = onEvent;

        return () =>
        {
            if (!handlers.Remove(id))
            {
                return false;
            }

            if (handlers.Count > 0)
            {
                return false;
            }

            _subscribed.Remove(topic);
            return true;
        };
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
            handler(element);
        }
    }
}
