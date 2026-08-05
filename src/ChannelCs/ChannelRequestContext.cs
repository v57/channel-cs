namespace ChannelCs;

public sealed class ChannelRequestContext<TState>
{
    internal ChannelRequestContext(
        object? bodyRaw,
        object? contextRaw,
        ChannelSender<TState> sender,
        TState state,
        string path,
        CancellableRequest? task)
    {
        BodyRaw = bodyRaw;
        ContextRaw = contextRaw;
        Sender = sender;
        State = state;
        Path = path;
        Task = task;
    }

    public object? BodyRaw { get; }

    public object? ContextRaw { get; }

    public ChannelSender<TState> Sender { get; }

    public TState State { get; }

    public string Path { get; }

    public CancellableRequest? Task { get; }

    public T? Body<T>()
    {
        return JsonCodec.DeserializeBody<T>(BodyRaw);
    }

    public T? Context<T>()
    {
        return JsonCodec.DeserializeBody<T>(ContextRaw);
    }
}
