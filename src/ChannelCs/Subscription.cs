namespace ChannelCs;

public readonly record struct SubscriptionEvent(string Topic, object? Body);

public interface ISubscriptionPublisher
{
    void Publish(SubscriptionEvent @event);
}

public sealed class Subscription
{
    private Func<object?, string> _topic = request => request?.ToString() ?? string.Empty;
    private Func<object?, ValueTask<object?>> _body = _ => ValueTask.FromResult<object?>(null);

    internal List<ISubscriptionPublisher> Publishers { get; } = [];

    internal string Prefix { get; set; } = string.Empty;

    public int PublisherCount => Publishers.Count;

    public Func<object?, ValueTask<object?>> BodyFactory
    {
        get => _body;
        set => _body = value;
    }

    public Subscription Topic(Func<object?, string> formatter)
    {
        _topic = formatter;
        return this;
    }

    // Intentional TS parity: Body replaces the topic formatter.
    public Subscription Body(Func<object?, string> formatter)
    {
        _topic = formatter;
        return this;
    }

    public void Send(object? request, object? body = null)
    {
        var topic = _topic(request);
        if (body is not null)
        {
            Publish(topic, body);
            return;
        }

        var valueTask = _body(request);
        if (valueTask.IsCompletedSuccessfully)
        {
            Publish(topic, valueTask.Result);
            return;
        }

        _ = PublishAsync(topic, valueTask);
    }

    public void Publish(string topic, object? body)
    {
        if (body is null)
        {
            return;
        }

        var eventTopic = Prefix.Length > 0 ? $"{Prefix}/{topic}" : topic;
        var @event = new SubscriptionEvent(eventTopic, body);
        foreach (var publisher in Publishers)
        {
            publisher.Publish(@event);
        }
    }

    public void AddPublisher(ISubscriptionPublisher publisher)
    {
        Publishers.Add(publisher);
    }

    public void RemovePublisher(ISubscriptionPublisher publisher)
    {
        Publishers.Remove(publisher);
    }

    internal string TopicFor(object? request)
    {
        return _topic(request);
    }

    internal ValueTask<object?> BodyForAsync(object? request)
    {
        return _body(request);
    }

    internal static void Parse(object source, string prefix, Dictionary<string, Subscription> map)
    {
        if (source is null)
        {
            return;
        }

        if (source is IDictionary<string, object?> objectDictionary)
        {
            foreach (var pair in objectDictionary)
            {
                ParseOne(pair.Key, pair.Value, prefix, map);
            }

            return;
        }

        if (source is IDictionary<string, Subscription> subscriptionDictionary)
        {
            foreach (var pair in subscriptionDictionary)
            {
                ParseOne(pair.Key, pair.Value, prefix, map);
            }

            return;
        }

        var properties = source.GetType().GetProperties(System.Reflection.BindingFlags.Instance | System.Reflection.BindingFlags.Public);
        foreach (var property in properties)
        {
            var value = property.GetValue(source);
            ParseOne(property.Name, value, prefix, map);
        }
    }

    private static void ParseOne(string key, object? value, string prefix, Dictionary<string, Subscription> map)
    {
        if (value is Subscription subscription)
        {
            subscription.Prefix = prefix.Length > 0 ? $"{prefix}/{key}" : key;
            map[prefix + key] = subscription;
            return;
        }

        if (value is null)
        {
            return;
        }

        if (value is string)
        {
            return;
        }

        var nestedPrefix = $"{prefix}{key}/";
        Parse(value, nestedPrefix, map);
    }

    private async Task PublishAsync(string topic, ValueTask<object?> valueTask)
    {
        try
        {
            var body = await valueTask.ConfigureAwait(false);
            Publish(topic, body);
        }
        catch
        {
            // Keep parity with TS: ignore async body errors.
        }
    }
}
