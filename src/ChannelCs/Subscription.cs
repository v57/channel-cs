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

    // Sets the body factory used by Send(request) and by the initial subscription reply (TS `body()`).
    public Subscription Body(Func<object?, object?> factory)
    {
        _body = request => ValueTask.FromResult(factory(request));
        return this;
    }

    public Subscription Body(Func<object?, Task<object?>> factory)
    {
        _body = request => new ValueTask<object?>(factory(request));
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
        // TS parity: `if (!body) return` drops every falsy body (null, false, 0, NaN, "").
        if (IsFalsy(body))
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

    /// <summary>JavaScript truthiness for a channel body.</summary>
    public static bool IsFalsy(object? value)
    {
        switch (value)
        {
            case null:
                return true;
            case bool b:
                return !b;
            case string s:
                return s.Length == 0;
            case double d:
                return d == 0 || double.IsNaN(d);
            case float f:
                return f == 0 || float.IsNaN(f);
            case decimal m:
                return m == 0;
            case int or long or short or sbyte or byte or uint or ulong or ushort:
                return Convert.ToDecimal(value, System.Globalization.CultureInfo.InvariantCulture) == 0;
            case System.Text.Json.JsonElement element:
                return element.ValueKind switch
                {
                    System.Text.Json.JsonValueKind.Null or System.Text.Json.JsonValueKind.Undefined
                        or System.Text.Json.JsonValueKind.False => true,
                    System.Text.Json.JsonValueKind.String => element.GetString()!.Length == 0,
                    System.Text.Json.JsonValueKind.Number => element.GetDouble() == 0,
                    _ => false,
                };
            default:
                return false;
        }
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
