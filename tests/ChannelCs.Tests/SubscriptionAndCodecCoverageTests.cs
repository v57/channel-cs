using System.Text;
using System.Text.Json;

namespace ChannelCs.Tests;

public sealed class SubscriptionAndCodecCoverageTests
{
    [Fact]
    public void JsonCodec_Deserialize_Covers_Array_Object_Empty_And_Invalid()
    {
        var empty = JsonCodec.Deserialize(ReadOnlyMemory<byte>.Empty);
        Assert.Empty(empty);

        var asArray = JsonCodec.Deserialize(Encoding.UTF8.GetBytes("[{\"id\":1,\"path\":\"hello\"}]"));
        Assert.Single(asArray);
        Assert.Equal(1, asArray[0].Id);

        var asObject = JsonCodec.Deserialize(Encoding.UTF8.GetBytes("{\"id\":2,\"path\":\"echo\"}"));
        Assert.Single(asObject);
        Assert.Equal("echo", asObject[0].Path);

        Assert.ThrowsAny<JsonException>(() => JsonCodec.Deserialize(Encoding.UTF8.GetBytes("   ")));
        Assert.ThrowsAny<JsonException>(() => JsonCodec.Deserialize(Encoding.UTF8.GetBytes("1")));
    }

    [Fact]
    public void JsonCodec_DeserializeBody_Covers_Null_JsonElement_And_Fallback_Serialization()
    {
        Assert.Null(JsonCodec.DeserializeBody<string>(null));

        using var nullDoc = JsonDocument.Parse("null");
        Assert.Null(JsonCodec.DeserializeBody<string>(nullDoc.RootElement.Clone()));

        using var numberDoc = JsonDocument.Parse("5");
        Assert.Equal(5, JsonCodec.DeserializeBody<int>(numberDoc.RootElement.Clone()));

        var anonymous = new { a = 1 };
        var dictionary = JsonCodec.DeserializeBody<Dictionary<string, int>>(anonymous);
        Assert.NotNull(dictionary);
        Assert.Equal(1, dictionary!["a"]);
    }

    [Fact]
    public async Task Subscription_Send_Publish_And_Async_BodyFactory_Paths()
    {
        var subscription = new Subscription();
        var published = new List<SubscriptionEvent>();
        var publisher = new CapturingPublisher(published);

        subscription.AddPublisher(publisher);
        subscription.Publish("ignored", null);
        Assert.Empty(published);

        subscription.BodyFactory = request => ValueTask.FromResult<object?>($"sync-{request}");
        subscription.Send("one");
        Assert.Contains(published, entry => entry.Topic == "one" && (string?)entry.Body == "sync-one");

        subscription.BodyFactory = async request =>
        {
            await Task.Yield();
            return $"async-{request}";
        };

        subscription.Send("two");
        await Task.Delay(20);
        Assert.Contains(published, entry => entry.Topic == "two" && (string?)entry.Body == "async-two");

        var beforeError = published.Count;
        subscription.BodyFactory = async _ =>
        {
            await Task.Yield();
            throw new InvalidOperationException("ignored");
        };

        subscription.Send("three");
        await Task.Delay(20);
        Assert.Equal(beforeError, published.Count);

        subscription.RemovePublisher(publisher);
        subscription.Send("four", "payload");
        Assert.Equal(beforeError, published.Count);
    }

    [Fact]
    public void Subscription_Parse_Covers_Null_Dictionaries_And_Reflection_Branches()
    {
        var channel = new Channel<Dictionary<string, object?>>();
        var alpha = new Subscription();
        var beta = new Subscription();
        var gamma = new Subscription();

        channel.Events((object?)null!);
        channel.Events((object)new Dictionary<string, object?>
        {
            ["alpha"] = alpha,
            ["nested"] = new Dictionary<string, object?>
            {
                ["beta"] = beta,
                ["skip"] = "text",
                ["empty"] = null,
            },
        });

        channel.Events((object)new Dictionary<string, Subscription>
        {
            ["gamma"] = gamma,
        });

        channel.Events(new
        {
            reflection = new Subscription(),
        });

        Assert.NotNull(channel.EventsApi);
        Assert.True(channel.EventsApi!.ContainsKey("alpha"));
        Assert.True(channel.EventsApi.ContainsKey("nested/beta"));
        Assert.True(channel.EventsApi.ContainsKey("gamma"));
        Assert.True(channel.EventsApi.ContainsKey("reflection"));
    }

    [Fact]
    public void ChannelStateFactory_Covers_Value_And_Reference_Defaults()
    {
        Assert.Equal(0, ChannelStateFactory.CreateDefault<int>());
        Assert.Null(ChannelStateFactory.CreateDefault<string>());
    }

    private sealed class CapturingPublisher : ISubscriptionPublisher
    {
        private readonly List<SubscriptionEvent> _events;

        public CapturingPublisher(List<SubscriptionEvent> events)
        {
            _events = events;
        }

        public void Publish(SubscriptionEvent @event)
        {
            _events.Add(@event);
        }
    }
}
