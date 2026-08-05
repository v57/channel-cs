using System.Text.Json;
using ChannelCs.Process;
using ChannelCs.Server;

namespace ChannelCs.Tests;

public sealed class ChannelParityTests
{
    [Fact]
    public async Task PostOther_And_StreamOther_Route_Dynamic_Paths()
    {
        var channel = new Channel<Dictionary<string, object?>>();
        channel.PostOther(path => path.StartsWith("dynamic/"), request =>
        {
            var body = request.Body<string>();
            return ValueTask.FromResult<object?>($"{request.Path}:{body}");
        });

        channel.StreamOther(
            path => path.EndsWith("/stream"),
            request => ValueTask.FromResult<IAsyncEnumerable<object?>>(YieldValues(request.Body<int>())));

        var sender = channel.CreateSender(new FakeConnection());
        var controller = new TestController(sender);

        channel.Receive(new ChannelMessage { Id = 1, Path = "dynamic/echo", Body = "ok" }, controller);
        channel.Receive(new ChannelMessage { Id = 2, Stream = "dynamic/stream", Body = 7 }, controller);
        await Task.Delay(20);

        Assert.Contains(controller.Responses, response => response.Id == 1 && (string?)response.Body == "dynamic/echo:ok");
        Assert.Contains(controller.Responses, response => response.Id == 2 && (int?)response.Body == 7);
        Assert.Contains(controller.Responses, response => response.Id == 2 && response.Done == true);
    }

    [Fact]
    public async Task Disconnect_Cleans_Up_Pending_Requests()
    {
        var disconnectCount = 0;
        var channel = new Channel<Dictionary<string, object?>>()
            .OnDisconnect((_, _) =>
            {
                disconnectCount += 1;
                throw new InvalidOperationException("ignored");
            })
            .Post("wait", request =>
            {
                request.Task?.OnCancel(() => { });
                var tcs = new TaskCompletionSource<object?>();
                return new ValueTask<object?>(tcs.Task);
            });

        var sender = channel.Process(new Dictionary<string, object?>());
        for (var i = 0; i < 10; i += 1)
        {
            sender.Request<object?>("wait");
        }

        await Task.Delay(20);
        Assert.True(sender.ActiveRequestCount > 0);
        Assert.True(channel.PendingRequestsCount > 0);

        sender.Stop();
        await Task.Delay(20);

        Assert.Equal(1, disconnectCount);
        Assert.Equal(0, sender.ActiveRequestCount);
        Assert.Equal(0, sender.ActiveStreamCount);
        Assert.Equal(0, channel.PendingRequestsCount);
    }

    [Fact]
    public void Cancel_Removes_Request_By_Id()
    {
        var channel = new Channel<Dictionary<string, object?>>();
        var sender = channel.CreateSender(new FakeConnection());
        sender.Request<object?>("hello");

        Assert.Equal(1, channel.PendingRequestsCount);
        channel.Cancel(0);
        Assert.Equal(0, channel.PendingRequestsCount);
    }

    [Fact]
    public async Task Receive_Async_Post_Rejection_Returns_Error()
    {
        var channel = new Channel<Dictionary<string, object?>>()
            .Post("fails", _ => FailingPostAsync());

        var sender = channel.CreateSender(new FakeConnection());
        var controller = new TestController(sender);

        channel.Receive(new ChannelMessage { Id = 1, Path = "fails" }, controller);
        await Task.Delay(20);

        Assert.Contains(controller.Responses, response => response.Id == 1 && response.Error == "boom");
    }

    private static async Task<object?> FailingPostAsync()
    {
        await Task.Yield();
        throw new ChannelException("boom");
    }

    [Fact]
    public async Task Receive_Cancel_Runs_All_Task_OnCancel_Actions()
    {
        var onCancelCalls = new List<int>();
        var channel = new Channel<Dictionary<string, object?>>()
            .Post("wait", request =>
            {
                request.Task?.OnCancel(() => onCancelCalls.Add(1));
                request.Task?.OnCancel(() => onCancelCalls.Add(2));
                var tcs = new TaskCompletionSource<object?>();
                return new ValueTask<object?>(tcs.Task);
            });

        var sender = channel.CreateSender(new FakeConnection());
        var controller = new TestController(sender);

        channel.Receive(new ChannelMessage { Id = 5, Path = "wait" }, controller);
        channel.Receive(new ChannelMessage { Cancel = 5 }, controller);

        await Task.Delay(20);

        Assert.Equal([1, 2], onCancelCalls);
        Assert.Contains(controller.Responses, response => response.Id == 5 && response.Error == "cancelled");
        Assert.Equal(0, sender.ActiveRequestCount);
    }

    [Fact]
    public async Task Receive_Subscribe_Waits_For_Async_Subscription_Body()
    {
        var news = new Subscription();
        news.Topic(_ => "ready");
        news.BodyFactory = async request =>
        {
            await Task.Yield();
            var payload = JsonCodec.DeserializeBody<Dictionary<string, string>>(request);
            return payload!["value"].ToUpperInvariant();
        };

        var channel = new Channel<Dictionary<string, object?>>().Events(new Dictionary<string, Subscription>
        {
            ["news"] = news,
        });

        var sender = channel.CreateSender(new FakeConnection());
        var controller = new TestController(sender);

        channel.Receive(new ChannelMessage
        {
            Id = 9,
            Sub = "news",
            Body = new Dictionary<string, string> { ["value"] = "ok" },
        }, controller);

        await Task.Delay(20);

        Assert.Equal(["news/ready"], controller.Subscriptions);
        Assert.Contains(controller.Responses, response =>
            response.Id == 9 && response.Topic == "news/ready" && (string?)response.Body == "OK");
    }

    [Fact]
    public void Subscription_Topic_And_Body_Both_Replace_Topic_Formatter()
    {
        var subscription = new Subscription();
        var events = new List<SubscriptionEvent>();
        var publisher = new TestPublisher(events);
        subscription.AddPublisher(publisher);

        subscription.Topic(request => $"topic-{request}");
        subscription.Body(request => $"body-{request}");
        subscription.Send("x", "payload");

        Assert.Equal([new SubscriptionEvent("body-x", "payload")], events);
    }

    [Fact]
    public void Request_Cancel_Notifies_When_Transport_Cancel_Fails()
    {
        var connection = new FakeConnection { CancelResult = false };
        var channel = new Channel<Dictionary<string, object?>>();
        var sender = channel.CreateSender(connection);

        var request = sender.Request<object?>("slow");
        request.Cancel();

        Assert.Equal([200L], connection.CancelIds);
        Assert.Contains(connection.Notifications, message => message.Cancel == 0);
        Assert.Equal(0, channel.PendingRequestsCount);
    }

    [Fact]
    public async Task Process_Stop_Clears_Pending_Requests()
    {
        var channel = new Channel<Dictionary<string, object?>>()
            .Post("wait", request =>
            {
                request.Task?.OnCancel(() => { });
                var tcs = new TaskCompletionSource<object?>();
                return new ValueTask<object?>(tcs.Task);
            });

        var sender = channel.Process(new Dictionary<string, object?>());

        for (var i = 0; i < 10; i += 1)
        {
            sender.Request<object?>("wait");
        }

        await Task.Delay(20);
        Assert.True(sender.ActiveRequestCount > 0);
        Assert.True(channel.PendingRequestsCount > 0);

        sender.Stop();
        await Task.Delay(20);

        Assert.Equal(0, sender.ActiveRequestCount);
        Assert.Equal(0, channel.PendingRequestsCount);
    }

    [Fact]
    public void Listen_Stop_Cleans_Subscription_Publishers()
    {
        var updates = new Subscription();
        var channel = new Channel<Dictionary<string, object?>>().Events(new Dictionary<string, Subscription>
        {
            ["updates"] = updates,
        });

        Assert.Equal(0, updates.PublisherCount);

        var first = channel.Listen("127.0.0.1:0");
        Assert.Equal(1, updates.PublisherCount);
        first.Stop();
        Assert.Equal(0, updates.PublisherCount);

        var second = channel.Listen("127.0.0.1:0");
        Assert.Equal(1, updates.PublisherCount);
        second.Stop();
        Assert.Equal(0, updates.PublisherCount);
    }

    [Fact]
    public async Task Values_Return_Clears_Pending_Stream_Request_Without_Transport_Cancel()
    {
        var connection = new FakeConnection
        {
            CancelResult = false,
            NextId = 100,
        };

        var channel = new Channel<Dictionary<string, object?>>();
        var sender = channel.CreateSender(connection);

        using var cts = new CancellationTokenSource(TimeSpan.FromMilliseconds(200));

        var streamTask = Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
        {
            await foreach (var _ in sender.Values<int>("stream/stuck", cancellationToken: cts.Token))
            {
            }
        });

        await Task.Delay(20);
        Assert.Equal(1, channel.PendingRequestsCount);

        await streamTask;
        await Task.Delay(20);

        Assert.Equal([100L], connection.CancelIds);
        Assert.Contains(connection.SentBodies, body => body.Cancel is not null);
        Assert.Equal(0, channel.PendingRequestsCount);
    }

    private static async IAsyncEnumerable<object?> YieldValues(int value)
    {
        yield return value;
        await Task.Yield();
    }

    private sealed class TestController : IChannelController<Dictionary<string, object?>>
    {
        public TestController(ChannelSender<Dictionary<string, object?>> sender)
        {
            Sender = sender;
            State = [];
        }

        public List<ChannelMessage> Responses { get; } = [];

        public List<string> Subscriptions { get; } = [];

        public List<string> Unsubscriptions { get; } = [];

        public List<(string Topic, object Body)> Events { get; } = [];

        public ChannelSender<Dictionary<string, object?>> Sender { get; }

        public Dictionary<string, object?> State { get; }

        public void Response(ChannelMessage response)
        {
            Responses.Add(response);
        }

        public void Subscribe(string topic)
        {
            Subscriptions.Add(topic);
        }

        public void Unsubscribe(string topic)
        {
            Unsubscriptions.Add(topic);
        }

        public void Event(string topic, object body)
        {
            Events.Add((topic, body));
        }
    }

    private sealed class TestPublisher : ISubscriptionPublisher
    {
        private readonly List<SubscriptionEvent> _events;

        public TestPublisher(List<SubscriptionEvent> events)
        {
            _events = events;
        }

        public void Publish(SubscriptionEvent @event)
        {
            _events.Add(@event);
        }
    }

    private sealed class FakeConnection : IConnection
    {
        public long NextId { get; set; } = 200;

        public bool CancelResult { get; set; }

        public List<long> SentIds { get; } = [];

        public List<long> CancelIds { get; } = [];

        public List<ChannelMessage> Notifications { get; } = [];

        public List<ChannelMessage> SentBodies { get; } = [];

        public long Send(object body)
        {
            if (body is ChannelMessage message)
            {
                SentBodies.Add(message);
            }

            return NextId++;
        }

        public void Sent(long id)
        {
            SentIds.Add(id);
        }

        public bool Cancel(long id)
        {
            CancelIds.Add(id);
            return CancelResult;
        }

        public void Notify(object body)
        {
            if (body is ChannelMessage message)
            {
                Notifications.Add(message);
            }
        }

        public Func<bool> AddTopic(string topic, Action<JsonElement> onEvent)
        {
            return () => true;
        }

        public void Stop()
        {
        }
    }
}
