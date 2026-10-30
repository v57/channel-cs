using System.Collections.Concurrent;

namespace ChannelCs.Client;

public static class ChannelClientExtensions
{
    public static ClientSender<TState> Connect<TState>(
        this Channel<TState> channel,
        int port,
        ClientOptions<TState>? options = null)
    {
        return channel.Connect($"ws://127.0.0.1:{port}", options);
    }

    /// <summary>
    /// Connects to <c>ws://host:port</c>, <c>wss://…</c> or <c>quic://host:port</c> (QUIC, see
    /// <see cref="ClientOptions{TState}.Quic"/>), and keeps reconnecting until stopped.
    /// </summary>
    public static ClientSender<TState> Connect<TState>(
        this Channel<TState> channel,
        string address,
        ClientOptions<TState>? options = null)
    {
        options ??= new ClientOptions<TState>();

        var webSocket = new WebSocketClient(new Uri(address), options.Headers, options.MaxMessageSize, options.Quic);
        var sender = channel.CreateSender(webSocket);
        var topics = new ConcurrentDictionary<string, byte>();
        var state = options.StateFactory is not null
            ? options.StateFactory()
            : ChannelStateFactory.CreateDefault<TState>();

        var controller = new Controller<TState>(
            response: webSocket.Notify,
            subscribe: topic => topics[topic] = 0,
            unsubscribe: topic => topics.TryRemove(topic, out _),
            onEvent: webSocket.ReceivedEvent,
            sender: sender,
            state: state);

        if (options.OnConnect is not null)
        {
            webSocket.OnOpenAsync = () => options.OnConnect(sender);
        }

        var publishers = new List<(Subscription Subscription, ISubscriptionPublisher Publisher)>();
        var cleanedPublishers = false;

        void CleanupPublishers()
        {
            if (cleanedPublishers)
            {
                return;
            }

            cleanedPublishers = true;
            foreach (var item in publishers)
            {
                item.Subscription.RemovePublisher(item.Publisher);
            }
        }

        if (channel.EventsApi is not null)
        {
            foreach (var subscription in channel.EventsApi.Values)
            {
                var publisher = new ClientSubscriptionPublisher(webSocket, topics);
                subscription.AddPublisher(publisher);
                publishers.Add((subscription, publisher));
            }
        }

        webSocket.OnClose = () =>
        {
            if (webSocket.IsRunning)
            {
                return;
            }

            CleanupPublishers();
            channel.Disconnect(state, sender);
        };

        webSocket.OnProtocolError = error => channel.FailPending(sender, error);
        webSocket.OnMessage = messages => channel.Receive(messages, controller);
        return new ClientSender<TState>(sender, webSocket);
    }

    private sealed class ClientSubscriptionPublisher : ISubscriptionPublisher
    {
        private readonly WebSocketClient _webSocket;
        private readonly ConcurrentDictionary<string, byte> _topics;

        public ClientSubscriptionPublisher(WebSocketClient webSocket, ConcurrentDictionary<string, byte> topics)
        {
            _webSocket = webSocket;
            _topics = topics;
        }

        public void Publish(SubscriptionEvent @event)
        {
            if (!_topics.ContainsKey(@event.Topic))
            {
                return;
            }

            _webSocket.Send(new ChannelMessage
            {
                Topic = @event.Topic,
                Body = @event.Body,
            });
        }
    }

    private sealed class Controller<TState> : IChannelController<TState>
    {
        private readonly Action<ChannelMessage> _response;
        private readonly Action<string> _subscribe;
        private readonly Action<string> _unsubscribe;
        private readonly Action<string, object> _event;

        public Controller(
            Action<ChannelMessage> response,
            Action<string> subscribe,
            Action<string> unsubscribe,
            Action<string, object> onEvent,
            ChannelSender<TState> sender,
            TState state)
        {
            _response = response;
            _subscribe = subscribe;
            _unsubscribe = unsubscribe;
            _event = onEvent;
            Sender = sender;
            State = state;
        }

        public ChannelSender<TState> Sender { get; }

        public TState State { get; }

        public void Response(ChannelMessage response)
        {
            _response(response);
        }

        public void Subscribe(string topic)
        {
            _subscribe(topic);
        }

        public void Unsubscribe(string topic)
        {
            _unsubscribe(topic);
        }

        public void Event(string topic, object body)
        {
            _event(topic, body);
        }
    }
}
