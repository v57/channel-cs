namespace ChannelCs.Process;

public static class ChannelProcessExtensions
{
    public static ChannelSender<TState> Process<TState>(this Channel<TState> channel)
    {
        return channel.Process(ChannelStateFactory.CreateDefault<TState>());
    }

    public static ChannelSender<TState> Process<TState>(this Channel<TState> channel, TState state)
    {
        var connection = new InMemoryConnection<TState>(channel);
        var sender = channel.CreateSender(connection);

        var controller = new Controller<TState>(channel, sender, state);
        connection.SetController(controller);
        return sender;
    }

    private sealed class InMemoryConnection<TState> : IConnection
    {
        private readonly Channel<TState> _channel;
        private Controller<TState>? _controller;

        public InMemoryConnection(Channel<TState> channel)
        {
            _channel = channel;
        }

        public long Send(object body)
        {
            if (_controller is null)
            {
                return 0;
            }

            if (body is ChannelMessage message)
            {
                _channel.Receive(message, _controller);
            }
            else if (body is IReadOnlyList<ChannelMessage> many)
            {
                _channel.Receive(many, _controller);
            }

            return 0;
        }

        public void Sent(long id)
        {
            // No-op for in-process transport.
        }

        public bool Cancel(long id)
        {
            return false;
        }

        public void Notify(object body)
        {
            Send(body);
        }

        public Func<bool> AddTopic(string topic, Action<System.Text.Json.JsonElement> onEvent)
        {
            return static () => false;
        }

        public void Stop()
        {
            if (_controller is null)
            {
                return;
            }

            var state = _controller.State;
            var sender = _controller.Sender;
            _controller = null;
            _channel.Disconnect(state, sender);
        }

        public void SetController(Controller<TState> controller)
        {
            _controller = controller;
        }
    }

    private sealed class Controller<TState> : IChannelController<TState>
    {
        private readonly Channel<TState> _channel;

        public Controller(Channel<TState> channel, ChannelSender<TState> sender, TState state)
        {
            _channel = channel;
            Sender = sender;
            State = state;
        }

        public ChannelSender<TState> Sender { get; }

        public TState State { get; }

        public void Response(ChannelMessage response)
        {
            _channel.Receive(response, this);
        }

        public void Subscribe(string topic)
        {
        }

        public void Unsubscribe(string topic)
        {
        }

        public void Event(string topic, object body)
        {
        }
    }
}
