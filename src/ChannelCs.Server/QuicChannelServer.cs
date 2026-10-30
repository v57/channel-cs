using System.Collections.Concurrent;
using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Runtime.Versioning;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;

namespace ChannelCs.Server;

public sealed class QuicListenOptions<TState>
{
    /// <summary>PEM certificate chain, leaf first.</summary>
    public required string Cert { get; init; }

    /// <summary>PEM private key of the certificate.</summary>
    public required string Key { get; init; }

    /// <summary>Builds the connection state from the hello headers (names lowercased). Throwing closes the connection.</summary>
    public Func<IReadOnlyDictionary<string, string?>, ValueTask<TState>>? StateFactory { get; init; }

    public Action<BodyContext<TState>>? OnConnect { get; init; }

    public Action<BodyContext<TState>>? OnDisconnect { get; init; }

    /// <summary>Largest incoming frame accepted, in bytes (default 16 MiB, like TS/Bun). A bigger one closes the connection.</summary>
    public int MaxMessageSize { get; init; } = QuicFraming.DefaultMaxPayload;
}

public static class ChannelQuicServerExtensions
{
    /// <summary>Serves the channel over QUIC (docs/quic.md in the TypeScript reference) on UDP 127.0.0.1:<paramref name="port"/>.</summary>
    public static QuicChannelServer<TState> ListenQuic<TState>(
        this Channel<TState> channel,
        int port,
        QuicListenOptions<TState> options)
    {
        return channel.ListenQuic($"127.0.0.1:{port}", options);
    }

    /// <summary>
    /// Serves the channel over QUIC on <c>host:port</c> (an IP address or <c>localhost</c>; <c>[::]:port</c> for IPv6).
    /// Throws <see cref="PlatformNotSupportedException"/> where System.Net.Quic has no libmsquic.
    /// </summary>
    public static QuicChannelServer<TState> ListenQuic<TState>(
        this Channel<TState> channel,
        string address,
        QuicListenOptions<TState> options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (!QuicListener.IsSupported)
        {
            throw new PlatformNotSupportedException(
                "QUIC isn't supported here: System.Net.Quic needs libmsquic (Linux: the libmsquic package; macOS: brew install libmsquic)");
        }

        var endpoint = ParseEndPoint(address);
        return QuicChannelServer<TState>.StartAsync(channel, endpoint, options).GetAwaiter().GetResult();
    }

    internal static IPEndPoint ParseEndPoint(string address)
    {
        if (int.TryParse(address, out var port))
        {
            return new IPEndPoint(IPAddress.Loopback, port);
        }

        var split = address.LastIndexOf(':');
        if (split > 0 && int.TryParse(address.AsSpan(split + 1), out port))
        {
            var host = address[..split].Trim('[', ']');
            if (string.Equals(host, "localhost", StringComparison.OrdinalIgnoreCase))
            {
                return new IPEndPoint(IPAddress.Loopback, port);
            }

            if (IPAddress.TryParse(host, out var ip))
            {
                return new IPEndPoint(ip, port);
            }
        }

        throw new ArgumentException("Address must be '<ip>:<port>', 'localhost:<port>' or '<port>'", nameof(address));
    }
}

/// <summary>A channel served over QUIC: one bidirectional stream per connection carrying framed JSON messages.</summary>
[SupportedOSPlatform("linux")]
[SupportedOSPlatform("macos")]
[SupportedOSPlatform("windows")]
public sealed class QuicChannelServer<TState> : IDisposable, IAsyncDisposable
{
    private readonly Channel<TState> _channel;
    private readonly QuicListener _listener;
    private readonly QuicListenOptions<TState> _options;
    private readonly ConcurrentDictionary<QuicConnection, ConnectionState?> _connections = new();
    private readonly CancellationTokenSource _stopping = new();
    private readonly ISubscriptionPublisher _publisher;
    private readonly Task _accepting;
    private int _isStopped;

    private QuicChannelServer(Channel<TState> channel, QuicListener listener, QuicListenOptions<TState> options)
    {
        _channel = channel;
        _listener = listener;
        _options = options;
        _publisher = new ServerPublisher(this);
        if (_channel.EventsApi is not null)
        {
            foreach (var subscription in _channel.EventsApi.Values)
            {
                subscription.AddPublisher(_publisher);
            }
        }

        _accepting = Task.Run(AcceptLoopAsync);
    }

    /// <summary>The UDP port it listens on (useful after listening on port 0).</summary>
    public int Port => _listener.LocalEndPoint.Port;

    public IPEndPoint LocalEndPoint => _listener.LocalEndPoint;

    internal static async Task<QuicChannelServer<TState>> StartAsync(
        Channel<TState> channel,
        IPEndPoint endpoint,
        QuicListenOptions<TState> options)
    {
        var tls = ServerTls(options.Cert, options.Key);
        var listener = await QuicListener.ListenAsync(new QuicListenerOptions
        {
            ListenEndPoint = endpoint,
            ApplicationProtocols = [new SslApplicationProtocol(QuicFraming.Alpn)],
            ConnectionOptionsCallback = (_, _, _) => ValueTask.FromResult(new QuicServerConnectionOptions
            {
                ServerAuthenticationOptions = tls,
                DefaultStreamErrorCode = 0,
                DefaultCloseErrorCode = 0,
                IdleTimeout = QuicFraming.IdleTimeout,
                // One stream per connection, opened by the client
                MaxInboundBidirectionalStreams = 1,
                MaxInboundUnidirectionalStreams = 0,
            }),
        }).ConfigureAwait(false);
        return new QuicChannelServer<TState>(channel, listener, options);
    }

    private static SslServerAuthenticationOptions ServerTls(string certPem, string keyPem)
    {
        using var leaf = X509Certificate2.CreateFromPem(certPem, keyPem);
        // Platform TLS stacks want a key they can load themselves, not an ephemeral one: round-trip it through PKCS#12
        var certificate = X509CertificateLoader.LoadPkcs12(leaf.Export(X509ContentType.Pkcs12), null);
        var chain = new X509Certificate2Collection();
        chain.ImportFromPem(certPem);
        var tls = new SslServerAuthenticationOptions
        {
            ApplicationProtocols = [new SslApplicationProtocol(QuicFraming.Alpn)],
        };

        if (chain.Count > 1)
        {
            tls.ServerCertificateContext = SslStreamCertificateContext.Create(
                certificate, new X509Certificate2Collection(chain.Skip(1).ToArray()), offline: true);
        }
        else
        {
            tls.ServerCertificate = certificate;
        }

        return tls;
    }

    public void Stop()
    {
        StopAsync().GetAwaiter().GetResult();
    }

    public async Task StopAsync()
    {
        if (Interlocked.Exchange(ref _isStopped, 1) != 0)
        {
            return;
        }

        if (_channel.EventsApi is not null)
        {
            foreach (var subscription in _channel.EventsApi.Values)
            {
                subscription.RemovePublisher(_publisher);
            }
        }

        _stopping.Cancel();
        foreach (var connection in _connections.Keys)
        {
            await CloseQuietlyAsync(connection).ConfigureAwait(false);
        }

        await _listener.DisposeAsync().ConfigureAwait(false);
        try
        {
            await _accepting.ConfigureAwait(false);
        }
        catch
        {
            // The accept loop ends with the listener.
        }
    }

    public void Dispose()
    {
        Stop();
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync().ConfigureAwait(false);
    }

    private async Task AcceptLoopAsync()
    {
        while (!_stopping.IsCancellationRequested)
        {
            QuicConnection connection;
            try
            {
                connection = await _listener.AcceptConnectionAsync(_stopping.Token).ConfigureAwait(false);
            }
            catch (Exception) when (_stopping.IsCancellationRequested)
            {
                return;
            }
            catch (ObjectDisposedException)
            {
                return;
            }
            catch
            {
                // A failed handshake (refused certificate, wrong ALPN): keep accepting.
                continue;
            }

            _ = HandleConnectionAsync(connection);
        }
    }

    private async Task HandleConnectionAsync(QuicConnection connection)
    {
        _connections[connection] = null;
        try
        {
            if (_stopping.IsCancellationRequested)
            {
                return;
            }

            var stream = await connection.AcceptInboundStreamAsync(_stopping.Token).ConfigureAwait(false);
            await using (stream.ConfigureAwait(false))
            {
                _ = RefuseMoreStreamsAsync(connection);
                if (stream.Type == QuicStreamType.Bidirectional)
                {
                    await ServeAsync(connection, stream).ConfigureAwait(false);
                }
            }
        }
        catch
        {
            // Invalid frame or JSON, rejected state, or the peer went away: drop the connection, like the WebSocket server.
        }
        finally
        {
            _connections.TryRemove(connection, out _);
            await CloseQuietlyAsync(connection).ConfigureAwait(false);
            try
            {
                await connection.DisposeAsync().ConfigureAwait(false);
            }
            catch
            {
                // Already gone.
            }
        }
    }

    /// <summary>One stream per connection: later ones are aborted (the stream limits already refuse most).</summary>
    private async Task RefuseMoreStreamsAsync(QuicConnection connection)
    {
        try
        {
            while (true)
            {
                var extra = await connection.AcceptInboundStreamAsync(_stopping.Token).ConfigureAwait(false);
                extra.Abort(QuicAbortDirection.Both, 0);
                await extra.DisposeAsync().ConfigureAwait(false);
            }
        }
        catch
        {
            // The connection closed.
        }
    }

    private async Task ServeAsync(QuicConnection connection, QuicStream stream)
    {
        var reader = new QuicFrameReader(stream, _options.MaxMessageSize);
        var writer = new QuicFrameWriter(stream, error => _ = CloseQuietlyAsync(connection));

        // First frame: { hello: headers }, used like the WebSocket upgrade request headers
        var first = await reader.ReadAsync(_stopping.Token).ConfigureAwait(false);
        if (first is null || ParseHello(first) is not { } headers)
        {
            return;
        }

        var state = _options.StateFactory is not null
            ? await _options.StateFactory(headers).ConfigureAwait(false)
            : ChannelStateFactory.CreateDefault<TState>();

        var subscriptions = new ServerSubscriptions();
        var serverConnection = new QuicServerConnection(writer, subscriptions, () => _ = CloseQuietlyAsync(connection));
        var sender = _channel.CreateSender(serverConnection);
        var topics = new ConcurrentDictionary<string, byte>();
        var body = new BodyContext<TState>(state, sender, subscriptions);
        var controller = new Controller(serverConnection, topics, subscriptions, sender, state);
        _connections[connection] = new ConnectionState(serverConnection, topics);
        try
        {
            _options.OnConnect?.Invoke(body);
            while (await reader.ReadAsync(_stopping.Token).ConfigureAwait(false) is { } payload)
            {
                var requests = JsonCodec.Deserialize(payload);
                try
                {
                    _channel.Receive(requests, controller);
                }
                catch
                {
                    // Keep one bad message (e.g. a throwing event handler) from killing this connection's loop.
                }
            }
        }
        finally
        {
            writer.Close();
            _connections.TryRemove(connection, out _);
            _channel.Disconnect(state, sender);
            _options.OnDisconnect?.Invoke(body);
        }
    }

    /// <summary>The headers of <c>{"hello": {...}}</c> with lowercased names, or null when the frame isn't a hello.</summary>
    internal static Dictionary<string, string?>? ParseHello(byte[] payload)
    {
        using var document = JsonDocument.Parse(payload);
        if (document.RootElement.ValueKind != JsonValueKind.Object
            || !document.RootElement.TryGetProperty("hello", out var hello)
            || hello.ValueKind != JsonValueKind.Object)
        {
            return null;
        }

        var headers = new Dictionary<string, string?>(StringComparer.OrdinalIgnoreCase);
        foreach (var property in hello.EnumerateObject())
        {
            if (property.Value.ValueKind == JsonValueKind.String)
            {
                headers[property.Name.ToLowerInvariant()] = property.Value.GetString();
            }
        }

        return headers;
    }

    private static async Task CloseQuietlyAsync(QuicConnection connection)
    {
        try
        {
            await connection.CloseAsync(0).ConfigureAwait(false);
        }
        catch
        {
            // Already closed.
        }
    }

    private void Publish(SubscriptionEvent @event)
    {
        if (@event.Body is null)
        {
            return;
        }

        byte[]? payload = null;
        foreach (var connection in _connections.Values)
        {
            if (connection is null || !connection.Topics.ContainsKey(@event.Topic))
            {
                continue;
            }

            payload ??= JsonCodec.Serialize(new ChannelMessage { Topic = @event.Topic, Body = @event.Body });
            connection.Connection.Writer.Write(payload);
        }
    }

    private sealed class ServerPublisher(QuicChannelServer<TState> server) : ISubscriptionPublisher
    {
        public void Publish(SubscriptionEvent @event)
        {
            server.Publish(@event);
        }
    }

    private sealed record ConnectionState(QuicServerConnection Connection, ConcurrentDictionary<string, byte> Topics);

    private sealed class QuicServerConnection(QuicFrameWriter writer, ServerSubscriptions subscriptions, Action stop)
        : IConnection
    {
        public QuicFrameWriter Writer => writer;

        public long Send(object body)
        {
            writer.Write(JsonCodec.Serialize(body));
            return 0;
        }

        public void Sent(long id)
        {
            // No-op for server transport.
        }

        public bool Cancel(long id)
        {
            return false;
        }

        public void Notify(object body)
        {
            writer.Write(JsonCodec.Serialize(body));
        }

        public Func<bool> AddTopic(string topic, Action<JsonElement> onEvent)
        {
            return subscriptions.AddTopic(topic, onEvent);
        }

        public void Stop()
        {
            stop();
        }
    }

    private sealed class Controller(
        QuicServerConnection connection,
        ConcurrentDictionary<string, byte> topics,
        ServerSubscriptions subscriptions,
        ChannelSender<TState> sender,
        TState state) : IChannelController<TState>
    {
        public ChannelSender<TState> Sender => sender;

        public TState State => state;

        public void Response(ChannelMessage response)
        {
            connection.Notify(response);
        }

        public void Subscribe(string topic)
        {
            topics[topic] = 0;
        }

        public void Unsubscribe(string topic)
        {
            topics.TryRemove(topic, out _);
        }

        public void Event(string topic, object body)
        {
            subscriptions.ReceivedEvent(topic, body);
        }
    }
}
