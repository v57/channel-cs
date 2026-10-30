using System.Net;
using System.Net.Quic;
using System.Net.Security;
using System.Runtime.Versioning;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Text.Json;

namespace ChannelCs.Client;

/// <summary>TLS settings for <c>quic://</c> addresses. Without them the server certificate must be trusted by the system.</summary>
public sealed class QuicClientOptions
{
    /// <summary>PEM certificate (or CA) to trust, for self-signed servers.</summary>
    public string? Ca { get; init; }

    /// <summary>
    /// SHA-256 fingerprint of the server certificate to pin instead of verifying it (hex, colons optional), as
    /// <c>openssl x509 -noout -fingerprint -sha256</c> prints it.
    /// </summary>
    public string? Fingerprint { get; init; }

    /// <summary>Name sent and checked instead of the address host.</summary>
    public string? ServerName { get; init; }
}

/// <summary>The checked form of <see cref="QuicClientOptions"/>.</summary>
internal sealed class QuicTrust
{
    private readonly byte[]? _pin;
    private readonly X509Certificate2Collection? _roots;
    private readonly string? _serverName;

    public QuicTrust(QuicClientOptions? options)
    {
        _serverName = options?.ServerName;
        if (!string.IsNullOrEmpty(options?.Fingerprint))
        {
            var hex = options.Fingerprint.Replace(":", "", StringComparison.Ordinal);
            try
            {
                _pin = Convert.FromHexString(hex);
            }
            catch (FormatException)
            {
            }

            if (_pin is not { Length: 32 })
            {
                throw new ArgumentException($"QUIC fingerprint isn't a SHA-256 in hex: '{options.Fingerprint}'", nameof(options));
            }
        }
        else if (!string.IsNullOrEmpty(options?.Ca))
        {
            _roots = [];
            _roots.ImportFromPem(options.Ca);
            if (_roots.Count == 0)
            {
                throw new ArgumentException("QUIC CA has no PEM certificate", nameof(options));
            }
        }
    }

    public SslClientAuthenticationOptions Create(string host)
    {
        var tls = new SslClientAuthenticationOptions
        {
            ApplicationProtocols = [new SslApplicationProtocol(QuicFraming.Alpn)],
            TargetHost = _serverName ?? host,
        };

        if (_pin is not null)
        {
            // Pinning replaces the other checks, as in the TypeScript client
            var pin = _pin;
            tls.RemoteCertificateValidationCallback = (_, certificate, _, _) =>
                certificate is not null && SHA256.HashData(certificate.GetRawCertData()).AsSpan().SequenceEqual(pin);
        }
        else if (_roots is not null)
        {
            var roots = _roots;
            tls.RemoteCertificateValidationCallback = (_, certificate, presented, errors) =>
            {
                // The name must match; the chain is checked against the given CA instead of the system roots
                if (certificate is null || (errors & ~SslPolicyErrors.RemoteCertificateChainErrors) != 0)
                {
                    return false;
                }

                using var chain = new X509Chain();
                chain.ChainPolicy.TrustMode = X509ChainTrustMode.CustomRootTrust;
                chain.ChainPolicy.CustomTrustStore.AddRange(roots);
                chain.ChainPolicy.RevocationMode = X509RevocationMode.NoCheck;
                if (presented is not null)
                {
                    foreach (var element in presented.ChainElements)
                    {
                        chain.ChainPolicy.ExtraStore.Add(element.Certificate);
                    }
                }

                return chain.Build(certificate as X509Certificate2 ?? X509CertificateLoader.LoadCertificate(certificate.GetRawCertData()));
            };
        }

        return tls;
    }
}

/// <summary>One client connection of a <c>quic://</c> address: its own UDP socket, one stream, framed messages.</summary>
[SupportedOSPlatform("linux")]
[SupportedOSPlatform("macos")]
[SupportedOSPlatform("windows")]
internal sealed class QuicClientTransport : IAsyncDisposable
{
    private readonly QuicConnection _connection;
    private readonly QuicStream _stream;
    private readonly Timer _keepAlive;
    private int _disposed;

    private QuicClientTransport(QuicConnection connection, QuicStream stream, int maxMessageSize)
    {
        _connection = connection;
        _stream = stream;
        Reader = new QuicFrameReader(stream, maxMessageSize);
        Writer = new QuicFrameWriter(stream, _ => Abort());
        _keepAlive = new Timer(_ => Writer.Write([]), null, QuicFraming.KeepAliveInterval, QuicFraming.KeepAliveInterval);
    }

    public QuicFrameReader Reader { get; }

    public QuicFrameWriter Writer { get; }

    public static async Task<QuicClientTransport> ConnectAsync(
        Uri address,
        IReadOnlyDictionary<string, string>? headers,
        QuicTrust trust,
        int maxMessageSize,
        CancellationToken cancellationToken)
    {
        var host = address.IdnHost;
        var port = address.Port;
        if (!IPAddress.TryParse(host, out var ip))
        {
            // IPv4 first: servers often listen on 0.0.0.0 only, while localhost resolves to ::1 first
            var addresses = await Dns.GetHostAddressesAsync(host, cancellationToken).ConfigureAwait(false);
            ip = addresses.FirstOrDefault(a => a.AddressFamily == System.Net.Sockets.AddressFamily.InterNetwork)
                ?? addresses.FirstOrDefault()
                ?? throw new IOException($"{host} has no address");
        }

        var remote = new IPEndPoint(ip, port);

        // QuicConnection binds a UDP socket of its own for each connection, as Network.framework's QUIC servers
        // (Swift's HubServer) take one connection per client address and port.
        var connection = await QuicConnection.ConnectAsync(new QuicClientConnectionOptions
        {
            RemoteEndPoint = remote,
            ClientAuthenticationOptions = trust.Create(host),
            DefaultStreamErrorCode = 0,
            DefaultCloseErrorCode = 0,
            IdleTimeout = QuicFraming.IdleTimeout,
            MaxInboundBidirectionalStreams = 0,
            MaxInboundUnidirectionalStreams = 0,
        }, cancellationToken).ConfigureAwait(false);

        try
        {
            var stream = await connection.OpenOutboundStreamAsync(QuicStreamType.Bidirectional, cancellationToken)
                .ConfigureAwait(false);
            var transport = new QuicClientTransport(connection, stream, maxMessageSize);
            transport.Writer.Write(Hello(headers));
            return transport;
        }
        catch
        {
            await connection.DisposeAsync().ConfigureAwait(false);
            throw;
        }
    }

    /// <summary>The first frame: <c>{"hello": headers}</c>, what a WebSocket client sends with its upgrade request.</summary>
    internal static byte[] Hello(IReadOnlyDictionary<string, string>? headers)
    {
        using var buffer = new MemoryStream();
        using (var json = new Utf8JsonWriter(buffer))
        {
            json.WriteStartObject();
            json.WriteStartObject("hello");
            foreach (var header in headers ?? new Dictionary<string, string>())
            {
                if (!string.IsNullOrEmpty(header.Value))
                {
                    json.WriteString(header.Key, header.Value);
                }
            }

            json.WriteEndObject();
            json.WriteEndObject();
        }

        return buffer.ToArray();
    }

    public void Abort()
    {
        _ = DisposeAsync().AsTask();
    }

    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        await _keepAlive.DisposeAsync().ConfigureAwait(false);
        Writer.Close();
        try
        {
            await _connection.CloseAsync(0).ConfigureAwait(false);
        }
        catch
        {
            // Already closed by the peer or the idle timeout.
        }

        try
        {
            await _stream.DisposeAsync().ConfigureAwait(false);
            await _connection.DisposeAsync().ConfigureAwait(false);
        }
        catch
        {
            // The connection is gone either way.
        }
    }
}
