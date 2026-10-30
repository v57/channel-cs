<h1>
  <img alt="Containerization logo" src="./icon.png" width="70" valign="middle">
  &nbsp;channel
</h1>

Powerful and lightweight communication built for modern language. C# implementation of Channel: run a websocket server, a websocket client, or communicate inside your process. Async/await and streaming values are supported out of the box.

# Installation
```bash
dotnet add package ChannelCs
```

# Dependencies

None outside .NET (net10.0). JSON is `System.Text.Json` and the client is `ClientWebSocket`, both in the base library;
the server runs on ASP.NET Core (Kestrel), which comes with .NET as the `Microsoft.AspNetCore.App` shared framework
rather than a NuGet package.

# Usage

```csharp
// Server
using ChannelCs;
using ChannelCs.Server;

var server = new Channel<Dictionary<string, object?>>()
    .Post("hello", _ => "world")
    .Listen(8080);
```

```csharp
// Client
using ChannelCs;
using ChannelCs.Client;

var client = new Channel<Dictionary<string, object?>>().Connect(2049);
var response = await client.SendAsync<string>("hello");

await foreach (var value in client.Values<string>("stream/values"))
{
    Console.WriteLine(value);
}
```

# Process transport (in-memory)
```csharp
using ChannelCs;
using ChannelCs.Process;

var sender = new Channel<Dictionary<string, object?>>()
    .Post("hello", _ => "world")
    .Process(new Dictionary<string, object?>());

var response = await sender.SendAsync<string>("hello");
```

# QUIC transport (experimental)

The same messages over QUIC instead of WebSocket (protocol: `channel/docs/quic.md` in the TypeScript reference). It
uses `System.Net.Quic`, which is part of .NET, so there is nothing to add: `ChannelCs.Client` connects to `quic://`
addresses and `ChannelCs.Server` has `ListenQuic`. At runtime .NET needs the native **libmsquic**:
- Linux: the `libmsquic` package from [packages.microsoft.com](https://learn.microsoft.com/linux/packages)
  (`apt install libmsquic` once its repository is set up);
- macOS: `brew install libmsquic`;
- Windows 11 / Server 2022: built in.

`QuicConnection.IsSupported` and `QuicListener.IsSupported` say whether it's there. Without it, `Connect("quic://…")`
and `ListenQuic` throw `PlatformNotSupportedException`; WebSocket works the same either way.

```csharp
// Server: QUIC always needs a certificate and its key (PEM)
using ChannelCs.Server;

var server = new Channel<Dictionary<string, string?>>()
    .Post("hello", _ => "world")
    .ListenQuic("0.0.0.0:4433", new QuicListenOptions<Dictionary<string, string?>>
    {
        Cert = File.ReadAllText("cert.pem"),
        Key = File.ReadAllText("key.pem"),
        // StateFactory, OnConnect, OnDisconnect and MaxMessageSize work as with Listen;
        // StateFactory gets the client's hello headers with lowercased names
    });
Console.WriteLine(server.Port);
await server.StopAsync();
```

```csharp
// Client: the same Connect, with a quic:// address
using ChannelCs.Client;

var client = new Channel<Dictionary<string, object?>>().Connect("quic://example.com:4433", new()
{
    Quic = new QuicClientOptions { Ca = File.ReadAllText("cert.pem") },
});
var response = await client.SendAsync<string>("hello");
```

Certificates (`QuicClientOptions`):
- nothing set: the server certificate must be trusted by the system;
- `Ca`: a PEM certificate (or CA) to trust, for self-signed servers. The address must be in its subject alternative
  names, as a DNS name or an IP address;
- `Fingerprint`: the server certificate's SHA-256 (hex, colons optional, as
  `openssl x509 -in cert.pem -noout -fingerprint -sha256` prints it). It replaces the other checks;
- `ServerName`: the name sent and checked instead of the address host.

The client dials each connection, reconnects included, from its own UDP socket, never one shared endpoint
(Network.framework's QUIC servers take one connection per client address and port). It reconnects and replays
unanswered requests like the WebSocket client, and sends a keepalive every 15 s (idle timeout 30 s).

Languages with QUIC: TypeScript, Go, Swift, C#, Rust, Python, Elixir, Java, Kotlin, C and Zig; Dart, Ruby and PHP stay
on WebSocket (libraries and status in `channel/docs/quic.md`).

# Requirements
- .NET SDK 10+
- libmsquic, only for the QUIC transport and its tests (`scripts/msquic` fetches it on Linux)
- Bun (only for running the TypeScript fixture server externally)
- lcov (for merging coverage output)

# Projects
- `src/ChannelCs`: protocol/core runtime (and the QUIC framing)
- `src/ChannelCs.Client`: WebSocket and QUIC client transport
- `src/ChannelCs.Server`: WebSocket and QUIC server transports
- `src/ChannelCs.Process`: in-process transport
- `tests/ChannelCs.Tests`: unit tests
- `tests/ChannelCs.IntegrationTests`: integration tests vs TS server on `127.0.0.1:2049`
- `bench/ChannelCs.Benchmark`: throughput benchmarks

# Scripts
- `scripts/test`: build + unit + integration tests
- `scripts/msquic`: on Linux without libmsquic, extracts it (and libnuma1) into `$HOME/.cache/msquic` once and prints
  the directory; `scripts/test` and `scripts/coverage` add it to `LD_LIBRARY_PATH`
- `scripts/benchmark`: run throughput benchmark
- `scripts/coverage`: run tests with coverage and output `coverage/lcov.info`

# Integration tests
Integration tests assume the TypeScript fixture server is already running at:
- `ws://127.0.0.1:2049`
- `quic://127.0.0.1:2049` (UDP, the same fixture; `CHANNEL_TEST_PORT` picks another port for the QUIC tests)

The QUIC unit tests also run the TypeScript reference's QUIC client against this port's server (`bun`, with
`../channel` next to this checkout).

Fixture source:
- `../channel/tests/server.ts`
