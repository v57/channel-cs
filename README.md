<h1>
  <img alt="Containerization logo" src="./icon.png" width="70" valign="middle">
  &nbsp;channel
</h1>

Powerful and lightweight communication built for modern language. C# implementation of Channel: run a websocket server, a websocket client, or communicate inside your process. Async/await and streaming values are supported out of the box.

# Installation
```bash
dotnet add package ChannelCs
```

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

# Requirements
- .NET SDK 10+
- Bun (only for running the TypeScript fixture server externally)
- lcov (for merging coverage output)

# Projects
- `src/ChannelCs`: protocol/core runtime
- `src/ChannelCs.Client`: WebSocket client transport
- `src/ChannelCs.Server`: WebSocket server transport
- `src/ChannelCs.Process`: in-process transport
- `tests/ChannelCs.Tests`: unit tests
- `tests/ChannelCs.IntegrationTests`: integration tests vs TS server on `127.0.0.1:2049`
- `bench/ChannelCs.Benchmark`: throughput benchmarks

# Scripts
- `scripts/test`: build + unit + integration tests
- `scripts/benchmark`: run throughput benchmark
- `scripts/coverage`: run tests with coverage and output `coverage/lcov.info`

# Integration tests
Integration tests assume the TypeScript fixture server is already running at:
- `ws://127.0.0.1:2049`

Fixture source:
- `../channel/tests/server.ts`
