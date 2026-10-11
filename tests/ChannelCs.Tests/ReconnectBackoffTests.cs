using System.Net;
using System.Net.Sockets;
using ChannelCs.Client;

namespace ChannelCs.Tests;

// The client used to retry every 100 ms forever while the server was down (TS parity: channel docs/client.md).
public sealed class ReconnectBackoffTests
{
    [Fact]
    public void ReconnectDelay_Doubles_From_100ms_Up_To_5s()
    {
        int[] expected = [100, 200, 400, 800, 1600, 3200, 5000, 5000];
        for (var attempt = 0; attempt < expected.Length; attempt += 1)
        {
            Assert.Equal(expected[attempt], WebSocketClient.ReconnectDelay(attempt, 1).TotalMilliseconds);
        }

        Assert.Equal(50, WebSocketClient.ReconnectDelay(0, 0).TotalMilliseconds);
        Assert.Equal(2500, WebSocketClient.ReconnectDelay(1000, 0).TotalMilliseconds);
    }

    [Fact]
    public async Task Client_Backs_Off_While_Server_Refuses_The_Upgrade()
    {
        // A server that refuses every upgrade with HTTP 500: each connection is one failed attempt.
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        listener.Start();
        var attempts = 0;
        using var cancel = new CancellationTokenSource();
        var accepting = Task.Run(async () =>
        {
            try
            {
                while (!cancel.IsCancellationRequested)
                {
                    using var socket = await listener.AcceptSocketAsync(cancel.Token);
                    Interlocked.Increment(ref attempts);
                    await socket.ReceiveAsync(new byte[4096], cancel.Token);
                    await socket.SendAsync(
                        "HTTP/1.1 500 Internal Server Error\r\nContent-Length: 0\r\nConnection: close\r\n\r\n"u8.ToArray(),
                        cancel.Token);
                }
            }
            catch (OperationCanceledException)
            {
            }
        });

        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        using var client = new WebSocketClient(new Uri($"ws://127.0.0.1:{port}"));
        await Task.Delay(2000);
        client.Stop();
        cancel.Cancel();
        await accepting;

        // 100 ms retries would make ~20 attempts in 2 s; backoff allows at most 1 + 6 (waits of 50..100, 100..200, ...).
        Assert.InRange(attempts, 2, 8);
    }
}
