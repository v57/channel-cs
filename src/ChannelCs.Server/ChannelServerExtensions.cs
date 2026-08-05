using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Logging;

namespace ChannelCs.Server;

public static class ChannelServerExtensions
{
    public static ChannelServer<TState> Listen<TState>(
        this Channel<TState> channel,
        int port,
        ListenOptions<TState>? options = null)
    {
        return channel.Listen($"127.0.0.1:{port}", options);
    }

    public static ChannelServer<TState> Listen<TState>(
        this Channel<TState> channel,
        string address,
        ListenOptions<TState>? options = null)
    {
        var split = address.Split(':', 2);
        var host = split.Length == 2 ? split[0] : "127.0.0.1";
        var portText = split.Length == 2 ? split[1] : split[0];
        if (!int.TryParse(portText, out var port))
        {
            throw new ArgumentException("Address must be '<host>:<port>' or '<port>'", nameof(address));
        }

        var builder = WebApplication.CreateSlimBuilder();
        builder.WebHost.UseUrls($"http://{host}:{port}");
        builder.Logging.ClearProviders();

        var app = builder.Build();
        app.UseWebSockets();

        var server = new ChannelServer<TState>(channel, app, options ?? new ListenOptions<TState>());
        app.Map("/", server.HandleHttpAsync);
        app.Map("/{**path}", server.HandleHttpAsync);
        app.StartAsync().GetAwaiter().GetResult();
        return server;
    }
}
