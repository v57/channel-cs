using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Reflection;
using System.Runtime.InteropServices;
using System.Text.Json;
using System.Text.Json.Serialization;
using ChannelCs;
using ChannelCs.Client;
using ChannelCs.Process;
using ChannelCs.Server;

await BenchmarkApp.RunAsync(args);

internal static class BenchmarkApp
{
    private static readonly int[] StageClients = [1, 10, 100, 1000];
    private const string Host = "127.0.0.1";
    private const string TextCountPath = "text/count";
    private const int TextCountExpected = 5;
    private static readonly Dictionary<string, string> TextCountBody = new()
    {
        ["text"] = "hello",
    };

    public static async Task RunAsync(string[] args)
    {
        var mode = GetArg(args, "--mode");
        if (string.Equals(mode, "server", StringComparison.OrdinalIgnoreCase))
        {
            await RunServerWorkerAsync();
            return;
        }

        if (string.Equals(mode, "client", StringComparison.OrdinalIgnoreCase))
        {
            await RunClientWorkerAsync();
            return;
        }

        if (string.Equals(mode, "process", StringComparison.OrdinalIgnoreCase))
        {
            await RunProcessWorkerAsync();
            return;
        }

        await RunCoordinatorAsync();
    }

    private static async Task RunCoordinatorAsync()
    {
        var stageDurationMs = GetEnvInt("BENCH_STAGE_MS", 5000);
        var targetRpsPerClient = GetEnvInt("BENCH_RPS_PER_CLIENT", 1000);
        var results = new List<Result>(StageClients.Length);

        for (var i = 0; i < StageClients.Length; i += 1)
        {
            var clients = StageClients[i];
            var stage = await RunStageAsync(clients, stageDurationMs, targetRpsPerClient);
            results.Add(stage);
        }

        var output = new BenchmarkOutput
        {
            Language = "C# (.NET)",
            Results = results,
        };

        var json = JsonSerializer.Serialize(
            output,
            new JsonSerializerOptions
            {
                PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
                WriteIndented = true,
            });
        var outputPath = Path.Combine(Directory.GetCurrentDirectory(), "benchmark.json");
        await File.WriteAllTextAsync(outputPath, $"{json}{Environment.NewLine}");
        Console.WriteLine($"benchmark written to {outputPath}");
    }

    private static async Task<Result> RunStageAsync(int clients, int stageDurationMs, int targetRpsPerClient)
    {
        var port = GetRandomPort();
        var server = StartWorker(
            "server",
            new Dictionary<string, string>
            {
                ["BENCH_PORT"] = port.ToString(CultureInfo.InvariantCulture),
            });
        var client = StartWorker(
            "client",
            new Dictionary<string, string>
            {
                ["BENCH_HOST"] = Host,
                ["BENCH_PORT"] = port.ToString(CultureInfo.InvariantCulture),
                ["BENCH_CLIENTS"] = clients.ToString(CultureInfo.InvariantCulture),
                ["BENCH_RPS_PER_CLIENT"] = targetRpsPerClient.ToString(CultureInfo.InvariantCulture),
            });

        try
        {
            await Task.WhenAll(server.Ready.Task, client.Ready.Task);
            await Task.Delay(stageDurationMs);

            var clientUsageTask = SampleUsageAsync(client.Process.Id);
            var serverUsageTask = SampleUsageAsync(server.Process.Id);
            await Task.WhenAll(clientUsageTask, serverUsageTask);

            SendSigTerm(client.Process.Id);
            SendSigTerm(server.Process.Id);

            var serverResult = await server.Result.Task;
            var handled = serverResult.Requests ?? 0;
            var rps = handled / (stageDurationMs / 1000d);

            await Task.WhenAll(client.Process.WaitForExitAsync(), server.Process.WaitForExitAsync());

            return new Result
            {
                Rps = Math.Round(rps, 2),
                Clients = clients,
                Client = await clientUsageTask,
                Server = await serverUsageTask,
            };
        }
        finally
        {
            TryTerminate(client.Process);
            TryTerminate(server.Process);
        }
    }

    private static Worker StartWorker(string mode, IReadOnlyDictionary<string, string> env)
    {
        var assemblyPath = Assembly.GetEntryAssembly()?.Location
            ?? throw new InvalidOperationException("Entry assembly path is not available.");

        var process = new Process();
        process.StartInfo = new ProcessStartInfo
        {
            FileName = "dotnet",
            Arguments = $"\"{assemblyPath}\" --mode {mode}",
            WorkingDirectory = Directory.GetCurrentDirectory(),
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };

        foreach (var pair in env)
        {
            process.StartInfo.Environment[pair.Key] = pair.Value;
        }

        if (!process.Start())
        {
            throw new InvalidOperationException($"Failed to start {mode} worker.");
        }

        var ready = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var result = new TaskCompletionSource<ResultMessage>(TaskCreationOptions.RunContinuationsAsynchronously);
        var worker = new Worker(process, ready, result);

        _ = ConsumeStreamAsync(worker, process.StandardOutput, process.StandardError, mode);
        _ = WatchExitAsync(worker, mode);

        return worker;
    }

    private static async Task ConsumeStreamAsync(
        Worker worker,
        StreamReader stdout,
        StreamReader stderr,
        string mode)
    {
        var stdoutTask = Task.Run(async () =>
        {
            string? line;
            while ((line = await stdout.ReadLineAsync()) is not null)
            {
                if (!TryParseMessage(line, out var message) || message is null)
                {
                    continue;
                }

                if (string.Equals(message.Type, "ready", StringComparison.OrdinalIgnoreCase))
                {
                    worker.Ready.TrySetResult();
                    continue;
                }

                if (string.Equals(message.Type, "result", StringComparison.OrdinalIgnoreCase))
                {
                    worker.Result.TrySetResult(message);
                    continue;
                }

                if (string.Equals(message.Type, "error", StringComparison.OrdinalIgnoreCase))
                {
                    var exception = new InvalidOperationException($"{mode} error: {message.Message ?? "unknown error"}");
                    worker.Ready.TrySetException(exception);
                    worker.Result.TrySetException(exception);
                }
            }
        });

        var stderrTask = Task.Run(async () =>
        {
            string? line;
            while ((line = await stderr.ReadLineAsync()) is not null)
            {
                if (line.Length > 0)
                {
                    Console.Error.WriteLine($"[{mode}] {line}");
                }
            }
        });

        await Task.WhenAll(stdoutTask, stderrTask);
    }

    private static async Task WatchExitAsync(Worker worker, string mode)
    {
        await worker.Process.WaitForExitAsync();
        if (worker.Process.ExitCode == 0)
        {
            worker.Ready.TrySetResult();
            worker.Result.TrySetResult(new ResultMessage { Type = "result" });
            return;
        }

        var exception = new InvalidOperationException($"{mode} exited with code {worker.Process.ExitCode}.");
        worker.Ready.TrySetException(exception);
        worker.Result.TrySetException(exception);
    }

    private static bool TryParseMessage(string line, out ResultMessage? message)
    {
        message = null;
        if (string.IsNullOrWhiteSpace(line))
        {
            return false;
        }

        try
        {
            message = JsonSerializer.Deserialize<ResultMessage>(line);
            return message is not null && !string.IsNullOrEmpty(message.Type);
        }
        catch
        {
            return false;
        }
    }

    private static async Task<ResourceUsage> SampleUsageAsync(int pid)
    {
        var process = new Process();
        process.StartInfo = new ProcessStartInfo
        {
            FileName = "ps",
            Arguments = $"-p {pid} -o %cpu= -o rss=",
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
        };

        if (!process.Start())
        {
            throw new InvalidOperationException($"Failed to sample process usage for pid {pid}.");
        }

        var stdoutTask = process.StandardOutput.ReadToEndAsync();
        var stderrTask = process.StandardError.ReadToEndAsync();
        await process.WaitForExitAsync();

        var stdout = await stdoutTask;
        var stderr = await stderrTask;
        if (process.ExitCode != 0)
        {
            throw new InvalidOperationException(
                $"Failed to sample process usage for pid {pid}: {stderr.Trim()}");
        }

        var parts = stdout.Split((char[]?)null, StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries);
        var cpu = parts.Length > 0 && double.TryParse(parts[0], NumberStyles.Float, CultureInfo.InvariantCulture, out var cpuValue)
            ? cpuValue
            : 0d;
        var rssKb = parts.Length > 1 && double.TryParse(parts[1], NumberStyles.Float, CultureInfo.InvariantCulture, out var rssValue)
            ? rssValue
            : 0d;

        return new ResourceUsage
        {
            Cpu = Math.Round(cpu, 2),
            Mem = Math.Round(rssKb / 1024d, 2),
        };
    }

    private static void SendSigTerm(int pid)
    {
        var process = new Process();
        process.StartInfo = new ProcessStartInfo
        {
            FileName = "kill",
            Arguments = $"-TERM {pid}",
            UseShellExecute = false,
        };

        if (!process.Start())
        {
            return;
        }

        process.WaitForExit();
    }

    private static void TryTerminate(Process process)
    {
        try
        {
            if (!process.HasExited)
            {
                SendSigTerm(process.Id);
                if (!process.WaitForExit(2000))
                {
                    process.Kill(entireProcessTree: true);
                    process.WaitForExit(2000);
                }
            }
        }
        catch
        {
            // Ignore cleanup failures.
        }
        finally
        {
            process.Dispose();
        }
    }

    private static int GetRandomPort()
    {
        using var listener = new TcpListener(IPAddress.Parse(Host), 0);
        listener.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        listener.Stop();
        return port;
    }

    private static async Task RunProcessWorkerAsync()
    {
        var totalRequestsRaw = Environment.GetEnvironmentVariable("BENCH_TOTAL_REQUESTS");
        var hasTotalRequests = !string.IsNullOrWhiteSpace(totalRequestsRaw);
        var totalRequests = 0L;

        if (hasTotalRequests &&
            (!long.TryParse(totalRequestsRaw, NumberStyles.Integer, CultureInfo.InvariantCulture, out totalRequests) ||
             totalRequests < 0))
        {
            Console.Error.WriteLine(JsonSerializer.Serialize(new Dictionary<string, object?>
            {
                ["type"] = "error",
                ["message"] = "BENCH_TOTAL_REQUESTS must be a valid non-negative integer",
            }));
            Environment.ExitCode = 1;
            return;
        }

        var durationMs = hasTotalRequests ? 0 : GetEnvInt("BENCH_DURATION_MS", 0);
        if (!hasTotalRequests && durationMs <= 0)
        {
            Console.Error.WriteLine(JsonSerializer.Serialize(new Dictionary<string, object?>
            {
                ["type"] = "error",
                ["message"] = "BENCH_DURATION_MS must be a valid positive integer",
            }));
            Environment.ExitCode = 1;
            return;
        }

        var stopRequested = false;
        Console.CancelKeyPress += (_, eventArgs) =>
        {
            stopRequested = true;
            eventArgs.Cancel = true;
        };
        AppDomain.CurrentDomain.ProcessExit += (_, _) =>
        {
            stopRequested = true;
        };

        var channel = new Channel<Dictionary<string, object?>>()
            .Post(TextCountPath, request =>
            {
                var payload = request.Body<Dictionary<string, string>>();
                return payload is not null && payload.TryGetValue("text", out var text) && text is not null
                    ? text.Length
                    : 0;
            })
            .Post("sync", _ => 1)
            .Post(
                "async",
                (Func<ChannelRequestContext<Dictionary<string, object?>>, Task<object?>>)(async _ =>
                {
                    await Task.Yield();
                    return 1;
                }));
        var sender = channel.Process(new Dictionary<string, object?>());

        Console.WriteLine(JsonSerializer.Serialize(new Dictionary<string, object?>
        {
            ["type"] = "ready",
            ["pid"] = Environment.ProcessId,
        }));

        async Task<(long Completed, long Errors, double ElapsedSeconds)> RunScenarioAsync(string path)
        {
            var completed = 0L;
            var errors = 0L;
            var started = Stopwatch.StartNew();
            while (!stopRequested && started.ElapsedMilliseconds < durationMs)
            {
                try
                {
                    _ = await sender.SendAsync<object?>(path);
                    completed += 1;
                }
                catch
                {
                    errors += 1;
                }
            }

            return (completed, errors, started.Elapsed.TotalSeconds);
        }

        async Task<(long Completed, long Errors, double ElapsedSeconds)> RunFixedScenarioAsync(string path, long requests)
        {
            var completed = 0L;
            var errors = 0L;
            var started = Stopwatch.StartNew();
            while (!stopRequested && (completed + errors) < requests)
            {
                try
                {
                    var result = await sender.SendAsync<object?>(path, TextCountBody);
                    if (result is int value && value == TextCountExpected)
                    {
                        completed += 1;
                    }
                    else
                    {
                        errors += 1;
                    }
                }
                catch
                {
                    errors += 1;
                }
            }

            return (completed, errors, started.Elapsed.TotalSeconds);
        }

        if (hasTotalRequests)
        {
            var fixedResult = await RunFixedScenarioAsync(TextCountPath, totalRequests);
            sender.Stop();
            Console.WriteLine(JsonSerializer.Serialize(new Dictionary<string, object?>
            {
                ["type"] = "result",
                ["pid"] = Environment.ProcessId,
                ["completed"] = fixedResult.Completed,
                ["errors"] = fixedResult.Errors,
                ["elapsedSeconds"] = fixedResult.ElapsedSeconds,
            }));
            return;
        }

        var sync = await RunScenarioAsync("sync");
        var async = await RunScenarioAsync("async");
        sender.Stop();

        Console.WriteLine(JsonSerializer.Serialize(new Dictionary<string, object?>
        {
            ["type"] = "result",
            ["pid"] = Environment.ProcessId,
            ["syncCompleted"] = sync.Completed,
            ["syncErrors"] = sync.Errors,
            ["syncElapsedSeconds"] = sync.ElapsedSeconds,
            ["asyncCompleted"] = async.Completed,
            ["asyncErrors"] = async.Errors,
            ["asyncElapsedSeconds"] = async.ElapsedSeconds,
            ["asyncFallback"] = false,
        }));
    }

    private static async Task RunServerWorkerAsync()
    {
        var port = GetEnvInt("BENCH_PORT", 0);
        if (port <= 0)
        {
            Console.Error.WriteLine(JsonSerializer.Serialize(new ResultMessage
            {
                Type = "error",
                Message = "BENCH_PORT must be a valid positive integer",
            }));
            Environment.ExitCode = 1;
            return;
        }

        long requests = 0;
        var started = Stopwatch.GetTimestamp();
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var stopped = 0;
        var runDurationMs = GetEnvInt("BENCH_DURATION_MS", 0);
        using var runDuration = runDurationMs > 0 ? new CancellationTokenSource(runDurationMs) : null;

        using var signalTerm = PosixSignalRegistration.Create(PosixSignal.SIGTERM, _ =>
        {
            if (Interlocked.Exchange(ref stopped, 1) == 0)
            {
                completion.TrySetResult();
            }
        });
        using var signalInt = PosixSignalRegistration.Create(PosixSignal.SIGINT, _ =>
        {
            if (Interlocked.Exchange(ref stopped, 1) == 0)
            {
                completion.TrySetResult();
            }
        });
        using CancellationTokenRegistration? runDurationRegistration = runDuration is null
            ? null
            : runDuration.Token.Register(() =>
            {
                if (Interlocked.Exchange(ref stopped, 1) == 0)
                {
                    completion.TrySetResult();
                }
            });

        using var server = new Channel<Dictionary<string, object?>>()
            .Post(TextCountPath, request =>
            {
                Interlocked.Increment(ref requests);
                var payload = request.Body<Dictionary<string, string>>();
                return payload is not null && payload.TryGetValue("text", out var text) && text is not null
                    ? text.Length
                    : 0;
            })
            .Listen(port);

        Console.WriteLine(JsonSerializer.Serialize(new ResultMessage
        {
            Type = "ready",
            Pid = Environment.ProcessId,
            Port = port,
        }));

        await completion.Task;

        server.Stop();
        var elapsedSeconds = Stopwatch.GetElapsedTime(started).TotalSeconds;
        Console.WriteLine(JsonSerializer.Serialize(new ResultMessage
        {
            Type = "result",
            Pid = Environment.ProcessId,
            Requests = requests,
            ElapsedSeconds = elapsedSeconds,
        }));
    }

    private static async Task RunClientWorkerAsync()
    {
        var port = GetEnvInt("BENCH_PORT", 0);
        var clientsCount = GetEnvInt("BENCH_CLIENTS", 0);
        var rpsPerClient = GetEnvInt("BENCH_RPS_PER_CLIENT", 0);
        var host = Environment.GetEnvironmentVariable("BENCH_HOST") ?? Host;
        var totalRequestsRaw = Environment.GetEnvironmentVariable("BENCH_TOTAL_REQUESTS");
        var hasTotalRequests = !string.IsNullOrWhiteSpace(totalRequestsRaw);
        var totalRequests = 0L;
        if (hasTotalRequests && (!long.TryParse(totalRequestsRaw, NumberStyles.Integer, CultureInfo.InvariantCulture, out totalRequests) || totalRequests < 0))
        {
            Console.Error.WriteLine(JsonSerializer.Serialize(new ResultMessage
            {
                Type = "error",
                Message = "BENCH_TOTAL_REQUESTS must be a valid non-negative integer",
            }));
            Environment.ExitCode = 1;
            return;
        }

        var requestMode = (Environment.GetEnvironmentVariable("BENCH_REQUEST_MODE") ?? "burst").Trim().ToLowerInvariant();
        if (hasTotalRequests && requestMode is not ("burst" or "stream"))
        {
            Console.Error.WriteLine(JsonSerializer.Serialize(new ResultMessage
            {
                Type = "error",
                Message = "BENCH_REQUEST_MODE must be either burst or stream",
            }));
            Environment.ExitCode = 1;
            return;
        }

        var maxInFlight = GetEnvInt("BENCH_MAX_IN_FLIGHT", 4096);
        if (hasTotalRequests && maxInFlight <= 0)
        {
            Console.Error.WriteLine(JsonSerializer.Serialize(new ResultMessage
            {
                Type = "error",
                Message = "BENCH_MAX_IN_FLIGHT must be a valid positive integer",
            }));
            Environment.ExitCode = 1;
            return;
        }

        if (port <= 0)
        {
            Console.Error.WriteLine(JsonSerializer.Serialize(new ResultMessage
            {
                Type = "error",
                Message = "BENCH_PORT must be a valid positive integer",
            }));
            Environment.ExitCode = 1;
            return;
        }

        if (clientsCount <= 0)
        {
            Console.Error.WriteLine(JsonSerializer.Serialize(new ResultMessage
            {
                Type = "error",
                Message = "BENCH_CLIENTS must be a valid positive integer",
            }));
            Environment.ExitCode = 1;
            return;
        }

        if (!hasTotalRequests && rpsPerClient <= 0)
        {
            Console.Error.WriteLine(JsonSerializer.Serialize(new ResultMessage
            {
                Type = "error",
                Message = "BENCH_RPS_PER_CLIENT must be a positive number",
            }));
            Environment.ExitCode = 1;
            return;
        }

        var started = Stopwatch.GetTimestamp();
        var targetRps = clientsCount * rpsPerClient;
        var clients = new List<ClientSender<Dictionary<string, object?>>>(clientsCount);
        var connected = new bool[clientsCount];

        var connectedCount = 0;
        var nextClientIndex = 0;
        long requestsSent = 0;
        long responsesReceived = 0;
        long errors = 0;
        var inFlight = 0;
        var running = false;
        var stopped = 0;
        var tickMs = hasTotalRequests ? 1 : 10;
        var tickTimer = (PeriodicTimer?)null;
        var runDurationMs = hasTotalRequests ? 0 : GetEnvInt("BENCH_DURATION_MS", 0);
        using var runDuration = runDurationMs > 0 ? new CancellationTokenSource(runDurationMs) : null;

        var connectedReady = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var completion = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var addressPort = port;
        for (var i = 0; i < clientsCount; i += 1)
        {
            var clientIndex = i;
            var sender = new Channel<Dictionary<string, object?>>().Connect(
                $"ws://{host}:{addressPort}",
                new ClientOptions<Dictionary<string, object?>>
                {
                    OnConnect = _ =>
                    {
                        if (!connected[clientIndex])
                        {
                            connected[clientIndex] = true;
                            if (Interlocked.Increment(ref connectedCount) == clientsCount)
                            {
                                connectedReady.TrySetResult();
                            }
                        }

                        return Task.CompletedTask;
                    },
                });
            clients.Add(sender);
        }

        using var setupCts = new CancellationTokenSource(TimeSpan.FromSeconds(10));
        using var setupRegistration = setupCts.Token.Register(() =>
        {
            if (Volatile.Read(ref connectedCount) != clientsCount)
            {
                Console.Error.WriteLine(JsonSerializer.Serialize(new ResultMessage
                {
                    Type = "error",
                    Message = $"Timed out waiting for clients to connect ({Volatile.Read(ref connectedCount)}/{clientsCount})",
                }));
                completion.TrySetResult();
            }
        });

        using var signalTerm = PosixSignalRegistration.Create(PosixSignal.SIGTERM, _ =>
        {
            completion.TrySetResult();
        });
        using var signalInt = PosixSignalRegistration.Create(PosixSignal.SIGINT, _ =>
        {
            completion.TrySetResult();
        });
        using CancellationTokenRegistration? runDurationRegistration = runDuration is null
            ? null
            : runDuration.Token.Register(() =>
            {
                completion.TrySetResult();
            });

        try
        {
            await connectedReady.Task.WaitAsync(setupCts.Token);
        }
        catch (OperationCanceledException)
        {
            return;
        }

        running = true;
        Console.WriteLine(JsonSerializer.Serialize(new ResultMessage
        {
            Type = "ready",
            Pid = Environment.ProcessId,
            Clients = clientsCount,
        }));

        void DispatchOne()
        {
            var client = clients[nextClientIndex];
            nextClientIndex = (nextClientIndex + 1) % clients.Count;
            Interlocked.Increment(ref requestsSent);
            Interlocked.Increment(ref inFlight);

            _ = client.SendAsync<int>(TextCountPath, TextCountBody)
                .ContinueWith(task =>
                {
                    if (task.IsCompletedSuccessfully)
                    {
                        Interlocked.Increment(ref responsesReceived);
                    }
                    else
                    {
                        Interlocked.Increment(ref errors);
                    }

                    Interlocked.Decrement(ref inFlight);
                }, TaskScheduler.Default);
        }

        tickTimer = new PeriodicTimer(TimeSpan.FromMilliseconds(tickMs));
        var scheduler = Task.Run(async () =>
        {
            double carry = 0d;
            while (running && tickTimer is not null && await tickTimer.WaitForNextTickAsync())
            {
                if (hasTotalRequests)
                {
                    if (requestMode == "stream")
                    {
                        while (Interlocked.Read(ref requestsSent) < totalRequests
                            && Volatile.Read(ref inFlight) < maxInFlight)
                        {
                            DispatchOne();
                        }
                    }
                    else
                    {
                        var budget = 4096;
                        while (Interlocked.Read(ref requestsSent) < totalRequests && budget > 0)
                        {
                            DispatchOne();
                            budget -= 1;
                        }
                    }

                    var accounted = Interlocked.Read(ref responsesReceived) + Interlocked.Read(ref errors);
                    if (Interlocked.Read(ref requestsSent) >= totalRequests && accounted >= totalRequests && Volatile.Read(ref inFlight) <= 0)
                    {
                        completion.TrySetResult();
                    }
                    continue;
                }

                carry += (targetRps * tickMs) / 1000d;
                var sends = (int)Math.Floor(carry);
                carry -= sends;

                for (var i = 0; i < sends; i += 1)
                {
                    DispatchOne();
                }
            }
        });

        if (hasTotalRequests && totalRequests == 0)
        {
            completion.TrySetResult();
        }

        await completion.Task;

        running = false;
        tickTimer?.Dispose();
        await scheduler;

        var waitUntil = DateTime.UtcNow + TimeSpan.FromMilliseconds(2000);
        while (Volatile.Read(ref inFlight) > 0 && DateTime.UtcNow < waitUntil)
        {
            await Task.Delay(5);
        }

        foreach (var client in clients)
        {
            client.Stop();
            client.Dispose();
        }

        var elapsedSeconds = Stopwatch.GetElapsedTime(started).TotalSeconds;
        Console.WriteLine(JsonSerializer.Serialize(new ResultMessage
        {
            Type = "result",
            Pid = Environment.ProcessId,
            RequestsSent = requestsSent,
            ResponsesReceived = responsesReceived,
            Errors = errors,
            ElapsedSeconds = elapsedSeconds,
        }));

        if (Interlocked.Exchange(ref stopped, 1) == 0)
        {
            Environment.ExitCode = 0;
        }
    }

    private static int GetEnvInt(string name, int fallback)
    {
        var text = Environment.GetEnvironmentVariable(name);
        return int.TryParse(text, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value) ? value : fallback;
    }

    private static string? GetArg(IReadOnlyList<string> args, string name)
    {
        for (var i = 0; i < args.Count - 1; i += 1)
        {
            if (string.Equals(args[i], name, StringComparison.OrdinalIgnoreCase))
            {
                return args[i + 1];
            }
        }

        return null;
    }

    private sealed record Worker(
        Process Process,
        TaskCompletionSource Ready,
        TaskCompletionSource<ResultMessage> Result);

    private sealed class BenchmarkOutput
    {
        [JsonPropertyName("language")]
        public required string Language { get; init; }

        [JsonPropertyName("results")]
        public required List<Result> Results { get; init; }
    }

    private sealed class Result
    {
        [JsonPropertyName("rps")]
        public required double Rps { get; init; }

        [JsonPropertyName("clients")]
        public required int Clients { get; init; }

        [JsonPropertyName("client")]
        public required ResourceUsage Client { get; init; }

        [JsonPropertyName("server")]
        public required ResourceUsage Server { get; init; }
    }

    private sealed class ResourceUsage
    {
        [JsonPropertyName("cpu")]
        public required double Cpu { get; init; }

        [JsonPropertyName("mem")]
        public required double Mem { get; init; }
    }

    private sealed class ResultMessage
    {
        [JsonPropertyName("type")]
        public required string Type { get; init; }

        [JsonPropertyName("message")]
        public string? Message { get; init; }

        [JsonPropertyName("pid")]
        public int? Pid { get; init; }

        [JsonPropertyName("port")]
        public int? Port { get; init; }

        [JsonPropertyName("clients")]
        public int? Clients { get; init; }

        [JsonPropertyName("requests")]
        public long? Requests { get; init; }

        [JsonPropertyName("elapsedSeconds")]
        public double? ElapsedSeconds { get; init; }

        [JsonPropertyName("requestsSent")]
        public long? RequestsSent { get; init; }

        [JsonPropertyName("responsesReceived")]
        public long? ResponsesReceived { get; init; }

        [JsonPropertyName("errors")]
        public long? Errors { get; init; }

        [JsonPropertyName("syncCompleted")]
        public long? SyncCompleted { get; init; }

        [JsonPropertyName("syncErrors")]
        public long? SyncErrors { get; init; }

        [JsonPropertyName("syncElapsedSeconds")]
        public double? SyncElapsedSeconds { get; init; }

        [JsonPropertyName("asyncCompleted")]
        public long? AsyncCompleted { get; init; }

        [JsonPropertyName("asyncErrors")]
        public long? AsyncErrors { get; init; }

        [JsonPropertyName("asyncElapsedSeconds")]
        public double? AsyncElapsedSeconds { get; init; }

        [JsonPropertyName("asyncFallback")]
        public bool? AsyncFallback { get; init; }
    }
}
