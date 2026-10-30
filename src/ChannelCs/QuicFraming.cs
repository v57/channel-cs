namespace ChannelCs;

/// <summary>
/// Framing of the QUIC transport (docs/quic.md in the TypeScript reference): one bidirectional stream per connection
/// carrying the same JSON messages as a WebSocket, each one prefixed with its length as a QUIC variable-length integer
/// (RFC 9000 section 16). An empty frame is a keepalive.
/// </summary>
public static class QuicFraming
{
    /// <summary>The QUIC application protocol (ALPN) of a Channel connection.</summary>
    public const string Alpn = "channel/1";

    /// <summary>Largest accepted frame, the same as the WebSocket transports' (Bun's default).</summary>
    public const int DefaultMaxPayload = 16 * 1024 * 1024;

    /// <summary>Connections silent this long are closed.</summary>
    public static readonly TimeSpan IdleTimeout = TimeSpan.FromSeconds(30);

    /// <summary>How often a client sends an empty frame to keep its connection open.</summary>
    public static readonly TimeSpan KeepAliveInterval = TimeSpan.FromSeconds(15);

    /// <summary>Bytes the varint length of a <paramref name="length"/>-byte payload takes: 1, 2, 4 or 8.</summary>
    public static int HeaderSize(long length) => length switch
    {
        < 1 << 6 => 1,
        < 1 << 14 => 2,
        < 1 << 30 => 4,
        _ => 8,
    };

    /// <summary>Writes the varint length of a payload and returns how many bytes it took.</summary>
    public static int WriteHeader(Span<byte> destination, long length)
    {
        var size = HeaderSize(length);
        var value = (ulong)length;
        for (var i = size - 1; i >= 0; i -= 1)
        {
            destination[i] = (byte)value;
            value >>= 8;
        }

        destination[0] |= (byte)(size switch { 1 => 0x00, 2 => 0x40, 4 => 0x80, _ => 0xc0 });
        return size;
    }

    /// <summary>One frame: the varint length of <paramref name="payload"/>, then the payload.</summary>
    public static byte[] Encode(ReadOnlySpan<byte> payload)
    {
        var frame = new byte[HeaderSize(payload.Length) + payload.Length];
        payload.CopyTo(frame.AsSpan(WriteHeader(frame, payload.Length)));
        return frame;
    }
}

/// <summary>A frame above the payload limit; the connection is closed.</summary>
public sealed class QuicFrameTooLargeException : IOException
{
    public QuicFrameTooLargeException(ulong length, int maxPayload)
        : base($"QUIC frame of {length} bytes is above the {maxPayload}-byte limit")
    {
    }
}

/// <summary>Splits a stream back into frame payloads, skipping keepalives.</summary>
public sealed class QuicFrameReader
{
    private readonly Stream _stream;
    private readonly int _maxPayload;
    private readonly byte[] _buffer = new byte[64 * 1024];
    private int _start;
    private int _end;

    public QuicFrameReader(Stream stream, int maxPayload = QuicFraming.DefaultMaxPayload)
    {
        _stream = stream;
        _maxPayload = maxPayload > 0 ? maxPayload : QuicFraming.DefaultMaxPayload;
    }

    /// <summary>
    /// The next non-empty payload, or null when the stream ends between frames. Throws
    /// <see cref="EndOfStreamException"/> when it ends inside one and <see cref="QuicFrameTooLargeException"/> for a
    /// frame above the limit.
    /// </summary>
    public async ValueTask<byte[]?> ReadAsync(CancellationToken cancellationToken = default)
    {
        while (true)
        {
            if (!await FillAsync(1, cancellationToken).ConfigureAwait(false))
            {
                return null;
            }

            var size = 1 << (_buffer[_start] >> 6);
            if (!await FillAsync(size, cancellationToken).ConfigureAwait(false))
            {
                throw new EndOfStreamException("QUIC stream ended inside a frame header");
            }

            ulong length = (uint)(_buffer[_start] & 0x3f);
            for (var i = 1; i < size; i += 1)
            {
                length = (length << 8) | _buffer[_start + i];
            }

            _start += size;
            if (length > (ulong)_maxPayload)
            {
                throw new QuicFrameTooLargeException(length, _maxPayload);
            }

            if (length == 0)
            {
                continue;
            }

            var payload = new byte[(int)length];
            var copied = Math.Min(_end - _start, payload.Length);
            _buffer.AsSpan(_start, copied).CopyTo(payload);
            _start += copied;
            while (copied < payload.Length)
            {
                var read = await _stream.ReadAsync(payload.AsMemory(copied), cancellationToken).ConfigureAwait(false);
                if (read == 0)
                {
                    throw new EndOfStreamException("QUIC stream ended inside a frame");
                }

                copied += read;
            }

            return payload;
        }
    }

    /// <summary>Buffers at least <paramref name="count"/> (at most 8) bytes; false at the end of the stream.</summary>
    private async ValueTask<bool> FillAsync(int count, CancellationToken cancellationToken)
    {
        if (_end - _start >= count)
        {
            return true;
        }

        if (_start > 0)
        {
            _buffer.AsSpan(_start, _end - _start).CopyTo(_buffer);
            _end -= _start;
            _start = 0;
        }

        while (_end < count)
        {
            var read = await _stream.ReadAsync(_buffer.AsMemory(_end), cancellationToken).ConfigureAwait(false);
            if (read == 0)
            {
                return false;
            }

            _end += read;
        }

        return true;
    }
}

/// <summary>
/// Queues frames and writes them from one loop, so senders never wait on flow control and frames queued meanwhile go
/// out in one write.
/// </summary>
public sealed class QuicFrameWriter
{
    private readonly Stream _stream;
    private readonly Action<Exception>? _onError;
    private readonly object _gate = new();
    private MemoryStream _pending = new();
    private MemoryStream _writing = new();
    private bool _running;
    private bool _closed;

    public QuicFrameWriter(Stream stream, Action<Exception>? onError = null)
    {
        _stream = stream;
        _onError = onError;
    }

    /// <summary>Queues one frame (an empty payload is a keepalive); false once the writer is closed.</summary>
    public bool Write(ReadOnlySpan<byte> payload)
    {
        Span<byte> header = stackalloc byte[8];
        var size = QuicFraming.WriteHeader(header, payload.Length);
        lock (_gate)
        {
            if (_closed)
            {
                return false;
            }

            _pending.Write(header[..size]);
            _pending.Write(payload);
            if (_running)
            {
                return true;
            }

            _running = true;
        }

        _ = Task.Run(RunAsync);
        return true;
    }

    /// <summary>Stops writing; queued frames that haven't gone out yet are dropped.</summary>
    public void Close()
    {
        lock (_gate)
        {
            _closed = true;
            _pending.SetLength(0);
        }
    }

    private async Task RunAsync()
    {
        while (true)
        {
            lock (_gate)
            {
                if (_closed || _pending.Length == 0)
                {
                    _running = false;
                    return;
                }

                (_pending, _writing) = (_writing, _pending);
            }

            try
            {
                await _stream.WriteAsync(_writing.GetBuffer().AsMemory(0, (int)_writing.Length)).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                lock (_gate)
                {
                    _closed = true;
                    _running = false;
                    _pending.SetLength(0);
                }

                _onError?.Invoke(exception);
                return;
            }
            finally
            {
                _writing.SetLength(0);
            }
        }
    }
}
