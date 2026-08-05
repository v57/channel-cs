using System.Text.Json;
using System.Text.Json.Serialization;

namespace ChannelCs;

public static class JsonCodec
{
    internal static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
    };

    public static IReadOnlyList<ChannelMessage> Deserialize(ReadOnlyMemory<byte> utf8)
    {
        if (utf8.IsEmpty)
        {
            return Array.Empty<ChannelMessage>();
        }

        var reader = new Utf8JsonReader(utf8.Span, true, default);
        reader.Read();

        if (reader.TokenType == JsonTokenType.StartArray)
        {
            var messages = JsonSerializer.Deserialize(utf8.Span, ChannelJsonContext.Default.ChannelMessageArray);
            return messages ?? Array.Empty<ChannelMessage>();
        }

        if (reader.TokenType == JsonTokenType.StartObject)
        {
            var message = JsonSerializer.Deserialize(utf8.Span, ChannelJsonContext.Default.ChannelMessage);
            return message is null ? Array.Empty<ChannelMessage>() : [message];
        }

        throw new JsonException("Expected a JSON object or array.");
    }

    public static byte[] Serialize(object value)
    {
        return JsonSerializer.SerializeToUtf8Bytes(value, Options);
    }

    public static T? DeserializeBody<T>(object? value)
    {
        if (value is null)
        {
            return default;
        }

        if (value is T typed)
        {
            return typed;
        }

        if (value is JsonElement element)
        {
            if (element.ValueKind is JsonValueKind.Null or JsonValueKind.Undefined)
            {
                return default;
            }

            return element.Deserialize<T>(Options);
        }

        var bytes = JsonSerializer.SerializeToUtf8Bytes(value, Options);
        return JsonSerializer.Deserialize<T>(bytes, Options);
    }

    public static JsonElement ToJsonElement(object value)
    {
        if (value is JsonElement element)
        {
            return element;
        }

        var bytes = JsonSerializer.SerializeToUtf8Bytes(value, Options);
        using var document = JsonDocument.Parse(bytes);
        return document.RootElement.Clone();
    }
}

[JsonSourceGenerationOptions(
    DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    PropertyNamingPolicy = JsonKnownNamingPolicy.CamelCase)]
[JsonSerializable(typeof(ChannelMessage))]
[JsonSerializable(typeof(ChannelMessage[]))]
internal partial class ChannelJsonContext : JsonSerializerContext
{
}
