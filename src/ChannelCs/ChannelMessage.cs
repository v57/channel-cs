using System.Text.Json;
using System.Text.Json.Serialization;

namespace ChannelCs;

[JsonConverter(typeof(ChannelMessageJsonConverter))]
public sealed class ChannelMessage
{
    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public int? Id { get; set; }

    /// <summary>
    /// A request id that is not a 32-bit integer (for example a string). The TS reference echoes any id back,
    /// so the server replies with this value when <see cref="Id"/> is null.
    /// </summary>
    [JsonIgnore]
    public JsonElement? RawId { get; set; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Path { get; set; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public object? Body { get; set; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public object? Context { get; set; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Error { get; set; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public int? Cancel { get; set; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Sub { get; set; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Unsub { get; set; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Topic { get; set; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public string? Stream { get; set; }

    [JsonIgnore(Condition = JsonIgnoreCondition.WhenWritingNull)]
    public bool? Done { get; set; }
}

/// <summary>
/// Lenient envelope codec: a field with an unexpected JSON type never fails the whole frame (which used to
/// close the connection or strand pending calls). Non-integer ids are kept in <see cref="ChannelMessage.RawId"/>,
/// non-string errors (e.g. <c>{"code":500}</c>) become their raw JSON text, other non-string names become
/// their raw JSON text too (so lookups fail with "api not found" like the TS reference), unknown fields are skipped.
/// </summary>
internal sealed class ChannelMessageJsonConverter : JsonConverter<ChannelMessage>
{
    public override ChannelMessage? Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType != JsonTokenType.StartObject)
        {
            throw new JsonException("Expected a JSON object.");
        }

        var message = new ChannelMessage();
        while (reader.Read())
        {
            if (reader.TokenType == JsonTokenType.EndObject)
            {
                return message;
            }

            if (reader.TokenType != JsonTokenType.PropertyName)
            {
                throw new JsonException("Expected a property name.");
            }

            var name = reader.GetString();
            reader.Read();

            switch (name)
            {
                case "id":
                    if (reader.TokenType == JsonTokenType.Number && reader.TryGetInt32(out var id))
                    {
                        message.Id = id;
                        message.RawId = null;
                    }
                    else if (reader.TokenType != JsonTokenType.Null)
                    {
                        message.Id = null;
                        message.RawId = JsonElement.ParseValue(ref reader);
                    }

                    break;
                case "path":
                    message.Path = ReadName(ref reader);
                    break;
                case "body":
                    message.Body = ReadValue(ref reader);
                    break;
                case "context":
                    message.Context = ReadValue(ref reader);
                    break;
                case "error":
                    message.Error = ReadName(ref reader);
                    break;
                case "cancel":
                    if (reader.TokenType == JsonTokenType.Number && reader.TryGetInt32(out var cancel))
                    {
                        message.Cancel = cancel;
                    }
                    else
                    {
                        reader.Skip();
                    }

                    break;
                case "sub":
                    message.Sub = ReadName(ref reader);
                    break;
                case "unsub":
                    message.Unsub = ReadName(ref reader);
                    break;
                case "topic":
                    message.Topic = ReadName(ref reader);
                    break;
                case "stream":
                    message.Stream = ReadName(ref reader);
                    break;
                case "done":
                    message.Done = reader.TokenType switch
                    {
                        JsonTokenType.True => true,
                        JsonTokenType.False => false,
                        _ => message.Done,
                    };
                    reader.Skip();
                    break;
                default:
                    reader.Skip();
                    break;
            }
        }

        throw new JsonException("Unexpected end of JSON.");
    }

    public override void Write(Utf8JsonWriter writer, ChannelMessage value, JsonSerializerOptions options)
    {
        writer.WriteStartObject();
        if (value.Id is int id)
        {
            writer.WriteNumber("id", id);
        }
        else if (value.RawId is JsonElement rawId)
        {
            writer.WritePropertyName("id");
            rawId.WriteTo(writer);
        }

        WriteString(writer, "path", value.Path);
        WriteValue(writer, "body", value.Body, options);
        WriteValue(writer, "context", value.Context, options);
        WriteString(writer, "error", value.Error);
        if (value.Cancel is int cancel)
        {
            writer.WriteNumber("cancel", cancel);
        }

        WriteString(writer, "sub", value.Sub);
        WriteString(writer, "unsub", value.Unsub);
        WriteString(writer, "topic", value.Topic);
        WriteString(writer, "stream", value.Stream);
        if (value.Done is bool done)
        {
            writer.WriteBoolean("done", done);
        }

        writer.WriteEndObject();
    }

    private static string? ReadName(ref Utf8JsonReader reader)
    {
        switch (reader.TokenType)
        {
            case JsonTokenType.String:
                return reader.GetString();
            case JsonTokenType.Null:
                return null;
            default:
                using (var document = JsonDocument.ParseValue(ref reader))
                {
                    return document.RootElement.GetRawText();
                }
        }
    }

    private static object? ReadValue(ref Utf8JsonReader reader)
    {
        if (reader.TokenType == JsonTokenType.Null)
        {
            return null;
        }

        return JsonElement.ParseValue(ref reader);
    }

    private static void WriteString(Utf8JsonWriter writer, string name, string? value)
    {
        if (value is not null)
        {
            writer.WriteString(name, value);
        }
    }

    private static void WriteValue(Utf8JsonWriter writer, string name, object? value, JsonSerializerOptions options)
    {
        if (value is null)
        {
            return;
        }

        writer.WritePropertyName(name);
        if (value is JsonElement element)
        {
            element.WriteTo(writer);
            return;
        }

        JsonSerializer.Serialize(writer, value, value.GetType(), options);
    }
}
