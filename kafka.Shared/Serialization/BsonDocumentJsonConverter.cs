using MongoDB.Bson;
using MongoDB.Bson.IO;
using System.Text.Json;
using System.Text.Json.Serialization;
using JsonTokenType = System.Text.Json.JsonTokenType;

namespace kafka.Shared.Serialization;

public sealed class BsonDocumentJsonConverter : JsonConverter<BsonDocument>
{
    #region Methods

    #region Public

    #region Read
    /// <summary>
    /// Reads a JSON object and converts it to a BsonDocument.
    /// </summary>
    /// <param name="reader">The Utf8JsonReader to read from.</param>
    /// <param name="typeToConvert">The type to convert.</param>
    /// <param name="options">The JsonSerializerOptions to use.</param>
    /// <returns>A BsonDocument representation of the JSON object.</returns>
    /// <exception cref="JsonException">Thrown when the JSON cannot be converted to a BsonDocument.</exception>
    public override BsonDocument Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        if (reader.TokenType == JsonTokenType.Null)
        {
            return new BsonDocument();
        }

        if (reader.TokenType != JsonTokenType.StartObject)
        {
            throw new JsonException("names must be a JSON object or null.");
        }

        using var document = JsonDocument.ParseValue(ref reader);

        try
        {
            return BsonDocument.Parse(document.RootElement.GetRawText());
        }
        catch (Exception exception) when (exception is FormatException or BsonSerializationException or ArgumentException or OverflowException)
        {
            throw new JsonException("names could not be converted to BSON.", exception);
        }
    }
    #endregion

    #region Write
    /// <summary>
    /// Writes a BsonDocument as a JSON object.
    /// </summary>
    /// <param name="writer">The Utf8JsonWriter to write to.</param>
    /// <param name="value">The BsonDocument to write.</param>
    /// <param name="options">The JsonSerializerOptions to use.</param>
    public override void Write(Utf8JsonWriter writer, BsonDocument value, JsonSerializerOptions options)
    {
        var json = (value ?? new BsonDocument()).ToJson(new JsonWriterSettings
        {
            OutputMode = JsonOutputMode.RelaxedExtendedJson
        });

        using var document = JsonDocument.Parse(json);
        document.RootElement.WriteTo(writer);
    }
    #endregion

    #endregion

    #endregion
}
