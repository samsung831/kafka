using MongoDB.Bson;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace kafka.Shared.Serialization;

public sealed class NamesDictionaryJsonConverter : JsonConverter<Dictionary<string, object>>
{
    #region Methods

    #region Public

    #region Read
    public override Dictionary<string, object> Read(ref Utf8JsonReader reader, Type typeToConvert, JsonSerializerOptions options)
    {
        var document = new BsonDocumentJsonConverter().Read(ref reader, typeof(BsonDocument), options);
        return (Dictionary<string, object>)BsonTypeMapper.MapToDotNetValue(document);
    }
    #endregion

    #region Write
    public override void Write(Utf8JsonWriter writer, Dictionary<string, object> value, JsonSerializerOptions options)
    {
        JsonSerializer.Serialize(writer, value, options);
    }
    #endregion

    #endregion

    #endregion
}
