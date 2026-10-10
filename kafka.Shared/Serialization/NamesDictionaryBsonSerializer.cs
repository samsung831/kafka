using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Bson.Serialization.Serializers;

namespace kafka.Shared.Serialization;

public sealed class NamesDictionaryBsonSerializer : SerializerBase<Dictionary<string, object>>
{
    #region Methods

    #region Public

    #region Deserialize
    public override Dictionary<string, object> Deserialize(BsonDeserializationContext context, BsonDeserializationArgs args)
    {
        if (context.Reader.GetCurrentBsonType() == BsonType.Null)
        {
            context.Reader.ReadNull();
            return new();
        }

        var document = BsonDocumentSerializer.Instance.Deserialize(context);
        return (Dictionary<string, object>)BsonTypeMapper.MapToDotNetValue(document);
    }
    #endregion

    #region Serialize
    public override void Serialize(BsonSerializationContext context, BsonSerializationArgs args, Dictionary<string, object> value)
    {
        BsonDocumentSerializer.Instance.Serialize(context, new BsonDocument(value));
    }
    #endregion

    #endregion

    #endregion
}
