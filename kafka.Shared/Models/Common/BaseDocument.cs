using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json.Serialization;

namespace kafka.Shared.Models.Common;

public abstract class BaseDocument
{
    #region Properties

    #region Public

    #region Id
    [BsonId]
    [BsonRepresentation(BsonType.ObjectId)]
    [BsonElement("_id")]
    [JsonPropertyName("_id")]
    public string Id { get; set; } = string.Empty;
    #endregion

    #region IsActive
    [BsonElement("isActive")]
    public bool IsActive { get; set; }
    #endregion

    #region IsDeleted
    [BsonElement("isDeleted")]
    public bool IsDeleted { get; set; }
    #endregion

    #region CreatedDate
    [BsonElement("createdDate")]
    [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
    public DateTime CreatedDate { get; set; }
    #endregion

    #region ModifiedDate
    [BsonElement("modifiedDate")]
    [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
    public DateTime ModifiedDate { get; set; }
    #endregion

    #region Version
    [BsonElement("version")]
    public long Version { get; set; }
    #endregion

    #endregion

    #endregion
}