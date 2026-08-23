using MongoDB.Bson.Serialization.Attributes;

namespace kafka.Shared.Models.Accounts;

public sealed class Address
{
    #region Properties

    #region Public

    #region Type
    [BsonElement("type")]
    public string? Type { get; set; }
    #endregion

    #region Country
    [BsonElement("country")]
    public string? Country { get; set; }
    #endregion

    #region State
    [BsonElement("state")]
    public string? State { get; set; }
    #endregion

    #region City
    [BsonElement("city")]
    public string? City { get; set; }
    #endregion

    #region ZipCode
    [BsonElement("zipCode")]
    public string? ZipCode { get; set; }
    #endregion

    #region AddressLine
    [BsonElement("address")]
    public string? AddressLine { get; set; }
    #endregion

    #endregion

    #endregion
}