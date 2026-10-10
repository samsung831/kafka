using kafka.Shared.Models.Common;
using kafka.Shared.Serialization;
using MongoDB.Bson;
using MongoDB.Bson.Serialization.Attributes;
using System.Net;
using System.Text.Json.Serialization;

namespace kafka.Shared.Models.Accounts;

public sealed class AccountDocument : BaseDocument
{
    #region Properties

    #region Public

    #region MappingFields
    [BsonElement("mappingFields")]
    public MappingFields MappingFields { get; set; } = new();
    #endregion

    #region Names
    [BsonElement("names")]
    [BsonSerializer(typeof(NamesDictionaryBsonSerializer))]
    [JsonConverter(typeof(NamesDictionaryJsonConverter))]
    public Dictionary<string, object> Names
    {
        get;
        set => field = value ?? new();
    } = new();
    #endregion

    #region Address
    [BsonElement("address")]
    public Address? Address { get; set; }
    #endregion

    #region PersonalData
    [BsonElement("personalData")]
    [JsonRequired]
    public PersonalData PersonalData { get; set; } = new();
    #endregion

    #region EmployeeContact
    [BsonElement("employeeContact")]
    public AccountEmployeeContact? EmployeeContact { get; set; }
    #endregion

    #region GroupId
    [BsonIgnore]
    public string GroupId => MappingFields.EmployeeId.GroupId;
    #endregion

    #endregion

    #endregion
}