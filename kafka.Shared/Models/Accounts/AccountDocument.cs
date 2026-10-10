using kafka.Shared.Models.Common;
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
    public Dictionary<string, object> Names
    {
        get;
        set => field = value ?? new();
    } = new();
    #endregion

    #region Address
    [BsonElement("address")]
    public Address Address { get; set; } = new();
    #endregion

    #region PersonalData
    [BsonElement("personalData")]
    [JsonRequired]
    public PersonalData PersonalData { get; set; } = new();
    #endregion

    #region EmployeeContact
    [BsonElement("employeeContact")]
    public AccountEmployeeContact EmployeeContact { get; set; } = new();
    #endregion

    #region GroupId
    [BsonIgnore]
    public string GroupId => MappingFields.EmployeeId.GroupId;
    #endregion

    #endregion

    #endregion
}