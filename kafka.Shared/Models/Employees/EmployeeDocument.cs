using kafka.Shared.Models.Common;
using MongoDB.Bson.Serialization.Attributes;

namespace kafka.Shared.Models.Employees;

public sealed class EmployeeDocument : BaseDocument
{
    #region Properties

    #region Public

    #region MappingFields
    [BsonElement("mappingFields")]
    public MappingFields MappingFields { get; set; } = new();
    #endregion

    #region EmploymentData
    [BsonElement("employmentData")]
    public EmploymentData EmploymentData { get; set; } = new();
    #endregion

    #region EmployeeContact
    [BsonElement("employeeContact")]
    public EmployeeContact? EmployeeContact { get; set; }
    #endregion

    #region GroupId
    [BsonIgnore]
    public string GroupId => MappingFields.EmployeeId.GroupId;
    #endregion

    #endregion

    #endregion
}