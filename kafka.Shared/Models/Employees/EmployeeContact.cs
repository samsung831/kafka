using MongoDB.Bson.Serialization.Attributes;

namespace kafka.Shared.Models.Employees;

public sealed class EmployeeContact
{
    #region Properties

    #region Public

    #region Work
    [BsonElement("work")]
    public WorkContact? Work { get; set; }
    #endregion

    #endregion

    #endregion
}