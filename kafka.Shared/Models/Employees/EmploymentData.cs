using MongoDB.Bson.Serialization.Attributes;

namespace kafka.Shared.Models.Employees;

public sealed class EmploymentData
{
    #region Properties

    #region Public

    #region EmploymentStatus
    [BsonElement("employmentStatus")]
    public string? EmploymentStatus { get; set; }
    #endregion

    #region OriginalHireDate
    [BsonElement("originalHireDate")]
    [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
    public DateTime? OriginalHireDate { get; set; }
    #endregion

    #region LastHireDate
    [BsonElement("lastHireDate")]
    [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
    public DateTime? LastHireDate { get; set; }
    #endregion

    #region LastJobPositionChangeDate
    [BsonElement("lastJobPositionChangeDate")]
    [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
    public DateTime? LastJobPositionChangeDate { get; set; }
    #endregion

    #region ExpiredContractDate
    [BsonElement("expiredContractDate")]
    [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
    public DateTime? ExpiredContractDate { get; set; }
    #endregion

    #endregion

    #endregion
}