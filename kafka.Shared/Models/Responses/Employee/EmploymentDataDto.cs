namespace kafka.Shared.Models.Responses.Employee;

public sealed class EmploymentDataDto
{
    #region Properties

    #region Public

    #region EmploymentStatus
    public string? EmploymentStatus { get; set; }
    #endregion

    #region OriginalHireDate
    public DateTime? OriginalHireDate { get; set; }
    #endregion

    #region LastHireDate
    public DateTime? LastHireDate { get; set; }
    #endregion

    #region LastJobPositionChangeDate
    public DateTime? LastJobPositionChangeDate { get; set; }
    #endregion

    #region ExpiredContractDate
    public DateTime? ExpiredContractDate { get; set; }
    #endregion

    #endregion

    #endregion
}