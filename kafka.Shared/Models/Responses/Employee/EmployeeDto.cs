namespace kafka.Shared.Models.Responses.Employee;

public sealed class EmployeeDto
{
    #region Properties

    #region Public

    #region IsActive
    public bool IsActive { get; set; }
    #endregion

    #region IsDeleted
    public bool IsDeleted { get; set; }
    #endregion

    #region EmploymentData
    public EmploymentDataDto EmploymentData { get; set; } = new();
    #endregion

    #region EmployeeContact
    public EmployeeContactDto? EmployeeContact { get; set; }
    #endregion

    #endregion

    #endregion
}