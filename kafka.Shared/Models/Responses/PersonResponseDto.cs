using kafka.Shared.Models.Responses.Account;
using kafka.Shared.Models.Responses.Employee;

namespace kafka.Shared.Models.Responses;

public sealed class PersonResponseDto
{
    #region Properties

    #region Public

    #region Account
    public required AccountDto Account { get; init; }
    #endregion

    #region Employees
    public IReadOnlyCollection<EmployeeDto> Employees { get; init; } = Array.Empty<EmployeeDto>();
    #endregion

    #endregion

    #endregion
}
