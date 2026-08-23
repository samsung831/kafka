using kafka.Shared.Models.Accounts;
using kafka.Shared.Models.Employees;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka.Shared.Models.Responses;

public sealed class PersonResponseDto
{
    #region Properties

    #region Public

    #region Account
    public required AccountDocument Account { get; init; }
    #endregion

    #region Employees
    public IReadOnlyCollection<EmployeeDocument> Employees { get; init; } = Array.Empty<EmployeeDocument>();
    #endregion

    #endregion

    #endregion
}
