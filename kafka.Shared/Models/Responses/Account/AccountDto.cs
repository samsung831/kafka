using System.Text.Json;

namespace kafka.Shared.Models.Responses.Account;

public sealed class AccountDto
{
    #region Properties

    #region Public

    #region IsActive
    public bool IsActive { get; set; }
    #endregion

    #region IsDeleted
    public bool IsDeleted { get; set; }
    #endregion

    #region Names
    public Dictionary<string, object> Names { get; set; } = new();
    #endregion

    #region Address
    public AddressDto Address { get; set; } = new();
    #endregion

    #region PersonalData
    public PersonalDataDto PersonalData { get; set; } = new();
    #endregion

    #region EmployeeContact
    public AccountEmployeeContactDto EmployeeContact { get; set; } = new();
    #endregion

    #endregion

    #endregion
}