namespace kafka.Shared.Models.Responses.Account;

public sealed class AccountDto
{
    #region Properties

    #region Public

    #region Names
    public Dictionary<string, object> Names { get; set; } = new();
    #endregion

    #region Address
    public AddressDto? Address { get; set; }
    #endregion

    #region PersonalData
    public PersonalDataDto PersonalData { get; set; } = new();
    #endregion

    #region EmployeeContact
    public AccountEmployeeContactDto? EmployeeContact { get; set; }
    #endregion

    #endregion

    #endregion
}