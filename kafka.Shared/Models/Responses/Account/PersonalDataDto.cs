namespace kafka.Shared.Models.Responses.Account;

public sealed class PersonalDataDto
{
    #region Properties

    #region Public

    #region Age
    public int? Age { get; set; }
    #endregion

    #region BirthDate
    public DateTime? BirthDate { get; set; }
    #endregion

    #region FirstName
    public string FirstName { get; set; } = string.Empty;
    #endregion

    #region LastName
    public string LastName { get; set; } = string.Empty;
    #endregion

    #region Gender
    public string? Gender { get; set; }
    #endregion

    #endregion

    #endregion
}