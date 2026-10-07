namespace kafka.Shared.Models.Responses.Account;

public sealed class AddressDto
{
    #region Properties

    #region Public

    #region Type
    public string? Type { get; set; }
    #endregion

    #region Country
    public string? Country { get; set; }
    #endregion

    #region State
    public string? State { get; set; }
    #endregion

    #region City
    public string? City { get; set; }
    #endregion

    #region ZipCode
    public string? ZipCode { get; set; }
    #endregion

    #region AddressLine
    public string? AddressLine { get; set; }
    #endregion

    #endregion

    #endregion
}