using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;
using kafka.Shared.Models.Accounts;
using kafka.Shared.Models.Common;
using kafka.Shared.Validation;

namespace kafka.UnitTests.Validator;

public sealed class AccountEventValidatorTests
{
    #region Methods

    #region Private

    #region CreateValidAccount
    /// <summary>
    /// Creates a valid AccountDocument for testing purposes.
    /// </summary>
    /// <returns>A valid AccountDocument.</returns>
    private static AccountDocument CreateValidAccount()
    {
        return new AccountDocument
        {
            Id = "64c3e0f5d1f4c2a1b2c3d4e5",
            Version = 48,
            IsActive = true,
            IsDeleted = false,
            MappingFields = new MappingFields
            {
                EmployeeId = new EmployeeIdentifier
                {
                    GroupId = "ABC123"
                }
            },
            PersonalData = new PersonalData
            {
                FirstName = "Testo",
                LastName = "Testic"
            }
        };
    }
    #endregion

    #endregion

    #region Public

    #region Validate_WhenAccountIsValid_DoesNotThrow
    /// <summary>
    /// Validates that the AccountEventValidator does not throw an exception when provided with a valid AccountDocument.
    /// </summary>
    [Fact]
    public void Validate_WhenAccountIsValid_DoesNotThrow()
    {
        var account = CreateValidAccount();

        var exception = Record.Exception(() => AccountEventValidator.Validate(account));

        Assert.Null(exception);
    }
    #endregion

    #region Deserialize_WhenRequiredPropertyIsOmitted_Throws
    /// <summary>
    /// Validates that deserialization of an AccountDocument throws a JsonException when a required property is omitted from the JSON payload.
    /// </summary>
    /// <param name="propertyName">The name of the required property to omit.</param>
    [Theory]
    [InlineData("version")]
    [InlineData("personalData")]
    public void Deserialize_WhenRequiredPropertyIsOmitted_Throws(string propertyName)
    {
        var payload = JsonSerializer.SerializeToNode(CreateValidAccount(), JsonSerializerOptions.Web)!.AsObject();
        payload.Remove(propertyName);

        var exception = Assert.Throws<JsonException>(() =>
            JsonSerializer.Deserialize<AccountDocument>(payload.ToJsonString(), JsonSerializerOptions.Web));

        Assert.Contains(propertyName, exception.Message);
    }
    #endregion

    #region Validate_WhenJsonPersonalDataIsNull_Throws
    /// <summary>
    /// Validates that the AccountEventValidator throws an ArgumentException when the PersonalData property of the AccountDocument is null.
    /// </summary>
    [Fact]
    public void Validate_WhenJsonPersonalDataIsNull_Throws()
    {
        var payload = JsonSerializer.SerializeToNode(CreateValidAccount(), JsonSerializerOptions.Web)!.AsObject();
        payload["personalData"] = null;
        var account = JsonSerializer.Deserialize<AccountDocument>(payload.ToJsonString(), JsonSerializerOptions.Web)!;

        var exception = Assert.Throws<ArgumentException>(() => AccountEventValidator.Validate(account));

        Assert.Contains("personalData is required", exception.Message);
    }
    #endregion

    #region Validate_WhenJsonVersionIsExplicitlyZero_DoesNotThrow
    /// <summary>
    /// Validates that the AccountEventValidator does not throw an exception when the Version property of the AccountDocument is explicitly set to zero in the JSON payload.
    /// </summary>
    [Fact]
    public void Validate_WhenJsonVersionIsExplicitlyZero_DoesNotThrow()
    {
        var original = CreateValidAccount();
        original.Version = 0;
        var json = JsonSerializer.Serialize(original, JsonSerializerOptions.Web);
        var account = JsonSerializer.Deserialize<AccountDocument>(json, JsonSerializerOptions.Web)!;

        Assert.Equal(0, account.Version);
        Assert.Null(Record.Exception(() => AccountEventValidator.Validate(account)));
    }
    #endregion

    #region Validate_WhenIdIsInvalid_Throws
    /// <summary>
    /// Validates that the AccountEventValidator throws an ArgumentException when the Id of the AccountDocument is invalid.
    /// </summary>
    [Fact]
    public void Validate_WhenIdIsInvalid_Throws()
    {
        var account = CreateValidAccount();
        account.Id = "not-an-object-id";

        var exception = Assert.Throws<ArgumentException>(() => AccountEventValidator.Validate(account));

        Assert.Contains("valid ObjectId", exception.Message);
    }
    #endregion

    #region Validate_WhenVersionIsNegative_Throws
    /// <summary>
    /// Validates that the AccountEventValidator throws an ArgumentException when the Version of the AccountDocument is negative.
    /// </summary>
    [Fact]
    public void Validate_WhenVersionIsNegative_Throws()
    {
        var account = CreateValidAccount();
        account.Version = -1;

        var exception = Assert.Throws<ArgumentException>(() => AccountEventValidator.Validate(account));

        Assert.Contains("version cannot be negative", exception.Message);
    }
    #endregion

    #region Validate_WhenGroupIdIsMissing_Throws
    /// <summary>
    /// Validates that the AccountEventValidator throws an ArgumentException when the GroupId of the AccountDocument is missing.
    /// </summary>
    [Fact]
    public void Validate_WhenGroupIdIsMissing_Throws()
    {
        var account = CreateValidAccount();

        account.MappingFields.EmployeeId.GroupId = string.Empty;

        var exception = Assert.Throws<ArgumentException>(() => AccountEventValidator.Validate(account));

        Assert.Contains("groupId is required", exception.Message);
    }
    #endregion

    #region Validate_WhenPersonalDataIsMissing_Throws
    /// <summary>
    /// Validates that the AccountEventValidator throws an ArgumentException when the PersonalData of the AccountDocument is missing.
    /// </summary>
    [Fact]
    public void Validate_WhenPersonalDataIsMissing_Throws()
    {
        var account = CreateValidAccount();
        account.PersonalData = null!;

        var exception = Assert.Throws<ArgumentException>(() => AccountEventValidator.Validate(account));

        Assert.Contains("personalData is required", exception.Message);
    }
    #endregion

    #region Validate_WhenFirstNameIsMissing_Throws
    /// <summary>
    /// Validates that the AccountEventValidator throws an ArgumentException when the FirstName of the PersonalData in the AccountDocument is missing.
    /// </summary>
    [Fact]
    public void Validate_WhenAccountIsNull_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => AccountEventValidator.Validate(null!));
    }
    #endregion

    #endregion

    #endregion
}