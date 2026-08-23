using System;
using System.Collections.Generic;
using System.Text;
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