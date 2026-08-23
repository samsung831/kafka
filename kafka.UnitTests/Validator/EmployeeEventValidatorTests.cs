using System;
using System.Collections.Generic;
using System.Text;
using kafka.Shared.Models.Common;
using kafka.Shared.Models.Employees;
using kafka.Shared.Validation;

namespace kafka.UnitTests.Validator;

public sealed class EmployeeEventValidatorTests
{
    #region Methods

    #region Private

    #region CreateValidEmployee
    /// <summary>
    /// Creates a valid EmployeeDocument for testing purposes.
    /// </summary>
    /// <returns></returns>
    private static EmployeeDocument CreateValidEmployee()
    {
        return new EmployeeDocument
        {
            Id = "74c3e0f5d1f4c2a1b2c3d4e6",
            Version = 157,
            IsActive = true,
            IsDeleted = false,
            MappingFields = new MappingFields
            {
                EmployeeId = new EmployeeIdentifier
                {
                    GroupId = "ABC123"
                }
            },
            EmploymentData = new EmploymentData
            {
                EmploymentStatus = "Working"
            }
        };
    }
    #endregion

    #endregion

    #region Public

    #region Validate_WhenEmployeeIsValid_DoesNotThrow
    /// <summary>
    /// Validates that the EmployeeEventValidator does not throw an exception when provided with a valid EmployeeDocument.
    /// </summary>
    [Fact]
    public void Validate_WhenEmployeeIsValid_DoesNotThrow()
    {
        var employee = CreateValidEmployee();

        var exception = Record.Exception(() => EmployeeEventValidator.Validate(employee));

        Assert.Null(exception);
    }
    #endregion

    #region Validate_WhenIdIsInvalid_Throws
    /// <summary>
    /// Validates that the EmployeeEventValidator throws an ArgumentException when the Id of the EmployeeDocument is invalid.
    /// </summary>
    [Fact]
    public void Validate_WhenIdIsInvalid_Throws()
    {
        var employee = CreateValidEmployee();
        employee.Id = "not-an-object-id";

        var exception = Assert.Throws<ArgumentException>(() => EmployeeEventValidator.Validate(employee));

        Assert.Contains("valid ObjectId", exception.Message);
    }
    #endregion

    #region Validate_WhenVersionIsNegative_Throws
    /// <summary>
    /// Validates that the EmployeeEventValidator throws an ArgumentException when the Version of the EmployeeDocument is negative.
    /// </summary>
    [Fact]
    public void Validate_WhenVersionIsNegative_Throws()
    {
        var employee = CreateValidEmployee();
        employee.Version = -1;

        var exception = Assert.Throws<ArgumentException>(() => EmployeeEventValidator.Validate(employee));

        Assert.Contains("version cannot be negative", exception.Message);
    }
    #endregion

    #region Validate_WhenGroupIdIsMissing_Throws
    /// <summary>
    /// Validates that the EmployeeEventValidator throws an ArgumentException when the GroupId of the EmployeeDocument is missing.
    /// </summary>
    [Fact]
    public void Validate_WhenGroupIdIsMissing_Throws()
    {
        var employee = CreateValidEmployee();

        employee.MappingFields.EmployeeId.GroupId = "   ";

        var exception = Assert.Throws<ArgumentException>(() => EmployeeEventValidator.Validate(employee));

        Assert.Contains("groupId is required", exception.Message);
    }
    #endregion

    #region Validate_WhenEmploymentDataIsMissing_Throws
    /// <summary>
    /// Validates that the EmployeeEventValidator throws an ArgumentException when the EmploymentData of the EmployeeDocument is missing.
    /// </summary>
    [Fact]
    public void Validate_WhenEmploymentDataIsMissing_Throws()
    {
        var employee = CreateValidEmployee();
        employee.EmploymentData = null!;

        var exception = Assert.Throws<ArgumentException>(() => EmployeeEventValidator.Validate(employee));

        Assert.Contains("employmentData is required", exception.Message);
    }
    #endregion

    #region Validate_WhenEmploymentStatusIsMissing_Throws
    /// <summary>
    /// Validates that the EmployeeEventValidator throws an ArgumentException when the EmploymentStatus of the EmploymentData in the EmployeeDocument is missing.
    /// </summary>
    [Fact]
    public void Validate_WhenEmployeeIsNull_Throws()
    {
        Assert.Throws<ArgumentNullException>(() => EmployeeEventValidator.Validate(null!));
    }
    #endregion

    #endregion

    #endregion
}