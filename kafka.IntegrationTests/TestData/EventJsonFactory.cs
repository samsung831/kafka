using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;

namespace kafka.IntegrationTests.TestData;

public static class EventJsonFactory
{
    #region Methods

    #region Public

    #region CreateAccount
    /// <summary>
    /// Creates a JSON representation of an account document with the specified parameters.
    /// </summary>
    /// <param name="id">The unique identifier of the account.</param>
    /// <param name="groupId">The group ID associated with the account.</param>
    /// <param name="version">The version number of the account document.</param>
    /// <param name="firstName">The first name of the account holder.</param>
    /// <param name="lastName">The last name of the account holder.</param>
    /// <param name="isActive">Indicates whether the account is active.</param>
    /// <param name="isDeleted">Indicates whether the account is deleted.</param>
    /// <returns>A JSON string representing the account document.</returns>
    public static string CreateAccount(string id, string groupId, long version, string firstName = "Testo", string lastName = "Testic", bool isActive = true,
        bool isDeleted = false)
    {
        return JsonSerializer.Serialize(
            new
            {
                _id = id,
                isActive,
                isDeleted,
                createdDate = "2026-06-20T16:21:01.742Z",
                modifiedDate = "2026-06-20T16:21:01.742Z",
                version,
                mappingFields = new
                {
                    EmployeeId = new
                    {
                        groupId
                    }
                },
                names = new Dictionary<string, object>(),
                address = new
                {
                    type = (string?)null,
                    country = "HR",
                    state = "ISTARSKA",
                    city = "LABIN",
                    zipCode = "52220",
                    address = "RAVNI 16"
                },
                personalData = new
                {
                    age = 61,
                    birthDate = "1964-01-01T00:00:00.000Z",
                    firstName,
                    lastName,
                    gender = "Z"
                },
                employeeContact = new
                {
                    @private = new
                    {
                        email = (string?)null,
                        mobile = "+385 98 123 456",
                        countryCode = "+385",
                        country = "HR"
                    }
                }
            });
    }
    #endregion

    #region CreateEmployee
    /// <summary>
    /// Creates a JSON representation of an employee document with the specified parameters.
    /// </summary>
    /// <param name="id">The unique identifier of the employee.</param>
    /// <param name="groupId">The group ID associated with the employee.</param>
    /// <param name="version">The version number of the employee document.</param>
    /// <param name="isActive">Indicates whether the employee is active.</param>
    /// <param name="isDeleted">Indicates whether the employee is deleted.</param>
    /// <param name="employmentStatus">The employment status of the employee.</param>
    /// <param name="email">The work email address of the employee.</param>
    /// <returns>A JSON string representing the employee document.</returns>
    public static string CreateEmployee(string id, string groupId, long version, bool isActive, bool isDeleted, string employmentStatus, string email)
    {
        return JsonSerializer.Serialize(
            new
            {
                _id = id,
                isActive,
                isDeleted,
                createdDate = "2026-03-27T13:44:05.263Z",
                modifiedDate = "2026-03-27T13:44:05.263Z",
                version,
                mappingFields = new
                {
                    EmployeeId = new
                    {
                        groupId
                    }
                },
                employmentData = new
                {
                    employmentStatus,
                    originalHireDate = "2025-11-24T00:00:00.000Z",
                    lastHireDate = "2025-11-24T00:00:00.000Z",
                    lastJobPositionChangeDate = "2025-11-24T00:00:00.000Z",
                    expiredContractDate = (string?)null
                },
                employeeContact = new
                {
                    work = new
                    {
                        email,
                        mobile = string.Empty
                    }
                }
            });
    }
    #endregion

    #endregion

    #endregion
}