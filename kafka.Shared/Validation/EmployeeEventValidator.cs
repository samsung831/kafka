using System;
using System.Collections.Generic;
using System.Text;

using kafka.Shared.Models.Employees;
using MongoDB.Bson;

namespace kafka.Shared.Validation;

public static class EmployeeEventValidator
{
    #region Methods

    #region Public

    #region Validate
    /// <summary>
    /// Validates the given EmployeeDocument object.
    /// </summary>
    /// <param name="employee">The EmployeeDocument object to validate.</param>
    /// <exception cref="ArgumentException">Thrown when the employee is invalid.</exception>
    public static void Validate(EmployeeDocument employee)
    {
        ArgumentNullException.ThrowIfNull(employee);

        if (!ObjectId.TryParse(employee.Id, out _))
        {
            throw new ArgumentException("Employee _id must be a valid ObjectId.", nameof(employee));
        }

        if (employee.Version < 0)
        {
            throw new ArgumentException("Employee version cannot be negative.", nameof(employee));
        }

        if (employee.MappingFields is null || employee.MappingFields.EmployeeId is null || string.IsNullOrWhiteSpace(employee.GroupId))
        {
            throw new ArgumentException("mappingFields.EmployeeId.groupId is required.", nameof(employee));
        }

        if (employee.EmploymentData is null)
        {
            throw new ArgumentException("employmentData is required.", nameof(employee));
        }
    }
    #endregion

    #endregion

    #endregion
}
