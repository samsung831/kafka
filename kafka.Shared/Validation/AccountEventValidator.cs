using System;
using System.Collections.Generic;
using System.Text;

using kafka.Shared.Models.Accounts;
using MongoDB.Bson;

namespace kafka.Shared.Validation;

public static class AccountEventValidator
{
    #region Methods

    #region Public

    #region Validate
    /// <summary>
    /// Validates the given AccountDocument object.
    /// </summary>
    /// <param name="account">The AccountDocument object to validate.</param>
    /// <exception cref="ArgumentException">Thrown when the account is invalid.</exception>
    public static void Validate(AccountDocument account)
    {
        ArgumentNullException.ThrowIfNull(account);

        if (!ObjectId.TryParse(account.Id, out _))
        {
            throw new ArgumentException("Account _id must be a valid ObjectId.", nameof(account));
        }

        if (account.Version < 0)
        {
            throw new ArgumentException("Account version cannot be negative.", nameof(account));
        }

        if (account.MappingFields is null || account.MappingFields.EmployeeId is null || string.IsNullOrWhiteSpace(account.GroupId))
        {
            throw new ArgumentException("mappingFields.EmployeeId.groupId is required.", nameof(account));
        }

        if (account.PersonalData is null)
        {
            throw new ArgumentException("personalData is required.", nameof(account));
        }
    }
    #endregion

    #endregion

    #endregion
}
