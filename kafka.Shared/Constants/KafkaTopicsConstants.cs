using System;
using System.Collections.Generic;
using System.Text;

namespace kafka.Shared.Constants;

public static class KafkaTopicsConstants
{
    #region Properties

    #region Public

    #region Accounts
    /// <summary>
    /// Gets the Kafka topic name for accounts.
    /// </summary>
    public const string Accounts = "topic.accounts";
    #endregion

    #region Employees
    /// <summary>
    /// Gets the Kafka topic name for employees.
    /// </summary>
    public const string Employees = "topic.employees";
    #endregion

    #region AccountsDeadLetter
    /// <summary>
    /// Gets the Kafka topic name for the accounts dead letter queue.
    /// </summary>
    public const string AccountsDeadLetter = "topic.accounts.dlq";
    #endregion

    #region EmployeesDeadLetter
    /// <summary>
    /// Gets the Kafka topic name for the employees dead letter queue.
    /// </summary>
    public const string EmployeesDeadLetter = "topic.employees.dlq";
    #endregion

    #endregion

    #endregion
}
