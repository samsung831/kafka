using kafka.Shared.Configuration;
using kafka.Shared.Constants;
using kafka.Shared.Models.Accounts;
using kafka.Shared.Models.Employees;
using MongoDB.Driver;
using Microsoft.Extensions.Options;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka.Shared.MongoDB;

public sealed class MongoContext
{
    #region Constructor
    public MongoContext(IOptions<MongoOptions> options)
    {
        var mongoOptions = options.Value;

        var client = new MongoClient(mongoOptions.ConnectionString);
        Database = client.GetDatabase(mongoOptions.DatabaseName);

        Accounts = Database.GetCollection<AccountDocument>(MongoCollectionsConstants.Accounts);

        Employees = Database.GetCollection<EmployeeDocument>(MongoCollectionsConstants.Employees);
    }
    #endregion

    #region Properties

    #region Public

    #region Database
    /// <summary>
    /// Gets the MongoDB database instance.
    /// </summary>
    public IMongoDatabase Database { get; }
    #endregion

    #region Accounts
    /// <summary>
    /// Gets the MongoDB collection for account documents.
    /// </summary>
    public IMongoCollection<AccountDocument> Accounts { get; }
    #endregion

    #region Employees
    /// <summary>
    /// Gets the MongoDB collection for employee documents.
    /// </summary>
    public IMongoCollection<EmployeeDocument> Employees { get; }
    #endregion

    #endregion

    #endregion
}