using kafka.Shared.Models.Accounts;
using MongoDB.Driver;
using kafka.Shared.Models.Employees;
using System;
using System.Collections.Generic;
using System.Reflection.Metadata;
using System.Text;
using System.Xml.Linq;

namespace kafka.Shared.MongoDB;

public sealed class MongoIndexInitializer
{
    #region Constructor
    public MongoIndexInitializer(MongoContext context)
    {
        _context = context;
    }
    #endregion

    #region Properties

    #region Private
    private readonly MongoContext _context;
    #endregion

    #endregion

    #region Methods

    #region Private

    #region CreateAccountIndexesAsync
    /// <summary>
    /// Creates indexes for the Accounts collection in MongoDB.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private async Task CreateAccountIndexesAsync(CancellationToken cancellationToken)
    {
        var groupIdAndStatus = new CreateIndexModel<AccountDocument>(
            Builders<AccountDocument>.IndexKeys
                .Ascending("mappingFields.EmployeeId.groupId")
                .Ascending(document => document.IsActive)
                .Ascending(document => document.IsDeleted),
            new CreateIndexOptions
            {
                Name = "ix_accounts_groupId_status"
            });

        var nameAndStatus = new CreateIndexModel<AccountDocument>(
            Builders<AccountDocument>.IndexKeys
                .Ascending("personalData.firstName")
                .Ascending("personalData.lastName")
                .Ascending(document => document.IsActive)
                .Ascending(document => document.IsDeleted),
            new CreateIndexOptions
            {
                Name = "ix_accounts_name_status"
            });

        await _context.Accounts.Indexes.CreateManyAsync(
            new[]
            {
                groupIdAndStatus,
                nameAndStatus
            },
            cancellationToken);
    }
    #endregion

    #region CreateEmployeeIndexesAsync
    /// <summary>
    /// Creates indexes for the Employees collection in MongoDB.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private async Task CreateEmployeeIndexesAsync(CancellationToken cancellationToken)
    {
        var groupIdAndStatus =
            new CreateIndexModel<EmployeeDocument>(
                Builders<EmployeeDocument>.IndexKeys
                    .Ascending("mappingFields.EmployeeId.groupId")
                    .Ascending(document => document.IsActive)
                    .Ascending(document => document.IsDeleted),
                new CreateIndexOptions
                {
                    Name = "ix_employees_group_id_status"
                });

        var oneActiveEmploymentPerPerson =
            new CreateIndexModel<EmployeeDocument>(
                Builders<EmployeeDocument>.IndexKeys
                    .Ascending("mappingFields.EmployeeId.groupId"),
                new CreateIndexOptions<EmployeeDocument>
                {
                    Name = "ux_employees_one_active_per_group_id",
                    Unique = true,
                    PartialFilterExpression =
                        Builders<EmployeeDocument>.Filter.And(
                            Builders<EmployeeDocument>.Filter.Eq(
                                document => document.IsActive,
                                true),
                            Builders<EmployeeDocument>.Filter.Eq(
                                document => document.IsDeleted,
                                false))
                });

        await _context.Employees.Indexes.CreateManyAsync(
            new[]
            {
                groupIdAndStatus,
                oneActiveEmploymentPerPerson
            },
            cancellationToken);
    }

    #endregion

    #endregion

    #region Public

    #region CreateIndexesAsync
    /// <summary>
    /// Creates indexes for both the Accounts and Employees collections in MongoDB.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    public async Task CreateIndexesAsync(CancellationToken cancellationToken = default)
    {
        await CreateAccountIndexesAsync(cancellationToken);
        await CreateEmployeeIndexesAsync(cancellationToken);
    }
    #endregion

    #endregion

    #endregion
}