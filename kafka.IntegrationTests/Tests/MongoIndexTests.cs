using System;
using System.Collections.Generic;
using System.Text;
using kafka.IntegrationTests.Infrastructure;
using kafka.Shared.Configuration;
using kafka.Shared.Constants;
using kafka.Shared.MongoDB;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;

namespace kafka.IntegrationTests.Tests;

[Collection(IntegrationTestCollection.Name)]
public sealed class MongoIndexTests
{
    #region Constructor
    public MongoIndexTests(IntegrationTestFixture fixture)
    {
        _fixture = fixture;
    }
    #endregion

    #region Properties

    #region Private
    private readonly IntegrationTestFixture _fixture;
    #endregion

    #endregion

    #region Methods

    #region Private

    #region CreateEmployee
    /// <summary>
    /// Creates an employee document with the specified ID, group ID, activity status, and employment status.
    /// </summary>
    /// <param name="id">The ID of the employee.</param>
    /// <param name="groupId">The group ID of the employee.</param>
    /// <param name="isActive">Whether the employment is active.</param>
    /// <param name="employmentStatus">The employment status.</param>
    /// <returns>The created employee document.</returns>
    private static kafka.Shared.Models.Employees.EmployeeDocument CreateEmployee(string id, string groupId, bool isActive, string employmentStatus)
    {
        return new kafka.Shared.Models.Employees.EmployeeDocument
        {
            Id = id,
            Version = 1,
            IsActive = isActive,
            IsDeleted = false,
            MappingFields = new kafka.Shared.Models.Common.MappingFields
            {
                EmployeeId = new kafka.Shared.Models.Common.EmployeeIdentifier
                {
                    GroupId = groupId
                }
            },
            EmploymentData = new kafka.Shared.Models.Employees.EmploymentData
            {
                EmploymentStatus = employmentStatus
            }
        };
    }
    #endregion

    #endregion

    #region Public

    #region AccountIndexes_CreatesRequiredIndexes
    /// <summary>
    /// Tests that account index initialization creates the required indexes for the Accounts collection.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task AccountIndexes_CreatesRequiredIndexes()
    {
        using var cursor = await _fixture.MongoContext.Accounts.Indexes.ListAsync();

        var indexes = await cursor.ToListAsync();

        var names = indexes.Select(index => index["name"].AsString).ToArray();

        Assert.Contains("ix_accounts_groupId_status", names);

        Assert.Contains("ix_accounts_name_status", names);
    }
    #endregion

    #region EmployeeIndexes_CreatesRequiredIndexes
    /// <summary>
    /// Tests that employee index initialization creates the required indexes for the Employees collection.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task EmployeeIndexes_CreatesRequiredIndexes()
    {
        using var cursor = await _fixture.MongoContext.Employees.Indexes.ListAsync();

        var indexes = await cursor.ToListAsync();

        var names = indexes.Select(index => index["name"].AsString).ToArray();

        Assert.Contains("ix_employees_group_id_status", names);

        Assert.Contains("ux_employees_one_active_per_group_id", names);

        var uniqueIndex = indexes.Single(index => index["name"].AsString == "ux_employees_one_active_per_group_id");

        Assert.True(uniqueIndex.TryGetValue("unique", out var uniqueValue));

        Assert.True(uniqueValue.AsBoolean);

        Assert.True(uniqueIndex.Contains("partialFilterExpression"));
    }
    #endregion

    #region InitializeIndexes_OnlyCreatesOwnedCollection
    /// <summary>
    /// Tests that the index initializer only creates the collection it is responsible for, either Accounts or Employees, based on the input parameter.
    /// </summary>
    /// <param name="initializeAccounts">Indicates whether to initialize account indexes (true) or employee indexes (false).</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task InitializeIndexes_OnlyCreatesOwnedCollection(bool initializeAccounts)
    {
        var databaseName = $"index_ownership_{Guid.NewGuid():N}";
        var context = new MongoContext(Options.Create(new MongoOptions
        {
            ConnectionString = _fixture.MongoConnectionString,
            DatabaseName = databaseName
        }));
        var initializer = new MongoIndexInitializer(context);

        try
        {
            if (initializeAccounts)
            {
                await initializer.CreateAccountIndexesAsync();
            }
            else
            {
                await initializer.CreateEmployeeIndexesAsync();
            }

            using var cursor = await context.Database.ListCollectionNamesAsync();
            var collections = await cursor.ToListAsync();
            var expectedCollection = initializeAccounts ? MongoCollectionsConstants.Accounts : MongoCollectionsConstants.Employees;

            Assert.Equal(expectedCollection, Assert.Single(collections));
        }
        finally
        {
            await context.Database.Client.DropDatabaseAsync(databaseName);
        }
    }
    #endregion

    #region EmployeeIndex_PreventsTwoActiveEmploymentsForSameGroupId
    /// <summary>
    /// Tests that the unique index on the Employees collection prevents inserting two active employments for the same group ID.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task EmployeeIndex_PreventsTwoActiveEmploymentsForSameGroupId()
    {
        const string groupId = "UNIQUE-ACTIVE-GROUP-001";

        var first = CreateEmployee("d4c3e0f5d1f4c2a1b2c3d4ec", groupId, isActive: true, employmentStatus: "Working");

        var second = CreateEmployee("e4c3e0f5d1f4c2a1b2c3d4ed", groupId, isActive: true, employmentStatus: "Working");

        await _fixture.MongoContext.Employees.InsertOneAsync(first);

        var exception = await Assert.ThrowsAsync<MongoWriteException>(() => _fixture.MongoContext.Employees.InsertOneAsync(second));

        Assert.Equal(ServerErrorCategory.DuplicateKey, exception.WriteError?.Category);
    }
    #endregion

    #region EmployeeIndex_AllowsMultipleHistoricalEmployments
    /// <summary>
    /// Tests that the Employees collection allows inserting multiple historical employments for the same group ID.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task EmployeeIndex_AllowsMultipleHistoricalEmployments()
    {
        var groupId = $"HISTORICAL-{Guid.NewGuid():N}";

        var first = CreateEmployee("f4c3e0f5d1f4c2a1b2c3d4ee", groupId, isActive: false, employmentStatus: "Expired");

        var second = CreateEmployee("14c3e0f5d1f4c2a1b2c3d4ef", groupId, isActive: false, employmentStatus: "Expired");

        await _fixture.MongoContext.Employees.InsertManyAsync(new[]
            {
                first,
                second
            });

        var groupIdFilter = Builders<kafka.Shared.Models.Employees.EmployeeDocument>
            .Filter
            .Eq("mappingFields.EmployeeId.groupId", groupId);

        var count = await _fixture.MongoContext.Employees.CountDocumentsAsync(groupIdFilter);

        Assert.Equal(2, count);
    }
    #endregion

    #endregion

    #endregion
}
