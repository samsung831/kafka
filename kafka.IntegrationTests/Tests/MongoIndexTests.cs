using System;
using System.Collections.Generic;
using System.Text;
using kafka.IntegrationTests.Infrastructure;
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

    #region PersonsApiStartup_CreatesRequiredAccountIndexes
    /// <summary>
    /// Tests that the required indexes for the Accounts collection are created during the startup of the Persons API.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task PersonsApiStartup_CreatesRequiredAccountIndexes()
    {
        using var cursor = await _fixture.MongoContext.Accounts.Indexes.ListAsync();

        var indexes = await cursor.ToListAsync();

        var names = indexes.Select(index => index["name"].AsString).ToArray();

        Assert.Contains("ix_accounts_groupId_status", names);

        Assert.Contains("ix_accounts_name_status", names);
    }
    #endregion

    #region PersonsApiStartup_CreatesRequiredEmployeeIndexes
    /// <summary>
    /// Tests that the required indexes for the Employees collection are created during the startup of the Persons API.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task PersonsApiStartup_CreatesRequiredEmployeeIndexes()
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
