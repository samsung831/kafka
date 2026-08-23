using System;
using System.Collections.Generic;
using System.Text;
using kafka.IntegrationTests.Infrastructure;
using kafka.Shared.Models.Accounts;
using kafka.Shared.Models.Common;
using kafka.Shared.MongoDB;
using MongoDB.Driver;

namespace kafka.IntegrationTests.Tests;

[Collection(IntegrationTestCollection.Name)]
public sealed class VersionedDocumentWriterTests
{
    #region Constructor
    public VersionedDocumentWriterTests(IntegrationTestFixture fixture)
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

    #region CreateAccount
    /// <summary>
    /// Creates a new account document with the specified id, version, and first name.
    /// </summary>
    /// <param name="id">The unique identifier of the account.</param>
    /// <param name="version">The version number of the account document.</param>
    /// <param name="firstName">The first name of the account holder.</param>
    /// <returns>A new instance of <see cref="AccountDocument"/>.</returns>
    private static AccountDocument CreateAccount(string id, long version, string firstName)
    {
        return new AccountDocument
        {
            Id = id,
            Version = version,
            IsActive = true,
            IsDeleted = false,
            MappingFields = new MappingFields
            {
                EmployeeId = new EmployeeIdentifier
                {
                    GroupId = "VERSION-WRITER-GROUP"
                }
            },
            PersonalData = new PersonalData
            {
                FirstName = firstName,
                LastName = "Integration"
            },
            Names = new Dictionary<string, object>()
        };
    }
    #endregion

    #endregion

    #region Public

    #region UpsertAsync_InsertUpdateIgnore_BehavesAtomically
    /// <summary>
    /// Tests the atomic behavior of the UpsertAsync method in the VersionedDocumentWriter class.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task UpsertAsync_InsertUpdateIgnore_BehavesAtomically()
    {
        var collectionName = $"version_test_{Guid.NewGuid():N}";

        var collection = _fixture.Database.GetCollection<AccountDocument>(collectionName);

        var writer = new VersionedDocumentWriter<AccountDocument>(collection);

        const string accountId = "b4c3e0f5d1f4c2a1b2c3d4ea";

        //Insert version 1.
        var versionOne = CreateAccount(accountId, version: 1, firstName: "VersionOne");

        var insertResult = await writer.UpsertAsync(versionOne, CancellationToken.None);

        Assert.Equal(VersionedWriteResult.Inserted, insertResult);

        //Update with version 2.
        var versionTwo = CreateAccount(accountId, version: 2, firstName: "VersionTwo");

        var updateResult =  await writer.UpsertAsync(versionTwo, CancellationToken.None);

        Assert.Equal(VersionedWriteResult.Updated, updateResult);

        //Ignore older version 1.
        var olderVersion = CreateAccount(accountId, version: 1, firstName: "MustNotOverwrite");

        var ignoreResult = await writer.UpsertAsync(olderVersion, CancellationToken.None);

        Assert.Equal(VersionedWriteResult.Ignored, ignoreResult);

        //Ignore duplicate version 2.
        var duplicateVersion = CreateAccount(accountId, version: 2, firstName: "MustAlsoNotOverwrite");

        var duplicateResult = await writer.UpsertAsync(duplicateVersion, CancellationToken.None);

        Assert.Equal(VersionedWriteResult.Ignored, duplicateResult);

        var stored = await collection.Find(document => document.Id == accountId).SingleAsync();

        Assert.Equal(2, stored.Version);
        Assert.Equal("VersionTwo", stored.PersonalData.FirstName);

        var count = await collection.CountDocumentsAsync(document => document.Id == accountId);

        Assert.Equal(1, count);
    }
    #endregion

    #region UpsertAsync_ConcurrentVersions_PreservesHighestVersion
    /// <summary>
    /// Tests the UpsertAsync method of the VersionedDocumentWriter class to ensure that when multiple versions of a document are inserted concurrently,
    /// the highest version is preserved in the database.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task UpsertAsync_ConcurrentVersions_PreservesHighestVersion()
    {
        var collectionName = $"concurrency_test_{Guid.NewGuid():N}";

        var collection = _fixture.Database.GetCollection<AccountDocument>(collectionName);

        var writer = new VersionedDocumentWriter<AccountDocument>(collection);

        const string accountId = "c4c3e0f5d1f4c2a1b2c3d4eb";

        var versions =Enumerable.Range(1, 20)
                .OrderBy(_ => Guid.NewGuid()).Select(version => CreateAccount(accountId, version, $"Version{version}")).ToArray();

        await Task.WhenAll(versions.Select(document => writer.UpsertAsync(document, CancellationToken.None)));

        var stored = await collection.Find(document => document.Id == accountId).SingleAsync();

        Assert.Equal(20, stored.Version);
        Assert.Equal("Version20", stored.PersonalData.FirstName);

        var count = await collection.CountDocumentsAsync(document => document.Id == accountId);

        Assert.Equal(1, count);
    }
    #endregion

    #endregion

    #endregion
}