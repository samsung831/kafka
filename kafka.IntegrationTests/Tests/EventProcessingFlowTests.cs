using System;
using System.Collections.Generic;
using System.Text;
using System.Net;
using System.Text.Json;
using kafka.IntegrationTests.Infrastructure;
using kafka.IntegrationTests.TestData;
using kafka.Shared.Models.Accounts;
using kafka.Shared.Models.Responses;
using MongoDB.Driver;

namespace kafka.IntegrationTests.Tests;

[Collection(IntegrationTestCollection.Name)]
public sealed class EventProcessingFlowTests
{
    #region Constructor
    public EventProcessingFlowTests(IntegrationTestFixture fixture)
    {
        _fixture = fixture;
    }
    #endregion

    #region Properties

    #region Private
    private readonly IntegrationTestFixture _fixture;
    private static readonly TimeSpan ProcessingTimeout = TimeSpan.FromSeconds(30);
    #endregion

    #endregion

    #region Methods

    #region Private

    #region PostJsonAsync
    /// <summary>
    /// Sends a POST request with JSON content to the specified path and includes a correlation ID in the headers.
    /// </summary>
    /// <param name="path">The path to which the POST request is sent.</param>
    /// <param name="json">The JSON content to include in the request body.</param>
    /// <param name="correlationId">The correlation ID to include in the request headers.</param>
    /// <returns>The HTTP response message.</returns>
    private async Task<HttpResponseMessage> PostJsonAsync(string path, string json, string correlationId)
    {
        using var request = new HttpRequestMessage(HttpMethod.Post, path)
        {
            Content = new StringContent(json, Encoding.UTF8, "application/json")
        };

        request.Headers.Add("X-Correlation-ID", correlationId);

        return await _fixture.KafkaApiClient.SendAsync(request);
    }
    #endregion

    #region WaitForAccountVersionAsync
    /// <summary>
    /// Waits until the account with the specified accountId reaches the expected version in the database.
    /// </summary>
    /// <param name="accountId">The unique identifier of the account.</param>
    /// <param name="expectedVersion">The expected version number of the account document.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private async Task WaitForAccountVersionAsync(string accountId, long expectedVersion)
    {
        await AsyncWait.UntilAsync(async cancellationToken =>
        {
            var account = await FindAccountAsync(accountId, cancellationToken);
            return account?.Version == expectedVersion;
        }, ProcessingTimeout);
    }
    #endregion

    #region WaitForEmployeeVersionAsync
    /// <summary>
    /// Waits until the employee with the specified employeeId reaches the expected version in the database.
    /// </summary>
    /// <param name="employeeId">The unique identifier of the employee.</param>
    /// <param name="expectedVersion">The expected version number of the employee document.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private async Task WaitForEmployeeVersionAsync(string employeeId, long expectedVersion)
    {
        await AsyncWait.UntilAsync(async cancellationToken =>
        {
            var employee = await _fixture.MongoContext.Employees.Find(document => document.Id == employeeId).FirstOrDefaultAsync(cancellationToken);

            return employee?.Version == expectedVersion;
        }, ProcessingTimeout);
    }
    #endregion

    #region WaitForEmployeeCountAsync
    /// <summary>
    /// Waits until the number of employee documents with the specified groupId reaches the expected count in the database.
    /// </summary>
    /// <param name="groupId">The group ID associated with the employee documents.</param>
    /// <param name="expectedCount">The expected number of employee documents.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private async Task WaitForEmployeeCountAsync(string groupId, long expectedCount)
    {
        var filter = Builders<kafka.Shared.Models.Employees.EmployeeDocument>.Filter
                .Eq("mappingFields.EmployeeId.groupId", groupId);

        await AsyncWait.UntilAsync(async cancellationToken =>
        {
            var count = await _fixture.MongoContext.Employees.CountDocumentsAsync(filter, cancellationToken: cancellationToken);
            return count == expectedCount;
        }, ProcessingTimeout);
    }
    #endregion

    #region FindAccountAsync
    /// <summary>
    /// Finds an account document in the database by its accountId.
    /// </summary>
    /// <param name="accountId">The unique identifier of the account.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the account document if found; otherwise, null.</returns>
    private async Task<AccountDocument?> FindAccountAsync(string accountId, CancellationToken cancellationToken)
    {
        return await _fixture.MongoContext.Accounts.Find(account => account.Id == accountId).FirstOrDefaultAsync(cancellationToken);
    }
    #endregion

    #region AssertAccountRemainsUnchangedAsync
    /// <summary>
    /// Asserts that the account with the specified accountId remains unchanged (i.e., retains the expected version and first name) over a specified stability window.
    /// </summary>
    /// <param name="accountId">The unique identifier of the account.</param>
    /// <param name="expectedVersion">The expected version number of the account document.</param>
    /// <param name="expectedFirstName">The expected first name of the account holder.</param>
    /// <param name="stabilityWindow">The duration of the stability window.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private async Task AssertAccountRemainsUnchangedAsync(string accountId, long expectedVersion, string expectedFirstName, TimeSpan stabilityWindow)
    {
        var deadline = DateTime.UtcNow + stabilityWindow;

        while (DateTime.UtcNow < deadline)
        {
            var account = await FindAccountAsync(accountId, CancellationToken.None);
            Assert.NotNull(account);
            Assert.Equal(expectedVersion, account.Version);
            Assert.Equal(expectedFirstName, account.PersonalData.FirstName);
            await Task.Delay(TimeSpan.FromMilliseconds(200));
        }
    }
    #endregion

    #region AssertAccountCountRemainsAsync
    /// <summary>
    /// Asserts that the count of account documents with the specified accountId remains equal to the expected count over a specified stability window.
    /// </summary>
    /// <param name="accountId">The unique identifier of the account.</param>
    /// <param name="expectedCount">The expected number of account documents.</param>
    /// <param name="stabilityWindow">The duration of the stability window.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private async Task AssertAccountCountRemainsAsync(string accountId, long expectedCount, TimeSpan stabilityWindow)
    {
        var deadline = DateTime.UtcNow + stabilityWindow;

        while (DateTime.UtcNow < deadline)
        {
            var count = await _fixture.MongoContext.Accounts.CountDocumentsAsync(account => account.Id == accountId);
            Assert.Equal(expectedCount, count);
            await Task.Delay(TimeSpan.FromMilliseconds(200));
        }
    }
    #endregion

    #region AssertCorrelationHeader
    /// <summary>
    /// Asserts that the HTTP response contains the expected correlation ID in the "X-Correlation-ID" header.
    /// </summary>
    /// <param name="response">The HTTP response message.</param>
    /// <param name="expectedCorrelationId">The expected correlation ID.</param>
    private static void AssertCorrelationHeader(HttpResponseMessage response, string expectedCorrelationId)
    {
        var headerExists = response.Headers.TryGetValues("X-Correlation-ID", out var values);
        Assert.True(headerExists, "The HTTP response does not contain X-Correlation-ID.");
        Assert.Contains(expectedCorrelationId, values!);
    }
    #endregion

    #region DeserializeAsync
    /// <summary>
    /// Deserializes the HTTP response content to the specified type.
    /// </summary>
    /// <typeparam name="T">The type to which the response content should be deserialized.</typeparam>
    /// <param name="response">The HTTP response message.</param>
    /// <returns>A task that represents the asynchronous operation. The task result contains the deserialized object.</returns>
    private static async Task<T> DeserializeAsync<T>(HttpResponseMessage response)
    {
        var responseBody = await response.Content.ReadAsStringAsync();

        var result = JsonSerializer.Deserialize<T>(responseBody, JsonSerializerOptions.Web);

        if (result is null)
        {
            throw new Xunit.Sdk.XunitException($"The response could not be deserialized to {typeof(T).Name}. Response body: {responseBody}");
        }

        return result;
    }
    #endregion

    #endregion

    #region Public

    #region FullFlow_EmployeeBeforeAccount_ReturnsCombinedPerson
    /// <summary>
    /// Tests the full flow of processing employee events before an account event and verifies that the combined person data is returned correctly.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task FullFlow_EmployeeBeforeAccount_ReturnsCombinedPerson()
    {
        await _fixture.DeleteAllDataAsync();

        const string groupId = "INTEGRATION-GROUP-001";

        const string accountId = "64c3e0f5d1f4c2a1b2c3d4e5";

        const string activeEmployeeId = "74c3e0f5d1f4c2a1b2c3d4e6";

        const string historicalEmployeeId = "84c3e0f5d1f4c2a1b2c3d4e7";

        const string correlationId = "integration-flow-001";

        //Publish active employee before account.
        var activeEmployeeJson = EventJsonFactory.CreateEmployee(activeEmployeeId, groupId, version: 157, isActive: true, isDeleted: false,
                employmentStatus: "Working", email: "active@example.com");

        using var activeEmployeeResponse = await PostJsonAsync("/api/events/employees", activeEmployeeJson, correlationId);

        Assert.Equal(HttpStatusCode.Accepted, activeEmployeeResponse.StatusCode);

        AssertCorrelationHeader(activeEmployeeResponse, correlationId);

        await WaitForEmployeeVersionAsync(activeEmployeeId, expectedVersion: 157);

        var accountBeforePublishing = await FindAccountAsync(accountId, CancellationToken.None);

        Assert.Null(accountBeforePublishing);

        //Publish historical employee for same groupId.
        var historicalEmployeeJson = EventJsonFactory.CreateEmployee(historicalEmployeeId, groupId, version: 25, isActive: false, isDeleted: false,
                employmentStatus: "Ended", email: "historical@example.com");

        using var historicalEmployeeResponse = await PostJsonAsync("/api/events/employees", historicalEmployeeJson, correlationId);

        Assert.Equal(HttpStatusCode.Accepted, historicalEmployeeResponse.StatusCode);

        await WaitForEmployeeCountAsync(groupId, expectedCount: 2);

        //Publish account after both employee events.
        var accountJson = EventJsonFactory.CreateAccount(accountId, groupId, version: 48, firstName: "Testo", lastName: "Testic");

        using var accountResponse = await PostJsonAsync("/api/events/accounts", accountJson, correlationId);

        Assert.Equal(HttpStatusCode.Accepted, accountResponse.StatusCode);

        AssertCorrelationHeader(accountResponse, correlationId);

        await WaitForAccountVersionAsync(accountId, expectedVersion: 48);

        //Retrieve combined person through PersonsApi.
        using var personHttpResponse = await _fixture.KafkaApiClient.GetAsync($"/api/persons/{groupId}");

        Assert.Equal(HttpStatusCode.OK, personHttpResponse.StatusCode);

        var person = await DeserializeAsync<PersonResponseDto>(personHttpResponse);

        Assert.Equal(accountId, person.Account.Id);

        Assert.Equal(groupId, person.Account.GroupId);

        Assert.Equal(48, person.Account.Version);

        Assert.Equal("Testo", person.Account.PersonalData.FirstName);

        Assert.Equal("Testic", person.Account.PersonalData.LastName);

        Assert.Equal(2, person.Employees.Count);

        var activeEmployee = Assert.Single(person.Employees, employee => employee.Id == activeEmployeeId);

        Assert.True(activeEmployee.IsActive);
        Assert.False(activeEmployee.IsDeleted);
        Assert.Equal(157, activeEmployee.Version);

        Assert.Equal("Working", activeEmployee.EmploymentData.EmploymentStatus);

        var historicalEmployee = Assert.Single(person.Employees, employee => employee.Id == historicalEmployeeId);

        Assert.False(historicalEmployee.IsActive);
        Assert.False(historicalEmployee.IsDeleted);
        Assert.Equal(25, historicalEmployee.Version);

        Assert.Equal("Ended", historicalEmployee.EmploymentData.EmploymentStatus);

        //Verify the search endpoint.
        using var searchHttpResponse = await _fixture.KafkaApiClient.GetAsync("/api/persons/search?firstName=Testo&lastName=Testic");

        Assert.Equal(HttpStatusCode.OK, searchHttpResponse.StatusCode);

        var searchResults = await DeserializeAsync<List<PersonResponseDto>>(searchHttpResponse);

        var matchingPerson = Assert.Single(searchResults, result => result.Account.GroupId == groupId);

        Assert.Equal(accountId, matchingPerson.Account.Id);

        Assert.Equal(2, matchingPerson.Employees.Count);
    }
    #endregion

    #region AccountVersioning_NewerVersionUpdatesDocument
    /// <summary>
    /// Tests that when a newer version of an account event is processed, it updates the existing account document in the database.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task AccountVersioning_NewerVersionUpdatesDocument()
    {
        await _fixture.DeleteAllDataAsync();

        const string accountId = "94c3e0f5d1f4c2a1b2c3d4e8";

        const string groupId = "INTEGRATION-VERSION-001";

        var versionOneJson = EventJsonFactory.CreateAccount(accountId, groupId, version: 1, firstName: "VersionOne", lastName: "Test");

        using var versionOneResponse = await PostJsonAsync("/api/events/accounts", versionOneJson, "version-test-1");

        Assert.Equal(HttpStatusCode.Accepted, versionOneResponse.StatusCode);

        await WaitForAccountVersionAsync(accountId, 1);

        var versionTwoJson = EventJsonFactory.CreateAccount(accountId, groupId, version: 2, firstName: "VersionTwo", lastName: "Test");

        using var versionTwoResponse = await PostJsonAsync("/api/events/accounts", versionTwoJson, "version-test-2");

        Assert.Equal(HttpStatusCode.Accepted, versionTwoResponse.StatusCode);

        await AsyncWait.UntilAsync(async cancellationToken =>
            {
                var account = await FindAccountAsync(accountId, cancellationToken);

                return account is not null && account.Version == 2 && account.PersonalData.FirstName == "VersionTwo";
            }, ProcessingTimeout);

        var storedAccount = await FindAccountAsync(accountId, CancellationToken.None);

        Assert.NotNull(storedAccount);
        Assert.Equal(2, storedAccount.Version);

        Assert.Equal("VersionTwo", storedAccount.PersonalData.FirstName);
    }
    #endregion

    #region AccountVersioning_OlderEventDoesNotOverwriteNewerDocument
    /// <summary>
    /// Tests that when an older version of an account event is processed after a newer version,
    /// it does not overwrite the existing account document in the database.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task AccountVersioning_OlderEventDoesNotOverwriteNewerDocument()
    {
        await _fixture.DeleteAllDataAsync();

        const string accountId = "a4c3e0f5d1f4c2a1b2c3d4e9";

        const string groupId = "INTEGRATION-OUT-OF-ORDER-001";

        var newerJson = EventJsonFactory.CreateAccount(accountId, groupId, version: 10, firstName: "Newer", lastName: "Version");

        using var newerResponse = await PostJsonAsync("/api/events/accounts", newerJson, "newer-event");

        Assert.Equal(HttpStatusCode.Accepted, newerResponse.StatusCode);

        await WaitForAccountVersionAsync(accountId, expectedVersion: 10);

        var olderJson = EventJsonFactory.CreateAccount(accountId, groupId, version: 5, firstName: "Older", lastName: "Version");

        using var olderResponse = await PostJsonAsync("/api/events/accounts", olderJson, "older-event");

        Assert.Equal(HttpStatusCode.Accepted, olderResponse.StatusCode);

        await AssertAccountRemainsUnchangedAsync(accountId, expectedVersion: 10, expectedFirstName: "Newer", stabilityWindow: TimeSpan.FromSeconds(3));
    }
    #endregion

    #region AccountVersioning_DuplicateEventCreatesOneDocument
    /// <summary>
    /// Tests that when duplicate account events with the same version are processed, only one account document is created in the database.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task AccountVersioning_DuplicateEventCreatesOneDocument()
    {
        await _fixture.DeleteAllDataAsync();

        const string accountId = "b4c3e0f5d1f4c2a1b2c3d4ea";

        const string groupId = "INTEGRATION-DUPLICATE-001";

        var accountJson = EventJsonFactory.CreateAccount(accountId, groupId, version: 3, firstName: "Duplicate", lastName: "Test");

        using var firstResponse = await PostJsonAsync("/api/events/accounts", accountJson, "duplicate-event-1");

        using var secondResponse = await PostJsonAsync("/api/events/accounts", accountJson, "duplicate-event-2");

        Assert.Equal(HttpStatusCode.Accepted, firstResponse.StatusCode);

        Assert.Equal(HttpStatusCode.Accepted, secondResponse.StatusCode);

        await WaitForAccountVersionAsync(accountId, expectedVersion: 3);

        await AssertAccountCountRemainsAsync(accountId, expectedCount: 1, stabilityWindow: TimeSpan.FromSeconds(3));
    }
    #endregion

    #region PersonsApi_StatusFilterReturnsOnlyActiveEmployee
    /// <summary>
    /// Tests that the Persons API correctly filters and returns only active employees when the appropriate query parameters are provided.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task PersonsApi_StatusFilterReturnsOnlyActiveEmployee()
    {
        await _fixture.DeleteAllDataAsync();

        const string groupId = "INTEGRATION-FILTER-001";

        const string accountId = "c4c3e0f5d1f4c2a1b2c3d4eb";

        const string activeEmployeeId = "d4c3e0f5d1f4c2a1b2c3d4ec";

        const string historicalEmployeeId = "e4c3e0f5d1f4c2a1b2c3d4ed";

        using var accountResponse = await PostJsonAsync("/api/events/accounts", EventJsonFactory.CreateAccount(accountId, groupId,
                    version: 1, firstName: "Filter", lastName: "Test"), "filter-account");

        Assert.Equal(HttpStatusCode.Accepted, accountResponse.StatusCode);

        using var activeResponse = await PostJsonAsync("/api/events/employees", EventJsonFactory.CreateEmployee(
                    activeEmployeeId, groupId, version: 1, isActive: true, isDeleted: false, employmentStatus: "Working", email: "active@example.com"),
                    "filter-active");

        Assert.Equal(HttpStatusCode.Accepted, activeResponse.StatusCode);

        using var historicalResponse = await PostJsonAsync("/api/events/employees", EventJsonFactory.CreateEmployee(
                    historicalEmployeeId, groupId, version: 1, isActive: false, isDeleted: false, employmentStatus: "Ended", email: "historical@example.com"),
                    "filter-historical");

        Assert.Equal(HttpStatusCode.Accepted, historicalResponse.StatusCode);

        await WaitForAccountVersionAsync(accountId, expectedVersion: 1);

        await WaitForEmployeeCountAsync(groupId, expectedCount: 2);

        using var filteredResponse = await _fixture.KafkaApiClient.GetAsync($"/api/persons/{groupId}?isActive=true&isDeleted=false");

        Assert.Equal(HttpStatusCode.OK, filteredResponse.StatusCode);

        var person = await DeserializeAsync<PersonResponseDto>(filteredResponse);

        var returnedEmployee = Assert.Single(person.Employees);

        Assert.Equal(activeEmployeeId, returnedEmployee.Id);

        Assert.True(returnedEmployee.IsActive);
        Assert.False(returnedEmployee.IsDeleted);
    }
    #endregion

    #endregion

    #endregion
}
