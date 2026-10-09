using System.Net;
using System.Text.Json;
using kafka.IntegrationTests.Infrastructure;
using kafka.IntegrationTests.TestData;
using kafka.Shared.Models.Accounts;
using kafka.Shared.Models.Employees;
using MongoDB.Bson;
using MongoDB.Driver;

namespace kafka.IntegrationTests.Tests;

[Collection(IntegrationTestCollection.Name)]
public sealed class PersonStatusFilterTests
{
    #region Constructor
    public PersonStatusFilterTests(IntegrationTestFixture fixture)
    {
        _fixture = fixture;
    }
    #endregion

    #region Properties

    #region Private
    private readonly IntegrationTestFixture _fixture;
    private static readonly (bool IsActive, bool IsDeleted)[] DocumentStatuses =
    [
        (true, false), (false, false), (true, true), (false, true)
    ];
    #endregion

    #endregion

    #region Methods

    #region Private

    #region CreateQuery
    /// <summary>
    /// Creates a query string based on the provided filter parameters for account and employment status.
    /// </summary>
    /// <param name="accountIsActive">The account active filter.</param>
    /// <param name="accountIsDeleted">The account deleted filter.</param>
    /// <param name="employmentIsActive">The employment active filter.</param>
    /// <param name="employmentIsDeleted">The employment deleted filter.</param>
    /// <returns>A query string representing the provided filter parameters.</returns>
    private static string CreateQuery(bool? accountIsActive, bool? accountIsDeleted,
        bool? employmentIsActive, bool? employmentIsDeleted)
    {
        var filters = new (string Name, bool? Value)[]
        {
            ("accountIsActive", accountIsActive),
            ("accountIsDeleted", accountIsDeleted),
            ("employmentIsActive", employmentIsActive),
            ("employmentIsDeleted", employmentIsDeleted)
        };

        return string.Join("&", filters.Where(filter => filter.Value.HasValue)
            .Select(filter => $"{filter.Name}={filter.Value!.Value.ToString().ToLowerInvariant()}"));
    }
    #endregion

    #region AssertPerson
    /// <summary>
    /// Asserts that the provided JSON representation of a person matches the expected account and employment status filters.
    /// </summary>
    /// <param name="person">The JSON representation of the person to assert.</param>
    /// <param name="accountIsActive">The expected account active status.</param>
    /// <param name="accountIsDeleted">The expected account deleted status.</param>
    /// <param name="employmentIsActive">The expected employment active status.</param>
    /// <param name="employmentIsDeleted">The expected employment deleted status.</param>
    private static void AssertPerson(JsonElement person, bool accountIsActive, bool accountIsDeleted,
        bool? employmentIsActive, bool? employmentIsDeleted)
    {
        var account = person.GetProperty("account");
        Assert.Equal(accountIsActive, account.GetProperty("isActive").GetBoolean());
        Assert.Equal(accountIsDeleted, account.GetProperty("isDeleted").GetBoolean());
        Assert.False(account.TryGetProperty("_id", out _));
        Assert.False(account.TryGetProperty("id", out _));

        var expectedStatuses = DocumentStatuses.Where(status =>
            (!employmentIsActive.HasValue || status.IsActive == employmentIsActive.Value)
            && status.IsDeleted == (employmentIsDeleted ?? false)).ToArray();
        var employees = person.GetProperty("employees").EnumerateArray().ToArray();
        Assert.Equal(expectedStatuses.Length, employees.Length);

        foreach (var status in expectedStatuses)
        {
            var employee = Assert.Single(employees.Where(employee =>
                employee.GetProperty("employmentData").GetProperty("employmentStatus").GetString()
                    == $"{status.IsActive}-{status.IsDeleted}"));
            Assert.Equal(status.IsActive, employee.GetProperty("isActive").GetBoolean());
            Assert.Equal(status.IsDeleted, employee.GetProperty("isDeleted").GetBoolean());
            Assert.False(employee.TryGetProperty("_id", out _));
            Assert.False(employee.TryGetProperty("id", out _));
        }
    }
    #endregion

    #endregion

    #region Public

    #region FilterCombinations
    /// <summary>
    /// Generates all combinations of filter parameters for account and employment status, including null values to represent omitted filters.
    /// </summary>
    /// <returns>An enumerable of object arrays, each containing a combination of filter parameters.</returns>
    public static IEnumerable<object?[]> FilterCombinations()
    {
        bool?[] values = [null, false, true];
        foreach (var accountIsActive in values)
        foreach (var accountIsDeleted in values)
        foreach (var employmentIsActive in values)
        foreach (var employmentIsDeleted in values)
        {
            yield return new object?[] { accountIsActive, accountIsDeleted, employmentIsActive, employmentIsDeleted };
        }
    }
    #endregion

    #region BothEndpoints_ApplyIndependentFiltersAndDefaults
    /// <summary>
    /// Tests that both the individual person endpoint and the search endpoint correctly apply independent filters for account and employment status, including default values when filters are omitted.
    /// </summary>
    /// <param name="accountIsActive">The expected active status for the account.</param>
    /// <param name="accountIsDeleted">The expected deleted status for the account.</param>
    /// <param name="employmentIsActive">The expected active status for the employment.</param>
    /// <param name="employmentIsDeleted">The expected deleted status for the employment.</param>
    /// <returns>A task that represents the asynchronous test operation.</returns>
    [Theory]
    [MemberData(nameof(FilterCombinations))]
    public async Task BothEndpoints_ApplyIndependentFiltersAndDefaults(bool? accountIsActive, bool? accountIsDeleted,
        bool? employmentIsActive, bool? employmentIsDeleted)
    {
        await _fixture.DeleteAllDataAsync();
        var firstName = $"Status{Guid.NewGuid():N}";
        var accounts = new List<AccountDocument>();
        var employees = new List<EmployeeDocument>();

        foreach (var status in DocumentStatuses)
        {
            var groupId = $"{firstName}-{status.IsActive}-{status.IsDeleted}";
            accounts.Add(JsonSerializer.Deserialize<AccountDocument>(EventJsonFactory.CreateAccount(
                ObjectId.GenerateNewId().ToString(), groupId, 1, firstName, "Filter",
                status.IsActive, status.IsDeleted), JsonSerializerOptions.Web)!);

            foreach (var employeeStatus in DocumentStatuses)
            {
                employees.Add(JsonSerializer.Deserialize<EmployeeDocument>(EventJsonFactory.CreateEmployee(
                    ObjectId.GenerateNewId().ToString(), groupId, 1, employeeStatus.IsActive, employeeStatus.IsDeleted,
                    $"{employeeStatus.IsActive}-{employeeStatus.IsDeleted}", "status@example.com"), JsonSerializerOptions.Web)!);
            }
        }

        await _fixture.MongoContext.Accounts.InsertManyAsync(accounts);
        await _fixture.MongoContext.Employees.InsertManyAsync(employees);
        var query = CreateQuery(accountIsActive, accountIsDeleted, employmentIsActive, employmentIsDeleted);
        var expectedAccounts = accounts.Where(account =>
            (!accountIsActive.HasValue || account.IsActive == accountIsActive.Value)
            && account.IsDeleted == (accountIsDeleted ?? false)).ToArray();

        foreach (var account in accounts)
        {
            var url = $"/api/persons/{account.GroupId}" + (query.Length > 0 ? $"?{query}" : string.Empty);
            using var response = await _fixture.KafkaApiClient.GetAsync(url);
            if (!expectedAccounts.Contains(account))
            {
                Assert.Equal(HttpStatusCode.NotFound, response.StatusCode);
                continue;
            }

            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            using var json = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
            AssertPerson(json.RootElement, account.IsActive, account.IsDeleted, employmentIsActive, employmentIsDeleted);
        }

        using var searchResponse = await _fixture.KafkaApiClient.GetAsync(
            $"/api/persons/search?firstName={firstName}&lastName=Filter" + (query.Length > 0 ? $"&{query}" : string.Empty));
        Assert.Equal(HttpStatusCode.OK, searchResponse.StatusCode);
        using var searchJson = JsonDocument.Parse(await searchResponse.Content.ReadAsStringAsync());
        var persons = searchJson.RootElement.EnumerateArray().ToArray();
        Assert.Equal(expectedAccounts.Length, persons.Length);
        foreach (var account in expectedAccounts)
        {
            var person = Assert.Single(persons.Where(person =>
                person.GetProperty("account").GetProperty("isActive").GetBoolean() == account.IsActive
                && person.GetProperty("account").GetProperty("isDeleted").GetBoolean() == account.IsDeleted));
            AssertPerson(person, account.IsActive, account.IsDeleted, employmentIsActive, employmentIsDeleted);
        }
    }
    #endregion

    #region BothEndpoints_ReturnMatchingAccountWithNoMatchingEmployments
    /// <summary>
    /// Tests that both the individual person endpoint and the search endpoint return a matching account with no matching employments when the account is inactive and the employment filter is set to active, regardless of whether there are historical employments present.
    /// </summary>
    /// <param name="hasHistoricalEmployment">Indicates whether there is a historical employment present.</param>
    /// <returns>A task that represents the asynchronous test operation.</returns>
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task BothEndpoints_ReturnMatchingAccountWithNoMatchingEmployments(bool hasHistoricalEmployment)
    {
        await _fixture.DeleteAllDataAsync();
        var groupId = $"Empty{Guid.NewGuid():N}";
        var account = JsonSerializer.Deserialize<AccountDocument>(EventJsonFactory.CreateAccount(
            ObjectId.GenerateNewId().ToString(), groupId, 1, groupId, "Filter", isActive: false), JsonSerializerOptions.Web)!;
        await _fixture.MongoContext.Accounts.InsertOneAsync(account);
        if (hasHistoricalEmployment)
        {
            var employee = JsonSerializer.Deserialize<EmployeeDocument>(EventJsonFactory.CreateEmployee(
                ObjectId.GenerateNewId().ToString(), groupId, 1, false, false, "Ended", "ended@example.com"), JsonSerializerOptions.Web)!;
            await _fixture.MongoContext.Employees.InsertOneAsync(employee);
        }

        foreach (var url in new[]
        {
            $"/api/persons/{groupId}?employmentIsActive=true",
            $"/api/persons/search?firstName={groupId}&lastName=Filter&employmentIsActive=true"
        })
        {
            using var response = await _fixture.KafkaApiClient.GetAsync(url);
            Assert.Equal(HttpStatusCode.OK, response.StatusCode);
            using var json = JsonDocument.Parse(await response.Content.ReadAsStringAsync());
            var person = json.RootElement.ValueKind == JsonValueKind.Array
                ? Assert.Single(json.RootElement.EnumerateArray()) : json.RootElement;
            Assert.False(person.GetProperty("account").GetProperty("isActive").GetBoolean());
            Assert.False(person.GetProperty("account").GetProperty("isDeleted").GetBoolean());
            Assert.Empty(person.GetProperty("employees").EnumerateArray());
        }
    }
    #endregion

    #region BothEndpoints_RejectInvalidStatusValues
    /// <summary>
    /// Tests that both the individual person endpoint and the search endpoint reject invalid status values for account and employment filters, returning a BadRequest response.
    /// </summary>
    /// <param name="parameter">The query parameter to test with an invalid value.</param>
    /// <returns>A task that represents the asynchronous test operation.</returns>
    [Theory]
    [InlineData("accountIsActive")]
    [InlineData("accountIsDeleted")]
    [InlineData("employmentIsActive")]
    [InlineData("employmentIsDeleted")]
    public async Task BothEndpoints_RejectInvalidStatusValues(string parameter)
    {
        foreach (var url in new[]
        {
            $"/api/persons/INVALID-STATUS?{parameter}=invalid",
            $"/api/persons/search?firstName=Test&lastName=Filter&{parameter}=invalid"
        })
        {
            using var response = await _fixture.KafkaApiClient.GetAsync(url);
            Assert.Equal(HttpStatusCode.BadRequest, response.StatusCode);
        }
    }
    #endregion

    #endregion

    #endregion
}
