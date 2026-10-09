using System.Reflection;
using System.Text.Json;
using kafka.Api.Services;
using kafka.Shared.Models.Accounts;
using kafka.Shared.Models.Employees;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;

namespace kafka.UnitTests.Services;

public sealed class PersonServiceStatusTests
{
    #region Methods

    #region Private

    #region RenderStatusFilter
    /// <summary>
    /// Renders the status filter for a given document type based on the provided isActive and isDeleted values.
    /// </summary>
    /// <typeparam name="TDocument">The type of the document.</typeparam>
    /// <param name="isActive">The active status filter.</param>
    /// <param name="isDeleted">The deleted status filter.</param>
    /// <returns>A BsonDocument representing the status filter.</returns>
    private static BsonDocument RenderStatusFilter<TDocument>(bool? isActive, bool? isDeleted)
    {
        var filters = new List<FilterDefinition<TDocument>>();
        var method = typeof(PersonService).GetMethod("AddStatusFilters", BindingFlags.Static | BindingFlags.NonPublic)!;
        method.MakeGenericMethod(typeof(TDocument)).Invoke(null, new object?[] { filters, isActive, isDeleted });

        return Builders<TDocument>.Filter.And(filters).Render(new RenderArgs<TDocument>(
            BsonSerializer.LookupSerializer<TDocument>(), BsonSerializer.SerializerRegistry));
    }
    #endregion

    #region AssertMappedStatus
    /// <summary>
    /// Asserts that the mapped status of a document is correctly represented in the DTO, ensuring that the isActive and isDeleted properties are preserved and that no ID properties are exposed.
    /// </summary>
    /// <param name="methodName">The name of the method to invoke.</param>
    /// <param name="document">The document to map.</param>
    /// <param name="isActive">The expected active status.</param>
    /// <param name="isDeleted">The expected deleted status.</param>
    private static void AssertMappedStatus(string methodName, object document, bool isActive, bool isDeleted)
    {
        var method = typeof(PersonService).GetMethod(methodName, BindingFlags.Instance | BindingFlags.NonPublic)!;
        var dto = method.Invoke(new PersonService(null!), new[] { document })!;
        var json = JsonSerializer.SerializeToElement(dto, dto.GetType(), JsonSerializerOptions.Web);

        Assert.Equal(isActive, json.GetProperty("isActive").GetBoolean());
        Assert.Equal(isDeleted, json.GetProperty("isDeleted").GetBoolean());
        Assert.False(json.TryGetProperty("_id", out _));
        Assert.False(json.TryGetProperty("id", out _));
    }
    #endregion

    #endregion

    #region Public

    #region StatusFilters_ExcludeDeletedByDefaultAndRespectExplicitValues
    /// <summary>
    /// Tests that the status filters for documents exclude deleted documents by default and respect explicit values for isActive and isDeleted.
    /// </summary>
    /// <param name="isActive">The expected active status.</param>
    /// <param name="isDeleted">The expected deleted status.</param>
    [Theory]
    [InlineData(null, null)]
    [InlineData(true, null)]
    [InlineData(false, null)]
    [InlineData(null, false)]
    [InlineData(null, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    [InlineData(false, false)]
    [InlineData(false, true)]
    public void StatusFilters_ExcludeDeletedByDefaultAndRespectExplicitValues(bool? isActive, bool? isDeleted)
    {
        var expected = new BsonDocument("isDeleted", isDeleted ?? false);
        if (isActive.HasValue)
        {
            expected.Add("isActive", isActive.Value);
        }

        foreach (var filter in new[]
        {
            RenderStatusFilter<AccountDocument>(isActive, isDeleted),
            RenderStatusFilter<EmployeeDocument>(isActive, isDeleted)
        })
        {
            Assert.Equal(expected.ElementCount, filter.ElementCount);
            foreach (var element in expected)
            {
                Assert.Equal(element.Value, filter[element.Name]);
            }
        }
    }
    #endregion

    #region Mapping_PreservesDocumentStatusWithoutExposingIds
    /// <summary>
    /// Tests that the mapping of documents to DTOs preserves the isActive and isDeleted status without exposing any ID properties in the resulting JSON representation.
    /// </summary>
    /// <param name="isActive">The expected active status.</param>
    /// <param name="isDeleted">The expected deleted status.</param>
    [Theory]
    [InlineData(false, false)]
    [InlineData(false, true)]
    [InlineData(true, false)]
    [InlineData(true, true)]
    public void Mapping_PreservesDocumentStatusWithoutExposingIds(bool isActive, bool isDeleted)
    {
        AssertMappedStatus("MapAccount", new AccountDocument
        {
            Id = "64c3e0f5d1f4c2a1b2c3d4e5",
            IsActive = isActive,
            IsDeleted = isDeleted
        }, isActive, isDeleted);

        AssertMappedStatus("MapEmployee", new EmployeeDocument
        {
            Id = "64c3e0f5d1f4c2a1b2c3d4e6",
            IsActive = isActive,
            IsDeleted = isDeleted
        }, isActive, isDeleted);
    }
    #endregion

    #endregion

    #endregion
}
