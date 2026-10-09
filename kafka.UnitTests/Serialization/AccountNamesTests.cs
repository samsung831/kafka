using System.Reflection;
using System.Text.Json;
using System.Text.Json.Nodes;
using kafka.Api.Services;
using kafka.Shared.Models.Accounts;
using kafka.Shared.Models.Responses.Account;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;

namespace kafka.UnitTests.Serialization;

public sealed class AccountNamesTests
{
    #region Methods

    #region Private

    #region DeserializeAccount
    /// <summary>
    /// Deserialize an account document from JSON, optionally including a "names" field.
    /// </summary>
    /// <param name="namesJson">The JSON string representing the "names" field, or null to omit it.</param>
    /// <returns>The deserialized AccountDocument.</returns>
    private static AccountDocument DeserializeAccount(string? namesJson)
    {
        var payload = JsonNode.Parse("""
            {"_id":"64c3e0f5d1f4c2a1b2c3d4e5","version":0,"personalData":{}}
            """)!.AsObject();

        if (namesJson is not null)
        {
            payload["names"] = JsonNode.Parse(namesJson);
        }

        return JsonSerializer.Deserialize<AccountDocument>(payload.ToJsonString(), JsonSerializerOptions.Web)!;
    }
    #endregion

    #region MapAccount
    /// <summary>
    /// Map an AccountDocument to an AccountDto using the private MapAccount method from PersonService.
    /// </summary>
    /// <param name="account">The AccountDocument to map.</param>
    /// <returns>The mapped AccountDto.</returns>
    private static AccountDto MapAccount(AccountDocument account)
    {
        var method = typeof(PersonService).GetMethod("MapAccount", BindingFlags.Instance | BindingFlags.NonPublic)!;
        return (AccountDto)method.Invoke(new PersonService(null!), new object[] { account })!;
    }
    #endregion

    #endregion

    #region Public

    #region NestedNames_RoundTripThroughBsonAndResponse
    /// <summary>
    /// Tests that an account document with nested names can be serialized to BSON and deserialized back, preserving the structure and values.
    /// </summary>
    [Fact]
    public void NestedNames_RoundTripThroughBsonAndResponse()
    {
        const string namesJson = """
            {"display":"Željko","nested":{"preferred":true,"middle":null},"aliases":["Z",{"locale":"hr"},7],"count":42,"score":1.5}
            """;
        var account = DeserializeAccount(namesJson);

        var bson = account.ToBsonDocument();
        Assert.Equal(BsonType.Document, bson["names"].BsonType);
        Assert.True(bson["names"]["nested"]["preferred"].AsBoolean);

        var restored = BsonSerializer.Deserialize<AccountDocument>(bson.ToBson());
        var response = MapAccount(restored);
        var responseJson = JsonSerializer.SerializeToElement(response, JsonSerializerOptions.Web);

        Assert.True(JsonNode.DeepEquals(JsonNode.Parse(namesJson), JsonNode.Parse(responseJson.GetProperty("names").GetRawText())));

        var serializedAccount = JsonSerializer.SerializeToElement(restored, JsonSerializerOptions.Web);
        Assert.True(JsonNode.DeepEquals(JsonNode.Parse(namesJson), JsonNode.Parse(serializedAccount.GetProperty("names").GetRawText())));
    }
    #endregion

    #region OmittedNullOrEmptyNames_NormalizeToEmptyObject
    /// <summary>
    /// Tests that when the "names" field is omitted, null, or an empty object, it normalizes to an empty object in the AccountDocument and AccountDto.
    /// </summary>
    /// <param name="namesJson"></param>
    [Theory]
    [InlineData(null)]
    [InlineData("null")]
    [InlineData("{}")]
    public void OmittedNullOrEmptyNames_NormalizeToEmptyObject(string? namesJson)
    {
        var account = DeserializeAccount(namesJson);
        Assert.Empty(account.Names);

        var restored = BsonSerializer.Deserialize<AccountDocument>(account.ToBson());
        Assert.Empty(restored.Names);
        Assert.Equal("{}", MapAccount(restored).Names.GetRawText());
    }
    #endregion

    #region LegacyBsonNullNames_NormalizeToEmptyObject
    /// <summary>
    /// Tests that when the "names" field is explicitly set to BsonNull in a BSON document, it normalizes to an empty object in the AccountDocument and AccountDto.
    /// </summary>
    [Fact]
    public void LegacyBsonNullNames_NormalizeToEmptyObject()
    {
        var bson = DeserializeAccount("{}").ToBsonDocument();
        bson["names"] = BsonNull.Value;

        var restored = BsonSerializer.Deserialize<AccountDocument>(bson.ToBson());

        Assert.Empty(restored.Names);
        Assert.Equal("{}", MapAccount(restored).Names.GetRawText());
        Assert.Equal(BsonType.Document, restored.ToBsonDocument()["names"].BsonType);
    }
    #endregion

    #region InvalidNames_ThrowJsonException
    /// <summary>
    /// Tests that deserializing an account document with invalid "names" JSON values throws a JsonException.
    /// </summary>
    /// <param name="namesJson"></param>
    [Theory]
    [InlineData("[]")]
    [InlineData("\"text\"")]
    [InlineData("42")]
    [InlineData("true")]
    [InlineData("{\"identifier\":{\"$oid\":\"invalid\"}}")]
    public void InvalidNames_ThrowJsonException(string namesJson)
    {
        Assert.Throws<JsonException>(() => DeserializeAccount(namesJson));
    }
    #endregion

    #endregion

    #endregion
}
