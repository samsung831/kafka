using MongoDB.Bson.Serialization.Attributes;

namespace kafka.Shared.Models.Accounts;

public sealed class PersonalData
{
    #region Properties

    #region Public

    #region Age
    [BsonElement("age")]
    public int? Age { get; set; }
    #endregion

    #region BirthDate
    [BsonElement("birthDate")]
    [BsonDateTimeOptions(Kind = DateTimeKind.Utc)]
    public DateTime? BirthDate { get; set; }
    #endregion

    #region FirstName
    [BsonElement("firstName")]
    public string FirstName { get; set; } = string.Empty;
    #endregion

    #region LastName
    [BsonElement("lastName")]
    public string LastName { get; set; } = string.Empty;
    #endregion

    #region Gender
    [BsonElement("gender")]
    public string? Gender { get; set; }
    #endregion

    #endregion

    #endregion
}