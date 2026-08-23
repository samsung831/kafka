using MongoDB.Bson.Serialization.Attributes;

namespace kafka.Shared.Models.Accounts;

public sealed class AccountEmployeeContact
{
    #region Properties

    #region Public

    #region PrivateContact
    [BsonElement("private")]
    public PrivateContact? Private { get; set; }
    #endregion

    #endregion

    #endregion
}