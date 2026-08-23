using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka.Shared.Models.Accounts;

public sealed class PrivateContact
{
    #region Properties

    #region Public

    #region Email
    [BsonElement("email")]
    public string? Email { get; set; }
    #endregion

    #region Mobile
    [BsonElement("mobile")]
    public string? Mobile { get; set; }
    #endregion

    #region CountryCode
    [BsonElement("countryCode")]
    public string? CountryCode { get; set; }
    #endregion

    #region Country
    [BsonElement("country")]
    public string? Country { get; set; }
    #endregion

    #endregion

    #endregion
}
