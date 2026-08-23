using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka.Shared.Models.Employees;

public sealed class WorkContact
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

    #endregion

    #endregion
}