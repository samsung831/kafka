using MongoDB.Bson.Serialization.Attributes;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka.Shared.Models.Common;

public sealed class EmployeeIdentifier
{
    #region Properties

    #region Public

    #region GroupId
    [BsonElement("groupId")]
    public string GroupId { get; set; } = string.Empty;
    #endregion

    #endregion

    #endregion
}