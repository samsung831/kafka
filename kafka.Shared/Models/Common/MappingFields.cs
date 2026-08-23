using System;
using System.Collections.Generic;
using System.Text;
using MongoDB.Bson.Serialization.Attributes;

namespace kafka.Shared.Models.Common;

public sealed class MappingFields
{
    #region Properties

    #region Public

    #region EmployeeId
    [BsonElement("EmployeeId")]
    public EmployeeIdentifier EmployeeId { get; set; } = new();
    #endregion

    #endregion

    #endregion
}