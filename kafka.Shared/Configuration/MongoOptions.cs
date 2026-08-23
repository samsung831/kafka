using System;
using System.Collections.Generic;
using System.Text;

namespace kafka.Shared.Configuration;

public sealed class MongoOptions
{
    #region Properties

    #region Public

    #region SectionName
    /// <summary>
    /// Gets the name of the configuration section for MongoDB options.
    /// </summary>
    public const string SectionName = "Mongo";
    #endregion

    #region ConnectionString
    /// <summary>
    /// Gets or sets the connection string for connecting to the MongoDB database.
    /// </summary>
    public string ConnectionString { get; init; } = string.Empty;
    #endregion

    #region DatabaseName
    /// <summary>
    /// Gets or sets the name of the MongoDB database to be used.
    /// </summary>
    public string DatabaseName { get; init; } = string.Empty;
    #endregion

    #endregion

    #endregion
}