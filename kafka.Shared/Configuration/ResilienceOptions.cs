using System;
using System.Collections.Generic;
using System.Text;

namespace kafka.Shared.Configuration;

public sealed class ResilienceOptions
{
    #region Properties

    #region Public

    #region SectionName
    /// <summary>
    /// Gets the name of the configuration section for resilience options.
    /// </summary>
    public const string SectionName = "Resilience";
    #endregion

    #region MaxRetryAttempts
    /// <summary>
    /// Gets or sets the maximum number of retry attempts for resilience operations.
    /// </summary>
    public int MaxRetryAttempts { get; set; } = 3;
    #endregion

    #region RetryDelayMilliseconds
    /// <summary>
    /// Gets or sets the delay in milliseconds between retry attempts for resilience operations.
    /// </summary>
    public int RetryDelayMilliseconds { get; set; } = 500;
    #endregion

    #endregion

    #endregion
}
