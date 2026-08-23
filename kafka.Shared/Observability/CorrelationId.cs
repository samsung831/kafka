using kafka.Shared.Constants;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka.Shared.Observability;

public static class CorrelationId
{
    #region Methods

    #region Public

    #region Create
    /// <summary>
    /// Creates a new correlation ID as a string without hyphens.
    /// </summary>
    /// <returns>A new correlation ID string.</returns>
    public static string Create()
    {
        return Guid.NewGuid().ToString("N");
    }
    #endregion

    #region NormalizeOrCreate
    /// <summary>
    /// Normalizes the provided correlation ID or creates a new one if the input is null, empty, or exceeds the maximum length.
    /// </summary>
    /// <param name="value">The correlation ID to normalize.</param>
    /// <returns>A normalized correlation ID string.</returns>
    public static string NormalizeOrCreate(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return Create();
        }

        var normalizedValue = value.Trim();

        if (normalizedValue.Length > CorrelationConstants.MaximumLength)
        {
            return Create();
        }

        return normalizedValue;
    }
    #endregion

    #endregion

    #endregion
}