namespace kafka.Api.Configuration;

public sealed class WorkerServiceEndpointOptions
{
    #region Properties

    #region Public

    #region HealthUrl
    /// <summary>
    /// Gets or sets the health check URL for the worker service.
    /// </summary>
    public string HealthUrl { get; set; } = string.Empty;
    #endregion

    #endregion

    #endregion
}
