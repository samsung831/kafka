using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace kafka.Api.Health;

public abstract class WorkerServiceHealthCheckBase : IHealthCheck
{
    #region Constructor
    protected WorkerServiceHealthCheckBase(HttpClient httpClient, string healthUrl, string serviceName)
    {
        _httpClient = httpClient;
        _healthUrl = healthUrl;
        _serviceName = serviceName;
    }
    #endregion

    #region Properties

    #region Private
    private readonly HttpClient _httpClient;
    private readonly string _healthUrl;
    private readonly string _serviceName;
    #endregion

    #endregion

    #region Methods

    #region Public

    #region CheckHealthAsync
    /// <summary>
    /// Checks the health of the worker service by sending an HTTP GET request to the configured health URL.
    /// </summary>
    /// <param name="context">The health check context.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation, containing the health check result.</returns>
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        if (string.IsNullOrWhiteSpace(_healthUrl))
        {
            return HealthCheckResult.Unhealthy($"{_serviceName} health URL is not configured.");
        }

        try
        {
            using var request = new HttpRequestMessage(HttpMethod.Get, _healthUrl);
                    
            using var response = await _httpClient.SendAsync(request, HttpCompletionOption.ResponseHeadersRead, cancellationToken);

            if (!response.IsSuccessStatusCode)
            {
                return HealthCheckResult.Unhealthy($"{_serviceName} reported an unhealthy status.", data:
                    new Dictionary<string, object>
                    {
                        ["service"] = _serviceName,
                        ["statusCode"] = (int)response.StatusCode
                    });
            }

            return HealthCheckResult.Healthy($"{_serviceName} is ready.", data:
                new Dictionary<string, object>
                {
                    ["service"] = _serviceName,
                    ["statusCode"] = (int)response.StatusCode
                });
        }
        catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
        {
            return HealthCheckResult.Unhealthy($"{_serviceName} health request timed out.");
        }
        catch (HttpRequestException exception)
        {
            return HealthCheckResult.Unhealthy($"{_serviceName} health endpoint is unavailable.", exception);
        }
    }
    #endregion

    #endregion

    #endregion
}