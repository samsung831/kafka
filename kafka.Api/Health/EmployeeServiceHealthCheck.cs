using kafka.Api.Configuration;
using Microsoft.Extensions.Options;

namespace kafka.Api.Health;

public sealed class EmployeeServiceHealthCheck : WorkerServiceHealthCheckBase
{
    #region Constructor
    public EmployeeServiceHealthCheck(IHttpClientFactory httpClientFactory, IOptions<WorkerServicesOptions> options) 
        : base(httpClientFactory.CreateClient("worker-health"), options.Value.EmployeeService.HealthUrl, "EmployeeService")
    {

    }
    #endregion
}