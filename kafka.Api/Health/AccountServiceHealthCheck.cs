using kafka.Api.Configuration;
using Microsoft.Extensions.Options;

namespace kafka.Api.Health;

public sealed class AccountServiceHealthCheck : WorkerServiceHealthCheckBase
{
    #region Constructor
    public AccountServiceHealthCheck(IHttpClientFactory httpClientFactory, IOptions<WorkerServicesOptions> options) 
        : base(httpClientFactory.CreateClient("worker-health"), options.Value.AccountService.HealthUrl, "AccountService")
    {

    }
    #endregion
}