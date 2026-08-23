using kafka.Shared.Health;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka.Shared.Consumer;

public class WorkerHealthCheckBase : IHealthCheck
{
    #region Constructor
    public WorkerHealthCheckBase(string consumerName, WorkerHealthState workerHealthState)
    {
        _consumerName = consumerName;
        _workerHealthState = workerHealthState;
    }
    #endregion

    #region Properties

    #region Private
    private readonly string _consumerName;
    private readonly WorkerHealthState _workerHealthState;
    #endregion

    #endregion

    #region Methods

    #region Public

    #region CheckHealthAsync
    /// <summary>
    /// Checks the health of the worker service by evaluating its running state.
    /// </summary>
    /// <param name="context">The health check context.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous operation, containing the health check result.</returns>
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        var data = _workerHealthState.CreateHealthData();

        if (!_workerHealthState.IsRunning)
        {
            return Task.FromResult(HealthCheckResult.Unhealthy($"{_consumerName} consumer worker is not running.", data: data));
        }

        return Task.FromResult(HealthCheckResult.Healthy($"{_consumerName} consumer worker is running.", data: data));
    }
    #endregion

    #endregion

    #endregion
}
