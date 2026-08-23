using System;
using System.Collections.Generic;
using System.Text;
using kafka.Shared.Consumer;
using kafka.Shared.Health;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace kafka.EmployeeService.Health;

public sealed class EmployeeWorkerHealthCheck : WorkerHealthCheckBase
{
    #region Constructor
    public EmployeeWorkerHealthCheck(WorkerHealthState workerHealthState) : base("Employee", workerHealthState)
    {

    }
    #endregion
}