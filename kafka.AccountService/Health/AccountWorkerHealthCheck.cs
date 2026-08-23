using System;
using System.Collections.Generic;
using System.Text;
using kafka.Shared.Consumer;
using kafka.Shared.Health;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace kafka.AccountService.Health;

public sealed class AccountWorkerHealthCheck : WorkerHealthCheckBase
{
    #region Constructor
    public AccountWorkerHealthCheck(WorkerHealthState workerHealthState) : base("Account", workerHealthState)
    {

    }
    #endregion
}