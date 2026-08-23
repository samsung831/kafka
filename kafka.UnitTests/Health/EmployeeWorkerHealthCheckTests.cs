using kafka.EmployeeService.Health;
using kafka.Shared.Health;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace kafka.UnitTests.Health;

public sealed class EmployeeWorkerHealthCheckTests
{
    #region Methods

    #region Public

    #region CheckHealthAsync_ReflectsEmployeeWorkerLifecycle
    /// <summary>
    /// Tests that the EmployeeWorkerHealthCheck correctly reflects the lifecycle of the employee worker, including its running state and health status.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task CheckHealthAsync_ReflectsEmployeeWorkerLifecycle()
    {
        var state = new WorkerHealthState();
        var healthCheck = new EmployeeWorkerHealthCheck(state);

        var beforeStart = await healthCheck.CheckHealthAsync(new HealthCheckContext());
        Assert.Equal(HealthStatus.Unhealthy, beforeStart.Status);
        Assert.Equal("Employee consumer worker is not running.", beforeStart.Description);

        state.MarkStarted();
        var whileRunning = await healthCheck.CheckHealthAsync(new HealthCheckContext());
        Assert.Equal(HealthStatus.Healthy, whileRunning.Status);
        Assert.Equal("Employee consumer worker is running.", whileRunning.Description);
        Assert.Equal(true, whileRunning.Data["isRunning"]);

        state.MarkStopped();
        var afterStop = await healthCheck.CheckHealthAsync(new HealthCheckContext());
        Assert.Equal(HealthStatus.Unhealthy, afterStop.Status);
        Assert.Equal(false, afterStop.Data["isRunning"]);
    }
    #endregion

    #region CheckHealthAsync_WhenCancelled_ThrowsOperationCanceledException
    /// <summary>
    /// Tests that the CheckHealthAsync method throws an OperationCanceledException when the provided cancellation token is canceled.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task CheckHealthAsync_WhenCancelled_ThrowsOperationCanceledException()
    {
        var healthCheck = new EmployeeWorkerHealthCheck(new WorkerHealthState());
        using var cancellationSource = new CancellationTokenSource();
        cancellationSource.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(() => healthCheck.CheckHealthAsync(new HealthCheckContext(), cancellationSource.Token));
    }
    #endregion

    #endregion

    #endregion
}
