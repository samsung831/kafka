using kafka.AccountService.Health;
using kafka.Shared.Health;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace kafka.UnitTests.Health;

public sealed class AccountWorkerHealthCheckTests
{
    #region Methods

    #region Public

    #region CheckHealthAsync_TracksWorkerLifecycleAndDiagnosticData
    /// <summary>
    /// Tests that the AccountWorkerHealthCheck correctly tracks the worker's lifecycle and diagnostic data.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task CheckHealthAsync_TracksWorkerLifecycleAndDiagnosticData()
    {
        var state = new WorkerHealthState();
        var healthCheck = new AccountWorkerHealthCheck(state);

        var beforeStart = await healthCheck.CheckHealthAsync(new HealthCheckContext());
        Assert.Equal(HealthStatus.Unhealthy, beforeStart.Status);
        Assert.Equal(false, beforeStart.Data["isRunning"]);

        state.MarkStarted();
        state.MarkProcessingSucceeded();
        var whileRunning = await healthCheck.CheckHealthAsync(new HealthCheckContext());
        Assert.Equal(HealthStatus.Healthy, whileRunning.Status);
        Assert.Equal(true, whileRunning.Data["isRunning"]);
        Assert.NotNull(whileRunning.Data["startedAtUtc"]);
        Assert.NotNull(whileRunning.Data["lastSuccessfulProcessingAtUtc"]);

        state.MarkProcessingFailed(new InvalidOperationException("Database unavailable."));
        var afterFailure = await healthCheck.CheckHealthAsync(new HealthCheckContext());
        Assert.Equal(HealthStatus.Healthy, afterFailure.Status);
        Assert.Equal("InvalidOperationException: Database unavailable.", afterFailure.Data["lastError"]);

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
        var healthCheck = new AccountWorkerHealthCheck(new WorkerHealthState());
        using var cancellationSource = new CancellationTokenSource();
        cancellationSource.Cancel();

        await Assert.ThrowsAsync<OperationCanceledException>(() => healthCheck.CheckHealthAsync(new HealthCheckContext(), cancellationSource.Token));
    }
    #endregion

    #endregion

    #endregion
}
