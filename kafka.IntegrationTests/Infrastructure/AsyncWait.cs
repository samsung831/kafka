using System;
using System.Collections.Generic;
using System.Text;

namespace kafka.IntegrationTests.Infrastructure;

public static class AsyncWait
{
    #region Methods

    #region Public

    #region UntilAsync
    /// <summary>
    /// Waits asynchronously until the specified condition is met or the timeout is reached.
    /// If the condition is not met within the timeout, a TimeoutException is thrown.
    /// </summary>
    /// <param name="condition">The condition to be evaluated asynchronously.</param>
    /// <param name="timeout">The maximum amount of time to wait for the condition to be met.</param>
    /// <param name="pollInterval">The interval at which to poll the condition.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous wait operation.</returns>
    /// <exception cref="TimeoutException">Thrown if the condition is not met within the specified timeout.</exception>
    public static async Task UntilAsync(Func<CancellationToken, Task<bool>> condition, TimeSpan timeout,
        TimeSpan? pollInterval = null, CancellationToken cancellationToken = default)
    {
        var interval = pollInterval ?? TimeSpan.FromMilliseconds(200);
        var deadline = DateTime.UtcNow + timeout;
        Exception? lastException = null;

        while (DateTime.UtcNow < deadline)
        {
            cancellationToken.ThrowIfCancellationRequested();
            try
            {
                if (await condition(cancellationToken))
                {
                    return;
                }

                lastException = null;
            }
            catch (Exception exception) when (exception is not OperationCanceledException)
            {
                lastException = exception;
            }

            await Task.Delay(interval, cancellationToken);
        }

        throw new TimeoutException("The expected integration-test condition was not reached within timeout.", lastException);
    }
    #endregion

    #endregion

    #endregion
}
