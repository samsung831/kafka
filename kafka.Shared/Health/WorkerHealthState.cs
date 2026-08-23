using System;
using System.Collections.Generic;
using System.Text;

namespace kafka.Shared.Health;

public sealed class WorkerHealthState
{
    #region Properties

    #region Private
    private readonly object _synchronization = new();
    private bool _isRunning;
    private DateTimeOffset? _startedAtUtc;
    private DateTimeOffset? _lastSuccessfulProcessingAtUtc;
    private DateTimeOffset? _lastErrorAtUtc;
    private string? _lastError;
    #endregion

    #region Public

    #region IsRunning
    /// <summary>
    /// Gets a value indicating whether the worker is currently running.
    /// </summary>
    public bool IsRunning
    {
        get
        {
            lock (_synchronization)
            {
                return _isRunning;
            }
        }
    }
    #endregion

    #region StartedAtUtc
    /// <summary>
    /// Gets the UTC timestamp when the worker was started, or null if it has not been started yet.
    /// </summary>
    public DateTimeOffset? StartedAtUtc
    {
        get
        {
            lock (_synchronization)
            {
                return _startedAtUtc;
            }
        }
    }
    #endregion

    #region LastSuccessfulProcessingAtUtc
    /// <summary>
    /// Gets the UTC timestamp of the last successful processing, or null if there has not been any successful processing yet.
    /// </summary>
    public DateTimeOffset? LastSuccessfulProcessingAtUtc
    {
        get
        {
            lock (_synchronization)
            {
                return _lastSuccessfulProcessingAtUtc;
            }
        }
    }
    #endregion

    #region LastErrorAtUtc
    /// <summary>
    /// Gets the UTC timestamp of the last error that occurred during processing, or null if there has not been any errors yet.
    /// </summary>
    public DateTimeOffset? LastErrorAtUtc
    {
        get
        {
            lock (_synchronization)
            {
                return _lastErrorAtUtc;
            }
        }
    }
    #endregion

    #region LastError
    /// <summary>
    /// Gets the last error message that occurred during processing, or null if there has not been any errors yet.
    /// </summary>
    public string? LastError
    {
        get
        {
            lock (_synchronization)
            {
                return _lastError;
            }
        }
    }
    #endregion

    #endregion

    #endregion

    #region Methods

    #region Public

    #region MarkStarted
    /// <summary>
    /// Marks the worker as started, setting the running state to true and recording the start time.
    /// </summary>
    public void MarkStarted()
    {
        lock (_synchronization)
        {
            _isRunning = true;
            _startedAtUtc = DateTimeOffset.UtcNow;
            _lastError = null;
            _lastErrorAtUtc = null;
        }
    }
    #endregion

    #region MarkProcessingSucceeded
    /// <summary>
    /// Marks the processing as succeeded, updating the last successful processing timestamp and clearing any previous error information.
    /// </summary>
    public void MarkProcessingSucceeded()
    {
        lock (_synchronization)
        {
            _lastSuccessfulProcessingAtUtc = DateTimeOffset.UtcNow;
            _lastError = null;
            _lastErrorAtUtc = null;
        }
    }
    #endregion

    #region MarkProcessingFailed
    /// <summary>
    /// Marks the processing as failed, updating the last error timestamp and recording the error message from the provided exception.
    /// </summary>
    /// <param name="exception"></param>
    public void MarkProcessingFailed(Exception exception)
    {
        ArgumentNullException.ThrowIfNull(exception);

        lock (_synchronization)
        {
            _lastErrorAtUtc = DateTimeOffset.UtcNow;
            _lastError = $"{exception.GetType().Name}: {exception.Message}";
        }
    }
    #endregion

    #region MarkStopped
    /// <summary>
    /// Marks the worker as stopped, setting the running state to false.
    /// </summary>
    public void MarkStopped()
    {
        lock (_synchronization)
        {
            _isRunning = false;
        }
    }
    #endregion

    #region CreateHealthData
    /// <summary>
    /// Creates a read-only dictionary containing the current health state of the worker, including running status,
    /// start time, last successful processing time, last error time, and last error message.
    /// </summary>
    /// <returns>A read-only dictionary containing the current health state of the worker.</returns>
    public IReadOnlyDictionary<string, object?> CreateHealthData()
    {
        lock (_synchronization)
        {
            return new Dictionary<string, object?>
            {
                ["isRunning"] =  _isRunning,
                ["startedAtUtc"] = _startedAtUtc,
                ["lastSuccessfulProcessingAtUtc"] = _lastSuccessfulProcessingAtUtc,
                ["lastErrorAtUtc"] = _lastErrorAtUtc,
                ["lastError"] = _lastError
            };
        }
    }
    #endregion

    #endregion

    #endregion
}