using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka.UnitTests.Helpers;

public sealed class CallbackResponseFeatureHelper : IHttpResponseFeature
{
    #region Properties

    #region Private
    private readonly List<(Func<object, Task> Callback, object State)> _onStarting = [];
    #endregion

    #region Public

    #region StatusCode
    /// <summary>
    /// Gets or sets the HTTP status code of the response.
    /// </summary>
    public int StatusCode { get; set; } = StatusCodes.Status200OK;
    #endregion

    #region ReasonPhrase
    /// <summary>
    /// Gets or sets the reason phrase associated with the HTTP status code of the response.
    /// </summary>
    public string? ReasonPhrase { get; set; }
    #endregion

    #region HEaders
    /// <summary>
    /// Gets or sets the collection of HTTP headers for the response.
    /// </summary>
    public IHeaderDictionary Headers { get; set; } = new HeaderDictionary();
    #endregion

    #region Body
    /// <summary>
    /// Gets or sets the body stream of the response.
    /// </summary>
    public Stream Body { get; set; } = new MemoryStream();
    #endregion

    #region HasStarted
    /// <summary>
    /// Gets a value indicating whether the response has started being sent to the client.
    /// </summary>
    public bool HasStarted { get; private set; }
    #endregion

    #endregion

    #endregion

    #region Methods

    #region Public

    #region OnStarting
    /// <summary>
    /// Registers a callback to be invoked just before the response starts being sent to the client.
    /// </summary>
    /// <param name="callback">The callback to invoke.</param>
    /// <param name="state">The state to pass to the callback.</param>
    public void OnStarting(Func<object, Task> callback, object state)
    {
        _onStarting.Add((callback, state));
    }
    #endregion

    #region OnCompleted
    /// <summary>
    /// Registers a callback to be invoked when the response has completed.
    /// </summary>
    /// <param name="callback">The callback to invoke.</param>
    /// <param name="state">The state to pass to the callback.</param>
    public void OnCompleted(Func<object, Task> callback, object state)
    {

    }
    #endregion

    #region RunOnStartingAsync
    /// <summary>
    /// Invokes all registered OnStarting callbacks in reverse order.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    public async Task RunOnStartingAsync()
    {
        foreach (var (callback, state) in _onStarting.AsEnumerable().Reverse())
        {
            await callback(state);
        }

        HasStarted = true;
    }
    #endregion

    #endregion

    #endregion
}
