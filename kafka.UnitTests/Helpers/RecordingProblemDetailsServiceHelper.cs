using Microsoft.AspNetCore.Http;
using System;
using System.Collections.Generic;
using System.Text;

namespace kafka.UnitTests.Helpers;

public sealed class RecordingProblemDetailsServiceHelper : IProblemDetailsService
{
    #region Properties

    #region Public

    #region Context
    public ProblemDetailsContext? Context { get; private set; }
    #endregion

    #endregion

    #endregion

    #region Methods

    #region Public

    #region TryWriteAsync
    /// <summary>
    /// Attempts to write problem details to the response asynchronously.
    /// </summary>
    /// <param name="context">The problem details context.</param>
    /// <returns>A value task representing the asynchronous operation, containing a boolean indicating whether the write was successful.</returns>
    public ValueTask<bool> TryWriteAsync(ProblemDetailsContext context)
    {
        Context = context;
        return ValueTask.FromResult(true);
    }
    #endregion

    #region WriteAsync
    /// <summary>
    /// Writes problem details to the response asynchronously.
    /// </summary>
    /// <param name="context">The problem details context.</param>
    /// <returns>A value task representing the asynchronous operation.</returns>
    public ValueTask WriteAsync(ProblemDetailsContext context)
    {
        Context = context;
        return ValueTask.CompletedTask;
    }
    #endregion

    #endregion

    #endregion
}