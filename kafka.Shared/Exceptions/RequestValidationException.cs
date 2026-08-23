using System;
using System.Collections.Generic;
using System.Text;

namespace kafka.Shared.Exceptions;

public sealed class RequestValidationException : ApplicationExceptionBase
{
    #region Constructor
    public RequestValidationException(string message) : base(message, DefaultErrorCode)
    {

    }
    #endregion

    #region Properties

    #region Public
    /// <summary>
    /// Gets the default error code for request validation exceptions.
    /// </summary>
    public const string DefaultErrorCode = "request.validation_failed";
    #endregion

    #endregion
}