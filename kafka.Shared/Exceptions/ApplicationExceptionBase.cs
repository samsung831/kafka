using System;
using System.Collections.Generic;
using System.Text;

namespace kafka.Shared.Exceptions;

public abstract class ApplicationExceptionBase : Exception
{
    #region Constructor
    protected ApplicationExceptionBase(string message, string errorCode) : base(message)
    {
        ErrorCode = errorCode;
    }

    protected ApplicationExceptionBase(string message, string errorCode, Exception innerException) : base(message, innerException)
    {
        ErrorCode = errorCode;
    }
    #endregion

    #region Properties

    #region Public

    #region ErrorCode
    /// <summary>
    /// Gets the error code associated with the exception.
    /// </summary>
    public string ErrorCode { get; }
    #endregion

    #endregion

    #endregion
}

