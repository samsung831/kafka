using System;
using System.Collections.Generic;
using System.Text;

namespace kafka.Shared.Exceptions;

public sealed class ResourceNotFoundException : ApplicationExceptionBase
{
    #region Constructor
    public ResourceNotFoundException(string message) : base(message, DefaultErrorCode)
    {
    }
    #endregion

    #region Properties

    #region Public
    /// <summary>
    /// Gets the default error code for resource not found exceptions.
    /// </summary>
    public const string DefaultErrorCode = "resource.not_found";
    #endregion

    #endregion
}