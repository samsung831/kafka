using System;
using System.Collections.Generic;
using System.Text;

using kafka.Shared.Exceptions;

namespace kafka.Api.Exceptions;

public sealed class KafkaPublishException : ApplicationExceptionBase
{
    #region Constructor
    public KafkaPublishException(string message, Exception innerException) : base(message, DefaultErrorCode, innerException)
    {

    }
    #endregion

    #region Properties

    #region Public
    /// <summary>
    /// Gets the default error code for Kafka publish exceptions.
    /// </summary>
    public const string DefaultErrorCode = "kafka.publish_failed";
    #endregion

    #endregion
}
