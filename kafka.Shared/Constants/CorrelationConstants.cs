using System;
using System.Collections.Generic;
using System.Text;

namespace kafka.Shared.Constants;

public static class CorrelationConstants
{
    #region Properties

    #region Public
    public const string HttpHeaderName = "X-Correlation-ID";
    public const string KafkaHeaderName = "correlation-id";
    public const string HttpContextItemName = "CorrelationId";
    public const string LogPropertyName = "CorrelationId";
    public const int MaximumLength = 128;
    #endregion

    #endregion
}
