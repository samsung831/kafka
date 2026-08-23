using System;
using System.Collections.Generic;
using System.Text;

namespace kafka.Shared.DeadLetter;

public sealed record DeadLetterMessage
{
    #region Properties

    #region Public

    #region SourceService
    /// <summary>
    /// Gets or sets the name of the service that produced the original message.
    /// </summary>
    public required string SourceService { get; init; }
    #endregion

    #region SourceTopic
    /// <summary>
    /// Gets or sets the name of the Kafka topic from which the original message was consumed.
    /// </summary>
    public required string SourceTopic { get; init; }
    #endregion

    #region SourcePartition
    /// <summary>
    /// Gets or sets the partition number of the Kafka topic from which the original message was consumed.
    /// </summary>
    public required int SourcePartition { get; init; }
    #endregion

    #region SourceOffset
    /// <summary>
    /// Gets or sets the offset of the original message within the Kafka topic partition.
    /// </summary>
    public required long SourceOffset { get; init; }
    #endregion

    #region SourceKey
    /// <summary>
    /// Gets or sets the key of the original message, if applicable. This property may be null if the original message did not have a key.
    /// </summary>
    public string? SourceKey { get; init; }
    #endregion

    #region OriginalPayload
    /// <summary>
    /// Gets or sets the original payload of the message that failed processing.
    /// This property contains the raw data of the message as it was received from the Kafka topic.
    /// </summary>
    public required string OriginalPayload { get; init; }
    #endregion

    #region CorrelationId
    /// <summary>
    /// Gets or sets the correlation ID associated with the original message.
    /// </summary>
    public required string CorrelationId { get; init; }
    #endregion

    #region FailureReason
    /// <summary>
    /// Gets or sets the reason for the failure that caused the message to be sent to the dead letter queue.
    /// </summary>
    public required string FailureReason { get; init; }
    #endregion

    #region ErrorType
    /// <summary>
    /// Gets or sets the type of error that occurred during the processing of the original message.
    /// </summary>
    public required string ErrorType { get; init; }
    #endregion

    #region RetryCount
    /// <summary>
    /// Gets or sets the number of times the original message was retried before being sent to the dead letter queue.
    /// </summary>
    public required int RetryCount { get; init; }
    #endregion

    #region FailedAtUtc
    /// <summary>
    /// Gets or sets the UTC timestamp indicating when the message failed processing and was sent to the dead letter queue.
    /// </summary>
    public required DateTimeOffset FailedAtUtc { get; init; }
    #endregion

    #endregion

    #endregion
}