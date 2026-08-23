namespace kafka.Api.Responses;


public sealed record PublishEventResponse
{
    #region Properties

    #region Public

    #region Message
    /// <summary>
    /// Gets or sets the message that was published to the Kafka topic.
    /// </summary>
    public required string Message { get; init; }
    #endregion

    #region CorrelationId
    /// <summary>
    /// Gets or sets the correlation ID associated with the published message.
    /// </summary>
    public required string CorrelationId { get; init; }
    #endregion

    #region Topic
    /// <summary>
    /// Gets or sets the name of the Kafka topic to which the message was published.
    /// </summary>
    public required string Topic { get; init; }
    #endregion

    #region Partition
    /// <summary>
    /// Gets or sets the partition number of the Kafka topic where the message was published.
    /// </summary>
    public required int Partition { get; init; }
    #endregion

    #region Offset
    /// <summary>
    /// Gets or sets the offset of the published message within the Kafka topic partition.
    /// </summary>
    public required long Offset { get; init; }
    #endregion

    #endregion

    #endregion
}
