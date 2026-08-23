using System;
using System.Collections.Generic;
using System.Text;

namespace kafka.Shared.Configuration;

public sealed class KafkaOptions
{
    #region Properties

    #region Public

    #region SectionName
    /// <summary>
    /// Gets the name of the configuration section for Kafka options.
    /// </summary>
    public const string SectionName = "Kafka";
    #endregion

    #region BootstrapServers
    /// <summary>
    /// Gets or sets the Kafka bootstrap servers used for connecting to the Kafka cluster.
    /// </summary>
    public string BootstrapServers { get; init; } = string.Empty;
    #endregion

    #region GroupId
    /// <summary>
    /// Gets or sets the Kafka consumer group ID.
    /// </summary>
    public string? GroupId { get; init; }
    #endregion

    #region Topic
    /// <summary>
    /// Gets or sets the Kafka topic to consume messages from.
    /// </summary>
    public string? Topic { get; init; }
    #endregion

    #region DeadLetterTopic
    /// <summary>
    /// Gets or sets the Kafka dead letter topic to which failed messages will be sent.
    /// </summary>
    public string? DeadLetterTopic { get; init; }
    #endregion

    #endregion

    #endregion
}