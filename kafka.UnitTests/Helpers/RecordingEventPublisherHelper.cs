using kafka.Api.Kafka;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;

namespace kafka.UnitTests.Helpers;

public sealed class RecordingEventPublisherHelper(PublishResult result) : IEventPublisher
{
    #region Properties

    #region Public

    #region Topic
    /// <summary>
    /// Gets the topic to which the event was published.
    /// </summary>
    public string Topic { get; private set; } = string.Empty;
    #endregion

    #region Payload
    /// <summary>
    /// Gets the payload of the event that was published.
    /// </summary>
    public JsonElement Payload { get; private set; }
    #endregion

    #region CorrelationId
    /// <summary>
    /// Gets the correlation ID associated with the published event.
    /// </summary>
    public string CorrelationId { get; private set; } = string.Empty;
    #endregion

    #region CancellationToken
    /// <summary>
    /// Gets the cancellation token that was passed to the PublishAsync method.
    /// </summary>
    public CancellationToken CancellationToken { get; private set; }
    #endregion

    #endregion

    #endregion

    #region Methods

    #region Public

    #region PublishAsync
    /// <summary>
    /// Publishes an event to the specified topic with the given payload and correlation ID.
    /// </summary>
    /// <param name="topic">The topic to which the event will be published.</param>
    /// <param name="payload">The payload of the event to be published.</param>
    /// <param name="correlationId">The correlation ID associated with the event.</param>
    /// <param name="cancellationToken">The cancellation token to observe while publishing the event.</param>
    /// <returns>A task that represents the asynchronous publish operation.</returns>
    public Task<PublishResult> PublishAsync(string topic, JsonElement payload, string correlationId, CancellationToken cancellationToken)
    {
        Topic = topic;
        Payload = payload;
        CorrelationId = correlationId;
        CancellationToken = cancellationToken;
        return Task.FromResult(result);
    }
    #endregion

    #endregion

    #endregion
}
