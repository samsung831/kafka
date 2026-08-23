using Confluent.Kafka;
using kafka.Api.Exceptions;
using kafka.Api.Kafka;
using kafka.Shared.Constants;
using kafka.Shared.Observability;
using Microsoft.AspNetCore.DataProtection.KeyManagement;
using Microsoft.AspNetCore.Mvc.RazorPages;
using System.Net.NetworkInformation;
using System.Security.Cryptography;
using System.Text;
using System.Text.Json;
using System.Xml.Linq;
using static Confluent.Kafka.ConfigPropertyNames;

namespace kafka.Api.Kafka;

public sealed class KafkaEventPublisher : IEventPublisher
{
    #region Constructor
    public KafkaEventPublisher(IProducer<string, string> producer, ILogger<KafkaEventPublisher> logger)
    {
        _producer = producer;
        _logger = logger;
    }
    #endregion

    #region Properties

    #region Private
    private readonly IProducer<string, string> _producer;
    private readonly ILogger<KafkaEventPublisher> _logger;
    #endregion

    #endregion

    #region Methods

    #region Private

    #region TryGetGroupId
    /// <summary>
    /// Tries to extract the groupId from the provided JSON payload.
    /// The method looks for the "mappingFields" property, then the "EmployeeId" property, and finally the "groupId" property within it. 
    /// If any of these properties are missing or if the "groupId" is not a string, the method returns null.
    /// </summary>
    /// <param name="payload">The JSON payload to extract the groupId from.</param>
    /// <returns>The extracted groupId if present; otherwise, null.</returns>
    private static string? TryGetGroupId(JsonElement payload)
    {
        if (!payload.TryGetProperty("mappingFields", out var mappingFields))
        {
            return null;
        }

        if (!mappingFields.TryGetProperty("EmployeeId", out var employeeId))
        {
            return null;
        }

        if (!employeeId.TryGetProperty("groupId", out var groupId))
        {
            return null;
        }

        return groupId.ValueKind == JsonValueKind.String ? groupId.GetString() : null;
    }
    #endregion

    #region ValidateTopic
    /// <summary>
    /// Validates the provided Kafka topic against a predefined list of supported topics.   
    /// </summary>
    /// <param name="topic">The Kafka topic to validate.</param>
    /// <exception cref="ArgumentException">Thrown if the topic is not supported.</exception>
    private static void ValidateTopic(string topic)
    {
        if (topic != KafkaTopicsConstants.Accounts && topic != KafkaTopicsConstants.Employees)
        {
            throw new ArgumentException($"Unsupported Kafka topic '{topic}'.", nameof(topic));
        }
    }
    #endregion

    #endregion

    #region Public

    #region PublishAsync
    /// <summary>
    /// Publishes a JSON payload to a specified Kafka topic with an optional correlation ID.    
    /// </summary>
    /// <param name="topic">The Kafka topic to publish the event to.</param>
    /// <param name="payload">The JSON payload to publish.</param>
    /// <param name="correlationId">The correlation ID for tracking the event.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation, containing the publish result.</returns>
    /// <exception cref="KafkaPublishException">Thrown if the event could not be published.</exception>
    public async Task<PublishResult> PublishAsync(string topic, JsonElement payload, string correlationId, CancellationToken cancellationToken)
    {
        ValidateTopic(topic);

        var normalizedCorrelationId = CorrelationId.NormalizeOrCreate(correlationId);
        var json = payload.GetRawText();
        var messageKey = TryGetGroupId(payload);

        var message = new Message<string, string>
        {
            Key = messageKey,
            Value = json,
            Headers = new Headers
            {
                new Header(CorrelationConstants.KafkaHeaderName, Encoding.UTF8.GetBytes(normalizedCorrelationId))
            }
        };
        _logger.LogInformation("Publishing event to Kafka. Topic: {Topic}, MessageKey: {MessageKey}.", topic, messageKey);

        try
        {
            var result = await _producer.ProduceAsync(topic, message, cancellationToken);

            _logger.LogInformation("Kafka event published. Topic: {Topic}, Partition: {Partition}, Offset: {Offset}, MessageKey: {MessageKey}.",
                result.Topic, result.Partition.Value, result.Offset.Value, messageKey);

            return new PublishResult(result.Topic, result.Partition.Value, result.Offset.Value);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (ProduceException<string, string> exception)
        {
            _logger.LogError(exception, "Kafka rejected event publication. Topic: {Topic}, MessageKey: {MessageKey}, Reason: {KafkaReason}.",
                topic, messageKey, exception.Error.Reason);

            throw new KafkaPublishException("The event could not be published at this time.", exception);
        }
        catch (KafkaException exception)
        {
            _logger.LogError(exception, "Kafka publication failed. Topic: {Topic}, MessageKey: {MessageKey}, Reason: {KafkaReason}.",
                topic, messageKey, exception.Error.Reason);

            throw new KafkaPublishException("The event could not be published at this time.", exception);
        }
    }
    #endregion

    #endregion

    #endregion
}