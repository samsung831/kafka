using Confluent.Kafka;
using kafka.Shared.Configuration;
using kafka.Shared.Constants;
using kafka.Shared.DeadLetter;
using kafka.Shared.Health;
using kafka.Shared.Models.Employees;
using kafka.Shared.MongoDB;
using kafka.Shared.Observability;
using kafka.Shared.Validation;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Driver;
using Polly;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;

namespace kafka.Shared.Consumer;


public class ConsumerBase : BackgroundService
{
    #region Constructor
    public ConsumerBase(string sourceServiceName, IOptions<KafkaOptions> kafkaOptions, IOptions<ResilienceOptions> resilienceOptions, ILogger<ConsumerBase> logger)
    {
        _sourceService = sourceServiceName;
        KafkaOptions = kafkaOptions.Value;
        _resilienceOptions = resilienceOptions.Value;
        Logger = logger;
    }
    #endregion

    #region Properties

    #region Private
    private readonly ResilienceOptions _resilienceOptions;
    private readonly string _sourceService;
    #endregion

    #region Public
    public readonly KafkaOptions KafkaOptions;
    public readonly ILogger<ConsumerBase> Logger;
    #endregion

    #endregion

    #region Methods

    #region Protected

    #region ExecuteAsync
    /// <summary>
    /// Executes the consumer worker asynchronously.
    /// This method is called when the BackgroundService starts and should contain the logic for consuming messages from Kafka.
    /// </summary>
    /// <param name="stoppingToken">A token that can be used to signal cancellation of the operation.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    /// <exception cref="NotImplementedException"></exception>
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        throw new NotImplementedException();
    }
    #endregion

    #region CommitInvalidMessage
    /// <summary>
    /// Commits an invalid message to Kafka.
    /// </summary>
    /// <param name="consumer">The Kafka consumer.</param>
    /// <param name="consumeResult">The consume result containing the message to commit.</param>
    protected void CommitInvalidMessage(IConsumer<string, string> consumer, ConsumeResult<string, string>? consumeResult)
    {
        if (consumeResult is null)
        {
            return;
        }

        consumer.StoreOffset(consumeResult);
        consumer.Commit(consumeResult);
    }
    #endregion

    #region GetCorrelationId
    /// <summary>
    /// Gets the correlation ID from the Kafka message headers.
    /// </summary>
    /// <param name="headers">The Kafka message headers.</param>
    /// <returns>The correlation ID.</returns>
    protected string GetCorrelationId(Headers? headers)
    {
        if (headers is not null)
        {
            try
            {
                var correlationBytes = headers.GetLastBytes(CorrelationConstants.KafkaHeaderName);

                if (correlationBytes is not null && correlationBytes.Length > 0)
                {
                    var headerValue = Encoding.UTF8.GetString(correlationBytes);

                    return CorrelationId.NormalizeOrCreate(headerValue);
                }
            }
            catch (KeyNotFoundException)
            {

            }
        }

        return CorrelationId.Create();
    }
    #endregion

    #region CreateDeadLetterProducer
    /// <summary>
    /// Creates a Kafka producer for publishing messages to the dead letter topic.
    /// </summary>
    /// <returns>An instance of <see cref="IProducer{TKey, TValue}"/> configured for the dead letter topic.</returns>
    protected IProducer<string, string> CreateDeadLetterProducer()
    {
        var producerConfig = new ProducerBuilder<string, string>(
            new ProducerConfig
            {
                BootstrapServers = KafkaOptions.BootstrapServers,
                Acks = Acks.All,
                EnableIdempotence = true
            }).Build();

        return producerConfig;
    }
    #endregion

    #region CreateConsumerConfig
    /// <summary>
    /// Creates a Kafka consumer configuration.
    /// </summary>
    /// <returns>An instance of <see cref="ConsumerConfig"/> configured for the Kafka consumer.</returns>
    protected ConsumerConfig CreateConsumerConfig()
    {
        return new ConsumerConfig
        {
            BootstrapServers = KafkaOptions.BootstrapServers,
            GroupId = KafkaOptions.GroupId,
            AutoOffsetReset = AutoOffsetReset.Earliest,
            EnableAutoCommit = false,
            EnableAutoOffsetStore = false
        };
    }
    #endregion

    #region CreatePersistenceRetryPipeline
    /// <summary>
    /// Creates a resilience pipeline for retrying persistence operations.
    /// </summary>
    /// <returns>An instance of <see cref="ResiliencePipeline"/> configured with retry strategy.</returns>
    protected ResiliencePipeline CreatePersistenceRetryPipeline()
    {
        return new ResiliencePipelineBuilder().AddRetry(
            new Polly.Retry.RetryStrategyOptions
            {
                MaxRetryAttempts = _resilienceOptions.MaxRetryAttempts,
                Delay = TimeSpan.FromMilliseconds(_resilienceOptions.RetryDelayMilliseconds),
                BackoffType = DelayBackoffType.Exponential,
                UseJitter = true,
                ShouldHandle = new PredicateBuilder()
                    .Handle<MongoConnectionException>()
                    .Handle<MongoExecutionTimeoutException>()
                    .Handle<TimeoutException>(),
                OnRetry = arguments =>
                {
                    Logger.LogWarning(arguments.Outcome.Exception, "MongoDB operation failed. Retry attempt: {RetryAttempt}, RetryDelay: {RetryDelay}.",
                        arguments.AttemptNumber + 1, arguments.RetryDelay);
                    return default;
                }
            }).Build();
    }

    #endregion

    #region PublishToDeadLetter
    /// <summary>
    /// Publishes a message to the dead letter topic in Kafka.
    /// </summary>
    /// <param name="producer">The Kafka producer.</param>
    /// <param name="consumeResult">The consume result containing the message to publish.</param>
    /// <param name="correlationId">The correlation ID for the message.</param>
    /// <param name="failureReason">The reason for the failure.</param>
    /// <param name="exception">The exception that caused the failure.</param>
    /// <param name="retryCount">The number of retry attempts.</param>
    /// <param name="cancellationToken">The cancellation token.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    protected async Task PublishToDeadLetterAsync(IProducer<string, string> producer, ConsumeResult<string, string> consumeResult,
        string correlationId, string failureReason, Exception exception, int retryCount, CancellationToken cancellationToken)
    {
        if (consumeResult is null)
        {
            return;
        }

        var deadLetterMessage = new DeadLetterMessage
        {
            SourceService = _sourceService,
            SourceTopic = consumeResult.Topic,
            SourcePartition = consumeResult.Partition.Value,
            SourceOffset = consumeResult.Offset.Value,
            SourceKey = consumeResult.Message.Key,
            OriginalPayload = consumeResult.Message.Value,
            CorrelationId = correlationId,
            FailureReason = failureReason,
            ErrorType = exception.GetType().Name,
            RetryCount = retryCount,
            FailedAtUtc = DateTimeOffset.UtcNow
        };

        var payload = JsonSerializer.Serialize(deadLetterMessage, JsonSerializerOptions.Web);

        var message = new Message<string, string>
        {
            Key = consumeResult.Message.Key,
            Value = payload,
            Headers = new Headers
                {
                    new Header(CorrelationConstants.KafkaHeaderName, Encoding.UTF8.GetBytes(correlationId))
                }
        };

        await producer.ProduceAsync(KafkaOptions.DeadLetterTopic, message, cancellationToken);

        Logger.LogWarning("Message published to DLQ. DeadLetterTopic: {DeadLetterTopic}, SourceTopic: {SourceTopic}, " +
            "Partition: {Partition}, Offset: {Offset}, FailureReason: {FailureReason}.",
            KafkaOptions.DeadLetterTopic, consumeResult.Topic, consumeResult.Partition.Value, consumeResult.Offset.Value, failureReason);
    }
    #endregion

    #endregion

    #endregion
}
