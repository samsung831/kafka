using Confluent.Kafka;
using kafka.Shared.Configuration;
using kafka.Shared.Constants;
using kafka.Shared.Consumer;
using kafka.Shared.DeadLetter;
using kafka.Shared.Health;
using kafka.Shared.Models.Accounts;
using kafka.Shared.MongoDB;
using kafka.Shared.Observability;
using kafka.Shared.Validation;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using Polly;
using System.Reflection.PortableExecutable;
using System.Text;
using System.Text.Json;
using static Confluent.Kafka.ConfigPropertyNames;

namespace kafka.AccountService.Consumers;

public sealed class AccountConsumerWorker : ConsumerBase
{
    #region Constructor
    public AccountConsumerWorker(IOptions<KafkaOptions> kafkaOptions, MongoContext mongoContext,
        WorkerHealthState workerHealthState, IOptions<ResilienceOptions> resilienceOptions, ILogger<AccountConsumerWorker> logger)
        : base("kafka.AccountService", kafkaOptions, resilienceOptions, logger)
    {
        _mongoContext = mongoContext;
        _workerHealthState = workerHealthState;
    }
    #endregion

    #region Properties

    #region Private
    private readonly MongoContext _mongoContext;
    private readonly WorkerHealthState _workerHealthState;
    #endregion

    #endregion

    #region Methods

    #region Protected

    #region ExecuteAsync
    /// <summary>
    /// Executes the account consumer worker asynchronously, consuming messages from the Kafka topic and processing them.
    /// </summary>
    /// <param name="stoppingToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    protected override Task ExecuteAsync(CancellationToken stoppingToken)
    {
        return ConsumeAsync(stoppingToken);
    }
    #endregion

    #endregion

    #region Private

    #region ConsumeAsync
    /// <summary>
    /// Consumes messages from the Kafka topic, processes them, and handles any exceptions that may occur during processing.
    /// </summary>
    /// <param name="stoppingToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private async Task ConsumeAsync(CancellationToken stoppingToken)
    {
        _workerHealthState.MarkStarted();

        var retryPipeline = CreatePersistenceRetryPipeline();
        using var deadLetterProducer = CreateDeadLetterProducer();
        var consumerConfig = CreateConsumerConfig();

        using var consumer = new ConsumerBuilder<string, string>(consumerConfig).Build();

        consumer.Subscribe(KafkaOptions.Topic);

        var writer = new VersionedDocumentWriter<AccountDocument>(_mongoContext.Accounts);

        Logger.LogInformation("Account consumer started. Topic: {Topic}, ConsumerGroup: {ConsumerGroup}.", KafkaOptions.Topic, KafkaOptions.GroupId);

        try
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                ConsumeResult<string, string>? consumeResult = null;
                IDisposable? messageScope = null;
                string? correlationId = null;

                try
                {
                    consumeResult = consumer.Consume(stoppingToken);
                    correlationId = GetCorrelationId(consumeResult.Message.Headers);
                    messageScope = Logger.BeginScope(new Dictionary<string, object?>
                        {
                            [CorrelationConstants.LogPropertyName] = correlationId,
                            ["Topic"] = consumeResult.Topic,
                            ["Partition"] = consumeResult.Partition.Value,
                            ["Offset"] = consumeResult.Offset.Value,
                            ["MessageKey"] = consumeResult.Message.Key
                        });

                    Logger.LogInformation("Account event received.");

                    var account = JsonSerializer.Deserialize<AccountDocument>(consumeResult.Message.Value, JsonSerializerOptions.Web);

                    if (account is null)
                    {
                        throw new JsonException("Account event deserialized to null.");
                    }

                    AccountEventValidator.Validate(account);

                    using (Logger.BeginScope(new Dictionary<string, object?>
                        {
                            ["AccountId"] = account.Id,
                            ["GroupId"] = account.GroupId,
                            ["Version"] = account.Version
                        }))
                    {
                        var writeResult = await retryPipeline.ExecuteAsync(async cancellationToken => 
                            await writer.UpsertAsync(account, cancellationToken), stoppingToken);

                        Logger.LogInformation("Account event processed. WriteResult: {WriteResult}.", writeResult);
                    }

                    consumer.StoreOffset(consumeResult);
                    consumer.Commit(consumeResult);

                    _workerHealthState.MarkProcessingSucceeded();

                    Logger.LogInformation("Account Kafka offset committed.");
                }
                catch (ConsumeException exception)
                {
                    Logger.LogError(exception, "Kafka consume error: {Reason}", exception.Error.Reason);
                    if (exception.Error.IsFatal)
                    {
                        _workerHealthState.MarkProcessingFailed(exception);
                        throw;
                    }
                }
                catch (JsonException exception)
                {
                    await PublishToDeadLetterAsync(deadLetterProducer, consumeResult, correlationId, "validation", exception,
                        0, stoppingToken);
                    CommitInvalidMessage(consumer, consumeResult);
                }
                catch (ArgumentException exception)
                {
                    await PublishToDeadLetterAsync(deadLetterProducer, consumeResult, correlationId, "validation", exception,
                        0, stoppingToken);
                    CommitInvalidMessage(consumer, consumeResult);
                }
                catch (MongoWriteException exception)
                {
                    await PublishToDeadLetterAsync(deadLetterProducer, consumeResult, correlationId, "validation", exception,
                        0, stoppingToken);
                    CommitInvalidMessage(consumer, consumeResult);
                }
                catch (OperationCanceledException) when (stoppingToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception exception)
                {
                    _workerHealthState.MarkProcessingFailed(exception);
                    Logger.LogError(exception, "Unexpected account processing error.");
                }
                finally
                {
                    messageScope?.Dispose();
                }
            }
        }
        finally
        {
            _workerHealthState.MarkStopped();
            consumer.Close();
            Logger.LogInformation("Account consumer stopped. Topic: {Topic}.", KafkaOptions.Topic);
        }
    }
    #endregion

    #endregion

    #endregion
}