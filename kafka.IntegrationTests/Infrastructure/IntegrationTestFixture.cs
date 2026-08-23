using Confluent.Kafka;
using Confluent.Kafka.Admin;
using kafka.AccountService.Consumers;
using kafka.EmployeeService.Consumers;
using kafka.Shared.Configuration;
using kafka.Shared.Constants;
using kafka.Shared.Models.Accounts;
using kafka.Shared.Models.Employees;
using kafka.Shared.MongoDB;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using MongoDB.Bson;
using MongoDB.Driver;
using Testcontainers.Kafka;
using Testcontainers.MongoDb;
using Xunit;

namespace kafka.IntegrationTests.Infrastructure;

public sealed class IntegrationTestFixture : IAsyncLifetime
{
    #region Constructor
    public IntegrationTestFixture()
    {
        DatabaseName = $"persons_integration_{Guid.NewGuid():N}";
        _loggerFactory = LoggerFactory.Create(builder => builder.SetMinimumLevel(LogLevel.Information).AddConsole());
        _kafkaContainer = new KafkaBuilder().Build();
        _mongoContainer = new MongoDbBuilder().WithImage("mongo:8.0").Build();
    }

    #endregion

    #region Properties

    #region Private
    private const string AccountConsumerGroupPrefix = "integration-account-service";
    private const string EmployeeConsumerGroupPrefix = "integration-employee-service";
    private static readonly TimeSpan InfrastructureTimeout = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan InfrastructurePollInterval = TimeSpan.FromMilliseconds(500);
    private static readonly TopicSpecification[] RequiredTopics =
    [
        new() { Name = KafkaTopicsConstants.Accounts, NumPartitions = 1, ReplicationFactor = 1 },
        new() { Name = KafkaTopicsConstants.Employees, NumPartitions = 1, ReplicationFactor = 1 }
    ];
    private readonly KafkaContainer _kafkaContainer;
    private readonly MongoDbContainer _mongoContainer;
    private AccountConsumerWorker? _accountWorker;
    private EmployeeConsumerWorker? _employeeWorker;
    private readonly ILoggerFactory _loggerFactory;
    private KafkaApiFactory? _kafkaApiFactory;
    private bool _initialized;
    private bool _disposed;
    #endregion

    #region Public

    #region DatabaseName
    /// <summary>
    /// Gets the name of the MongoDB database used for integration testing.
    /// </summary>
    public string DatabaseName { get; }
    #endregion

    #region KafkaBootstrapServers
    /// <summary>
    /// Gets the Kafka bootstrap servers used for integration testing.
    /// </summary>
    public string KafkaBootstrapServers
    {
        get
        {
            return _kafkaContainer.GetBootstrapAddress();
        }
    }
    #endregion

    #region MongoConnectionString
    /// <summary>
    /// Gets the MongoDB connection string used for integration testing.
    /// </summary>
    public string MongoConnectionString
    {
        get
        {
            return _mongoContainer.GetConnectionString();
        }
    }
    #endregion

    #region Database
    /// <summary>
    /// Gets the MongoDB database instance used for integration testing.
    /// </summary>
    public IMongoDatabase Database { get; private set; } = null!;
    #endregion

    #region MongoContext
    /// <summary>
    /// Gets the MongoContext instance used for integration testing.
    /// </summary>
    public MongoContext MongoContext { get; private set; } = null!;
    #endregion

    #region KafkaApiClient
    /// <summary>
    /// Gets the HttpClient instance used to interact with the Kafka API for integration testing.
    /// </summary>
    public HttpClient KafkaApiClient { get; private set; } = null!;
    #endregion

    #endregion

    #endregion

    #region Methods

    #region Private

    #region StartContainersAsync
    /// <summary>
    /// Starts the Kafka and MongoDB containers for integration testing and waits for MongoDB to be ready.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private async Task StartContainersAsync()
    {
        await Task.WhenAll(_kafkaContainer.StartAsync(), _mongoContainer.StartAsync());

        await WaitForMongoDbAsync();
    }
    #endregion

    #region WaitForMongoDbAsync
    /// <summary>
    /// Waits for the MongoDB container to be ready by pinging the database until a successful response is received or a timeout occurs.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private async Task WaitForMongoDbAsync()
    {
        await AsyncWait.UntilAsync(async cancellationToken =>
        {
            try
            {
                var mongoClient = new MongoClient(MongoConnectionString);

                var adminDatabase = mongoClient.GetDatabase("admin");

                var result = await adminDatabase.RunCommandAsync<BsonDocument>(new BsonDocument("ping", 1), cancellationToken: cancellationToken);

                return result.TryGetValue("ok", out var okValue) && okValue.ToDouble() == 1;
            }
            catch (Exception exception) when (exception is MongoException || exception is TimeoutException)
            {
                return false;
            }
        },
            timeout: InfrastructureTimeout,
            pollInterval: InfrastructurePollInterval);
    }
    #endregion

    #region InitializeMongoDbAsync
    /// <summary>
    /// Initializes the MongoDB database and context for integration testing by creating a MongoClient,
    /// getting the database, and running a ping command to ensure connectivity.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private async Task InitializeMongoDbAsync()
    {
        var mongoClient = new MongoClient(MongoConnectionString);

        Database = mongoClient.GetDatabase(DatabaseName);

        await Database.RunCommandAsync<BsonDocument>(new BsonDocument("ping", 1));

        MongoContext = new MongoContext(Options.Create(new MongoOptions
        {
            ConnectionString = MongoConnectionString,
            DatabaseName = DatabaseName
        }));
    }
    #endregion

    #region CreateKafkaTopicsAsync
    /// <summary>
    /// Creates the required Kafka topics for integration testing using the AdminClient.
    /// It waits for Kafka to be available, attempts to create the topics, and handles the case where the topics already exist.
    /// Finally, it waits for the topics to be available before proceeding.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private async Task CreateKafkaTopicsAsync()
    {
        using var adminClient = new AdminClientBuilder(new AdminClientConfig
        {
            BootstrapServers = KafkaBootstrapServers
        }).Build();

        await WaitForKafkaAsync(adminClient);

        try
        {
            await adminClient.CreateTopicsAsync(RequiredTopics);
        }
        catch (CreateTopicsException exception) when (AllTopicErrorsAreAlreadyExists(exception))
        {

        }

        await WaitForTopicsAsync(adminClient);
    }
    #endregion

    #region AllTopicErrorsAreAlreadyExists
    /// <summary>
    /// Checks if all topic creation errors in the provided CreateTopicsException are due to the topics already existing.
    /// </summary>
    /// <param name="exception">The CreateTopicsException to check.</param>
    /// <returns>True if all topic creation errors are due to the topics already existing; otherwise, false.</returns>
    private static bool AllTopicErrorsAreAlreadyExists(CreateTopicsException exception)
    {
        return exception.Results.Count > 0 && exception.Results.All(result => result.Error.Code == ErrorCode.TopicAlreadyExists);
    }
    #endregion

    #region WaitForKafkaAsync
    /// <summary>
    /// Waits for the Kafka broker to be available by repeatedly attempting to retrieve metadata until a successful response is received or a timeout occurs.
    /// </summary>
    /// <param name="adminClient">The IAdminClient instance used to interact with the Kafka broker.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private async Task WaitForKafkaAsync(IAdminClient adminClient)
    {
        await AsyncWait.UntilAsync(_ =>
        {
            try
            {
                var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(2));

                var brokerIsAvailable = metadata.Brokers.Count > 0;

                return Task.FromResult(brokerIsAvailable);
            }
            catch (KafkaException)
            {
                return Task.FromResult(false);
            }
        },
            timeout: InfrastructureTimeout,
            pollInterval: InfrastructurePollInterval);
    }
    #endregion

    #region WaitForTopicsAsync
    /// <summary>
    /// Waits for the required Kafka topics to be available by repeatedly checking the metadata until both topics are found or a timeout occurs.
    /// </summary>
    /// <param name="adminClient">The IAdminClient instance used to interact with the Kafka broker.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private async Task WaitForTopicsAsync(IAdminClient adminClient)
    {
        await AsyncWait.UntilAsync(_ =>
        {
            try
            {
                var metadata = adminClient.GetMetadata(TimeSpan.FromSeconds(2));

                var availableTopics = metadata.Topics.Where(topic => topic.Error.Code == ErrorCode.NoError)
                    .Select(topic => topic.Topic)
                    .ToHashSet(StringComparer.Ordinal);

                var accountsTopicIsAvailable = availableTopics.Contains(KafkaTopicsConstants.Accounts);

                var employeesTopicIsAvailable = availableTopics.Contains(KafkaTopicsConstants.Employees);

                return Task.FromResult(accountsTopicIsAvailable && employeesTopicIsAvailable);
            }
            catch (KafkaException)
            {
                return Task.FromResult(false);
            }
        },
            timeout: InfrastructureTimeout,
            pollInterval: InfrastructurePollInterval);
    }
    #endregion

    #region StartWorkersAsync
    /// <summary>
    /// Starts the AccountConsumerWorker and EmployeeConsumerWorker for integration testing by creating unique consumer groups,
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private async Task StartWorkersAsync()
    {
        var accountConsumerGroup = $"{AccountConsumerGroupPrefix}-{Guid.NewGuid():N}";

        var employeeConsumerGroup = $"{EmployeeConsumerGroupPrefix}-{Guid.NewGuid():N}";

        _accountWorker = new AccountConsumerWorker(CreateKafkaOptions(KafkaTopicsConstants.Accounts, accountConsumerGroup),
            MongoContext,
            new Shared.Health.WorkerHealthState(),
            Options.Create(new ResilienceOptions()),
            _loggerFactory.CreateLogger<AccountConsumerWorker>());

        _employeeWorker = new EmployeeConsumerWorker(CreateKafkaOptions(KafkaTopicsConstants.Employees, employeeConsumerGroup),
            MongoContext,
            new Shared.Health.WorkerHealthState(),
            Options.Create(new ResilienceOptions()),
            _loggerFactory.CreateLogger<EmployeeConsumerWorker>());

        await _accountWorker.StartAsync(CancellationToken.None);

        await _employeeWorker.StartAsync(CancellationToken.None);
    }
    #endregion

    #region CreateKafkaOptions
    /// <summary>
    /// Creates Kafka consumer options for an integration-test worker.
    /// </summary>
    /// <param name="topic">The topic the worker consumes.</param>
    /// <param name="groupId">The unique consumer group ID.</param>
    /// <returns>The configured Kafka options.</returns>
    private IOptions<KafkaOptions> CreateKafkaOptions(string topic, string groupId)
    {
        return Options.Create(new KafkaOptions
        {
            BootstrapServers = KafkaBootstrapServers,
            GroupId = groupId,
            Topic = topic
        });
    }
    #endregion

    #region StopWorkersAsync
    /// <summary>
    /// Stops the AccountConsumerWorker and EmployeeConsumerWorker for integration testing, ensuring that they are properly disposed of after stopping.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private async Task StopWorkersAsync()
    {
        if (_accountWorker is not null)
        {
            try
            {
                await _accountWorker.StopAsync(CancellationToken.None);
            }
            finally
            {
                _accountWorker.Dispose();
                _accountWorker = null;
            }
        }

        if (_employeeWorker is not null)
        {
            try
            {
                await _employeeWorker.StopAsync(CancellationToken.None);
            }
            finally
            {
                _employeeWorker.Dispose();
                _employeeWorker = null;
            }
        }
    }
    #endregion

    #region StartApisAsync
    /// <summary>
    /// Starts the Kafka API for integration testing by creating a KafkaApiFactory and an HttpClient to interact with the API.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private Task StartApisAsync()
    {
        _kafkaApiFactory = new KafkaApiFactory(
            KafkaBootstrapServers,
            MongoConnectionString,
            DatabaseName);

        KafkaApiClient = _kafkaApiFactory.CreateClient(new WebApplicationFactoryClientOptions
        {
            AllowAutoRedirect = false
        });

        return Task.CompletedTask;
    }
    #endregion

    #region VerifyApplicationsAsync
    /// <summary>
    /// Verifies that the PersonsApi is running and responding correctly by sending a GET request to the integration-startup-check endpoint.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    /// <exception cref="InvalidOperationException"></exception>
    private async Task VerifyApplicationsAsync()
    {
        using var personsApiResponse = await KafkaApiClient.GetAsync("/api/persons/integration-startup-check");

        if (personsApiResponse.StatusCode != System.Net.HttpStatusCode.NotFound)
        {
            var responseBody = await personsApiResponse.Content.ReadAsStringAsync();

            throw new InvalidOperationException("PersonsApi failed the integration-test startup check. " +
                $"HTTP status: {(int)personsApiResponse.StatusCode}. Response body: {responseBody}");
        }
    }
    #endregion

    #region DisposeResourcesAsync
    /// <summary>
    /// Disposes of the resources used for integration testing, including stopping the workers, disposing of the KafkaApiClient,
    /// KafkaApiFactory, and the Kafka and MongoDB containers.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    private async Task DisposeResourcesAsync()
    {
        try
        {
            await StopWorkersAsync();
        }
        catch
        {

        }

        KafkaApiClient?.Dispose();

        KafkaApiClient = null!;

        if (_kafkaApiFactory is not null)
        {
            try
            {
                await _kafkaApiFactory.DisposeAsync();
            }
            finally
            {
                _kafkaApiFactory = null;
            }
        }

        try
        {
            await _kafkaContainer.DisposeAsync();
        }
        finally
        {
            try
            {
                await _mongoContainer.DisposeAsync();
            }
            finally
            {
                _loggerFactory.Dispose();
            }
        }
    }
    #endregion

    #region EnsureInitialized
    /// <summary>
    /// Ensures that the integration-test fixture has been initialized and not disposed.
    /// If the fixture is not initialized or has been disposed, it throws an appropriate exception.
    /// </summary>
    /// <exception cref="InvalidOperationException">Thrown if the fixture has not been initialized.</exception>
    /// <exception cref="ObjectDisposedException">Thrown if the fixture has been disposed.</exception>
    private void EnsureInitialized()
    {
        if (!_initialized)
        {
            throw new InvalidOperationException("The integration-test fixture has not been initialized.");
        }

        if (_disposed)
        {
            throw new ObjectDisposedException(nameof(IntegrationTestFixture));
        }
    }
    #endregion

    #endregion

    #region Public

    #region IAsyncLifetime
    /// <summary>
    /// Initializes the integration-test fixture by starting the Kafka and MongoDB containers, creating Kafka topics,
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    public async Task InitializeAsync()
    {
        if (_initialized)
        {
            return;
        }

        try
        {
            await StartContainersAsync();
            await CreateKafkaTopicsAsync();
            await InitializeMongoDbAsync();
            await StartWorkersAsync();
            await StartApisAsync();
            await VerifyApplicationsAsync();
            _initialized = true;
        }
        catch
        {
            await DisposeResourcesAsync();

            throw;
        }
    }
    #endregion

    #region DisposeAsync
    /// <summary>
    /// Disposes of the integration-test fixture by stopping the workers, disposing of the KafkaApiClient,
    /// KafkaApiFactory, and the Kafka and MongoDB containers.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    public async Task DisposeAsync()
    {
        if (_disposed)
        {
            return;
        }

        _disposed = true;

        await DisposeResourcesAsync();

        GC.SuppressFinalize(this);
    }
    #endregion

    #region DeleteAllDataAsync
    /// <summary>
    /// Deletes all data from the Accounts and Employees collections in the MongoDB database used for integration testing.
    /// </summary>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    public async Task DeleteAllDataAsync(CancellationToken cancellationToken = default)
    {
        EnsureInitialized();

        await MongoContext.Accounts.DeleteManyAsync(FilterDefinition<AccountDocument>.Empty, cancellationToken);

        await MongoContext.Employees.DeleteManyAsync(FilterDefinition<EmployeeDocument>.Empty, cancellationToken);
    }
    #endregion

    #region PublishRawAsync
    /// <summary>
    /// Publishes a raw JSON message to the specified Kafka topic with the given key and correlation ID.
    /// </summary>
    /// <param name="topic">The Kafka topic to which the message will be published.</param>
    /// <param name="key">The key of the Kafka message.</param>
    /// <param name="json">The raw JSON payload of the Kafka message.</param>
    /// <param name="correlationId">The correlation ID for tracking the message.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation, containing the delivery result of the Kafka message.</returns>
    /// <exception cref="ArgumentException">Thrown if any of the required parameters are null or whitespace.</exception>
    public async Task<DeliveryResult<string, string>> PublishRawAsync(string topic, string key, string json, string correlationId,
        CancellationToken cancellationToken = default)
    {
        EnsureInitialized();

        if (string.IsNullOrWhiteSpace(topic))
        {
            throw new ArgumentException("Kafka topic is required.", nameof(topic));
        }
        if (string.IsNullOrWhiteSpace(key))
        {
            throw new ArgumentException("Kafka message key is required.", nameof(key));
        }
        if (string.IsNullOrWhiteSpace(json))
        {
            throw new ArgumentException("Kafka JSON payload is required.", nameof(json));
        }
        if (string.IsNullOrWhiteSpace(correlationId))
        {
            throw new ArgumentException("Correlation ID is required.", nameof(correlationId));
        }

        var producerConfig = new ProducerConfig
            {
                BootstrapServers = KafkaBootstrapServers,
                Acks = Acks.All,
                EnableIdempotence = true
            };

        using var producer = new ProducerBuilder<string, string>(producerConfig).Build();

        var message = new Message<string, string>
            {
                Key = key,
                Value = json,
                Headers = new Headers
                {
                    new Header("correlation-id", System.Text.Encoding.UTF8.GetBytes(correlationId))
                }
            };

        var deliveryResult = await producer.ProduceAsync(topic, message, cancellationToken);

        producer.Flush(cancellationToken);

        return deliveryResult;
    }
    #endregion

    #endregion

    #endregion
}