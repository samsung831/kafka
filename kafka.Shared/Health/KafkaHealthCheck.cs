using System;
using System.Collections.Generic;
using System.Text;
using Confluent.Kafka;
using kafka.Shared.Configuration;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

namespace kafka.Shared.Health;

public sealed class KafkaHealthCheck : IHealthCheck
{
    #region Constructor
    public KafkaHealthCheck(IOptions<KafkaOptions> options)
    {
        _options = options.Value;
    }
    #endregion

    #region Properties

    #region Private
    private readonly KafkaOptions _options;
    #endregion

    #endregion

    #region Methods

    #region Public

    #region CheckHealthAsync
    /// <summary>
    /// Checks the health of the Kafka topic by attempting to retrieve its metadata and verifying its availability.
    /// </summary>
    /// <param name="context">The health check context.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the health check.</param>
    /// <returns>A task that represents the asynchronous operation, containing the health check result.</returns>
    public Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        cancellationToken.ThrowIfCancellationRequested();

        try
        {
            using var adminClient = new AdminClientBuilder(new AdminClientConfig
            {
                BootstrapServers = _options.BootstrapServers
            }).Build();

            var metadata = adminClient.GetMetadata(_options.Topic, TimeSpan.FromSeconds(3));

            var topic = metadata.Topics.FirstOrDefault(item => string.Equals(item.Topic, _options.Topic, StringComparison.Ordinal));

            if (topic is null)
            {
                return Task.FromResult(HealthCheckResult.Unhealthy($"Kafka topic '{_options.Topic}' was not found."));
            }

            if (topic.Error.Code != ErrorCode.NoError)
            {
                return Task.FromResult(HealthCheckResult.Unhealthy($"Kafka topic '{_options.Topic}' is unavailable."));
            }

            if (topic.Partitions.Count == 0)
            {
                return Task.FromResult(HealthCheckResult.Unhealthy($"Kafka topic '{_options.Topic}' has no partitions."));
            }

            return Task.FromResult(HealthCheckResult.Healthy($"Kafka topic '{_options.Topic}' is available.",
                data: new Dictionary<string, object> { ["topic"] = _options.Topic, ["partitions"] = topic.Partitions.Count }));
        }
        catch (Exception exception) when (exception is KafkaException || exception is TimeoutException)
        {
            return Task.FromResult(HealthCheckResult.Unhealthy("Kafka is unavailable.", exception));
        }
    }
    #endregion

    #endregion

    #endregion
}
