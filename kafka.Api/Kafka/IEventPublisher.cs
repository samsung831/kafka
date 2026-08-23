using Confluent.Kafka;
using System.Text.Json;

namespace kafka.Api.Kafka;

public sealed record PublishResult(string Topic, int Partition, long Offset);

public interface IEventPublisher
{
    Task<PublishResult> PublishAsync(string topic, JsonElement payload, string correlationId, CancellationToken cancellationToken);
}
