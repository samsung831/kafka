using Confluent.Kafka;
using kafka.Api.Exceptions;
using kafka.Api.Kafka;
using kafka.Shared.Constants;
using kafka.Shared.Observability;
using kafka.UnitTests.Helpers;
using Microsoft.Extensions.Logging.Abstractions;
using System.Reflection;
using System.Text;
using System.Text.Json;

namespace kafka.UnitTests.Kafka;

public sealed class KafkaEventPublisherTests
{
    #region Methods

    #region Public

    #region PublicAsync_BuildsExpectedKafkaMessageAndReturnsDeliveryDetails
    /// <summary>
    /// Tests that the PublishAsync method of KafkaEventPublisher builds the expected Kafka message and returns the correct delivery details.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task PublishAsync_BuildsExpectedKafkaMessageAndReturnsDeliveryDetails()
    {
        var producer = DispatchProxy.Create<IProducer<string, string>, RecordingProducerHelper>();
        var recordingProducer = (RecordingProducerHelper)(object)producer;
        recordingProducer.DeliveryResult = new DeliveryResult<string, string>
        {
            Topic = KafkaTopicsConstants.Accounts,
            Partition = new Partition(2),
            Offset = new Offset(42)
        };
        var publisher = new KafkaEventPublisher(producer, NullLogger<KafkaEventPublisher>.Instance);
        using var document = JsonDocument.Parse("{\"mappingFields\":{\"EmployeeId\":{\"groupId\":\"ABC123\"}}}");

        var result = await publisher.PublishAsync(KafkaTopicsConstants.Accounts, document.RootElement, " request-001 ", CancellationToken.None);

        Assert.Equal(new PublishResult(KafkaTopicsConstants.Accounts, 2, 42), result);
        Assert.Equal(KafkaTopicsConstants.Accounts, recordingProducer.Topic);
        Assert.Equal("ABC123", recordingProducer.Message!.Key);
        Assert.Equal(document.RootElement.GetRawText(), recordingProducer.Message.Value);
        Assert.Equal("request-001", Encoding.UTF8.GetString(recordingProducer.Message.Headers!.GetLastBytes(CorrelationConstants.KafkaHeaderName)!));
    }
    #endregion

    #region PublicAsync_WithUnsupportedTopic_ThrowsWithoutCallingProducer
    /// <summary>
    /// Tests that the PublishAsync method of KafkaEventPublisher throws an ArgumentException when called with an unsupported topic,
    /// and does not call the producer.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task PublishAsync_WithUnsupportedTopic_ThrowsWithoutCallingProducer()
    {
        var producer = DispatchProxy.Create<IProducer<string, string>, RecordingProducerHelper>();
        var recordingProducer = (RecordingProducerHelper)(object)producer;
        var publisher = new KafkaEventPublisher(producer, NullLogger<KafkaEventPublisher>.Instance);
        using var document = JsonDocument.Parse("{}");

        await Assert.ThrowsAsync<ArgumentException>(() => publisher.PublishAsync("topic.unsupported", document.RootElement, "request-001", CancellationToken.None));

        Assert.Null(recordingProducer.Topic);
    }
    #endregion

    #region PublicAsync_WhenKafkaFails_ThrowsKafkaPublishException
    /// <summary>
    /// Tests that the PublishAsync method of KafkaEventPublisher throws a KafkaPublishException when the underlying Kafka producer fails,
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task PublishAsync_WhenKafkaFails_ThrowsKafkaPublishException()
    {
        var producer = DispatchProxy.Create<IProducer<string, string>, RecordingProducerHelper>();
        var recordingProducer = (RecordingProducerHelper)(object)producer;
        recordingProducer.Exception = new KafkaException(new Error(ErrorCode.Local_Transport, "Transport failure"));
        var publisher = new KafkaEventPublisher(producer, NullLogger<KafkaEventPublisher>.Instance);
        using var document = JsonDocument.Parse("{}");

        var exception = await Assert.ThrowsAsync<KafkaPublishException>(() => publisher.PublishAsync(KafkaTopicsConstants.Employees, document.RootElement, "request-001", CancellationToken.None));

        Assert.IsType<KafkaException>(exception.InnerException);
    }
    #endregion

    #endregion

    #endregion
}
