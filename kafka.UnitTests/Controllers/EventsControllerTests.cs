using kafka.Api.Controllers;
using kafka.Api.Kafka;
using kafka.Api.Responses;
using kafka.Shared.Constants;
using kafka.Shared.Observability;
using kafka.UnitTests.Helpers;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using System.Text.Json;

namespace kafka.UnitTests.Controllers;

public sealed class EventsControllerTests
{
    #region Methods

    #region Public

    #region PublishAccountAsync_UsesRequestCorrelationIdAndReturnsAcceptedResponse
    /// <summary>
    /// Tests that the PublishAccountAsync method uses the correlation ID from the request context and returns an AcceptedResult with the expected response.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task PublishAccountAsync_UsesRequestCorrelationIdAndReturnsAcceptedResponse()
    {
        var publisher = new RecordingEventPublisherHelper(new PublishResult(KafkaTopicsConstants.Accounts, 2, 42));
        var controller = new EventsController(publisher)
        {
            ControllerContext = new ControllerContext
            {
                HttpContext = new DefaultHttpContext()
            }
        };
        controller.HttpContext.Items[CorrelationConstants.HttpContextItemName] = "request-001";
        using var document = JsonDocument.Parse("{\"mappingFields\":{\"EmployeeId\":{\"groupId\":\"ABC123\"}}}");

        var result = await controller.PublishAccountAsync(document.RootElement, CancellationToken.None);

        var accepted = Assert.IsType<AcceptedResult>(result);
        var response = Assert.IsType<PublishEventResponse>(accepted.Value);
        Assert.Equal(KafkaTopicsConstants.Accounts, publisher.Topic);
        Assert.Equal("request-001", publisher.CorrelationId);
        Assert.Equal(document.RootElement.GetRawText(), publisher.Payload.GetRawText());
        Assert.Equal("Account event accepted for processing.", response.Message);
        Assert.Equal("request-001", response.CorrelationId);
        Assert.Equal(2, response.Partition);
        Assert.Equal(42, response.Offset);
    }
    #endregion

    #region PublishEmployeeAsync_WhenCorrelationIdIsMissing_GeneratesCorrelationId
    /// <summary>
    /// Tests that the PublishEmployeeAsync method generates a correlation ID when it is missing from the request context
    /// and returns an AcceptedResult with the expected response.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task PublishEmployeeAsync_WhenCorrelationIdIsMissing_GeneratesCorrelationId()
    {
        var cancellationToken = new CancellationTokenSource().Token;
        var publisher = new RecordingEventPublisherHelper(new PublishResult(KafkaTopicsConstants.Employees, 1, 7));
        var controller = new EventsController(publisher)
        {
            ControllerContext = new ControllerContext
            {
                HttpContext = new DefaultHttpContext()
            }
        };
        using var document = JsonDocument.Parse("{\"event\":\"created\"}");

        var result = await controller.PublishEmployeeAsync(document.RootElement, cancellationToken);

        var accepted = Assert.IsType<AcceptedResult>(result);
        var response = Assert.IsType<PublishEventResponse>(accepted.Value);
        Assert.Equal(KafkaTopicsConstants.Employees, publisher.Topic);
        Assert.False(string.IsNullOrWhiteSpace(publisher.CorrelationId));
        Assert.Equal(32, publisher.CorrelationId.Length);
        Assert.Equal(publisher.CorrelationId, response.CorrelationId);
        Assert.Equal(cancellationToken, publisher.CancellationToken);
    }
    #endregion

    #endregion

    #endregion
}
