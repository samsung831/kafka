using Confluent.Kafka;
using kafka.Api.Kafka;
using kafka.Api.Responses;
using kafka.Shared.Constants;
using kafka.Shared.Observability;
using Microsoft.AspNetCore.Mvc;
using System.Text.Json;

namespace kafka.Api.Controllers;

[ApiController]
[Route("api/events")]
[Produces("application/json")]
public sealed class EventsController : ControllerBase
{
    #region Constructor
    public EventsController(IEventPublisher publisher)
    {
        _publisher = publisher;
    }
    #endregion

    #region Properties

    #region Private
    private readonly IEventPublisher _publisher;
    #endregion

    #endregion

    #region Methods

    #region Private

    #region GetCorrelationId
    /// <summary>
    /// Gets the correlation ID from the HTTP context or generates a new one if not present.
    /// </summary>
    /// <returns>The correlation ID.</returns>
    private string GetCorrelationId()
    {
        if (HttpContext.Items.TryGetValue(CorrelationConstants.HttpContextItemName, out var value) && value is string correlationId)
        {
            return correlationId;
        }

        return CorrelationId.Create();
    }
    #endregion

    #endregion

    #region Public

    #region Accounts
    /// <summary>
    /// Publishes an account event to the Kafka topic for accounts.
    /// </summary>
    /// <param name="payload">The JSON payload of the account event.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>An IActionResult representing the result of the publish operation.</returns>
    [HttpPost("accounts")]
    [Consumes("application/json")]
    [ProducesResponseType(typeof(PublishEventResponse),StatusCodes.Status202Accepted)]
    [ProducesResponseType(typeof(ValidationProblemDetails),StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(ProblemDetails),StatusCodes.Status503ServiceUnavailable)]
    [ProducesResponseType(typeof(ProblemDetails),StatusCodes.Status500InternalServerError)]
    public async Task<IActionResult> PublishAccountAsync([FromBody] JsonElement payload, CancellationToken cancellationToken)
    {
        var correlationId = GetCorrelationId();
        var result = await _publisher.PublishAsync(KafkaTopicsConstants.Accounts, payload, correlationId, cancellationToken);

        var response = new PublishEventResponse
            {
                Message = "Account event accepted for processing.",
                CorrelationId = correlationId,
                Topic = result.Topic,
                Partition = result.Partition,
                Offset = result.Offset
            };

        return Accepted(response);
    }
    #endregion

    #region Employees
    /// <summary>
    /// Publishes an employee event to the Kafka topic for employees.
    /// </summary>
    /// <param name="payload">The JSON payload of the employee event.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>An IActionResult representing the result of the publish operation.</returns>
    [HttpPost("employees")]
    [Consumes("application/json")]
    [ProducesResponseType(typeof(PublishEventResponse),StatusCodes.Status202Accepted)]
    [ProducesResponseType(typeof(ValidationProblemDetails),StatusCodes.Status400BadRequest)]
    [ProducesResponseType(typeof(ProblemDetails),StatusCodes.Status503ServiceUnavailable)]
    [ProducesResponseType(typeof(ProblemDetails),StatusCodes.Status500InternalServerError)]
    public async Task<IActionResult> PublishEmployeeAsync([FromBody] JsonElement payload, CancellationToken cancellationToken)
    {
        var correlationId = GetCorrelationId();
        var result = await _publisher.PublishAsync(KafkaTopicsConstants.Employees, payload, correlationId, cancellationToken);

        var response = new PublishEventResponse
        {
            Message = "Employee event accepted for processing.",
            CorrelationId = correlationId,
            Topic = result.Topic,
            Partition = result.Partition,
            Offset = result.Offset
        };

        return Accepted(response);
    }
    #endregion

    #endregion

    #endregion
}