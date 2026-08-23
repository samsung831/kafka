using kafka.Api.Exceptions;
using kafka.Shared.Constants;
using kafka.Shared.Exceptions;
using kafka.Shared.Observability;
using Microsoft.AspNetCore.Diagnostics;
using Microsoft.AspNetCore.Mvc;

namespace kafka.Api.ErrorHandling;

public sealed class GlobalExceptionHandler : IExceptionHandler
{
    private sealed record ExceptionMapping(int StatusCode, string Title, string Detail, string ErrorCode);

    #region Constructor
    public GlobalExceptionHandler(ILogger<GlobalExceptionHandler> logger,IProblemDetailsService problemDetailsService)
    {
        _logger = logger;
        _problemDetailsService = problemDetailsService;
    }
    #endregion

    #region Properties

    #region Private
    private readonly ILogger<GlobalExceptionHandler> _logger;
    private readonly IProblemDetailsService _problemDetailsService;
    #endregion

    #endregion

    #region Methods

    #region Private

    #region MapException
    /// <summary>
    /// Maps an exception to an ExceptionMapping containing the appropriate HTTP status code, title, detail, and error code.
    /// </summary>
    /// <param name="exception">The exception to map.</param>
    /// <returns>An ExceptionMapping containing the HTTP status code, title, detail, and error code.</returns>
    private static ExceptionMapping MapException(Exception exception)
    {
        return exception switch
        {
            RequestValidationException validationException =>
                new ExceptionMapping(
                    StatusCodes.Status400BadRequest,
                    "Request validation failed",
                    validationException.Message,
                    validationException.ErrorCode),

            ResourceNotFoundException notFoundException =>
                new ExceptionMapping(
                    StatusCodes.Status404NotFound,
                    "Resource not found",
                    notFoundException.Message,
                    notFoundException.ErrorCode),

            KafkaPublishException kafkaException =>
                new ExceptionMapping(
                    StatusCodes.Status503ServiceUnavailable,
                    "Kafka service unavailable",
                    kafkaException.Message,
                    kafkaException.ErrorCode),

            _ =>
                new ExceptionMapping(
                    StatusCodes.Status500InternalServerError,
                    "Internal server error",
                    "An unexpected server error occurred.",
                    ApiErrorCodes.InternalServerError)
        };
    }
    #endregion

    #endregion

    #region Public

    #region TryHandleAsync
    /// <summary>
    /// Attempts to handle an exception and write a ProblemDetails response to the HTTP context.
    /// </summary>
    /// <param name="httpContext">The HTTP context.</param>
    /// <param name="exception">The exception to handle.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A ValueTask representing the asynchronous operation, containing a boolean indicating whether the exception was handled.</returns>
    public async ValueTask<bool> TryHandleAsync(HttpContext httpContext, Exception exception, CancellationToken cancellationToken)
    {
        if (exception is OperationCanceledException && httpContext.RequestAborted.IsCancellationRequested)
        {
            _logger.LogInformation("HTTP request was cancelled by the client.");
            return false;
        }

        var mapping = MapException(exception);
        var correlationId = ProblemDetailsFactory.GetCorrelationId(httpContext);

        using (_logger.BeginScope(new Dictionary<string, object?>
            {
                [CorrelationConstants.LogPropertyName] = correlationId,
                ["ErrorCode"] = mapping.ErrorCode,
                ["HttpStatusCode"] = mapping.StatusCode
            }))
        {
            if (mapping.StatusCode >= StatusCodes.Status500InternalServerError)
            {
                _logger.LogError(exception, "Unhandled API exception. ErrorCode: {ErrorCode}, StatusCode: {StatusCode}.", mapping.ErrorCode, mapping.StatusCode);
            }
            else
            {
                _logger.LogWarning(exception, "API request failed. ErrorCode: {ErrorCode}, StatusCode: {StatusCode}.", mapping.ErrorCode, mapping.StatusCode);
            }
        }

        httpContext.Response.StatusCode = mapping.StatusCode;

        var problemDetails = ProblemDetailsFactory.Create(httpContext, mapping.StatusCode, mapping.Title, mapping.Detail, mapping.ErrorCode);

        return await _problemDetailsService.TryWriteAsync(new ProblemDetailsContext
            {
                HttpContext = httpContext,
                ProblemDetails = problemDetails,
                Exception = exception
            });
    }
    #endregion

    #endregion

    #endregion
}