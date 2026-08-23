using kafka.Api.ErrorHandling;
using kafka.Shared.Constants;
using kafka.Shared.Exceptions;
using kafka.Shared.Observability;
using kafka.UnitTests.Helpers;
using Microsoft.AspNetCore.Diagnostics;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Logging.Abstractions;

namespace kafka.UnitTests.ErrorHandling;

public sealed class GlobalExceptionHandlerTests
{
    #region Methods

    #region Public

    #region TryHandleAsync_MapsExceptionToProblemDetails
    /// <summary>
    /// Tests that the TryHandleAsync method correctly maps different exception types to the expected HTTP status code
    /// and error code in the ProblemDetails response.
    /// </summary>
    /// <param name="exceptionType">The type of exception to test.</param>
    /// <param name="expectedStatusCode">The expected HTTP status code.</param>
    /// <param name="expectedErrorCode">The expected error code.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Theory]
    [InlineData("validation", StatusCodes.Status400BadRequest, "request.validation_failed")]
    [InlineData("not-found", StatusCodes.Status404NotFound, "resource.not_found")]
    [InlineData("unexpected", StatusCodes.Status500InternalServerError, "server.internal_error")]
    public async Task TryHandleAsync_MapsExceptionToProblemDetails(string exceptionType, int expectedStatusCode, string expectedErrorCode)
    {
        var writer = new RecordingProblemDetailsServiceHelper();
        var handler = new GlobalExceptionHandler(NullLogger<GlobalExceptionHandler>.Instance, writer);
        var context = new DefaultHttpContext();
        context.Items[CorrelationConstants.HttpContextItemName] = "request-001";
        Exception exception = exceptionType switch
        {
            "validation" => new RequestValidationException("Invalid request."),
            "not-found" => new ResourceNotFoundException("Missing person."),
            _ => new InvalidOperationException("Unexpected.")
        };

        var handled = await handler.TryHandleAsync(context, exception, CancellationToken.None);

        Assert.True(handled);
        Assert.Equal(expectedStatusCode, context.Response.StatusCode);
        var problemDetails = Assert.IsType<ProblemDetails>(writer.Context!.ProblemDetails);
        Assert.Equal(expectedStatusCode, problemDetails.Status);
        Assert.Equal(expectedErrorCode, problemDetails.Extensions["errorCode"]);
        Assert.Equal("request-001", problemDetails.Extensions["correlationId"]);
    }
    #endregion

    #region TryHandleAsync_WhenClientCancelled_DoesNotWriteProblemDetails
    /// <summary>
    /// Tests that the TryHandleAsync method does not write ProblemDetails when the client has cancelled the request.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task TryHandleAsync_WhenClientCancelled_DoesNotWriteProblemDetails()
    {
        var writer = new RecordingProblemDetailsServiceHelper();
        var handler = new GlobalExceptionHandler(NullLogger<GlobalExceptionHandler>.Instance, writer);
        using var cancellationSource = new CancellationTokenSource();
        cancellationSource.Cancel();
        var context = new DefaultHttpContext { RequestAborted = cancellationSource.Token };

        var handled = await handler.TryHandleAsync(context, new OperationCanceledException(), CancellationToken.None);

        Assert.False(handled);
        Assert.Null(writer.Context);
    }
    #endregion

    #endregion

    #endregion
}
