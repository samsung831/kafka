using kafka.Shared.Constants;
using kafka.Shared.Observability;
using Microsoft.AspNetCore.Mvc;

namespace kafka.Api.ErrorHandling;

public static class ProblemDetailsFactory
{
    #region Methods

    #region Public

    #region Create
    /// <summary>
    /// Creates a ProblemDetails object with the specified parameters and adds the correlation ID to the extensions.
    /// </summary>
    /// <param name="httpContext">The HTTP context.</param>
    /// <param name="statusCode">The HTTP status code.</param>
    /// <param name="title">The title of the problem.</param>
    /// <param name="detail">The detail of the problem.</param>
    /// <param name="errorCode">The error code associated with the problem.</param>
    /// <returns>A ProblemDetails object representing the problem.</returns>
    public static ProblemDetails Create(HttpContext httpContext, int statusCode, string title, string detail, string errorCode)
    {
        var correlationId = GetCorrelationId(httpContext);

        var problemDetails = new ProblemDetails
        {
            Type = $"urn:problem-type:{errorCode}",
            Title = title,
            Status = statusCode,
            Detail = detail,
            Instance = httpContext.Request.Path
        };

        problemDetails.Extensions["errorCode"] = errorCode;

        problemDetails.Extensions["correlationId"] = correlationId;

        return problemDetails;
    }
    #endregion

    #region CreateValidationProblem
    /// <summary>
    /// Creates a ValidationProblemDetails object with the specified parameters and adds the correlation ID to the extensions.
    /// </summary>
    /// <param name="httpContext">The HTTP context.</param>
    /// <param name="errors">A dictionary containing the validation errors.</param>
    /// <returns>A ValidationProblemDetails object representing the validation problem.</returns>
    public static ValidationProblemDetails CreateValidationProblem(HttpContext httpContext, IDictionary<string, string[]> errors)
    {
        var correlationId = GetCorrelationId(httpContext);

        var problemDetails = new ValidationProblemDetails(errors)
        {
            Type = $"urn:problem-type:{ApiErrorCodes.ModelValidationFailed}",
            Title = "Request validation failed",
            Status = StatusCodes.Status400BadRequest,
            Detail = "One or more request values are invalid.",
            Instance = httpContext.Request.Path
        };

        problemDetails.Extensions["errorCode"] = ApiErrorCodes.ModelValidationFailed;

        problemDetails.Extensions["correlationId"] = correlationId;

        return problemDetails;
    }
    #endregion

    #region GetCorrelationId
    /// <summary>
    /// Retrieves the correlation ID from the HTTP context. If the correlation ID is not present, a new one is created.
    /// </summary>
    /// <param name="httpContext">The HTTP context.</param>
    /// <returns>The correlation ID.</returns>
    public static string GetCorrelationId(HttpContext httpContext)
    {
        if (httpContext.Items.TryGetValue(CorrelationConstants.HttpContextItemName, out var value) && value is string correlationId)
        {
            return correlationId;
        }

        return CorrelationId.Create();
    }
    #endregion

    #endregion

    #endregion
}