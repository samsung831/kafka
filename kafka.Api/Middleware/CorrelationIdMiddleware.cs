using kafka.Shared.Constants;
using kafka.Shared.Observability;

namespace kafka.Api.Middleware;

public sealed class CorrelationIdMiddleware
{
    #region Constructor
    public CorrelationIdMiddleware(RequestDelegate next, ILogger<CorrelationIdMiddleware> logger)
    {
        _next = next;
        _logger = logger;
    }
    #endregion

    #region Properties

    #region Private
    private readonly RequestDelegate _next;
    private readonly ILogger<CorrelationIdMiddleware> _logger;
    #endregion

    #endregion

    #region Methods

    #region Public

    #region InvokeAsync
    /// <summary>
    /// Middleware to handle correlation IDs for incoming HTTP requests.
    /// It checks for a correlation ID in the request headers, normalizes or creates one if not present, and adds it to the response headers.
    /// It also logs the start and completion of the HTTP request with the correlation ID included in the log scope.
    /// </summary>
    /// <param name="httpContext">The HTTP context for the current request.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    public async Task InvokeAsync(HttpContext httpContext)
    {
        var suppliedCorrelationId = httpContext.Request.Headers[CorrelationConstants.HttpHeaderName].FirstOrDefault();

        var correlationId = CorrelationId.NormalizeOrCreate(suppliedCorrelationId);

        httpContext.Items[CorrelationConstants.HttpContextItemName] = correlationId;

        httpContext.Response.OnStarting(static state =>
            {
                var context = (HttpContext)state;

                if (context.Items.TryGetValue(CorrelationConstants.HttpContextItemName, out var value) && value is string responseCorrelationId)
                {
                    context.Response.Headers[CorrelationConstants.HttpHeaderName] = responseCorrelationId;
                }

                return Task.CompletedTask;
            },
            httpContext);

        using (_logger.BeginScope(new Dictionary<string, object>
            {
                [CorrelationConstants.LogPropertyName] = correlationId
            }))
        {
            _logger.LogInformation("HTTP request started. Method: {HttpMethod}, Path: {RequestPath}.", httpContext.Request.Method, httpContext.Request.Path);

            await _next(httpContext);

            _logger.LogInformation("HTTP request completed. Method: {HttpMethod}, Path: {RequestPath}, StatusCode: {StatusCode}.",
                httpContext.Request.Method, httpContext.Request.Path, httpContext.Response.StatusCode);
        }
    }
    #endregion

    #endregion

    #endregion
}