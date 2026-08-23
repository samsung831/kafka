using kafka.Api.Middleware;
using kafka.Shared.Constants;
using kafka.Shared.Observability;
using kafka.UnitTests.Helpers;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Http.Features;
using Microsoft.Extensions.Logging.Abstractions;

namespace kafka.UnitTests.Middleware;

public sealed class CorrelationIdMiddlewareTests
{
    #region Methods

    #region Public

    #region InvokeAsync_WithSuppliedCorrelationId_NormalizesStoresAndReturnsIt
    /// <summary>
    /// Tests that when a correlation ID is supplied in the request headers, the middleware normalizes it (trims whitespace),
    /// stores it in the HttpContext.Items, and returns it in the response headers.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task InvokeAsync_WithSuppliedCorrelationId_NormalizesStoresAndReturnsIt()
    {
        var context = new DefaultHttpContext();
        var responseFeature = new CallbackResponseFeatureHelper();
        context.Features.Set<IHttpResponseFeature>(responseFeature);
        context.Request.Headers[CorrelationConstants.HttpHeaderName] = " request-001 ";
        string? downstreamCorrelationId = null;
        var middleware = new CorrelationIdMiddleware(next: httpContext =>
            {
                downstreamCorrelationId = Assert.IsType<string>(httpContext.Items[CorrelationConstants.HttpContextItemName]);
                return Task.CompletedTask;
            },
            NullLogger<CorrelationIdMiddleware>.Instance);

        await middleware.InvokeAsync(context);
        await responseFeature.RunOnStartingAsync();

        Assert.Equal("request-001", downstreamCorrelationId);
        Assert.Equal("request-001", context.Response.Headers[CorrelationConstants.HttpHeaderName].ToString());
    }
    #endregion

    #region InvokeAsync_WithMissingCorrelationId_GeneratesAndPropagatesId
    /// <summary>
    /// Tests that when no correlation ID is supplied in the request headers, the middleware generates a new correlation ID,
    /// stores it in the HttpContext.Items, and returns it in the response headers.
    /// </summary>
    /// <returns>A task that represents the asynchronous operation.</returns>
    [Fact]
    public async Task InvokeAsync_WithMissingCorrelationId_GeneratesAndPropagatesId()
    {
        var context = new DefaultHttpContext();
        var responseFeature = new CallbackResponseFeatureHelper();
        context.Features.Set<IHttpResponseFeature>(responseFeature);
        var middleware = new CorrelationIdMiddleware(next: _ => Task.CompletedTask, NullLogger<CorrelationIdMiddleware>.Instance);

        await middleware.InvokeAsync(context);
        await responseFeature.RunOnStartingAsync();

        var correlationId = Assert.IsType<string>(context.Items[CorrelationConstants.HttpContextItemName]);
        Assert.Equal(32, correlationId.Length);
        Assert.Equal(correlationId, context.Response.Headers[CorrelationConstants.HttpHeaderName].ToString());
    }
    #endregion

    #endregion

    #endregion
}
