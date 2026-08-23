using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using System;
using System.Collections.Generic;
using System.Text;
using System.Text.Json;

namespace kafka.Shared.Health;

public static class HealthResponseWriter
{
    #region Methods

    #region Public

    #region WriteAsync
    /// <summary>
    /// Writes the health report as a JSON response to the HTTP context.
    /// </summary>
    /// <param name="httpContext">The HTTP context to write the response to.</param>
    /// <param name="report">The health report to write.</param>
    /// <returns>A task that represents the asynchronous write operation.</returns>
    public static async Task WriteAsync(HttpContext httpContext, HealthReport report)
    {
        httpContext.Response.ContentType = "application/json";

        var response = new
            {
                status = report.Status.ToString(),
                totalDuration = report.TotalDuration,
                checks = report.Entries.Select(
                    entry => new
                    {
                        name = entry.Key,
                        status = entry.Value.Status.ToString(),
                        description = entry.Value.Description,
                        duration = entry.Value.Duration,
                        data = entry.Value.Data
                    })
            };

        JsonSerializerOptions JsonOptions = new(JsonSerializerDefaults.Web)
        {
            WriteIndented = true
        };

        await httpContext.Response.WriteAsync(JsonSerializer.Serialize(response, JsonOptions));
    }
    #endregion

    #endregion

    #endregion
}