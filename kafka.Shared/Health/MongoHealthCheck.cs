using System;
using System.Collections.Generic;
using System.Text;
using kafka.Shared.MongoDB;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using MongoDB.Bson;

namespace kafka.Shared.Health;

public sealed class MongoHealthCheck : IHealthCheck
{
    #region Constructor
    public MongoHealthCheck(MongoContext mongoContext)
    {
        _mongoContext = mongoContext;
    }
    #endregion

    #region Properties

    #region Private
    private readonly MongoContext _mongoContext;
    #endregion

    #endregion

    #region Methods

    #region Public

    #region CheckHealthAsync
    /// <summary>
    /// Checks the health of the MongoDB connection by sending a ping command to the database.
    /// </summary>
    /// <param name="context">The health check context.</param>
    /// <param name="cancellationToken">A cancellation token that can be used to cancel the health check.</param>
    /// <returns>A task that represents the asynchronous operation, containing the health check result.</returns>
    public async Task<HealthCheckResult> CheckHealthAsync(HealthCheckContext context, CancellationToken cancellationToken = default)
    {
        try
        {
            var result = await _mongoContext.Database.RunCommandAsync<BsonDocument>(new BsonDocument("ping", 1), cancellationToken: cancellationToken);

            if (!result.TryGetValue("ok", out var okValue) || okValue.ToDouble() != 1)
            {
                return HealthCheckResult.Unhealthy("MongoDB ping returned an unsuccessful result.");
            }

            return HealthCheckResult.Healthy("MongoDB is available.");
        }
        catch (Exception exception)
        {
            return HealthCheckResult.Unhealthy("MongoDB is unavailable.", exception);
        }
    }
    #endregion

    #endregion

    #endregion
}