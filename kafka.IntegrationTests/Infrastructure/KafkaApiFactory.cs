using System;
using System.Collections.Generic;
using System.Text;
using kafka.Api;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Mvc.Testing;
using Microsoft.Extensions.Configuration;

namespace kafka.IntegrationTests.Infrastructure;

public sealed class KafkaApiFactory : WebApplicationFactory<ApiAssemblyMarker>
{
    #region Constructor
    public KafkaApiFactory(string bootstrapServers, string mongoConnectionString, string mongoDatabaseName)
    {
        _bootstrapServers = bootstrapServers;
        _mongoConnectionString = mongoConnectionString;
        _mongoDatabaseName = mongoDatabaseName;
    }
    #endregion

    #region Properties

    #region Private
    private readonly string _bootstrapServers;
    private readonly string _mongoConnectionString;
    private readonly string _mongoDatabaseName;
    #endregion

    #endregion

    #region Methods

    #region Protected

    #region ConfigureWebHost
    /// <summary>
    /// Configures the web host for the integration tests, setting up the environment and application configuration.
    /// </summary>
    /// <param name="builder">The web host builder to configure.</param>
    protected override void ConfigureWebHost(IWebHostBuilder builder)
    {
        builder.UseEnvironment("IntegrationTesting");

        builder.ConfigureAppConfiguration((_, configurationBuilder) =>
        {
            configurationBuilder.Sources.Clear();
            configurationBuilder.AddInMemoryCollection(new Dictionary<string, string?>
            {
                ["Kafka:BootstrapServers"] = _bootstrapServers,
                ["Mongo:ConnectionString"] = _mongoConnectionString,
                ["Mongo:DatabaseName"] = _mongoDatabaseName,
                ["WorkerServices:AccountService:HealthUrl"] = "http://localhost:5101/health",
                ["WorkerServices:EmployeeService:HealthUrl"] = "http://localhost:5102/health",
                ["Serilog:MinimumLevel:Default"] = "Warning"
            });
        });
    }
    #endregion

    #endregion

    #endregion
}