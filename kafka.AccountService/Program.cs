using kafka.AccountService.Consumers;
using kafka.AccountService.Health;
using kafka.Shared.Configuration;
using kafka.Shared.Health;
using kafka.Shared.MongoDB;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Serilog;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSerilog(
    (services, loggerConfiguration) =>
    {
        loggerConfiguration
        .ReadFrom.Configuration(builder.Configuration)
        .ReadFrom.Services(services)
        .Enrich.FromLogContext()
        .Enrich.WithProperty(
        "Service",
        "kafka.AccountService");
    });

builder.Services
    .AddOptions<KafkaOptions>()
    .Bind(builder.Configuration.GetSection(KafkaOptions.SectionName))
    .Validate(
        options => !string.IsNullOrWhiteSpace(options.BootstrapServers),
            "Kafka BootstrapServers is required.")
    .Validate(
        options => !string.IsNullOrWhiteSpace(options.GroupId),
            "Kafka GroupId is required.")
    .Validate(
        options => !string.IsNullOrWhiteSpace(options.Topic),
            "Kafka Topic is required.")
    .ValidateOnStart();

builder.Services
    .AddOptions<MongoOptions>()
    .Bind(builder.Configuration.GetSection(MongoOptions.SectionName))
    .Validate(
        options => !string.IsNullOrWhiteSpace(options.ConnectionString),
            "Mongo ConnectionString is required.")
    .Validate(
        options => !string.IsNullOrWhiteSpace(options.DatabaseName),
            "Mongo DatabaseName is required.")
    .ValidateOnStart();

builder.Services.AddSingleton<MongoContext>();
builder.Services.AddSingleton<WorkerHealthState>();
builder.Services.AddHostedService<AccountConsumerWorker>();

builder.Services.AddHealthChecks()
    .AddCheck<AccountWorkerHealthCheck>(
        name: "account-worker",
        failureStatus: HealthStatus.Unhealthy,
        tags: new[] { "ready" })
    .AddCheck<KafkaHealthCheck>(
        name: "kafka",
        failureStatus: HealthStatus.Unhealthy,
        tags: new[] { "ready" })
    .AddCheck<MongoHealthCheck>(
        name: "mongodb",
        failureStatus: HealthStatus.Unhealthy,
        tags: new[] { "ready" });

var app = builder.Build();

app.MapHealthChecks("/health",
    new HealthCheckOptions
    {
        Predicate = registration => registration.Tags.Contains("ready"),
        ResponseWriter = HealthResponseWriter.WriteAsync
    });

app.Run();
