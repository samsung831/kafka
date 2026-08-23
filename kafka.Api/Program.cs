using Confluent.Kafka;
using kafka.Api.Configuration;
using kafka.Api.ErrorHandling;
using kafka.Api.Health;
using kafka.Api.Kafka;
using kafka.Api.Middleware;
using kafka.Api.OpenApi;
using kafka.Api.Services;
using kafka.Shared.Configuration;
using kafka.Shared.Health;
using kafka.Shared.MongoDB;
using Microsoft.AspNetCore.Diagnostics.HealthChecks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;
using Serilog;

var builder = WebApplication.CreateBuilder(args);

builder.Services.AddSerilog(
    (services, loggerConfiguration) =>
    {
        loggerConfiguration
        .ReadFrom.Configuration(builder.Configuration)
        .ReadFrom.Services(services)
        .Enrich.FromLogContext()
        .Enrich.WithProperty("Service", "kafka.Api");
    });

// Add services to the container.
builder.Services.AddControllers();

// Learn more about configuring OpenAPI at https://aka.ms/aspnet/openapi
builder.Services.AddOpenApi("v1",
    options =>
    {
        options.AddOperationTransformer<EventExamplesOperationTransformer>();
    });

builder.Services.AddProblemDetails();

builder.Services.AddExceptionHandler<GlobalExceptionHandler>();

builder.Services.Configure<ApiBehaviorOptions>(
    options =>
    {
        options.InvalidModelStateResponseFactory =
            actionContext =>
            {
                var errors = actionContext.ModelState
                    .Where(entry => entry.Value?.Errors.Count > 0)
                    .ToDictionary(entry => entry.Key, entry => entry.Value!.Errors
                    .Select(error => string.IsNullOrWhiteSpace(error.ErrorMessage) ? "The supplied value is invalid." : error.ErrorMessage)
                    .ToArray());

                var problemDetails = ProblemDetailsFactory.CreateValidationProblem(actionContext.HttpContext, errors);

                return new BadRequestObjectResult(problemDetails)
                {
                    ContentTypes =
                    {
                        "application/problem+json"
                    }
                };
            };
    });

builder.Services
    .AddOptions<KafkaOptions>()
    .Bind(builder.Configuration.GetSection(KafkaOptions.SectionName))
    .Validate(
        options => !string.IsNullOrWhiteSpace(options.BootstrapServers),
            "Kafka BootstrapServers is required.")
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

builder.Services.AddSingleton<IProducer<string, string>>(
    serviceProvider =>
{
    var kafkaOptions = serviceProvider
        .GetRequiredService<IOptions<KafkaOptions>>()
        .Value;

    var config = new ProducerConfig
    {
        BootstrapServers = kafkaOptions.BootstrapServers,
        Acks = Acks.All,
        EnableIdempotence = true
    };

    return new ProducerBuilder<string, string>(config).Build();
});

builder.Services.AddSingleton<IEventPublisher, KafkaEventPublisher>();
builder.Services.AddSingleton<MongoContext>();
builder.Services.AddSingleton<MongoIndexInitializer>();
builder.Services.AddScoped<IPersonService, PersonService>();

builder.Services
    .AddOptions<WorkerServicesOptions>()
    .Bind(builder.Configuration.GetSection(WorkerServicesOptions.SectionName))
    .Validate(
        options =>
            Uri.TryCreate(options.AccountService.HealthUrl, UriKind.Absolute, out _),
        "AccountService HealthUrl must be a valid absolute URL.")
    .Validate(
        options =>
            Uri.TryCreate(options.EmployeeService.HealthUrl, UriKind.Absolute, out _),
        "EmployeeService HealthUrl must be a valid absolute URL.")
    .ValidateOnStart();

builder.Services.AddHttpClient("worker-health",
    client =>
    {
        client.Timeout = TimeSpan.FromSeconds(3);
    });

builder.Services
    .AddHealthChecks()
    .AddCheck<AccountServiceHealthCheck>(
        name: "account-service",
        failureStatus: HealthStatus.Unhealthy,
        tags: new[] { "ready" })
    .AddCheck<EmployeeServiceHealthCheck>(
        name: "employee-service",
        failureStatus: HealthStatus.Unhealthy,
        tags: new[] { "ready" });

var app = builder.Build();

using (var scope = app.Services.CreateScope())
{
    var indexInitializer = scope.ServiceProvider.GetRequiredService<MongoIndexInitializer>();

    await indexInitializer.CreateIndexesAsync();
}

app.UseExceptionHandler();

app.UseMiddleware<CorrelationIdMiddleware>();

app.MapHealthChecks("/health",
    new HealthCheckOptions
    {
        Predicate = registration => registration.Tags.Contains("ready"),
        ResponseWriter = HealthResponseWriter.WriteAsync
    });

app.MapOpenApi();

app.UseSwaggerUI(
    options =>
    {
        options.SwaggerEndpoint("/openapi/v1.json", "Kafka Event Producer API v1");
        options.RoutePrefix = "swagger";
        options.DocumentTitle = "Kafka Event Producer API";
        options.DisplayRequestDuration();
        options.EnableTryItOutByDefault();
        options.DefaultModelsExpandDepth(2);
        options.DefaultModelExpandDepth(2);
    });

app.MapControllers();

app.Run();
