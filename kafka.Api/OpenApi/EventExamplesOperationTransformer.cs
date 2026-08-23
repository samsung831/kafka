using Microsoft.AspNetCore.OpenApi;
using Microsoft.OpenApi;
using System.Text.Json.Nodes;

namespace kafka.Api.OpenApi;

public sealed class EventExamplesOperationTransformer : IOpenApiOperationTransformer
{
    #region Properties

    #region Private
    private const string AccountExample =
        """
        {
          "_id": "64c3e0f5d1f4c2a1b2c3d4e5",
          "isActive": true,
          "isDeleted": false,
          "createdDate": "2026-06-20T16:21:01.742Z",
          "modifiedDate": "2026-06-20T16:21:01.742Z",
          "version": 48,
          "mappingFields": {
            "EmployeeId": {
              "groupId": "ABC123"
            }
          },
          "names": {},
          "address": {
            "type": null,
            "country": "HR",
            "state": "ISTARSKA",
            "city": "LABIN",
            "zipCode": "52220",
            "address": "RAVNI 16"
          },
          "personalData": {
            "age": 61,
            "birthDate": "1964-01-01T00:00:00.000Z",
            "firstName": "Testo",
            "lastName": "Testic",
            "gender": "Z"
          },
          "employeeContact": {
            "private": {
              "email": null,
              "mobile": "+385 98 123 456",
              "countryCode": "+385",
              "country": "HR"
            }
          }
        }
        """;

    private const string EmployeeExample =
        """
        {
          "_id": "64c3e0f5d1f4c2a1b2c3d4e7",
          "isActive": true,
          "isDeleted": false,
          "createdDate": "2026-03-27T13:44:05.263Z",
          "modifiedDate": "2026-03-27T13:44:05.263Z",
          "version": 157,
          "mappingFields": {
            "EmployeeId": {
              "groupId": "ABC123"
            }
          },
          "employmentData": {
            "employmentStatus": "Working",
            "originalHireDate": "2025-11-24T00:00:00.000Z",
            "lastHireDate": "2025-11-24T00:00:00.000Z",
            "lastJobPositionChangeDate": "2025-11-24T00:00:00.000Z",
            "expiredContractDate": null
          },
          "employeeContact": {
            "work": {
              "email": "testo.testic@example.com",
              "mobile": ""
            }
          }
        }
        """;
    #endregion

    #endregion

    #region Methods

    #region Private

    #region AddCorrelationHeader
    /// <summary>
    /// Adds a correlation header parameter to the OpenAPI operation if it doesn't already exist. This header is used for tracking requests across services.
    /// </summary>
    /// <param name="operation">The OpenAPI operation to which the correlation header will be added.</param>
    private static void AddCorrelationHeader(OpenApiOperation operation)
    {
        operation.Parameters ??= new List<IOpenApiParameter>();

        var correlationHeaderAlreadyExists = operation.Parameters.Any(parameter => string.Equals(parameter.Name, "X-Correlation-ID"));

        if (correlationHeaderAlreadyExists)
        {
            return;
        }

        operation.Parameters.Add(new OpenApiParameter
            {
                Name = "X-Correlation-ID",
                In = ParameterLocation.Header,
                Required = false,
                Description = "Optional correlation identifier. The API generates one when it is not supplied.",
                Schema = new OpenApiSchema
                {
                    Type = JsonSchemaType.String,
                    MaxLength = 128
                },
                Example = "openapi-test-001"
            });
    }
    #endregion

    #endregion

    #region Public

    #region TransformAsync
    /// <summary>
    /// Transforms the OpenAPI operation by adding example request bodies for specific endpoints and adding a correlation header parameter.
    /// </summary>
    /// <param name="operation">The OpenAPI operation to transform.</param>
    /// <param name="context">The context for the OpenAPI operation transformation.</param>
    /// <param name="cancellationToken">A token to monitor for cancellation requests.</param>
    /// <returns>A task that represents the asynchronous operation.</returns>
    public Task TransformAsync(OpenApiOperation operation, OpenApiOperationTransformerContext context, CancellationToken cancellationToken)
    {
        var exampleJson = context.Description.RelativePath switch
        {
            "api/events/accounts" => AccountExample,
            "api/events/employees" => EmployeeExample,
            _ => null
        };

        if (exampleJson is not null && operation.RequestBody?.Content.TryGetValue("application/json", out var mediaType) == true)
        {
            mediaType.Example = JsonNode.Parse(exampleJson);
        }

        AddCorrelationHeader(operation);

        return Task.CompletedTask;
    }
    #endregion

    #endregion

    #endregion
}
