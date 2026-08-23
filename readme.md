# Kafka project

A .NET 10 event-driven system that receives account and employee events through an HTTP API, publishes them to Apache Kafka, processes them in independent consumer services, stores them in MongoDB, and exposes a consolidated person through the same API.

## System overview

The solution contains the following projects:

- `kafka.Api`: receives arbitrary account and employee JSON and publishes events to Kafka. Also reads MongoDB and returns consolidated account and employment information. 
- `kafka.AcountService`: consumes account events from `topic.accounts` and stores account documents in MongoDB.
- `kafka.EmployeeService`: consumes employee events from `topic.employees` and stores employment documents in MongoDB.
- `kafka.Shared`: contains shared models, configuration, MongoDB infrastructure, validation, observability, health, and dead-letter models.

The account and employee documents are linked by:

```text
mappingFields.EmployeeId.groupId
```

One account may have multiple employment records, but only one employment may be active and non-deleted for the same `groupId` at a time.

## Technology stack

- .NET 10
- ASP.NET Core Web API
- Apache Kafka in KRaft mode
- MongoDB 8
- Polly retry policy
- Serilog structured logging
- OpenAPI and Swagger
- xUnit
- Docker

## Prerequisites

Install the following tools:

- .NET SDK 10
- Docker Desktop with Linux containers enabled
- Windows PowerShell

## Ports

| Component | Address |
|---|---|
| Kafka from the host | `localhost:29092` |
| MongoDB from the host | `localhost:27018` |
| AccountService health | `http://localhost:5101` |
| EmployeeService health | `http://localhost:5102` |
| kafka.Api | `http://localhost:5210` |
| kafka.PersonsApi | `http://localhost:5042` |

Inside Docker, the consumer services use:

```text
Kafka:   kafka:9092
MongoDB: mongodb:27017
```

## Kafka topics

The environment uses four Kafka topics:

```text
topic.accounts
topic.employees
topic.accounts.dlq
topic.employees.dlq
```

The two source topics carry account and employment events. The two DLQ topics contain messages that could not be processed after validation or persistence handling.

# Running the system

The simplified deployment runs Kafka, MongoDB, AccountService, and EmployeeService with Docker Compose. `kafka.Api` is started locally with `dotnet run`.

## 1. Validate the Docker Compose configuration

From the solution root:

```powershell
docker compose config
```

Resolve any YAML or environment-variable error before continuing.

## 2. Start Kafka, MongoDB, topic initialization, and both consumers

Run command:

```powershell
docker compose up -d
```

The expected startup flow is:

```text
Kafka and MongoDB start
Kafka becomes healthy
MongoDB becomes healthy
kafka-init creates all four topics and exits with code 0
AccountService starts
EmployeeService starts
```

A `kafka-init` container with exit code `0` is expected. It is a one-time initializer, not a long-running service.

## 3. Check container status

```powershell
docker compose ps
```

Expected services:

```text
kafka
mongodb
account-service
employee-service
```

Expected initializer state:

```text
kafka-init: exited with code 0
```

## 4. Check consumer logs

AccountService:

```powershell
docker compose logs --follow account-service
```

EmployeeService:

```powershell
docker compose logs --follow employee-service
```

Stop log following with `Ctrl+C`. This does not stop the containers.

## 5. Start the API locally

Open a new terminal in the solution root:

```powershell
dotnet run --project .\kafka.Api\kafka.Api.csproj
```

The default HTTP address is:

```text
http://localhost:5210
```

Swagger is available at:

```text
http://localhost:5210/swagger
```

# API endpoints

`kafka.Api` accepts arbitrary JSON as `JsonElement`. The API publishes the body to the appropriate Kafka topic. Business validation is performed later by the appropriate consumer.

## Publish an account event

```text
POST /api/events/accounts
```

Full local address:

```text
http://localhost:5210/api/events/accounts
```

The event is published to:

```text
topic.accounts
```

### Example account JSON

```json
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
```

Expected result:

```text
HTTP 202 Accepted
```

Example response shape:

```json
{
  "message": "Account event accepted for processing.",
  "correlationId": "readme-account-001",
  "topic": "topic.accounts",
  "partition": 0,
  "offset": 0
}
```

The actual partition and offset depend on the current Kafka topic state.

## Publish an employee event

```text
POST /api/events/employees
```

Full local address:

```text
http://localhost:5210/api/events/employees
```

The event is published to:

```text
topic.employees
```

### Example active employee JSON

The `groupId` must match the account event if both records represent the same person.

```json
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
```

Expected result:

```text
HTTP 202 Accepted
```

## Publish a historical employee event

Use a different `_id`, the same `groupId`, and `isActive: false`.

```json
{
  "_id": "64c3e0f5d1f4c2a1b2c3d4e6",
  "isActive": false,
  "isDeleted": false,
  "createdDate": "2025-01-01T08:00:00.000Z",
  "modifiedDate": "2025-12-31T16:00:00.000Z",
  "version": 25,
  "mappingFields": {
    "EmployeeId": {
      "groupId": "ABC123"
    }
  },
  "employmentData": {
    "employmentStatus": "Ended",
    "originalHireDate": "2025-01-01T00:00:00.000Z",
    "lastHireDate": "2025-01-01T00:00:00.000Z",
    "lastJobPositionChangeDate": "2025-06-01T00:00:00.000Z",
    "expiredContractDate": "2025-12-31T00:00:00.000Z"
  },
  "employeeContact": {
    "work": {
      "email": "historical@example.com",
      "mobile": ""
    }
  }
}
```

## Get a person by `groupId`

```text
GET /api/persons/{groupId}
```

Full local address:

```text
http://localhost:5210/api/persons/{groupId}
```

The response contains:

```text
Account
All matching employment records
```

Example response shape:

```json
{
  "account": {
    "_id": "64c3e0f5d1f4c2a1b2c3d4e5",
    "version": 48,
    "mappingFields": {
      "employeeId": {
        "groupId": "ABC123"
      }
    },
    "personalData": {
      "firstName": "Testo",
      "lastName": "Testic"
    }
  },
  "employees": [
    {
      "_id": "64c3e0f5d1f4c2a1b2c3d4e7",
      "version": 157,
      "isActive": true
    },
    {
      "_id": "64c3e0f5d1f4c2a1b2c3d4e6",
      "version": 25,
      "isActive": false
    }
  ]
}
```

If no account exists for the requested `groupId`, the API returns HTTP `404` with a Problem Details response.

## Filter by status

Full local address:

```text
http://localhost:5210/api/persons/ABC123?isActive=true&isDeleted=false
```

Optional query parameters:

```text
isActive
isDeleted
```

## Search by first and last name

```text
GET /api/persons/search?firstName=...&lastName=...
```

Example:

```text
http://localhost:5210/api/persons/search?firstName=Testo&lastName=Testic
```

With optional status filters:

```text
http://localhost:5210/api/persons/search?firstName=Testo&lastName=Testic&isActive=true&isDeleted=false
```

Both `firstName` and `lastName` are required.

# Sample data and Postman collection

## JSON Samples

The `samples` folder contains JSON sample files for testing:

- **Account samples**: JSON samples for account events to use with the account publishing endpoint
- **Employee samples**: JSON samples for employee events to use with the employee publishing endpoint

Use these samples to quickly test the API without manually constructing JSON payloads.

## Postman Collection

The `postman` folder contains a Postman collection JSON file that includes pre-configured requests for:

- **AddAccount**: Publish an account event to the API
- **AddEmployee**: Publish an employee event to the API
- **GetByGroupId**: Retrieve a person and their employment records by `groupId`
- **GetByGroupIDAndStatus**: Retrieve a person filtered by active/deleted status
- **Search**: Search for persons by first and last name
- **SearchWithStatus**: Search for persons with optional active/deleted status filters

Import the Postman collection file to quickly test all API endpoints with pre-configured requests and examples.

# Processing rules

## Idempotence and event versions

Each MongoDB document uses the event `_id` as its MongoDB `_id`.

The consumer applies version-aware persistence:

```text
No existing document      -> insert
Higher incoming version   -> update
Equal incoming version    -> ignore
Lower incoming version    -> ignore
```

An older event cannot overwrite a newer document.

## Active-employment constraint

The employee collection allows multiple historical employment records for the same `groupId`, but only one active and non-deleted employment.

The active record satisfies:

```text
isActive = true
isDeleted = false
```

A second active, non-deleted employment for the same `groupId` is treated as a persistence conflict and is sent to `topic.employees.dlq`.

## Retry behavior

MongoDB persistence is executed through a bounded Polly retry policy.

Default values:

```text
Maximum retries: 3
Initial delay: 500 milliseconds
Backoff: exponential
Jitter: enabled
```

Invalid JSON and deterministic validation errors are not retried because retrying the same payload cannot correct the message.

## Dead-letter handling

Messages are sent to a DLQ when they cannot be safely processed.

Examples:

```text
Malformed JSON
Missing groupId
Invalid ObjectId
Negative version
Missing required event sections
MongoDB failure after retries are exhausted
Duplicate active employment conflict
```

The source Kafka offset is committed only after:

```text
Successful persistence
Version-based ignore
Successful DLQ publication
```

If DLQ publication fails, the source offset is not committed.

# Testing dead-letter handling

## Send an invalid account event

This event is valid JSON but has an empty `groupId`.

```json
{
  "_id": "74c3e0f5d1f4c2a1b2c3d401",
  "isActive": true,
  "isDeleted": false,
  "createdDate": "2026-08-22T08:00:00Z",
  "modifiedDate": "2026-08-22T08:00:00Z",
  "version": 1,
  "mappingFields": {
    "EmployeeId": {
      "groupId": ""
    }
  },
  "names": {},
  "personalData": {
    "firstName": "Invalid",
    "lastName": "Account"
  }
}
```

## Inspect the account DLQ

Kafka images do not all place the console consumer under the same filename or path. The following command locates it first:

```powershell
docker exec kafka /opt/kafka/bin/kafka-console-consumer.sh `
--bootstrap-server localhost:9092 `
--topic topic.accounts.dlq `
--from-beginning
```

To inspect employee failures, replace:

```text
topic.accounts.dlq
```

with:

```text
topic.employees.dlq
```

# Structured logging and correlation IDs

Clients may supply:

```text
X-Correlation-ID
```

Example:

```powershell
-Headers @{
    "X-Correlation-ID" = "business-flow-001"
}
```

If the header is missing, `kafka.Api` generates a correlation ID.

The correlation ID is propagated through:

```text
HTTP request
Kafka source-message header
consumer logging scope
DLQ envelope and header
```

Search container logs by correlation ID:

```powershell
docker compose logs |
    Select-String "business-flow-001"
```

# OpenAPI and Swagger

When the API runs, interactive Swagger pages are available at:

```text
kafka.Api:
http://localhost:5210/swagger
```

Generated OpenAPI documents are available at:

```text
http://localhost:5210/openapi/v1.json
```

Swagger includes the producer endpoints, query endpoints, request examples, response models, status codes, and Problem Details responses.

# Health checks

## AccountService

```text
http://localhost:5101/health
```

## EmployeeService

```text
http://localhost:5102/health
```

## kafka.Api aggregate health

When both worker services are reachable at their configured health addresses:

```text
http://localhost:5210/health
```

The aggregate endpoint reports both AccountService and EmployeeService.

If `kafka.Api` runs locally while the workers run through Docker, these values should be configured:

```json
{
  "WorkerServices": {
    "AccountService": {
      "HealthUrl": "http://localhost:5101/health/ready"
    },
    "EmployeeService": {
      "HealthUrl": "http://localhost:5102/health/ready"
    }
  }
}
```

# MongoDB inspection

Open MongoDB shell:

```powershell
docker exec -it mongodb mongosh `
    --username root `
    --password root-password `
    --authenticationDatabase admin
```

Select the application database:

```javascript
use persons
```

List accounts:

```javascript
db.accounts.find().pretty()
```

List employees:

```javascript
db.employees.find().pretty()
```

Find a person by `groupId` in the account collection:

```javascript
db.accounts.findOne({
  "mappingFields.EmployeeId.groupId": "ABC123"
})
```

Find all employments for the same person:

```javascript
db.employees.find({
  "mappingFields.EmployeeId.groupId": "ABC123"
}).pretty()
```

# Running tests

## Run all tests

```powershell
dotnet test .\kafka.slnx --configuration Debug
```

## Run unit tests only

```powershell
dotnet test .\kafka.UnitTests\kafka.UnitTests.csproj
```

## Run integration tests

Docker must be running because the integration tests use Kafka and MongoDB Testcontainers.

```powershell
dotnet test `
    .\kafka.IntegrationTests\kafka.IntegrationTests.csproj `
    --configuration Debug `
    --logger "console;verbosity=detailed"
```

The Testcontainers tests do not require the normal Compose stack. Stop the Compose workers first if shared host resources or test assumptions conflict.

# Rebuilding after source changes

If only AccountService changes:

```powershell
docker compose build account-service
docker compose up -d --no-deps account-service
```

If only EmployeeService changes:

```powershell
docker compose build employee-service
docker compose up -d --no-deps employee-service
```

If `kafka.Shared` changes, rebuild both workers:

```powershell
docker compose build account-service employee-service
docker compose up -d --no-deps account-service employee-service
```

# Stopping and cleanup

Stop containers while preserving them and their volumes:

```powershell
docker compose stop
```

Start them again:

```powershell
docker compose start
```

Remove containers while preserving named volumes:

```powershell
docker compose down
```

Remove containers and all Kafka and MongoDB data:

```powershell
docker compose down -v
```

The last command permanently deletes Kafka messages, consumer offsets, MongoDB documents, and DLQ records.

# Troubleshooting

## Docker build fails with `FallbackPackagePathResolver`

The Linux Docker build may have received Windows-generated `obj/project.assets.json` files.

Ensure `.dockerignore` excludes all `bin` and `obj` directories:

```dockerignore
**/bin
**/bin/**
**/obj
**/obj/**
```

Clear the Docker builder cache and rebuild:

```powershell
docker builder prune --force
docker compose build --no-cache account-service employee-service
```

## Consumer cannot connect to Kafka

Inspect container configuration:

```powershell
docker compose exec account-service env |
    Select-String "Kafka__"
```

The container must use:

```text
Kafka__BootstrapServers=kafka:9092
```

It must not use `localhost:29092` from inside Docker.

## Consumer cannot connect to MongoDB

Inspect:

```powershell
docker compose exec account-service env |
    Select-String "Mongo__"
```

The container must use:

```text
mongodb://root:root-password@mongodb:27017/?authSource=admin
```

It must not use `localhost:27018` from inside Docker.

## A second active employment goes to DLQ

This is expected when a different employee document has:

```text
same groupId
isActive = true
isDeleted = false
```

Use `isActive: false` for historical employment records, or update the existing active record using the same `_id` and a higher `version`.

## Older event does not update MongoDB

This is expected. The consumer ignores events whose version is equal to or lower than the stored document version.

Use a higher `version` value for a valid update.
