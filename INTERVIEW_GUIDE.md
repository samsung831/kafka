# Interview Guide: .NET 10, Kafka, and MongoDB Person System

This guide explains the **current solution**, not an idealized implementation. It connects the assignment to the code, explains how important mechanisms work, and provides answers you can adapt in an interview.

Suggested answers describe defensible technical rationale inferred from the implementation. They are not a record of your original intentions: use them only if they match your understanding.

For detailed startup commands and example payloads, see [readme.md](readme.md). This document focuses on understanding and explaining the design.

## Contents

1. [The short system explanation](#1-the-short-system-explanation)
2. [Architecture and project boundaries](#2-architecture-and-project-boundaries)
3. [kafka.Api](#3-kafkaapi)
4. [kafka.AccountService](#4-kafkaaccountservice)
5. [kafka.EmployeeService](#5-kafkaemployeeservice)
6. [kafka.Shared and the domain models](#6-kafkashared-and-the-domain-models)
7. [Flexible names and serialization](#7-flexible-names-and-serialization)
8. [Kafka ordering, delivery, and scaling](#8-kafka-ordering-delivery-and-scaling)
9. [Version-aware persistence](#9-version-aware-persistence)
10. [MongoDB schema, indexes, and active employment](#10-mongodb-schema-indexes-and-active-employment)
11. [Failures, retries, and dead letters](#11-failures-retries-and-dead-letters)
12. [Observability, errors, health, and configuration](#12-observability-errors-health-and-configuration)
13. [Docker and local development](#13-docker-and-local-development)
14. [The two test projects](#14-the-two-test-projects)
15. [Assignment coverage and differences](#15-assignment-coverage-and-differences)
16. [Scenario walkthroughs](#16-scenario-walkthroughs)
17. [Interview questions and suggested answers](#17-interview-questions-and-suggested-answers)
18. [Production improvements and final checklist](#18-production-improvements-and-final-checklist)

---

## 1. The short system explanation

### A 30-second answer

> The system receives account and employment events through a .NET 10 API and publishes the original JSON to two Kafka topics. Independent consumers validate the messages and persist account and employment documents in MongoDB. The read API combines them through `mappingFields.EmployeeId.groupId`. Writes use the message `_id` and a version condition, so duplicate or older events cannot overwrite newer state. A partial unique MongoDB index prevents more than one active, non-deleted employment for a person. The solution also includes structured logging, correlation IDs, health checks, retries, dead-letter topics, and unit and integration tests.

### A two-minute explanation

The two event streams describe different aspects of the same person:

- **Account:** personal data, address, and private contact information.
- **Employee:** a particular employment, its status, dates, and work contact.
- **`groupId`:** the business identity joining a person's account and employment records.
- **`_id`:** the identity of one document, not the join key.
- **`version`:** the source's revision number for that document.

The API waits for Kafka publication before returning `202 Accepted`, but it does **not** wait for MongoDB processing. Consequently, the read side is eventually consistent. An employment can arrive before the corresponding account without breaking persistence.

The consumers commit Kafka offsets only after persistence succeeds, an obsolete event is safely ignored, or dead-letter publication succeeds. Reprocessing after a crash is possible, so database idempotency is required even though the Kafka producers enable idempotence.

### The most important distinctions

| Concept | Meaning in this solution |
|---|---|
| Accepted | Kafka publication succeeded; business processing may still fail later. |
| Account | A person's general data document. |
| Employee | One employment document; multiple employment `_id` values can share one `groupId`. |
| Current state | The latest accepted document revision, not a complete event history. |
| Active employment | `isActive = true` **and** `isDeleted = false` for the unique index. |
| At-least-once processing | A record can be processed again if its offset was not committed. |
| Idempotent persistence | Replaying the same document version does not change the stored state. |
| Eventual consistency | The combined read view may temporarily contain only part of the latest information. |

## 2. Architecture and project boundaries

### Logical flow

Client → `kafka.Api` → `topic.accounts` → `kafka.AccountService` → MongoDB accounts.

Client → `kafka.Api` → `topic.employees` → `kafka.EmployeeService` → MongoDB employees.

Read client → `kafka.Api` → both MongoDB collections → consolidated response.

Each worker also publishes rejected messages to its corresponding `.dlq` topic.

### Every project

| Project | Responsibility | Why it is separate |
|---|---|---|
| `kafka.Api` | HTTP ingestion, Kafka publishing, person queries, error responses, OpenAPI, and aggregate worker health. | Keeps transport and the client-facing contract separate from asynchronous processing. |
| `kafka.AccountService` | Consumes accounts, validates account documents, writes account state, initializes account indexes. | Account processing has an independent consumer group and deployable lifecycle. |
| `kafka.EmployeeService` | Consumes employments, validates employment documents, writes employment state, initializes employee indexes. | Employment rules and failures are isolated from account processing. |
| `kafka.Shared` | Models, configuration, validation, serialization, MongoDB infrastructure, common consumer behavior, health, observability, exceptions, and dead-letter envelope. | Reuses contracts and important correctness mechanisms instead of copying them. |
| `kafka.UnitTests` | Fast isolated tests for validation, mapping, controllers, publishing, serialization, error handling, health, and writer recovery branches. | Supports rapid feedback without real Kafka or MongoDB. |
| `kafka.IntegrationTests` | Tests using real Kafka and MongoDB containers plus an in-process API and actual consumer workers. | Verifies cross-component behavior that substitutes cannot prove. |

All six projects target `net10.0`. The solution path spells the account folder `kafka.AcCountService`, while the physical directory and Dockerfile use `kafka.AccountService`. Windows generally tolerates this casing difference; Linux tooling may not. This is a portability detail, not an architectural distinction.

### What architectural style is this?

It is an **event-driven system with independently hosted consumer components** and a combined producer/read API. The write and read responsibilities are separated in a CQRS-like way, but there is no full CQRS framework or independently maintained person projection.

Do not describe it as event sourcing: MongoDB holds the latest snapshot for each `_id`, rather than a persisted sequence of every revision.

Do not overstate microservice independence: both workers use the same MongoDB database, the API reads both collections directly, and all applications share the same library. This is pragmatic for the assignment but creates schema and deployment coupling.

> Suggested rationale: “I separated the two asynchronous processing responsibilities, while keeping the API and shared infrastructure small enough for the assignment. I would introduce stronger service-owned data boundaries only when the operational or organizational requirements justify the extra complexity.”

## 3. kafka.Api

Key sources: [Program.cs](kafka.Api/Program.cs), [EventsController](kafka.Api/Controllers/EventsController.cs), [KafkaEventPublisher](kafka.Api/Kafka/KafkaEventPublisher.cs), [PersonsController](kafka.Api/Controllers/PersonsController.cs), and [PersonService](kafka.Api/Services/PersonService.cs).

### 3.1 Why two ingestion endpoints?

The implementation exposes:

- `POST /api/events/accounts` → `topic.accounts`.
- `POST /api/events/employees` → `topic.employees`.

This keeps the public contract business-oriented. Clients select an event category, not an arbitrary infrastructure topic. `KafkaEventPublisher` also restricts publication to the two supported topics.

Separate routes are easy to discover in Swagger and could later receive different authorization or rate-limit policies. Those policies are **not currently implemented**.

A generic route would be reasonable for a general Kafka gateway, but that is not this API's purpose.

### 3.2 Why JsonElement instead of AccountDocument in the POST action?

The assignment explicitly requires permissive ingestion. Both actions accept `JsonElement`; they do not run the domain validators.

The publisher uses `GetRawText()` to forward the payload without mapping it to a typed document or reconstructing its fields. Missing properties, an unexpected structure, or even valid scalar JSON can reach Kafka and be rejected by the consumer later.

This does **not** mean malformed JSON text is accepted. ASP.NET Core still needs to parse the request body, and the endpoint consumes `application/json`.

The publisher makes a best-effort inspection of `mappingFields.EmployeeId.groupId` only to obtain the Kafka key. Missing, non-object parent properties or a non-string value produce a null key rather than a domain validation error.

> Suggested answer: “The ingestion boundary validates the HTTP/JSON transport, while the owning consumer validates the domain structure. That satisfies the task and keeps the producer independent of account and employment processing rules.”

### 3.3 What does 202 mean?

The controller awaits `ProduceAsync` and then returns the topic, partition, offset, and correlation ID.

`202 Accepted` means the event has been published for asynchronous processing. It does not mean that the account or employment is valid, stored, or already visible through GET.

There is no implemented status endpoint tracking a published event to its final outcome. The correlation ID and logs help investigation, but are not a durable status resource.

### 3.4 Read endpoints and exact filter behavior

The routes are:

- `GET /api/persons/{groupId}`.
- `GET /api/persons/search?firstName=...&lastName=...`.

The implementation exposes **four independent status parameters**:

| Parameter | Applies to | Omitted behavior |
|---|---|---|
| `accountIsActive` | Account documents | Includes active and inactive accounts. |
| `accountIsDeleted` | Account documents | Selects non-deleted accounts only. |
| `employmentIsActive` | Employment documents | Includes active and inactive employments. |
| `employmentIsDeleted` | Employment documents | Selects non-deleted employments only. |

For example, `/api/persons/ABC123?accountIsActive=true&employmentIsActive=false` selects an active account and its inactive, non-deleted employments.

**Assignment difference:** the requested parameter names were `isActive` and `isDeleted`. They are not exposed as aliases. A client using only those names does not activate the intended filters in these actions. The four-parameter design is more expressive, but strict assignment compatibility would require documented aliases or a contract change.

Passing `accountIsDeleted=true` selects deleted accounts; it does not mean “include both deleted and non-deleted.” The same applies to employments. There is no “include all deletion states” switch.

The controllers trim `groupId`, `firstName`, and `lastName`. Both names are required for search. Status values bind as nullable booleans; invalid boolean text yields a request validation response.

### 3.5 How does the join work?

For one person:

1. Find the first account matching `groupId` and account filters.
2. If none matches, return no person; the controller produces `404`.
3. Query employments matching the same `groupId` and employment filters.
4. Map the account and all matching employments to response DTOs.

If there are no matching employments, the response still contains the account and an **empty `employees` array**. It is not necessarily an account-only JSON object with the property omitted.

For name search:

1. Find matching accounts by exact first and last name plus account filters.
2. Extract distinct account `groupId` values.
3. Issue one employment query using `$in` for those identifiers plus employment filters.
4. Group the result in memory and attach the matching employment arrays.

This avoids an N+1 employment query per account. It is still two database queries, not a MongoDB `$lookup` aggregation or a transactional snapshot. Changes between the reads can create a temporarily mixed view.

Search returns `200` with an empty array when nothing matches. There is no explicit sorting, pagination, fuzzy search, or application-configured case-insensitive collation. With normal default collection behavior, matching is case-sensitive exact equality.

### 3.6 Why DTOs?

`PersonResponseDto` contains account and employment DTOs rather than exposing MongoDB documents directly.

Mapping deliberately omits database IDs, mapping identifiers, versions, and document audit dates from the public account/employment DTOs. It keeps relevant status and business fields. Employment business dates such as hire dates are still exposed.

Benefits: persistence metadata does not automatically become an API contract, and response shape can evolve separately. Costs: mapping code must be maintained and consumers cannot identify a particular employment through an exposed `_id` in the current response.

The `names` dictionary is copied during account mapping; this is a shallow copy, not a deep clone of nested objects.

### 3.7 Dependency injection choices

- Singleton `IProducer<string, string>`: reuse the thread-safe Kafka producer and its connections instead of creating one per request.
- Singleton `IEventPublisher`: a lightweight wrapper over that producer and a logger.
- Singleton `MongoContext`: reuses the MongoDB client and collection handles; it is not an EF-style request unit of work.
- Scoped `IPersonService`: a query service resolved per request.
- Named HTTP client for worker health: centrally configures the three-second timeout and uses the HTTP client factory.

Controllers depend on abstractions such as `IEventPublisher` and `IPersonService`, making them easier to test. Not every dependency is abstracted: the query service directly uses `MongoContext`.

## 4. kafka.AccountService

Key sources: [Program.cs](kafka.AccountService/Program.cs) and [AccountConsumerWorker](kafka.AccountService/Consumers/AccountConsumerWorker.cs).

The worker is a hosted `BackgroundService` inside an ASP.NET Core application. It uses HTTP for `/health`, but its business processing happens in the background Kafka loop.

Startup validates configuration and creates **account-owned indexes before consumption begins**. A MongoDB/index failure during startup prevents the service from beginning processing.

For each message, the worker:

1. Consumes a Kafka record.
2. Resolves the correlation ID from headers or generates one.
3. Opens a structured logging scope with topic, partition, offset, and key.
4. Deserializes with `JsonSerializerOptions.Web`.
5. Runs `AccountEventValidator`.
6. Runs the versioned MongoDB writer through the persistence retry pipeline.
7. Stores and commits the offset after successful handling.
8. Updates its worker health state.

The validator requires a valid ObjectId string, non-negative version, non-empty nested `groupId`, and non-null `personalData`. Required JSON attributes also reject omitted required fields during deserialization.

It does not implement comprehensive validation of age, email, gender, address, timestamp relationships, or all nested personal fields. Do not claim that every business attribute is validated.

Accounts are upserted by `_id`, not `groupId`. The account lookup index is not unique, so the database does not enforce exactly one account per business identifier. If multiple matching accounts exist, the single-person query uses `FirstOrDefaultAsync` without a deterministic sort.

## 5. kafka.EmployeeService

Key sources: [Program.cs](kafka.EmployeeService/Program.cs) and [EmployeeConsumerWorker](kafka.EmployeeService/Consumers/EmployeeConsumerWorker.cs).

This project has the same message lifecycle as AccountService, but consumes `topic.employees`, writes employment documents, uses its own Kafka group, and initializes employment indexes.

`EmployeeEventValidator` requires a valid ObjectId string, non-negative version, non-empty `groupId`, and non-null `employmentData`. It does not require the corresponding account to exist first.

### Multiple employments versus multiple revisions

- Different employment `_id` values represent different employment records.
- Multiple messages with the same employment `_id` are revisions of one employment.
- All employments for a person share `groupId`.
- Versions are compared only within one `_id`; an employment version is not compared to an account version or another employment's version.

### What happens to a conflicting active employment?

A partial unique index rejects a second active, non-deleted employment for the same `groupId`.

The worker catches the propagated duplicate-key write exception, publishes the rejected event to the employment DLQ with a validation reason, commits after DLQ publication succeeds, and continues consuming.

It **does not automatically deactivate the old employment** or choose the newest active employment by hire date or version. Versions are per document, so “version 200 wins over version 150 on another employment” is not a rule here.

> Suggested rationale: “I enforce the invariant in MongoDB so concurrent service instances cannot bypass it. Conflicting events are quarantined rather than silently changing a different employment document.”

## 6. kafka.Shared and the domain models

Key sources: [BaseDocument](kafka.Shared/Models/Common/BaseDocument.cs), [AccountDocument](kafka.Shared/Models/Accounts/AccountDocument.cs), [EmployeeDocument](kafka.Shared/Models/Employees/EmployeeDocument.cs), and [MongoContext](kafka.Shared/MongoDB/MongoContext.cs).

### 6.1 Why a common base class?

`BaseDocument` defines `_id`, `isActive`, `isDeleted`, `createdDate`, `modifiedDate`, and `version` once.

A base class is appropriate here because both models share actual state and serialization mapping, not merely behavior. An interface would describe a contract but would not remove repeated property declarations and attributes.

The generic constraint `where TDocument : BaseDocument` allows one versioned writer to handle both document types.

### 6.2 JSON and BSON are different boundaries

JSON is the HTTP/Kafka representation. BSON is MongoDB's typed binary document representation.

- `JsonPropertyName("_id")` maps the unusual JSON identifier name.
- `BsonId` identifies the MongoDB primary key.
- `BsonRepresentation(BsonType.ObjectId)` lets the C# property be a string while MongoDB stores it as an ObjectId.
- `BsonElement` maps stored field names.
- `BsonDateTimeOptions(Kind = DateTimeKind.Utc)` establishes UTC date handling for the document audit dates.
- `JsonRequired` requires the presence of selected JSON properties during deserialization.
- The computed `GroupId` convenience property is ignored by BSON; the real field remains nested under `mappingFields`.

ObjectId syntax and ISODate expressions in the assignment are Mongo shell notation, **not valid HTTP JSON**. Actual payloads use a 24-character hexadecimal string for `_id` and ISO 8601 date strings. The main document deserialization does not promise support for an Extended JSON `{ "$oid": ... }` wrapper in place of that string.

Presence and validity differ: a required property can be present as null, so validators still check nested objects. Explicit version zero is valid, while an omitted version is rejected. Several other fields are not marked required and can receive CLR defaults if omitted.

### 6.3 Why shared infrastructure?

The shared library centralizes:

- Topic and collection names.
- Typed options.
- Common consumer configuration, retry policy, correlation handling, and DLQ publishing.
- MongoDB access, index definitions, and versioned persistence.
- Account/employee models and response DTOs.
- Validators, serializers, exceptions, and health support.

Important libraries include Confluent.Kafka, MongoDB.Driver, and Polly. Serilog is configured in the executable projects.

Tradeoff: all services depend on shared contracts and infrastructure. A small solution benefits from consistency, but broad changes to this library may require coordinated deployments. The library is not a completely persistence-independent domain layer.

## 7. Flexible names and serialization

This is especially useful if asked about [NamesDictionaryJsonConverter](kafka.Shared/Serialization/NamesDictionaryJsonConverter.cs), the currently relevant serialization component.

### 7.1 Why Dictionary<string, object>?

The task explicitly leaves `names` undefined. A strict class with predefined fields would invent a schema and reject or lose future data.

The dictionary supports arbitrary keys with nested objects, arrays, strings, booleans, numeric values, and nulls. Flexibility is confined to this property: it does **not** mean arbitrary unknown fields throughout the entire account are retained in MongoDB. Typed deserialization normally ignores unmapped JSON properties unless extension-data handling is added.

### 7.2 What does each converter do?

| Component | Responsibility |
|---|---|
| [BsonDocumentJsonConverter](kafka.Shared/Serialization/BsonDocumentJsonConverter.cs) | Reads an object as a BsonDocument, treats null as an empty document, and translates relevant BSON parsing errors into JsonException. Its write path emits relaxed Extended JSON. |
| [NamesDictionaryJsonConverter](kafka.Shared/Serialization/NamesDictionaryJsonConverter.cs) | Uses the BSON JSON converter to read, then maps BSON values to .NET dictionary values. Writes the dictionary using normal System.Text.Json serialization. |
| [NamesDictionaryBsonSerializer](kafka.Shared/Serialization/NamesDictionaryBsonSerializer.cs) | Writes the dictionary as a BSON document and reads BSON documents back into .NET dictionary values; legacy BSON null becomes an empty dictionary. |

The JSON and BSON serializers are attached to the account's `Names` property using attributes. The model setter also converts assigned null to a new empty dictionary.

### 7.3 Why use BSON while reading JSON?

Default System.Text.Json deserialization of an `object` value commonly produces `JsonElement` values. BSON parsing followed by `BsonTypeMapper.MapToDotNetValue` gives the dictionary a representation suitable for the MongoDB serialization path, while preserving nested structure.

The flow is:

HTTP/Kafka JSON → JSON converter → BsonDocument → .NET dictionary → BSON serializer → MongoDB.

Reading reverses the persistence conversion; mapping then exposes dictionary data in the response DTO.

A null, omitted, or empty `names` property becomes `{}`. A top-level array, string, number, or boolean in `names` throws `JsonException` during consumer deserialization and follows validation/DLQ handling. Nested arrays remain valid.

The converter's write method delegates to `JsonSerializer.Serialize` without reattaching itself globally. In the current property-level configuration, that serializes the dictionary normally. Registering the same converter globally without considering delegation could introduce recursive conversion; that is not how this implementation registers it.

### 7.4 What is tested, and what is not guaranteed?

[AccountNamesTests](kafka.UnitTests/Serialization/AccountNamesTests.cs) checks nested values, Unicode, arrays, null normalization, legacy BSON null, invalid top-level shapes, and round trips through BSON and API mapping.

Do not claim byte-for-byte JSON identity after persistence. Numbers can be mapped to BSON/.NET numeric types, property formatting can change, and BSON-specific Extended JSON values need explicit compatibility expectations.

> Suggested answer: “I kept `names` schema-flexible but still required an object at its root. Custom conversion bridges JSON and MongoDB serialization, and round-trip tests check that normal nested values survive ingestion, persistence, and response mapping.”

## 8. Kafka ordering, delivery, and scaling

Key source: [ConsumerBase](kafka.Shared/Consumer/ConsumerBase.cs).

### 8.1 Why Kafka?

Kafka decouples ingestion from processing, retains records for replay within retention, and distributes work through partitions and consumer groups. A temporary MongoDB problem need not prevent an already-published record from remaining available in Kafka.

Costs include broker operation, eventual consistency, duplicate handling, and more complicated troubleshooting than a synchronous database write.

### 8.2 Why use groupId as the record key?

The producer attempts to key events by the person identifier. With consistent partitioning and a stable partition count, records with the same key in the **same topic** go to the same partition, supporting person-local processing order.

This does not create order between `topic.accounts` and `topic.employees`. It also does not ensure that upstream events are logically ordered by version. Different producers or replays can still deliver older state later.

The permissive producer can publish unkeyed events when the expected identifier is absent. Business validation remains the worker's responsibility.

### 8.3 Consumer groups and partitions

Account and employee workers use different groups and subscribe to different topics.

For one group, Kafka normally assigns a partition to one consumer at a time. Adding replicas helps distribute partitions, not create unlimited parallelism.

The supplied Compose configuration creates **one partition per topic**. More replicas of a given worker therefore do not increase steady-state consumption parallelism for that topic. Topic initialization uses `--if-not-exists`; changing that declaration alone is not a partition migration for existing topics.

The worker processes one record at a time in its loop. Slow database calls and retries block that worker's next record. Production tuning would consider processing duration, `max.poll.interval.ms`, safe partition-aware concurrency, and batching.

### 8.4 Why disable automatic offset handling?

Consumer configuration sets:

- `AutoOffsetReset = Earliest`.
- `EnableAutoCommit = false`.
- `EnableAutoOffsetStore = false`.

The worker explicitly calls `StoreOffset` and `Commit` after handling. Committing a consumed record advances the committed position to the next record, rather than meaning “start again at this exact offset.”

`Earliest` applies when the group has no usable committed offset; it does not rewind an established consumer group on every restart.

### 8.5 Producer idempotence is not end-to-end exactly-once

Both normal and DLQ producers use `Acks.All` and `EnableIdempotence = true`. Kafka producer idempotence addresses duplicates from producer retries within Kafka's supported producer semantics.

It does not deduplicate a client submitting the same HTTP request twice, atomically coordinate MongoDB with offset commits, or eliminate duplicate DLQ envelopes after a crash.

`Acks.All` waits for acknowledgments from the in-sync replicas required by the broker/topic configuration. With the single-broker, replication-factor-one local environment, it does not provide multi-node durability.

> Suggested answer: “The end-to-end processing model is at-least-once with idempotent state updates. I deliberately avoid claiming exactly-once across Kafka and MongoDB because those operations are not one atomic transaction.”

## 9. Version-aware persistence

Key source: [VersionedDocumentWriter](kafka.Shared/MongoDB/VersionedDocumentWriter.cs).

This is one of the most important interview topics.

### 9.1 The normal operation

The writer validates basic input and issues a `ReplaceOneAsync` with upsert enabled. Its filter requires:

- Stored `_id` equals incoming `_id`.
- Stored `version` is strictly less than incoming `version`.

Consequences:

| Stored state | Incoming version | Outcome |
|---|---|---|
| No document | 48 | Insert version 48. |
| Version 48 | 49 | Replace with version 49. |
| Version 49 | 48 | Ignore older revision. |
| Version 49 | 49 | Ignore equal revision. |

The result is `Inserted`, `Updated`, or `Ignored` and is logged by the consumer.

The version comparison is part of the MongoDB write filter, not an unprotected read-then-write check. MongoDB's single-document atomicity prevents a lower version from replacing a higher version during a race.

### 9.2 Why catch duplicate keys?

When a document already has an equal or greater version, the conditional filter does not match it. With upsert enabled, MongoDB may then try to insert the incoming document. Its `_id` conflicts with the existing primary key.

The writer therefore handles duplicate-key errors carefully:

1. Read the stored document with the same `_id`.
2. If no such document exists, rethrow. The collision may be from a different unique index, such as the active-employment invariant.
3. If stored version is equal or greater, return `Ignored`.
4. Otherwise retry the same conditional replacement with **upsert disabled**.
5. Return `Updated` if modified, otherwise `Ignored`.

The fallback still uses the version condition. A concurrent writer that advances the version between the read and fallback cannot be overwritten by a lower revision.

Not every duplicate-key exception is a harmless duplicate event. A recovery write can still violate another unique index and propagate a real error to the worker.

### 9.3 Why replacement rather than patching?

This treats each message as a complete snapshot. Replacement keeps stored state aligned with the newer source revision, including removal of fields no longer represented by the mapped document.

Tradeoff: partial patch messages are not supported as such. A newer event that omits optional fields may clear them or replace them with model defaults. Full snapshots and monotonic versions must be part of the source contract.

### 9.4 Assumptions behind the guarantee

- Versions increase for a given `_id`.
- Equal versions represent the same logical revision; different payloads with the same version are not reconciled.
- Existing data has a compatible version field/type.
- Distinct documents are not being ranked against each other.
- Historical snapshots are not all stored in MongoDB.

A correction must arrive with a higher version. There is no comparison using modified timestamps, and no version-gap validation.

## 10. MongoDB schema, indexes, and active employment

Key source: [MongoIndexInitializer](kafka.Shared/MongoDB/MongoIndexInitializer.cs).

### 10.1 Why separate collections instead of embedding all employments?

Accounts and employments are stored separately and linked by `groupId`.

This matches two independently arriving streams and lets one employment update without rewriting a person's whole employment array. Employment data can exist before account data. It also avoids unbounded growth of one account document as employment history increases.

Tradeoffs: the API must join records, referential integrity is application-managed, and reads are not automatically atomic across both collections.

Embedding could be attractive for a small bounded history almost always read with the account, but would complicate independent upserts, contention, and ownership of a shared document.

### 10.2 Exact indexes

MongoDB automatically provides the unique `_id` index. The application creates:

| Collection | Index name | Fields / rule |
|---|---|---|
| Accounts | `ix_accounts_groupId_status` | `mappingFields.EmployeeId.groupId`, `isActive`, `isDeleted`. |
| Accounts | `ix_accounts_name_status` | `personalData.firstName`, `personalData.lastName`, `isActive`, `isDeleted`. |
| Employees | `ix_employees_group_id_status` | `mappingFields.EmployeeId.groupId`, `isActive`, `isDeleted`. |
| Employees | `ux_employees_one_active_per_group_id` | Unique `groupId`, only where `isActive=true` and `isDeleted=false`. |

Compound indexes follow the application's actual lookup predicates. They are not independent indexes for every field mentioned in the assignment. `EmployeeId` is a nested mapping object in these messages, not a separately modeled scalar employee identifier.

Index field order matters. A leading `groupId` or name prefix supports the corresponding lookup; an omitted intermediate status condition can affect how efficiently later keys narrow a scan. Do not claim these indexes optimize every status-only or last-name-only query. Use real query plans before tuning.

Indexes cost storage and work on writes, so adding every possible index is not automatically better.

### 10.3 Why a partial unique index?

A unique index on `groupId` across all employments would forbid history. The partial filter includes only live active records, so many inactive or deleted employment documents can coexist.

The rule depends on flags, not on `employmentStatus`, contract dates, or hire dates. A document marked `isActive=true, isDeleted=true` is outside the uniqueness subset and normally excluded from default API reads.

A database constraint is stronger than a “check for existing active employment, then insert” application sequence, which can race between replicas.

### 10.4 Ownership and startup

Definitions are centralized in Shared, but each worker initializes only its collection's indexes. The read API does not create indexes.

Creating the unique index can fail if preexisting data already violates the invariant. Startup does not automatically reconcile or delete conflicting records.

Standalone MongoDB is sufficient for the implemented single-document updates and index constraint. No multi-document transaction or change stream is used.

## 11. Failures, retries, and dead letters

Key sources: [ConsumerBase](kafka.Shared/Consumer/ConsumerBase.cs), both workers, and [DeadLetterMessage](kafka.Shared/DeadLetter/DeadLetterMessage.cs).

### 11.1 What is retried?

The Polly pipeline retries only:

- `MongoConnectionException`.
- `MongoExecutionTimeoutException`.
- `TimeoutException`.

Configuration uses three retries with a base delay of 500 milliseconds, exponential backoff, and jitter. Three retries mean up to four executions including the initial attempt.

Backoff avoids rapidly hammering an unavailable dependency. Jitter reduces synchronized retry bursts across replicas. JSON validation and duplicate-key business conflicts are not retried by this pipeline.

### 11.2 Exact outcome by failure type

| Failure | Current behavior |
|---|---|
| Malformed or incompatible event JSON in Kafka; invalid model | Publish to the relevant DLQ, commit only after successful publication, then continue. |
| Conflicting active employment | Employee worker publishes a validation DLQ envelope, commits after success, then continues. |
| Other MongoWriteException | Sent to DLQ; account worker labels these `validation`, while employee worker labels non-duplicate writes `persistence`. |
| Retried transient persistence error eventually succeeds | Commit after the successful or safely ignored write. |
| Retried transient persistence error exhausts attempts | Falls into unexpected-error handling, marks worker failed, throws, and does not commit that record. |
| Nonfatal ConsumeException | Log and continue; no fabricated commit of an unprocessed record. |
| Fatal ConsumeException | Mark failure and throw. |
| DLQ publication fails | Source offset is not committed by the DLQ helper; failure escapes. |
| Shutdown cancellation | Exit the loop and close the consumer. |

**Important:** there is no blanket “every error goes to DLQ after three retries” policy. In particular, exhausted handled transient exceptions stop the worker rather than automatically being dead-lettered.

Stopping instead of advancing after an unexpected processing error avoids accidentally committing past an unhandled earlier record. Recovery still requires a running/restarted consumer and retained source data.

### 11.3 What is in a dead-letter envelope?

It records source service, topic, partition, offset, original key, original payload, correlation ID, failure reason, exception type, retry count, and failure timestamp.

This preserves enough context to investigate the rejected event. The current DLQ calls pass retry count zero; the value is not a reliable count of all transient retry activity. There is no implemented automatic replay worker, remediation UI, or deduplication of DLQ envelopes.

DLQ messages contain original payloads, potentially including personal/contact information. Production access and retention policies must account for that.

### 11.4 Why publish to DLQ before committing?

If commit happened first and DLQ publication failed, the event could disappear from normal consumption without a durable failure record.

Publishing first avoids that loss window, but leaves a duplicate window: a crash after DLQ publication and before source commit can cause another DLQ envelope on replay. Kafka publication and source commit are not transactional here.

## 12. Observability, errors, health, and configuration

### 12.1 Correlation across the flow

[CorrelationIdMiddleware](kafka.Api/Middleware/CorrelationIdMiddleware.cs) takes `X-Correlation-ID` from the request, normalizes it or generates a value, stores it in `HttpContext.Items`, and schedules it for the response header.

The publisher adds the correlation identifier to Kafka headers. Consumers recover it, attach it to logging scopes, and include it in DLQ envelopes.

Structured logs also include service identity and, where appropriate, topic, partition, offset, key, document ID, `groupId`, version, and write result. Serilog console output is configured as compact JSON.

A correlation ID links related logs; it is not a Kafka partition key, business identity, idempotency key, authorization token, or full distributed trace. OpenTelemetry spans and metrics are not implemented.

### 12.2 Global API errors

[GlobalExceptionHandler](kafka.Api/ErrorHandling/GlobalExceptionHandler.cs) and [ProblemDetailsFactory](kafka.Api/ErrorHandling/ProblemDetailsFactory.cs) produce a consistent error contract:

- Request validation exception → `400`.
- Person not found → `404`.
- Kafka publication exception → `503`.
- Other unexpected exception → `500`, with a generic public detail.

The error payload includes correlation/error metadata. Invalid model binding is also adapted to a validation problem response. Internal exceptions are logged rather than exposing stack traces to clients.

Client-aborted requests are recognized separately; cancellation is not intentionally converted to an ordinary internal server error.

Middleware order puts exception handling around the correlation middleware and endpoint pipeline. The exception handler obtains correlation information from the request context when available.

### 12.3 Health checks

Each worker exposes `/health` with worker-state, Kafka, and MongoDB checks. `WorkerHealthState` uses locking because the consumer modifies state while HTTP health requests read it.

Worker state records running status, start time, last success, last error time, and error information. A running idle worker is not automatically unhealthy simply because no recent event arrived.

The API exposes `/health` by requesting both configured worker health endpoints. It checks their HTTP success status; it does not deeply inspect their response bodies or directly test its own Kafka producer and MongoDB connection. This is useful aggregate readiness, but an API-local dependency failure can still escape that proxy check.

All current health registrations use the `ready` tag. Separate liveness/readiness routes, consumer lag thresholds, and watchdog detection of a stuck loop are future improvements.

### 12.4 Typed options and fail-fast startup

`KafkaOptions`, `MongoOptions`, `ResilienceOptions`, and API worker endpoint options bind from configuration sections. Startup validation rejects selected missing or invalid required values rather than discovering them only when processing the first event.

Validation is not exhaustive: for example, the workers validate broker/group/topic values, but do not perform comprehensive DLQ configuration validation. `ValidateOnStart` checks configured predicates, not network readiness.

Environment keys such as `Kafka__BootstrapServers` override nested configuration through .NET's double-underscore convention. Connection strings and local credentials are development settings, not an adequate production secret-management approach.

### 12.5 OpenAPI and Swagger

The API maps an OpenAPI document and Swagger UI, with an operation transformer providing event examples. Because ingestion accepts arbitrary JSON, examples explain an expected event without making the controller strongly typed.

The code maps these endpoints without a Development-only condition. Production exposure and authorization would need an explicit policy.

## 13. Docker and local development

Key sources: [docker-compose.yml](docker-compose.yml), [AccountService Dockerfile](kafka.AccountService/Dockerfile), [EmployeeService Dockerfile](kafka.EmployeeService/Dockerfile), and [readme.md](readme.md).

### 13.1 What Compose actually starts

- Apache Kafka `4.3.1` in combined broker/controller KRaft mode.
- `kafka-init`, a one-time process that waits for the broker and creates two source and two DLQ topics.
- MongoDB `8` with a health check and named data volume.
- Containerized AccountService and EmployeeService.

**The API is not a Compose service.** It is started locally using `dotnet run --project .\kafka.Api\kafka.Api.csproj`.

There is no ZooKeeper dependency. KRaft uses Kafka's own metadata quorum machinery. The local single-node setup simplifies development, but is not a highly available cluster.

### 13.2 Why separate Kafka listeners?

Containers use `kafka:9092`; the locally running API uses `localhost:29092`.

Kafka clients bootstrap to obtain broker metadata and then connect to advertised addresses. An advertised hostname that is reachable only inside Docker cannot serve a host-based client, and `localhost` inside a worker container is not the Kafka container.

The separate internal/external listener configuration solves that networking distinction for this local topology.

MongoDB is available to host clients at `localhost:27018`, while workers use `mongodb:27017` internally. Workers expose health on host ports 5101 and 5102; the API's documented HTTP launch address is 5210.

### 13.3 Dockerfile choices

Both workers use multi-stage builds:

1. Restore/publish with the .NET 10 SDK image.
2. Copy published output into the smaller ASP.NET runtime image.
3. Run under the image's application UID, not the default root identity.
4. Expose HTTP port 8080 for health.

The ASP.NET runtime image is needed because these background consumers also host HTTP endpoints.

Copying project files before the whole source tree supports Docker layer caching for restore. The root build context makes the Shared project available to each worker build.

### 13.4 Development durability and security caveats

MongoDB mounts `mongodb-data`. The Compose file declares `kafka-data`, but **does not mount it on Kafka**. Do not claim that broker data survives container replacement through that declared volume.

Kafka listeners use PLAINTEXT and replication factor one. MongoDB uses explicit local development credentials. These choices reduce local setup effort, not production risk.

Workers use `restart: unless-stopped`; exhausted failures can therefore lead to restart/replay cycles. A durable message that repeatedly fails needs alerting and an operational remediation policy, not merely infinite restarts.

## 14. The two test projects

### 14.1 kafka.UnitTests

This xUnit project uses recording substitutes/helpers to inspect collaborators without external infrastructure.

Coverage includes:

- Account and employment validators, including missing required fields and explicit version zero.
- Events and persons controller delegation, trimming, and status/error paths.
- Kafka payload preservation, key extraction, correlation headers, and publication error behavior.
- Correlation middleware propagation.
- Global exception mappings and safe response details.
- Worker health lifecycle.
- Versioned writer duplicate-key recovery and propagated failures.
- Status filter defaults and response metadata omission.
- Flexible `names` round trips and invalid shapes.

Some tests access private mapping/filter methods through reflection. That avoids a production API change purely for testing, but makes those tests more tightly coupled to implementation details than public behavior tests.

> Suggested answer: “Unit tests give quick feedback about our own decisions. They cannot prove Kafka behavior or real MongoDB concurrency, so those guarantees also need integration tests.”

### 14.2 kafka.IntegrationTests

Key sources: [IntegrationTestFixture](kafka.IntegrationTests/Infrastructure/IntegrationTestFixture.cs), [KafkaApiFactory](kafka.IntegrationTests/Infrastructure/KafkaApiFactory.cs), and the [Tests directory](kafka.IntegrationTests/Tests).

The fixture uses Testcontainers for real Kafka and MongoDB, creates required topics/indexes, configures real consumer worker instances, and hosts the API in process through ASP.NET Core testing infrastructure.

It uses generated database/group identities for isolation and shares infrastructure through an xUnit collection. `AsyncWait` polls conditions until a deadline instead of assuming that a fixed sleep is enough for asynchronous processing.

Important test scenarios include:

- Employment arrives before account, then a combined person is returned.
- Account version increases, stale revisions are ignored, and duplicates create only one document.
- Concurrent writes preserve the highest document version.
- Invalid/omitted required fields reach DLQ and processing continues.
- Source offset is committed after invalid event handling.
- Conflicting active employment is dead-lettered.
- Required indexes and per-worker index ownership.
- Multiple historical employments and unique live active employment.
- Independent status filters, defaults, no matching employments, and invalid boolean parameters.

These are integration tests, not full deployment acceptance tests: the API and workers are not all started as Compose application containers, and they do not prove the exact Docker deployment, every broker outage, or production-scale behavior.

### 14.3 Running and reporting tests honestly

From the root, use `dotnet test .\kafka.UnitTests\kafka.UnitTests.csproj` for fast tests and `dotnet test .\kafka.IntegrationTests\kafka.IntegrationTests.csproj` with a working Docker engine for integration tests.

Validation performed while preparing this guide: the solution built successfully, all 95 unit test cases passed in a fresh run, and all local documentation links resolved. Integration tests were not rerun. Visual Studio listed 109 integration cases as failed from previous runs; that historical state is not a fresh result or a diagnosis. Do not present integration coverage as proof that the suite currently passes; inspect the current test output before making that claim.

Useful next reliability tests would inject commit failures, DLQ publication failures, exhausted transient retries, process termination between write and commit, and multi-partition rebalances.

## 15. Assignment coverage and differences

| Assignment area | Current solution | Interview point / caveat |
|---|---|---|
| .NET 10 ingestion API | Both POST actions accept JsonElement. | JSON transport validity differs from domain validity. |
| Endpoint design decision | Two business-oriented routes mapped to fixed topics. | Clients do not choose arbitrary topics. |
| Account/employee consumers | Independently hosted workers with separate groups. | Shared database/library mean some architectural coupling remains. |
| Join through groupId | Nested mapping identifier used in queries and Kafka key extraction. | `_id` identifies a document, not a person across both streams. |
| Idempotency/out-of-order handling | Conditional version-aware replacement/upsert. | Assumes full snapshots and monotonic versions per document. |
| Shared system fields | BaseDocument. | Centralizes properties and serialization attributes. |
| Flexible names | Dictionary plus JSON/BSON conversion. | Arbitrary keys inside names, not preservation of every unknown event field. |
| Person lookup/search | Account plus matching employment array; name search batches the join. | Empty employments remain an array; no pagination. |
| Requested isActive/isDeleted parameters | Replaced by four entity-specific parameters. | More expressive, but not strict compatibility with requested names. |
| MongoDB indexes | Query-oriented compound indexes plus partial unique employment index. | Not separate indexes for every listed field; account groupId is not unique. |
| Message _id as MongoDB ObjectId | String model with ObjectId BSON representation and validation. | Shell notation must be converted to ordinary JSON. |
| At most one active employment | Database partial unique index. | Conflict is rejected; active-employment transitions are not automatically reconciled. |
| Poison message robustness | Validation/write failures can go to dedicated DLQs. | Exhausted transient and unexpected errors can stop the worker. |
| Docker Kafka/MongoDB | KRaft and standalone MongoDB, workers included. | API runs locally; Kafka data volume is declared but not mounted. |
| Structured logging/correlation | Serilog JSON and HTTP → Kafka → worker/DLQ propagation. | Not full tracing, lag metrics, or an event-status resource. |
| Global error handling | ProblemDetails and explicit exception mappings. | Worker failures are handled by consumer logic, not HTTP middleware. |
| Tests | Unit tests and real-infrastructure Testcontainers tests. | Verify current results; existence alone is not a passing guarantee. |
| OpenAPI | Document, Swagger UI, and event examples. | Mapped outside a Development-only condition. |
| Health | Both workers and API aggregate endpoint. | No separate liveness route or API-local dependency probe. |
| Dockerfiles | Both workers have multi-stage Dockerfiles. | No API container in the current Compose topology. |
| README, samples, Postman | Root README, sample-data accounts/employees, Postman collection. | A public repository link/access permission is outside code verification. |

## 16. Scenario walkthroughs

### A. Employment arrives before the account

The employment validator does not require an account lookup. EmployeeService persists it using employment `_id`. Until a matching account exists, person lookup returns `404`. Once the account is processed, the read API joins the existing employment through `groupId`.

This supports independent streams, but does not prove referential integrity or prevent permanently orphaned employments.

### B. Version 48 arrives after version 49

The conditional write does not match version 49. Any duplicate `_id` upsert attempt is inspected, version 49 is retained, and the worker records `Ignored`. The stale Kafka record can then be committed safely.

### C. The same version is sent twice

Two Kafka records may exist, but the second document write is ignored. If the equal-version payloads differ, the first stored revision is retained; the source must send a higher version for a correction.

### D. MongoDB succeeds, then the process dies before commit

MongoDB already contains the state, but Kafka may redeliver because the committed position did not advance. The version check makes replay harmless for database state, and the worker can commit after handling it again.

Logs or other future side effects are not automatically deduplicated by this writer.

### E. A second active employment arrives

The first live active employment occupies the partial unique index entry. A different employment `_id` for the same `groupId` is rejected, dead-lettered, and committed after DLQ success. The old employment remains active.

### F. New activation arrives before old deactivation

Even if both are logically valid source events, the new activation can conflict and reach DLQ before the old deactivation is processed. Later deactivation does not automatically replay the rejected event.

This exposes a distinction between enforcing the invariant and converging to the intended state under cross-document out-of-order transitions. Replay/reconciliation or an explicit transition workflow would be needed for stronger behavior.

### G. Bad JSON-shaped domain data is posted

Syntactically valid JSON can receive `202` and enter Kafka. The consumer rejects incompatible types, required-field omissions, or validation failures, writes a DLQ envelope, and continues after successful quarantine.

Syntactically malformed HTTP JSON is instead rejected by the API binding layer.

### H. MongoDB is unavailable longer than the retry budget

Handled transient errors receive bounded retries with backoff and jitter. If still failing, the worker fails without committing the record. In Compose, restart policy may start it again and the group can replay from the committed position.

The event is not automatically moved to DLQ merely because those retries were exhausted.

### I. No employment matches the requested filters

A matching account is still returned with `employees: []`. Employment filters do not determine whether the account itself matches. This is the reason independent account and employment filters can be useful.

### J. DLQ publication succeeds, then the process dies before commit

The source record can be replayed and produce a duplicate DLQ envelope. An operational DLQ consumer should consider source topic/partition/offset when identifying repeat failure reports.

## 17. Interview questions and suggested answers

### Why use asynchronous messaging instead of calling the workers over HTTP?

> Kafka separates request acceptance from processing availability and buffers retained work. Workers can operate independently and recover by replaying records. The tradeoff is eventual consistency and the need to handle duplicates, ordering, and operational complexity.

### Why is groupId different from _id?

> `groupId` identifies the person across both streams. `_id` identifies one account or employment document. Several employment documents share one groupId, but each has its own `_id` and revision sequence.

### Does Kafka guarantee that your data arrives in the correct business order?

> Kafka preserves record order within a partition, not global or cross-topic order. Keying helps person-local order within one topic, but source revisions can still be stale when they arrive. MongoDB's version condition is the final protection against overwriting newer document state.

### Do you have exactly-once processing?

> No. Kafka publication, MongoDB persistence, and offset commits are not one transaction. Reprocessing is possible. Producer idempotence reduces Kafka retry duplicates, and version-aware writes make repeated processing idempotent for stored document state.

### Why not just find the stored version and then replace it?

> A separate read-then-unconditional-write sequence can race. I include the version comparison in the database replacement filter, and the duplicate-key recovery still uses that condition.

### Why can an obsolete event generate a duplicate-key exception?

> The version condition excludes the existing document. Upsert then attempts an insert with the same `_id`. The writer checks the document with that ID to determine whether the event is genuinely obsolete or whether another uniqueness rule caused the conflict.

### Why use a partial unique index for active employment?

> Global uniqueness on groupId would prohibit history. Partial uniqueness includes only active, non-deleted employment records, enforcing the invariant even across concurrent worker instances while allowing multiple historical records.

### How do you switch the active employment?

> The source must deactivate the old record and activate the new record in a safe sequence. The current implementation rejects conflicting activation rather than automatically updating another document. A stronger transition/reconciliation workflow would be needed for arbitrary cross-document ordering.

### Why not embed employments inside the account?

> The streams arrive independently and employment history can grow. Separate documents support independent per-employment versioned writes and permit employment-before-account arrival. The tradeoff is a join and lack of automatic cross-collection consistency.

### Is your GET response immediately consistent after POST?

> No. POST confirms publication, not worker completion. GET reads the current MongoDB state and can temporarily return no account or a partially updated combined view.

### Why do deleted records disappear by default?

> Default reads filter `isDeleted=false`, which keeps soft-deleted data out of the normal view. Explicit entity-specific deletion filters select deleted records when requested. Omitted active filters still include both active and inactive non-deleted records.

### Why does search require both names?

> The current endpoint implements the assignment's combined first-name/last-name lookup using exact equality and a matching compound index. Partial search, normalization, and pagination would be explicit API changes rather than assumed behavior.

### What happens when one malformed event is received?

> Deserialization or domain validation fails in the owning consumer. The worker publishes a failure envelope to its DLQ, commits only after that succeeds, and continues. Unexpected infrastructure failures follow a different path and can stop the worker.

### Why bounded retries rather than retrying forever in one loop?

> Bounded retries handle brief outages without permanently hiding persistent failures. Backoff and jitter reduce load. After exhaustion the current worker fails without advancing the offset; restart and operations policy determine recovery.

### What is the purpose of the correlation ID?

> It connects the HTTP request, Kafka publication, worker logs, and DLQ report. It is useful for investigation, but it is not a business key or a substitute for distributed tracing and metrics.

### Why are the worker applications web hosts?

> Their processing is a BackgroundService, but an ASP.NET Core host also exposes HTTP health endpoints. That allows external readiness checks without adding business request handlers to the workers.

### How would you scale the consumers?

> Increase partitions and deploy more worker instances in the same consumer group, while preserving keying and safe commit behavior. The supplied one-partition topics limit each group's current processing parallelism. I would also monitor lag and database capacity.

### What do your integration tests add beyond unit tests?

> They exercise actual Kafka records, offsets, MongoDB indexes and atomic writes, and the asynchronous API-to-worker flow. Unit substitutes can verify calls and branch logic but cannot prove real broker/database semantics.

### What would you improve first for production?

> I would resolve contract compatibility and active-employment transition semantics, establish a green reproducible integration run, add failure-injection tests and lag metrics, secure the transports/secrets, and make broker durability and deployment explicit. Those matter more than claiming the demo already has every production guarantee.

## 18. Production improvements and final checklist

### Prioritized improvements, not implemented features

1. **Contract clarity:** reconcile requested `isActive`/`isDeleted` names with the four filters; define snapshot/version semantics, deletion behavior, and identifier stability.
2. **Business identity:** decide whether account `groupId` must be unique and reconcile existing data before enforcing it.
3. **Employment transitions:** define replay/reconciliation or a coordinated transition workflow for activation-before-deactivation events.
4. **Reliability evidence:** obtain a clean integration run and test write/commit crash windows, DLQ failures, exhausted retries, and rebalances.
5. **Operational visibility:** monitor consumer lag, retry/DLQ rates, processing latency, restarts, and version conflicts; add tracing where useful.
6. **Durability:** mount Kafka storage, choose retention explicitly, and use a replicated broker deployment when availability requires it.
7. **Security/privacy:** add API authentication and authorization, Kafka TLS/SASL as appropriate, managed secrets, restricted health/docs exposure, and PII/DLQ retention controls.
8. **Read scalability:** add pagination and deterministic ordering; evaluate query plans and projections. Consider a consolidated read model if reads justify maintaining another projection.
9. **Deployment portability:** normalize path casing, test Linux builds, and containerize the API if a fully Compose-managed system is required.
10. **Service boundaries:** split contracts/infrastructure or move toward service-owned reads when shared-schema coupling becomes a real constraint.

### Before the interview

Be able to open and explain these files:

- [EventsController](kafka.Api/Controllers/EventsController.cs): raw ingestion and `202`.
- [KafkaEventPublisher](kafka.Api/Kafka/KafkaEventPublisher.cs): unchanged JSON, optional key, correlation header, and publication errors.
- [PersonService](kafka.Api/Services/PersonService.cs): two-query join, status defaults, and DTO mapping.
- [ConsumerBase](kafka.Shared/Consumer/ConsumerBase.cs): manual offsets, selected retry exceptions, and DLQ-before-commit.
- [VersionedDocumentWriter](kafka.Shared/MongoDB/VersionedDocumentWriter.cs): conditional upsert and duplicate-key recovery.
- [MongoIndexInitializer](kafka.Shared/MongoDB/MongoIndexInitializer.cs): lookup indexes and the active-employment invariant.
- [NamesDictionaryJsonConverter](kafka.Shared/Serialization/NamesDictionaryJsonConverter.cs): flexible JSON-to-dictionary conversion through BSON.
- [EmployeeConsumerWorker](kafka.EmployeeService/Consumers/EmployeeConsumerWorker.cs): conflict quarantine and fatal processing paths.
- [IntegrationTestFixture](kafka.IntegrationTests/Infrastructure/IntegrationTestFixture.cs): real infrastructure and isolation.
- [docker-compose.yml](docker-compose.yml): KRaft, listener addresses, topic initialization, workers, and persistence limitations.

Demonstrate one account with multiple employments, employment-before-account arrival, a duplicate event, an older version, an invalid event reaching DLQ, and a filtered GET. Use the sample files under [sample-data](sample-data) and the [Postman collection](postman/Kafka.postman_collection.json).

### Claims to avoid

- “202 means it is already saved.”
- “Kafka gives global order across both topics.”
- “Producer idempotence means exactly-once across MongoDB and Kafka.”
- “Every exhausted retry goes to DLQ.”
- “The system automatically chooses/deactivates active employments.”
- “Every arbitrary event field is preserved in MongoDB.”
- “Every account groupId is database-unique.”
- “Search is fuzzy, case-insensitive, paginated, or sorted.”
- “The API runs in the supplied Compose file.”
- “The declared Kafka volume is already mounted.”
- “Integration tests exist, therefore they currently pass.”

The strongest explanation connects **a requirement, the concrete mechanism, its benefit, and its limitation**. For example: “To prevent stale events overwriting newer state, I put a version comparison into the MongoDB replacement filter. That gives atomic protection per document, but it does not order changes across two different employment documents.”
