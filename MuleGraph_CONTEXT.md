# MuleGraph — Antigravity Project Context and Implementation Contract

## 0. Status and authority

This file is the primary source of truth for building **MuleGraph — Real-Time Payment Fraud and Mule-Account Detection Engine**.

The coding agent must read this entire file before planning or changing code. It must implement **one approved phase at a time**. It must not generate the full repository in one pass.

### Source-of-truth order

When instructions conflict, use this order:

1. This file.
2. Accepted Architecture Decision Records under `docs/decisions/`.
3. Versioned event schemas, database migrations, and executable tests.
4. Existing production code.
5. Comments, generated documentation, and agent assumptions.

If a conflict remains material, stop and report it. Do not silently choose a new architecture.

---

## 1. Project purpose

MuleGraph is a portfolio-grade, production-inspired backend that processes **synthetic payment transaction events** and detects suspicious coordinated behaviour in near real time.

It must teach and demonstrate:

- Java and Spring Boot backend engineering
- Event-driven architecture
- Apache Kafka
- Kafka Streams
- Event time and late-event handling
- Partitioning, repartitioning, and consumer groups
- At-least-once delivery and idempotency
- PostgreSQL audit projections
- Neo4j graph projections and bounded path analysis
- Testing with Testcontainers
- Failure recovery and dead-letter handling
- Observability with OpenTelemetry, Prometheus, and Grafana
- Docker and Kubernetes deployment
- Helm and CI/CD

This is a learning project. The code must remain understandable enough for the owner to explain in an interview.

### Non-goals

- Do not connect to a real banking or payment system.
- Do not use real customer, account, IP, device, or transaction data.
- Do not make an actual payment.
- Do not use machine learning before deterministic rules and evaluation work.
- Do not create unnecessary microservices.
- Do not claim fraud is proven; alerts require human investigation.
- Do not invent performance, precision, recall, or recovery numbers.

---

## 2. Plain-language system model

MuleGraph receives a transaction event, records it durably in Kafka, and processes it asynchronously.

```text
Synthetic client
    -> Spring Boot ingestion API
    -> Kafka durable event log
    -> validation/normalization
    -> PostgreSQL ledger projection
    -> Kafka Streams fraud rules
    -> fraud candidate
    -> risk/graph confirmation where required
    -> fraud alert
    -> PostgreSQL alert projection
    -> asynchronous Neo4j graph projection
```

Technology responsibilities are intentionally separate:

- **Kafka** remembers what happened and decouples producers from consumers.
- **Kafka Streams** evaluates recent, stateful, event-time behaviour.
- **PostgreSQL** stores the detailed auditable ledger and investigation records.
- **Neo4j** stores a relationship projection for bounded multi-hop investigation.
- **Redis is not part of core rule correctness.** Add it only through an ADR for a concrete supporting use case.
- **Kubernetes** deploys, scales, restarts, and observes runtime workloads after the application works locally.

---

## 3. Fixed architecture decisions

These decisions are fixed unless an ADR is proposed and explicitly accepted.

### 3.1 Application shape

- One Git repository.
- One Spring Boot codebase for the MVP.
- Package-by-feature modular monolith.
- One Maven build and one deployable application image initially.
- Runtime responsibilities selected through Spring profiles:
  - `api`
  - `normalizer`
  - `ledger-projector`
  - `stream-worker`
  - `alert-projector`
  - `graph-projector`
- Do not split into separate repositories or independently versioned microservices.
- Do not convert to a Maven multi-module build without an accepted ADR.

### 3.2 Language and build

- Java 21.
- Maven Wrapper must be committed.
- Spring Boot must be a stable GA release compatible with Java 21.
- Never use snapshot, milestone, release-candidate, or unverified dependency versions.
- At project initialization, create `docs/version-matrix.md` containing exact versions and the official source used to verify each version.
- Prefer the Spring Boot dependency-management BOM and Testcontainers BOM rather than independently guessing transitive versions.

### 3.3 Local infrastructure

Use Docker Compose for Phases 1–6.

- Kafka in KRaft mode; no ZooKeeper for the new project.
- PostgreSQL.
- Neo4j only from Phase 3.
- OpenTelemetry Collector, Prometheus, and Grafana only from Phase 6.
- Containers must use pinned, verified tags; do not use `latest` in committed files.
- Health checks and named volumes are required where supported.

### 3.4 API semantics

Endpoint:

```text
POST /api/v1/transactions
```

Authentication for MVP:

```text
X-API-Key: <value from environment variable>
```

The API must:

1. Authenticate the caller.
2. Validate the request.
3. Create system fields such as `event_id`, `correlation_id`, and `ingested_at`.
4. Publish the event to `transactions.raw`.
5. Wait for successful broker acknowledgement.
6. Return `202 Accepted` only after acknowledgement.

`202 Accepted` means the event was durably accepted for asynchronous fraud analysis. It does not mean a payment was executed or fraud evaluation finished.

Use Kafka producer settings appropriate for durable publication, including `acks=all` and producer idempotence when compatible with the chosen client configuration. Do not describe this as end-to-end exactly-once processing.

### 3.5 Money and time

Money:

```text
amount_minor: signed 64-bit integer
currency: ISO-style three-letter uppercase code
```

Never use `float` or `double` for money.

Time fields:

```text
occurred_at  = when the transaction happened
ingested_at  = when MuleGraph accepted it
processed_at = when a projector/processor completed its relevant work
```

All timestamps must use UTC and ISO-8601 at system boundaries. Fraud windows use `occurred_at`.

### 3.6 Delivery model

The honest guarantee is:

```text
At-least-once delivery + idempotent projections
```

Kafka Streams may use exactly-once-v2 for Kafka-to-Kafka processing if tests verify it. That does not make PostgreSQL and Neo4j writes globally exactly once.

Required mechanisms:

- `event_id`
- `transaction_id`
- unique database constraints
- idempotent inserts/upserts
- retry-safe consumers
- committed offsets only after successful handling
- alert deduplication keys
- replayable Kafka topics
- dead-letter topics

### 3.7 Serialization

For the MVP:

- JSON events with Jackson.
- Explicit `schema_version` in every event.
- Strict DTOs and contract tests.
- Unknown fields may be tolerated only if explicitly configured and tested.
- Do not add Avro, Protobuf, or Schema Registry before an ADR explains the need.

---

## 4. Required transaction contract

The accepted internal transaction event must contain:

```json
{
  "event_id": "uuid",
  "transaction_id": "uuid",
  "schema_version": 1,
  "correlation_id": "uuid-or-trace-id",
  "source_account_id": "uuid",
  "destination_account_id": "uuid",
  "amount_minor": 125050,
  "currency": "INR",
  "device_id": "device-123",
  "ip_hash": "sha256-value",
  "occurred_at": "2026-07-11T10:20:30Z",
  "ingested_at": "2026-07-11T10:20:31Z"
}
```

Validation requirements:

- IDs must be present and valid UUIDs where the contract says UUID.
- Source and destination accounts must differ.
- `amount_minor` must be greater than zero.
- Currency must be exactly three uppercase letters for the MVP.
- `occurred_at` must be present and parseable.
- Requests beyond the configured size limit must be rejected.
- Raw client IP should not flow through the system; the project uses synthetic `ip_hash`.
- Generic client errors; internal stack traces must not be returned.

Recommended accepted response:

```json
{
  "event_id": "uuid",
  "transaction_id": "uuid",
  "status": "ACCEPTED"
}
```

---

## 5. Kafka topic and key contract

Create topics only when their phase requires them.

### Core topics

```text
transactions.raw
transactions.validated
transactions.invalid
fraud.candidates
fraud.alerts
graph.updates
processing.dead-letter
```

Later or internally generated repartition topics are acceptable, but must not be manually depended upon by application code.

### Keys

```text
transactions.raw        key = transaction_id
transactions.validated  key = transaction_id
fraud.candidates        key = primary_account_id or deterministic candidate key
fraud.alerts            key = alert_id or deterministic deduplication key
graph.updates           key = source_account_id
```

Kafka Streams must create rule-specific views:

```text
source_account_id      -> fan-out
destination_account_id -> fan-in
device_id              -> shared-device
ip_hash                -> shared-IP
```

A single record has one key in one topic. Re-keying may create repartition topics. Never claim global Kafka ordering; ordering exists only inside a partition.

### Consumer groups

Use distinct, stable consumer-group names for separate projections. Document them in `docs/kafka.md`.

Examples:

```text
mulegraph-normalizer-v1
mulegraph-ledger-projector-v1
mulegraph-alert-projector-v1
mulegraph-graph-projector-v1
```

Do not randomly rename groups, because doing so changes offset history and replay behaviour.

---

## 6. Fraud-rule contract

### 6.1 Fan-out

One source account sends to multiple **distinct** destination accounts inside an event-time window.

Initial configurable defaults:

```text
window: 60 seconds
grace: 15 seconds
minimum distinct destinations: 5
minimum total amount: configurable
```

Group by `source_account_id`.

### 6.2 Fan-in

Multiple **distinct** source accounts send to one destination inside an event-time window.

Group by `destination_account_id`.

Five transactions from the same source count as one distinct source.

### 6.3 Shared device

Several distinct accounts use the same `device_id` during the configured period.

Group by `device_id` and count distinct account IDs, not transaction count alone.

### 6.4 Shared IP

Several distinct accounts use the same synthetic `ip_hash` during the configured period.

Group by `ip_hash`.

### 6.5 Circular flow

Circular-flow detection is not a full-graph scan and is not part of Phase 2.

Required constraints:

- candidate-first workflow
- Neo4j confirmation
- maximum path depth 3 or 4
- bounded time range
- chronologically increasing transfers
- amount-similarity tolerance
- distinct intermediate accounts
- deterministic duplicate-cycle suppression

### 6.6 Candidate and alert semantics

- A **candidate** is suspicious evidence emitted by a rule.
- An **alert** is an investigation record created after configured evaluation.
- An alert does not prove fraud.
- Not every candidate requires Neo4j confirmation.
- Circular-flow candidates do require graph confirmation.

---

## 7. PostgreSQL model

Use Flyway migrations. Do not use Hibernate automatic schema creation for committed environments.

Minimum tables:

```text
transactions
alerts
alert_accounts
alert_transactions
rule_configurations
```

Optional table only when justified:

```text
processed_events
```

### Required transaction constraints

- `transaction_id` primary key or unique business key.
- `event_id` unique and not null.
- positive amount check.
- source and destination must differ.
- indexes on:
  - `(source_account_id, occurred_at)`
  - `(destination_account_id, occurred_at)`
  - `(device_id, occurred_at)` when device is present
  - `(ip_hash, occurred_at)` when IP hash is present

### Required alert constraints

- `alert_id` primary key.
- `deduplication_key` unique.
- rule type not null.
- detected time not null.

Idempotent PostgreSQL writes must rely on constraints plus an explicit conflict strategy. Do not use read-then-insert as the only duplicate defence.

PostgreSQL is the auditable ledger projection built from Kafka. Do not call it the only source of truth when the API writes to Kafka first.

---

## 8. Neo4j model

Add only in Phase 3.

Nodes:

```text
(:Account {account_id})
(:Device {device_id})
(:IPAddress {ip_hash})
```

Relationships:

```text
(:Account)-[:TRANSFERRED_TO]->(:Account)
(:Account)-[:USED_DEVICE]->(:Device)
(:Account)-[:USED_IP]->(:IPAddress)
```

Required unique constraints:

```text
Account.account_id
Device.device_id
IPAddress.ip_hash
```

Prefer an aggregated transfer relationship containing:

```text
first_seen
last_seen
transaction_count
total_amount_minor
latest_transaction_id
```

Do not store every transaction as a permanent graph relationship unless an ADR and benchmark justify it. Full transaction details remain in PostgreSQL.

Graph writes must consume `graph.updates`; never rely on an in-memory durable queue. Use idempotent Cypher such as `MERGE` with deterministic update rules.

---

## 9. Error, retry, and late-event policy

### 9.1 Invalid data

- Structurally invalid HTTP requests: reject synchronously with a safe 4xx response.
- Events that pass HTTP validation but fail internal normalization: publish to `transactions.invalid` with reason metadata that does not expose secrets.
- Poison events that repeatedly fail technical processing: publish to `processing.dead-letter` after bounded retries.

### 9.2 Downstream outage

- Kafka unavailable: API must not return 202; return a safe 503-style response.
- PostgreSQL unavailable: ledger consumer fails/retries; Kafka retains events; lag grows.
- Neo4j unavailable: ingestion and ledger continue; graph lag grows; projector catches up later.
- A failed external write must not result in an offset being committed as successful.

### 9.3 Late events

Initial policy:

```text
within grace period -> include in original event-time window
after grace period  -> persist in ledger, count/record as late, do not silently rewrite a closed live window
```

Historical re-evaluation is a later feature, not an implicit promise.

---

## 10. Repository structure

Create the minimum structure needed for the active phase. The intended final logical structure is:

```text
mulegraph/
├── src/main/java/com/mulegraph/
│   ├── MuleGraphApplication.java
│   ├── ingestion/
│   ├── normalization/
│   ├── ledger/
│   ├── stream/
│   ├── detection/
│   ├── alerts/
│   ├── graph/
│   ├── configuration/
│   ├── security/
│   ├── observability/
│   └── shared/
├── src/main/resources/
│   ├── application.yml
│   ├── application-api.yml
│   ├── application-normalizer.yml
│   ├── application-ledger-projector.yml
│   ├── application-stream-worker.yml
│   ├── application-alert-projector.yml
│   ├── application-graph-projector.yml
│   └── db/migration/
├── src/test/
├── tools/
│   ├── transaction-generator/
│   └── evaluation/
├── infrastructure/
│   ├── docker/
│   ├── kubernetes/
│   └── helm/
├── scripts/
│   ├── demo/
│   └── verification/
├── docs/
│   ├── architecture.md
│   ├── version-matrix.md
│   ├── kafka.md
│   ├── testing.md
│   ├── runbooks/
│   ├── decisions/
│   └── evidence/
├── .env.example
├── docker-compose.yml
├── pom.xml
├── mvnw
├── mvnw.cmd
├── README.md
└── PROJECT_STATE.md
```

Do not create empty placeholder packages for all future phases. Add them when needed.

---

## 11. Agent operating contract

For every phase or subtask, the agent must follow this workflow.

### Before editing

1. Read this file, `PROJECT_STATE.md`, relevant ADRs, and existing tests.
2. Inspect the current repository rather than assuming its contents.
3. Run the current baseline tests.
4. Produce an implementation plan listing:
   - files to add/change
   - technical choices
   - tests to add
   - verification commands
   - risks and exclusions
5. State what is explicitly out of scope.

### During implementation

- Work only on the active phase.
- Prefer the smallest coherent change.
- Do not hide failing tests, skip tests, or weaken assertions to pass.
- Do not replace production integrations with mocks in integration tests when Testcontainers is required.
- Do not invent command output.
- Do not claim a container, endpoint, test, or query worked unless it was actually executed.
- Preserve useful error logs in the walkthrough.
- Keep secrets out of source control.

### After implementation

Produce a walkthrough containing:

1. Summary of the change.
2. Exact file list.
3. Exact commands executed.
4. Test results with counts.
5. Runtime verification evidence.
6. Known limitations.
7. Concepts the owner should understand.
8. Three interview questions.
9. Updated `PROJECT_STATE.md`.
10. A proposed Git commit message.

### Stop gate

Do not begin the next phase automatically. Stop after the current phase’s acceptance criteria and showcase have passed.

If a required test cannot be run because a dependency is missing, report the blocker and do not mark the phase complete.

---

## 12. Evidence and showcase standard

Every phase must create:

```text
docs/evidence/phase-N/README.md
scripts/demo/phase-N.ps1
```

A shell script may also be added for Linux/macOS:

```text
scripts/demo/phase-N.sh
```

The evidence README must include:

- environment versions
- startup commands
- requests/events used
- expected result
- actual result
- test command and test count
- relevant database query output
- relevant Kafka/consumer evidence
- screenshots to capture manually, where useful
- failure demonstration, where required
- cleanup commands

A phase is not complete because code compiles. It is complete only when its behaviour is demonstrated.

---

# IMPLEMENTATION ROADMAP

## Phase 0 — Workspace, decisions, and tool verification

### Goal

Create a controlled, reproducible project workspace without implementing business functionality.

### Tasks

1. Initialize Git repository if absent.
2. Add this context file at repository root as `MuleGraph_CONTEXT.md`.
3. Add Antigravity workspace rules.
4. Create:
   - `PROJECT_STATE.md`
   - `docs/version-matrix.md`
   - `docs/architecture.md`
   - `docs/decisions/ADR-0001-modular-monolith.md`
   - `.gitignore`
5. Verify installed tools:
   - Java 21
   - Git
   - Docker and Docker Compose
   - Maven or ability to use Maven Wrapper after scaffolding
   - Python 3
6. Record exact versions; do not install application dependencies yet.
7. Draw the accepted architecture and transaction sequence in Mermaid.

### Acceptance criteria

- Tool version commands executed and recorded.
- No production code generated.
- Architecture matches this file.
- No Redis, Neo4j, Kubernetes, or microservice code added.
- `PROJECT_STATE.md` says active phase is Phase 0 and lists blockers.

### Showcase

Run a single verification script that prints tool versions and validates Docker is reachable. Save actual output in `docs/evidence/phase-0/README.md`.

---

## Phase 1A — Spring Boot foundation

### Goal

Create the smallest healthy Java 21 Spring Boot application.

### Tasks

1. Generate a Maven Spring Boot project using a verified stable version.
2. Commit Maven Wrapper.
3. Add only foundational dependencies:
   - Spring Web
   - Validation
   - Actuator
   - test starter
4. Add package structure only for current needs.
5. Configure `/actuator/health`, liveness, and readiness groups.
6. Add application startup test and health endpoint test.
7. Add structured, environment-based configuration placeholders without secrets.

### Acceptance criteria

- `./mvnw test` or `mvnw.cmd test` passes.
- Application starts on the configured port.
- Readiness and liveness endpoints return healthy responses.
- No Kafka or PostgreSQL dependency yet.

### Showcase

Start the application and demonstrate:

```text
GET /actuator/health
GET /actuator/health/liveness
GET /actuator/health/readiness
```

Store command output and test count.

---

## Phase 1B — Transaction API contract and security

### Goal

Validate and authenticate synthetic transaction requests without Kafka yet.

### Tasks

1. Add request/response DTOs.
2. Add API-key filter/interceptor using an environment variable.
3. Add validation and safe global exception handling.
4. Add request-size configuration.
5. Create the internal transaction event mapper.
6. Use a temporary application port interface for publishing; the test implementation must not be presented as durable acceptance.
7. Until Kafka exists, return a clearly temporary non-production response or keep the endpoint integration-test-only. Do not incorrectly return durable 202.

### Acceptance criteria

Tests cover:

- missing API key -> 401/403 according to chosen documented convention
- invalid API key -> rejected
- valid payload -> mapped correctly
- zero/negative amount -> 400
- same source/destination -> 400
- malformed UUID/timestamp/currency -> 400
- internal exception -> generic 5xx body without stack trace

### Showcase

Run automated API tests and a local request matrix. Explain why durable `202 Accepted` is deferred until Phase 1C.

---

## Phase 1C — Reliable Kafka ingestion

### Goal

Publish accepted transaction events durably to Kafka and return 202 only after broker acknowledgement.

### Tasks

1. Add Kafka dependency and verified client configuration.
2. Add Docker Compose Kafka in KRaft mode.
3. Create required Phase 1 topics:
   - `transactions.raw`
   - `transactions.validated`
   - `transactions.invalid`
   - `processing.dead-letter`
4. Implement producer adapter for `transactions.raw`.
5. Key records by `transaction_id`.
6. Wait for publish acknowledgement before returning 202.
7. Map broker timeout/failure to a safe 503 response.
8. Add Testcontainers Kafka integration tests.
9. Document acknowledgement semantics and limitations.

### Acceptance criteria

- Valid request with Kafka available -> 202 and event exists in `transactions.raw`.
- Kafka unavailable -> no 202; safe failure response.
- Producer uses verified durability settings.
- Integration test proves the record key and payload.
- No PostgreSQL write occurs in the API request path.

### Showcase

1. Start Kafka and API.
2. Send one valid request.
3. Display 202 response.
4. Consume the exact event from `transactions.raw`.
5. Stop Kafka or point to an unavailable broker.
6. Repeat request and show non-202 behaviour.

---

## Phase 1D — Normalization and PostgreSQL ledger

### Goal

Consume raw events, publish validated events, and create an idempotent audit projection in PostgreSQL.

### Tasks

1. Add PostgreSQL and Flyway.
2. Add normalizer consumer:
   - raw -> validated
   - internal-invalid -> invalid
3. Add ledger projector consuming `transactions.validated`.
4. Create `transactions` migration with required constraints and indexes.
5. Implement idempotent insert using unique constraints and explicit conflict handling.
6. Add consumer retry and DLQ policy for technical poison events.
7. Add Testcontainers integration tests using Kafka and PostgreSQL.
8. Add correlation IDs to structured logs.

### Acceptance criteria

- One valid API request eventually creates one ledger row.
- Replaying the same event does not create a second row.
- Re-submitting the same `transaction_id` with a different event ID does not duplicate the business transaction; behaviour is documented.
- Invalid normalized event goes to `transactions.invalid` and not the ledger.
- PostgreSQL outage does not lose Kafka data; after recovery, projector catches up.

### Showcase

1. Send a valid request.
2. Query PostgreSQL and show the row.
3. Replay the event twice.
4. Query row count and show it remains one.
5. Stop PostgreSQL, send events, show consumer failure/lag.
6. Restart PostgreSQL and show rows catch up.

### Phase 1 completion gate

Do not start fraud rules until all Phase 1A–1D tests and demonstrations pass.

---

## Phase 2A — Kafka Streams foundation and event-time policy

### Goal

Create a recoverable Kafka Streams topology without implementing all rules at once.

### Tasks

1. Add Kafka Streams.
2. Configure timestamp extraction from `occurred_at`.
3. Configure application ID and state directory.
4. Configure exactly-once-v2 only if compatible and verified; document its Kafka-only scope.
5. Add topology tests for timestamp and re-keying behaviour.
6. Add late-event metric and explicit grace policy.
7. Document internal repartition topics.

### Acceptance criteria

- Topology reads `transactions.validated`.
- Event time is proven to come from `occurred_at`.
- Source, destination, device, and IP re-key operations are testable.
- Restart/state-recovery test plan exists.

### Showcase

Feed out-of-order events and display the event-time assignments used by the topology.

---

## Phase 2B — Fan-out rule

### Goal

Detect one source sending to multiple distinct destinations in a bounded event-time window.

### Tasks

1. Add typed rule configuration.
2. Implement distinct-destination aggregation.
3. Track transaction count and total amount separately.
4. Emit deterministic `FAN_OUT` candidates.
5. Add candidate deduplication logic.
6. Add topology and integration tests for boundaries, duplicates, and late events.

### Acceptance criteria

- Four distinct destinations do not trigger when threshold is five.
- Five distinct destinations within window trigger one candidate.
- Five transfers to the same destination do not trigger distinct-destination threshold.
- Duplicate transaction does not inflate distinct count or create duplicate candidate.
- Event inside grace is handled as documented.
- Event after grace is not silently applied to a closed window.

### Showcase

Run a deterministic fan-out scenario generator and consume the resulting candidate with evidence fields.

---

## Phase 2C — Fan-in rule

Mirror Phase 2B using `destination_account_id` and distinct source accounts.

### Showcase requirement

Prove that `A -> X` repeated five times is one distinct source, while `A/B/C/D/E -> X` produces one candidate.

---

## Phase 2D — Shared-device and shared-IP rules

### Goal

Detect multiple distinct accounts sharing a device or IP hash.

### Acceptance criteria

- Count distinct accounts, not raw events.
- Device and IP rules have separate configurations and candidate types.
- Synthetic legitimate repeat activity from one account does not trigger a distinct-account threshold.
- PII rule is respected: only synthetic hash values are used.

### Showcase

Run one normal and one suspicious scenario for each rule and display candidate output.

---

## Phase 2E — Alert projection and deduplication

### Goal

Turn eligible candidates into investigation alerts and persist them idempotently.

### Tasks

1. Define candidate-to-alert policy per rule.
2. Publish `fraud.alerts`.
3. Create alert tables and migrations.
4. Persist alert, involved accounts, and involved transaction IDs.
5. Use deterministic unique deduplication keys.
6. Add alert status such as `OPEN`, not a verdict of fraud.

### Acceptance criteria

- Candidate creates one alert according to policy.
- Replayed candidate does not create duplicate alert.
- Evidence links are queryable.
- Normal scenarios create no alerts under test configuration.

### Showcase

Send a complete fan-out scenario, then query and display the alert plus its linked accounts and transactions.

---

## Phase 3 — Neo4j graph projection

### Goal

Build an asynchronous, replay-safe relationship projection.

### Tasks

1. Add Neo4j to Docker Compose.
2. Add graph constraints.
3. Publish `graph.updates` from validated transactions.
4. Add graph projector with idempotent Cypher.
5. Create aggregated transfers and account-device/IP relationships.
6. Add bounded retry and DLQ handling.
7. Add Testcontainers Neo4j integration tests.

### Acceptance criteria

- One transaction creates/updates expected nodes and relationships.
- Replaying an update does not double-count incorrectly.
- Neo4j outage does not stop API, normalization, ledger, or stream rules.
- Graph backlog catches up after Neo4j recovery.

### Showcase

1. Show a Neo4j query returning the transaction relationship and shared identities.
2. Stop Neo4j.
3. Send several transactions.
4. Show graph-projector failures/lag while ingestion remains healthy.
5. Restart Neo4j and show the graph catches up.

---

## Phase 4 — Bounded circular-flow confirmation

### Goal

Detect synthetic cycles without unbounded graph scans.

### Tasks

1. Define candidate-generation trigger.
2. Implement bounded Neo4j path query with maximum depth.
3. Verify chronological order from evidence.
4. Apply configured amount-tolerance rule.
5. Suppress duplicate representations of the same cycle.
6. Store alert evidence and query latency.

### Acceptance criteria

- `A -> B -> C -> A` inside configured bounds creates one circular-flow alert.
- Similar non-cycle does not create the alert.
- Path beyond maximum depth does not run as an unlimited query.
- Wrong chronological order or amount tolerance fails confirmation as configured.
- Query plan and measured latency are recorded; no invented numbers.

### Showcase

Run a deterministic positive and negative scenario and display the bounded Cypher query, result, alert, and measured latency.

---

## Phase 5 — Synthetic evaluation harness

### Goal

Measure detection quality using labelled synthetic data.

### Tasks

1. Build deterministic scenario generator with random seed.
2. Generate normal, fan-in, fan-out, shared-device, shared-IP, and circular scenarios.
3. Store ground-truth scenario IDs and labels.
4. Match alerts to scenarios.
5. Calculate precision, recall, F1, false-positive rate, false-negative rate, and detection latency.
6. Produce machine-readable and human-readable reports.

### Acceptance criteria

- Metrics are calculated from actual runs.
- Dataset size, seed, rule configuration, and environment are recorded.
- Unmatched alerts and missed scenarios are inspectable.
- No metric appears in README or resume unless backed by evidence.

### Showcase

Run one command that generates data, sends it, waits with a bounded timeout, evaluates results, and writes a report under `docs/evidence/phase-5/`.

---

## Phase 6 — Reliability and observability

### Goal

Make system behaviour visible and test recoverability.

### Tasks

1. Add structured JSON logging.
2. Add OpenTelemetry context propagation through Kafka headers.
3. Add Prometheus metrics.
4. Add Grafana dashboards.
5. Track at minimum:
   - API count and latency
   - producer failures
   - processed transactions
   - duplicates
   - late events
   - candidates and alerts by rule
   - DLQ count
   - PostgreSQL write latency
   - Neo4j projection lag
   - consumer lag where measurable
6. Add runbooks for Kafka, PostgreSQL, and Neo4j outages.
7. Add failure-injection and replay tests.

### Acceptance criteria

- Dashboard panels show real generated traffic.
- A trace or correlated log path can connect API acceptance to downstream processing where implemented.
- Limitations of Kafka trace propagation are documented honestly.
- Failure demos match the runbooks.

### Showcase

Generate traffic and demonstrate dashboards, one trace/correlation chain, one DLQ event, and one downstream outage recovery.

---

## Phase 7 — Kubernetes and Helm showcase

### Goal

Demonstrate Kubernetes knowledge after local correctness is established.

### Local cluster

- Use `kind` unless an ADR selects another local distribution.
- Use a named cluster such as `mulegraph`.
- Keep resource requirements suitable for a developer laptop.

### Application deployment

Deploy the same immutable MuleGraph image as separate Kubernetes Deployments using profiles:

```text
mulegraph-api
mulegraph-normalizer
mulegraph-ledger-projector
mulegraph-stream-worker
mulegraph-alert-projector
mulegraph-graph-projector
```

Required Kubernetes concepts:

- Namespace
- Deployment
- Service
- ConfigMap
- Secret references
- readiness and liveness probes
- resource requests and limits
- rolling update strategy
- PodDisruptionBudget where meaningful
- HorizontalPodAutoscaler for stateless API only after metrics are available
- persistent storage for stateful dependencies
- network policy if supported by local CNI and verified

### Stateful dependencies

For local demonstration:

- Kafka through a verified Kubernetes-native deployment approach such as Strimzi; do not create a fake stateless Kafka Deployment.
- PostgreSQL and Neo4j may use verified charts/operators or explicit StatefulSets with PVCs for local demonstration.
- Document that production would generally prefer managed stateful services where appropriate.
- Never claim high availability unless multi-replica failure behaviour is actually configured and tested.

### Helm

Create a Helm chart for MuleGraph application workloads. Values must control:

- image repository and immutable tag
- profiles
- replica counts
- resource requests/limits
- probes
- external service endpoints
- secrets by reference, not plaintext values

### Acceptance criteria

- Fresh kind cluster can be created from documented commands.
- Helm lint passes.
- Helm install/upgrade succeeds.
- All application pods become ready.
- API can accept a synthetic event and downstream processing works.
- Deleting an API pod demonstrates self-healing.
- Rolling image update completes without sending traffic to unready pods.
- Scaling a Kafka consumer is explained relative to partition count.
- Neo4j outage shows backlog/recovery rather than total ingestion failure.

### Showcase

Provide a scripted demo:

1. Create cluster.
2. Install dependencies.
3. Build/load immutable image.
4. Helm install MuleGraph.
5. Wait for readiness.
6. Send transaction and verify ledger/alert flow.
7. Delete an API pod and show replacement.
8. Perform rolling upgrade.
9. Scale a projector and show Kafka consumer assignment/limitations.
10. Simulate Neo4j outage and recovery.
11. Clean up cluster.

Save `kubectl get` outputs, rollout status, and relevant logs in `docs/evidence/phase-7/`.

---

## Phase 8 — CI/CD, security, and portfolio polish

### Goal

Create a reproducible delivery pipeline and honest project presentation.

### Tasks

1. GitHub Actions pipeline:
   - compile
   - unit tests
   - integration tests
   - package
   - container build
   - image vulnerability scan
   - Helm lint/template validation
   - optional kind smoke test when resource limits allow
2. Add dependency and secret scanning.
3. Add architecture diagrams, runbooks, API docs, event contracts, and demo guide.
4. Add measured results only.
5. Add resume bullets backed by evidence.

### Acceptance criteria

- Pipeline runs from a clean checkout.
- No committed credentials.
- README includes exact local and Kubernetes demos.
- Known limitations and production changes are explicit.

### Showcase

Open one successful CI run, show test evidence, image scan result, Helm validation, and a clean deployment from an immutable image tag.

---

## 13. Phase status template

`PROJECT_STATE.md` must use this format:

```markdown
# MuleGraph Project State

## Active phase
Phase X — Name

## Last verified commit
<git SHA or "not committed yet">

## Completed acceptance criteria
- ...

## Failing or blocked criteria
- ...

## Exact verification commands
```text
...
```

## Known limitations
- ...

## Next allowed task
One narrowly defined task only.
```

---

## 14. Definition of project success

The final project is successful only if the owner can demonstrate and explain:

1. Why Kafka sits before PostgreSQL.
2. Why 202 waits for broker acknowledgement.
3. Why fan-in and fan-out require different keys.
4. How repartitioning and consumer groups work.
5. Why event time differs from processing time.
6. How grace periods affect late events.
7. Why at-least-once delivery requires idempotency.
8. Why Kafka exactly-once does not cover Neo4j/PostgreSQL globally.
9. Why PostgreSQL and Neo4j have different jobs.
10. How downstream outages create lag rather than silent loss.
11. How duplicate transactions and alerts are suppressed.
12. How Kubernetes restarts and scales workloads, including partition-count limits.
13. Which metrics were actually measured and under what conditions.

The agent must optimize for correctness, evidence, and owner understanding—not maximum generated code.
