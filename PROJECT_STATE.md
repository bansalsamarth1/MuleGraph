# MuleGraph Project State

## Active phase
Phase 2E — Alert Projection and Persistence

## Last verified commit
not committed yet

## Current Phase
Phase 2 (Fraud Detection Topologies) is active. Alert Deduplication logic and Threshold properties are implemented and tested. Phase 1D (Ledger & Validation) was successfully retrofitted and verified.

## Progress
- [x] Phase 1A: Domain, API, and Basic Ingestion
- [x] Phase 1B: PostgreSQL Ledger setup
- [x] Phase 1C: Kafka Streams basic setup
- [x] Phase 1D: Validation pipeline (TransactionNormalizationTopology)
- [x] Phase 2: Fraud Detection Topologies (FanOut, FanIn, SharedDevice, SharedIp)
- [x] Alert Deduplication via ON CONFLICT DO NOTHING
- [ ] Phase 3: Neo4j and Complex Queries

## Completed acceptance criteria
- Configured Postgres using Docker Compose (`mulegraph-postgres` on port 5432).
- Enabled Flyway with migration scripts `V1__Create_Transaction_Tables.sql` and `V2__Create_Alert_Tables.sql`.
- Added `AlertProjector` component with `@KafkaListener(topics = "fraud.alerts")`.
- Idempotently inserted incoming `AlertEvent` into `alerts` table.
- Idempotently inserted corresponding `involvedAccounts` into `alert_accounts` mapping table.
- Idempotently inserted corresponding `transactionIds` into `alert_transactions` mapping table.
- Adjusted Kafka Streams properties (`commit.interval.ms: 1000`) to guarantee prompt local emission.
- Fixed `TransactionTimestampExtractor` to safely handle `FraudCandidateEvent`.
- Verified persistence flow locally using `./scripts/demo/phase-2e.sh`.

## Failing or blocked criteria
### Status: BLOCKED

### Known Blocker:
Docker Desktop API `1.54` incompatibility with `docker-java` 3.4.0 (Testcontainers 1.20.4).
- Testcontainers fails to execute `/info` and image pulls (`ContainerFetch`) due to Docker Desktop rejecting older API versions (`BadRequestException Status 400`).
- User provided workaround `TESTCONTAINERS_RYUK_DISABLED=true` (via `src/test/resources/testcontainers.properties`) was applied but does not resolve the `docker-java` 400 Bad Request exception.
- Per strict rules, I am reporting this blocker clearly and leaving the phase incomplete rather than skipping the tests or replacing them with mocks.

### Completed Work Before Blocker:
- [x] Restored `transactions.invalid` validation pipeline (Phase 1D).
- [x] Implemented Dead-Letter logic via `@KafkaListener` error handlers and Streams `DeserializationExceptionHandler`.
- [x] Refactored `AlertProjector` to use `INSERT ... ON CONFLICT DO NOTHING` + `SELECT id` for strict deduplication lookup.
- [x] Added `ConflictingTransactionException` logic to `TransactionProjector` to ensure identical payload for same `transaction_id`.
- [x] Fixed topology keys, disabled Kafka Streams in test contexts appropriately, and fixed `TopologyTestDriver` concurrent state-dir clashes.
- [x] Generated validation scripts in `scripts/demo/` and populated `docs/evidence/`.

## Exact verification commands
```bash
./scripts/demo/pre-phase-3-gate.sh
```

## Known limitations
- Testcontainers tests fail due to host Docker API version incompatibility.
- Still using synthetic currency (INR) and dummy API authentication.

## Next allowed task
Resolve the Docker API mismatch blocker before starting Neo4j (Phase 3).
