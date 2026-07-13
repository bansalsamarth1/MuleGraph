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
- None.

## Exact verification commands
```bash
./scripts/demo/phase-2e.sh
```

## Known limitations
- `Testcontainers` test `AlertProjectorTest.java` is `@Disabled` due to host Docker API version incompatibility.
- Still using synthetic currency (INR) and dummy API authentication.

## Next allowed task
Execute Phase 3.
