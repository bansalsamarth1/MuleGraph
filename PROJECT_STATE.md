# MuleGraph Project State

## Active phase
Pre-Phase-3 Final Gate

## Last verified commit
not committed yet

## Current Phase
Phase 2 (Fraud Detection Topologies) is completed. Alert Deduplication logic, optional device/IP logic, and transaction idempotency are fully implemented and tested. Phase 1D (Ledger & Validation) was successfully retrofitted and verified.

## Progress
- [x] Phase 1A: Domain, API, and Basic Ingestion
- [x] Phase 1B: PostgreSQL Ledger setup
- [x] Phase 1C: Kafka Streams basic setup
- [x] Phase 1D: Validation pipeline (TransactionNormalizationTopology)
- [x] Phase 2: Fraud Detection Topologies (FanOut, FanIn, SharedDevice, SharedIp)
- [x] Alert Deduplication via ON CONFLICT DO NOTHING
- [x] Final Pre-Phase-3 Architectural Validation and Testcontainers fix
- [ ] Phase 3: Neo4j and Complex Queries

## Completed acceptance criteria
- Configured Postgres using Docker Compose (`mulegraph-postgres` on port 5432).
- Added `AlertProjector` component with `@KafkaListener(topics = "fraud.alerts")` using strict `deduplication_key` idempotency.
- Idempotently inserted corresponding `involvedAccounts` into `alert_accounts` mapping table.
- Idempotently inserted corresponding `transactionIds` into `alert_transactions` mapping table.
- Relaxed ledger `device_id` and `ip_hash` constraints to allow missing (optional) data, filtering correctly in topology streams.
- Ensured strict `transaction_id` payload conflict rejection in `TransactionProjector`.
- Successfully resolved Docker API `400 Bad Request` blocker by upgrading Testcontainers to version `1.21.4`.
- All integration and unit tests pass with exactly 0 `@Disabled` required tests.
- Executed `pre-phase-3-gate.sh` successfully with a clean environment (`docker compose down -v`).

## Failing or blocked criteria
None.

## Exact verification commands
```bash
./mvnw clean verify
./scripts/demo/pre-phase-3-gate.sh
```

## Known limitations
- Still using synthetic currency (USD/INR) and dummy API authentication.
- No historical load testing has been performed.

## Next allowed task
Start Phase 3: Neo4j and Complex Queries.
