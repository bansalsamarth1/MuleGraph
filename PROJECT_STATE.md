# MuleGraph Project State

## Active phase
Phase 2E — Alert Projection and Persistence

## Last verified commit
not committed yet

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
