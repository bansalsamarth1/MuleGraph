# Runbook: PostgreSQL Outage

## Symptoms
- Consumer lag for `mulegraph-ledger-projector` and `mulegraph-alert-projector` steadily increases.
- Application logs show JDBC `ConnectionException` or `DataAccessException`.
- API continues to return 202 successfully.

## Impact
- **Ingestion continues normally**. Kafka durably buffers all new events.
- **Audit ledger is delayed**. Transactions are not visible in the SQL DB.
- **Alerts are delayed**. Fraud candidates are detected by Kafka Streams but not persisted to SQL.

## Resolution
1. Verify Postgres container: `docker ps | grep postgres`.
2. Check PostgreSQL logs for connection limits or storage space issues.
3. Restart PostgreSQL: `docker compose restart mulegraph-postgres`.

## Recovery Behaviour
- `TransactionProjector` and `AlertProjector` will reconnect automatically.
- Projectors will consume the backlog from Kafka at maximum throughput.
- Consumer lag will decrease to 0.
- Idempotent writes ensure no duplicates if an outage occurred mid-batch.
