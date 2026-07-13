# Phase 2E Evidence: Alert Projection and Persistence

## What Changed
- Validated `AlertProjector` functionality to listen on `fraud.alerts` topic.
- Fixed `TransactionTimestampExtractor` to correctly extract timestamps from both `InternalTransactionEvent` and `FraudCandidateEvent`. Kafka Streams was skipping `FraudCandidateEvent` due to negative (-1) timestamp extraction.
- Configured Kafka Streams `commit.interval.ms: 1000` for near real-time aggregate emission in `application.yml`.
- Verified end-to-end data flow: `POST /api/v1/transactions` -> `fraud.transactions` -> `transactions.raw` -> `transactions.by-device` -> `SharedDeviceTopology` -> `fraud.candidates` -> `AlertGenerationTopology` -> `fraud.alerts` -> `AlertProjector` -> PostgreSQL (`alerts`, `alert_accounts`, `alert_transactions`).

## Commands Executed
```bash
# Set Java 21 environment
export PATH="/opt/homebrew/opt/openjdk@21/bin:$PATH"
export JAVA_HOME="/opt/homebrew/opt/openjdk@21"

# Start infrastructure and app
docker compose up -d
./mvnw spring-boot:run

# Send transactions simulating a Shared Device rule trigger (3 distinct sources, same device_id)
curl -v -X POST http://localhost:8080/api/v1/transactions \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-local-api-key" \
  -d '{
    "transaction_id": "00000000-0000-0000-0000-000000000001",
    "source_account_id": "11111111-1111-1111-1111-111111111111",
    "destination_account_id": "99999999-9999-9999-9999-999999999999",
    "amount_minor": 50000,
    "currency": "INR",
    "device_id": "DEV-SHARED-001",
    "ip_hash": "hash123",
    "occurred_at": "2023-10-01T10:00:00Z"
  }'

# ... (Transactions 2 & 3 sent identically except for transaction_id and source_account_id)

# Verify Postgres
docker exec mulegraph-postgres psql -U muleuser -d mulegraph -c "SELECT * FROM alerts;"
docker exec mulegraph-postgres psql -U muleuser -d mulegraph -c "SELECT * FROM alert_accounts;"
docker exec mulegraph-postgres psql -U muleuser -d mulegraph -c "SELECT * FROM alert_transactions;"
```

## Actual Results
Alerts table output:
```text
               alert_id               |   rule_type   |          primary_account_id          | status |          created_at           
--------------------------------------+---------------+--------------------------------------+--------+-------------------------------
 23833f25-5e03-34a9-b2ad-370735e5c8b9 | SHARED_IP     | 35e5d160-921d-331d-9114-f1b4ee5f9d55 | OPEN   | 2026-07-13 10:39:18.707734+00
 89085fe5-27ca-30ef-a36f-5136e80955f9 | SHARED_DEVICE | 9022ee8d-1551-3fd8-8ce2-07dd9e12db39 | OPEN   | 2026-07-13 10:39:18.723408+00
```
`alert_accounts` output showed all 3 `account_ids` linked to both `alert_ids`.
`alert_transactions` output showed all 3 `transaction_ids` linked to both `alert_ids`.

## Known Limitations
- `Testcontainers` test `AlertProjectorTest.java` is `@Disabled` due to host Docker API version incompatibility.
- Relying on `commit.interval.ms: 1000` is useful for testing, but in production, we'll want to balance latency vs CPU usage.

## Next Allowed Task
Phase 2F (REST API for Alert Retrieval)
