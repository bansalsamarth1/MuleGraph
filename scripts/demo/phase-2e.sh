#!/bin/bash
set -e

echo "=== MuleGraph Phase 2E Verification ==="

export PATH="/opt/homebrew/opt/openjdk@21/bin:$PATH"

# 1. Start Infrastructure
echo "1. Starting infrastructure..."
docker compose up -d
sleep 10 # Wait for Kafka and Postgres

# 2. Start Application
echo "2. Starting application..."
./mvnw spring-boot:run &
APP_PID=$!

echo "Waiting for application..."
sleep 20

# 3. Simulate Fraud (Shared Device)
echo "3. Sending transactions to trigger Shared Device rule..."

# Transaction 1
curl -s -X POST http://localhost:8080/api/v1/transactions \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-local-api-key" \
  -d '{
    "transaction_id": "00000000-0000-0000-0000-000000000001",
    "source_account_id": "11111111-1111-1111-1111-111111111111",
    "destination_account_id": "99999999-9999-9999-9999-999999999999",
    "amount_minor": 5000,
    "currency": "USD",
    "timestamp": "'$(date -u +"%Y-%m-%dT%H:%M:%SZ")'",
    "device_id": "device-123",
    "ip_address": "192.168.1.100"
  }' > /dev/null

# Transaction 2
curl -s -X POST http://localhost:8080/api/v1/transactions \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-local-api-key" \
  -d '{
    "transaction_id": "00000000-0000-0000-0000-000000000002",
    "source_account_id": "22222222-2222-2222-2222-222222222222",
    "destination_account_id": "99999999-9999-9999-9999-999999999999",
    "amount_minor": 5000,
    "currency": "USD",
    "timestamp": "'$(date -u +"%Y-%m-%dT%H:%M:%SZ")'",
    "device_id": "device-123",
    "ip_address": "192.168.1.101"
  }' > /dev/null

# Transaction 3 (Should cross the threshold of 3 distinct accounts)
curl -s -X POST http://localhost:8080/api/v1/transactions \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-local-api-key" \
  -d '{
    "transaction_id": "00000000-0000-0000-0000-000000000003",
    "source_account_id": "33333333-3333-3333-3333-333333333333",
    "destination_account_id": "99999999-9999-9999-9999-999999999999",
    "amount_minor": 5000,
    "currency": "USD",
    "timestamp": "'$(date -u +"%Y-%m-%dT%H:%M:%SZ")'",
    "device_id": "device-123",
    "ip_address": "192.168.1.102"
  }' > /dev/null

echo "Waiting for stream processing and projection..."
sleep 10

# 4. Query PostgreSQL
echo "4. Verifying alerts in PostgreSQL..."
docker exec mulegraph-postgres psql -U muleuser -d mulegraph -c "SELECT alert_id, rule_type, primary_account_id, status FROM alerts;"
docker exec mulegraph-postgres psql -U muleuser -d mulegraph -c "SELECT alert_id, COUNT(*) as accounts FROM alert_accounts GROUP BY alert_id;"
docker exec mulegraph-postgres psql -U muleuser -d mulegraph -c "SELECT alert_id, COUNT(*) as transactions FROM alert_transactions GROUP BY alert_id;"

# 5. Clean up
echo "5. Stopping application and infrastructure..."
kill $APP_PID
wait $APP_PID 2>/dev/null || true
docker compose down -v

echo "=== End Verification ==="
