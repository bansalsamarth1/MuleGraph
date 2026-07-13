#!/bin/bash
set -e

echo "MuleGraph Phase 4 - Circular Flow Confirmation Demo"
echo "==================================================="

echo "1. Starting infrastructure (Kafka, PostgreSQL, Neo4j)..."
docker compose up -d

echo "2. Waiting for services to be ready..."
sleep 15

echo "3. Starting MuleGraph application (graph-projector profile is active)..."
export SPRING_PROFILES_ACTIVE=api,normalizer,ledger-projector,stream-worker,alert-projector,graph-projector
./mvnw spring-boot:run &
APP_PID=$!

sleep 30

echo "4. Seeding a circular flow: A -> B -> C"
API_KEY="dev-local-api-key"
ACCOUNT_A="a0000000-0000-0000-0000-00000000000a"
ACCOUNT_B="b0000000-0000-0000-0000-00000000000b"
ACCOUNT_C="c0000000-0000-0000-0000-00000000000c"

# A -> B
curl -X POST http://localhost:8080/api/v1/transactions \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "11111111-1111-1111-1111-111111111111",
    "source_account_id": "'$ACCOUNT_A'",
    "destination_account_id": "'$ACCOUNT_B'",
    "amount_minor": 1000,
    "currency": "USD",
    "device_id": "dev1",
    "ip_hash": "ip1",
    "occurred_at": "2026-07-14T10:00:00Z"
  }'
echo " (A -> B sent)"
sleep 2

# B -> C
curl -X POST http://localhost:8080/api/v1/transactions \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "22222222-2222-2222-2222-222222222222",
    "source_account_id": "'$ACCOUNT_B'",
    "destination_account_id": "'$ACCOUNT_C'",
    "amount_minor": 1000,
    "currency": "USD",
    "device_id": "dev2",
    "ip_hash": "ip2",
    "occurred_at": "2026-07-14T10:05:00Z"
  }'
echo " (B -> C sent)"
sleep 5

echo "5. Sending candidate closing transaction: C -> A"
curl -X POST http://localhost:8080/api/v1/transactions \
  -H "X-API-Key: $API_KEY" \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "33333333-3333-3333-3333-333333333333",
    "source_account_id": "'$ACCOUNT_C'",
    "destination_account_id": "'$ACCOUNT_A'",
    "amount_minor": 1000,
    "currency": "USD",
    "device_id": "dev3",
    "ip_hash": "ip3",
    "occurred_at": "2026-07-14T10:10:00Z"
  }'
echo " (C -> A sent)"

echo "Waiting for Circular Flow Confirmation in Neo4j and Alert projection..."
sleep 15

echo "6. Checking generated alerts in PostgreSQL..."
docker compose exec postgres psql -U muleuser -d mulegraph -c "SELECT deduplication_key, rule_type, detected_at, primary_account_id FROM alerts WHERE rule_type='CIRCULAR_FLOW';"

echo "Demo complete! Shutting down."
kill $APP_PID
docker compose down
