#!/bin/bash
set -e

echo "=== MuleGraph Final Pre-Phase-3 Gate Demo ==="

echo -e "\n1. Start Infrastructure"
docker compose down -v
docker compose up -d
sleep 15

echo -e "\n2. Exact Transaction Replay"
TX_ID="55555555-5555-5555-5555-555555555555"
DATA='{
  "schema_version": 1,
  "transaction_id": "'$TX_ID'",
  "source_system": "WEB",
  "source_account_id": "11111111-1111-1111-1111-111111111111",
  "destination_account_id": "22222222-2222-2222-2222-222222222222",
  "amount_minor": 1000,
  "currency": "USD",
  "occurred_at": "'$(date -u +"%Y-%m-%dT%H:%M:%SZ")'"
}'

curl -s -X POST http://localhost:8080/v1/transactions \
  -H "Content-Type: application/json" \
  -H "X-API-KEY: dev-local-api-key" \
  -d "$DATA" > /dev/null
echo "First insert complete."

# Replay
curl -s -X POST http://localhost:8080/v1/transactions \
  -H "Content-Type: application/json" \
  -H "X-API-KEY: dev-local-api-key" \
  -d "$DATA" > /dev/null
echo "Replay insert complete. Check DB for duplicates (should be 1)."

echo -e "\n3. Conflicting Transaction Identity"
CONFLICT_DATA='{
  "schema_version": 1,
  "transaction_id": "'$TX_ID'",
  "source_system": "WEB",
  "source_account_id": "11111111-1111-1111-1111-111111111111",
  "destination_account_id": "33333333-3333-3333-3333-333333333333",
  "amount_minor": 9000,
  "currency": "USD",
  "occurred_at": "'$(date -u +"%Y-%m-%dT%H:%M:%SZ")'"
}'
curl -s -X POST http://localhost:8080/v1/transactions \
  -H "Content-Type: application/json" \
  -H "X-API-KEY: dev-local-api-key" \
  -d "$CONFLICT_DATA" > /dev/null
echo "Conflict insert sent. Should be routed to dead-letter."

echo -e "\nGate demo commands executed."
