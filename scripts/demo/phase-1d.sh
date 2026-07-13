#!/bin/bash
set -e

echo "=== MuleGraph Phase 1D Validation Demo ==="

echo "1. Valid Transaction (API to Validated)"
curl -X POST http://localhost:8080/v1/transactions \
  -H "Content-Type: application/json" \
  -H "X-API-KEY: dev-local-api-key" \
  -d '{
    "schema_version": 1,
    "transaction_id": "00000000-0000-0000-0000-000000000001",
    "source_system": "WEB_UI",
    "source_account_id": "11111111-1111-1111-1111-111111111111",
    "destination_account_id": "22222222-2222-2222-2222-222222222222",
    "amount_minor": 1000,
    "currency": "USD",
    "device_id": "dev1",
    "ip_hash": "ip1",
    "occurred_at": "'$(date -u +"%Y-%m-%dT%H:%M:%SZ")'"
  }'

echo -e "\n2. Invalid Transaction (Invalid Schema, to transactions.invalid)"
curl -X POST http://localhost:8080/v1/transactions \
  -H "Content-Type: application/json" \
  -H "X-API-KEY: dev-local-api-key" \
  -d '{
    "schema_version": 999,
    "transaction_id": "00000000-0000-0000-0000-000000000002",
    "source_system": "WEB_UI",
    "source_account_id": "11111111-1111-1111-1111-111111111111",
    "destination_account_id": "22222222-2222-2222-2222-222222222222",
    "amount_minor": -500,
    "currency": "USD",
    "occurred_at": "'$(date -u +"%Y-%m-%dT%H:%M:%SZ")'"
  }'

echo -e "\n3. Malformed JSON (to Dead Letter Topic)"
curl -X POST http://localhost:8080/v1/transactions \
  -H "Content-Type: application/json" \
  -H "X-API-KEY: dev-local-api-key" \
  -d '{ "schema_version": 1, "transaction_id": '

echo -e "\nDone."
