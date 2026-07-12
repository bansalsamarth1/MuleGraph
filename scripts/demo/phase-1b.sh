#!/bin/bash
set -e

echo "=== MuleGraph Phase 1B Verification ==="

export PATH="/opt/homebrew/opt/openjdk@21/bin:$PATH"

# Run tests
echo "1. Running unit tests..."
./mvnw test

# Start the application in the background
echo "2. Starting application..."
./mvnw spring-boot:run &
APP_PID=$!

echo "Waiting for application to become healthy..."
sleep 15

UUID_SRC=$(uuidgen)
UUID_DST=$(uuidgen)
UUID_TX=$(uuidgen)

VALID_PAYLOAD=$(cat <<EOF
{
  "transaction_id": "$UUID_TX",
  "source_account_id": "$UUID_SRC",
  "destination_account_id": "$UUID_DST",
  "amount_minor": 125050,
  "currency": "INR",
  "device_id": "device-123",
  "ip_hash": "e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855",
  "occurred_at": "2026-07-11T10:20:30Z"
}
EOF
)

# Test 1: Missing API Key
echo "3. Testing missing API key"
HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost:8080/api/v1/transactions \
  -H "Content-Type: application/json" \
  -d "$VALID_PAYLOAD")
echo "Result: $HTTP_STATUS"

# Test 2: Invalid API Key
echo "4. Testing invalid API key"
HTTP_STATUS=$(curl -s -o /dev/null -w "%{http_code}" -X POST http://localhost:8080/api/v1/transactions \
  -H "X-API-Key: invalid-key" \
  -H "Content-Type: application/json" \
  -d "$VALID_PAYLOAD")
echo "Result: $HTTP_STATUS"

# Test 3: Valid Payload
echo "5. Testing valid payload"
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST http://localhost:8080/api/v1/transactions \
  -H "X-API-Key: dev-local-api-key" \
  -H "Content-Type: application/json" \
  -d "$VALID_PAYLOAD")
echo "Result: $RESPONSE"

# Test 4: Same Source and Destination
echo "6. Testing same source and destination"
SAME_PAYLOAD=$(echo "$VALID_PAYLOAD" | sed "s/$UUID_DST/$UUID_SRC/g")
RESPONSE=$(curl -s -w "\n%{http_code}" -X POST http://localhost:8080/api/v1/transactions \
  -H "X-API-Key: dev-local-api-key" \
  -H "Content-Type: application/json" \
  -d "$SAME_PAYLOAD")
echo "Result: $RESPONSE"

# Stop the application
echo "7. Stopping application..."
kill $APP_PID
wait $APP_PID 2>/dev/null || true

echo "=== End Verification ==="
