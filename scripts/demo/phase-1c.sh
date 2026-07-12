#!/bin/bash
set -e

echo "=== MuleGraph Phase 1C Verification ==="

export PATH="/opt/homebrew/opt/openjdk@21/bin:$PATH"
export DOCKER_API_VERSION=1.41

# Run tests
echo "1. Running unit & integration tests..."
./mvnw test

# Start Docker Compose
echo "2. Starting Kafka (KRaft) via Docker Compose..."
docker compose up -d

# Wait for Kafka to become healthy/accessible
echo "Waiting for Kafka to be ready..."
sleep 20

# Start the application in the background
echo "3. Starting application..."
./mvnw spring-boot:run &
APP_PID=$!

echo "Waiting for application to become healthy..."
sleep 20

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
  "occurred_at": "2026-07-13T10:20:30Z"
}
EOF
)

# Test Valid Payload
echo "4. Testing valid payload (Expect 202 Accepted)"
RESPONSE=$(curl -s -w "\nHTTP_STATUS: %{http_code}" -X POST http://localhost:8080/api/v1/transactions \
  -H "X-API-Key: dev-local-api-key" \
  -H "Content-Type: application/json" \
  -d "$VALID_PAYLOAD")
echo "$RESPONSE"

echo "5. Verifying message on Kafka topic transactions.raw"
# Read from topic using console consumer
docker exec mulegraph-kafka kafka-console-consumer --bootstrap-server localhost:9092 --topic transactions.raw --from-beginning --timeout-ms 5000 --max-messages 1 || true

# Stop the application
echo "6. Stopping application..."
kill $APP_PID
wait $APP_PID 2>/dev/null || true

# Tear down Docker Compose
echo "7. Tearing down Kafka..."
docker compose down -v

echo "=== End Verification ==="
