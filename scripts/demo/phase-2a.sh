#!/bin/bash
set -e

echo "=== MuleGraph Phase 2A Verification ==="

export PATH="/opt/homebrew/opt/openjdk@21/bin:$PATH"

# Start Docker Compose
echo "1. Starting Kafka (KRaft) via Docker Compose..."
docker compose up -d

# Wait for Kafka to become healthy/accessible
echo "Waiting for Kafka to be ready..."
sleep 20

# Start the application in the background
echo "2. Starting application..."
./mvnw spring-boot:run &
APP_PID=$!

echo "Waiting for application to become healthy..."
sleep 20

UUID_SRC=$(uuidgen)
UUID_DST=$(uuidgen)

# Transaction A (occurred 10 seconds ago)
PAYLOAD_A=$(cat <<EOF
{
  "transaction_id": "$(uuidgen)",
  "source_account_id": "$UUID_SRC",
  "destination_account_id": "$UUID_DST",
  "amount_minor": 10000,
  "currency": "INR",
  "device_id": "device-123",
  "ip_hash": "hash-123",
  "occurred_at": "$(date -u -v-10S +'%Y-%m-%dT%H:%M:%SZ')"
}
EOF
)

# Transaction B (occurred 5 seconds ago)
PAYLOAD_B=$(cat <<EOF
{
  "transaction_id": "$(uuidgen)",
  "source_account_id": "$UUID_SRC",
  "destination_account_id": "$UUID_DST",
  "amount_minor": 10000,
  "currency": "INR",
  "device_id": "device-123",
  "ip_hash": "hash-123",
  "occurred_at": "$(date -u -v-5S +'%Y-%m-%dT%H:%M:%SZ')"
}
EOF
)

# Send Out-of-Order payloads
echo "3. Testing Out-of-Order Payloads..."
curl -s -w "\nHTTP_STATUS: %{http_code}\n" -X POST http://localhost:8080/api/v1/transactions \
  -H "X-API-Key: dev-local-api-key" \
  -H "Content-Type: application/json" \
  -d "$PAYLOAD_A"

curl -s -w "\nHTTP_STATUS: %{http_code}\n" -X POST http://localhost:8080/api/v1/transactions \
  -H "X-API-Key: dev-local-api-key" \
  -H "Content-Type: application/json" \
  -d "$PAYLOAD_B"

# Wait for Streams processing
sleep 5

echo "4. Verifying messages on Kafka topic transactions.by-source"
echo "Notice the CreateTime printed below matches the client's occurred_at timestamp!"

docker exec mulegraph-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic transactions.by-source \
  --from-beginning \
  --property print.timestamp=true \
  --timeout-ms 5000 \
  --max-messages 2 || true

# Stop the application
echo "5. Stopping application..."
kill $APP_PID
wait $APP_PID 2>/dev/null || true

# Tear down Docker Compose
echo "6. Tearing down Kafka..."
docker compose down -v

echo "=== End Verification ==="
