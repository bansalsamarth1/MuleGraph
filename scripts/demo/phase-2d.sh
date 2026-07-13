#!/bin/bash
set -e

echo "=== MuleGraph Phase 2D Verification: Shared Device & Shared IP Rules ==="

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

# Scenario 1: Shared Device
DEVICE_ID="shared-device-999"
IP_HASH_SAFE="safe-hash-000"

echo "3. Sending 3 transactions from 3 distinct sources using ONE device..."

for i in {1..3}
do
  SRC_ID=$(uuidgen)
  PAYLOAD=$(cat <<EOF
{
  "transaction_id": "$(uuidgen)",
  "source_account_id": "$SRC_ID",
  "destination_account_id": "$(uuidgen)",
  "amount_minor": 10000,
  "currency": "INR",
  "device_id": "$DEVICE_ID",
  "ip_hash": "$IP_HASH_SAFE",
  "occurred_at": "$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
}
EOF
  )

  echo "Sending transaction $i (Shared Device scenario)"
  curl -s -X POST http://localhost:8080/api/v1/transactions \
    -H "X-API-Key: dev-local-api-key" \
    -H "Content-Type: application/json" \
    -d "$PAYLOAD"
  echo ""
done

# Scenario 2: Shared IP
DEVICE_ID_SAFE="safe-device-000"
IP_HASH="shared-ip-hash-888"

echo "4. Sending 3 transactions from 3 distinct sources using ONE IP hash..."

for i in {1..3}
do
  SRC_ID=$(uuidgen)
  PAYLOAD=$(cat <<EOF
{
  "transaction_id": "$(uuidgen)",
  "source_account_id": "$SRC_ID",
  "destination_account_id": "$(uuidgen)",
  "amount_minor": 10000,
  "currency": "INR",
  "device_id": "$DEVICE_ID_SAFE",
  "ip_hash": "$IP_HASH",
  "occurred_at": "$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
}
EOF
  )

  echo "Sending transaction $i (Shared IP scenario)"
  curl -s -X POST http://localhost:8080/api/v1/transactions \
    -H "X-API-Key: dev-local-api-key" \
    -H "Content-Type: application/json" \
    -d "$PAYLOAD"
  echo ""
done

# Wait for Streams processing
echo "Waiting for Kafka Streams to aggregate and emit candidates..."
sleep 15

echo "5. Verifying candidates on Kafka topic fraud.candidates"
echo "We expect EXACTLY TWO candidates to be emitted (one SHARED_DEVICE, one SHARED_IP)."

docker exec mulegraph-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic fraud.candidates \
  --from-beginning \
  --timeout-ms 5000 \
  --max-messages 10 || true

# Stop the application
echo "6. Stopping application..."
kill $APP_PID
wait $APP_PID 2>/dev/null || true

# Tear down Docker Compose
echo "7. Tearing down Kafka..."
docker compose down -v

echo "=== End Verification ==="
