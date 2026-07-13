#!/bin/bash
set -e

echo "=== MuleGraph Phase 2B Verification: Fan-Out Rule ==="

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

echo "3. Sending 5 transactions from ONE source to FIVE distinct destinations..."

for i in {1..5}
do
  DEST_ID=$(uuidgen)
  PAYLOAD=$(cat <<EOF
{
  "transaction_id": "$(uuidgen)",
  "source_account_id": "$UUID_SRC",
  "destination_account_id": "$DEST_ID",
  "amount_minor": 10000,
  "currency": "INR",
  "device_id": "device-123",
  "ip_hash": "hash-123",
  "occurred_at": "$(date -u +'%Y-%m-%dT%H:%M:%SZ')"
}
EOF
  )

  echo "Sending transaction $i to destination $DEST_ID"
  curl -s -X POST http://localhost:8080/api/v1/transactions \
    -H "X-API-Key: dev-local-api-key" \
    -H "Content-Type: application/json" \
    -d "$PAYLOAD"
  echo ""
done

# Wait for Streams processing
echo "Waiting for Kafka Streams to aggregate and emit candidates..."
sleep 10

echo "4. Verifying candidates on Kafka topic fraud.candidates"
echo "We expect EXACTLY ONE candidate to be emitted."

docker exec mulegraph-kafka kafka-console-consumer \
  --bootstrap-server localhost:9092 \
  --topic fraud.candidates \
  --from-beginning \
  --timeout-ms 5000 \
  --max-messages 10 || true

# Stop the application
echo "5. Stopping application..."
kill $APP_PID
wait $APP_PID 2>/dev/null || true

# Tear down Docker Compose
echo "6. Tearing down Kafka..."
docker compose down -v

echo "=== End Verification ==="
