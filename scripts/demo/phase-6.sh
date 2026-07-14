#!/usr/bin/env bash
set -e

echo "============================================="
echo " MuleGraph Phase 6: Observability Showcase"
echo "============================================="

echo "1. Cleaning previous state..."
docker compose down -v
rm -rf /tmp/kafka-streams

echo "2. Starting infrastructure (Kafka, Postgres, Neo4j, Prometheus, Grafana, OTEL)..."
docker compose up -d
sleep 15

echo "3. Starting MuleGraph application with Observability enabled..."
export JAVA_HOME=/opt/homebrew/opt/openjdk@21
# Running all profiles including observability (NO evaluator here)
./mvnw spring-boot:run -Dspring-boot.run.profiles=api,normalizer,ledger-projector,stream-worker,alert-projector,graph-projector > app.log 2>&1 &
APP_PID=$!

echo "Waiting for application to become ready..."
until curl -s -f http://localhost:8080/actuator/health/readiness > /dev/null; do
  sleep 2
done
echo "Application is ready!"

echo "3.5. Generating traffic (Phase 5 harness)..."
export JAVA_HOME=/opt/homebrew/opt/openjdk@21
# We run the Evaluation Harness separately on a different port and state dir so it doesn't conflict
./mvnw spring-boot:run -Dspring-boot.run.profiles=evaluator -Dspring-boot.run.jvmArguments="-Dserver.port=8081 -Dspring.kafka.streams.state.dir=/tmp/kafka-streams-evaluator" > evaluator.log 2>&1

echo "4. Verifying Observability Endpoints..."
curl -s http://localhost:8080/actuator/prometheus | grep -E "mulegraph_alerts_created_total|mulegraph_neo4j_projection_latency|http_server_requests" || echo "Metrics missing!"

echo "5. Injecting a Failure (Stopping Neo4j)..."
docker compose stop mulegraph-neo4j
echo "Neo4j stopped. Sending a transaction..."
curl -s -X POST http://localhost:8080/api/v1/transactions \
  -H "X-API-Key: dev-local-api-key" \
  -H "Content-Type: application/json" \
  -d '{
    "transaction_id": "8f310f54-d865-4f4c-83b3-8a3068dae178",
    "source_account_id": "11111111-1111-1111-1111-111111111111",
    "destination_account_id": "22222222-2222-2222-2222-222222222222",
    "amount_minor": 500,
    "currency": "INR",
    "device_id": "d-111",
    "ip_hash": "ip-hash",
    "occurred_at": "2026-07-11T10:20:30Z"
  }'
echo ""
echo "Transaction accepted by API (202). Check logs for Neo4j connection errors."
sleep 5

echo "6. Recovering from Failure (Starting Neo4j)..."
docker compose start mulegraph-neo4j
echo "Neo4j started. Watch logs for GraphProjector catching up."
sleep 10

echo "7. Saving Evidence..."
mkdir -p docs/evidence/phase-6
curl -s http://localhost:8080/actuator/prometheus > docs/evidence/phase-6/prometheus-metrics-snapshot.txt
echo "Phase 6 Showcase Complete!"
echo "Check Grafana at http://localhost:3000 (Anonymous Admin access enabled)."

kill $APP_PID
docker compose down -v
