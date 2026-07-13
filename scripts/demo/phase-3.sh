#!/bin/bash
set -e

echo "================================================="
echo " MuleGraph Phase 3 Demo: Neo4j Graph Projection"
echo "================================================="

echo "1. Building the project..."
export JAVA_HOME=/opt/homebrew/opt/openjdk@21
./mvnw clean package -DskipTests

echo "2. Starting Kafka, PostgreSQL, and Neo4j..."
docker compose up -d

echo "Waiting for services to be healthy..."
sleep 15

echo "3. Starting MuleGraph application (API + Graph Projector)..."
java -jar target/mulegraph-0.0.1-SNAPSHOT.jar \
  --spring.profiles.active=graph-projector \
  > application.log 2>&1 &
APP_PID=$!
echo "Application started with PID: $APP_PID"

echo "Waiting for application to initialize..."
sleep 15

echo "4. Sending sample valid transactions to the API..."
curl -s -X POST http://localhost:8080/api/v1/transactions \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-local-api-key" \
  -d '{
    "transaction_id": "11111111-1111-1111-1111-111111111111",
    "source_account_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "destination_account_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
    "amount_minor": 5000,
    "currency": "USD",
    "device_id": "device-1",
    "ip_address": "192.168.1.1",
    "occurred_at": "2026-07-13T10:00:00Z"
  }'
echo ""

curl -s -X POST http://localhost:8080/api/v1/transactions \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-local-api-key" \
  -d '{
    "transaction_id": "22222222-2222-2222-2222-222222222222",
    "source_account_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "destination_account_id": "bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb",
    "amount_minor": 15000,
    "currency": "USD",
    "device_id": "device-2",
    "ip_address": "192.168.1.2",
    "occurred_at": "2026-07-13T10:05:00Z"
  }'
echo ""

echo "5. Wait for Kafka Streams and Graph Projector to process..."
sleep 5

echo "6. Checking Neo4j Graph..."
docker exec mulegraph-neo4j cypher-shell -u neo4j -p mulepass "MATCH (s:Account)-[r:TRANSFERRED_TO]->(d:Account) RETURN s.account_id AS source, d.account_id AS destination, r.transaction_count AS count, r.total_amount_minor AS totalAmount;"

echo "7. Stopping Neo4j dynamically to test runtime isolation..."
docker compose stop mulegraph-neo4j

echo "8. Sending another transaction while Neo4j is down (API should still return 202 ACCEPTED)..."
curl -s -X POST http://localhost:8080/api/v1/transactions \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-local-api-key" \
  -d '{
    "transaction_id": "33333333-3333-3333-3333-333333333333",
    "source_account_id": "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa",
    "destination_account_id": "cccccccc-cccc-cccc-cccc-cccccccccccc",
    "amount_minor": 2000,
    "currency": "USD",
    "occurred_at": "2026-07-13T10:10:00Z"
  }'
echo ""
echo "Notice that the API accepted the transaction smoothly."

echo "Waiting a few seconds..."
sleep 5

echo "9. Restarting Neo4j (Graph Projector should recover with exponential backoff and catch up)..."
docker compose start mulegraph-neo4j
sleep 15

echo "10. Checking Neo4j Graph again for catch-up processing..."
docker exec mulegraph-neo4j cypher-shell -u neo4j -p mulepass "MATCH (s:Account)-[r:TRANSFERRED_TO]->(d:Account) RETURN s.account_id AS source, d.account_id AS destination, r.transaction_count AS count, r.total_amount_minor AS totalAmount;"

echo "11. Shutting down application..."
kill $APP_PID

echo "================================================="
echo " Demo completed successfully."
echo "================================================="
