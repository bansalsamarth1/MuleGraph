#!/bin/bash
set -e

echo "MuleGraph Phase 5 - Metric Evaluation Demo"
echo "=========================================="

echo "1. Starting infrastructure (Kafka, PostgreSQL, Neo4j)..."
docker compose up -d

echo "2. Waiting for services to be ready..."
sleep 15

echo "Cleaning local Kafka Streams state..."
rm -rf /tmp/kafka-streams

echo "3. Starting MuleGraph Evaluation Harness (all profiles + evaluator)..."
export SPRING_PROFILES_ACTIVE=api,normalizer,ledger-projector,stream-worker,alert-projector,graph-projector,evaluator
export JAVA_HOME=/opt/homebrew/opt/openjdk@21
./mvnw spring-boot:run

echo "Evaluation Harness finished. Check docs/evidence/phase-5/evaluation_report.md for results."

echo "Demo complete! Shutting down infrastructure."
echo "Demo complete! Shutting down infrastructure."
docker compose down -v
