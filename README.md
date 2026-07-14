# MuleGraph

> **Status:** Functional engineering prototype  
> **Warning:** This is a resume-oriented prototype using synthetic data. It is not production-ready banking software and does not use machine learning.

Event-driven payment fraud prototype using Java, Spring Boot, Kafka Streams, PostgreSQL, and Neo4j to detect mule-account patterns over synthetic transactions.

## Overview

MuleGraph demonstrates an event-driven architecture designed to analyze streams of payment transactions in real time. It uses **Kafka Streams** to evaluate transactions against known fraud typologies (e.g., Fan-out, Fan-in) and projects validated transaction graphs into **Neo4j** for complex pattern traversal like circular flow detection.

## Architecture

![Architecture](docs/images/architecture.png) 
*(Note: See [docs/architecture.md](docs/architecture.md) for deeper design decisions).*

- **Ingestion API**: A Spring Boot application exposing a REST API for synthetic transactions.
- **Ledger Projection**: Subscribes to events to update PostgreSQL.
- **Fraud Evaluation Topologies**: Kafka Streams processors checking for Fan-out, Fan-in, Shared Device, and Shared IP.
- **Graph Projector**: Synchronizes the transaction history into Neo4j.

## Technologies

- **Java 21** / **Spring Boot 3.4.1**
- **Apache Kafka** & **Kafka Streams**
- **PostgreSQL 15**
- **Neo4j 5**
- **OpenTelemetry**, **Prometheus**, & **Grafana**
- **Docker Compose** & **Kubernetes (Helm, kind)**

## Setup & Demo

### Local Docker Environment

Start the infrastructure:
```bash
docker compose up -d
```

Run the application (requires API profile):
```bash
./mvnw spring-boot:run -Dspring-boot.run.profiles=api
```

### API Example

Submit a transaction (Requires API Key from your local `.env` or configuration):

```bash
curl -X POST http://localhost:8080/api/v1/transactions \
  -H "Content-Type: application/json" \
  -H "X-API-Key: dev-local-api-key" \
  -d '{
    "transaction_id": "97e682fb-a798-444a-a035-7c093a2eb67e",
    "source_account_id": "31b0fc2f-cda2-4ef8-a53f-e18e0018f6d6",
    "destination_account_id": "2bc0f711-2b02-4bb3-a309-8d1844101e40",
    "amount_minor": 100000,
    "currency": "USD",
    "device_id": "device-1",
    "ip_hash": "testhash",
    "occurred_at": "2026-07-13T10:00:00Z"
  }'
```

Expected Response:
```json
{
  "status": "ACCEPTED",
  "message": "Transaction submitted for processing"
}
```

## Testing

MuleGraph uses `Testcontainers` for true integration testing against Kafka, PostgreSQL, and Neo4j.

```bash
./mvnw clean verify
```

## Limitations

- Built entirely around synthetic payloads; no real financial data.
- Fraud rules are static heuristics, not trained ML models.
- Authentication is limited to basic API keys for demonstration.

See [PROJECT_STATUS.md](PROJECT_STATUS.md) for more details.
