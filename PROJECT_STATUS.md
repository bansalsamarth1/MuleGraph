# MuleGraph Project Status

MuleGraph is an event-driven payment fraud prototype demonstrating a modular monolith architecture using Java, Spring Boot, Kafka Streams, PostgreSQL, and Neo4j. It was developed in phases and successfully deployed to a local Kubernetes environment.

## Current Status: Completed

The functional engineering prototype is complete. All major planned phases have been successfully implemented, verified, and demonstrated.

### Implemented Capabilities

1. **Domain & Ledger (Phase 1)**
   - Core domain model for Accounts, Transactions, Alerts.
   - REST API for ingestion with synthetic payload validation.
   - Idempotent PostgreSQL ledger projections.
2. **Fraud Detection (Phase 2)**
   - Stateless and stateful Kafka Streams topologies (Fan-out, Fan-in, Shared Device, Shared IP).
   - Real-time generation of Alert events.
3. **Graph Projection (Phase 3)**
   - Event-driven population of a Neo4j graph using the `graph-projector` profile.
4. **Metrics & Observability (Phases 5 & 6)**
   - Precision tracking and baseline metric evaluation.
   - Full OpenTelemetry and Prometheus/Grafana stack provisioning.
5. **Kubernetes Orchestration (Phase 7)**
   - Helm-based deployment to a local `kind` cluster.
   - Strimzi Kafka, Neo4j, and Bitnami PostgreSQL StatefulSets.
   - Horizontal Pod Autoscaler and ResourceQuotas.

## Verified Claims

- End-to-end event flow (API -> Kafka -> Postgres/Neo4j) is fully functional using Testcontainers integration tests.
- Infrastructure is deployable via both `docker-compose.yml` and Helm/Kubernetes manifests.
- Resiliency against stateful backend outages has been tested locally via pod deletion and scale-to-zero recovery tests.

## Limitations

- **Synthetic Data**: The project is designed to run with generated test payloads. It does not contain models trained on real banking transactions.
- **Machine Learning**: Not implemented. Fraud rules are purely heuristic/topological.
- **Production Scale**: While HPA is configured, the system has not been load-tested or benchmarked for high-throughput production workloads. The current environment is strictly local/development.
- **Authentication**: Simple API Key authentication is present, but it lacks OAuth2 or fine-grained RBAC.
