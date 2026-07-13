# Phase 3 Evidence

## Goal
Implement Phase 3: Neo4j Graph Projection with runtime isolation, robust retries, and exactly-once semantics using `ProcessedGraphEvent` markers.

## Executed Commands

```bash
docker compose config
export JAVA_HOME=/opt/homebrew/opt/openjdk@21
./mvnw clean verify
```

## Implementation Details
1. **Neo4j Constraints**: Implemented via `GraphConstraintInitializer` ensuring unique `account_id`, `device_id`, `ip_hash`, and `event_id` in `ProcessedGraphEvent`.
2. **Graph Topology**: Added `TransactionToGraphTopology` mapping `transactions.validated` to `graph.updates`.
3. **Graph Projector**: Implemented `GraphProjector` with an atomic Cypher query to assert idempotency and correctly aggregate `transaction_count` and `total_amount_minor` on `TRANSFERRED_TO` relationships.
4. **Retry Mechanism**: Developed a dedicated `graphProjectorKafkaListenerContainerFactory` with `ExponentialBackOff` up to 60s to endure temporary Neo4j absence, pushing non-recoverable records to `graph.updates.DLT`.

## Verification Results

### Tests Passing
- `GraphProjectorTest`: Integration tests isolating the Neo4j aggregation logic logic using Neo4j Testcontainers. Validated atomic aggregations, duplicates ignored, optional fields processing, and correct updates.
- `GraphProjectorKafkaIntegrationTest`: Integration tests isolating the Spring Kafka dead letter logic using Kafka Testcontainers. Proved transient/fatal exception routing.
- All pre-existing transaction normalization and ingestion tests continue to pass.

### Runtime Proof
Demonstrated in `scripts/demo/phase-3.sh`.
1. The Postgres ledger projector and REST API start flawlessly with or without Neo4j running.
2. The `graph-projector` handles Neo4j shutdown seamlessly via retries.
3. Catch-up processing is verified successfully when Neo4j container restarts.

## Known Limitations
- Idempotency is enforced within the boundaries of the `ProcessedGraphEvent`. High-concurrency duplicate deliveries within the same millisecond could result in transient deadlocks, which the exponential backoff handles correctly.
- Global transaction ordering is not guaranteed across partitions; ordering is maintained only per outgoing source account (`source_account_id` routing key).
