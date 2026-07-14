# Runbook: Neo4j Outage

## Symptoms
- Consumer lag for `mulegraph-graph-projector` increases.
- API continues to return 202 successfully.
- PostgreSQL ledger updates successfully.
- Logs show Neo4j `ServiceUnavailableException`.

## Impact
- **Ingestion continues normally**.
- **Audit ledger continues normally**.
- **Graph updates are delayed**.
- **Circular Flow Detection may miss alerts** because the `CircularFlowConfirmationService` queries Neo4j instantly upon receiving an event, and if Neo4j is down or lagging, the required graph paths will not be found in time.

## Resolution
1. Verify Neo4j container: `docker ps | grep neo4j`.
2. Check memory limits or page cache constraints in Neo4j logs.
3. Restart Neo4j: `docker compose restart mulegraph-neo4j`.

## Recovery Behaviour
- `GraphProjector` reconnects and catches up on the `graph.updates` topic.
- Cypher `MERGE` statements ensure idempotent state restoration.
- Note: Circular flow events evaluated *during* the outage will have failed path confirmation and will not automatically re-evaluate unless a manual replay is triggered.
