# Phase 7 Evidence: Kubernetes Deployment

## What Changed
- Migrated local orchestration from Docker Compose to a fully functional Kubernetes cluster using `kind`.
- Installed Helm-managed stateful dependencies (Bitnami PostgreSQL, Strimzi Kafka Operator, Neo4j Helm chart).
- Packaged the MuleGraph application as a Helm chart (`k8s/charts/mulegraph`) with distinct profiles (API, streams, graph, ledger, alerts) deployed as separate stateless components.
- Configured dynamic resource management and scaling with Horizontal Pod Autoscaler (HPA) and Metrics Server.

## Files Changed
- `k8s/kind-config.yaml`
- `k8s/charts/mulegraph/*` (Helm chart scaffolding)
- `scripts/demo/phase-7.sh` (End-to-End Orchestration)

## Commands Executed
```bash
./scripts/demo/phase-7.sh
```

## Runtime Proof
- A fresh `kind` cluster was spun up and all foundational CRDs established.
- Strimzi deployed a KRaft Kafka Node Pool and Broker.
- Bitnami Postgres and Neo4j charts spun up StatefulSets with PersistentVolumeClaims.
- The `mulegraph:v1` image was compiled (skipping tests) and loaded into the node.
- Five distinct Helm releases were deployed corresponding to `api`, `ledger-projector`, `stream-worker`, `alert-projector`, and `graph-projector` profiles.
- Validated via automated `curl` request for transaction ingestion against the NodePort (30080).
- Automatic pod self-healing confirmed by killing the `mulegraph-api` pod.
- Resiliency verified by scaling the `neo4j` StatefulSet to 0 and back to 1.
- Final infrastructure state output to `docs/evidence/phase-7/k8s-state.txt`.

## Known Limitations
- Resource constraints on local desktop development environments require drastically restricted CPU/memory requests (`32Mi` memory requests) to avoid `Pending` scheduling states. Spring Boot takes up to 60s to boot under heavy CPU constraints.

## Proposed Commit Message
```text
feat: implement Phase 7 kubernetes orchestration

- Add kind configuration and local dev deployment scripts
- Scaffold MuleGraph Helm chart for stateless profile rollouts
- Provision Strimzi Kafka, Neo4j, and PostgreSQL via Helm
- Configure HPA, ResourceQuotas, and Probes
```
