# MuleGraph Project State

## Active phase
Completed (Phase 7 done)

## Last verified commit
not committed yet

## Current Phase
Phase 7 (Kubernetes Deployment) is completed. MuleGraph stateless profiles are containerized and deployed via Helm charts. Stateful services (PostgreSQL, Neo4j, Strimzi Kafka) are provisioned. HPA and resource limits are tested on a local `kind` cluster. End-to-end processing and stateful resiliency verified.

## Progress
- `[x]` Phase 1A: Domain, API, and Basic Ingestion
- `[x]` Phase 1B: PostgreSQL Ledger setup
- `[x]` Phase 1C: Kafka Streams basic setup
- `[x]` Phase 1D: Validation pipeline (TransactionNormalizationTopology)
- `[x]` Phase 2: Fraud Detection Topologies (FanOut, FanIn, SharedDevice, SharedIp)
- `[x]` Phase 3: Neo4j Graph Projection and Robust Error Handling
- `[x]` Phase 4: Circular Flow Detection
- `[x]` Phase 5: Metric Evaluation
- `[x]` Phase 6: Observability
- `[x]` Phase 7: Kubernetes

## Completed acceptance criteria
- Instrumented `AlertProjector` and `GraphProjector` with Micrometer.
- Exposed `/actuator/prometheus` endpoint.
- Provisioned Prometheus, Grafana, and OpenTelemetry via Docker Compose.
- Verified observability of business flows by running the `EvaluationHarness` JVM in a separate process.
- Implemented script to demonstrate a simulated failure (restarting Neo4j) and verified telemetry capture.
- Containerized all components and validated via Helm on Minikube.

## Failing or blocked criteria
None.

## Exact verification commands
```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@21
./scripts/demo/phase-6.sh
```

## Known limitations
- Grafana dashboards are currently blank unless manually imported; future enhancements can provision default dashboards as config files.
- The `EvaluationHarness` JVM and Main JVM require strict port and state directory segregation during concurrent execution.

## Next allowed task
Start Phase 7: Kubernetes.
