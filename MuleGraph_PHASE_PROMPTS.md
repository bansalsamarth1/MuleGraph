# MuleGraph — Copy/Paste Prompts for Antigravity

Use these prompts sequentially. Put `MuleGraph_CONTEXT.md` at the repository root and add the contents of `MuleGraph_ANTIGRAVITY_WORKSPACE_RULES.md` as a workspace rule first.

## Initial project prompt

```text
Read @MuleGraph_CONTEXT.md completely and treat it as the primary source of truth. Also read the MuleGraph workspace rules. Do not write production code yet.

Execute Phase 0 only. First inspect this workspace and produce an Implementation Plan artifact. Verify the local toolchain, create only the Phase 0 documentation/state files, and create executable Phase 0 verification evidence. Run every verification command you claim to run. Update PROJECT_STATE.md and stop after the Phase 0 walkthrough. Do not scaffold Spring Boot or begin Phase 1.
```

## Phase 1A prompt

```text
Read @MuleGraph_CONTEXT.md and @PROJECT_STATE.md. Confirm that Phase 0 acceptance criteria are complete from evidence; if not, stop and report the exact missing criteria.

Execute Phase 1A only: create the smallest Java 21 Maven Spring Boot foundation with Maven Wrapper, Web, Validation, Actuator, and tests. Verify stable dependency versions and update docs/version-matrix.md. Do not add Kafka, PostgreSQL, Neo4j, Redis, Kubernetes, or future-phase packages.

Before editing, produce an implementation plan. After implementation, run tests, start the application, verify health/liveness/readiness endpoints, create Phase 1A evidence and demo script, update PROJECT_STATE.md, produce the walkthrough, and stop.
```

## Phase 1B prompt

```text
Read the context, project state, ADRs, and existing tests. Execute Phase 1B only.

Implement the synthetic transaction API contract, API-key authentication from environment configuration, validation, safe exception handling, request-size configuration, and mapping to an internal event. Kafka does not exist yet, so do not falsely implement durable 202 semantics. Use a publishing port/test double only as allowed by the context and clearly document the temporary behaviour.

Add exhaustive API tests listed in the Phase 1B acceptance criteria. Run them, create evidence and the request matrix, update PROJECT_STATE.md, and stop. Do not add Kafka or PostgreSQL.
```

## Phase 1C prompt

```text
Read the context and verify Phase 1B evidence. Execute Phase 1C only.

Add Kafka in Docker Compose using verified pinned versions and KRaft mode. Implement durable publication to transactions.raw keyed by transaction_id. The API may return 202 only after broker acknowledgement. Configure safe failure behaviour when Kafka is unavailable. Add Testcontainers Kafka integration tests that prove key, payload, acknowledgement, and failure semantics.

Run the positive and Kafka-down showcases, preserve actual outputs, update documentation and PROJECT_STATE.md, and stop. Do not add PostgreSQL or fraud rules.
```

## Phase 1D prompt

```text
Read the context and verify Phase 1C evidence. Execute Phase 1D only.

Add PostgreSQL, Flyway, the raw-event normalizer, transactions.validated/transactions.invalid routing, and the idempotent ledger projector. Implement constraints, indexes, explicit conflict handling, retries, and DLQ behaviour exactly as specified. Add Kafka+PostgreSQL Testcontainers integration tests.

Demonstrate: one accepted request becomes one ledger row; replay remains one row; invalid internal event is not stored; PostgreSQL outage creates backlog/failure and recovery catches up. Save real evidence, update PROJECT_STATE.md, and stop. Do not add Kafka Streams rules.
```

## Phase 2A prompt

```text
Execute Phase 2A only after verifying the complete Phase 1 gate.

Add the Kafka Streams foundation, occurred_at timestamp extraction, state/application configuration, tested re-keying for source/destination/device/IP, and explicit late-event/grace behaviour. Configure exactly-once-v2 only if verified compatible, and document that it covers Kafka-to-Kafka processing only.

Use topology tests and a runtime out-of-order event showcase. Save evidence, update PROJECT_STATE.md, and stop before implementing a fraud rule.
```

## Phase 2B prompt

```text
Execute Phase 2B only: implement fan-out detection according to the context. Count distinct destinations, transaction count, and total amount separately in a 60-second event-time window with 15-second grace using configurable values. Emit deterministic FAN_OUT candidates with deduplication.

Prove every Phase 2B boundary case with topology/integration tests and run the deterministic fan-out demo. Save candidate output and actual results, update PROJECT_STATE.md, and stop.
```

## Phase 2C prompt

```text
Execute Phase 2C only: implement fan-in detection keyed by destination and counting distinct source accounts. Reuse shared abstractions only where they make the code clearer; do not create premature frameworks.

Prove that five repeats from A to X count as one distinct source, while A/B/C/D/E to X creates one candidate. Run tests and demo, save evidence, update PROJECT_STATE.md, and stop.
```

## Phase 2D prompt

```text
Execute Phase 2D only: implement separate shared-device and shared-IP rules using distinct account counts and independent configuration. Use synthetic ip_hash only.

Run normal and suspicious scenarios for each rule, save actual candidate output, complete tests/evidence, update PROJECT_STATE.md, and stop.
```

## Phase 2E prompt

```text
Execute Phase 2E only: implement candidate-to-alert policies, fraud.alerts publication, Flyway alert tables, evidence links, unique deterministic alert deduplication, and idempotent alert projection. Alerts must be OPEN investigation records, never statements of proven fraud.

Demonstrate a complete fan-out flow from HTTP request to stored alert and prove candidate replay does not duplicate the alert. Save evidence, update PROJECT_STATE.md, and stop.
```

## Phase 3 prompt

```text
Execute Phase 3 only. Add Neo4j with pinned verified image, graph constraints, graph.updates publication, and a replay-safe graph projector. Use aggregated TRANSFERRED_TO relationships and Account-Device/IP relationships. Do not store every transaction as its own permanent graph edge.

Use Testcontainers Neo4j tests. Demonstrate normal graph projection, replay idempotency, Neo4j outage while ingestion continues, backlog/lag, and catch-up after recovery. Save actual Cypher results and evidence, update PROJECT_STATE.md, and stop. Do not implement circular detection.
```

## Phase 4 prompt

```text
Execute Phase 4 only. Implement candidate-first bounded circular-flow confirmation in Neo4j with configured maximum depth, time range, chronological ordering, amount tolerance, and deterministic cycle deduplication. Never run an unbounded full-graph cycle scan.

Run deterministic positive and negative cycle scenarios, capture the bounded query, query plan, measured latency, resulting alert, and all tests. Update evidence and PROJECT_STATE.md, then stop.
```

## Phase 5 prompt

```text
Execute Phase 5 only. Build a seeded, labelled synthetic evaluation harness for normal, fan-in, fan-out, shared-device, shared-IP, and circular scenarios. Match alerts to ground truth and calculate precision, recall, F1, false-positive rate, false-negative rate, and detection latency from actual runs.

Create a single reproducible evaluation command and reports containing seed, dataset size, environment, rule configuration, unmatched alerts, and missed scenarios. Never invent metrics. Save evidence, update PROJECT_STATE.md, and stop.
```

## Phase 6 prompt

```text
Execute Phase 6 only. Add structured logs, OpenTelemetry propagation through Kafka headers, Prometheus metrics, Grafana dashboards, runbooks, failure injection, and replay tests according to the context.

Generate traffic and prove the required metrics, one trace or correlation chain, one DLQ event, and one downstream outage recovery. Document trace limitations honestly. Save dashboards/evidence, update PROJECT_STATE.md, and stop.
```

## Phase 7 prompt

```text
Execute Phase 7 only after local Phases 1-6 are verified. Create a kind-based Kubernetes deployment and a Helm chart for the MuleGraph application profiles. Use immutable image tags, ConfigMaps, secret references, probes, resources, rolling updates, and an API HPA only when metrics are available. Use a verified Kubernetes-native approach for Kafka; do not create a stateless fake Kafka Deployment. Use persistent storage for local stateful dependencies and do not claim HA unless tested.

Build the complete scripted showcase: cluster creation, dependency installation, image build/load, Helm install, readiness, end-to-end event verification, API pod deletion/self-healing, rolling upgrade, consumer scaling explanation against partition count, Neo4j outage/backlog/recovery, and cleanup. Save actual kubectl/Helm outputs, update PROJECT_STATE.md, and stop.
```

## Phase 8 prompt

```text
Execute Phase 8 only. Add CI/CD, tests, container build, vulnerability scanning, Helm validation, optional kind smoke test, secret/dependency scanning, final documentation, evidence-backed metrics, and honest resume bullets.

Run the pipeline from a clean checkout where possible. Do not place any number in README or resume bullets unless linked to actual evidence. Update PROJECT_STATE.md and produce the final architecture and demo walkthrough.
```

## Debugging prompt template

```text
Read @MuleGraph_CONTEXT.md, @PROJECT_STATE.md, and the current phase evidence. Do not redesign the architecture.

Reproduce this problem before changing code:
<PASTE ERROR OR OBSERVED BEHAVIOUR>

First report the reproduction command and likely root cause. Then make the smallest fix, add a regression test, rerun the relevant full test set, update evidence and PROJECT_STATE.md, and stop. Do not proceed to another phase.
```

## Code-explanation prompt template

```text
Do not change code. Read these files:
<LIST FILES>

Teach me the flow in simple language first, then explain each class and important method, then trace one transaction through the code. Identify where idempotency, failure handling, and tests are implemented. Finish with five interview questions. Do not edit anything.
```
