# MuleGraph Antigravity Workspace Rules

Read `MuleGraph_CONTEXT.md` and `PROJECT_STATE.md` before every task. They are authoritative. Work on one active phase only.

## Mandatory workflow

1. Inspect the repository; never assume files or behaviour.
2. Run baseline tests before editing.
3. Produce an implementation plan before code, including files, tests, verification commands, risks, and out-of-scope items.
4. Make the smallest coherent change for the active phase.
5. Execute tests and runtime verification.
6. Produce a walkthrough with exact commands and actual results.
7. Update `PROJECT_STATE.md`.
8. Stop. Never start the next phase automatically.

## No hallucination rules

- Never claim a command, test, endpoint, topic, database write, deployment, metric, or recovery worked unless it was actually executed and observed.
- Never invent command output, test counts, benchmark results, precision, recall, latency, throughput, versions, image tags, or Kubernetes status.
- Verify dependency versions and container tags from official sources; use stable GA versions only. Record exact versions in `docs/version-matrix.md`.
- If a material architecture decision is not specified, propose an ADR and stop rather than silently deciding.
- Do not hide failures, skip tests, weaken assertions, or replace required integration tests with mocks.
- Report blockers clearly and leave the phase incomplete.

## Fixed architecture

- Java 21, Spring Boot, Maven Wrapper.
- One repository and one package-by-feature modular-monolith codebase.
- One deployable image initially; runtime responsibilities use Spring profiles.
- Docker Compose for Phases 1–6; kind and Helm in Phase 7.
- Kafka is the durable event backbone.
- PostgreSQL is the auditable ledger/alert projection.
- Kafka Streams owns core real-time window state.
- Neo4j is a bounded relationship projection and circular-flow confirmation store.
- Redis is excluded unless an accepted ADR proves a supporting need.
- Synthetic data only.
- No machine learning before deterministic rules and measured evaluation.
- No microservice or multi-repository split without an accepted ADR.

## Correctness rules

- API returns 202 only after Kafka broker acknowledgement.
- 202 means accepted for asynchronous analysis, not payment completion.
- Use `amount_minor` integer plus three-letter currency; never float/double for money.
- Use UTC timestamps. Fraud windows use `occurred_at`, not processing time.
- Honest guarantee: at-least-once delivery with idempotent projections.
- Do not claim end-to-end exactly once across Kafka, PostgreSQL, and Neo4j.
- Use unique `event_id`, `transaction_id`, database constraints, upserts, retry-safe consumers, and alert deduplication keys.
- Do not commit offsets as successful before required external writes succeed.
- Do not claim global Kafka ordering; ordering is per partition.
- Fan-out groups by source and counts distinct destinations.
- Fan-in groups by destination and counts distinct sources.
- Shared-device/IP rules count distinct accounts.
- Circular searches must be candidate-first and bounded by depth/time/order/amount rules.
- Alerts are investigation records, not proof of fraud.

## Security and data

- No real banking/customer data.
- No credentials or plaintext Kubernetes secrets in Git.
- Configuration comes from environment variables, ConfigMaps, and secret references.
- Do not expose stack traces or internal exception messages to clients.
- Use synthetic `ip_hash`, not raw IP data.

## Testing requirements

- Unit tests for validation and pure rule logic.
- Testcontainers for real Kafka, PostgreSQL, and Neo4j integration tests when their phases require them.
- Contract tests for API and events.
- Failure tests for duplicates, restarts, late events, poison messages, and downstream outages.
- Do not mark a phase complete until its acceptance criteria and showcase pass.

## Evidence requirement

Every phase creates or updates:

- `docs/evidence/phase-N/README.md`
- `scripts/demo/phase-N.ps1`
- optional `scripts/demo/phase-N.sh`

Evidence must contain exact commands, actual outputs, expected versus actual results, test counts, data queries, known limitations, and cleanup steps.

## Scope restrictions

Do not add future-phase technology early. In particular:

- No Kafka/PostgreSQL in Phase 1A.
- No Neo4j before Phase 3.
- No circular-flow implementation before Phase 4.
- No invented evaluation metrics before Phase 5.
- No observability stack before Phase 6 except basic Actuator/logging needed earlier.
- No Kubernetes manifests before Phase 7.
- No full repository generation in one task.

## Completion response

At task completion, report:

1. What changed.
2. Files changed.
3. Commands executed.
4. Tests and actual results.
5. Runtime proof.
6. Known limitations.
7. Concepts to learn.
8. Three interview questions.
9. Proposed commit message.
10. The single next allowed task.
