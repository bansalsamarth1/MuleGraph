# MuleGraph Project State

## Active phase
Phase 5

## Last verified commit
not committed yet

## Current Phase
Phase 5 (Metric Evaluation) is completed. A synthetic evaluation harness correctly calculates True Positives, False Positives, False Negatives, Precision, and Recall. Fixes were applied to stream-time mechanisms and EOS commit intervals to achieve 1.000 Precision and 1.000 Recall on all scenarios.

## Progress
- `[x]` Phase 1A: Domain, API, and Basic Ingestion
- `[x]` Phase 1B: PostgreSQL Ledger setup
- `[x]` Phase 1C: Kafka Streams basic setup
- `[x]` Phase 1D: Validation pipeline (TransactionNormalizationTopology)
- `[x]` Phase 2: Fraud Detection Topologies (FanOut, FanIn, SharedDevice, SharedIp)
- `[x]` Phase 3: Neo4j Graph Projection and Robust Error Handling
- `[x]` Phase 4: Circular Flow Detection
- `[x]` Phase 5: Metric Evaluation
- `[ ]` Phase 6: Observability

## Completed acceptance criteria
- Implemented `EvaluationHarness` mapping specific topologies (Fan-Out, Fan-In, Shared-Device, Shared-IP, Circular-Flow) to expected Alert models.
- Tuned Kafka Streams time alignment logic to avoid tumbling window fragmentation.
- Tuned evaluation pacing to absorb `EXACTLY_ONCE_V2` transaction latency into graph projection.
- Achieved perfect evaluation metrics: 1.000 Precision, 1.000 Recall, 0 FP, 0 FN.

## Failing or blocked criteria
None.

## Exact verification commands
```bash
export JAVA_HOME=/opt/homebrew/opt/openjdk@21
./scripts/demo/phase-5.sh
```

## Known limitations
- The synthetic harness runs entirely in a single JVM via Spring Boot, which is simpler but not fully representative of distributed cluster environments.
- Stream time strictly requires synthetic background transactions to push event time accurately.

## Next allowed task
Start Phase 6: Observability.
