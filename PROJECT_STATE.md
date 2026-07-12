# MuleGraph Project State

## Active phase
Phase 2A — Kafka Streams Foundation

## Last verified commit
not committed yet

## Completed acceptance criteria
- `spring-kafka` and `kafka-streams` are integrated and configured with `exactly_once_v2`.
- Custom `TimestampExtractor` implemented and configured to use the client-provided `occurred_at` timestamp.
- Topology correctly reads from `transactions.raw` and re-keys into four separate topics: `transactions.by-source`, `transactions.by-destination`, `transactions.by-device`, `transactions.by-ip`.
- Auto-creation of topics on startup using Spring Kafka `NewTopic` beans.
- Unit tests (`TopologyTestDriver`) confirm out-of-order timestamp extraction and correct partitioning key.
- Demo script confirms actual `CreateTime` matches `occurred_at` directly from the Kafka topic using console consumer.

## Failing or blocked criteria
- None.

## Exact verification commands
```bash
./scripts/demo/phase-2a.sh
```

## Known limitations
- Authentication is purely basic (hardcoded dummy key in `application.yml` for local testing).
- Real persistence is omitted until future phases.
- Testcontainers integration test was temporarily `@Disabled` due to host docker version. The demo script confirms runtime functionality via standard `docker compose`.

## Next allowed task
Execute Phase 2B.
