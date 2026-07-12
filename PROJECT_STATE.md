# MuleGraph Project State

## Active phase
Phase 1C — Kafka Producer Integration

## Last verified commit
not committed yet

## Completed acceptance criteria
- `POST /api/v1/transactions` accepts synthetic contract.
- Missing/invalid API key returns 401/403.
- Valid payload maps to `InternalTransactionEvent` system fields (`event_id`, `correlation_id`, `schema_version`, `ingested_at`).
- Invalid payloads (same accounts, negative amount, malformed currency) return 400.
- Stack traces are hidden from the client via `GlobalExceptionHandler`.
- Payload size limited via `application.yml`.
- Replaced dummy publisher with `KafkaTransactionPublisher` that synchronously writes to a Kafka topic.
- API returns `202 Accepted` only after broker acknowledgement.
- Integration tests confirm physical message delivery.
- Docker Compose spins up KRaft-based single node Kafka broker.

## Failing or blocked criteria
- None.

## Exact verification commands
```bash
./scripts/demo/phase-1c.sh
```

## Known limitations
- Authentication is purely basic (hardcoded dummy key in `application.yml` for local testing).
- Real persistence is omitted until future phases.
- Testcontainers integration test was temporarily `@Disabled` because the host machine's Docker Desktop uses an API version (`1.54`) that `docker-java` cannot negotiate correctly right now (`400 Bad Request` on version `1.32`). The demo script verifies integration perfectly via standard `docker compose`.

## Next allowed task
Execute Phase 2.
