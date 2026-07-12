# MuleGraph Project State

## Active phase
Phase 1B — Transaction API contract and security

## Last verified commit
not committed yet

## Completed acceptance criteria
- `POST /api/v1/transactions` accepts synthetic contract.
- Missing/invalid API key returns 401/403.
- Valid payload maps to `InternalTransactionEvent` system fields (`event_id`, `correlation_id`, `schema_version`, `ingested_at`).
- Invalid payloads (same accounts, negative amount, malformed currency) return 400.
- Stack traces are hidden from the client via `GlobalExceptionHandler`.
- Payload size limited via `application.yml`.
- Kafka is NOT used (Publisher interface uses a Dummy implementation, returning a temporary `200 OK ACCEPTED_TEST_MODE`).
- exhaustive unit tests run and pass.

## Failing or blocked criteria
- None.

## Exact verification commands
```bash
./scripts/demo/phase-1b.sh
```

## Known limitations
- Authentication is purely basic (hardcoded dummy key in `application.yml` for local testing).
- Real persistence is omitted until future phases.

## Next allowed task
Execute Phase 1C.
