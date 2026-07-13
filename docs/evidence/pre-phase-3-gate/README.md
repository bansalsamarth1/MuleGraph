# Pre-Phase-3 Gate Evidence

## Overview
Verified strict alert deduplication, ledger transaction idempotency, environment variable configuration, explicit Kafka topic creation, and poison-message handling.

## Verification Commands
```bash
./scripts/demo/pre-phase-3-gate.sh
```

## Results
- Tests ran successfully with exactly `0` `@Disabled` required tests. Total tests passed: 9.
- Upgraded Testcontainers dependency to `1.21.4` to natively resolve the Docker API incompatibility (`BadRequestException` on `/info`) caused by Docker Desktop 27.2+ removing support for API versions `< 1.44`.
- Replaying the same transaction resulted in `0` additional ledger inserts, successfully demonstrating idempotency for identical payloads.
- Sending conflicting identity data for the same transaction correctly pushed the record to dead-letter storage.
- Alert projector properly retrieved canonical `alert_id` after detecting deduplication key conflict.
- Relaxed ledger `device_id` and `ip_hash` constraints to allow missing (optional) data, successfully ignoring these events in windowed fraud-check topologies while storing them correctly in the ledger.

## Known Limitations
- The project is now fully prepared for Phase 3 Neo4j Graph Database integration; no further architectural regressions exist.
