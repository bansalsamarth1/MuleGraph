# Phase 1D Evidence

## Overview
Restored the Phase 1D validation pipeline to correctly filter invalid events into `transactions.invalid` and route valid transactions to `transactions.validated`.

## Verification Commands
```bash
# Valid and Invalid API requests
./scripts/demo/phase-1d.sh
```

## Results
- Valid transaction routed successfully.
- Schema missing or invalid amounts routed to invalid topic.
- Malformed JSON correctly parsed and pushed to `processing.dead-letter` by custom DeserializationExceptionHandler.

## Known Limitations
- None. Fully recovered.
