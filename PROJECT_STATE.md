# MuleGraph Project State

## Active phase
Phase 2D — Shared Device and Shared IP rules

## Last verified commit
not committed yet

## Completed acceptance criteria
- Created `SharedDeviceRuleProperties` and `SharedIpRuleProperties` in application context.
- Configured minimum distinct sources to 3 for both properties, keeping window to 60s.
- Created `SharedDeviceState` and `SharedIpState` tracking distinct `source_account_id`s.
- Created `SharedDeviceTopology` consuming from `transactions.by-device`.
- Created `SharedIpTopology` consuming from `transactions.by-ip`.
- Emitted deterministic `FraudCandidateEvent` objects matching rule rules `SHARED_DEVICE` and `SHARED_IP` respectively, deduplicated within their tumbling windows.
- Verified bounding limits and deduplication behaviour for both rules via `TopologyTestDriver`.
- Verified exactly 2 independent candidates emitted in local cluster integration run.

## Failing or blocked criteria
- None.

## Exact verification commands
```bash
./scripts/demo/phase-2d.sh
```

## Known limitations
- Still using synthetic currency (INR) and dummy API authentication.
- Real persistence (Phase 2E) is not yet implemented.

## Next allowed task
Execute Phase 2E.
