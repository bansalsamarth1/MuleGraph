# MuleGraph Project State

## Active phase
Phase 2B — Fan-out rule

## Last verified commit
not committed yet

## Completed acceptance criteria
- `FanOutRuleProperties` configured and added to application context.
- `FanOutState` and `FraudCandidateEvent` created to represent rule states and candidates.
- `FanOutTopology` successfully detects when one source sends to multiple distinct destinations within a fixed time window.
- Duplicate transaction check and distinct-destination aggregation logic correctly prevents emitting duplicate candidates for the same window.
- Topology tests (`TopologyTestDriver`) pass for multiple boundaries (4 destinations, 5 destinations, same destinations).
- The demo script triggers exactly one `FAN_OUT` candidate and logs its payload.

## Failing or blocked criteria
- None.

## Exact verification commands
```bash
./scripts/demo/phase-2b.sh
```

## Known limitations
- Authentication is purely basic (hardcoded dummy key for local testing).
- Real persistence of candidates to Neo4j/Postgres is omitted until future phases.
- Single synthetic currency (INR) used for totals.

## Next allowed task
Execute Phase 2C.
