# MuleGraph Project State

## Active phase
Phase 2C — Fan-in rule

## Last verified commit
not committed yet

## Completed acceptance criteria
- `FanInRuleProperties` configured and added to application context.
- `FanInState` created to represent rule states and candidate status.
- `FanInTopology` successfully detects when one destination receives from multiple distinct sources within a fixed time window.
- Duplicate transaction check and distinct-source aggregation logic correctly prevents emitting duplicate candidates for the same window.
- Topology tests (`TopologyTestDriver`) pass for multiple boundaries (4 sources, 5 sources, same sources).
- The demo script triggers exactly one `FAN_IN` candidate and logs its payload.

## Failing or blocked criteria
- None.

## Exact verification commands
```bash
./scripts/demo/phase-2c.sh
```

## Known limitations
- Authentication is purely basic (hardcoded dummy key for local testing).
- Real persistence of candidates to Neo4j/Postgres is omitted until future phases.
- Single synthetic currency (INR) used for totals.

## Next allowed task
Execute Phase 2D.
