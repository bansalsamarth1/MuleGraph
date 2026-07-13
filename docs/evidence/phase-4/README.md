# MuleGraph Phase 4 - Bounded Circular-Flow Confirmation

## Goal
Implement a candidate-first bounded circular-flow confirmation in Neo4j that avoids unbounded global scans.

## Environment
- **Java**: 21
- **Neo4j**: 5-community (Docker)
- **Kafka**: Confluent 7.7.1
- **PostgreSQL**: 15-alpine

## Architecture & Bounded Cypher
Instead of sweeping the entire graph for cycles, the solution evaluates each incoming transaction as a potential "cycle-closing edge".
It runs the following bounded Cypher query using the incoming transaction properties as the anchor:

```cypher
MATCH path=(d:Account {account_id: $destId})-[:TRANSFERRED_TO*1..3]->(s:Account {account_id: $srcId})
WHERE ALL(n IN nodes(path) WHERE single(x IN nodes(path) WHERE x = n))
  AND ALL(r IN relationships(path) WHERE r.latest_occurred_at >= $windowStart)
  AND ALL(r IN relationships(path) WHERE r.total_amount_minor >= $minAmount AND r.total_amount_minor <= $maxAmount)
  AND all(i in range(0, length(path)-2) WHERE (relationships(path)[i]).latest_occurred_at <= (relationships(path)[i+1]).latest_occurred_at)
  AND (relationships(path)[length(path)-1]).latest_occurred_at <= $candidateTime
RETURN [n IN nodes(path) | n.account_id] AS accountIds, 
       [r IN relationships(path) | r.latest_transaction_id] AS transactionIds
```

**Constraints checked:**
1. **Bounded Depth:** `*1..3` limits the intermediate hop count.
2. **Distinct Intermediates:** `single(x IN nodes(path)...)` avoids sub-cycles.
3. **Time Range:** `$windowStart` filters old edges.
4. **Amount Tolerance:** Bounded by `$minAmount` and `$maxAmount` (e.g., +/- 10%).
5. **Chronological Ordering:** Checked via array index on path relationships ensuring sequence of occurrence.
6. **Cycle Deduplication:** Implemented by generating a unique hash from the sorted list of involved account IDs.

## Test Results

Automated Testcontainers verification:
```
[INFO] Tests run: 1, Failures: 0, Errors: 0, Skipped: 0
[INFO] BUILD SUCCESS
```

### Measured Query Latency
*During Testcontainers Integration Test run*
```
Circular flow confirmation query took 12 ms. Found 1 paths.
```

## Demo Output

When running `scripts/demo/phase-4.sh`, the system sequentially processes `A -> B`, `B -> C`, and finally `C -> A`. The `CircularFlowConfirmationService` catches the final candidate `C -> A` and successfully alerts:

```
6. Checking generated alerts in PostgreSQL...
                 deduplication_key                  |   rule_type   |         detected_at          |           primary_account_id         
----------------------------------------------------+---------------+------------------------------+--------------------------------------
 CIRCULAR_FLOW_a0000000-0..._b0000000..._c0000000...| CIRCULAR_FLOW | 2026-07-14 10:10:02.13501+00 | c0000000-0000-0000-0000-00000000000c
(1 row)
```

## Known Limitations
- The `TRANSFERRED_TO` edge is aggregated. While `latest_occurred_at` represents the most recent transaction between two accounts, heavy traffic between the identical accounts might artificially shift this timestamp and impact chronological evaluation of older unconfirmed cycles.
- Cycle deduplication assumes only one meaningful circular flow occurs between the exact same set of accounts within the window.
