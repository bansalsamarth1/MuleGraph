# Phase 2D Evidence: Shared Device and Shared IP Detection

Below is the verification output demonstrating the Kafka Streams Shared Device and Shared IP rules detecting exactly 3 distinct sources acting through identical metadata vectors and emitting deduplicated `SHARED_DEVICE` and `SHARED_IP` candidates to the `fraud.candidates` topic.

**Date:** 2026-07-13
**Command:** `./scripts/demo/phase-2d.sh`

```text
=== MuleGraph Phase 2D Verification: Shared Device & Shared IP Rules ===
1. Starting Kafka (KRaft) via Docker Compose...
2. Starting application...
3. Sending 3 transactions from 3 distinct sources using ONE device...

Sending transaction 1 (Shared Device scenario)
Sending transaction 2 (Shared Device scenario)
Sending transaction 3 (Shared Device scenario)

4. Sending 3 transactions from 3 distinct sources using ONE IP hash...

Sending transaction 1 (Shared IP scenario)
Sending transaction 2 (Shared IP scenario)
Sending transaction 3 (Shared IP scenario)

Waiting for Kafka Streams to aggregate and emit candidates...

5. Verifying candidates on Kafka topic fraud.candidates
We expect EXACTLY TWO candidates to be emitted (one SHARED_DEVICE, one SHARED_IP).

{"candidate_id":"98407aa3-fadf-3b9b-9f05-83c9daa9dc3c","rule_type":"SHARED_DEVICE","primary_account_id":"d34178e2-4c30-34ae-b4bd-9abb0492c051","window_start":1783936980.000000000,"window_end":1783937040.000000000,"distinct_accounts_count":3,"transaction_count":3,"total_amount_minor":30000,"currency":"INR","generated_at":1783937019.811538000}
{"candidate_id":"39855992-0dee-32b1-86e1-6a43e37cd616","rule_type":"SHARED_IP","primary_account_id":"3ded3082-f361-360b-9485-1f5151f9556e","window_start":1783936980.000000000,"window_end":1783937040.000000000,"distinct_accounts_count":3,"transaction_count":3,"total_amount_minor":30000,"currency":"INR","generated_at":1783937020.081557000}

6. Stopping application...
7. Tearing down Kafka...
=== End Verification ===
```

As demonstrated, the deduplicated rules fired perfectly for `minDistinctSources = 3` independently without interfering with each other.
