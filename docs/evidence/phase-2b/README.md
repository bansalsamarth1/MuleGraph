# Phase 2B Evidence: Fan-Out Detection

Below is the verification output demonstrating the Kafka Streams Fan-Out rule detecting exactly 5 distinct destinations and emitting a single deduplicated `FAN_OUT` candidate to the `fraud.candidates` topic.

**Date:** 2026-07-13
**Command:** `./scripts/demo/phase-2b.sh`

```text
=== MuleGraph Phase 2B Verification: Fan-Out Rule ===
1. Starting Kafka (KRaft) via Docker Compose...
2. Starting application...
3. Sending 5 transactions from ONE source to FIVE distinct destinations...

Sending transaction 1 to destination 5eb67a99-b1d5-455f-846f-c1f0b0c6114e
Sending transaction 2 to destination 28315147-380d-4581-9dd5-91db4057885b
Sending transaction 3 to destination e7cba100-305f-4a0b-9dfc-50bc9318a002
Sending transaction 4 to destination c3a70a4f-56bb-49e0-ad87-846174a7cb08
Sending transaction 5 to destination b9a66d11-53ec-49f8-b3ec-37e81dfd7bfa

Waiting for Kafka Streams to aggregate and emit candidates...

4. Verifying candidates on Kafka topic fraud.candidates
We expect EXACTLY ONE candidate to be emitted.
{"candidate_id":"d3b3b7fd-0a70-3e1f-a9c7-dcd6bf627733","rule_type":"FAN_OUT","primary_account_id":"417a019a-8ea5-433c-8328-666a89cc77c5","window_start":1783934940.000000000,"window_end":1783935000.000000000,"distinct_accounts_count":5,"transaction_count":5,"total_amount_minor":50000,"currency":"INR","generated_at":1783934988.252297000}

Processed a total of 1 messages
5. Stopping application...
6. Tearing down Kafka...
=== End Verification ===
```

As demonstrated, `distinct_accounts_count` reached `5` for the single source account, triggering exactly one emitted `FraudCandidateEvent` with the generated UUID `d3b3b7fd-0a70-3e1f-a9c7-dcd6bf627733`. Deduplication logic in the topology prevented multiple candidate events for the same time window.
