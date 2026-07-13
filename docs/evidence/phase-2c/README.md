# Phase 2C Evidence: Fan-In Detection

Below is the verification output demonstrating the Kafka Streams Fan-In rule detecting exactly 5 distinct sources and emitting a single deduplicated `FAN_IN` candidate to the `fraud.candidates` topic.

**Date:** 2026-07-13
**Command:** `./scripts/demo/phase-2c.sh`

```text
=== MuleGraph Phase 2C Verification: Fan-In Rule ===
1. Starting Kafka (KRaft) via Docker Compose...
2. Starting application...
3. Sending 5 transactions from FIVE distinct sources to ONE destination...

Sending transaction 1 from source 850a5814-7cf6-455b-bf42-0fbc49eb1222
Sending transaction 2 from source bc1ed1fa-6531-4ec9-86ab-d2427f272a80
Sending transaction 3 from source a5d893d5-e325-4b07-94d3-cd72fb0869a8
Sending transaction 4 from source 78370de8-61d5-451e-b851-5b7fb56e6d19
Sending transaction 5 from source cf8c11bd-3453-4318-9c60-a2dbd5dbf861

Waiting for Kafka Streams to aggregate and emit candidates...

4. Verifying candidates on Kafka topic fraud.candidates
We expect EXACTLY ONE candidate to be emitted.
{"candidate_id":"18a7193d-2769-3fd4-b5fc-6f5efc6081e0","rule_type":"FAN_IN","primary_account_id":"218718bf-43f7-4d41-858c-2d7390cc746f","window_start":1783935960.000000000,"window_end":1783936020.000000000,"distinct_accounts_count":5,"transaction_count":5,"total_amount_minor":50000,"currency":"INR","generated_at":1783935980.948817000}

Processed a total of 1 messages
5. Stopping application...
6. Tearing down Kafka...
=== End Verification ===
```

As demonstrated, `distinct_accounts_count` reached `5` for the single destination account, triggering exactly one emitted `FraudCandidateEvent` with the generated UUID `18a7193d-2769-3fd4-b5fc-6f5efc6081e0`. Deduplication logic in the topology prevented multiple candidate events for the same time window.
