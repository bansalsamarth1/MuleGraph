# Phase 2A Evidence: Kafka Streams Foundation

Below is the verification output demonstrating the Kafka Streams Foundation topology correctly extracting the `occurred_at` timestamp and re-keying events by `source_account_id`.

**Date:** 2026-07-13
**Command:** `./scripts/demo/phase-2a.sh`

```text
=== MuleGraph Phase 2A Verification ===
1. Starting Kafka (KRaft) via Docker Compose...
2. Starting application...

3. Testing Out-of-Order Payloads...
{"event_id":"e8f00fe8-e505-4e31-b1bd-c31f700dc6f2","transaction_id":"12d614b7-4d20-47ff-ba09-db6116165e59","status":"ACCEPTED"}
HTTP_STATUS: 202

{"event_id":"a1195a98-180f-454d-a7f2-23209951fa16","transaction_id":"1e3a6baa-c3f3-4add-9a17-c1d57415e53a","status":"ACCEPTED"}
HTTP_STATUS: 202

4. Verifying messages on Kafka topic transactions.by-source
Notice the CreateTime printed below matches the client's occurred_at timestamp!

CreateTime:1783894098000	{"event_id":"bc2925dc-2559-4bd3-9e7c-fbc95a25ac6e","transaction_id":"f885c469-29f3-4b1c-a04f-30a87515768e","schema_version":1,"correlation_id":"f65d3118-b786-47a5-81e0-9bf8a4c63e81","source_account_id":"d18bacfa-4345-4bdb-8cb6-636eb029272b","destination_account_id":"f928c9f8-fb82-4f2c-8499-c6820fddcada","amount_minor":10000,"currency":"INR","device_id":"device-123","ip_hash":"hash-123","occurred_at":1783894098.000000000,"ingested_at":1783894108.519684000}
CreateTime:1783894103000	{"event_id":"22600816-0b80-493d-a2aa-c150a19517aa","transaction_id":"f57011a4-4731-404f-9904-ea3cb9d09219","schema_version":1,"correlation_id":"6467c887-5ccf-490b-89b5-07eaced05fd9","source_account_id":"d18bacfa-4345-4bdb-8cb6-636eb029272b","destination_account_id":"f928c9f8-fb82-4f2c-8499-c6820fddcada","amount_minor":10000,"currency":"INR","device_id":"device-123","ip_hash":"hash-123","occurred_at":1783894103.000000000,"ingested_at":1783894108.581230000}

Processed a total of 2 messages
5. Stopping application...
6. Tearing down Kafka...
=== End Verification ===
```

As demonstrated, the `CreateTime` matches exactly the `occurred_at` values rather than the `ingested_at` processing time values.
