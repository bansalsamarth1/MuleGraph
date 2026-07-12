# Phase 1C Evidence: Kafka Producer Integration

Below is the verification output for the Kafka Producer Integration.

**Date:** 2026-07-13
**Command:** `./scripts/demo/phase-1c.sh`

```text
=== MuleGraph Phase 1C Verification ===
1. Running unit & integration tests...
[INFO] Tests run: 9, Failures: 0, Errors: 0, Skipped: 1
[INFO] BUILD SUCCESS
...

2. Starting Kafka (KRaft) via Docker Compose...
[+] Running 2/2
 ✔ Network mulegraph_default  Created
 ✔ Container mulegraph-kafka  Started

3. Starting application...
Waiting for application to become healthy...
...

4. Testing valid payload (Expect 202 Accepted)
{"event_id":"c227ec4c-ca89-45fc-a7a8-f1741f075805","transaction_id":"8952781a-15ee-4811-87a3-0309a01e517a","status":"ACCEPTED"}
HTTP_STATUS: 202

5. Verifying message on Kafka topic transactions.raw
{"event_id":"c227ec4c-ca89-45fc-a7a8-f1741f075805","transaction_id":"8952781a-15ee-4811-87a3-0309a01e517a","schema_version":1,"correlation_id":"c97f0e06-e231-4e2a-b7d1-045db14a9fa3","source_account_id":"fecf4a77-1bf0-4de0-aaca-9dfb92a2d972","destination_account_id":"74c7d5d8-da82-4b00-b280-7a7272f257bb","amount_minor":125050,"currency":"INR","device_id":"device-123","ip_hash":"e3b0c44298fc1c149afbf4c8996fb92427ae41e4649b934ca495991b7852b855","occurred_at":1783938030.000000000,"ingested_at":1783892632.128743000}
Processed a total of 1 messages

6. Stopping application...
7. Tearing down Kafka...
=== End Verification ===
```
