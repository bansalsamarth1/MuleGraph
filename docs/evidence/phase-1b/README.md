# Phase 1B Evidence: API Contract and Security

Below is the verification output for the Transaction API contract and security layer in Phase 1B.

**Date:** 2026-07-13
**Command:** `./scripts/demo/phase-1b.sh`

```text
=== MuleGraph Phase 1B Verification ===
1. Running unit tests...
...
[INFO] Tests run: 8, Failures: 0, Errors: 0, Skipped: 0
[INFO] ------------------------------------------------------------------------
[INFO] BUILD SUCCESS
...

2. Starting application...
Waiting for application to become healthy...
3. Testing missing API key
Result: 401
4. Testing invalid API key
Result: 403
5. Testing valid payload
Result: {"event_id":"9080c38e-47aa-47a1-b31f-734d52d617cd","transaction_id":"56f35229-5b1d-4e3e-bc47-2d9b6637757c","status":"ACCEPTED_TEST_MODE"}
200
6. Testing same source and destination
Result: {"error":"Source and destination accounts must be different."}
400
7. Stopping application...
=== End Verification ===
```
