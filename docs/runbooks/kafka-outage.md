# Runbook: Kafka Outage

## Symptoms
- API returns 503 Service Unavailable (due to `acks=all` failing or broker timeout).
- `mulegraph-prometheus` dashboard shows sharp drop in API acceptance.
- `mulegraph-prometheus` shows producer failures.
- Logs show `TimeoutException` or `BrokerNotAvailableException`.

## Impact
- Ingestion is **completely blocked**. New transactions cannot be accepted.
- Downstream processing (ledger, fraud, graph) is **paused** (waiting for new data).
- No data is lost for already-accepted transactions, as they were acknowledged only after durable storage.

## Resolution
1. Check Kafka container status: `docker ps | grep kafka`.
2. Inspect Kafka logs for disk space issues or memory pressure.
3. Restart Kafka: `docker compose restart kafka`.
4. Wait for controller quorum to re-establish.

## Recovery Behaviour
- API will automatically resume accepting transactions once producer reconnects.
- Kafka Streams will reconnect, rebalance, and resume processing from committed offsets.
- No manual intervention is needed for data recovery.
