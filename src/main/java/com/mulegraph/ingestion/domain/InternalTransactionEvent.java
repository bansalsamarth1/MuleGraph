package com.mulegraph.ingestion.domain;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.time.Instant;
import java.util.UUID;

public record InternalTransactionEvent(
        @JsonProperty("event_id")
        UUID eventId,

        @JsonProperty("transaction_id")
        UUID transactionId,

        @JsonProperty("schema_version")
        Integer schemaVersion,

        @JsonProperty("correlation_id")
        String correlationId,

        @JsonProperty("source_account_id")
        UUID sourceAccountId,

        @JsonProperty("destination_account_id")
        UUID destinationAccountId,

        @JsonProperty("amount_minor")
        Long amountMinor,

        @JsonProperty("currency")
        String currency,

        @JsonProperty("device_id")
        String deviceId,

        @JsonProperty("ip_hash")
        String ipHash,

        @JsonProperty("occurred_at")
        Instant occurredAt,

        @JsonProperty("ingested_at")
        Instant ingestedAt
) {
}
