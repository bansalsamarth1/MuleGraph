package com.mulegraph.ingestion.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import java.util.UUID;

public record TransactionResponse(
        @JsonProperty("event_id")
        UUID eventId,

        @JsonProperty("transaction_id")
        UUID transactionId,

        @JsonProperty("status")
        String status
) {
}
