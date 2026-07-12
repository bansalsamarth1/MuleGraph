package com.mulegraph.ingestion.api;

import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import jakarta.validation.constraints.Positive;

import java.time.Instant;
import java.util.UUID;

public record TransactionRequest(
        @NotNull
        @JsonProperty("transaction_id")
        UUID transactionId,

        @NotNull
        @JsonProperty("source_account_id")
        UUID sourceAccountId,

        @NotNull
        @JsonProperty("destination_account_id")
        UUID destinationAccountId,

        @NotNull
        @Positive
        @JsonProperty("amount_minor")
        Long amountMinor,

        @NotBlank
        @Pattern(regexp = "^[A-Z]{3}$")
        @JsonProperty("currency")
        String currency,

        @JsonProperty("device_id")
        String deviceId,

        @JsonProperty("ip_hash")
        String ipHash,

        @NotNull
        @JsonProperty("occurred_at")
        Instant occurredAt
) {
}
