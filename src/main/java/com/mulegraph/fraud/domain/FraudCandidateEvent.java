package com.mulegraph.fraud.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.UUID;

public record FraudCandidateEvent(
        @JsonProperty("candidate_id")
        UUID candidateId,

        @JsonProperty("rule_type")
        String ruleType,

        @JsonProperty("primary_account_id")
        UUID primaryAccountId,

        @JsonProperty("window_start")
        Instant windowStart,

        @JsonProperty("window_end")
        Instant windowEnd,

        @JsonProperty("distinct_accounts_count")
        int distinctAccountsCount,

        @JsonProperty("transaction_count")
        long transactionCount,

        @JsonProperty("total_amount_minor")
        long totalAmountMinor,

        @JsonProperty("currency")
        String currency,

        @JsonProperty("generated_at")
        Instant generatedAt
) {}
