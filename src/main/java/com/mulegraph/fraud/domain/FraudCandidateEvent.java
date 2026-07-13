package com.mulegraph.fraud.domain;

import com.fasterxml.jackson.annotation.JsonProperty;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

public record FraudCandidateEvent(
        @JsonProperty("candidate_id")
        UUID candidateId,

        @JsonProperty("deduplication_key")
        String deduplicationKey,

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

        @JsonProperty("involved_accounts")
        Set<UUID> involvedAccounts,

        @JsonProperty("transaction_ids")
        Set<UUID> transactionIds,

        @JsonProperty("generated_at")
        Instant generatedAt
) {}
