package com.mulegraph.fraud.domain;

import java.time.Instant;
import java.util.Set;
import java.util.UUID;

public record AlertEvent(
        UUID alertId,
        String ruleType,
        UUID primaryAccountId,
        String status,
        Set<UUID> involvedAccounts,
        Set<UUID> transactionIds,
        Instant generatedAt
) {
}
