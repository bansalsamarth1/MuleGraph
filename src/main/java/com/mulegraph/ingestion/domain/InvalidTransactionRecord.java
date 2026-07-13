package com.mulegraph.ingestion.domain;

import java.time.Instant;

public record InvalidTransactionRecord(
        InternalTransactionEvent event,
        String failureReason,
        Instant failedAt
) {}
