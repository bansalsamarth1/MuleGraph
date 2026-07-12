package com.mulegraph.ingestion.port;

import com.mulegraph.ingestion.domain.InternalTransactionEvent;

public interface TransactionPublisher {
    void publish(InternalTransactionEvent event);
}
