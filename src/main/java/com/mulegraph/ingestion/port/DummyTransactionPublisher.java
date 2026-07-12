package com.mulegraph.ingestion.port;

import com.mulegraph.ingestion.domain.InternalTransactionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Component;

@Component
public class DummyTransactionPublisher implements TransactionPublisher {

    private static final Logger log = LoggerFactory.getLogger(DummyTransactionPublisher.class);

    @Override
    public void publish(InternalTransactionEvent event) {
        log.info("DUMMY PUBLISH: Event {} accepted for transaction {}. Note: Kafka is not yet configured (Phase 1B).",
                event.eventId(), event.transactionId());
    }
}
