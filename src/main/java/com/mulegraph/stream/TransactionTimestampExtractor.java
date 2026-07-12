package com.mulegraph.stream;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import com.mulegraph.ingestion.domain.InternalTransactionEvent;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.streams.processor.TimestampExtractor;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

public class TransactionTimestampExtractor implements TimestampExtractor {

    private static final Logger log = LoggerFactory.getLogger(TransactionTimestampExtractor.class);
    private static final ObjectMapper objectMapper = new ObjectMapper().registerModule(new JavaTimeModule());

    @Override
    public long extract(ConsumerRecord<Object, Object> record, long partitionTime) {
        if (record.value() == null) {
            return partitionTime;
        }

        try {
            InternalTransactionEvent event;
            if (record.value() instanceof InternalTransactionEvent) {
                event = (InternalTransactionEvent) record.value();
            } else if (record.value() instanceof String) {
                event = objectMapper.readValue((String) record.value(), InternalTransactionEvent.class);
            } else if (record.value() instanceof byte[]) {
                event = objectMapper.readValue((byte[]) record.value(), InternalTransactionEvent.class);
            } else {
                log.warn("Unknown value type for timestamp extraction: {}", record.value().getClass());
                return partitionTime;
            }

            if (event.occurredAt() != null) {
                return event.occurredAt().toEpochMilli();
            }

        } catch (IOException e) {
            log.warn("Failed to extract timestamp from transaction event", e);
        }

        return partitionTime;
    }
}
