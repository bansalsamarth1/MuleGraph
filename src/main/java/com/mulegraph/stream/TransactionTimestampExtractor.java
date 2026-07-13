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
            InternalTransactionEvent event = null;
            com.mulegraph.fraud.domain.FraudCandidateEvent candidateEvent = null;
            if (record.value() instanceof InternalTransactionEvent) {
                event = (InternalTransactionEvent) record.value();
            } else if (record.value() instanceof com.mulegraph.fraud.domain.FraudCandidateEvent) {
                candidateEvent = (com.mulegraph.fraud.domain.FraudCandidateEvent) record.value();
            } else if (record.value() instanceof String) {
                String strVal = (String) record.value();
                if (strVal.contains("\"candidate_id\"")) {
                    candidateEvent = objectMapper.readValue(strVal, com.mulegraph.fraud.domain.FraudCandidateEvent.class);
                } else {
                    event = objectMapper.readValue(strVal, InternalTransactionEvent.class);
                }
            } else if (record.value() instanceof byte[]) {
                byte[] byteVal = (byte[]) record.value();
                String strVal = new String(byteVal);
                if (strVal.contains("\"candidate_id\"")) {
                    candidateEvent = objectMapper.readValue(byteVal, com.mulegraph.fraud.domain.FraudCandidateEvent.class);
                } else {
                    event = objectMapper.readValue(byteVal, InternalTransactionEvent.class);
                }
            } else {
                log.warn("Unknown value type for timestamp extraction: {}", record.value().getClass());
                return partitionTime;
            }

            if (event != null && event.occurredAt() != null) {
                return event.occurredAt().toEpochMilli();
            } else if (candidateEvent != null && candidateEvent.generatedAt() != null) {
                return candidateEvent.generatedAt().toEpochMilli();
            }

        } catch (IOException e) {
            log.warn("Failed to extract timestamp from transaction event", e);
        }

        return partitionTime;
    }
}
