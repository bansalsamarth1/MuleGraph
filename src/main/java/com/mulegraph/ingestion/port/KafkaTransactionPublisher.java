package com.mulegraph.ingestion.port;

import com.mulegraph.ingestion.domain.InternalTransactionEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.concurrent.ExecutionException;

@Component
public class KafkaTransactionPublisher implements TransactionPublisher {

    private static final String TOPIC = "transactions.raw";
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final ObjectMapper objectMapper;

    public KafkaTransactionPublisher(KafkaTemplate<String, String> kafkaTemplate, ObjectMapper objectMapper) {
        this.kafkaTemplate = kafkaTemplate;
        this.objectMapper = objectMapper;
    }

    @Override
    public void publish(InternalTransactionEvent event) {
        try {
            String payload = objectMapper.writeValueAsString(event);
            // Synchronously wait for broker acknowledgment to satisfy API contract
            kafkaTemplate.send(TOPIC, event.sourceAccountId().toString(), payload).get();
        } catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException("Failed to publish transaction to Kafka", e);
        }
    }
}
