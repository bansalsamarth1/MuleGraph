package com.mulegraph.ingestion.port;

import com.mulegraph.ingestion.domain.InternalTransactionEvent;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import java.util.concurrent.ExecutionException;

@Component
public class KafkaTransactionPublisher implements TransactionPublisher {

    private static final String TOPIC = "transactions.raw";
    private final KafkaTemplate<String, InternalTransactionEvent> kafkaTemplate;

    public KafkaTransactionPublisher(KafkaTemplate<String, InternalTransactionEvent> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    @Override
    public void publish(InternalTransactionEvent event) {
        try {
            // Synchronously wait for broker acknowledgment to satisfy API contract
            kafkaTemplate.send(TOPIC, event.sourceAccountId().toString(), event).get();
        } catch (InterruptedException | ExecutionException e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw new RuntimeException("Failed to publish transaction to Kafka", e);
        }
    }
}
