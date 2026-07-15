package com.mulegraph.graph.projection;

import com.mulegraph.graph.event.GraphUpdateEvent;
import org.springframework.context.annotation.Profile;
import org.springframework.data.neo4j.core.Neo4jClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Component
@Profile("graph-projector")
public class GraphProjector {

    private final Neo4jClient neo4jClient;
    private final MeterRegistry meterRegistry;

    public GraphProjector(Neo4jClient neo4jClient, MeterRegistry meterRegistry) {
        this.neo4jClient = neo4jClient;
        this.meterRegistry = meterRegistry;
    }

    @KafkaListener(
            topics = "graph.updates",
            groupId = "mulegraph-graph-projector-v1",
            containerFactory = "graphProjectorKafkaListenerContainerFactory"
    )
    public void project(GraphUpdateEvent event) {
        String processingAttempt = UUID.randomUUID().toString();

        String cypher = """
                MERGE (e:ProcessedGraphEvent {event_id: $eventId})
                ON CREATE SET e.processed_at = timestamp(), e.processing_attempt = $processingAttempt
                WITH e
                WHERE e.processing_attempt = $processingAttempt
                
                MERGE (src:Account {account_id: $sourceAccountId})
                MERGE (dst:Account {account_id: $destinationAccountId})
                
                MERGE (src)-[r:TRANSFERRED_TO {currency: $currency}]->(dst)
                ON CREATE SET 
                    r.first_seen = $occurredAt, 
                    r.last_seen = $occurredAt,
                    r.transaction_count = 1, 
                    r.total_amount_minor = $amountMinor, 
                    r.latest_transaction_id = $transactionId,
                    r.latest_occurred_at = $occurredAt
                ON MATCH SET 
                    r.first_seen = CASE WHEN $occurredAt < r.first_seen THEN $occurredAt ELSE r.first_seen END,
                    r.last_seen = CASE WHEN $occurredAt > r.last_seen THEN $occurredAt ELSE r.last_seen END,
                    r.transaction_count = r.transaction_count + 1, 
                    r.total_amount_minor = r.total_amount_minor + $amountMinor, 
                    r.latest_transaction_id = CASE WHEN $occurredAt > r.latest_occurred_at THEN $transactionId ELSE r.latest_transaction_id END,
                    r.latest_occurred_at = CASE WHEN $occurredAt > r.latest_occurred_at THEN $occurredAt ELSE r.latest_occurred_at END
                
                WITH src, e
                FOREACH (deviceId IN CASE WHEN $deviceId IS NOT NULL AND $deviceId <> '' THEN [$deviceId] ELSE [] END |
                    MERGE (d:Device {device_id: deviceId})
                    MERGE (src)-[:USED_DEVICE]->(d)
                )
                FOREACH (ipHash IN CASE WHEN $ipHash IS NOT NULL AND $ipHash <> '' THEN [$ipHash] ELSE [] END |
                    MERGE (ip:IPAddress {ip_hash: ipHash})
                    MERGE (src)-[:USED_IP]->(ip)
                )
                """;

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("eventId", event.getEventId().toString());
        parameters.put("processingAttempt", processingAttempt);
        parameters.put("sourceAccountId", event.getSourceAccountId().toString());
        parameters.put("destinationAccountId", event.getDestinationAccountId().toString());
        parameters.put("currency", event.getCurrency());
        parameters.put("occurredAt", event.getOccurredAt().toString());
        parameters.put("amountMinor", event.getAmountMinor());
        parameters.put("transactionId", event.getTransactionId().toString());
        if (event.getDeviceId() != null) {
            parameters.put("deviceId", event.getDeviceId());
        } else {
            parameters.put("deviceId", "");
        }
        
        if (event.getIpHash() != null) {
            parameters.put("ipHash", event.getIpHash());
        } else {
            parameters.put("ipHash", "");
        }

        Timer.builder("mulegraph.neo4j.projection.latency")
                .description("Time taken to project a transaction to Neo4j")
                .register(meterRegistry)
                .record(() -> {
                    neo4jClient.query(cypher).bindAll(parameters).run();
                });
    }
}
