package com.mulegraph.graph.projection;

import com.mulegraph.fraud.config.CircularFlowRuleProperties;
import com.mulegraph.fraud.domain.AlertEvent;
import com.mulegraph.ingestion.domain.InternalTransactionEvent;
import org.springframework.context.annotation.Profile;
import org.springframework.data.neo4j.core.Neo4jClient;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Instant;
import java.util.*;
import java.util.stream.Collectors;

@Component
@Profile("graph-projector")
public class CircularFlowConfirmationService {

    private static final Logger log = LoggerFactory.getLogger(CircularFlowConfirmationService.class);

    private final Neo4jClient neo4jClient;
    private final KafkaTemplate<String, Object> kafkaTemplate;
    private final CircularFlowRuleProperties properties;

    public CircularFlowConfirmationService(Neo4jClient neo4jClient,
                                           KafkaTemplate<String, Object> kafkaTemplate,
                                           CircularFlowRuleProperties properties) {
        this.neo4jClient = neo4jClient;
        this.kafkaTemplate = kafkaTemplate;
        this.properties = properties;
    }

    @KafkaListener(
            topics = "transactions.validated",
            groupId = "mulegraph-circular-flow-projector-v1",
            containerFactory = "graphProjectorKafkaListenerContainerFactory"
    )
    public void confirmCircularFlow(InternalTransactionEvent event) {
        long startTime = System.nanoTime();

        int maxDepth = properties.getMaxDepth();
        long windowSeconds = properties.getWindowSeconds();
        int tolerance = properties.getAmountTolerancePercent();

        Instant windowStart = event.occurredAt().minusSeconds(windowSeconds);
        long candidateAmount = event.amountMinor();
        long minAmount = candidateAmount - (candidateAmount * tolerance / 100);
        long maxAmount = candidateAmount + (candidateAmount * tolerance / 100);

        String cypher = String.format("""
                MATCH path=(d:Account {account_id: $destId})-[:TRANSFERRED_TO*1..%d]->(s:Account {account_id: $srcId})
                WHERE ALL(n IN nodes(path) WHERE single(x IN nodes(path) WHERE x = n))
                  AND ALL(r IN relationships(path) WHERE r.latest_occurred_at >= $windowStart)
                  AND ALL(r IN relationships(path) WHERE r.total_amount_minor >= $minAmount AND r.total_amount_minor <= $maxAmount)
                  AND all(i in range(0, length(path)-2) WHERE (relationships(path)[i]).latest_occurred_at <= (relationships(path)[i+1]).latest_occurred_at)
                  AND (relationships(path)[length(path)-1]).latest_occurred_at <= $candidateTime
                RETURN [n IN nodes(path) | n.account_id] AS accountIds, 
                       [r IN relationships(path) | r.latest_transaction_id] AS transactionIds
                """, maxDepth - 1);

        Map<String, Object> parameters = new HashMap<>();
        parameters.put("destId", event.destinationAccountId().toString());
        parameters.put("srcId", event.sourceAccountId().toString());
        parameters.put("windowStart", windowStart.toString());
        parameters.put("candidateTime", event.occurredAt().toString());
        parameters.put("minAmount", minAmount);
        parameters.put("maxAmount", maxAmount);

        Collection<Map<String, Object>> results = neo4jClient.query(cypher).bindAll(parameters).fetch().all();
        
        long latencyMs = (System.nanoTime() - startTime) / 1000000;
        log.info("Circular flow confirmation query took {} ms. Found {} paths.", latencyMs, results.size());

        for (Map<String, Object> row : results) {
            List<String> accountIdsList = (List<String>) row.get("accountIds");
            List<String> txIdsList = (List<String>) row.get("transactionIds");

            Set<UUID> involvedAccounts = accountIdsList.stream()
                    .map(UUID::fromString)
                    .collect(Collectors.toSet());

            Set<UUID> transactionIds = txIdsList.stream()
                    .map(UUID::fromString)
                    .collect(Collectors.toSet());
            
            // Add the candidate transaction itself
            transactionIds.add(event.transactionId());

            // Deterministic deduplication key: sorted account IDs
            List<String> sortedAccounts = new ArrayList<>(accountIdsList);
            Collections.sort(sortedAccounts);
            String deduplicationKey = "CIRCULAR_FLOW_" + String.join("_", sortedAccounts);

            AlertEvent alert = new AlertEvent(
                    UUID.randomUUID(),
                    deduplicationKey,
                    "CIRCULAR_FLOW",
                    event.sourceAccountId(),
                    "OPEN",
                    involvedAccounts,
                    transactionIds,
                    Instant.now()
            );

            kafkaTemplate.send("fraud.alerts", alert.deduplicationKey(), alert);
        }
    }
}
