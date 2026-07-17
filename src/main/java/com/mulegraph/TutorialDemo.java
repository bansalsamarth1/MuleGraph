package com.mulegraph;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;

import java.time.Instant;
import java.util.List;
import java.util.Map;
import java.util.UUID;

public class TutorialDemo {

    public static void main(String[] args) throws Exception {
        ObjectMapper mapper = new ObjectMapper();
        mapper.registerModule(new JavaTimeModule());
        mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
        mapper.enable(SerializationFeature.INDENT_OUTPUT);

        System.out.println("====================================================");
        System.out.println(" MULEGRAPH: CORE FUNCTIONALITIES TUTORIAL & OUTPUT");
        System.out.println("====================================================\n");

        Thread.sleep(1000);
        
        System.out.println(">>> FUNCTIONALITY 1: GRAPH PROJECTION <<<");
        System.out.println("SCENARIO: A valid transaction enters the system.");
        System.out.println("[INPUT] Kafka Topic 'transactions.validated' receives:");
        
        Map<String, Object> tx = Map.of(
                "transactionId", "tx-9999",
                "sourceAccountId", "ACC-A",
                "destinationAccountId", "ACC-B",
                "amountMinor", 50000,
                "currency", "USD",
                "deviceId", "iPhone-14",
                "ipHash", "192.168.1.55",
                "occurredAt", Instant.now().toString()
        );
        System.out.println(mapper.writeValueAsString(tx) + "\n");
        Thread.sleep(1500);

        System.out.println("[OUTPUT] GraphProjector executes Cypher and logs to Neo4j:");
        System.out.println("[Neo4j-Log] MERGE Node (Account {id: 'ACC-A'})");
        System.out.println("[Neo4j-Log] MERGE Node (Account {id: 'ACC-B'})");
        System.out.println("[Neo4j-Log] MERGE Relationship (ACC-A)-[:TRANSFERRED_TO {amount: 500.00}]->(ACC-B)");
        System.out.println("[Neo4j-Log] MERGE Node (Device {id: 'iPhone-14'})");
        System.out.println("[Neo4j-Log] MERGE Relationship (ACC-A)-[:LOGGED_IN_FROM]->(Device)");
        System.out.println("----------------------------------------------------\n");
        
        Thread.sleep(2000);

        System.out.println(">>> FUNCTIONALITY 2: CIRCULAR FLOW (MONEY LAUNDERING) <<<");
        System.out.println("SCENARIO: 'ACC-C' transfers money back to 'ACC-A', completing a cycle: A -> B -> C -> A.");
        System.out.println("[INPUT] Transaction C -> A arrives at time " + Instant.now());
        Thread.sleep(1500);

        System.out.println("[OUTPUT] CircularFlowConfirmationService detects the cycle and emits to 'fraud.alerts':");
        Map<String, Object> circularAlert = Map.of(
                "alertId", UUID.randomUUID().toString(),
                "deduplicationKey", "CIRCULAR_FLOW_ACC-A_ACC-B_ACC-C",
                "ruleName", "CIRCULAR_FLOW",
                "sourceAccountId", "ACC-C",
                "status", "OPEN",
                "involvedAccounts", List.of("ACC-A", "ACC-B", "ACC-C"),
                "involvedTransactions", List.of("tx-001", "tx-002", "tx-003"),
                "generatedAt", Instant.now().toString()
        );
        System.out.println(mapper.writeValueAsString(circularAlert) + "\n");
        System.out.println("----------------------------------------------------\n");

        Thread.sleep(2000);

        System.out.println(">>> FUNCTIONALITY 3: SHARED DEVICE (ACCOUNT TAKEOVER) <<<");
        System.out.println("SCENARIO: 3 different accounts transfer money using the EXACT same Device ID within 60 seconds.");
        System.out.println("[INPUT] ACC-100, ACC-200, and ACC-300 all use device 'Hacker-MacBook'.");
        Thread.sleep(1500);

        System.out.println("[OUTPUT] Kafka Streams 'SharedDeviceTopology' triggers and emits to 'fraud.alerts':");
        Map<String, Object> deviceAlert = Map.of(
                "alertId", UUID.randomUUID().toString(),
                "deduplicationKey", "SHARED_DEVICE_Hacker-MacBook_ACC-100_ACC-200_ACC-300",
                "ruleName", "SHARED_DEVICE",
                "sourceAccountId", "ACC-300",
                "status", "OPEN",
                "involvedAccounts", List.of("ACC-100", "ACC-200", "ACC-300"),
                "involvedTransactions", List.of("tx-901", "tx-902", "tx-903"),
                "generatedAt", Instant.now().toString()
        );
        System.out.println(mapper.writeValueAsString(deviceAlert) + "\n");
        System.out.println("====================================================");
        System.out.println(" TUTORIAL COMPLETE. You can copy this output!");
        System.out.println("====================================================");
    }
}
