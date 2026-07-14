package com.mulegraph.graph.projection;

import com.mulegraph.fraud.domain.AlertEvent;
import com.mulegraph.ingestion.domain.InternalTransactionEvent;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.neo4j.core.Neo4jClient;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.test.utils.KafkaTestUtils;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.Collections;
import java.util.Map;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = {
        "spring.kafka.streams.state.dir=/tmp/kafka-streams-${random.uuid}"
    }
)
@ActiveProfiles("graph-projector")
@Testcontainers
class CircularFlowConfirmationServiceTest {

    @Container
    @ServiceConnection
    static Neo4jContainer<?> neo4jContainer = new Neo4jContainer<>("neo4j:5-community")
            .withEnv("NEO4J_PLUGINS", "[\"apoc\"]");

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine");

    @Container
    @ServiceConnection
    static KafkaContainer kafka = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.1"))
            .withEnv("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:9093,BROKER://0.0.0.0:9092")
            .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
            .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER");

    @org.springframework.test.context.DynamicPropertySource
    static void kafkaProperties(org.springframework.test.context.DynamicPropertyRegistry registry) {
        registry.add("KAFKA_BOOTSTRAP_SERVERS", kafka::getBootstrapServers);
    }

    @Autowired
    private KafkaTemplate<String, Object> kafkaTemplate;

    @Autowired
    private Neo4jClient neo4jClient;

    private Consumer<String, AlertEvent> consumer;

    @BeforeEach
    void setUp() {
        neo4jClient.query("MATCH (n) DETACH DELETE n").run();

        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(kafka.getBootstrapServers(), "testGroup-circular", "true");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        JsonDeserializer<AlertEvent> jsonDeserializer = new JsonDeserializer<>(AlertEvent.class);
        jsonDeserializer.addTrustedPackages("com.mulegraph.*");
        
        consumer = new KafkaConsumer<>(consumerProps, new StringDeserializer(), jsonDeserializer);
        consumer.subscribe(Collections.singleton("fraud.alerts"));
    }

    @AfterEach
    void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Autowired
    private com.fasterxml.jackson.databind.ObjectMapper objectMapper;

    @Test
    void cycleDetected_emitsAlert() throws Exception {
        UUID accountA = UUID.randomUUID();
        UUID accountB = UUID.randomUUID();
        UUID accountC = UUID.randomUUID();

        Instant t1 = Instant.now().minus(2, ChronoUnit.HOURS);
        Instant t2 = Instant.now().minus(1, ChronoUnit.HOURS);
        Instant t3 = Instant.now(); // the candidate transaction time

        // Pre-seed A -> B
        neo4jClient.query("""
                MERGE (a:Account {account_id: $src})
                MERGE (b:Account {account_id: $dst})
                MERGE (a)-[r:TRANSFERRED_TO]->(b)
                SET r.total_amount_minor = 1000, r.latest_occurred_at = $time, r.latest_transaction_id = $txId
                """)
                .bind(accountA.toString()).to("src")
                .bind(accountB.toString()).to("dst")
                .bind(t1.toString()).to("time")
                .bind(UUID.randomUUID().toString()).to("txId")
                .run();

        // Pre-seed B -> C
        neo4jClient.query("""
                MERGE (b:Account {account_id: $src})
                MERGE (c:Account {account_id: $dst})
                MERGE (b)-[r:TRANSFERRED_TO]->(c)
                SET r.total_amount_minor = 1000, r.latest_occurred_at = $time, r.latest_transaction_id = $txId
                """)
                .bind(accountB.toString()).to("src")
                .bind(accountC.toString()).to("dst")
                .bind(t2.toString()).to("time")
                .bind(UUID.randomUUID().toString()).to("txId")
                .run();

        // Candidate C -> A
        InternalTransactionEvent tx = new InternalTransactionEvent(
                UUID.randomUUID(),
                UUID.randomUUID(),
                1,
                UUID.randomUUID().toString(),
                accountC,
                accountA,
                1000L,
                "USD",
                "device1",
                "ip1",
                t3,
                t3
        );

        String jsonPayload = objectMapper.writeValueAsString(tx);
        org.springframework.messaging.Message<String> message = org.springframework.messaging.support.MessageBuilder
                .withPayload(jsonPayload)
                .setHeader(org.springframework.kafka.support.KafkaHeaders.TOPIC, "transactions.validated")
                .setHeader("__TypeId__", InternalTransactionEvent.class.getName().getBytes(java.nio.charset.StandardCharsets.UTF_8))
                .build();
        kafkaTemplate.send(message);

        ConsumerRecord<String, AlertEvent> record = KafkaTestUtils.getSingleRecord(consumer, "fraud.alerts", Duration.ofSeconds(10));
        assertThat(record).isNotNull();
        assertThat(record.value().ruleType()).isEqualTo("CIRCULAR_FLOW");
        assertThat(record.value().involvedAccounts()).containsExactlyInAnyOrder(accountA, accountB, accountC);
    }
}
