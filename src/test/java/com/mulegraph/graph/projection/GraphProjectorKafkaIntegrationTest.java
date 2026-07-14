package com.mulegraph.graph.projection;

import com.mulegraph.graph.event.GraphUpdateEvent;
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
import org.springframework.kafka.core.KafkaTemplate;
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
class GraphProjectorKafkaIntegrationTest {

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

    private Consumer<String, String> consumer;

    @BeforeEach
    void setUp() {
        Map<String, Object> consumerProps = KafkaTestUtils.consumerProps(kafka.getBootstrapServers(), "testGroup", "true");
        consumerProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        consumer = new KafkaConsumer<>(consumerProps, new StringDeserializer(), new StringDeserializer());
        consumer.subscribe(Collections.singleton("graph.updates.DLT"));
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
    void permanentInvalidEvent_routesToDLT() {
        // Send a completely invalid payload (a raw string instead of JSON object)
        kafkaTemplate.send("graph.updates", "invalid-json-payload");

        ConsumerRecord<String, String> record = KafkaTestUtils.getSingleRecord(consumer, "graph.updates.DLT", Duration.ofSeconds(10));
        assertThat(record).isNotNull();
        assertThat(record.value()).isEqualTo("invalid-json-payload");
    }

    @Test
    void validEvent_processedWithoutDLT() throws Exception {
        GraphUpdateEvent event = new GraphUpdateEvent();
        event.setEventId(UUID.randomUUID());
        event.setTransactionId(UUID.randomUUID());
        event.setSourceAccountId(UUID.randomUUID());
        event.setDestinationAccountId(UUID.randomUUID());
        event.setAmountMinor(100L);
        event.setCurrency("USD");
        event.setOccurredAt(Instant.now());
        event.setIngestedAt(Instant.now());
        
        String jsonPayload = objectMapper.writeValueAsString(event);
        kafkaTemplate.send("graph.updates", jsonPayload);

        // Verify it doesn't end up in DLT
        try {
            KafkaTestUtils.getSingleRecord(consumer, "graph.updates.DLT", Duration.ofSeconds(5));
            throw new AssertionError("Should not have received message in DLT");
        } catch (IllegalStateException e) {
            // Expected: no records found
            assertThat(e.getMessage()).contains("No records found");
        }
    }
}
