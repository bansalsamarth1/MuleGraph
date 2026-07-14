package com.mulegraph.ingestion;

import com.mulegraph.ingestion.domain.InternalTransactionEvent;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.web.client.TestRestTemplate;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.http.*;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Duration;
import java.util.Collections;
import java.util.Properties;
import java.util.UUID;

import static org.assertj.core.api.Assertions.assertThat;

// @Disabled removed
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT,
    properties = {
        "spring.kafka.streams.state.dir=/tmp/kafka-streams-${random.uuid}",
        "mulegraph.api.key=dev-local-api-key"
    }
)
@Testcontainers
public class KafkaProducerIntegrationTest {

    @Container
    @ServiceConnection
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine");

    @Container
    @ServiceConnection
    static KafkaContainer kafkaContainer = new KafkaContainer(DockerImageName.parse("confluentinc/cp-kafka:7.7.1"))
            .withEnv("KAFKA_LISTENERS", "PLAINTEXT://0.0.0.0:9093,BROKER://0.0.0.0:9092")
            .withEnv("KAFKA_LISTENER_SECURITY_PROTOCOL_MAP", "BROKER:PLAINTEXT,PLAINTEXT:PLAINTEXT")
            .withEnv("KAFKA_INTER_BROKER_LISTENER_NAME", "BROKER");

    @org.springframework.test.context.DynamicPropertySource
    static void kafkaProperties(org.springframework.test.context.DynamicPropertyRegistry registry) {
        registry.add("KAFKA_BOOTSTRAP_SERVERS", kafkaContainer::getBootstrapServers);
    }

    @Autowired
    private TestRestTemplate restTemplate;

    private KafkaConsumer<String, InternalTransactionEvent> consumer;

    @BeforeEach
    void setUp() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaContainer.getBootstrapServers());
        props.put(ConsumerConfig.GROUP_ID_CONFIG, "test-group-" + UUID.randomUUID());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        
        JsonDeserializer<InternalTransactionEvent> jsonDeserializer = new JsonDeserializer<>(InternalTransactionEvent.class);
        jsonDeserializer.addTrustedPackages("*");

        consumer = new KafkaConsumer<>(props, new org.apache.kafka.common.serialization.StringDeserializer(), jsonDeserializer);
        consumer.subscribe(Collections.singletonList("transactions.raw"));
    }

    @AfterEach
    void tearDown() {
        if (consumer != null) {
            consumer.close();
        }
    }

    @Test
    void validTransactionIsPublishedToKafkaAndReturns202() {
        UUID txId = UUID.randomUUID();
        UUID srcId = UUID.randomUUID();
        UUID destId = UUID.randomUUID();

        String payload = """
                {
                  "transaction_id": "%s",
                  "source_account_id": "%s",
                  "destination_account_id": "%s",
                  "amount_minor": 50000,
                  "currency": "USD",
                  "device_id": "integration-device",
                  "ip_hash": "testhash",
                  "occurred_at": "2026-07-13T10:00:00Z"
                }
                """.formatted(txId, srcId, destId);

        HttpHeaders headers = new HttpHeaders();
        headers.setContentType(MediaType.APPLICATION_JSON);
        headers.set("X-API-Key", "dev-local-api-key");

        HttpEntity<String> request = new HttpEntity<>(payload, headers);

        ResponseEntity<String> response = restTemplate.postForEntity("/api/v1/transactions", request, String.class);
        
        assertThat(response.getStatusCode()).isEqualTo(HttpStatus.ACCEPTED);

        // Verify Kafka message
        var records = consumer.poll(Duration.ofSeconds(10));
        assertThat(records.count()).isGreaterThanOrEqualTo(1);

        boolean found = false;
        for (ConsumerRecord<String, InternalTransactionEvent> record : records) {
            InternalTransactionEvent event = record.value();
            if (txId.equals(event.transactionId())) {
                found = true;
                assertThat(event.sourceAccountId()).isEqualTo(srcId);
                assertThat(event.destinationAccountId()).isEqualTo(destId);
                assertThat(event.amountMinor()).isEqualTo(50000L);
                assertThat(event.eventId()).isNotNull();
                assertThat(event.ingestedAt()).isNotNull();
                
                // Key should be source account ID
                assertThat(record.key()).isEqualTo(srcId.toString());
            }
        }
        assertThat(found).isTrue();
    }
}
