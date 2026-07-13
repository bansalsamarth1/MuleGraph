package com.mulegraph.graph.projection;

import com.mulegraph.graph.config.GraphConstraintInitializer;
import com.mulegraph.graph.event.GraphUpdateEvent;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.neo4j.driver.Driver;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.testcontainers.service.connection.ServiceConnection;
import org.springframework.data.neo4j.core.Neo4jClient;
import org.springframework.test.context.ActiveProfiles;
import org.testcontainers.containers.KafkaContainer;
import org.testcontainers.containers.Neo4jContainer;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.utility.DockerImageName;

import java.time.Instant;
import java.util.Collection;
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
class GraphProjectorTest {

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
    static void dynamicProperties(org.springframework.test.context.DynamicPropertyRegistry registry) {
        registry.add("KAFKA_BOOTSTRAP_SERVERS", kafka::getBootstrapServers);
        registry.add("spring.neo4j.uri", neo4jContainer::getBoltUrl);
        registry.add("spring.neo4j.authentication.username", () -> "neo4j");
        registry.add("spring.neo4j.authentication.password", neo4jContainer::getAdminPassword);
    }

    @Autowired
    private Neo4jClient neo4jClient;

    @Autowired
    private Driver driver;

    @Autowired
    private GraphProjector graphProjector;

    @Autowired
    private GraphConstraintInitializer constraintInitializer;

    @BeforeEach
    void setUp() {
        try (var session = driver.session()) {
            session.run("MATCH (n) DETACH DELETE n");
        }
    }

    @Test
    void project_insertsDistinctAccountsAndRelationship() {
        GraphUpdateEvent event = createEvent(UUID.randomUUID(), UUID.randomUUID(), 1000L, "USD");
        graphProjector.project(event);

        Collection<Map<String, Object>> results = neo4jClient.query(
                "MATCH (s:Account)-[r:TRANSFERRED_TO]->(d:Account) RETURN s.account_id AS src, d.account_id AS dst, r.transaction_count AS count, r.total_amount_minor AS amt"
        ).fetch().all();

        assertThat(results).hasSize(1);
        Map<String, Object> row = results.iterator().next();
        assertThat(row.get("src")).isEqualTo(event.getSourceAccountId().toString());
        assertThat(row.get("dst")).isEqualTo(event.getDestinationAccountId().toString());
        assertThat(row.get("count")).isEqualTo(1L);
        assertThat(row.get("amt")).isEqualTo(1000L);
    }

    @Test
    void project_idempotent_duplicateEventReplayIgnored() {
        GraphUpdateEvent event = createEvent(UUID.randomUUID(), UUID.randomUUID(), 1000L, "USD");
        
        graphProjector.project(event);
        graphProjector.project(event); // Replay
        
        Collection<Map<String, Object>> results = neo4jClient.query(
                "MATCH (s:Account)-[r:TRANSFERRED_TO]->(d:Account) RETURN r.transaction_count AS count, r.total_amount_minor AS amt"
        ).fetch().all();

        assertThat(results).hasSize(1);
        Map<String, Object> row = results.iterator().next();
        // Should still be 1 count and 1000 amt
        assertThat(row.get("count")).isEqualTo(1L);
        assertThat(row.get("amt")).isEqualTo(1000L);
    }

    @Test
    void project_twoDistinctEventsSameAccounts_aggregatesProperly() {
        UUID src = UUID.randomUUID();
        UUID dst = UUID.randomUUID();

        GraphUpdateEvent event1 = createEvent(src, dst, 1000L, "USD");
        GraphUpdateEvent event2 = createEvent(src, dst, 2500L, "USD");
        // Ensure event2 is later
        event2.setOccurredAt(event1.getOccurredAt().plusSeconds(10));

        graphProjector.project(event1);
        graphProjector.project(event2);

        Collection<Map<String, Object>> results = neo4jClient.query(
                "MATCH (s:Account)-[r:TRANSFERRED_TO]->(d:Account) RETURN r.transaction_count AS count, r.total_amount_minor AS amt, r.latest_transaction_id AS latest_tx"
        ).fetch().all();

        assertThat(results).hasSize(1);
        Map<String, Object> row = results.iterator().next();
        assertThat(row.get("count")).isEqualTo(2L);
        assertThat(row.get("amt")).isEqualTo(3500L);
        assertThat(row.get("latest_tx")).isEqualTo(event2.getTransactionId().toString());
    }

    @Test
    void project_currencySeparation() {
        UUID src = UUID.randomUUID();
        UUID dst = UUID.randomUUID();

        GraphUpdateEvent event1 = createEvent(src, dst, 1000L, "USD");
        GraphUpdateEvent event2 = createEvent(src, dst, 500L, "EUR");

        graphProjector.project(event1);
        graphProjector.project(event2);

        Collection<Map<String, Object>> results = neo4jClient.query(
                "MATCH (s:Account)-[r:TRANSFERRED_TO]->(d:Account) RETURN r.currency AS curr, r.total_amount_minor AS amt"
        ).fetch().all();

        assertThat(results).hasSize(2);
        boolean hasUsd = results.stream().anyMatch(r -> r.get("curr").equals("USD") && r.get("amt").equals(1000L));
        boolean hasEur = results.stream().anyMatch(r -> r.get("curr").equals("EUR") && r.get("amt").equals(500L));
        
        assertThat(hasUsd).isTrue();
        assertThat(hasEur).isTrue();
    }

    @Test
    void project_missingDeviceAndIpAreIgnored() {
        GraphUpdateEvent event = createEvent(UUID.randomUUID(), UUID.randomUUID(), 1000L, "USD");
        event.setDeviceId(null);
        event.setIpHash(null);

        graphProjector.project(event);

        long deviceCount = neo4jClient.query("MATCH (d:Device) RETURN count(d)").fetchAs(Long.class).mappedBy((ts, r) -> r.get(0).asLong()).one().get();
        long ipCount = neo4jClient.query("MATCH (i:IPAddress) RETURN count(i)").fetchAs(Long.class).mappedBy((ts, r) -> r.get(0).asLong()).one().get();

        assertThat(deviceCount).isZero();
        assertThat(ipCount).isZero();
    }

    private GraphUpdateEvent createEvent(UUID src, UUID dst, long amt, String curr) {
        GraphUpdateEvent e = new GraphUpdateEvent();
        e.setEventId(UUID.randomUUID());
        e.setTransactionId(UUID.randomUUID());
        e.setSourceAccountId(src);
        e.setDestinationAccountId(dst);
        e.setAmountMinor(amt);
        e.setCurrency(curr);
        e.setDeviceId(UUID.randomUUID().toString());
        e.setIpHash(UUID.randomUUID().toString());
        e.setOccurredAt(Instant.now());
        e.setIngestedAt(Instant.now());
        e.setSchemaVersion(1);
        return e;
    }
}
