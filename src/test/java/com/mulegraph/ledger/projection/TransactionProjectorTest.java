package com.mulegraph.ledger.projection;

import com.mulegraph.ingestion.domain.InternalTransactionEvent;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.testcontainers.containers.PostgreSQLContainer;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;

import java.time.Instant;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = {
        "spring.kafka.streams.state.dir=/tmp/kafka-streams-${random.uuid}",
        "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.neo4j.Neo4jAutoConfiguration,org.springframework.boot.autoconfigure.data.neo4j.Neo4jDataAutoConfiguration,org.springframework.boot.autoconfigure.data.neo4j.Neo4jRepositoriesAutoConfiguration"
    }
)
@Testcontainers
class TransactionProjectorTest {

    @Container
    static PostgreSQLContainer<?> postgres = new PostgreSQLContainer<>("postgres:15-alpine")
            .withDatabaseName("mulegraph")
            .withUsername("muleuser")
            .withPassword("mulepass");

    @DynamicPropertySource
    static void configureProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.datasource.url", postgres::getJdbcUrl);
        registry.add("spring.datasource.username", postgres::getUsername);
        registry.add("spring.datasource.password", postgres::getPassword);
        registry.add("spring.flyway.enabled", () -> "true");
        registry.add("spring.kafka.listener.auto-startup", () -> "false");
    }

    @Autowired
    private TransactionProjector projector;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void testExactTransactionReplayIsSafeNoOp() {
        UUID txId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        InternalTransactionEvent event = new InternalTransactionEvent(eventId, txId, 1, "SYSTEM_A", UUID.randomUUID(), UUID.randomUUID(), 1000L, "USD", "device1", "ip1", Instant.now(), Instant.now());
        
        projector.project(event);
        Long count1 = jdbcTemplate.queryForObject("SELECT count(*) FROM transactions WHERE transaction_id = ?", Long.class, txId);
        assertEquals(1, count1);

        // Replay exact same event
        projector.project(event);
        Long count2 = jdbcTemplate.queryForObject("SELECT count(*) FROM transactions WHERE transaction_id = ?", Long.class, txId);
        assertEquals(1, count2, "Should safely no-op duplicate replay");
    }

    @Test
    void testSameTransactionIdWithConflictingEventDataThrows() {
        UUID txId = UUID.randomUUID();
        UUID eventId1 = UUID.randomUUID();
        UUID eventId2 = UUID.randomUUID(); // Conflicting event ID

        InternalTransactionEvent event1 = new InternalTransactionEvent(eventId1, txId, 1, "SYSTEM_A", UUID.randomUUID(), UUID.randomUUID(), 1000L, "USD", "device1", "ip1", Instant.now(), Instant.now());
        InternalTransactionEvent event2 = new InternalTransactionEvent(eventId2, txId, 1, "SYSTEM_B", UUID.randomUUID(), UUID.randomUUID(), 1000L, "USD", "device1", "ip1", Instant.now(), Instant.now());
        
        projector.project(event1);

        assertThrows(ConflictingTransactionException.class, () -> projector.project(event2), 
            "Should throw ConflictingTransactionException when payload/identity differs");
    }

    @Test
    void testTransactionWithMissingDeviceAndIp() {
        UUID txId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        InternalTransactionEvent event = new InternalTransactionEvent(eventId, txId, 1, "SYSTEM_A", UUID.randomUUID(), UUID.randomUUID(), 1000L, "USD", null, null, Instant.now(), Instant.now());
        
        projector.project(event);
        Long count = jdbcTemplate.queryForObject("SELECT count(*) FROM transactions WHERE transaction_id = ?", Long.class, txId);
        assertEquals(1, count, "Should successfully project transaction with null device and ip");
    }

    @Test
    void testTransactionWithDeviceOnly() {
        UUID txId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        InternalTransactionEvent event = new InternalTransactionEvent(eventId, txId, 1, "SYSTEM_A", UUID.randomUUID(), UUID.randomUUID(), 1000L, "USD", "device1", null, Instant.now(), Instant.now());
        
        projector.project(event);
        Long count = jdbcTemplate.queryForObject("SELECT count(*) FROM transactions WHERE transaction_id = ?", Long.class, txId);
        assertEquals(1, count, "Should successfully project transaction with device but null ip");
    }

    @Test
    void testTransactionWithIpOnly() {
        UUID txId = UUID.randomUUID();
        UUID eventId = UUID.randomUUID();
        InternalTransactionEvent event = new InternalTransactionEvent(eventId, txId, 1, "SYSTEM_A", UUID.randomUUID(), UUID.randomUUID(), 1000L, "USD", null, "ip1", Instant.now(), Instant.now());
        
        projector.project(event);
        Long count = jdbcTemplate.queryForObject("SELECT count(*) FROM transactions WHERE transaction_id = ?", Long.class, txId);
        assertEquals(1, count, "Should successfully project transaction with ip but null device");
    }
}
