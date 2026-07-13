package com.mulegraph.fraud.projection;

import com.mulegraph.fraud.domain.AlertEvent;
import org.junit.jupiter.api.Disabled;
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
import java.util.Set;
import java.util.UUID;

import org.junit.jupiter.api.Disabled;

import static org.junit.jupiter.api.Assertions.assertEquals;

// @Disabled removed
@SpringBootTest(
    webEnvironment = SpringBootTest.WebEnvironment.NONE,
    properties = {
        "spring.kafka.streams.state.dir=/tmp/kafka-streams-${random.uuid}",
        "spring.autoconfigure.exclude=org.springframework.boot.autoconfigure.neo4j.Neo4jAutoConfiguration,org.springframework.boot.autoconfigure.data.neo4j.Neo4jDataAutoConfiguration,org.springframework.boot.autoconfigure.data.neo4j.Neo4jRepositoriesAutoConfiguration"
    }
)
@Testcontainers
class AlertProjectorTest {

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
        // Disable flyway for test if we want, but since we are testing projection, we NEED flyway to run!
        registry.add("spring.flyway.enabled", () -> "true");
        // Use random state dir for tests to avoid conflicts
        registry.add("spring.kafka.streams.state.dir", () -> "/tmp/kafka-streams-" + java.util.UUID.randomUUID().toString());
        // Disable kafka auto-start since we only test the projector bean method
        registry.add("spring.kafka.listener.auto-startup", () -> "false");
    }

    @Autowired
    private AlertProjector alertProjector;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void testIdempotentAlertProjection() {
        System.out.println("DOCKER_API_VERSION = " + System.getenv("DOCKER_API_VERSION"));
        System.out.println("DOCKER_HOST = " + System.getenv("DOCKER_HOST"));
        System.out.println("ryuk.disabled = " + System.getProperty("ryuk.disabled"));
        UUID alertId = UUID.randomUUID();
        UUID account1 = UUID.randomUUID();
        UUID account2 = UUID.randomUUID();
        UUID tx1 = UUID.randomUUID();
        UUID tx2 = UUID.randomUUID();

        AlertEvent event = new AlertEvent(
                alertId,
                "DEDUP-" + alertId,
                "FAN_OUT",
                UUID.randomUUID(),
                "OPEN",
                Set.of(account1, account2),
                Set.of(tx1, tx2),
                Instant.now()
        );

        // First projection
        alertProjector.handleAlert(event);

        // Verify inserts
        Long alertCount = jdbcTemplate.queryForObject("SELECT count(*) FROM alerts WHERE alert_id = ?", Long.class, alertId);
        assertEquals(1, alertCount);

        Long accountCount = jdbcTemplate.queryForObject("SELECT count(*) FROM alert_accounts WHERE alert_id = ?", Long.class, alertId);
        assertEquals(2, accountCount);

        Long txCount = jdbcTemplate.queryForObject("SELECT count(*) FROM alert_transactions WHERE alert_id = ?", Long.class, alertId);
        assertEquals(2, txCount);

        // Idempotent projection (simulate exactly-once failure retry)
        alertProjector.handleAlert(event);

        // Verify counts remain identical
        alertCount = jdbcTemplate.queryForObject("SELECT count(*) FROM alerts WHERE alert_id = ?", Long.class, alertId);
        assertEquals(1, alertCount, "Duplicate projection should not insert another alert");

        accountCount = jdbcTemplate.queryForObject("SELECT count(*) FROM alert_accounts WHERE alert_id = ?", Long.class, alertId);
        assertEquals(2, accountCount, "Duplicate projection should not insert duplicate accounts");

        txCount = jdbcTemplate.queryForObject("SELECT count(*) FROM alert_transactions WHERE alert_id = ?", Long.class, alertId);
        assertEquals(2, txCount, "Duplicate projection should not insert duplicate transactions");
    }

    @Test
    void testDifferentAlertIdSameDeduplicationKey() {
        String dedupKey = "DEDUP-TEST-DIFF-ID";
        UUID alert1Id = UUID.randomUUID();
        UUID alert2Id = UUID.randomUUID();
        
        AlertEvent event1 = new AlertEvent(alert1Id, dedupKey, "FAN_IN", UUID.randomUUID(), "OPEN", Set.of(UUID.randomUUID()), Set.of(UUID.randomUUID()), Instant.now());
        AlertEvent event2 = new AlertEvent(alert2Id, dedupKey, "FAN_IN", UUID.randomUUID(), "OPEN", Set.of(UUID.randomUUID()), Set.of(UUID.randomUUID()), Instant.now());
        
        alertProjector.handleAlert(event1);
        alertProjector.handleAlert(event2);
        
        Long totalAlerts = jdbcTemplate.queryForObject("SELECT count(*) FROM alerts WHERE deduplication_key = ?", Long.class, dedupKey);
        assertEquals(1, totalAlerts, "Different alert IDs with same dedup key should result in 1 alert");

        Long alert2Count = jdbcTemplate.queryForObject("SELECT count(*) FROM alerts WHERE alert_id = ?", Long.class, alert2Id);
        assertEquals(0, alert2Count, "Second alert ID should not exist");
        
        Long evidenceCountForAlert1 = jdbcTemplate.queryForObject("SELECT count(*) FROM alert_accounts WHERE alert_id = ?", Long.class, alert1Id);
        assertEquals(2, evidenceCountForAlert1, "Evidence for second alert should be linked to canonical first alert ID");
    }

    @Test
    void testDifferentWindowsCreateTwoAlerts() {
        UUID alert1Id = UUID.randomUUID();
        UUID alert2Id = UUID.randomUUID();
        
        AlertEvent event1 = new AlertEvent(alert1Id, "DEDUP-WIN1", "FAN_OUT", UUID.randomUUID(), "OPEN", null, null, Instant.now());
        AlertEvent event2 = new AlertEvent(alert2Id, "DEDUP-WIN2", "FAN_OUT", UUID.randomUUID(), "OPEN", null, null, Instant.now());
        
        alertProjector.handleAlert(event1);
        alertProjector.handleAlert(event2);
        
        Long totalAlerts = jdbcTemplate.queryForObject("SELECT count(*) FROM alerts WHERE alert_id IN (?, ?)", Long.class, alert1Id, alert2Id);
        assertEquals(2, totalAlerts, "Different deduplication keys (windows) should create two alerts");
    }

    @Test
    void testPartialEvidenceRollback() {
        UUID alertId = UUID.randomUUID();
        // Insert a dummy transaction first to avoid foreign key errors, wait, we don't have FKs on alert_accounts/transactions to accounts table? 
        // We will simulate a failure by using an invalid rule_type that violates a check constraint, or we can just mock an exception, but it's an integration test.
        // If we pass null to rule_type, it will fail NOT NULL constraint, rolling back everything.
        AlertEvent invalidEvent = new AlertEvent(alertId, "DEDUP-FAIL", null, UUID.randomUUID(), "OPEN", Set.of(UUID.randomUUID()), null, Instant.now());
        
        try {
            alertProjector.handleAlert(invalidEvent);
        } catch (Exception expected) {
            // expected due to NOT NULL on rule_type
        }
        
        Long alertCount = jdbcTemplate.queryForObject("SELECT count(*) FROM alerts WHERE alert_id = ?", Long.class, alertId);
        assertEquals(0, alertCount, "Alert should not exist due to rollback");
        
        Long accountCount = jdbcTemplate.queryForObject("SELECT count(*) FROM alert_accounts WHERE alert_id = ?", Long.class, alertId);
        assertEquals(0, accountCount, "Evidence should be rolled back");
    }
}
