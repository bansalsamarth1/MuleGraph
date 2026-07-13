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

@Disabled("Testcontainers docker-java version incompatibility with host Docker API 1.54")
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
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
        // Disable kafka auto-start since we only test the projector bean method
        registry.add("spring.kafka.listener.auto-startup", () -> "false");
    }

    @Autowired
    private AlertProjector alertProjector;

    @Autowired
    private JdbcTemplate jdbcTemplate;

    @Test
    void testIdempotentAlertProjection() {
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
}
