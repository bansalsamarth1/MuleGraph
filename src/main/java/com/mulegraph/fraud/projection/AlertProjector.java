package com.mulegraph.fraud.projection;

import com.mulegraph.fraud.domain.AlertEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

@Service
public class AlertProjector {

    private static final Logger log = LoggerFactory.getLogger(AlertProjector.class);
    private final JdbcTemplate jdbcTemplate;

    public AlertProjector(JdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @KafkaListener(topics = "fraud.alerts", groupId = "mulegraph-alert-projector-v1")
    @Transactional
    public void handleAlert(AlertEvent alert) {
        log.info("Received Alert {} for rule {}", alert.alertId(), alert.ruleType());

        // Idempotent insert into alerts
        jdbcTemplate.update("""
                INSERT INTO alerts (alert_id, deduplication_key, rule_type, primary_account_id, status, created_at)
                VALUES (?, ?, ?, ?, ?, ?)
                ON CONFLICT (deduplication_key) DO NOTHING
                """,
                alert.alertId(),
                alert.deduplicationKey(),
                alert.ruleType(),
                alert.primaryAccountId(),
                alert.status(),
                java.sql.Timestamp.from(alert.generatedAt())
        );

        // Idempotent insert into alert_accounts
        if (alert.involvedAccounts() != null) {
            for (java.util.UUID accountId : alert.involvedAccounts()) {
                jdbcTemplate.update("""
                        INSERT INTO alert_accounts (alert_id, account_id)
                        VALUES (?, ?)
                        ON CONFLICT DO NOTHING
                        """,
                        alert.alertId(),
                        accountId
                );
            }
        }

        // Idempotent insert into alert_transactions
        if (alert.transactionIds() != null) {
            for (java.util.UUID transactionId : alert.transactionIds()) {
                jdbcTemplate.update("""
                        INSERT INTO alert_transactions (alert_id, transaction_id)
                        VALUES (?, ?)
                        ON CONFLICT DO NOTHING
                        """,
                        alert.alertId(),
                        transactionId
                );
            }
        }
    }
}
