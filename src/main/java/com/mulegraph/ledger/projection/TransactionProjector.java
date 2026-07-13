package com.mulegraph.ledger.projection;

import com.mulegraph.ingestion.domain.InternalTransactionEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.core.namedparam.MapSqlParameterSource;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Component;

import java.sql.Timestamp;
import java.time.Instant;

@Component
public class TransactionProjector {

    private static final Logger log = LoggerFactory.getLogger(TransactionProjector.class);
    private final NamedParameterJdbcTemplate jdbcTemplate;

    public TransactionProjector(NamedParameterJdbcTemplate jdbcTemplate) {
        this.jdbcTemplate = jdbcTemplate;
    }

    @KafkaListener(topics = "transactions.validated", groupId = "mulegraph-transaction-projector-v1")
    public void project(InternalTransactionEvent event) {
        try {
            String sql = """
                INSERT INTO transactions (
                    transaction_id, event_id, source_account_id, destination_account_id,
                    amount_minor, currency, device_id, ip_hash,
                    occurred_at, ingested_at, processed_at
                ) VALUES (
                    :transactionId, :eventId, :sourceAccountId, :destinationAccountId,
                    :amountMinor, :currency, :deviceId, :ipHash,
                    :occurredAt, :ingestedAt, :processedAt
                ) ON CONFLICT (transaction_id) DO NOTHING
            """;

            MapSqlParameterSource params = new MapSqlParameterSource()
                    .addValue("transactionId", event.transactionId())
                    .addValue("eventId", event.eventId())
                    .addValue("sourceAccountId", event.sourceAccountId())
                    .addValue("destinationAccountId", event.destinationAccountId())
                    .addValue("amountMinor", event.amountMinor())
                    .addValue("currency", event.currency())
                    .addValue("deviceId", event.deviceId())
                    .addValue("ipHash", event.ipHash())
                    .addValue("occurredAt", Timestamp.from(event.occurredAt()))
                    .addValue("ingestedAt", Timestamp.from(event.ingestedAt()))
                    .addValue("processedAt", Timestamp.from(Instant.now()));

            int rowsAffected = jdbcTemplate.update(sql, params);
            if (rowsAffected > 0) {
                log.info("Persisted transaction {}", event.transactionId());
            } else {
                log.info("Transaction {} already exists, ignoring duplicate", event.transactionId());
            }
        } catch (Exception e) {
            log.error("Failed to project transaction {}", event.transactionId(), e);
            throw e;
        }
    }
}
