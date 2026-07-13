package com.mulegraph.ingestion.topology;

import com.mulegraph.ingestion.domain.InternalTransactionEvent;
import com.mulegraph.ingestion.domain.InvalidTransactionRecord;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.time.Instant;
import java.util.Map;

@Component
public class TransactionNormalizationTopology {

    @Autowired
    public void buildTopology(StreamsBuilder builder) {
        JsonSerde<InternalTransactionEvent> eventSerde = new JsonSerde<>(InternalTransactionEvent.class);
        JsonSerde<InvalidTransactionRecord> invalidSerde = new JsonSerde<>(InvalidTransactionRecord.class);

        KStream<String, InternalTransactionEvent> rawStream = builder.stream(
                "transactions.raw",
                Consumed.with(Serdes.String(), eventSerde)
        );

        rawStream.split()
                .branch((key, event) -> isValid(event), org.apache.kafka.streams.kstream.Branched.withConsumer(ks ->
                        ks.to("transactions.validated", Produced.with(Serdes.String(), eventSerde))
                ))
                .defaultBranch(org.apache.kafka.streams.kstream.Branched.withConsumer(ks ->
                        ks.mapValues(event -> new InvalidTransactionRecord(
                                event,
                                getFailureReason(event),
                                Instant.now()
                        )).to("transactions.invalid", Produced.with(Serdes.String(), invalidSerde))
                ));
    }

    private boolean isValid(InternalTransactionEvent event) {
        return event != null
                && event.eventId() != null
                && event.transactionId() != null
                && event.sourceAccountId() != null
                && event.destinationAccountId() != null
                && !event.sourceAccountId().equals(event.destinationAccountId())
                && event.amountMinor() > 0
                && event.currency() != null
                && event.currency().matches("^[A-Z]{3}$")
                && event.schemaVersion() == 1
                && event.occurredAt() != null
                && event.ingestedAt() != null;
    }

    private String getFailureReason(InternalTransactionEvent event) {
        if (event == null) return "Event is null";
        if (event.eventId() == null || event.transactionId() == null) return "Missing IDs";
        if (event.sourceAccountId() == null || event.destinationAccountId() == null) return "Missing account IDs";
        if (event.sourceAccountId().equals(event.destinationAccountId())) return "Source equals destination";
        if (event.amountMinor() <= 0) return "Amount must be positive";
        if (event.currency() == null || !event.currency().matches("^[A-Z]{3}$")) return "Invalid currency format";
        if (event.schemaVersion() != 1) return "Unsupported schema version";
        if (event.occurredAt() == null || event.ingestedAt() == null) return "Missing timestamps";
        return "Unknown validation failure";
    }
}
