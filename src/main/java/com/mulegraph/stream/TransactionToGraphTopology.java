package com.mulegraph.stream;

import com.mulegraph.graph.event.GraphUpdateEvent;
import com.mulegraph.ingestion.domain.InternalTransactionEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

@Component
public class TransactionToGraphTopology {

    @Autowired
    public void buildTopology(StreamsBuilder builder) {
        KStream<String, InternalTransactionEvent> validatedStream = builder.stream(
                "transactions.validated",
                Consumed.with(Serdes.String(), new JsonSerde<>(InternalTransactionEvent.class))
        );

        validatedStream
                .mapValues(this::mapToGraphEvent)
                .selectKey((k, v) -> v.getSourceAccountId().toString())
                .to("graph.updates", Produced.with(Serdes.String(), new JsonSerde<>(GraphUpdateEvent.class)));
    }

    private GraphUpdateEvent mapToGraphEvent(InternalTransactionEvent tx) {
        GraphUpdateEvent evt = new GraphUpdateEvent();
        evt.setEventId(tx.eventId());
        evt.setTransactionId(tx.transactionId());
        evt.setSourceAccountId(tx.sourceAccountId());
        evt.setDestinationAccountId(tx.destinationAccountId());
        evt.setAmountMinor(tx.amountMinor());
        evt.setCurrency(tx.currency());
        evt.setDeviceId(tx.deviceId());
        evt.setIpHash(tx.ipHash());
        evt.setOccurredAt(tx.occurredAt());
        evt.setIngestedAt(tx.ingestedAt());
        evt.setSchemaVersion(tx.schemaVersion());
        if (tx.correlationId() != null) {
            evt.setCorrelationId(java.util.UUID.fromString(tx.correlationId()));
        }
        return evt;
    }
}
