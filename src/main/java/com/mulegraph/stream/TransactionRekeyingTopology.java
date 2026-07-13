package com.mulegraph.stream;

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
public class TransactionRekeyingTopology {

    @Autowired
    public void buildTopology(StreamsBuilder builder) {
        JsonSerde<InternalTransactionEvent> eventSerde = new JsonSerde<>(InternalTransactionEvent.class);
        
        KStream<String, InternalTransactionEvent> rawStream = builder.stream(
                "transactions.validated",
                Consumed.with(Serdes.String(), eventSerde)
        );

        // Rekey by source account
        rawStream.selectKey((k, v) -> v.sourceAccountId().toString())
                .to("transactions.by-source", Produced.with(Serdes.String(), eventSerde));

        // Rekey by destination account
        rawStream.selectKey((k, v) -> v.destinationAccountId().toString())
                .to("transactions.by-destination", Produced.with(Serdes.String(), eventSerde));

        // Rekey by device ID
        rawStream.selectKey((k, v) -> v.deviceId())
                .to("transactions.by-device", Produced.with(Serdes.String(), eventSerde));

        // Rekey by IP Hash
        rawStream.selectKey((k, v) -> v.ipHash())
                .to("transactions.by-ip", Produced.with(Serdes.String(), eventSerde));
    }
}
