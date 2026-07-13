package com.mulegraph.fraud.topology;

import com.mulegraph.fraud.config.FanOutRuleProperties;
import com.mulegraph.fraud.domain.FanOutState;
import com.mulegraph.fraud.domain.FraudCandidateEvent;
import com.mulegraph.ingestion.domain.InternalTransactionEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.kstream.Windowed;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Component
public class FanOutTopology {

    private final FanOutRuleProperties properties;

    public FanOutTopology(FanOutRuleProperties properties) {
        this.properties = properties;
    }

    @Autowired
    public void buildTopology(StreamsBuilder builder) {
        JsonSerde<InternalTransactionEvent> eventSerde = new JsonSerde<>(InternalTransactionEvent.class);
        JsonSerde<FanOutState> stateSerde = new JsonSerde<>(FanOutState.class);
        JsonSerde<FraudCandidateEvent> candidateSerde = new JsonSerde<>(FraudCandidateEvent.class);

        KStream<String, InternalTransactionEvent> sourceStream = builder.stream(
                "transactions.by-source",
                Consumed.with(Serdes.String(), eventSerde)
        );

        TimeWindows tumblingWindow = TimeWindows
                .ofSizeAndGrace(
                        Duration.ofSeconds(properties.getWindowSeconds()),
                        Duration.ofSeconds(properties.getGraceSeconds())
                );

        sourceStream
                .groupByKey(Grouped.with(Serdes.String(), eventSerde))
                .windowedBy(tumblingWindow)
                .aggregate(
                        FanOutState::new,
                        (key, event, state) -> {
                            boolean wasEmitted = state.isCandidateEmitted();
                            state.getDestinations().add(event.destinationAccountId());
                            state.setTransactionCount(state.getTransactionCount() + 1);
                            state.setTotalAmountMinor(state.getTotalAmountMinor() + event.amountMinor());
                            
                            if (!wasEmitted && state.getDestinations().size() >= properties.getMinDistinctDestinations()) {
                                state.setCandidateEmitted(true);
                                state.setThresholdCrossed(true);
                            } else {
                                state.setThresholdCrossed(false);
                            }
                            return state;
                        },
                        Materialized.<String, FanOutState, WindowStore<Bytes, byte[]>>as("fan-out-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(stateSerde)
                )
                .toStream()
                // Transform to Candidate exactly when threshold is breached for the first time
                .map((windowedKey, state) -> {
                    if (state.isThresholdCrossed()) {
                        UUID primaryAccountId = UUID.fromString(windowedKey.key());
                        Instant windowStart = windowedKey.window().startTime();
                        Instant windowEnd = windowedKey.window().endTime();
                        
                        String uniqueString = String.format("FAN_OUT-%s-%d", primaryAccountId, windowStart.toEpochMilli());
                        UUID candidateId = UUID.nameUUIDFromBytes(uniqueString.getBytes());
                        
                        FraudCandidateEvent candidate = new FraudCandidateEvent(
                                candidateId,
                                "FAN_OUT",
                                primaryAccountId,
                                windowStart,
                                windowEnd,
                                state.getDestinations().size(),
                                state.getTransactionCount(),
                                state.getTotalAmountMinor(),
                                "INR", // synthetic currency
                                Instant.now()
                        );
                        return KeyValue.pair(windowedKey.key(), candidate);
                    }
                    return KeyValue.pair(windowedKey.key(), (FraudCandidateEvent) null);
                })
                .filter((key, candidate) -> candidate != null)
                .to("fraud.candidates", Produced.with(Serdes.String(), candidateSerde));
    }
}
