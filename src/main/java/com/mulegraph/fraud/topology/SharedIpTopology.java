package com.mulegraph.fraud.topology;

import com.mulegraph.fraud.config.SharedIpRuleProperties;
import com.mulegraph.fraud.domain.FraudCandidateEvent;
import com.mulegraph.fraud.domain.SharedIpState;
import com.mulegraph.ingestion.domain.InternalTransactionEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Bytes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Grouped;
import org.apache.kafka.streams.kstream.Materialized;
import org.apache.kafka.streams.kstream.Produced;
import org.apache.kafka.streams.kstream.TimeWindows;
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Component
public class SharedIpTopology {

    private final SharedIpRuleProperties properties;

    @Autowired
    public SharedIpTopology(SharedIpRuleProperties properties) {
        this.properties = properties;
    }

    @Autowired
    public void buildTopology(StreamsBuilder builder) {
        JsonSerde<InternalTransactionEvent> eventSerde = new JsonSerde<>(InternalTransactionEvent.class);
        JsonSerde<SharedIpState> stateSerde = new JsonSerde<>(SharedIpState.class);
        JsonSerde<FraudCandidateEvent> candidateSerde = new JsonSerde<>(FraudCandidateEvent.class);

        TimeWindows tumblingWindow = TimeWindows
                .ofSizeAndGrace(
                        Duration.ofSeconds(properties.getWindowSeconds()),
                        Duration.ofSeconds(properties.getGraceSeconds())
                );

        builder.stream("transactions.by-ip", Consumed.with(Serdes.String(), eventSerde))
                .groupByKey(Grouped.with(Serdes.String(), eventSerde))
                .windowedBy(tumblingWindow)
                .aggregate(
                        SharedIpState::new,
                        (key, event, state) -> {
                            boolean wasEmitted = state.isCandidateEmitted();
                            state.getSources().add(event.sourceAccountId());
                            state.getTransactionIds().add(event.transactionId());
                            state.setTransactionCount(state.getTransactionCount() + 1);
                            state.setTotalAmountMinor(state.getTotalAmountMinor() + event.amountMinor());

                            if (!wasEmitted && state.getSources().size() >= properties.getMinDistinctSources()) {
                                state.setCandidateEmitted(true);
                                state.setThresholdCrossed(true);
                            } else {
                                state.setThresholdCrossed(false);
                            }
                            return state;
                        },
                        Materialized.<String, SharedIpState, WindowStore<Bytes, byte[]>>as("shared-ip-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(stateSerde)
                )
                .toStream()
                .map((windowedKey, state) -> {
                    if (state.isThresholdCrossed()) {
                        String ipHash = windowedKey.key();
                        Instant windowStart = windowedKey.window().startTime();
                        Instant windowEnd = windowedKey.window().endTime();

                        String uniqueString = String.format("SHARED_IP-%s-%d", ipHash, windowStart.toEpochMilli());
                        UUID candidateId = UUID.nameUUIDFromBytes(uniqueString.getBytes());
                        
                        // We use a deterministic UUID generated from the IP hash for primaryAccountId
                        UUID ipUuid = UUID.nameUUIDFromBytes(ipHash.getBytes());

                        FraudCandidateEvent candidate = new FraudCandidateEvent(
                                candidateId,
                                "SHARED_IP",
                                ipUuid,
                                windowStart,
                                windowEnd,
                                state.getSources().size(),
                                state.getTransactionCount(),
                                state.getTotalAmountMinor(),
                                "INR", // synthetic currency
                                state.getSources(),
                                state.getTransactionIds(),
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
