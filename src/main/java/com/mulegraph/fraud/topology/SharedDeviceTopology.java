package com.mulegraph.fraud.topology;

import com.mulegraph.fraud.config.SharedDeviceRuleProperties;
import com.mulegraph.fraud.domain.FraudCandidateEvent;
import com.mulegraph.fraud.domain.SharedDeviceState;
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
import org.apache.kafka.streams.state.WindowStore;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.time.Instant;
import java.util.UUID;

@Component
public class SharedDeviceTopology {

    private final SharedDeviceRuleProperties properties;

    @Autowired
    public SharedDeviceTopology(SharedDeviceRuleProperties properties) {
        this.properties = properties;
    }

    @Autowired
    public void buildTopology(StreamsBuilder builder) {
        JsonSerde<InternalTransactionEvent> eventSerde = new JsonSerde<>(InternalTransactionEvent.class);
        JsonSerde<SharedDeviceState> stateSerde = new JsonSerde<>(SharedDeviceState.class);
        JsonSerde<FraudCandidateEvent> candidateSerde = new JsonSerde<>(FraudCandidateEvent.class);

        TimeWindows tumblingWindow = TimeWindows
                .ofSizeAndGrace(
                        Duration.ofSeconds(properties.getWindowSeconds()),
                        Duration.ofSeconds(properties.getGraceSeconds())
                );

        var sourceStream = builder.stream("transactions.by-device", Consumed.with(Serdes.String(), eventSerde));
        sourceStream
                .groupBy((key, event) -> key + ":" + event.currency(), Grouped.with(Serdes.String(), eventSerde))
                .windowedBy(tumblingWindow)
                .aggregate(
                        SharedDeviceState::new,
                        (key, event, state) -> {
                            boolean wasEmitted = state.isCandidateEmitted();
                            state.getSources().add(event.sourceAccountId());
                            state.getTransactionIds().add(event.transactionId());
                            state.setTransactionCount(state.getTransactionCount() + 1);
                            state.setTotalAmountMinor(state.getTotalAmountMinor() + event.amountMinor());
                            state.setCurrency(event.currency());

                            boolean distinctMet = state.getSources().size() >= properties.getMinDistinctSources();
                            boolean countMet = properties.getMinTransactionCount() <= 0 || state.getTransactionCount() >= properties.getMinTransactionCount();
                            boolean amountMet = properties.getMinTotalAmountMinor() <= 0 || state.getTotalAmountMinor() >= properties.getMinTotalAmountMinor();

                            if (!wasEmitted && distinctMet && countMet && amountMet) {
                                state.setCandidateEmitted(true);
                                state.setThresholdCrossed(true);
                            } else {
                                state.setThresholdCrossed(false);
                            }
                            return state;
                        },
                        Materialized.<String, SharedDeviceState, WindowStore<Bytes, byte[]>>as("shared-device-store")
                                .withKeySerde(Serdes.String())
                                .withValueSerde(stateSerde)
                )
                .toStream()
                .map((windowedKey, state) -> {
                    if (state.isThresholdCrossed()) {
                        String[] parts = windowedKey.key().split(":");
                        String deviceId = parts[0];
                        String currency = parts.length > 1 ? parts[1] : "UNKNOWN";
                        Instant windowStart = windowedKey.window().startTime();
                        Instant windowEnd = windowedKey.window().endTime();

                        String uniqueString = String.format("SHARED_DEVICE-%s-%s-%d", deviceId, currency, windowStart.toEpochMilli());
                        UUID candidateId = UUID.nameUUIDFromBytes(uniqueString.getBytes());
                        UUID primaryAccountId = UUID.nameUUIDFromBytes(deviceId.getBytes());

                        FraudCandidateEvent candidate = new FraudCandidateEvent(
                                candidateId,
                                uniqueString,
                                "SHARED_DEVICE",
                                primaryAccountId,
                                windowStart,
                                windowEnd,
                                state.getSources().size(),
                                state.getTransactionCount(),
                                state.getTotalAmountMinor(),
                                state.getCurrency(),
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
