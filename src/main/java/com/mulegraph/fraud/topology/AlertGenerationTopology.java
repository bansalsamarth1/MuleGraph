package com.mulegraph.fraud.topology;

import com.mulegraph.fraud.domain.AlertEvent;
import com.mulegraph.fraud.domain.FraudCandidateEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.support.serializer.JsonSerde;
import org.springframework.stereotype.Component;

@Component
public class AlertGenerationTopology {

    @Autowired
    public void buildTopology(StreamsBuilder builder) {
        JsonSerde<FraudCandidateEvent> candidateSerde = new JsonSerde<>(FraudCandidateEvent.class);
        JsonSerde<AlertEvent> alertSerde = new JsonSerde<>(AlertEvent.class);

        builder.stream("fraud.candidates", Consumed.with(Serdes.String(), candidateSerde))
                .mapValues(candidate -> new AlertEvent(
                        candidate.candidateId(),
                        candidate.deduplicationKey(),
                        candidate.ruleType(),
                        candidate.primaryAccountId(),
                        "OPEN",
                        candidate.involvedAccounts(),
                        candidate.transactionIds(),
                        candidate.generatedAt()
                ))
                .to("fraud.alerts", Produced.with(Serdes.String(), alertSerde));
    }
}
