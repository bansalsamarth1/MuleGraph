package com.mulegraph.fraud.topology;

import com.mulegraph.fraud.domain.AlertEvent;
import com.mulegraph.fraud.domain.FraudCandidateEvent;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.kafka.support.serializer.JsonDeserializer;
import org.springframework.kafka.support.serializer.JsonSerializer;

import java.time.Instant;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;

class AlertGenerationTopologyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, FraudCandidateEvent> inputTopic;
    private TestOutputTopic<String, AlertEvent> outputTopic;

    @BeforeEach
    void setup() {
        AlertGenerationTopology topology = new AlertGenerationTopology();
        StreamsBuilder builder = new StreamsBuilder();
        topology.buildTopology(builder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "alert-generation-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        testDriver = new TopologyTestDriver(builder.build(), props);

        inputTopic = testDriver.createInputTopic(
                "fraud.candidates",
                new StringSerializer(),
                new JsonSerializer<>()
        );

        outputTopic = testDriver.createOutputTopic(
                "fraud.alerts",
                new StringDeserializer(),
                new JsonDeserializer<>(AlertEvent.class)
        );
    }

    @AfterEach
    void teardown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    void testCandidateMappedToAlert() {
        UUID candidateId = UUID.randomUUID();
        UUID primaryAccountId = UUID.randomUUID();
        
        FraudCandidateEvent candidate = new FraudCandidateEvent(
                candidateId,
                "DEDUP-KEY-123",
                "FAN_OUT",
                primaryAccountId,
                Instant.now(),
                Instant.now().plusSeconds(60),
                5,
                5,
                50000,
                "INR",
                Set.of(UUID.randomUUID(), UUID.randomUUID()),
                Set.of(UUID.randomUUID(), UUID.randomUUID()),
                Instant.now()
        );

        inputTopic.pipeInput(primaryAccountId.toString(), candidate);

        assertEquals(1, outputTopic.getQueueSize(), "Should emit exactly one alert");
        AlertEvent alert = outputTopic.readValue();
        
        assertEquals(candidateId, alert.alertId());
        assertEquals("OPEN", alert.status());
        assertEquals("FAN_OUT", alert.ruleType());
        assertEquals(2, alert.involvedAccounts().size());
        assertEquals(2, alert.transactionIds().size());
    }
}
