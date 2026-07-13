package com.mulegraph.fraud.topology;

import com.mulegraph.fraud.config.FanOutRuleProperties;
import com.mulegraph.fraud.domain.FraudCandidateEvent;
import com.mulegraph.ingestion.domain.InternalTransactionEvent;
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
import java.util.UUID;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class FanOutTopologyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, InternalTransactionEvent> inputTopic;
    private TestOutputTopic<String, FraudCandidateEvent> outputTopic;

    @BeforeEach
    void setup() {
        FanOutRuleProperties properties = new FanOutRuleProperties();
        properties.setWindowSeconds(60);
        properties.setGraceSeconds(15);
        properties.setMinDistinctDestinations(5);

        FanOutTopology topology = new FanOutTopology(properties);
        StreamsBuilder builder = new StreamsBuilder();
        topology.buildTopology(builder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "fan-out-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        // No custom timestamp extractor needed here as we will pipe timestamped records

        testDriver = new TopologyTestDriver(builder.build(), props);

        inputTopic = testDriver.createInputTopic(
                "transactions.by-source",
                new StringSerializer(),
                new JsonSerializer<>()
        );

        outputTopic = testDriver.createOutputTopic(
                "fraud.candidates",
                new StringDeserializer(),
                new JsonDeserializer<>(FraudCandidateEvent.class)
        );
    }

    @AfterEach
    void teardown() {
        if (testDriver != null) {
            testDriver.close();
        }
    }

    @Test
    void testFourDistinctDestinations_DoesNotTrigger() {
        UUID source = UUID.randomUUID();
        Instant now = Instant.now();

        for (int i = 0; i < 4; i++) {
            inputTopic.pipeInput(
                    source.toString(),
                    createEvent(source, UUID.randomUUID()),
                    now.plusSeconds(i).toEpochMilli()
            );
        }

        assertTrue(outputTopic.isEmpty(), "Output should be empty for 4 destinations");
    }

    @Test
    void testFiveDistinctDestinations_TriggersExactlyOneCandidate() {
        UUID source = UUID.randomUUID();
        Instant now = Instant.now();

        for (int i = 0; i < 5; i++) {
            inputTopic.pipeInput(
                    source.toString(),
                    createEvent(source, UUID.randomUUID()),
                    now.plusSeconds(i).toEpochMilli()
            );
        }

        assertEquals(1, outputTopic.getQueueSize(), "Should emit exactly one candidate");
        FraudCandidateEvent candidate = outputTopic.readValue();
        assertEquals(5, candidate.distinctAccountsCount());
        assertEquals("FAN_OUT", candidate.ruleType());
        assertEquals(source, candidate.primaryAccountId());

        // Piping 6th should NOT trigger another candidate for the same window
        inputTopic.pipeInput(
                source.toString(),
                createEvent(source, UUID.randomUUID()),
                now.plusSeconds(10).toEpochMilli()
        );
        assertTrue(outputTopic.isEmpty(), "Should not emit a second candidate for the same window");
    }

    @Test
    void testFiveTransfersToSameDestination_DoesNotTrigger() {
        UUID source = UUID.randomUUID();
        UUID dest = UUID.randomUUID();
        Instant now = Instant.now();

        for (int i = 0; i < 5; i++) {
            inputTopic.pipeInput(
                    source.toString(),
                    createEvent(source, dest),
                    now.plusSeconds(i).toEpochMilli()
            );
        }

        assertTrue(outputTopic.isEmpty(), "Output should be empty for 5 transfers to same destination");
    }

    @Test
    void testMultipleWindows_TriggersAppropriately() {
        UUID source = UUID.randomUUID();
        Instant now = Instant.parse("2026-01-01T10:00:00Z");

        // Window 1: 5 distinct
        for (int i = 0; i < 5; i++) {
            inputTopic.pipeInput(source.toString(), createEvent(source, UUID.randomUUID()), now.plusSeconds(i).toEpochMilli());
        }

        assertEquals(1, outputTopic.getQueueSize());
        outputTopic.readValue(); // clear it

        // Window 2: (starts at 10:01:00Z) - 5 distinct
        Instant window2Time = now.plusSeconds(61);
        for (int i = 0; i < 5; i++) {
            inputTopic.pipeInput(source.toString(), createEvent(source, UUID.randomUUID()), window2Time.plusSeconds(i).toEpochMilli());
        }

        assertEquals(1, outputTopic.getQueueSize(), "Should trigger new candidate for a new window");
    }

    private InternalTransactionEvent createEvent(UUID source, UUID dest) {
        return new InternalTransactionEvent(
                UUID.randomUUID(),
                UUID.randomUUID(),
                1,
                UUID.randomUUID().toString(),
                source,
                dest,
                10000L,
                "INR",
                "device-1",
                "hash-1",
                Instant.now(),
                Instant.now()
        );
    }
}
