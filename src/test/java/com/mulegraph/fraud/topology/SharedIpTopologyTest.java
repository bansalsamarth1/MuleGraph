package com.mulegraph.fraud.topology;

import com.mulegraph.fraud.config.SharedIpRuleProperties;
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

class SharedIpTopologyTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, InternalTransactionEvent> inputTopic;
    private TestOutputTopic<String, FraudCandidateEvent> outputTopic;

    @BeforeEach
    void setup() {
        SharedIpRuleProperties properties = new SharedIpRuleProperties();
        properties.setWindowSeconds(60);
        properties.setGraceSeconds(15);
        properties.setMinDistinctSources(3);

        SharedIpTopology topology = new SharedIpTopology(properties);
        StreamsBuilder builder = new StreamsBuilder();
        topology.buildTopology(builder);

        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "shared-ip-test");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");

        testDriver = new TopologyTestDriver(builder.build(), props);

        inputTopic = testDriver.createInputTopic(
                "transactions.by-ip",
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
    void testTwoDistinctSources_DoesNotTrigger() {
        String ipHash = "hash-456";
        Instant now = Instant.now();

        for (int i = 0; i < 2; i++) {
            inputTopic.pipeInput(
                    ipHash,
                    createEvent(UUID.randomUUID(), ipHash),
                    now.plusSeconds(i).toEpochMilli()
            );
        }

        assertTrue(outputTopic.isEmpty(), "Output should be empty for 2 sources");
    }

    @Test
    void testThreeDistinctSources_TriggersExactlyOneCandidate() {
        String ipHash = "hash-456";
        Instant now = Instant.now();

        for (int i = 0; i < 3; i++) {
            inputTopic.pipeInput(
                    ipHash,
                    createEvent(UUID.randomUUID(), ipHash),
                    now.plusSeconds(i).toEpochMilli()
            );
        }

        assertEquals(1, outputTopic.getQueueSize(), "Should emit exactly one candidate");
        FraudCandidateEvent candidate = outputTopic.readValue();
        assertEquals(3, candidate.distinctAccountsCount());
        assertEquals("SHARED_IP", candidate.ruleType());
        
        UUID expectedIpUuid = UUID.nameUUIDFromBytes(ipHash.getBytes());
        assertEquals(expectedIpUuid, candidate.primaryAccountId());

        // Piping 4th should NOT trigger another candidate for the same window
        inputTopic.pipeInput(
                ipHash,
                createEvent(UUID.randomUUID(), ipHash),
                now.plusSeconds(10).toEpochMilli()
        );
        assertTrue(outputTopic.isEmpty(), "Should not emit a second candidate for the same window");
    }

    private InternalTransactionEvent createEvent(UUID source, String ipHash) {
        return new InternalTransactionEvent(
                UUID.randomUUID(),
                UUID.randomUUID(),
                1,
                UUID.randomUUID().toString(),
                source,
                UUID.randomUUID(),
                10000L,
                "INR",
                "device-1",
                ipHash,
                Instant.now(),
                Instant.now()
        );
    }
}
